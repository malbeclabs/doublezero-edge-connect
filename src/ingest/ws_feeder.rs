//! Hyperliquid **public** WebSocket input feeder — a second ingest source, off by default, that
//! backstops the DZ Edge multicast feed.
//!
//! It connects to Hyperliquid's own `wss://api.hyperliquid.xyz/ws`, subscribes `bbo` + `trades` per
//! configured coin, decodes the JSON into the same `FeedMessage`s the multicast pipeline produces,
//! and emits them through the **shared [`crate::ingest::arbiter`]** as [`Publisher::PublicWs`]. It is
//! a different transport from the multicast receiver — it never touches the `FrameProcessor` /
//! `recv_any` machinery — but it converges on the *same* per-`(venue, symbol)` latch-to-leader floor,
//! so a public copy of an update the edge already emitted collapses into a no-op, and when the edge
//! gaps the public copy is the first to cross the floor and fills in. That backstop falls out of the
//! floor with no health check (see the arbiter docs).
//!
//! **Failure isolation.** The feeder is its own task with reconnect + exponential backoff; every
//! decode/socket error is logged and swallowed, so neither a reconnect storm nor a malformed frame
//! can ever wedge the multicast hot path.
//!
//! **Precision before price.** Each public quote/trade is gated on its `(venue, symbol)` instrument
//! already being present in the shared [`InstrumentSnapshot`] (populated by the edge refdata stream).
//! The realistic backstop scenario is edge refdata healthy while mktdata stalls; a standalone public
//! feed with no edge refdata ever is a documented limitation (it would emit nothing).
//!
//! ⚠️ Decimal-string px/sz are parsed straight to real-unit `f64`s — the same unit space the edge
//! side produces via `apply_exponent` — so no canonical-exponent rescale is needed. Cross-source
//! dedup is decided by publisher leadership per tick, never by content equality (see the arbiter).

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::{
    ingest::arbiter::{lock, Publisher, SharedArbiter},
    metrics::metrics,
    model::{now_ns, FeedMessage, InstrumentSnapshot, NormalizedQuote, NormalizedTrade},
};

/// Hyperliquid's public WebSocket endpoint.
pub const DEFAULT_WS_INPUT_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// The venue every public message is tagged with (must match the edge HL feed's venue so both land
/// in the same arbiter floor).
const HL_VENUE: &str = "Hyperliquid";

/// Hyperliquid documents a cap of 1000 subscriptions per WebSocket connection. We fan out two
/// subscriptions (`bbo` + `trades`) per coin over a single connection and log if the configured coin
/// set would exceed the cap.
const HL_MAX_SUBSCRIPTIONS_PER_CONN: usize = 1000;

/// Reconnect backoff bounds. Backoff resets to the minimum once a connection is established, then
/// doubles up to the maximum across consecutive failures.
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Minimum session duration that counts as "stable": only after staying connected this long do we
/// reset the backoff to `BACKOFF_MIN`. A connect-then-immediate-drop (rejected subscriptions,
/// instant server close) keeps escalating instead of pinning at the floor and hammering the public
/// endpoint ~2 reconnects/sec.
const STABLE_SESSION: Duration = Duration::from_secs(30);

/// One Hyperliquid WS message envelope: a channel tag plus its channel-specific payload.
#[derive(Deserialize)]
struct Envelope {
    channel: String,
    #[serde(default)]
    data: serde_json::Value,
}

/// A `bbo` payload: the coin, the venue block time (ms), and the two-sided top of book. Either side
/// can be `null` (a one-sided book), in which case we cannot form a two-sided quote and skip it.
#[derive(Deserialize)]
struct BboData {
    coin: String,
    time: u64,
    bbo: [Option<Level>; 2],
}

/// One book level. `n` is the order/source count at this level — the public-feed counterpart of the
/// edge's `Bid/Ask Source Count`; it is part of the canonical `bbo_hash` identity the arbiter keys
/// on, so both sources must report it (absent → 0, "unavailable", matching the edge sentinel).
#[derive(Deserialize)]
struct Level {
    px: String,
    sz: String,
    #[serde(default)]
    n: u16,
}

/// A `trades` payload element. `tid` is Hyperliquid's trade id — the same value the edge feed carries
/// as `trade_id`, so the arbiter's windowed trade dedup collapses cross-source copies on it.
#[derive(Deserialize)]
struct TradeData {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    tid: u64,
}

/// Run the public WS input feeder forever (reconnecting on any failure). Returns immediately as a
/// no-op when `coins` is empty (the feeder is off by default). Spawned as its own task; it never
/// propagates an error, so the multicast hot path is unaffected by its churn.
pub async fn run(
    url: String,
    coins: Vec<String>,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
) {
    if coins.is_empty() {
        return;
    }
    let want_subs = coins.len() * 2; // bbo + trades per coin
    if want_subs > HL_MAX_SUBSCRIPTIONS_PER_CONN {
        warn!(
            coins = coins.len(),
            subscriptions = want_subs,
            cap = HL_MAX_SUBSCRIPTIONS_PER_CONN,
            "public WS coin set exceeds Hyperliquid's per-connection subscription cap; \
             some subscriptions may be rejected"
        );
    }

    let m = metrics();
    let mut backoff = BACKOFF_MIN;
    loop {
        match connect_async(&url).await {
            Ok((ws, _resp)) => {
                info!(%url, coins = ?coins, "public WS input feeder connected");
                m.ws_feeder_up.set(1);
                let started = Instant::now();
                match stream(ws, &coins, &arbiter, &instruments).await {
                    Ok(()) => info!("public WS input feeder closed; reconnecting"),
                    Err(e) => {
                        warn!(error = %e, "public WS input feeder session error; reconnecting")
                    }
                }
                m.ws_feeder_up.set(0);
                // Reset the backoff only after a *stable* session; a connect-then-immediate-drop
                // keeps escalating so a flapping endpoint isn't hammered (see `STABLE_SESSION`).
                backoff = if started.elapsed() >= STABLE_SESSION {
                    BACKOFF_MIN
                } else {
                    (backoff * 2).min(BACKOFF_MAX)
                };
            }
            Err(e) => {
                warn!(error = %e, %url, "public WS input feeder connect failed; retrying");
                m.ws_feeder_up.set(0);
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
        // Each loop iteration past the initial connect is one reconnect cycle (a drop or a failed
        // attempt, now backing off to retry).
        m.ws_feeder_reconnects.inc();
        tokio::time::sleep(backoff).await;
    }
}

/// Subscribe `bbo` + `trades` for every coin on the open connection, then pump decoded messages into
/// the arbiter until the socket closes or errors.
async fn stream<S>(
    mut ws: S,
    coins: &[String],
    arbiter: &SharedArbiter,
    instruments: &InstrumentSnapshot,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
        + StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    for coin in coins {
        for kind in ["bbo", "trades"] {
            let sub = format!(
                r#"{{"method":"subscribe","subscription":{{"type":"{kind}","coin":"{coin}"}}}}"#
            );
            ws.send(Message::Text(sub.into())).await?;
        }
    }

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(txt) => handle_text(&txt, arbiter, instruments),
            // Reply to server pings so the connection is not reaped while a coin is quiet.
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// Decode one text frame and emit any resulting quote/trade. Unknown channels (e.g.
/// `subscriptionResponse`, `pong`) and malformed payloads are ignored — this is a best-effort feed.
fn handle_text(txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
    let env: Envelope = match serde_json::from_str(txt) {
        Ok(e) => e,
        Err(e) => {
            metrics().ws_feeder_decode_errors.inc();
            debug!(error = %e, "public WS: undecodable frame ignored");
            return;
        }
    };
    match env.channel.as_str() {
        "bbo" => {
            if let Ok(d) = serde_json::from_value::<BboData>(env.data) {
                emit_bbo(d, arbiter, instruments);
            }
        }
        "trades" => {
            if let Ok(trades) = serde_json::from_value::<Vec<TradeData>>(env.data) {
                for t in trades {
                    emit_trade(t, arbiter, instruments);
                }
            }
        }
        _ => {} // subscriptionResponse, pong, error, etc. — nothing to emit
    }
}

/// True if the `(HL_VENUE, symbol)` instrument is already in the shared snapshot, so a price emitted
/// for it carries known precision (precision before price).
fn instrument_known(instruments: &InstrumentSnapshot, symbol: &str) -> bool {
    crate::model::lock(instruments).contains_key(&(HL_VENUE.to_string(), symbol.to_string()))
}

/// Parse a non-negative, finite `f64` from a decimal string, or `None`. Rejects `NaN`/`±inf`
/// (which `str::parse::<f64>` accepts from `"nan"`/`"inf"` and produces on overflow like `"1e400"`)
/// and negatives, so a malformed public px/sz is dropped rather than emitted — a non-finite would
/// otherwise serialize to JSON `null` on the wire (breaking the numeric contract) and a `NaN` would
/// defeat the floor's content dedup (`NaN != NaN`).
fn parse_decimal(s: &str) -> Option<f64> {
    let v: f64 = s.parse().ok()?;
    (v.is_finite() && v >= 0.0).then_some(v)
}

/// Parse a decimal-string level into real-unit `(price, size)` `f64`s, or `None` if either fails or
/// is non-finite/negative.
fn parse_level(l: &Level) -> Option<(f64, f64, u16)> {
    Some((parse_decimal(&l.px)?, parse_decimal(&l.sz)?, l.n))
}

/// Convert a public block time in **milliseconds** to nanoseconds, or `None` if it is unusable.
/// Rejects `0` (the "not available" sentinel — never a real block time; passing it through would
/// make this public quote bypass the floor and emit as an undeduped duplicate of the edge copy) and
/// a multiply that would overflow `u64` (a saturated `u64::MAX` `source_ts` would advance the floor's
/// high-water to the maximum and permanently drop every later real quote for that `(venue, symbol)`
/// as stale — a one-symbol wedge until restart; the arbiter also clamps implausibly-far-future
/// timestamps as a second line of defense).
fn block_time_ms_to_ns(time_ms: u64) -> Option<u64> {
    if time_ms == 0 {
        return None;
    }
    time_ms.checked_mul(1_000_000)
}

/// Build a `NormalizedQuote` from a public `bbo` and emit it through the arbiter as `PublicWs`.
/// Skips one-sided books (a quote needs both sides), unparseable px/sz, and symbols whose instrument
/// definition is not yet known.
fn emit_bbo(d: BboData, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
    let (Some(bid), Some(ask)) = (&d.bbo[0], &d.bbo[1]) else {
        return; // one-sided book; cannot form a two-sided quote
    };
    if !instrument_known(instruments, &d.coin) {
        return; // precision unknown; drop until the edge refdata defines this instrument
    }
    let (Some((bid_px, bid_sz, bid_n)), Some((ask_px, ask_sz, ask_n))) =
        (parse_level(bid), parse_level(ask))
    else {
        return;
    };
    // Public block time (ms) → ns: the SAME canonical source_ts the edge copy carries
    // (`source_timestamp_ns = block_time_ms × 1_000_000`), so both land in one floor tick.
    let Some(source_ts_ns) = block_time_ms_to_ns(d.time) else {
        return;
    };
    let quote = NormalizedQuote {
        venue: HL_VENUE.to_string(),
        symbol: d.coin,
        bid: bid_px,
        ask: ask_px,
        bid_size: bid_sz,
        ask_size: ask_sz,
        bid_n,
        ask_n,
        source_ts_ns,
        recv_ts_ns: now_ns(),
        kernel_rx_ts_ns: 0, // no kernel RX timestamp for a user-space WS read (0 = sentinel)
        ws_send_ts_ns: 0,   // stamped by the WS server just before send
    };
    metrics()
        .ws_feeder_messages
        .with_label_values(&["quote"])
        .inc();
    lock(arbiter).emit(FeedMessage::Quote(quote), Publisher::PublicWs);
}

/// Build a `NormalizedTrade` from a public `trades` element and emit it through the arbiter.
fn emit_trade(t: TradeData, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
    if !instrument_known(instruments, &t.coin) {
        return;
    }
    let (Some(price), Some(size)) = (parse_decimal(&t.px), parse_decimal(&t.sz)) else {
        return;
    };
    let Some(source_ts_ns) = block_time_ms_to_ns(t.time) else {
        return;
    };
    let trade = NormalizedTrade {
        venue: HL_VENUE.to_string(),
        symbol: t.coin,
        price,
        size,
        // HL trade side: "B" = aggressing buy, "A" = aggressing sell.
        aggressor_side: match t.side.as_str() {
            "B" => "buy",
            "A" => "sell",
            _ => "unknown",
        }
        .to_string(),
        trade_id: t.tid,
        cumulative_volume: 0.0, // not carried on the public trades feed
        source_ts_ns,
        recv_ts_ns: now_ns(),
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    };
    metrics()
        .ws_feeder_messages
        .with_label_values(&["trade"])
        .inc();
    lock(arbiter).emit(FeedMessage::Trade(trade), Publisher::PublicWs);
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tokio::sync::broadcast;

    use super::*;
    use crate::{ingest::arbiter::Arbiter, model::NormalizedInstrument};

    fn instruments_with(symbol: &str) -> InstrumentSnapshot {
        let map = Arc::new(Mutex::new(HashMap::new()));
        map.lock().unwrap().insert(
            (HL_VENUE.to_string(), symbol.to_string()),
            NormalizedInstrument {
                venue: HL_VENUE.to_string(),
                symbol: symbol.to_string(),
                price_exponent: -2,
                qty_exponent: -2,
            },
        );
        map
    }

    fn arbiter_with_rx() -> (SharedArbiter, broadcast::Receiver<FeedMessage>) {
        let (tx, rx) = broadcast::channel(64);
        (Arc::new(Mutex::new(Arbiter::new(tx, 8))), rx)
    }

    /// A well-formed `bbo` frame decodes to a quote with ms→ns source_ts and real-unit f64 px/sz.
    #[test]
    fn bbo_frame_emits_quote() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("BTC");
        let frame = r#"{"channel":"bbo","data":{"coin":"BTC","time":1700000000000,
            "bbo":[{"px":"104783.0","sz":"1.5","n":3},{"px":"104784.0","sz":"2.0","n":4}]}}"#;
        handle_text(frame, &arbiter, &instruments);
        match rx.try_recv().expect("a quote was emitted") {
            FeedMessage::Quote(q) => {
                assert_eq!(q.venue, "Hyperliquid");
                assert_eq!(q.symbol, "BTC");
                assert_eq!(q.bid, 104783.0);
                assert_eq!(q.ask, 104784.0);
                assert_eq!(q.bid_size, 1.5);
                assert_eq!(q.ask_size, 2.0);
                // ms × 1e6 == ns, matching the edge's canonical source_ts.
                assert_eq!(q.source_ts_ns, 1700000000000 * 1_000_000);
            }
            other => panic!("expected a quote, got {other:?}"),
        }
    }

    /// Precision-before-price: a quote for an unknown instrument is dropped (snapshot empty).
    #[test]
    fn bbo_without_instrument_is_dropped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments: InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let frame = r#"{"channel":"bbo","data":{"coin":"BTC","time":1,
            "bbo":[{"px":"1.0","sz":"1.0"},{"px":"2.0","sz":"1.0"}]}}"#;
        handle_text(frame, &arbiter, &instruments);
        assert!(rx.try_recv().is_err(), "no quote without an instrument def");
    }

    /// Non-finite px/sz (`NaN`/`inf`, incl. overflow like `1e400`) and negatives are rejected, so a
    /// malformed level never reaches the wire as JSON `null` (and never defeats content dedup).
    #[test]
    fn non_finite_or_negative_px_sz_rejected() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("BTC");
        for frame in [
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"nan","sz":"1.0"},{"px":"2.0","sz":"1.0"}]}}"#,
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1e400","sz":"1.0"},{"px":"2.0","sz":"1.0"}]}}"#,
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"-1.0","sz":"1.0"},{"px":"2.0","sz":"1.0"}]}}"#,
        ] {
            handle_text(frame, &arbiter, &instruments);
            assert!(
                rx.try_recv().is_err(),
                "non-finite/negative level must not emit: {frame}"
            );
        }
        assert!(parse_decimal("nan").is_none());
        assert!(parse_decimal("inf").is_none());
        assert!(parse_decimal("-0.5").is_none());
        assert_eq!(parse_decimal("104783.0"), Some(104783.0));
    }

    /// A `time` whose ms→ns multiply overflows `u64` is dropped — it must not saturate to `u64::MAX`
    /// and permanently latch the floor's high-water for that symbol.
    #[test]
    fn overflowing_block_time_rejected() {
        assert_eq!(
            block_time_ms_to_ns(1_700_000_000_000),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(block_time_ms_to_ns(u64::MAX), None);
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("BTC");
        let frame = format!(
            r#"{{"channel":"bbo","data":{{"coin":"BTC","time":{},"bbo":[{{"px":"1.0","sz":"1.0"}},{{"px":"2.0","sz":"1.0"}}]}}}}"#,
            u64::MAX
        );
        handle_text(&frame, &arbiter, &instruments);
        assert!(
            rx.try_recv().is_err(),
            "overflowing block time must not emit"
        );
    }

    /// A one-sided book (a null side) cannot form a two-sided quote and is skipped.
    #[test]
    fn one_sided_bbo_is_skipped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("BTC");
        let frame = r#"{"channel":"bbo","data":{"coin":"BTC","time":1,
            "bbo":[null,{"px":"2.0","sz":"1.0"}]}}"#;
        handle_text(frame, &arbiter, &instruments);
        assert!(rx.try_recv().is_err(), "one-sided book must not emit");
    }

    /// A `trades` frame decodes to a trade with the venue tid as trade_id and the side mapped.
    #[test]
    fn trades_frame_emits_trade() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("ETH");
        let frame = r#"{"channel":"trades","data":[
            {"coin":"ETH","side":"B","px":"2500.5","sz":"0.3","time":1700000000000,"tid":42}]}"#;
        handle_text(frame, &arbiter, &instruments);
        match rx.try_recv().expect("a trade was emitted") {
            FeedMessage::Trade(t) => {
                assert_eq!(t.symbol, "ETH");
                assert_eq!(t.price, 2500.5);
                assert_eq!(t.size, 0.3);
                assert_eq!(t.aggressor_side, "buy");
                assert_eq!(t.trade_id, 42);
            }
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    /// Non-emitting channels and garbage frames are ignored without panicking.
    #[test]
    fn unknown_and_garbage_frames_ignored() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("BTC");
        handle_text(
            r#"{"channel":"subscriptionResponse","data":{}}"#,
            &arbiter,
            &instruments,
        );
        handle_text(r#"{"channel":"pong"}"#, &arbiter, &instruments);
        handle_text("not json at all", &arbiter, &instruments);
        assert!(
            rx.try_recv().is_err(),
            "no business message from control/garbage frames"
        );
    }
}
