//! Hyperliquid **public** WebSocket input — the first [`PublicVenue`] backstop, off by
//! default.
//!
//! It connects to Hyperliquid's own `wss://api.hyperliquid.xyz/ws`, subscribes `bbo` + `trades` per
//! configured coin, decodes the JSON into the same `FeedMessage`s the multicast pipeline produces,
//! and emits them through the **shared [`crate::ingest::arbiter`]** as [`Transport::PublicWs`]. The
//! reconnect/backoff transport and the validation helpers live in [`crate::ingest::public_input`];
//! this module owns only the Hyperliquid wire decode.
//!
//! **Precision before price.** Each public quote/trade is gated on its `(venue, symbol)` instrument
//! already being present in the shared [`InstrumentSnapshot`] (populated by the edge refdata feed).
//! The realistic backstop scenario is edge refdata healthy while mktdata stalls; a standalone public
//! feed with no edge refdata ever is a documented limitation (it would emit nothing).
//!
//! ⚠️ Decimal-string px/sz are parsed straight to real-unit `f64`s — the same unit space the edge
//! side produces via `apply_exponent` — so no canonical-exponent rescale is needed. Cross-transport
//! dedup is decided by publisher leadership per tick, never by content equality (see the arbiter).

use serde::Deserialize;
use tracing::warn;

use crate::{
    ingest::{
        arbiter::{lock, SharedArbiter, Transport},
        public_input::{self, instrument_known, parse_decimal, resolve_instrument, PublicVenue},
    },
    metrics::metrics,
    model::{
        category_arc, now_ns, venue_arc, FeedMessage, InstrumentSnapshot, NormalizedQuote,
        NormalizedTrade, Side,
    },
};

/// Hyperliquid's public WebSocket endpoint.
pub const DEFAULT_WS_INPUT_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// This backstop's registry Source ID (`ingest::sources`). The label is derived from it rather than
/// written out as a separate constant, so the edge path (which names a market from the wire Source
/// ID, see `processor.rs`) and this backstop cannot drift into naming one market two different
/// things — a split that would fork the arbiter's `(venue, symbol)` dedup floor and emit both
/// copies to the wire as duplicates under two names.
const HL_SOURCE_ID: u16 = 1;

/// The venue every public message is tagged with — always [`HL_SOURCE_ID`]'s registry name, so it
/// matches whatever the edge HL feed names the same market and both land in the same arbiter floor.
fn hl_venue() -> &'static str {
    crate::ingest::sources::source_label(HL_SOURCE_ID)
}

/// The instrument **universe** this backstop mirrors, in the same vocabulary as `Feed::category`,
/// handed to the arbiter on every emit.
///
/// **Inert on this venue today**, and the comment should not pretend otherwise: `category` is read
/// only by the `Sticky` tape gate, and this backstop serves a `Coordinated` venue, where the value
/// is passed and ignored. It becomes load-bearing the day this venue is declared `Sticky` — the gate
/// keys on `(venue, category)`, so a value disagreeing with the mirrored row's would make this
/// backstop its own universe rather than another path of the same one, and the gate would stop
/// collapsing the two copies of one fill. What survives that is whatever the `trade_id` window
/// cannot collapse on its own, i.e. a public copy stamped with a different id than the edge copy;
/// those would reach the wire twice. `category_names_the_row_this_backstop_mirrors` pins the value.
const HL_CATEGORY: &str = "perps";

/// Hyperliquid documents a cap of 1000 subscriptions per WebSocket connection. We fan out two
/// subscriptions (`bbo` + `trades`) per coin over a single connection and log if the configured coin
/// set would exceed the cap.
const HL_MAX_SUBSCRIPTIONS_PER_CONN: usize = 1000;

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
/// as `trade_id`, so the arbiter's windowed trade dedup collapses cross-transport copies on it.
#[derive(Deserialize)]
struct TradeData {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    tid: u64,
}

/// The Hyperliquid public-WS [`PublicVenue`]: connects to one URL and subscribes `bbo` + `trades`
/// per coin on a single connection.
struct HyperliquidVenue {
    url: String,
    coins: Vec<String>,
}

impl PublicVenue for HyperliquidVenue {
    fn venue(&self) -> &str {
        hl_venue()
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn subscribe_msgs(&self) -> Vec<String> {
        let mut subs = Vec::with_capacity(self.coins.len() * 2);
        for coin in &self.coins {
            for kind in ["bbo", "trades"] {
                subs.push(format!(
                    r#"{{"method":"subscribe","subscription":{{"type":"{kind}","coin":"{coin}"}}}}"#
                ));
            }
        }
        subs
    }

    fn handle_text(&self, txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
        handle_text(txt, arbiter, instruments)
    }
}

/// Run the Hyperliquid public WS input forever (reconnecting on any failure). Returns
/// immediately as a no-op when `coins` is empty (the input is off by default). Thin wrapper over the
/// venue-generic [`public_input::run`].
pub async fn run(
    url: String,
    coins: Vec<String>,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
) {
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
    public_input::run(HyperliquidVenue { url, coins }, arbiter, instruments).await
}

/// Decode one text frame and emit any resulting quote/trade. Unknown channels (e.g.
/// `subscriptionResponse`, `pong`) and malformed payloads are ignored — this is a best-effort feed.
fn handle_text(txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
    let env: Envelope = match serde_json::from_str(txt) {
        Ok(e) => e,
        Err(e) => {
            metrics()
                .ws_input_decode_errors
                .with_label_values(&[hl_venue()])
                .inc();
            tracing::debug!(error = %e, "public WS: undecodable frame ignored");
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
    if !instrument_known(instruments, hl_venue(), &d.coin) {
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
        venue: venue_arc(hl_venue()),
        source_name: venue_arc(hl_venue()),
        source_id: HL_SOURCE_ID,
        symbol: d.coin.into(),
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
        .ws_input_messages
        .with_label_values(&[hl_venue(), "quote"])
        .inc();
    lock(arbiter).emit(FeedMessage::Quote(quote), Transport::PublicWs, HL_CATEGORY);
}

/// Build a `NormalizedTrade` from a public `trades` element and emit it through the arbiter.
fn emit_trade(t: TradeData, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
    // Resolves precision AND the (channel, instrument_id) identity in one scan — see
    // `resolve_instrument`'s doc for why a bare symbol match is safe for this venue.
    let Some((channel, instrument_id)) = resolve_instrument(instruments, hl_venue(), &t.coin)
    else {
        return;
    };
    let (Some(price), Some(size)) = (parse_decimal(&t.px), parse_decimal(&t.sz)) else {
        return;
    };
    let Some(source_ts_ns) = block_time_ms_to_ns(t.time) else {
        return;
    };
    let trade = NormalizedTrade {
        venue: venue_arc(hl_venue()),
        source_name: venue_arc(hl_venue()),
        source_id: HL_SOURCE_ID,
        symbol: t.coin.into(),
        channel,
        instrument_id,
        category: category_arc(HL_CATEGORY),
        price,
        size,
        // HL trade side: "B" = aggressing buy, "A" = aggressing sell.
        aggressor_side: match t.side.as_str() {
            "B" => Side::Buy,
            "A" => Side::Sell,
            _ => Side::Unknown,
        },
        trade_id: t.tid,
        cumulative_volume: 0.0, // not carried on the public trades feed
        source_ts_ns,
        recv_ts_ns: now_ns(),
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    };
    metrics()
        .ws_input_messages
        .with_label_values(&[hl_venue(), "trade"])
        .inc();
    lock(arbiter).emit(FeedMessage::Trade(trade), Transport::PublicWs, HL_CATEGORY);
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tokio::sync::broadcast;

    use super::*;

    /// [`HL_CATEGORY`] must name the universe of the row this backstop actually mirrors — the
    /// venue's top-of-book row, the row that carries both the quotes and the prints it emits.
    ///
    /// Deliberately **not** "every row under this venue agrees": one venue carrying two disjoint
    /// universes is the case `Feed::category` exists to express, so that assertion would fail a
    /// legitimate registry change and leave deleting the test as the only fix. This one still fails
    /// if the constant drifts, because then no row matches it.
    #[test]
    fn category_names_the_row_this_backstop_mirrors() {
        let mirrored = crate::ingest::feeds::feeds().iter().find(|f| {
            f.venue == hl_venue()
                && f.category == HL_CATEGORY
                && f.kind == crate::ingest::feeds::FeedKind::TopOfBook
        });
        assert!(
            mirrored.is_some(),
            "no {} top-of-book row carries category {HL_CATEGORY:?}: this backstop claims to \
             mirror a universe the registry does not publish",
            hl_venue()
        );
    }
    use crate::{ingest::arbiter::Arbiter, model::NormalizedInstrument};

    fn instruments_with(symbol: &str) -> InstrumentSnapshot {
        let map = Arc::new(Mutex::new(HashMap::new()));
        map.lock().unwrap().insert(
            (hl_venue().into(), HL_CATEGORY.into(), 0u8, 1u32),
            NormalizedInstrument {
                tick_size: 0,
                venue: hl_venue().into(),
                source_name: hl_venue().into(),
                source_id: 0,
                symbol: symbol.into(),
                channel: 0,
                instrument_id: 1,
                category: HL_CATEGORY.into(),
                price_exponent: -2,
                qty_exponent: -2,
            },
        );
        map
    }

    fn arbiter_with_rx() -> (
        SharedArbiter,
        broadcast::Receiver<std::sync::Arc<FeedMessage>>,
    ) {
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
        match &*rx.try_recv().expect("a quote was emitted") {
            FeedMessage::Quote(q) => {
                assert_eq!(q.venue, "HYPERLIQUID".into());
                assert_eq!(q.symbol, "BTC".into());
                assert_eq!(q.bid, 104783.0);
                assert_eq!(q.ask, 104784.0);
                assert_eq!(q.bid_size, 1.5);
                assert_eq!(q.ask_size, 2.0);
                // ms × 1e6 == ns, matching the edge's canonical source_ts.
                assert_eq!(q.source_ts_ns, 1700000000000 * 1_000_000);
                // The registry Source ID, not the `0` "unknown" sentinel: a consumer joining this
                // backstop to the edge copy on `source_id` must see the same id both sides.
                assert_eq!(q.source_id, HL_SOURCE_ID);
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
        match &*rx.try_recv().expect("a trade was emitted") {
            FeedMessage::Trade(t) => {
                assert_eq!(t.symbol, "ETH".into());
                assert_eq!(t.price, 2500.5);
                assert_eq!(t.size, 0.3);
                assert_eq!(t.aggressor_side, crate::model::Side::Buy);
                assert_eq!(t.trade_id, 42);
                assert_eq!(t.source_id, HL_SOURCE_ID);
                // Resolved from the edge catalog (`instruments_with`'s (channel=0, instrument_id=1)
                // entry), not left at the zero default — this is the identity `history::Key` groups
                // trades on downstream.
                assert_eq!(t.channel, 0);
                assert_eq!(t.instrument_id, 1);
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

    /// The Hyperliquid venue builds two subscribe frames (bbo + trades) per coin.
    #[test]
    fn subscribe_msgs_bbo_and_trades_per_coin() {
        let v = HyperliquidVenue {
            url: DEFAULT_WS_INPUT_URL.to_string(),
            coins: vec!["BTC".to_string(), "ETH".to_string()],
        };
        let subs = v.subscribe_msgs();
        assert_eq!(subs.len(), 4);
        assert!(subs
            .iter()
            .any(|s| s.contains(r#""type":"bbo""#) && s.contains(r#""coin":"BTC""#)));
        assert!(subs
            .iter()
            .any(|s| s.contains(r#""type":"trades""#) && s.contains(r#""coin":"ETH""#)));
    }

    /// The backstop must name itself the way the edge does, or one market becomes two keys in the
    /// arbiter's `(venue, symbol)` dedup floor and both copies reach the wire.
    #[test]
    fn a_public_input_labels_itself_from_the_registry() {
        let hl = HyperliquidVenue {
            url: DEFAULT_WS_INPUT_URL.to_string(),
            coins: vec!["BTC".to_string()],
        };
        assert_eq!(
            hl.venue(),
            crate::ingest::sources::source_label(HL_SOURCE_ID)
        );
        assert_eq!(
            hl.venue(),
            "HYPERLIQUID",
            "must match what the edge emits for this id"
        );
    }
}
