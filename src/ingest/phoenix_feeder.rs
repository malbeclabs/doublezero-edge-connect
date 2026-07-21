//! Phoenix **public-API** WebSocket trade feeder — a [`PublicVenue`] backstop for the edge Phoenix
//! multicast TRADE stream, off by default.
//!
//! It connects to Phoenix's public `wss://perp-api.phoenix.trade/v1/ws`, subscribes the `trades`
//! channel per configured market, decodes each fill into a `NormalizedTrade`, and emits it through
//! the **shared [`crate::ingest::arbiter`]** as [`Publisher::PublicWs`]. The arbiter dedups trades
//! per `(venue, symbol)` on `trade_id` (= the public `tradeSequenceNumber`), so a public trade races
//! the edge copy and only fills in when the edge gaps — no arbiter change needed.
//!
//! **Trades only.** We deliberately do NOT emit quotes: the edge Quote is a spline-blended BBO, while
//! Phoenix's public orderbook channel is resting-only — a different quantity. A BBO backstop is
//! deferred until a comparable public blended source is identified.
//!
//! Validated against a concurrent edge+public capture (2026-06-30, `edge-pcaps/phoenix-capture-20260630`),
//! on the 257 fills shared by both feeds: the public `tradeSequenceNumber` equals the edge on-chain
//! `trade_id` **exactly** (zero mismatches); Phoenix uses the **same bare ticker on both feeds** (edge
//! `instrument_id == public assetId`; no namespace prefix, no `-PERP` suffix); `side` maps
//! `bid -> buy` / `ask -> sell`; and the fill's `baseAmount` (size) and `quoteAmount / baseAmount`
//! (price) equal the edge's size and trade price (every shared fill had `numFills == 1`). So a public
//! fill tagged `(venue="Phoenix", symbol, trade_id)` lines up 1:1 with the edge copy and dedups. No
//! `FEEDS` row depends on this feeder; it stays off until explicitly enabled with
//! `--phoenix-ws-input-markets`.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::{
    ingest::{
        arbiter::{lock, Publisher, SharedArbiter},
        public_feeder::{self, finite_non_negative, instrument_known, PublicVenue},
    },
    metrics::metrics,
    model::{now_ns, venue_arc, FeedMessage, InstrumentSnapshot, NormalizedTrade, Side},
};

/// Phoenix's public WebSocket endpoint.
pub const DEFAULT_PHOENIX_WS_URL: &str = "wss://perp-api.phoenix.trade/v1/ws";

/// The venue every emitted trade is tagged with (must match the edge Phoenix feed's venue so both
/// land in the same arbiter dedup window).
const PHOENIX_VENUE: &str = "Phoenix";

/// One Phoenix `trades` frame: the channel tag, the public market symbol, and the fills. Only the
/// `trades` channel is acted on; every other frame (subscription status, heartbeat/pong, or one with
/// no `channel` key at all) is ignored without counting as a decode error — so
/// `dz_ws_feeder_decode_errors_total{venue=Phoenix}` stays a real health signal on a chatty control
/// channel. Fills are decoded **per element** (as raw JSON here, then one-by-one in `handle_text`) so
/// a single malformed fill can't fail the whole batch — the backstop matters most during bursts, when
/// `trades` arrays are largest.
#[derive(Deserialize)]
struct TradesFrame {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    trades: Option<Vec<serde_json::Value>>,
}

/// One fill in a Phoenix `trades` frame. Fields beyond these (slot, slotIndex, taker, lots, fees,
/// numFills) are present on the wire but unused here.
#[derive(Deserialize)]
struct Fill {
    #[serde(rename = "tradeSequenceNumber")]
    trade_sequence_number: String,
    side: String,
    #[serde(rename = "baseAmount")]
    base_amount: f64,
    #[serde(rename = "quoteAmount")]
    quote_amount: f64,
    /// Unix **seconds**, as a string.
    timestamp: String,
}

/// The Phoenix public-WS [`PublicVenue`]: subscribes the `trades` channel per market. Phoenix carries
/// the **same bare ticker on the edge and public feeds** (edge `instrument_id == public assetId`), so
/// the wire `symbol` is already the edge symbol — there is no mapping, only the set of markets to back.
struct PhoenixVenue {
    url: String,
    symbols: BTreeSet<String>,
}

impl PhoenixVenue {
    /// Build a venue from the market symbols to back (bare tickers, e.g. `["SOL", "BTC"]`). The same
    /// symbol subscribes the public feed and tags the emitted trade, because the edge and public
    /// feeds share it verbatim.
    fn new(url: String, markets: Vec<String>) -> Self {
        Self {
            url,
            symbols: markets.into_iter().collect(),
        }
    }

    /// Decode one Phoenix `trades` frame and emit each fill as a `NormalizedTrade`.
    fn handle_text(&self, txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
        let frame: TradesFrame = match serde_json::from_str(txt) {
            Ok(f) => f,
            Err(e) => {
                self.decode_error();
                tracing::debug!(error = %e, "Phoenix public WS: undecodable frame ignored");
                return;
            }
        };
        if frame.channel.as_deref() != Some("trades") {
            return; // subscription status / heartbeats / other (or no) channels — nothing to emit
        }
        let (Some(symbol), Some(fills)) = (frame.symbol, frame.trades) else {
            return; // a trades frame missing its symbol or fills carries nothing actionable
        };
        // Decode each fill on its own: one malformed element (a missing field, a null, a new shape)
        // is skipped with a single decode-error bump, so the rest of the batch still emits.
        for fill in fills {
            match serde_json::from_value::<Fill>(fill) {
                Ok(fill) => self.emit_trade(&symbol, &fill, arbiter, instruments),
                Err(e) => {
                    self.decode_error();
                    tracing::debug!(error = %e, "Phoenix public WS: undecodable fill skipped");
                }
            }
        }
    }

    /// Map one public fill onto a `NormalizedTrade` (venue `Phoenix`, the shared bare symbol) and emit
    /// it through the arbiter. Every malformed/unknown field path drops the fill (best-effort feed).
    fn emit_trade(
        &self,
        symbol: &str,
        fill: &Fill,
        arbiter: &SharedArbiter,
        instruments: &InstrumentSnapshot,
    ) {
        if !self.symbols.contains(symbol) {
            return; // a market we didn't subscribe to; ignore
        }
        // Precision-before-price, and the load-bearing safety net for the edge==public symbol
        // assumption: we only emit under a `(Phoenix, symbol)` the edge refdata has actually defined.
        // If Phoenix ever renamed a market on one side so the two feeds diverged, a public fill whose
        // symbol the edge doesn't define is DROPPED here (the backstop silently stops for it) rather
        // than emitted under a mismatched key that would double-print alongside the edge copy. The
        // resolved market set is logged at startup (see `run`) so a divergence is at least visible.
        if !instrument_known(instruments, PHOENIX_VENUE, symbol) {
            return; // precision unknown / symbol not defined by the edge; drop
        }
        let Ok(trade_id) = fill.trade_sequence_number.parse::<u64>() else {
            self.decode_error();
            return;
        };
        let (Some(base), Some(quote)) = (
            finite_non_negative(fill.base_amount),
            finite_non_negative(fill.quote_amount),
        ) else {
            return;
        };
        if base == 0.0 {
            return; // no VWAP divide-by-zero
        }
        // Fill price = quoteAmount / baseAmount, verified equal to the edge `trade_price` on all 257
        // shared fills in the capture (each a single fill, `numFills == 1`). If Phoenix ever emitted a
        // `trades` element aggregating multiple resting orders (`numFills > 1`), this would be that
        // element's VWAP; dedup keys on `trade_id` only, so it can't mis-dedup — a backstop fill-in
        // would simply carry the element VWAP as its price.
        let price = quote / base;
        if !price.is_finite() {
            return;
        }
        // trade dedup keys on trade_id, so an unparseable timestamp falls back to the 0 "not
        // available" sentinel rather than dropping the trade.
        let source_ts_ns = unix_seconds_to_ns(&fill.timestamp);
        let trade = NormalizedTrade {
            venue: venue_arc(PHOENIX_VENUE),
            symbol: symbol.into(),
            price,
            // `baseAmount` is the fill's base-asset quantity in the *same real units* the edge emits
            // (edge `trade_qty_raw * 10^qty_exponent`) — verified equal on all 257 shared fills in the
            // 2026-06-30 capture, so a backstop fill carries the same magnitude as its edge copy.
            size: base,
            // Phoenix trade side: "bid" = aggressing buy, "ask" = aggressing sell (capture-confirmed).
            aggressor_side: match fill.side.as_str() {
                "bid" => Side::Buy,
                "ask" => Side::Sell,
                _ => Side::Unknown,
            },
            trade_id,
            cumulative_volume: 0.0, // not carried on the public trades feed
            source_ts_ns,
            recv_ts_ns: now_ns(),
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        };
        metrics()
            .ws_feeder_messages
            .with_label_values(&[PHOENIX_VENUE, "trade"])
            .inc();
        lock(arbiter).emit(FeedMessage::Trade(trade), Publisher::PublicWs);
    }

    fn decode_error(&self) {
        metrics()
            .ws_feeder_decode_errors
            .with_label_values(&[PHOENIX_VENUE])
            .inc();
    }
}

impl PublicVenue for PhoenixVenue {
    fn venue(&self) -> &str {
        PHOENIX_VENUE
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn subscribe_msgs(&self) -> Vec<String> {
        self.symbols
            .iter()
            .map(|symbol| {
                format!(
                    r#"{{"type":"subscribe","subscription":{{"channel":"trades","symbol":"{symbol}"}}}}"#
                )
            })
            .collect()
    }

    fn handle_text(&self, txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
        PhoenixVenue::handle_text(self, txt, arbiter, instruments)
    }
}

/// Parse a Phoenix public `timestamp` (Unix **seconds**) into nanoseconds. The capture shows integral
/// seconds (e.g. `"1782831977"`); this tolerates a fractional suffix (`"1782831977.5"`) by taking the
/// whole-seconds part rather than failing — so a fractional value degrades to second precision instead
/// of silently becoming the `0` "not available" sentinel for every affected fill. Returns `0` only
/// when the whole-seconds part is unparseable or overflows ns.
fn unix_seconds_to_ns(ts: &str) -> u64 {
    let secs = ts.split_once('.').map_or(ts, |(whole, _frac)| whole);
    secs.parse::<u64>()
        .ok()
        .and_then(|s| s.checked_mul(1_000_000_000))
        .unwrap_or(0)
}

/// Run the Phoenix public-API trade feeder forever (reconnecting on any failure). Returns
/// immediately as a no-op when `markets` is empty (the feeder is off by default). Thin wrapper over
/// the venue-generic [`public_feeder::run`].
pub async fn run(
    url: String,
    markets: Vec<String>,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
) {
    if !markets.is_empty() {
        // Surface the resolved market set: each symbol must match the edge Phoenix symbol verbatim
        // (edge `instrument_id == public assetId`) or its public fills are dropped at the precision
        // gate (no dedup, no backstop) — logging it makes an edge/public symbol divergence visible.
        tracing::info!(
            venue = PHOENIX_VENUE,
            markets = ?markets,
            "Phoenix public trade feeder backing markets (must match the edge Phoenix symbols verbatim)"
        );
    }
    public_feeder::run(PhoenixVenue::new(url, markets), arbiter, instruments).await
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
            (PHOENIX_VENUE.into(), symbol.into()),
            NormalizedInstrument {
                venue: PHOENIX_VENUE.into(),
                symbol: symbol.into(),
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

    fn venue(markets: &[&str]) -> PhoenixVenue {
        PhoenixVenue::new(
            DEFAULT_PHOENIX_WS_URL.to_string(),
            markets.iter().map(|s| s.to_string()).collect(),
        )
    }

    fn trades_frame(
        symbol: &str,
        seq: &str,
        side: &str,
        base: f64,
        quote: f64,
        ts: &str,
    ) -> String {
        format!(
            r#"{{"channel":"trades","symbol":"{symbol}","trades":[{{"tradeSequenceNumber":"{seq}","side":"{side}","baseAmount":{base},"quoteAmount":{quote},"timestamp":"{ts}","slot":1,"taker":"x"}}]}}"#
        )
    }

    /// A well-formed trades frame maps onto a Phoenix trade with the shared bare symbol, derived VWAP
    /// price, base size, mapped side, and seconds→ns source_ts.
    #[test]
    fn trades_frame_emits_trade() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        v.handle_text(
            &trades_frame("SOL", "100", "bid", 10.0, 1500.0, "1775578550"),
            &arbiter,
            &instruments,
        );
        match &*rx.try_recv().expect("a trade was emitted") {
            FeedMessage::Trade(t) => {
                assert_eq!(t.venue, "Phoenix".into());
                assert_eq!(t.symbol, "SOL".into());
                assert_eq!(t.trade_id, 100);
                assert_eq!(t.price, 150.0);
                assert_eq!(t.size, 10.0);
                assert_eq!(t.aggressor_side, crate::model::Side::Buy);
                assert_eq!(t.source_ts_ns, 1_775_578_550_000_000_000);
            }
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    /// A fill for a market we didn't subscribe to is dropped.
    #[test]
    fn unsubscribed_symbol_dropped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        v.handle_text(
            &trades_frame("BTC", "1", "bid", 1.0, 100.0, "1775578550"),
            &arbiter,
            &instruments,
        );
        assert!(rx.try_recv().is_err(), "unsubscribed market must not emit");
    }

    /// Precision-before-price: a subscribed market with no instrument def yet is dropped.
    #[test]
    fn trade_without_instrument_dropped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments: InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let v = venue(&["SOL"]);
        v.handle_text(
            &trades_frame("SOL", "1", "bid", 1.0, 100.0, "1775578550"),
            &arbiter,
            &instruments,
        );
        assert!(rx.try_recv().is_err(), "no trade without an instrument def");
    }

    /// Non-numeric sequence, zero base, and negative base each emit nothing.
    #[test]
    fn malformed_fills_rejected() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        for frame in [
            trades_frame("SOL", "notanumber", "bid", 10.0, 1500.0, "1775578550"),
            trades_frame("SOL", "1", "bid", 0.0, 1500.0, "1775578550"),
            trades_frame("SOL", "2", "bid", -10.0, 1500.0, "1775578550"),
        ] {
            v.handle_text(&frame, &arbiter, &instruments);
            assert!(
                rx.try_recv().is_err(),
                "malformed fill must not emit: {frame}"
            );
        }
    }

    /// Orderbook-channel frames, subscription confirmations, and non-JSON are ignored without panic.
    #[test]
    fn non_trades_and_garbage_ignored() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        v.handle_text(
            r#"{"channel":"orderbook","symbol":"SOL","bids":[],"asks":[]}"#,
            &arbiter,
            &instruments,
        );
        v.handle_text(
            r#"{"channel":"subscriptionResponse","success":true}"#,
            &arbiter,
            &instruments,
        );
        v.handle_text("not json at all", &arbiter, &instruments);
        assert!(
            rx.try_recv().is_err(),
            "no trade from non-trades/garbage frames"
        );
    }

    /// Subscribe frames carry the configured bare symbols verbatim (no transform), one per market.
    #[test]
    fn subscribe_msgs_carry_configured_symbols() {
        let v = venue(&["SOL", "BTC"]);
        let subs = v.subscribe_msgs();
        assert_eq!(subs.len(), 2);
        assert!(subs.iter().all(|s| s.contains(r#""channel":"trades""#)));
        assert!(subs.iter().any(|s| s.contains(r#""symbol":"SOL""#)));
        assert!(subs.iter().any(|s| s.contains(r#""symbol":"BTC""#)));
    }

    /// A **verbatim** public `trades` frame captured from `perp-api.phoenix.trade` (2026-06-30,
    /// INTC seq 20274) decodes and emits — pinning the live wire types (`baseAmount`/`quoteAmount`
    /// as JSON numbers, `tradeSequenceNumber`/`timestamp`/`side` as strings) and the extra fields
    /// (`slot`, `slotIndex`, `taker`, `*LotsFilled`, `numFills`) the decoder must ignore. The emitted
    /// `size` is `baseAmount` verbatim, in the same units the edge emits (capture-verified).
    #[test]
    fn real_public_trades_frame_decodes_and_emits() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("INTC");
        let v = venue(&["INTC"]);
        let real = r#"{"channel":"trades","symbol":"INTC","trades":[{"slot":"429901814","slotIndex":1304,"timestamp":"1782831977","symbol":"INTC","taker":"CrhWhzxEohhjooVdd8G9ZQnyWg9Kpuo2fmqWHhtZvEq2","tradeSequenceNumber":"20274","side":"ask","baseLotsFilled":"24","quoteLotsFilled":"3326640","feeInQuoteLots":"0","baseAmount":0.024,"quoteAmount":3.32664,"numFills":1}]}"#;
        v.handle_text(real, &arbiter, &instruments);
        match &*rx.try_recv().expect("the real frame emitted a trade") {
            FeedMessage::Trade(t) => {
                assert_eq!(t.symbol, "INTC".into());
                assert_eq!(t.trade_id, 20274);
                assert_eq!(t.aggressor_side, crate::model::Side::Sell); // "ask" -> sell
                assert_eq!(t.size, 0.024); // baseAmount verbatim
                assert_eq!(t.price, 3.32664 / 0.024); // quoteAmount / baseAmount
                assert_eq!(t.source_ts_ns, 1_782_831_977_000_000_000);
            }
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    /// One malformed fill (a `baseAmount` sent as a string) must not sink the rest of the batch: the
    /// good fill alongside it still emits. Guards the burst path, where `trades` arrays are largest.
    #[test]
    fn one_malformed_fill_does_not_sink_the_batch() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        let frame = r#"{"channel":"trades","symbol":"SOL","trades":[{"tradeSequenceNumber":"199","side":"bid","baseAmount":"not-a-number","quoteAmount":1.0,"timestamp":"1"},{"tradeSequenceNumber":"200","side":"bid","baseAmount":10.0,"quoteAmount":1500.0,"timestamp":"1775578550"}]}"#;
        v.handle_text(frame, &arbiter, &instruments);
        match &*rx.try_recv().expect("the good fill still emitted") {
            FeedMessage::Trade(t) => assert_eq!(t.trade_id, 200),
            other => panic!("expected the good fill, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "only the good fill should emit");
    }

    /// A control frame with no `channel` key (e.g. a heartbeat/pong) deserializes fine and is ignored
    /// — it must not emit, and must not be treated as a decode error (channel is `Option`).
    #[test]
    fn control_frame_without_channel_is_ignored() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL");
        let v = venue(&["SOL"]);
        v.handle_text(r#"{"type":"pong"}"#, &arbiter, &instruments);
        v.handle_text(
            r#"{"channel":"subscriptionStatus","status":"subscribed"}"#,
            &arbiter,
            &instruments,
        );
        assert!(rx.try_recv().is_err(), "control frames must not emit");
    }

    /// `unix_seconds_to_ns` scales integral seconds, degrades a fractional value to whole-second
    /// precision (never a silent `0`), and returns the `0` sentinel only for an unparseable value.
    #[test]
    fn unix_seconds_to_ns_scales_and_degrades() {
        assert_eq!(unix_seconds_to_ns("1782831977"), 1_782_831_977_000_000_000);
        assert_eq!(
            unix_seconds_to_ns("1775578550.5"),
            1_775_578_550_000_000_000
        );
        assert_eq!(unix_seconds_to_ns(""), 0);
        assert_eq!(unix_seconds_to_ns("notanumber"), 0);
    }
}
