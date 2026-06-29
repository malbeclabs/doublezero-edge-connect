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
//! ⚠️ The public trades schema (field names/units, the `-PERP` strip rule, whether
//! `tradeSequenceNumber` equals the edge on-chain `trade_sequence_number`) is taken from
//! docs.phoenix.trade (closed beta) and is NOT yet reference-validated against a live socket. No
//! `FEEDS` row depends on this feeder; confirm the schema before production enablement.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::{
    ingest::{
        arbiter::{lock, Publisher, SharedArbiter},
        public_feeder::{self, finite_non_negative, instrument_known, PublicVenue},
    },
    metrics::metrics,
    model::{now_ns, FeedMessage, InstrumentSnapshot, NormalizedTrade},
};

/// Phoenix's public WebSocket endpoint.
pub const DEFAULT_PHOENIX_WS_URL: &str = "wss://perp-api.phoenix.trade/v1/ws";

/// The venue every emitted trade is tagged with (must match the edge Phoenix feed's venue so both
/// land in the same arbiter dedup window).
const PHOENIX_VENUE: &str = "Phoenix";

/// One Phoenix `trades` frame: the channel tag, the public market symbol, and the fills. Only the
/// `trades` channel is acted on; confirmations/other channels parse with `trades` absent and are
/// ignored.
#[derive(Deserialize)]
struct TradesFrame {
    channel: String,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    trades: Option<Vec<Fill>>,
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

/// The Phoenix public-WS [`PublicVenue`]: subscribes the `trades` channel per market. `symbol_map`
/// maps the public base symbol (the wire `symbol`, e.g. `SOL`) → the EDGE symbol (e.g. `SOL-PERP`),
/// so an emitted trade carries the same `(venue, symbol)` identity as the edge copy.
struct PhoenixVenue {
    url: String,
    symbol_map: BTreeMap<String, String>,
}

impl PhoenixVenue {
    /// Build a venue from the EDGE symbols to back (e.g. `["SOL-PERP"]`). The public key for each is
    /// the edge symbol with a `-PERP` suffix stripped (`SOL-PERP` → `SOL`).
    fn new(url: String, markets: Vec<String>) -> Self {
        let symbol_map = markets
            .into_iter()
            .map(|edge| (public_symbol(&edge).to_string(), edge))
            .collect();
        Self { url, symbol_map }
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
        if frame.channel != "trades" {
            return; // subscription confirmations / other channels — nothing to emit
        }
        let (Some(public_symbol), Some(fills)) = (frame.symbol, frame.trades) else {
            return; // a trades frame missing its symbol or fills carries nothing actionable
        };
        for fill in &fills {
            self.emit_trade(&public_symbol, fill, arbiter, instruments);
        }
    }

    /// Map one public fill onto a `NormalizedTrade` (venue `Phoenix`, the edge symbol) and emit it
    /// through the arbiter. Every malformed/unmappable field path drops the fill (best-effort feed).
    fn emit_trade(
        &self,
        public_symbol: &str,
        fill: &Fill,
        arbiter: &SharedArbiter,
        instruments: &InstrumentSnapshot,
    ) {
        let Some(edge_symbol) = self.symbol_map.get(public_symbol) else {
            return; // unsubscribed/unmappable market
        };
        if !instrument_known(instruments, PHOENIX_VENUE, edge_symbol) {
            return; // precision unknown; drop until the edge refdata defines this instrument
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
        let price = quote / base;
        if !price.is_finite() {
            return;
        }
        // Unix seconds → ns. trade dedup keys on trade_id, so a bad/overflowing timestamp falls back
        // to the 0 "not available" sentinel rather than dropping the trade.
        let source_ts_ns = fill
            .timestamp
            .parse::<u64>()
            .ok()
            .and_then(|s| s.checked_mul(1_000_000_000))
            .unwrap_or(0);
        let trade = NormalizedTrade {
            venue: PHOENIX_VENUE.to_string(),
            symbol: edge_symbol.clone(),
            price,
            size: base,
            // Phoenix trade side: "bid" = aggressing buy, "ask" = aggressing sell.
            aggressor_side: match fill.side.as_str() {
                "bid" => "buy",
                "ask" => "sell",
                _ => "unknown",
            }
            .to_string(),
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
        self.symbol_map
            .keys()
            .map(|public| {
                format!(
                    r#"{{"type":"subscribe","subscription":{{"channel":"trades","symbol":"{public}"}}}}"#
                )
            })
            .collect()
    }

    fn handle_text(&self, txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot) {
        PhoenixVenue::handle_text(self, txt, arbiter, instruments)
    }
}

/// The public base symbol for an EDGE symbol: strip a trailing `-PERP` (`SOL-PERP` → `SOL`), else
/// pass through unchanged.
fn public_symbol(edge: &str) -> &str {
    edge.strip_suffix("-PERP").unwrap_or(edge)
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
            (PHOENIX_VENUE.to_string(), symbol.to_string()),
            NormalizedInstrument {
                venue: PHOENIX_VENUE.to_string(),
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

    /// A well-formed trades frame maps onto a Phoenix trade with the edge symbol, derived VWAP price,
    /// base size, mapped side, and seconds→ns source_ts.
    #[test]
    fn trades_frame_emits_trade() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL-PERP");
        let v = venue(&["SOL-PERP"]);
        v.handle_text(
            &trades_frame("SOL", "100", "bid", 10.0, 1500.0, "1775578550"),
            &arbiter,
            &instruments,
        );
        match rx.try_recv().expect("a trade was emitted") {
            FeedMessage::Trade(t) => {
                assert_eq!(t.venue, "Phoenix");
                assert_eq!(t.symbol, "SOL-PERP");
                assert_eq!(t.trade_id, 100);
                assert_eq!(t.price, 150.0);
                assert_eq!(t.size, 10.0);
                assert_eq!(t.aggressor_side, "buy");
                assert_eq!(t.source_ts_ns, 1_775_578_550_000_000_000);
            }
            other => panic!("expected a trade, got {other:?}"),
        }
    }

    /// A fill for an unconfigured public market is dropped (no mapping to an edge symbol).
    #[test]
    fn unmapped_symbol_dropped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments = instruments_with("SOL-PERP");
        let v = venue(&["SOL-PERP"]);
        v.handle_text(
            &trades_frame("BTC", "1", "bid", 1.0, 100.0, "1775578550"),
            &arbiter,
            &instruments,
        );
        assert!(rx.try_recv().is_err(), "unmapped market must not emit");
    }

    /// Precision-before-price: a mapped market with no instrument def yet is dropped.
    #[test]
    fn trade_without_instrument_dropped() {
        let (arbiter, mut rx) = arbiter_with_rx();
        let instruments: InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let v = venue(&["SOL-PERP"]);
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
        let instruments = instruments_with("SOL-PERP");
        let v = venue(&["SOL-PERP"]);
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
        let instruments = instruments_with("SOL-PERP");
        let v = venue(&["SOL-PERP"]);
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

    /// Subscribe frames strip the `-PERP` suffix and carry the public base per configured market.
    #[test]
    fn subscribe_msgs_strip_perp() {
        let v = venue(&["SOL-PERP", "BTC-PERP"]);
        let subs = v.subscribe_msgs();
        assert_eq!(subs.len(), 2);
        assert!(subs.iter().all(|s| s.contains(r#""channel":"trades""#)));
        assert!(subs.iter().any(|s| s.contains(r#""symbol":"SOL""#)));
        assert!(subs.iter().any(|s| s.contains(r#""symbol":"BTC""#)));
        assert!(
            !subs.iter().any(|s| s.contains("-PERP")),
            "subscribe frames must carry public base symbols, not edge -PERP symbols"
        );
    }
}
