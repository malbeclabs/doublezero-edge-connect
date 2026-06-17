//! Semantic invariants over collected WS messages. These hold regardless of how many
//! publishers fed the input, so they double as the deduplication oracle.

use std::collections::HashSet;

use serde_json::Value;

fn s<'a>(m: &'a Value, k: &str) -> &'a str {
    m.get(k).and_then(|v| v.as_str()).unwrap_or_default()
}
fn f(m: &Value, k: &str) -> f64 {
    m.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0)
}
fn u(m: &Value, k: &str) -> u64 {
    m.get(k).and_then(|v| v.as_u64()).unwrap_or(0)
}
fn ty(m: &Value) -> &str {
    s(m, "type")
}

/// Every quote/trade/depth references a symbol that was already declared by an
/// `instrument` message earlier in the stream (precision before price). The bridge
/// gates all price-carrying emissions on a known instrument definition, so trades
/// must also be preceded by their instrument.
pub fn instrument_before_price(msgs: &[Value]) {
    let mut known: HashSet<(String, String)> = HashSet::new();
    for m in msgs {
        let key = (s(m, "venue").to_string(), s(m, "symbol").to_string());
        match ty(m) {
            "instrument" => {
                known.insert(key);
            }
            "quote" | "trade" | "depth" => {
                assert!(
                    known.contains(&key),
                    "{} for {:?} arrived before its instrument definition",
                    ty(m),
                    key
                );
            }
            _ => {}
        }
    }
}

/// No two messages carry identical business content.
///
/// **Key design — business-content only, BY DESIGN.** Per-receipt timestamps
/// (`recv_ts_ns`, `kernel_rx_ts_ns`, `ws_send_ts_ns`) are deliberately excluded.
/// Duplicate frames from competing publishers carry identical business fields but
/// different receipt timestamps; a business-content key collapses them so this
/// assertion catches missing dedup. Do NOT add `recv_ts_ns` to any key arm — that
/// gives each copy a distinct key and defeats the oracle.
///
/// **Known limitation — `source_ts_ns == 0` collisions.** Two genuinely distinct
/// quotes that both have `source_ts_ns == 0` (the "unknown" sentinel) and identical
/// bid/ask/sizes share the same key and produce a false-positive duplicate failure.
/// This does not occur in the current single-publisher fixtures. Future multi-publisher
/// dedup work must define duplicate identity precisely (e.g. by frame channel + sequence
/// number at ingest) rather than relying on this heuristic.
pub fn no_business_duplicates(msgs: &[Value]) {
    let mut seen: HashSet<String> = HashSet::new();
    for m in msgs {
        let key = match ty(m) {
            "quote" => format!(
                "q|{}|{}|{}|{}|{}|{}|{}",
                s(m, "venue"),
                s(m, "symbol"),
                u(m, "source_ts_ns"),
                f(m, "bid"),
                f(m, "ask"),
                f(m, "bid_size"),
                f(m, "ask_size")
            ),
            "trade" => format!(
                "t|{}|{}|{}",
                s(m, "venue"),
                s(m, "symbol"),
                u(m, "trade_id")
            ),
            "depth" => format!(
                "d|{}|{}|{}",
                s(m, "venue"),
                s(m, "symbol"),
                u(m, "source_ts_ns")
            ),
            _ => continue,
        };
        assert!(
            seen.insert(key.clone()),
            "duplicate business message: {key}"
        );
    }
}

/// Quotes are human-scaled and carry valid prices/sizes.
pub fn quotes_well_formed(msgs: &[Value]) {
    for q in msgs.iter().filter(|m| ty(m) == "quote") {
        assert!(f(q, "bid") > 0.0, "non-positive bid: {q}");
        assert!(f(q, "ask") > 0.0, "non-positive ask: {q}");
        assert!(
            f(q, "bid_size") >= 0.0 && f(q, "ask_size") >= 0.0,
            "negative size: {q}"
        );
        // source_ts_ns may be 0 ("unknown" sentinel, per model.rs/PROTOCOL.md) — do not require > 0
    }
}

/// Trades have a valid aggressor side and positive price/size.
pub fn trades_well_formed(msgs: &[Value]) {
    for t in msgs.iter().filter(|m| ty(m) == "trade") {
        let side = s(t, "aggressor_side");
        assert!(
            matches!(side, "buy" | "sell" | "unknown"),
            "bad aggressor_side {side:?}: {t}"
        );
        assert!(
            f(t, "price") > 0.0 && f(t, "size") > 0.0,
            "non-positive trade: {t}"
        );
    }
}
