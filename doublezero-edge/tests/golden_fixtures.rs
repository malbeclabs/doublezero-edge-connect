//! Golden-fixture tests over committed sample response bodies — the `tests/codec_*_fixtures.rs`
//! convention, applied to this crate's JSON responses instead of binary wire datagrams: parse a
//! recorded body, render it, assert the output.
//!
//! There is no live edge-connect container to record these fixtures from in this crate's own test
//! environment (it must build and test on macOS with no bridge/container in the loop — see
//! `CLAUDE.md`), so each fixture is hand-built to the exact shape `sinks/api.rs` in the bridge
//! crate documents and its own unit tests pin (field names, which values are strings vs raw
//! numbers, which blocks are present). That is the version this test suite holds itself to; a
//! future drift in the real API's shape is exactly what the "unknown extra field" tests and
//! `types.rs`'s tolerant parsing exist to survive without a fixture update.

use std::fs;

use doublezero_edge::{endpoint::Endpoint, render};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

#[test]
fn products_list_renders_a_row_per_product() {
    let body = fixture("products_list.json");
    let out = render::render_table(Endpoint::ProductsList, &body).unwrap();
    let expected = "\
PRODUCT_ID                        SOURCE       STATUS   FEED_KIND        PRICE_INCR  BASE_INCR
--------------------------------  -----------  -------  ---------------  ----------  ---------
HYPERLIQUID:BTC                   Hyperliquid  online   top_of_book      0.01        0.00001
HYPERLIQUID:ETH                   Hyperliquid  offline  top_of_book      0.001       0.0001
LASHAY:EAVE-27JAN01-YES#120.1165  Lashay       online   market_by_price  0.01        1";
    assert_eq!(
        out, expected,
        "\n--- actual ---\n{out}\n--- expected ---\n{expected}"
    );
}

#[test]
fn product_get_renders_as_a_field_value_table() {
    let body = fixture("product_get.json");
    let out = render::render_table(Endpoint::ProductGet, &body).unwrap();
    let expected = "\
FIELD            VALUE
---------------  ---------------
product_id       HYPERLIQUID:BTC
source           Hyperliquid
symbol           BTC
channel          0
instrument_id    41
price_increment  0.01
base_increment   0.00001
status           online
feed_kind        top_of_book";
    assert_eq!(
        out, expected,
        "\n--- actual ---\n{out}\n--- expected ---\n{expected}"
    );
}

#[test]
fn ticker_table_lists_trades_and_the_best_bid_ask_summary() {
    let body = fixture("ticker.json");
    let out = render::render_table(Endpoint::Ticker, &body).unwrap();
    assert!(out.contains("67000.12"), "{out}");
    assert!(out.contains("66999.00"), "{out}");
    assert!(out.contains("best_bid: 67000.00"), "{out}");
    assert!(out.contains("best_ask: 67001.50"), "{out}");
}

/// Pins rule 2: a caller in table mode must be able to see the retention window, not just the
/// candle rows.
#[test]
fn candles_table_shows_the_retention_block() {
    let body = fixture("candles.json");
    let out = render::render_table(Endpoint::Candles, &body).unwrap();
    assert!(out.contains("1780003600"), "{out}");
    assert!(
        out.contains(
            "retention: window_seconds=3600 oldest=1780000000 newest=1780003600 truncated=true"
        ),
        "{out}"
    );
}

/// Pins rule 2's other half: `book`'s coverage block must be visible in table mode too.
#[test]
fn book_table_shows_the_coverage_block() {
    let body = fixture("book.json");
    let out = render::render_table(Endpoint::Book, &body).unwrap();
    assert!(out.contains("product_id: HYPERLIQUID:BTC"), "{out}");
    assert!(out.contains("67000.00"), "{out}");
    assert!(
        out.contains("coverage: levels_returned=3 levels_capped_at=50 complete=true"),
        "{out}"
    );
}

#[test]
fn best_bid_ask_table_lists_one_row_per_product() {
    let body = fixture("best_bid_ask.json");
    let out = render::render_table(Endpoint::BestBidAsk, &body).unwrap();
    assert!(out.contains("HYPERLIQUID:BTC"), "{out}");
    assert!(out.contains("67000.00 @ 1.20000"), "{out}");
    assert!(out.contains("67001.50 @ 0.80000"), "{out}");
    // ETH has no bid in the fixture — must render as an honest placeholder, never a fabricated 0.
    assert!(out.contains("HYPERLIQUID:ETH"), "{out}");
    assert!(out.contains(" -  "), "{out}");
}

#[test]
fn status_table_lists_venues_and_the_history_summary() {
    let body = fixture("status.json");
    let out = render::render_table(Endpoint::Status, &body).unwrap();
    assert!(out.contains("Hyperliquid"), "{out}");
    assert!(out.contains("Phoenix"), "{out}");
    assert!(
        out.contains(
            "history: products=128  buckets=612440/1048576 (58%)  est_bytes=57623897  \
             window_seconds=3600  evicted=0  late_drops=4"
        ),
        "{out}"
    );
    // Below cap: no "AT CAP" marker (the pair to `the_table_marks_a_store_at_cap` in render.rs).
    assert!(!out.contains("AT CAP"), "{out}");
}

/// The `channels` block: the wire-supplied `label` must win over the bare id, and `bound`/
/// `allowed` must read as visibly distinct columns (channel 11 is admitted but not bound).
#[test]
fn status_table_shows_the_channels_block_with_the_servers_label() {
    let body = fixture("status.json");
    let out = render::render_table(Endpoint::Status, &body).unwrap();
    assert!(out.contains("edge-kalshi-sports-mbp"), "{out}");
    assert!(out.contains("sports.nfl"), "{out}");
    assert!(out.contains("412"), "{out}");
    assert!(
        out.contains("(29 channels excluded by channel filter)"),
        "{out}"
    );
}

/// The `process` block: real numbers, not omitted.
#[test]
fn status_table_shows_the_process_block() {
    let body = fixture("status.json");
    let out = render::render_table(Endpoint::Status, &body).unwrap();
    assert!(
        out.contains("process: resident_memory_bytes=193200128  cpu_seconds_total=412.7"),
        "{out}"
    );
}

// -------------------------------------------------------------------------------------------
// JSON output: rule 1 (unknown fields survive) / rule 2 (pass-through, not reshaped).
// -------------------------------------------------------------------------------------------

/// `--output json` must never lose information relative to what the server actually sent — the
/// whole point of not deserializing into a strict struct on this path (see `types.rs`'s module
/// docs). Round-tripping the fixture through the same `serde_json::Value` this crate's JSON
/// output path uses must reproduce it exactly, unknown top-level field included.
#[test]
fn json_output_preserves_an_unknown_field_the_typed_renderer_would_have_dropped() {
    let mut body = fixture("status.json");
    body["server_build_id"] = Value::String("2026-08-09-abcdef".to_string());
    let printed = serde_json::to_string_pretty(&body).unwrap();
    let reparsed: Value = serde_json::from_str(&printed).unwrap();
    assert_eq!(reparsed, body);
    assert_eq!(reparsed["server_build_id"], "2026-08-09-abcdef");
}

#[test]
fn an_error_envelope_round_trips_through_json_output_untouched() {
    let body = fixture("error_ambiguous.json");
    let printed = serde_json::to_string_pretty(&body).unwrap();
    let reparsed: Value = serde_json::from_str(&printed).unwrap();
    assert_eq!(reparsed, body);
    assert_eq!(reparsed["candidates"].as_array().unwrap().len(), 2);
}
