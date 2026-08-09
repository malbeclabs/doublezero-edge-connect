//! Phoenix backstop-arbitrage E2E: drive the real bridge with the `--feed Phoenix` multicast
//! replayer and the mock Phoenix **public** WS trade feeder, and assert the shared arbiter races
//! them correctly over the unchanged WS output contract.
//!
//! Two cases, both trades only (the Phoenix feeder backstops trades, not quotes):
//!   1. **edge gap → public fills in** — replay the full edge golden (SOL prints real quotes/trades,
//!      revealing its Source ID — see `ingest::processor`'s per-instrument deferral, which holds a
//!      `NormalizedInstrument`/price back until the edge has revealed it at least once; an edge that
//!      never once goes live for an instrument is not backstoppable, only a mid-stream gap is), THEN
//!      the edge goes quiet and a public Phoenix fill opens its `(venue, symbol, trade_id)` and
//!      reaches the wire, tagged `Phoenix`.
//!   2. **edge leads → public deduped** — replay a real edge Phoenix trade (`AMD`, `trade_id`
//!      20418), then push the public copy of that same fill (same id, distinctive price). The
//!      arbiter's windowed `trade_id` dedup drops the public copy, so `trade_id` 20418 reaches the
//!      wire exactly once and the distinctive public price never leaks.
//!
//! Both use the committed Phoenix golden (`phoenix_tob_refdata.bin` / `phoenix_tob_marketdata.bin`,
//! the clean source_id=2 slice of the 2026-06-30 capture; see `fixtures/PROVENANCE.md`). They are
//! the live-data half of verification item #1 (public `tradeSequenceNumber` == edge `trade_id`):
//! the codec golden pins the edge ids, and these prove the feeder keys dedup on the same identity.

mod common;

use std::time::Duration;

use common::{assertions, bridge::Bridge, replay, ws_client, ws_input::MockWsInput};
use serial_test::serial;

fn refdata() -> Vec<Vec<u8>> {
    replay::split_frames(
        &std::fs::read("tests/fixtures/phoenix_tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    )
}
fn mktdata() -> Vec<Vec<u8>> {
    replay::split_frames(
        &std::fs::read("tests/fixtures/phoenix_tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    )
}

/// All `trade` messages for a given symbol with a given `trade_id`.
fn trades_for<'a>(
    msgs: &'a [serde_json::Value],
    symbol: &str,
    trade_id: u64,
) -> Vec<&'a serde_json::Value> {
    ws_client::by_type(msgs, "trade")
        .into_iter()
        .filter(|t| {
            t.get("symbol").and_then(|v| v.as_str()) == Some(symbol)
                && t.get("trade_id").and_then(|v| v.as_u64()) == Some(trade_id)
        })
        .collect()
}

/// Edge gap → public fills in: replay the FULL Phoenix golden first (SOL prints real edge quotes
/// and trades, revealing its Source ID and advancing past whatever the capture holds), THEN the edge
/// goes quiet and a public Phoenix fill is pushed. With nothing ahead of it in the trade dedup
/// window, the public fill is emitted — the consumer keeps seeing prints through the gap.
///
/// This is a mid-stream gap, not a cold start: `ingest::processor`'s per-instrument deferral holds
/// `NormalizedInstrument`/prices back until the edge has revealed an instrument's Source ID at least
/// once (refdata alone never does — see the module docs), so an edge that never once goes live for
/// an instrument has nothing for the public feeder to key its precision gate against either; that is
/// a feed that was never live for the instrument, not a gap in one that was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn phoenix_edge_gap_public_trade_fills_in() {
    let mock = MockWsInput::start().await;
    let url = mock.url();
    let bridge = Bridge::spawn_with_args(
        "Phoenix",
        18185,
        &[
            "--phoenix-ws-input-url",
            &url,
            "--phoenix-ws-input-markets",
            "SOL",
        ],
    );
    let ws_addr = bridge.ws_addr.clone();

    // A trade_id not present in the mktdata fixture, so this can only be the public fill.
    const PUBLIC_TID: u64 = 999_000_001;
    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(8), |m| {
            !trades_for(m, "SOL", PUBLIC_TID).is_empty()
        })
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Edge refdata AND mktdata: SOL prints real quotes/trades (instrument_id 0), revealing its
    // Source ID, before the edge goes quiet — the gap that follows is mid-stream.
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::PHOENIX_GROUP, 9202, &refdata()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::PHOENIX_GROUP, 9201, &mktdata()).unwrap();
    })
    .await
    .unwrap();
    // Let refdata+mktdata be processed (SOL revealed) and the feeder connect.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Public Phoenix fill during the gap that follows: bid (aggressing buy),
    // price = quote/base = 300/2 = 150.
    mock.send_phoenix_trade("SOL", PUBLIC_TID, "bid", 2.0, 300.0, 1_775_578_550);

    let msgs = collector.await.unwrap();
    assertions::instrument_before_price(&msgs);
    assertions::no_business_duplicates(&msgs);
    assertions::trades_well_formed(&msgs);

    let hits = trades_for(&msgs, "SOL", PUBLIC_TID);
    assert_eq!(
        hits.len(),
        1,
        "the public fill should reach the wire exactly once"
    );
    let t = hits[0];
    assert_eq!(t.get("venue").and_then(|v| v.as_str()), Some("Phoenix"));
    assert_eq!(
        t.get("aggressor_side").and_then(|v| v.as_str()),
        Some("buy")
    );
    assert_eq!(t.get("price").and_then(|v| v.as_f64()), Some(150.0));
}

/// Edge leads → public deduped: replay the full edge Phoenix golden (which prints `AMD` trade_id
/// 20418, among others), let it process, THEN push the public copy of that exact fill with a
/// distinctive price. The arbiter's windowed `trade_id` dedup drops the public copy: `trade_id`
/// 20418 reaches the wire exactly once, and the distinctive public price never leaks. Falsifiable —
/// bypass the dedup and `no_business_duplicates` trips on the duplicate `(Phoenix, AMD, 20418)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn phoenix_edge_leads_public_trade_deduped() {
    let mock = MockWsInput::start().await;
    let url = mock.url();
    let bridge = Bridge::spawn_with_args(
        "Phoenix",
        18186,
        &[
            "--phoenix-ws-input-url",
            &url,
            "--phoenix-ws-input-markets",
            "AMD",
        ],
    );
    let ws_addr = bridge.ws_addr.clone();

    // The edge AMD fill in the golden. The public copy carries the same id with a distinctive price.
    const AMD_TID: u64 = 20418;
    const PUBLIC_PRICE: f64 = 1.0; // far from the real ~$565 AMD print, so a leak is unmistakable.

    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(6), |_| false).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Replay the edge feed (refdata then mktdata): the AMD trade 20418 prints and advances the
    // arbiter's trade dedup window.
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::PHOENIX_GROUP, 9202, &refdata()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::PHOENIX_GROUP, 9201, &mktdata()).unwrap();
    })
    .await
    .unwrap();
    // Let the edge mktdata be fully processed and the feeder connect/subscribe.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Push the public copy of the same fill: same id, distinctive price (base=1, quote=PUBLIC_PRICE).
    mock.send_phoenix_trade("AMD", AMD_TID, "bid", 1.0, PUBLIC_PRICE, 1_775_578_560);

    let msgs = collector.await.unwrap();
    assertions::instrument_before_price(&msgs);
    // The dedup oracle: no two trades share (venue, symbol, trade_id). If the public copy of 20418
    // leaked, this trips.
    assertions::no_business_duplicates(&msgs);
    assertions::trades_well_formed(&msgs);

    let hits = trades_for(&msgs, "AMD", AMD_TID);
    assert_eq!(
        hits.len(),
        1,
        "AMD trade_id {AMD_TID} must appear exactly once (edge wins, public copy deduped)"
    );
    // The survivor is the edge print, not the distinctive public copy.
    assert_ne!(
        hits[0].get("price").and_then(|v| v.as_f64()),
        Some(PUBLIC_PRICE),
        "the public (losing) copy leaked onto the wire"
    );
    assert!(
        !ws_client::by_type(&msgs, "trade").iter().any(|t| {
            t.get("symbol").and_then(|v| v.as_str()) == Some("AMD")
                && t.get("price").and_then(|v| v.as_f64()) == Some(PUBLIC_PRICE)
        }),
        "no AMD trade should carry the distinctive public price"
    );
}
