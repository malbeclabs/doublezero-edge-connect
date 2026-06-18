//! Backstop-arbitrage E2E: drive the real bridge with **both** the DZ Edge multicast replayer and
//! the mock Hyperliquid public WS input feeder, and assert the shared arbiter races them correctly.
//!
//! Two cases, both over the unchanged WS output contract (`ws_client` + `assertions`):
//!   1. **edge leads in steady state** — the edge feed advances the per-(venue, symbol) floor first,
//!      so the public copies of those ticks lose the race and are dropped as no-ops (quote count is
//!      exactly the edge-only count; `no_business_duplicates` green; `source_ts` non-decreasing).
//!   2. **edge gap → public fills in** — with no edge quotes (refdata only), the public feed opens
//!      each tick and is emitted, so a consumer keeps seeing top-of-book through the gap.
//!
//! Both reuse the BTC single-publisher golden (`tob_refdata.bin` / `tob_marketdata.bin`, venue
//! Hyperliquid) so the public coin `BTC` shares the edge feed's `(venue, symbol)` floor.

mod common;

use std::time::Duration;

use common::{assertions, bridge::Bridge, replay, ws_client, ws_input::MockWsInput};
use serial_test::serial;

/// The edge-only quote count for the BTC single-publisher golden (pinned in `e2e.rs`). In the
/// steady-state case the public copies must add nothing to this.
const EDGE_ONLY_QUOTES: usize = 41;

fn refdata() -> Vec<Vec<u8>> {
    replay::split_frames(
        &std::fs::read("tests/fixtures/tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    )
}
fn mktdata() -> Vec<Vec<u8>> {
    replay::split_frames(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    )
}

/// Edge leads in steady state: replay the full edge feed (advancing the floor across all its ticks),
/// THEN have the public feed emit copies at already-passed `source_ts`. Each public copy is below
/// the floor's high-water, so it is dropped — the edge already won every tick. The emitted quote
/// count therefore equals the edge-only count, and the output contract holds. Falsifiable: with the
/// floor bypassed, the public copies would re-emit and the count would exceed `EDGE_ONLY_QUOTES`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn edge_leads_steady_state_public_dropped() {
    let mock = MockWsInput::start().await;
    let url = mock.url();
    let bridge = Bridge::spawn_with_args(
        "Hyperliquid",
        18181,
        &["--ws-input-url", &url, "--ws-input-coins", "BTC"],
    );
    let ws_addr = bridge.ws_addr.clone();

    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(8), |_| false).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Replay the full edge feed first, in wire order (refdata then mktdata).
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9201, &mktdata()).unwrap();
    })
    .await
    .unwrap();

    // Let the edge mktdata be fully processed (floor advanced) and the feeder connect/subscribe.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Now the public feed mirrors several updates at long-passed source_ts (time_ms=1 → 1e6 ns,
    // far below the edge's epoch-ns floor) with a distinctive price. Every one is stale → dropped.
    for i in 0..5 {
        mock.send_bbo("BTC", 1 + i, 99999.0, 1.0, 100000.0, 1.0);
    }

    let msgs = collector.await.unwrap();
    let quotes = ws_client::by_type(&msgs, "quote");

    assertions::instrument_before_price(&msgs);
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);
    // The public copies were all stale and dropped: the count is exactly the edge-only count.
    assert_eq!(
        quotes.len(),
        EDGE_ONLY_QUOTES,
        "public copies must be dropped as non-leaders; expected {EDGE_ONLY_QUOTES} edge quotes only"
    );
    // Belt and suspenders: the distinctive public price never reached the wire.
    assert!(
        !quotes
            .iter()
            .any(|q| q.get("bid").and_then(|v| v.as_f64()) == Some(99999.0)),
        "a public (losing) quote leaked onto the wire"
    );
}

/// Edge gap → public fills in: replay edge refdata only (so BTC's precision is known but no edge
/// quote ever opens a tick), then have the public feed emit. With nothing ahead of it on the floor,
/// each public update opens its tick and is emitted — the consumer keeps seeing top-of-book through
/// the gap, with no health check anywhere in the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn edge_gap_public_fills_in() {
    let mock = MockWsInput::start().await;
    let url = mock.url();
    let bridge = Bridge::spawn_with_args(
        "Hyperliquid",
        18182,
        &["--ws-input-url", &url, "--ws-input-coins", "BTC"],
    );
    let ws_addr = bridge.ws_addr.clone();

    // Stop as soon as both a quote and a trade have arrived (or the window elapses).
    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(8), |m| {
            !ws_client::by_type(m, "quote").is_empty() && !ws_client::by_type(m, "trade").is_empty()
        })
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Edge refdata ONLY: BTC instrument is defined, but the edge sends no quotes (the gap).
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata()).unwrap();
    })
    .await
    .unwrap();
    // Let refdata be processed (so the feeder's precision gate passes) and the feeder connect.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Public feed opens two successive ticks with a distinctive price, plus a trade.
    mock.send_bbo("BTC", 1_700_000_000_000, 12345.0, 2.0, 12346.0, 3.0);
    mock.send_bbo("BTC", 1_700_000_000_001, 12347.0, 2.0, 12348.0, 3.0);
    mock.send_trade("BTC", "B", 12345.5, 0.5, 1_700_000_000_000, 777);

    let msgs = collector.await.unwrap();
    let quotes = ws_client::by_type(&msgs, "quote");
    let trades = ws_client::by_type(&msgs, "trade");

    assertions::instrument_before_price(&msgs);
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);

    // The public feed filled the edge gap: its quotes reached the wire, tagged Hyperliquid.
    assert!(
        !quotes.is_empty(),
        "public feed produced no quotes to fill the edge gap"
    );
    assert!(
        quotes
            .iter()
            .all(|q| q.get("venue").and_then(|v| v.as_str()) == Some("Hyperliquid")),
        "public quotes must be tagged with the Hyperliquid venue"
    );
    assert!(
        quotes
            .iter()
            .any(|q| q.get("bid").and_then(|v| v.as_f64()) == Some(12345.0)),
        "the public quote's price did not reach the wire"
    );
    // The public trade also fills in, keyed by the venue tid.
    assert!(
        trades
            .iter()
            .any(|t| t.get("trade_id").and_then(|v| v.as_u64()) == Some(777)),
        "the public trade did not reach the wire"
    );
}
