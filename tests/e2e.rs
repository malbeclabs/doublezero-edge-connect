mod common;

use common::assertions;
use common::bridge::Bridge;
use common::replay;
use common::ws_client;
use serial_test::serial;
use std::time::Duration;

#[test]
fn tob_golden_splits_into_valid_frames() {
    let bytes =
        std::fs::read("tests/fixtures/tob_marketdata.bin").expect("read tob_marketdata.bin");
    let frames = replay::split_frames(&bytes, replay::TOB_MAGIC);
    assert!(!frames.is_empty(), "expected at least one TOB frame");
    for f in &frames {
        assert!(f.len() >= 24);
        assert_eq!(u16::from_le_bytes([f[0], f[1]]), replay::TOB_MAGIC);
    }
}

#[test]
fn tob_refdata_golden_splits_into_valid_frames() {
    let bytes = std::fs::read("tests/fixtures/tob_refdata.bin").expect("read tob_refdata.bin");
    let frames = replay::split_frames(&bytes, replay::TOB_MAGIC);
    assert!(
        !frames.is_empty(),
        "expected at least one TOB refdata frame"
    );
    for f in &frames {
        assert!(f.len() >= 24);
        assert_eq!(u16::from_le_bytes([f[0], f[1]]), replay::TOB_MAGIC);
    }
}

#[test]
#[serial]
fn bridge_starts_and_serves_ws() {
    let bridge = Bridge::spawn("Hyperliquid", 18090);
    assert!(std::net::TcpStream::connect(&bridge.ws_addr).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn ws_client_connects_and_times_out_clean() {
    let bridge = Bridge::spawn("Hyperliquid", 18091);
    // No data replayed: we just prove the client connects and the timeout path returns.
    let msgs = ws_client::collect(&bridge.ws_addr, Duration::from_millis(500), |_| false).await;
    // Connection succeeded; with no input there are no quotes.
    assert!(ws_client::by_type(&msgs, "quote").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn spike_loopback_multicast_produces_a_quote() {
    let bridge = Bridge::spawn("Hyperliquid", 18081);
    let ws_addr = bridge.ws_addr.clone();

    // Connect first so we don't miss streamed quotes (quotes are not replayed on connect).
    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(15), |m| {
            !ws_client::by_type(m, "quote").is_empty()
        })
        .await
    });
    // Let the collector connect before replay begins.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let refdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    let mktdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );

    // Fixture is in real wire order (manifest before def), so a single refdata pass retains the
    // def and the quote precision gate resolves immediately.
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9201, &mktdata).unwrap();
    })
    .await
    .unwrap();

    let msgs = collector.await.unwrap();
    let quotes = ws_client::by_type(&msgs, "quote");
    assert!(!quotes.is_empty(), "expected at least one quote on the WS");
    assert_eq!(
        quotes[0].get("venue").and_then(|v| v.as_str()),
        Some("Hyperliquid")
    );
}

/// Spawn the bridge, replay the full TOB golden once (single publisher), and assert the
/// output contract. The `quote_count` baseline is pinned on first green run (Step 4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn tob_single_publisher_contract() {
    let bridge = Bridge::spawn("Hyperliquid", 18082);
    let ws_addr = bridge.ws_addr.clone();

    // Collect for a fixed window after replay completes (we do not know the exact count
    // up front; the window must comfortably exceed replay duration but stay under the 30s
    // idle-rejoin watchdog).
    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(8), |_| false).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let refdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    let mktdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9201, &mktdata).unwrap();
    })
    .await
    .unwrap();

    let msgs = collector.await.unwrap();

    assert!(
        !ws_client::by_type(&msgs, "instrument").is_empty(),
        "no instrument messages"
    );
    assert!(
        !ws_client::by_type(&msgs, "quote").is_empty(),
        "no quote messages"
    );
    assertions::instrument_before_price(&msgs);
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);
    assertions::trades_well_formed(&msgs);

    let quote_count = ws_client::by_type(&msgs, "quote").len();
    assert_eq!(quote_count, 41, "TOB quote count regressed");
}
