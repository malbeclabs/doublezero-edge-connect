mod common;

use common::{assertions, bridge::Bridge, replay, ws_client};
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

#[test]
fn mbo_goldens_split_into_valid_frames() {
    for name in ["mbo_mktdata.bin", "mbo_refdata.bin", "mbo_snapshot.bin"] {
        let bytes = std::fs::read(format!("tests/fixtures/{name}")).unwrap();
        let frames = replay::split_frames(&bytes, replay::MBO_MAGIC);
        assert!(!frames.is_empty(), "{name}: no frames");
    }
}

/// Spawn the bridge, replay the MBO golden once in wire order (refdata, snapshot, mktdata),
/// and assert the depth output contract. The snapshot is an empty-book anchor (anchor_seq=0,
/// last_instrument_seq=0): it flips the book to `Synced` at empty, then the 140 replayed deltas
/// (per-instrument seq 1..=140, all contiguous after the anchor) build the live book and `depth`
/// flows. The capture is mid-session (the first deltas cancel resting orders added before the
/// capture began); those cancels no-op against the empty book, so the resulting depth is the
/// real subset of orders added during the capture — no fabricated state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn mbo_single_publisher_depth_contract() {
    let bridge = Bridge::spawn("Hyperliquid", 18083);
    let ws_addr = bridge.ws_addr.clone();

    let collector = tokio::spawn(async move {
        ws_client::collect(&ws_addr, Duration::from_secs(10), |_| false).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let refdata = replay::split_frames(
        &std::fs::read("tests/fixtures/mbo_refdata.bin").unwrap(),
        replay::MBO_MAGIC,
    );
    let snapshot = replay::split_frames(
        &std::fs::read("tests/fixtures/mbo_snapshot.bin").unwrap(),
        replay::MBO_MAGIC,
    );
    let mktdata = replay::split_frames(
        &std::fs::read("tests/fixtures/mbo_mktdata.bin").unwrap(),
        replay::MBO_MAGIC,
    );

    // Refdata first (definitions), then snapshot (anchor the book at empty), then mktdata
    // (deltas). The book reaches `Synced` on SnapshotEnd, after which live deltas apply directly.
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 10202, &refdata).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 10203, &snapshot).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 10201, &mktdata).unwrap();
    })
    .await
    .unwrap();

    let msgs = collector.await.unwrap();

    let depths = ws_client::by_type(&msgs, "depth");
    assert!(
        !depths.is_empty(),
        "no depth messages — book never synced (check snapshot fixture ordering/anchor)"
    );
    assert!(
        !ws_client::by_type(&msgs, "instrument").is_empty(),
        "no instrument messages"
    );

    assertions::instrument_before_price(&msgs);
    assertions::no_business_duplicates(&msgs);

    // Depth ordering + bounds: bids descending, asks ascending, <= 10 levels.
    for d in &depths {
        let bids = d
            .get("bids")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let asks = d
            .get("asks")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            bids.len() <= 10 && asks.len() <= 10,
            "depth exceeds 10 levels: {d}"
        );
        let px =
            |lvl: &serde_json::Value| lvl.as_array().and_then(|a| a[0].as_f64()).unwrap_or(0.0);
        for w in bids.windows(2) {
            assert!(px(&w[0]) >= px(&w[1]), "bids not descending: {d}");
        }
        for w in asks.windows(2) {
            assert!(px(&w[0]) <= px(&w[1]), "asks not ascending: {d}");
        }
        // A crossed book (best bid >= best ask) indicates the MBO side labels are inverted.
        if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            assert!(
                px(best_bid) < px(best_ask),
                "crossed book: best_bid={} >= best_ask={} (MBO side inversion?)",
                px(best_bid),
                px(best_ask)
            );
        }
    }

    // The fixture is predominantly buy-side (wire side=0=Bid). At least one depth must carry bids;
    // if none do, the MBO side constants are inverted (bids wrongly routed to asks).
    assert!(
        depths.iter().any(|d| d
            .get("bids")
            .and_then(|v| v.as_array())
            .map(|b| !b.is_empty())
            .unwrap_or(false)),
        "no depth message had a non-empty bid side — likely MBO side-constant inversion"
    );

    // MBO is depth-only: no trades from this venue (TOB owns trades, idle here).
    assert!(
        ws_client::by_type(&msgs, "trade").is_empty(),
        "MBO feed emitted trades despite emit_trades=false"
    );
}
