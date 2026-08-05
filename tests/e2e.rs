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

    let quotes = ws_client::by_type(&msgs, "quote");
    // The quote dedup is a per-(venue, symbol) source_ts staleness floor keyed on raw BBO content: it
    // keeps every distinct (source_ts, content) at a non-decreasing floor and drops only strictly-
    // older replays and exact duplicates. A single publisher delivers each of its samples once with
    // non-decreasing source_ts, so none are stale and none are exact duplicates — all 41 are emitted.
    assert_eq!(
        quotes.len(),
        41,
        "TOB single-publisher quote count under the source_ts staleness floor (no stale replays or exact dupes from one publisher, so all 41 distinct samples emit)"
    );
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
/// and assert the depth output contract on a REAL two-sided book. The snapshot is a complete
/// resting-order capture from the TYO recorder (publisher 148.51.123.3, BTC, snapshot_id 1106238:
/// 44598 orders, both sides) — `book.rs` installs it on `SnapshotEnd` and the book is `Synced`
/// two-sided immediately, then the contiguous post-anchor deltas apply live. Because the snapshot
/// carries genuine bids AND asks, the crossed-book assertion below is ACTIVE: a real best_bid >=
/// best_ask here would be a true side-mapping inversion, not a no-op on an empty/one-sided book.
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
        // ACTIVE two-sided crossed-book check. The fixture's snapshot is a real two-sided BTC book,
        // so every emitted depth carries bids AND asks; best_bid must be strictly below best_ask.
        // A crossed or one-sided book here is a genuine signal (MBO side-constant inversion or a
        // book-reconstruction bug) — investigate it, do NOT paper over it by swapping side labels.
        let (best_bid, best_ask) = (bids.first(), asks.first());
        assert!(
            best_bid.is_some() && best_ask.is_some(),
            "depth is one-sided on a two-sided fixture (MBO side-constant inversion?): {d}"
        );
        assert!(
            px(best_bid.unwrap()) < px(best_ask.unwrap()),
            "crossed book: best_bid={} >= best_ask={} (MBO side inversion?)",
            px(best_bid.unwrap()),
            px(best_ask.unwrap())
        );
    }

    // MBO is depth-only: no trades from this venue (TOB owns trades, idle here).
    assert!(
        ws_client::by_type(&msgs, "trade").is_empty(),
        "MBO feed emitted trades despite emit_trades=false"
    );
}

/// Two Hyperliquid TOB publishers on distinct port blocks (9101/9102 and 9201/9202 of the same
/// group) are BOTH ingested: each gets its own `dz_datagrams_received_total{publisher=...}`
/// series. Before multi-publisher support the bridge bound only one block and the other
/// publisher's datagrams were never received at all.
///
/// Both senders share one source IP (see `tests/common/replay.rs` TODO(#3)), so the arbiter
/// correctly collapses their identical content on the wire — deliberately NOT asserted here. This
/// test is about ingest reach; per-source-IP dedup is covered by `tests/dedup.rs`.
#[test]
#[serial]
fn two_publisher_port_blocks_are_both_ingested() {
    let metrics_bind = "127.0.0.1:19231".to_string();
    let _bridge = Bridge::spawn_with_args("Hyperliquid", 18231, &["--metrics-bind", &metrics_bind]);

    let refdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    let mktdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    // `Bridge::spawn` returns on the FIRST receiver-bound marker, but `--feed Hyperliquid` now
    // binds twelve receivers, so a one-shot send can land before these two have joined — an
    // un-joined group silently discards the datagram. Re-send each round until both publishers
    // register traffic (idempotent: the assertion is `> 0`, and duplicate frames are deduped).
    let body = scrape_until(
        &metrics_bind,
        Duration::from_secs(20),
        |b| publisher_datagrams(b, "aws-tyo-1") > 0 && publisher_datagrams(b, "aws-tyo-2") > 0,
        || {
            // Refdata before mktdata on each block (definitions gate emission), aws-tyo-1's first.
            replay::send_frames(replay::HYPERLIQUID_GROUP, 9102, &refdata).unwrap();
            replay::send_frames(replay::HYPERLIQUID_GROUP, 9101, &mktdata).unwrap();
            replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata).unwrap();
            replay::send_frames(replay::HYPERLIQUID_GROUP, 9201, &mktdata).unwrap();
        },
    );
    assert!(
        publisher_datagrams(&body, "aws-tyo-1") > 0,
        "publisher aws-tyo-1 (9101/9102) received nothing:\n{body}"
    );
    assert!(
        publisher_datagrams(&body, "aws-tyo-2") > 0,
        "publisher aws-tyo-2 (9201/9202) received nothing:\n{body}"
    );
}

/// Sum of the `dz_datagrams_received_total` samples whose labels name `publisher`.
fn publisher_datagrams(body: &str, publisher: &str) -> u64 {
    body.lines()
        .filter(|l| l.starts_with("dz_datagrams_received_total{"))
        .filter(|l| l.contains(&format!("publisher=\"{publisher}\"")))
        .filter_map(|l| l.rsplit(' ').next())
        .filter_map(|v| v.trim().parse::<f64>().ok())
        .map(|v| v as u64)
        .sum()
}

/// Run `send` then poll `GET /metrics` until `done` or the deadline, re-running `send` each round;
/// returns the last body scraped (so a failing assertion can print it).
fn scrape_until(
    addr: &str,
    timeout: Duration,
    done: impl Fn(&str) -> bool,
    send: impl Fn(),
) -> String {
    use std::io::{Read, Write};
    let deadline = std::time::Instant::now() + timeout;
    let mut last = String::new();
    loop {
        send();
        if let Ok(mut s) = std::net::TcpStream::connect(addr) {
            let _ = s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n");
            let mut body = String::new();
            let _ = s.read_to_string(&mut body);
            last = body;
            if done(&last) {
                return last;
            }
        }
        if std::time::Instant::now() >= deadline {
            return last;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
