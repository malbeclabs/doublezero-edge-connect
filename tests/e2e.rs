mod common;

use common::bridge::Bridge;
use common::replay;
use common::ws_client;
use serial_test::serial;
use std::time::Duration;

#[test]
fn tob_golden_splits_into_valid_frames() {
    let bytes = std::fs::read("tests/fixtures/tob_marketdata.bin").expect("read tob_marketdata.bin");
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
    assert!(!frames.is_empty(), "expected at least one TOB refdata frame");
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
