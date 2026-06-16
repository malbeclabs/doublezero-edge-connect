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
    tokio::time::sleep(Duration::from_millis(300)).await;

    let refdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_refdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    let mktdata = replay::split_frames(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );

    // The golden refdata capture orders the frames ChannelReset -> InstrumentDefinition ->
    // ManifestSummary. The subscriber state machine only *retains* a definition that arrives
    // while a valid manifest is in effect, and it clears the def set both on a ChannelReset and
    // on each new manifest. So the lone def in the capture (which precedes its manifest) is
    // discarded by a single pass, and replaying the whole capture again doesn't help: every pass
    // begins with the ChannelReset and ends with the manifest clearing defs. On the live feed
    // definitions repeat in bursts, so one eventually lands *after* a valid manifest. We
    // reproduce that here by replaying the full refdata (which leaves a valid manifest, seq=1, in
    // effect) and then re-sending just the InstrumentDefinition frames last, so they are retained
    // and the quote precision gate resolves. InstrumentDefinition is message type 0x02; every
    // fixture frame carries msg_count=1, so byte 24 is the message type.
    let instrument_defs: Vec<Vec<u8>> = refdata
        .iter()
        .filter(|f| f.len() > 24 && f[24] == 0x02)
        .cloned()
        .collect();
    assert!(
        !instrument_defs.is_empty(),
        "refdata fixture has no InstrumentDefinition frame"
    );
    tokio::task::spawn_blocking(move || {
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &refdata).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        replay::send_frames(replay::HYPERLIQUID_GROUP, 9202, &instrument_defs).unwrap();
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
