//! `GET /v1/race` end to end against the mock public inputs: a made-up name is refused as each
//! venue refuses it, and a live symbol counts both inputs.
#![cfg(unix)] // Multicast ingest is unsupported on Windows.

mod common;

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

use common::{bridge::Bridge, replay, ws_input::MockWsInput};
use doublezero_edge_connect::{
    history,
    ingest::{
        arbiter::{Arbiter, SharedArbiter, TRADE_DEDUP_WINDOW},
        channel_filter::ChannelFilter,
        codec, feeds,
        health::FeedHealth,
        processor::TobProcessor,
        receiver::{DatagramCtx, DatagramProcessor, PortRole},
        ws_input,
    },
    metrics::metrics,
    model::{self, InstrumentSnapshot},
    sinks::api,
};
use serde_json::{json, Value};
use serial_test::serial;
use tokio::sync::broadcast;

/// `/v1/race` once `done` holds of it, or the last answer at `timeout`.
async fn race_until(api: &str, timeout: Duration, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let got = match reqwest::get(format!("http://{api}/v1/race")).await {
            Ok(r) => r.json().await.unwrap_or_default(),
            Err(_) => Value::Null,
        };
        if done(&got) || tokio::time::Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn venue<'a>(race: &'a Value, name: &str) -> &'a Value {
    race["venues"]
        .as_array()
        .and_then(|v| v.iter().find(|v| v["venue"] == name))
        .unwrap_or(&Value::Null)
}

fn symbol<'a>(race: &'a Value, venue_name: &str, name: &str) -> &'a Value {
    venue(race, venue_name)["symbols"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["symbol"] == name))
        .unwrap_or(&Value::Null)
}

/// The binary runs both inputs and serves their race: Hyperliquid refuses a made-up coin by
/// dropping the connection twice, Phoenix a made-up market with a `400` and no reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn the_binary_serves_both_inputs_race() {
    let (hl, px) = (
        MockWsInput::start_knowing(&["BTC"]).await,
        MockWsInput::start_knowing(&["SOL"]).await,
    );
    let (hl_url, px_url) = (hl.url(), px.url());
    let api = "127.0.0.1:19197";
    let _bridge = Bridge::spawn_with_args(
        "HYPERLIQUID",
        18197,
        &[
            "--ws-input-url",
            &hl_url,
            "--ws-input-coins",
            "BTC,NOPECOINZZ",
            "--phoenix-ws-input-url",
            &px_url,
            "--phoenix-ws-input-markets",
            "SOL,NOPEMARKETZZ",
            "--api-bind",
            api,
        ],
    );

    let race = race_until(api, Duration::from_secs(20), |r| {
        symbol(r, "HYPERLIQUID", "NOPECOINZZ")["status"] == "refused"
            && venue(r, "HYPERLIQUID")["public_input"]["up"] == true
            && symbol(r, "PHOENIX", "NOPEMARKETZZ")["status"] == "refused"
    })
    .await;
    assert!(race["since_ns"].as_u64().is_some_and(|ns| ns > 0), "{race}");
    let names: Vec<&Value> = race["venues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| &v["venue"])
        .collect();
    assert_eq!(names, ["HYPERLIQUID", "PHOENIX"], "{race}");
    assert_eq!(
        venue(&race, "HYPERLIQUID")["public_input"]["reconnects"],
        2,
        "{race}"
    );
    assert_eq!(
        symbol(&race, "HYPERLIQUID", "NOPECOINZZ")["status"],
        "refused"
    );
    let phoenix = venue(&race, "PHOENIX");
    assert_eq!(phoenix["public_input"]["reconnects"], 0, "{race}");
    assert_eq!(
        symbol(&race, "PHOENIX", "NOPEMARKETZZ")["status"],
        "refused"
    );
    assert!(phoenix.get("quotes").is_none(), "trades only: {race}");
    // Nothing on the edge defines either venue's symbols in this test.
    assert_eq!(symbol(&race, "HYPERLIQUID", "BTC")["status"], "pending");
    assert_eq!(symbol(&race, "PHOENIX", "SOL")["status"], "pending");
}

/// An empty list, as a shell sends to mean off, starts no input and leaves the bridge serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn an_empty_name_list_leaves_its_input_off() {
    let api = "127.0.0.1:19196";
    let mut bridge = Bridge::spawn_with_args(
        "HYPERLIQUID",
        18196,
        &[
            "--ws-input-coins",
            "",
            "--phoenix-ws-input-markets",
            ",",
            "--api-bind",
            api,
        ],
    );
    let race = race_until(api, Duration::from_secs(10), |r| r["venues"].is_array()).await;
    assert_eq!(race["venues"], json!([]), "{race}");
    assert!(bridge.running(), "the bridge exited");
}

/// The edge's datagrams handed to the processor the way a receiver hands them on, so the race is
/// counted through the real decode, arbiter, input and API without a multicast socket.
struct Edge {
    tob: TobProcessor,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
}

impl Edge {
    fn feed(&mut self, file: &str, role: PortRole) {
        let datagrams = replay::split_datagrams(
            &std::fs::read(format!("tests/fixtures/{file}")).unwrap(),
            replay::TOB_MAGIC,
        );
        self.send(&datagrams, role);
    }

    fn send(&mut self, datagrams: &[Vec<u8>], role: PortRole) {
        for d in datagrams {
            self.tob.on_datagram(
                d,
                &DatagramCtx {
                    venue: "HYPERLIQUID",
                    category: "hl-perps",
                    arbiter: &self.arbiter,
                    instruments: &self.instruments,
                    kernel_rx_ts_ns: 0,
                    recv_ts_ns: model::now_ns(),
                    role,
                    publisher: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                    mirror_offset: None,
                    declared_source_id: None,
                    per_channel: false,
                    synthesized: false,
                },
            );
        }
    }
}

/// The datagrams' quote ticks, the `0` sentinel included, and trade ids.
fn decode(datagrams: &[Vec<u8>]) -> (Vec<u64>, Vec<u64>) {
    let (mut ticks, mut trades) = (Vec::new(), Vec::new());
    for d in datagrams {
        for m in codec::decode_datagram(d).unwrap().1 {
            match m {
                codec::Message::Quote(q) => ticks.push(q.source_ts),
                codec::Message::Trade(t) => trades.push(t.trade_id),
                _ => {}
            }
        }
    }
    (ticks, trades)
}

/// BTC races once the edge defines it and its subscriptions are past the venue's replay: the edge's
/// ticks score for the edge, a late public copy gives the edge a lead, and a tick and a print only
/// the public input delivers score for it. The venue's figures are BTC's alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial] // each waits on the process-wide `dz_ws_input_messages_total`
async fn a_live_symbol_counts_both_inputs() {
    race_btc(false).await;
}

/// A coin that has never traded replays an empty batch, which names no coin: the batch after it
/// still counts, and so do the edge's trades before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial] // each waits on the process-wide `dz_ws_input_messages_total`
async fn an_empty_trades_replay_lets_the_next_batch_count() {
    race_btc(true).await;
}

async fn race_btc(empty_trades_replay: bool) {
    feeds::init_built_in();
    let (tx, _rx) = broadcast::channel(4096);
    let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, TRADE_DEDUP_WINDOW)));
    let instruments: InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let listener = api::bind("127.0.0.1:0").await.unwrap();
    let api = listener.local_addr().unwrap().to_string();
    tokio::spawn(api::serve(
        listener,
        instruments.clone(),
        Default::default(),
        Default::default(),
        Arc::new(Mutex::new(history::Store::new())),
        Arc::new(FeedHealth::new()),
        Arc::new(Mutex::new(ChannelFilter::default())),
        Vec::new(),
        arbiter.clone(),
    ));
    let mock = MockWsInput::start_knowing(&["BTC"]).await;
    tokio::spawn(ws_input::run(
        mock.url(),
        vec!["BTC".into(), "NOPECOINZZ".into()],
        arbiter.clone(),
        instruments.clone(),
    ));
    let mut edge = Edge {
        tob: TobProcessor::new(Arc::new(AtomicBool::new(true))),
        arbiter,
        instruments,
    };
    const HL: &str = "HYPERLIQUID";

    let race = race_until(&api, Duration::from_secs(20), |r| {
        symbol(r, HL, "NOPECOINZZ")["status"] == "refused"
            && venue(r, HL)["public_input"]["up"] == true
    })
    .await;
    assert_eq!(venue(&race, HL)["public_input"]["reconnects"], 2, "{race}");

    // The edge defines BTC with its first quote; the rest of its feed waits until BTC is live.
    let mktdata = replay::split_datagrams(
        &std::fs::read("tests/fixtures/tob_marketdata.bin").unwrap(),
        replay::TOB_MAGIC,
    );
    let reveal = 1 + mktdata
        .iter()
        .position(|d| !decode(std::slice::from_ref(d)).0.is_empty())
        .unwrap();
    let (first, rest) = mktdata.split_at(reveal);
    edge.feed("tob_refdata.bin", PortRole::Refdata);
    edge.send(first, PortRole::Mktdata);
    let race = race_until(&api, Duration::from_secs(10), |r| {
        symbol(r, HL, "BTC")["status"] == "racing"
    })
    .await;
    assert_eq!(symbol(&race, HL, "BTC")["status"], "racing", "{race}");

    // The venue's replay of earlier state, one frame per subscription, which never counts. Its
    // tick sits below every one the edge has still to deliver.
    let revealed = decode(first).0.into_iter().max().unwrap();
    let (ticks, trades) = decode(rest);
    let mut opened: Vec<u64> = ticks.into_iter().filter(|&t| t > revealed).collect();
    opened.dedup();
    let (low_ms, high_ms) = (opened[0] / 1_000_000, opened.last().unwrap() / 1_000_000);
    let emitted = |kind| {
        metrics()
            .ws_input_messages
            .with_label_values(&[HL, kind])
            .get()
    };
    let replayed = emitted("quote");
    if empty_trades_replay {
        mock.send_raw(r#"{"channel":"trades","data":[]}"#.to_string());
    } else {
        mock.send_trade("BTC", "B", 1.0, 1.0, low_ms - 1_000, 1);
    }
    // Last, so once it is emitted the input has read the trades replay too.
    mock.send_bbo("BTC", low_ms - 1_000, 1.0, 1.0, 2.0, 1.0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while emitted("quote") == replayed && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The input marks each live right after emitting its frame.
    tokio::time::sleep(Duration::from_millis(100)).await;

    edge.send(rest, PortRole::Mktdata);
    // Late public copies of the edge's last tick and print, then a tick and a print only the public
    // input has.
    mock.send_bbo("BTC", high_ms, 1.0, 1.0, 2.0, 1.0);
    mock.send_trade("BTC", "B", 1.0, 1.0, high_ms, trades[0]);
    mock.send_bbo("BTC", high_ms + 1_000, 1.0, 1.0, 2.0, 1.0);
    mock.send_trade("BTC", "B", 1.0, 1.0, high_ms + 1_000, 999_000_001);

    let race = race_until(&api, Duration::from_secs(10), |r| {
        let btc = symbol(r, HL, "BTC");
        btc["trades"]["won"]["public"] == 1
            && btc["quotes"]["won"]["public"] == 1
            && btc["quotes"]["lead_ns"]["edge"]["p50"].is_u64()
            && btc["trades"]["lead_ns"]["edge"]["p50"].is_u64()
    })
    .await;
    let btc = symbol(&race, HL, "BTC");
    assert_eq!(
        btc["quotes"]["won"],
        json!({ "edge": opened.len(), "public": 1 }),
        "{race}"
    );
    assert_eq!(
        btc["trades"]["won"],
        json!({ "edge": trades.len(), "public": 1 }),
        "{race}"
    );
    for kind in ["quotes", "trades"] {
        assert!(btc[kind]["lead_ns"]["edge"]["p99"].is_u64(), "{race}");
        assert_eq!(
            btc[kind]["lead_ns"]["public"],
            json!({ "p50": null, "p99": null })
        );
    }
    let hl = venue(&race, HL);
    assert_eq!(hl["quotes"], btc["quotes"], "only BTC counts: {race}");
    assert_eq!(hl["trades"], btc["trades"], "only BTC counts: {race}");
}
