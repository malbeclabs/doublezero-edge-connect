//! Our Kalshi-compatible output must deserialize into the types its consumer parses.
//!
//! These mirror NautilusTrader **2.0.0rc9** (`crates/adapters/kalshi`, published as `nautilus-kalshi`
//! 0.68.0): `websocket/messages.rs` for the envelope, `book/sync.rs` for the book payloads,
//! `data/parse.rs` for ticker and trade, and `http/models.rs` for `FixedPoint` and the sides. They are
//! a copy, so they go stale silently: when bumping the reference, re-read those files first.
//!
//! Golden data frames come from `tests/fixtures/kalshi_*_golden.json`, which the sink's own unit tests
//! pin to its output. Control frames come from a running sink answering the commands Nautilus sends.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use doublezero_edge_connect::{
    model::{BookReplay, BookSnapshot, FeedMessage, InstrumentSnapshot},
    sinks::kalshi,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

/// `http/models.rs`'s `FixedPoint`, held as the string it validated: Nautilus parses it exactly with
/// `rust_decimal`, which this crate does not carry.
#[derive(Debug, PartialEq)]
struct FixedPoint(String);

impl<'de> Deserialize<'de> for FixedPoint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let digits = value.strip_prefix('-').unwrap_or(&value);
        let valid = !digits.is_empty()
            && digits.bytes().all(|c| c.is_ascii_digit() || c == b'.')
            && !digits.starts_with('.')
            && !digits.ends_with('.');
        if !valid {
            return Err(serde::de::Error::custom("invalid fixed-point string"));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum OutcomeSide {
    Yes,
    No,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum KalshiChannel {
    OrderbookDelta,
    Ticker,
    Trade,
    Fill,
    MarketPositions,
    MarketLifecycleV2,
    UserOrders,
}

/// The envelope, internally tagged on `type`. A shape that misses its variant fails the whole frame
/// as malformed, which Nautilus treats as an error on every book.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KalshiWsMessage {
    Subscribed {
        id: u64,
        msg: KalshiSubscribed,
    },
    Unsubscribed {
        #[allow(dead_code)]
        id: Option<u64>,
        sid: u64,
        seq: u64,
    },
    Ok {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
        #[allow(dead_code)]
        sending_ts_ms: Option<u64>,
        msg: Option<KalshiUpdated>,
    },
    Error {
        id: Option<u64>,
        #[allow(dead_code)]
        sid: Option<u64>,
        msg: KalshiServerError,
    },
    OrderbookSnapshot(KalshiDataMessage),
    OrderbookDelta(KalshiDataMessage),
    Ticker(KalshiDataMessage),
    Trade(KalshiDataMessage),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct KalshiSubscribed {
    channel: KalshiChannel,
    sid: u64,
}

#[derive(Debug, Deserialize)]
struct KalshiUpdated {
    market_tickers: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiServerError {
    code: u32,
    #[allow(dead_code)]
    msg: String,
}

#[derive(Debug, Deserialize)]
struct KalshiDataMessage {
    id: Option<u64>,
    sid: u64,
    seq: Option<u64>,
    sending_ts_ms: Option<u64>,
    msg: Value,
}

/// `book/sync.rs`.
#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(default)]
    yes_dollars_fp: Vec<[FixedPoint; 2]>,
    #[serde(default)]
    no_dollars_fp: Vec<[FixedPoint; 2]>,
}

/// `book/sync.rs`.
#[derive(Debug, Deserialize)]
struct Delta {
    price_dollars: FixedPoint,
    delta_fp: FixedPoint,
    side: OutcomeSide,
    ts_ms: Option<u64>,
}

/// `data/parse.rs`.
#[derive(Debug, Deserialize)]
struct Ticker {
    yes_bid_dollars: Option<FixedPoint>,
    yes_ask_dollars: Option<FixedPoint>,
    yes_bid_size_fp: Option<FixedPoint>,
    yes_ask_size_fp: Option<FixedPoint>,
    ts_ms: Option<u64>,
    #[allow(dead_code)]
    ts: Option<u64>,
}

/// `data/parse.rs`.
#[derive(Debug, Deserialize)]
struct Trade {
    trade_id: String,
    yes_price_dollars: FixedPoint,
    no_price_dollars: Option<FixedPoint>,
    count_fp: FixedPoint,
    taker_book_side: Option<BookSide>,
    taker_outcome_side: Option<OutcomeSide>,
    #[serde(default)]
    is_block_trade: bool,
    ts_ms: Option<u64>,
    #[allow(dead_code)]
    ts: Option<u64>,
}

fn data(frame: &str) -> (String, KalshiDataMessage) {
    let value: Value = serde_json::from_str(frame.trim_end()).unwrap();
    let kind = value["type"].as_str().unwrap().to_string();
    let data = match serde_json::from_value(value).expect("must parse into Nautilus's envelope") {
        KalshiWsMessage::OrderbookSnapshot(d)
        | KalshiWsMessage::OrderbookDelta(d)
        | KalshiWsMessage::Ticker(d)
        | KalshiWsMessage::Trade(d) => d,
        other => panic!("not a data frame: {other:?}"),
    };
    (kind, data)
}

#[test]
fn a_golden_orderbook_snapshot_parses_as_nautilus_parses_it() {
    let (kind, d) = data(include_str!(
        "fixtures/kalshi_orderbook_snapshot_golden.json"
    ));
    assert_eq!(
        (kind.as_str(), d.sid, d.seq, d.id),
        ("orderbook_snapshot", 1, Some(1), None)
    );
    assert!(d.sending_ts_ms.is_some());
    assert_eq!(d.msg["market_ticker"], "KXNFLGAME-26NOV22DALPHI-DAL");
    let s: Snapshot = serde_json::from_value(d.msg).unwrap();
    assert_eq!(
        s.yes_dollars_fp[0],
        [FixedPoint("0.0100".into()), FixedPoint("300.00".into())]
    );
    assert_eq!(s.no_dollars_fp[1][0], FixedPoint("0.4600".into()));
}

#[test]
fn a_golden_orderbook_delta_parses_as_nautilus_parses_it() {
    let (kind, d) = data(include_str!("fixtures/kalshi_orderbook_delta_golden.json"));
    assert_eq!((kind.as_str(), d.seq), ("orderbook_delta", Some(2)));
    let delta: Delta = serde_json::from_value(d.msg).unwrap();
    assert_eq!(delta.side, OutcomeSide::No);
    assert_eq!(delta.price_dollars, FixedPoint("0.4400".into()));
    assert_eq!(delta.delta_fp, FixedPoint("-54.00".into()));
    assert!(delta.ts_ms.is_some());
}

/// Nautilus reads a quote only when all four fields are present and both sizes positive.
#[test]
fn a_golden_ticker_parses_as_nautilus_parses_it() {
    let (kind, d) = data(include_str!("fixtures/kalshi_ticker_golden.json"));
    assert_eq!((kind.as_str(), d.seq), ("ticker", None));
    let t: Ticker = serde_json::from_value(d.msg).unwrap();
    assert_eq!(t.yes_bid_dollars, Some(FixedPoint("0.2200".into())));
    assert_eq!(t.yes_ask_dollars, Some(FixedPoint("0.4400".into())));
    assert_eq!(t.yes_bid_size_fp, Some(FixedPoint("333.00".into())));
    assert_eq!(t.yes_ask_size_fp, Some(FixedPoint("146.00".into())));
    assert!(t.ts_ms.is_some());
}

/// Nautilus requires a non-block print whose YES and NO prices sum to one, with a canonical taker.
#[test]
fn a_golden_trade_parses_as_nautilus_parses_it() {
    let (kind, d) = data(include_str!("fixtures/kalshi_trade_golden.json"));
    assert_eq!((kind.as_str(), d.seq), ("trade", Some(1)));
    let t: Trade = serde_json::from_value(d.msg).unwrap();
    assert_eq!(
        t.trade_id.len(),
        36,
        "Nautilus's TradeId holds at most 36 characters"
    );
    assert_eq!(t.yes_price_dollars, FixedPoint("0.2200".into()));
    assert_eq!(t.no_price_dollars, Some(FixedPoint("0.7800".into())));
    assert_eq!(t.count_fp, FixedPoint("12.50".into()));
    assert_eq!(
        (t.taker_book_side, t.taker_outcome_side),
        (Some(BookSide::Ask), Some(OutcomeSide::No))
    );
    assert!(!t.is_block_trade);
    assert!(t.ts_ms.is_some());
}

/// Every control frame the sink sends parses into the variant Nautilus expects, for the commands
/// Nautilus sends, in the form it serializes them.
#[tokio::test]
async fn control_frames_parse_as_nautilus_parses_them() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
    let books: BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
    let instruments: InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(kalshi::serve(listener, tx, books, instruments));
    let request = format!("ws://{addr}/trade-api/ws/v2")
        .into_client_request()
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    let mut exchange = async |cmd: Value, frames: usize| {
        ws.send(Message::Text(cmd.to_string().into()))
            .await
            .unwrap();
        let mut out = Vec::new();
        while out.len() < frames {
            let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap();
            match next {
                Some(Ok(Message::Text(t))) => {
                    out.push(serde_json::from_str::<KalshiWsMessage>(&t).expect("must parse"))
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                other => panic!("after {out:?}, the sink sent {other:?}"),
            }
        }
        out
    };
    let market = "KXNFLGAME-26NOV22DALPHI-DAL";

    let lifecycle = exchange(
        json!({"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}),
        1,
    )
    .await;
    let [KalshiWsMessage::Subscribed { id: 1, msg }] = lifecycle.as_slice() else {
        panic!("{lifecycle:?}")
    };
    assert_eq!(
        (&msg.channel, msg.sid),
        (&KalshiChannel::MarketLifecycleV2, 1)
    );

    let book = json!({"id": 2, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": [market], "use_yes_price": true}});
    let frames = exchange(book, 2).await;
    let [KalshiWsMessage::Subscribed { id: 2, msg }, KalshiWsMessage::OrderbookSnapshot(snap)] =
        frames.as_slice()
    else {
        panic!("{frames:?}")
    };
    assert_eq!(
        (&msg.channel, snap.sid, snap.seq, snap.id),
        (&KalshiChannel::OrderbookDelta, msg.sid, Some(1), None)
    );

    let ticker = json!({"id": 3, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": [market], "send_initial_snapshot": true}});
    assert!(matches!(
        exchange(ticker, 1).await.as_slice(),
        [KalshiWsMessage::Subscribed { id: 3, .. }]
    ));

    let snapshot = json!({"id": 4, "cmd": "update_subscription", "params": {"sid": 2, "market_tickers": [market], "action": "get_snapshot"}});
    let frames = exchange(snapshot, 1).await;
    let [KalshiWsMessage::OrderbookSnapshot(snap)] = frames.as_slice() else {
        panic!("{frames:?}")
    };
    assert_eq!((snap.id, snap.seq), (Some(4), Some(2)));

    let add = json!({"id": 5, "cmd": "update_subscription", "params": {"sid": 2, "market_tickers": ["KXA"], "action": "add_markets"}});
    let frames = exchange(add, 2).await;
    let [KalshiWsMessage::Ok {
        id: Some(5),
        sid: Some(2),
        seq: Some(3),
        msg: Some(updated),
        ..
    }, KalshiWsMessage::OrderbookSnapshot(_)] = frames.as_slice()
    else {
        panic!("{frames:?}")
    };
    assert_eq!(updated.market_tickers, ["KXA", market]);

    let delete = json!({"id": 6, "cmd": "update_subscription", "params": {"sid": 2, "market_tickers": ["KXA"], "action": "delete_markets"}});
    let frames = exchange(delete, 1).await;
    assert!(
        matches!(
            frames.as_slice(),
            [KalshiWsMessage::Ok {
                id: Some(6),
                seq: Some(5),
                ..
            }]
        ),
        "{frames:?}"
    );

    let fill = json!({"id": 7, "cmd": "subscribe", "params": {"channels": ["fill"]}});
    let frames = exchange(fill, 1).await;
    let [KalshiWsMessage::Error {
        id: Some(7), msg, ..
    }] = frames.as_slice()
    else {
        panic!("{frames:?}")
    };
    assert_eq!(msg.code, 9);

    let unsubscribe = json!({"id": 8, "cmd": "unsubscribe", "params": {"sids": [2]}});
    let frames = exchange(unsubscribe, 1).await;
    assert!(
        matches!(
            frames.as_slice(),
            [KalshiWsMessage::Unsubscribed { sid: 2, seq: 6, .. }]
        ),
        "{frames:?}"
    );
    assert!(!frames.iter().any(|f| matches!(f, KalshiWsMessage::Unknown)));
}
