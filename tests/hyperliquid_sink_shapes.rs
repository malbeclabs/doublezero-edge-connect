//! Our Hyperliquid-compatible output must deserialize into the types NautilusTrader parses.
//!
//! These mirror `crates/adapters/hyperliquid/src/websocket/messages.rs` at **v1.227.0**, the version
//! pinned by the reference consumer, and `common/enums.rs` for the side spelling. They are a copy, so
//! they go stale silently: when bumping the supported Nautilus version, re-read those files and update
//! these first.
//!
//! Every field here is required with no `serde(default)` upstream, which is the point — a field we
//! rename or retype fails our build instead of a trader's session. Nautilus sets no
//! `deny_unknown_fields`, so extra fields of ours are tolerated and are deliberately not pinned.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct WsLevelData {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Debug, Deserialize)]
struct WsBookData {
    coin: String,
    levels: [Vec<WsLevelData>; 2],
    #[allow(dead_code)]
    time: u64,
}

/// Nautilus deserializes only `"B"` and `"A"`; the container's `rename_all` is overridden per variant.
#[derive(Debug, Deserialize, PartialEq, Eq)]
enum HyperliquidSide {
    #[serde(rename = "B")]
    Buy,
    #[serde(rename = "A")]
    Sell,
}

#[derive(Debug, Deserialize)]
struct WsTradeData {
    coin: String,
    side: HyperliquidSide,
    px: String,
    sz: String,
    #[allow(dead_code)]
    hash: String,
    #[allow(dead_code)]
    time: u64,
    tid: u64,
    #[allow(dead_code)]
    users: [String; 2],
}

/// The envelope is internally tagged on `channel`; `l2Book`'s `data` is an object and `trades`' is an
/// array. An unknown `channel` is a hard deserialization error upstream, so the tag must match exactly.
#[derive(Debug, Deserialize)]
#[serde(tag = "channel")]
enum HyperliquidWsMessage {
    #[serde(rename = "l2Book")]
    L2Book { data: WsBookData },
    #[serde(rename = "trades")]
    Trades { data: Vec<WsTradeData> },
}

/// A golden `l2Book` frame, byte-for-byte as the sink emits it (the unit test
/// `golden_l2book_frame_matches_the_committed_fixture` pins that), must parse as Nautilus parses it.
#[test]
fn a_golden_l2book_frame_parses_as_nautilus_parses_it() {
    let frame = include_str!("fixtures/hl_l2book_golden.json");
    let HyperliquidWsMessage::L2Book { data } =
        serde_json::from_str(frame.trim_end()).expect("must parse into Nautilus's shape")
    else {
        panic!("must dispatch as l2Book")
    };
    assert_eq!(data.coin, "BTC");
    assert_eq!(data.levels[0][0].px, "100.5");
    assert_eq!(data.levels[0][0].sz, "8");
    assert_eq!(data.levels[0][0].n, 2);
    assert_eq!(data.levels[1][0].px, "101");
}

/// `px`/`sz` must be JSON strings and `n`/`time` JSON numbers. A number where Nautilus wants a string
/// fails with `invalid type: floating point`, which is exactly the silent break this pins.
#[test]
fn numeric_prices_would_not_parse() {
    let bad = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":100.5,"sz":"8","n":2}],[]]}}"#;
    assert!(serde_json::from_str::<HyperliquidWsMessage>(bad).is_err());
}

/// The trade envelope: `data` is an array, `side` is `"B"`/`"A"`, `tid` is a number.
#[test]
fn a_trade_frame_parses_as_nautilus_parses_it() {
    let frame = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100.5","sz":"2","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","time":1700000000000,"tid":424242,"users":["0x0000000000000000000000000000000000000000","0x0000000000000000000000000000000000000000"]}]}"#;
    let HyperliquidWsMessage::Trades { data } =
        serde_json::from_str(frame).expect("must parse into Nautilus's shape")
    else {
        panic!("must dispatch as trades")
    };
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].coin, "BTC");
    assert_eq!(data[0].side, HyperliquidSide::Buy);
    assert_eq!(data[0].px, "100.5");
    assert_eq!(data[0].sz, "2");
    assert_eq!(data[0].tid, 424_242);
}

/// `"buy"` is not a side Nautilus accepts, however natural it looks.
#[test]
fn a_spelled_out_side_would_not_parse() {
    let bad = r#"{"channel":"trades","data":[{"coin":"BTC","side":"buy","px":"1","sz":"1","hash":"0x0","time":1,"tid":1,"users":["",""]}]}"#;
    assert!(serde_json::from_str::<HyperliquidWsMessage>(bad).is_err());
}
