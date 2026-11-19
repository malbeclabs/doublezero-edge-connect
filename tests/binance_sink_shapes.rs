//! Our Binance-compatible output must deserialize into the types its consumers parse.
//!
//! These mirror NautilusTrader **v1.231.0**: the Rust futures client
//! (`crates/adapters/binance/src/futures/websocket/streams/{messages,handler}.rs`), the Rust spot JSON
//! client (`src/spot/websocket/public_json/{messages,handler}.rs`) and the Python client a Python
//! `TradingNode` runs (`nautilus_trader/adapters/binance/{data.py,common/schemas/market.py,
//! spot/schemas/market.py}`, msgspec structs mirrored as serde ones). They are a copy, so they go
//! stale silently: when bumping the reference, re-read those files and update these first.
//!
//! Every field here is required upstream unless marked optional, which is the point — a field we
//! rename or retype fails our build instead of a trader's session. None of the upstream types refuse
//! unknown fields, so extra fields of ours are tolerated and deliberately not pinned.

#![allow(dead_code)]

use serde::Deserialize;
use serde_json::Value;

const USDSM_BOOK_TICKER: &str = include_str!("fixtures/binance_usdsm_bookticker_golden.json");
const USDSM_AGG_TRADE: &str = include_str!("fixtures/binance_usdsm_aggtrade_golden.json");
const SPOT_BOOK_TICKER: &str = include_str!("fixtures/binance_spot_bookticker_golden.json");
const SPOT_TRADE: &str = include_str!("fixtures/binance_spot_trade_golden.json");

/// The sink's acknowledgement and refusal, as its unit tests pin them.
const ACK: &str = r#"{"result":null,"id":1}"#;
const REFUSAL: &str = r#"{"code":2,"msg":"Invalid request: this sink serves <symbol>@bookTicker, <symbol>@aggTrade and <symbol>@trade, at most 1024 per connection; refused: btcusdt@depth@0ms"}"#;

// --- the Rust futures client ---

/// `common/enums.rs`: the `e` a futures frame is dispatched on.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum BinanceWsEventType {
    AggTrade,
    Trade,
    BookTicker,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct BinanceFuturesBookTickerMsg {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
}

#[derive(Debug, Deserialize)]
struct BinanceFuturesAggTradeMsg {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "a")]
    agg_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "f")]
    first_trade_id: u64,
    #[serde(rename = "l")]
    last_trade_id: u64,
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceFuturesWsSubscribeResponse {
    result: Option<Value>,
    id: u64,
}

/// What the futures handler makes of one frame, in its own order: anything carrying `result` or `id`
/// is a subscription response, then anything carrying `code` an error, then an event by `e`.
#[derive(Debug, PartialEq, Eq)]
enum Futures {
    Response,
    Error,
    Event(BinanceWsEventType),
}

fn futures_dispatch(frame: &str) -> Futures {
    let json: Value = serde_json::from_str(frame).unwrap();
    if json.get("result").is_some() || json.get("id").is_some() {
        return Futures::Response;
    }
    if json.get("code").and_then(Value::as_i64).is_some() {
        return Futures::Error;
    }
    Futures::Event(serde_json::from_value(json["e"].clone()).unwrap())
}

#[test]
fn the_futures_client_parses_the_golden_book_ticker() {
    assert_eq!(
        futures_dispatch(USDSM_BOOK_TICKER),
        Futures::Event(BinanceWsEventType::BookTicker)
    );
    let m: BinanceFuturesBookTickerMsg = serde_json::from_str(USDSM_BOOK_TICKER).unwrap();
    assert_eq!(
        m.symbol, "BTCUSDT",
        "the raw symbol its instrument cache is keyed on"
    );
    assert_eq!(
        (m.best_bid_price.as_str(), m.best_ask_price.as_str()),
        ("61825.40", "61825.50")
    );
    assert_eq!(m.transaction_time, 1_700_000_000_123, "milliseconds");
    assert_eq!(m.update_id, 0, "the documented placeholder");
}

#[test]
fn the_futures_client_parses_the_golden_agg_trade() {
    assert_eq!(
        futures_dispatch(USDSM_AGG_TRADE),
        Futures::Event(BinanceWsEventType::AggTrade)
    );
    let m: BinanceFuturesAggTradeMsg = serde_json::from_str(USDSM_AGG_TRADE).unwrap();
    assert_eq!(m.agg_trade_id, 6_778_484_593, "Nautilus's trade id");
    assert_eq!(
        (m.first_trade_id, m.last_trade_id),
        (m.agg_trade_id, m.agg_trade_id)
    );
    assert!(m.is_buyer_maker, "a sell aggressor");
}

#[test]
fn the_futures_client_reads_the_ack_as_a_success() {
    assert_eq!(futures_dispatch(ACK), Futures::Response);
    let r: BinanceFuturesWsSubscribeResponse = serde_json::from_str(ACK).unwrap();
    assert!(
        r.result.is_none(),
        "a null result is what confirms the streams"
    );
}

/// With an `id`, a refusal would dispatch as a response and, lacking `result`, confirm its streams.
#[test]
fn the_futures_client_reads_the_refusal_as_an_error_not_an_ack() {
    assert_eq!(futures_dispatch(REFUSAL), Futures::Error);
    let with_id = r#"{"code":2,"msg":"x","id":1}"#;
    let r: BinanceFuturesWsSubscribeResponse = serde_json::from_str(with_id).unwrap();
    assert!(
        r.result.is_none(),
        "why the refusal must not carry an id: this would read as a success"
    );
}

// --- the Rust spot JSON client ---

#[derive(Debug, Deserialize)]
struct BinanceCombinedStreamEvent {
    stream: String,
    data: Value,
}

/// Tried before anything else on every frame, so a `bookTicker` must not parse as one.
#[derive(Debug, Deserialize)]
struct BinanceSpotPartialDepthPayload {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct BinanceSpotBookTickerMsg {
    #[serde(rename = "e", default)]
    event_type: Option<String>,
    #[serde(rename = "E", default)]
    event_time: Option<i64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "u")]
    book_update_id: u64,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
    #[serde(rename = "T", default)]
    transaction_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BinanceSpotTradeMsg {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceWsResponse {
    result: Option<Value>,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct BinanceWsErrorResponse {
    code: i32,
    msg: String,
    id: Option<u64>,
}

#[test]
fn the_spot_client_parses_the_golden_book_ticker() {
    let w: BinanceCombinedStreamEvent = serde_json::from_str(SPOT_BOOK_TICKER).unwrap();
    assert!(serde_json::from_value::<BinanceSpotPartialDepthPayload>(w.data.clone()).is_err());
    assert!(w.stream.ends_with("@bookTicker"), "how it recognizes one");
    let m: BinanceSpotBookTickerMsg = serde_json::from_value(w.data).unwrap();
    assert_eq!(m.symbol, "BTCUSDT");
    assert_eq!(
        m.transaction_time,
        Some(1_700_000_000_123),
        "read before the receive time"
    );
    assert_eq!(m.best_bid_price, "82783.38000000");
}

#[test]
fn the_spot_client_parses_the_golden_trade() {
    let w: BinanceCombinedStreamEvent = serde_json::from_str(SPOT_TRADE).unwrap();
    assert!(serde_json::from_value::<BinanceSpotPartialDepthPayload>(w.data.clone()).is_err());
    assert_eq!(w.data["e"], "trade", "the event type it dispatches on");
    let m: BinanceSpotTradeMsg = serde_json::from_value(w.data).unwrap();
    assert_eq!(m.trade_id, 6_778_484_593);
    assert!(!m.is_buyer_maker, "a buy aggressor");
}

/// The spot handler checks `code` first, then `result`; an error without an `id` marks nothing failed
/// and is logged.
#[test]
fn the_spot_client_reads_the_ack_and_the_refusal() {
    let ack: Value = serde_json::from_str(ACK).unwrap();
    assert!(ack.get("code").is_none() && ack.get("result").is_some());
    assert!(serde_json::from_str::<BinanceWsResponse>(ACK)
        .unwrap()
        .result
        .is_none());
    let e: BinanceWsErrorResponse = serde_json::from_str(REFUSAL).unwrap();
    assert_eq!((e.code, e.id), (2, None));
}

// --- the Python client (msgspec structs, mirrored; `Py` where a name clashes with the Rust client's) ---

/// `BinanceDataMsgWrapper`: a frame without a `stream` is a control response and is skipped.
#[derive(Debug, Deserialize)]
struct BinanceDataMsgWrapper {
    #[serde(default)]
    stream: Option<String>,
    #[serde(default)]
    id: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct BinanceQuoteData {
    s: String,
    u: i64,
    b: String,
    B: String,
    a: String,
    A: String,
    #[serde(default)]
    T: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BinanceQuoteMsg {
    stream: String,
    data: BinanceQuoteData,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct BinanceAggregatedTradeData {
    e: String,
    E: i64,
    s: String,
    a: i64,
    p: String,
    q: String,
    f: i64,
    l: i64,
    T: i64,
    m: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceAggregatedTradeMsg {
    stream: String,
    data: BinanceAggregatedTradeData,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct BinanceSpotTradeData {
    e: String,
    E: i64,
    s: String,
    t: i64,
    p: String,
    q: String,
    T: i64,
    m: bool,
}

#[derive(Debug, Deserialize)]
struct PyBinanceSpotTradeMsg {
    stream: String,
    data: BinanceSpotTradeData,
}

/// The Python client's handlers, matched by substring of `stream` exactly as `data.py` does.
fn python_handlers(stream: &str) -> Vec<&'static str> {
    ["@bookTicker", "@trade", "@aggTrade"]
        .into_iter()
        .filter(|h| stream.contains(h))
        .collect()
}

/// `Price.from_str` takes a value's precision from its decimals, and `QuoteTick` refuses a bid and
/// ask of different precisions.
fn decimals(s: &str) -> usize {
    s.split_once('.').map_or(0, |(_, f)| f.len())
}

/// The Python client reads futures through `/stream` too, so it sees the bare frame wrapped.
fn wrapped(stream: &str, raw: &str) -> String {
    format!(r#"{{"stream":"{stream}","data":{}}}"#, raw.trim_end())
}

#[test]
fn the_python_client_parses_the_golden_book_tickers() {
    for frame in [
        wrapped("btcusdt@bookTicker", USDSM_BOOK_TICKER),
        SPOT_BOOK_TICKER.trim_end().to_string(),
    ] {
        let m: BinanceQuoteMsg = serde_json::from_str(&frame).unwrap();
        assert_eq!(python_handlers(&m.stream), ["@bookTicker"]);
        assert_eq!(decimals(&m.data.b), decimals(&m.data.a), "{frame}");
        assert_eq!(decimals(&m.data.B), decimals(&m.data.A), "{frame}");
        assert_eq!(m.data.T, Some(1_700_000_000_123));
    }
}

#[test]
fn the_python_client_parses_the_golden_trades() {
    let agg = wrapped("btcusdt@aggTrade", USDSM_AGG_TRADE);
    let m: BinanceAggregatedTradeMsg = serde_json::from_str(&agg).unwrap();
    assert_eq!(
        python_handlers(&m.stream),
        ["@aggTrade"],
        "never also @trade"
    );
    assert_eq!(m.data.a, 6_778_484_593);

    let m: PyBinanceSpotTradeMsg = serde_json::from_str(SPOT_TRADE.trim_end()).unwrap();
    assert_eq!(python_handlers(&m.stream), ["@trade"]);
    assert_eq!(m.data.t, 6_778_484_593);
}

#[test]
fn the_python_client_skips_the_ack_and_the_refusal() {
    for frame in [ACK, REFUSAL] {
        let w: BinanceDataMsgWrapper = serde_json::from_str(frame).unwrap();
        assert!(w.stream.is_none(), "{frame}");
    }
}
