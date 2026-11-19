//! Binance-compatible WebSocket sink: re-serve Binance's top-of-book quotes and trades in **Binance's
//! own market-stream schema**, so a Binance client consumes edge-connect by pointing its WebSocket
//! base URL here. Off by default (`--binance-ws-bind`).
//!
//! A rendering, not a second pipeline, exactly as [`super::hyperliquid`] is: it holds no ingest state
//! and nothing about it belongs in PROTOCOL.md. NautilusTrader **v1.231.0**'s Binance adapter is the
//! compatibility target: its Rust data clients (`crates/adapters/binance`) and the Python one a Python
//! `TradingNode` runs (`nautilus_trader/adapters/binance`).
//!
//! The URL path picks the matching engine: `/usdsm` serves USD-margined perpetuals (Source ID 6) and
//! `/spot` serves spot (Source ID 8). Both list `BTCUSDT`, so there is no default.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::{
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
        Message as WsMessage,
    },
    WebSocketStream,
};
use tracing::{info, warn};

use super::hyperliquid::{
    finish_close, inbound_allowed, inbound_limits, json, ms_or_now, ClientGuard,
    CLOSE_HANDSHAKE_TIMEOUT, HANDSHAKE_TIMEOUT, HEARTBEAT, IDLE_TIMEOUT, MAX_CLIENTS,
};
use crate::{
    ingest::sources::{BINANCE_SPOT_SOURCE_ID, BINANCE_USDSM_SOURCE_ID},
    metrics::metrics,
    model::{
        FeedMessage, InstrumentSnapshot, NormalizedQuote, NormalizedTrade, QuoteWire, Side,
        SourceKey,
    },
};

/// Binance's own per-connection stream limit.
const MAX_STREAMS: usize = 1024;

/// Longest symbol accepted in a stream name, which is retained per subscription.
const MAX_SYMBOL_LEN: usize = 32;

/// The exponent a value renders at when its instrument's is unknown: Binance's finest precision.
const FALLBACK_EXPONENT: i8 = -8;

const NOT_FOUND: &str = "not found: connect to /usdsm (Binance USD-M perpetuals, Source ID 6) or \
                         /spot (Binance spot, Source ID 8)\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    UsdM,
    Spot,
}

impl Engine {
    fn source_id(self) -> u16 {
        match self {
            Self::UsdM => BINANCE_USDSM_SOURCE_ID,
            Self::Spot => BINANCE_SPOT_SOURCE_ID,
        }
    }
}

/// Binance's two endpoints: `/ws` sends each event bare and `/stream` wraps it as
/// `{"stream":…,"data":…}`. Nautilus's futures client connects to the base URL as given and reads
/// the bare form; its spot and Python clients append `/stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    Raw,
    Combined,
}

#[derive(Debug, PartialEq, Eq)]
struct Route {
    engine: Engine,
    framing: Framing,
    /// Streams named in the URL (`?streams=a/b`), subscribed without a `SUBSCRIBE`.
    streams: Vec<String>,
}

/// Resolve a handshake's path and query, or the status and body it is refused with.
///
/// A stream this sink cannot serve is refused in-band rather than here: Nautilus's Python client
/// names its first stream in the URL, and a failed handshake leaves that client waiting for good.
fn route(path: &str, query: Option<&str>) -> Result<Route, (StatusCode, &'static str)> {
    let path = path.trim_end_matches('/');
    let (engine, endpoint) = if let Some(rest) = path.strip_prefix("/usdsm") {
        (Engine::UsdM, rest)
    } else if let Some(rest) = path.strip_prefix("/spot") {
        (Engine::Spot, rest)
    } else {
        return Err((StatusCode::NOT_FOUND, NOT_FOUND));
    };
    let framing = match endpoint {
        "" | "/ws" => Framing::Raw,
        "/stream" => Framing::Combined,
        _ => return Err((StatusCode::NOT_FOUND, NOT_FOUND)),
    };
    let mut streams = Vec::new();
    for param in query
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
    {
        // Binance also takes `timeUnit`; ignoring it would serve milliseconds to a client expecting
        // microseconds.
        let Some(("streams", names)) = param.split_once('=') else {
            return Err((
                StatusCode::BAD_REQUEST,
                "unsupported query parameter: only streams= is supported\n",
            ));
        };
        streams.extend(
            names
                .split('/')
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
    }
    Ok(Route {
        engine,
        framing,
        streams,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    BookTicker,
    AggTrade,
    Trade,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::BookTicker => "bookTicker",
            Self::AggTrade => "aggTrade",
            Self::Trade => "trade",
        }
    }
}

/// `<symbol>@<kind>` as our upper-case symbol and the kind, or `None` for a stream this sink cannot
/// serve: depth, mark price, klines, tickers and the all-market streams among them.
fn parse_stream(stream: &str) -> Option<(String, Kind)> {
    let (symbol, kind) = stream.split_once('@')?;
    let kind = match kind {
        "bookTicker" => Kind::BookTicker,
        "aggTrade" => Kind::AggTrade,
        "trade" => Kind::Trade,
        _ => return None,
    };
    (!symbol.is_empty() && symbol.len() <= MAX_SYMBOL_LEN).then(|| (symbol.to_uppercase(), kind))
}

/// One connection's streams: the symbols subscribed for each kind.
#[derive(Default)]
struct Subs([HashSet<String>; 3]);

impl Subs {
    fn has(&self, kind: Kind, symbol: &str) -> bool {
        self.0[kind as usize].contains(symbol)
    }

    fn len(&self) -> usize {
        self.0.iter().map(HashSet::len).sum()
    }

    /// Add or remove each named stream, returning the ones a subscribe asked for and this sink
    /// cannot serve. Removing a stream it never served is a no-op.
    fn apply<'a>(
        &mut self,
        streams: impl IntoIterator<Item = &'a Value>,
        add: bool,
    ) -> Vec<String> {
        let mut refused = Vec::new();
        for s in streams {
            match s.as_str().and_then(parse_stream) {
                Some((symbol, kind)) if !add => {
                    self.0[kind as usize].remove(&symbol);
                }
                Some((symbol, kind)) if self.has(kind, &symbol) || self.len() < MAX_STREAMS => {
                    self.0[kind as usize].insert(symbol);
                }
                _ if !add => {}
                _ => refused.push(s.as_str().map_or_else(|| s.to_string(), str::to_string)),
            }
        }
        refused
    }
}

// --- control ---

fn text(s: String) -> WsMessage {
    WsMessage::Text(s.into())
}

/// Binance's acknowledgement: `{"result":null,"id":<the request's id>}`.
fn ack(id: Value) -> WsMessage {
    #[derive(Serialize)]
    struct Ack {
        result: Option<()>,
        id: Value,
    }
    text(json(Ack { result: None, id }))
}

/// Binance's error shape, without the `id` Binance would echo: Nautilus's futures client reads any
/// frame with an `id` as that request's acknowledgement, and one without `result` as a success, so an
/// error naming the request would mark the refused streams subscribed.
fn refusal(code: i64, msg: &str) -> WsMessage {
    #[derive(Serialize)]
    struct Refusal<'a> {
        code: i64,
        msg: &'a str,
    }
    text(json(Refusal { code, msg }))
}

fn refused_streams(refused: &[String]) -> WsMessage {
    let msg = format!(
        "Invalid request: this sink serves <symbol>@bookTicker, <symbol>@aggTrade and \
         <symbol>@trade, at most {MAX_STREAMS} per connection; refused: {}",
        refused.join(", ")
    );
    refusal(2, &msg)
}

/// Apply one inbound control frame, returning the frames that answer it. A request is acknowledged
/// unless every stream it named was refused.
fn control(frame: &str, subs: &mut Subs) -> Vec<WsMessage> {
    let Ok(request) = serde_json::from_str::<Value>(frame) else {
        return vec![refusal(3, "Invalid JSON")];
    };
    let add = match request.get("method").and_then(Value::as_str) {
        Some("SUBSCRIBE") => true,
        Some("UNSUBSCRIBE") => false,
        _ => {
            return vec![refusal(
                2,
                "Invalid request: this sink supports SUBSCRIBE and UNSUBSCRIBE only",
            )]
        }
    };
    let params = match request.get("params") {
        Some(Value::Array(params)) => params.as_slice(),
        None => &[],
        Some(_) => return vec![refusal(2, "Invalid request: params must be an array")],
    };
    let refused = subs.apply(params, add);
    let mut out = Vec::new();
    if params.is_empty() || refused.len() < params.len() {
        out.push(ack(request.get("id").cloned().unwrap_or_default()));
    }
    if !refused.is_empty() {
        out.push(refused_streams(&refused));
    }
    out
}

// --- the wire shapes ---

/// At the instrument's declared precision, trailing zeros kept, as Binance formats it. Nautilus's
/// Python client takes precision from the string: its `QuoteTick` refuses a bid and ask of different
/// precision, and its risk engine an order more precise than the instrument.
fn fixed(v: f64, exponent: i8) -> String {
    format!("{v:.*}", usize::from(exponent.min(0).unsigned_abs()))
}

/// The quote's wire exponents, or the fallback for a quote not decoded from top-of-book (no Binance
/// quote today).
fn quote_exponents(q: &NormalizedQuote) -> (i8, i8) {
    if q.wire == QuoteWire::default() {
        (FALLBACK_EXPONENT, FALLBACK_EXPONENT)
    } else {
        (q.wire.price_exponent, q.wire.qty_exponent)
    }
}

/// A trade carries no exponents of its own, so they come from its instrument's catalog entry.
fn trade_exponents(instruments: &InstrumentSnapshot, t: &NormalizedTrade) -> (i8, i8) {
    let key = (
        SourceKey::of(t),
        t.category.clone(),
        t.channel,
        t.instrument_id,
    );
    crate::model::lock(instruments)
        .get(&key)
        .map_or((FALLBACK_EXPONENT, FALLBACK_EXPONENT), |i| {
            (i.price_exponent, i.qty_exponent)
        })
}

#[derive(Serialize)]
struct BookTicker<'a> {
    /// Futures only: Binance's spot `bookTicker` names neither its event type nor its event time.
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<&'static str>,
    /// Binance's book update id, a placeholder `0`: the feed carries only its low 32 bits, and
    /// Nautilus reads a sequence of `0` as none.
    u: u64,
    #[serde(rename = "E", skip_serializing_if = "Option::is_none")]
    event_time: Option<u64>,
    /// Absent from Binance's spot frame; Nautilus's spot parsers read it when present.
    #[serde(rename = "T")]
    transaction_time: u64,
    s: &'a str,
    b: String,
    #[serde(rename = "B")]
    bid_qty: String,
    a: String,
    #[serde(rename = "A")]
    ask_qty: String,
}

/// Each frame is one execution, so all three ids are the venue's raw trade id.
#[derive(Serialize)]
struct AggTrade<'a> {
    e: &'static str,
    #[serde(rename = "E")]
    event_time: u64,
    s: &'a str,
    a: u64,
    p: String,
    q: String,
    f: u64,
    l: u64,
    #[serde(rename = "T")]
    trade_time: u64,
    m: bool,
}

#[derive(Serialize)]
struct Trade<'a> {
    e: &'static str,
    #[serde(rename = "E")]
    event_time: u64,
    s: &'a str,
    t: u64,
    p: String,
    q: String,
    #[serde(rename = "T")]
    trade_time: u64,
    m: bool,
}

#[derive(Serialize)]
struct Combined<'a, T> {
    stream: String,
    data: &'a T,
}

fn framed<T: Serialize>(framing: Framing, symbol: &str, kind: Kind, data: &T) -> String {
    match framing {
        Framing::Raw => json(data),
        Framing::Combined => json(Combined {
            stream: format!("{}@{}", symbol.to_lowercase(), kind.name()),
            data,
        }),
    }
}

/// Venue time in both `E` and `T`: the feed carries one timestamp.
fn book_ticker(q: &NormalizedQuote, engine: Engine, framing: Framing) -> String {
    let (px, qty) = quote_exponents(q);
    let time = ms_or_now(q.source_ts_ns);
    let futures = engine == Engine::UsdM;
    let data = BookTicker {
        e: futures.then_some("bookTicker"),
        u: 0,
        event_time: futures.then_some(time),
        transaction_time: time,
        s: &q.symbol,
        b: fixed(q.bid, px),
        bid_qty: fixed(q.bid_size, qty),
        a: fixed(q.ask, px),
        ask_qty: fixed(q.ask_size, qty),
    };
    framed(framing, &q.symbol, Kind::BookTicker, &data)
}

fn trade(
    t: &NormalizedTrade,
    kind: Kind,
    buyer_is_maker: bool,
    (px, qty): (i8, i8),
    framing: Framing,
) -> String {
    let time = ms_or_now(t.source_ts_ns);
    let (p, q) = (fixed(t.price, px), fixed(t.size, qty));
    if kind == Kind::AggTrade {
        let data = AggTrade {
            e: "aggTrade",
            event_time: time,
            s: &t.symbol,
            a: t.trade_id,
            p,
            q,
            f: t.trade_id,
            l: t.trade_id,
            trade_time: time,
            m: buyer_is_maker,
        };
        framed(framing, &t.symbol, kind, &data)
    } else {
        let data = Trade {
            e: "trade",
            event_time: time,
            s: &t.symbol,
            t: t.trade_id,
            p,
            q,
            trade_time: time,
            m: buyer_is_maker,
        };
        framed(framing, &t.symbol, kind, &data)
    }
}

/// Every frame one broadcast message produces for this connection. Another Source ID is another
/// engine, never rendered here even under the same symbol.
fn frames(
    msg: &FeedMessage,
    engine: Engine,
    framing: Framing,
    subs: &Subs,
    instruments: &InstrumentSnapshot,
) -> Vec<(Kind, String)> {
    match msg {
        FeedMessage::Quote(q)
            if q.source_id == engine.source_id() && subs.has(Kind::BookTicker, &q.symbol) =>
        {
            vec![(Kind::BookTicker, book_ticker(q, engine, framing))]
        }
        FeedMessage::Trade(t) if t.source_id == engine.source_id() => {
            let kinds: Vec<Kind> = [Kind::AggTrade, Kind::Trade]
                .into_iter()
                .filter(|k| subs.has(*k, &t.symbol))
                .collect();
            if kinds.is_empty() {
                return Vec::new();
            }
            // `m` is the one field a consumer reads directionally, and Binance's schema has no
            // "unknown", so such a print is dropped rather than guessed.
            let buyer_is_maker = match t.aggressor_side {
                Side::Buy => false,
                Side::Sell => true,
                Side::Unknown => {
                    metrics()
                        .binance_sink_dropped
                        .with_label_values(&["unknown_aggressor"])
                        .inc();
                    return Vec::new();
                }
            };
            let exponents = trade_exponents(instruments, t);
            kinds
                .into_iter()
                .map(|k| (k, trade(t, k, buyer_is_maker, exponents, framing)))
                .collect()
        }
        _ => Vec::new(),
    }
}

// --- the server ---

/// Bound up front so a taken port disables this sink, never the process and its tunnel.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "Binance-compatible WebSocket sink listening");
    Ok(listener)
}

/// Never returns an accept error: `main`'s `select!` would exit the process over it.
pub async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<Arc<FeedMessage>>,
    instruments: InstrumentSnapshot,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!("binance sink accept failed: {e}");
                continue;
            }
        };
        if clients.fetch_add(1, Ordering::SeqCst) >= MAX_CLIENTS {
            clients.fetch_sub(1, Ordering::SeqCst);
            warn!(%peer, max = MAX_CLIENTS, "binance sink at max clients; rejecting connection");
            continue;
        }
        metrics().binance_sink_clients.inc();
        let guard = ClientGuard {
            clients: clients.clone(),
            gauge: metrics().binance_sink_clients.clone(),
        };
        let rx = tx.subscribe();
        let instruments = instruments.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, rx, instruments).await {
                warn!(%peer, "binance sink client ended: {e}");
            }
        });
    }
}

fn refuse_handshake(status: StatusCode, body: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(body.to_string()));
    *response.status_mut() = status;
    response
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback fixes the error type"
)]
async fn accept(stream: TcpStream) -> Result<(WebSocketStream<TcpStream>, Route)> {
    let mut resolved = None;
    let ws = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async_with_config(
            stream,
            |request: &Request, response: Response| match route(
                request.uri().path(),
                request.uri().query(),
            ) {
                Ok(r) => {
                    resolved = Some(r);
                    Ok(response)
                }
                Err((status, body)) => Err(refuse_handshake(status, body)),
            },
            Some(inbound_limits()),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("handshake timed out"))??;
    let route = resolved.ok_or_else(|| anyhow::anyhow!("handshake accepted without a route"))?;
    Ok((ws, route))
}

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<Arc<FeedMessage>>,
    instruments: InstrumentSnapshot,
) -> Result<()> {
    let (
        ws,
        Route {
            engine,
            framing,
            streams,
        },
    ) = accept(stream).await?;
    let (mut write, mut read) = ws.split();
    let mut subs = Subs::default();
    let named: Vec<Value> = streams.into_iter().map(Value::String).collect();
    let refused = subs.apply(&named, true);
    if !refused.is_empty() {
        write.send(refused_streams(&refused)).await?;
    }
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval(HEARTBEAT);
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Text(frame))) => {
                    last_seen = Instant::now();
                    if !inbound_allowed(&mut win_start, &mut win_count) {
                        return end_rate_limited(&mut write, &mut read).await;
                    }
                    for answer in control(&frame, &mut subs) {
                        write.send(answer).await?;
                    }
                }
                // Charged like a control frame: it costs an outbound `Pong`.
                Some(Ok(WsMessage::Ping(p))) => {
                    last_seen = Instant::now();
                    if !inbound_allowed(&mut win_start, &mut win_count) {
                        return end_rate_limited(&mut write, &mut read).await;
                    }
                    write.send(WsMessage::Pong(p)).await?;
                }
                Some(Ok(WsMessage::Pong(_))) => last_seen = Instant::now(),
                Some(Ok(WsMessage::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
            },

            _ = hb.tick() => {
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    let _ = write.send(WsMessage::Close(None)).await;
                    break;
                }
                write.send(WsMessage::Ping(Vec::new().into())).await?;
            },

            msg = rx.recv() => match msg {
                Ok(m) => {
                    for (kind, frame) in frames(&m, engine, framing, &subs, &instruments) {
                        metrics().binance_sink_messages.with_label_values(&[kind.name()]).inc();
                        write.send(text(frame)).await?;
                    }
                }
                // Every frame here is a whole quote or a single print, so a gap costs the prints
                // it held and nothing after it: counted, not disconnected.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("binance sink client lagged, dropped {n}");
                    metrics().binance_sink_dropped.with_label_values(&["client_lagged"]).inc();
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}

/// End a connection that crossed the inbound cap, sending the reason before the `Close` and then
/// waiting for the peer's, so the reason is not lost to a reset (see [`finish_close`]).
async fn end_rate_limited<S, R, E>(write: &mut S, read: &mut R) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
    R: StreamExt<Item = std::result::Result<WsMessage, E>> + Unpin,
{
    write
        .send(refusal(
            -1003,
            "Too many requests: inbound rate limit exceeded",
        ))
        .await?;
    write.send(WsMessage::Close(None)).await?;
    finish_close(read, CLOSE_HANDSHAKE_TIMEOUT).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{net::TcpListener, sync::broadcast, time::timeout};
    use tokio_tungstenite::tungstenite::Error as WsError;

    use super::*;
    use crate::model::NormalizedInstrument;

    const VENUE: &str = "BINANCE";
    /// 2023-11-14T22:13:20.123Z, so `E`/`T` carry a millisecond fraction.
    const TS_NS: u64 = 1_700_000_000_123_000_000;
    const TS_MS: u64 = 1_700_000_000_123;
    const TRADE_ID: u64 = 6_778_484_593;

    /// A quote as the top-of-book decoder emits it: `(price, qty)` per side, with the wire's
    /// exponents.
    fn quote(
        source_id: u16,
        symbol: &str,
        bid: (f64, f64),
        ask: (f64, f64),
        (price_exponent, qty_exponent): (i8, i8),
    ) -> NormalizedQuote {
        NormalizedQuote {
            venue: VENUE.into(),
            source_name: VENUE.into(),
            source_id,
            symbol: symbol.into(),
            bid: bid.0,
            bid_size: bid.1,
            bid_n: 0,
            ask: ask.0,
            ask_size: ask.1,
            ask_n: 0,
            source_ts_ns: TS_NS,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            channel: 0,
            instrument_id: 7,
            category: "usdsm".into(),
            venue_update_id: 0,
            wire: QuoteWire {
                channel: 0,
                bid_price: 1,
                bid_qty: 1,
                ask_price: 1,
                ask_qty: 1,
                price_exponent,
                qty_exponent,
            },
        }
    }

    fn usdsm_quote() -> NormalizedQuote {
        quote(6, "BTCUSDT", (61825.4, 5.512), (61825.5, 9.879), (-2, -3))
    }

    fn spot_quote() -> NormalizedQuote {
        let mut q = quote(8, "BTCUSDT", (82783.38, 0.00008), (82783.39, 1.5), (-8, -8));
        q.category = "spot".into();
        q
    }

    fn print(source_id: u16, side: Side) -> NormalizedTrade {
        NormalizedTrade {
            venue: VENUE.into(),
            source_name: VENUE.into(),
            source_id,
            symbol: "BTCUSDT".into(),
            channel: 0,
            instrument_id: 7,
            category: "usdsm".into(),
            price: 61825.5,
            size: 0.002,
            aggressor_side: side,
            trade_id: TRADE_ID,
            cumulative_volume: 0.0,
            source_ts_ns: TS_NS,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// The catalog holding the definition [`print`]'s trade was scaled with.
    fn catalog(price_exponent: i8, qty_exponent: i8) -> InstrumentSnapshot {
        let inst = NormalizedInstrument {
            venue: VENUE.into(),
            source_name: VENUE.into(),
            source_id: 6,
            symbol: "BTCUSDT".into(),
            channel: 0,
            instrument_id: 7,
            category: "usdsm".into(),
            price_exponent,
            qty_exponent,
            tick_size: 0,
        };
        let key = (
            SourceKey::of(&inst),
            inst.category.clone(),
            inst.channel,
            inst.instrument_id,
        );
        Arc::new(Mutex::new(HashMap::from([(key, inst)])))
    }

    fn subs(streams: &[&str]) -> Subs {
        let mut s = Subs::default();
        let named: Vec<Value> = streams.iter().map(|n| Value::from(*n)).collect();
        assert!(
            s.apply(&named, true).is_empty(),
            "fixture streams are served"
        );
        s
    }

    fn parse(frame: &str) -> Value {
        serde_json::from_str(frame).unwrap()
    }

    fn text_of(m: &WsMessage) -> Value {
        let WsMessage::Text(t) = m else {
            panic!("a text frame, got {m:?}")
        };
        parse(t)
    }

    // --- routing ---

    /// The path names the engine however a Nautilus client builds it.
    #[test]
    fn the_path_names_the_engine_and_the_framing() {
        for (path, engine, framing) in [
            ("/usdsm", Engine::UsdM, Framing::Raw),
            ("/usdsm/", Engine::UsdM, Framing::Raw),
            ("/usdsm/ws", Engine::UsdM, Framing::Raw),
            ("/usdsm/stream", Engine::UsdM, Framing::Combined),
            ("/spot", Engine::Spot, Framing::Raw),
            ("/spot/ws", Engine::Spot, Framing::Raw),
            ("/spot/stream/", Engine::Spot, Framing::Combined),
        ] {
            assert_eq!(
                route(path, None),
                Ok(Route {
                    engine,
                    framing,
                    streams: vec![]
                }),
                "{path}"
            );
        }
    }

    /// Both engines list `BTCUSDT`, so a path that names neither is refused rather than defaulted.
    #[test]
    fn a_path_naming_no_engine_is_not_found() {
        for path in [
            "/",
            "",
            "/ws",
            "/stream",
            "/usdsmx",
            "/spot/ws/btcusdt@trade",
            "/coinm",
        ] {
            let Err((status, body)) = route(path, None) else {
                panic!("{path} must be refused")
            };
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert!(body.contains("/usdsm") && body.contains("/spot"), "{body}");
        }
    }

    /// The Python client's `<base>/stream?streams=<first>`, and Binance's `/`-separated list.
    #[test]
    fn streams_named_in_the_url_are_subscribed() {
        let r = route(
            "/spot/stream",
            Some("streams=btcusdt@bookTicker/ethusdt@trade"),
        )
        .unwrap();
        assert_eq!(r.streams, ["btcusdt@bookTicker", "ethusdt@trade"]);
    }

    #[test]
    fn any_other_query_parameter_is_refused() {
        let Err((status, _)) = route("/spot/stream", Some("timeUnit=MICROSECOND")) else {
            panic!("a time unit this sink does not serve must be refused")
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // --- streams and control ---

    #[test]
    fn served_streams_parse_to_our_symbol() {
        assert_eq!(
            parse_stream("btcusdt@bookTicker"),
            Some(("BTCUSDT".into(), Kind::BookTicker))
        );
        assert_eq!(
            parse_stream("ethusdt@aggTrade"),
            Some(("ETHUSDT".into(), Kind::AggTrade))
        );
        assert_eq!(
            parse_stream("solusdt@trade"),
            Some(("SOLUSDT".into(), Kind::Trade))
        );
    }

    #[test]
    fn streams_this_sink_cannot_serve_are_refused() {
        for s in [
            "btcusdt@depth@0ms",
            "btcusdt@depth20",
            "btcusdt@markPrice@1s",
            "btcusdt@kline_1m",
            "btcusdt@ticker",
            "!bookTicker",
            "!markPrice@arr",
            "@trade",
            "btcusdt",
        ] {
            assert_eq!(parse_stream(s), None, "{s}");
        }
        assert_eq!(parse_stream(&format!("{}@trade", "x".repeat(33))), None);
    }

    /// Nautilus correlates the acknowledgement by `id`, which its Python client starts at `0`.
    #[test]
    fn a_subscribe_is_acknowledged_with_its_own_id() {
        for id in [Value::from(0), Value::from(42), Value::from("abc")] {
            let mut s = Subs::default();
            let frame =
                serde_json::json!({"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":id});
            let out = control(&frame.to_string(), &mut s);
            assert_eq!(out.len(), 1);
            assert_eq!(
                text_of(&out[0]),
                serde_json::json!({"result": null, "id": id})
            );
            assert!(s.has(Kind::BookTicker, "BTCUSDT"));
        }
    }

    /// The refusal names the streams and carries no `id` (see [`refusal`]).
    #[test]
    fn a_partly_refused_subscribe_is_acknowledged_and_names_the_refused_streams() {
        let mut s = Subs::default();
        let out = control(
            r#"{"method":"SUBSCRIBE","params":["btcusdt@aggTrade","btcusdt@depth@0ms"],"id":3}"#,
            &mut s,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(
            text_of(&out[0]),
            serde_json::json!({"result": null, "id": 3})
        );
        assert_eq!(
            text_of(&out[1]),
            serde_json::json!({
                "code": 2,
                "msg": "Invalid request: this sink serves <symbol>@bookTicker, <symbol>@aggTrade \
                        and <symbol>@trade, at most 1024 per connection; refused: btcusdt@depth@0ms"
            }),
            "no id: tests/binance_sink_shapes.rs parses this exact frame"
        );
        assert!(s.has(Kind::AggTrade, "BTCUSDT"));
    }

    /// With nothing accepted there is nothing to acknowledge.
    #[test]
    fn a_wholly_refused_subscribe_is_not_acknowledged() {
        let mut s = Subs::default();
        let out = control(
            r#"{"method":"SUBSCRIBE","params":["btcusdt@markPrice@1s"],"id":4}"#,
            &mut s,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(text_of(&out[0])["code"], 2);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn an_unsubscribe_removes_the_stream() {
        let mut s = subs(&["btcusdt@bookTicker", "btcusdt@trade"]);
        let out = control(
            r#"{"method":"UNSUBSCRIBE","params":["btcusdt@bookTicker"],"id":5}"#,
            &mut s,
        );
        assert_eq!(
            text_of(&out[0]),
            serde_json::json!({"result": null, "id": 5})
        );
        assert!(!s.has(Kind::BookTicker, "BTCUSDT") && s.has(Kind::Trade, "BTCUSDT"));
    }

    #[test]
    fn other_methods_and_malformed_frames_are_refused() {
        let mut s = Subs::default();
        for (frame, code) in [
            (r#"{"method":"LIST_SUBSCRIPTIONS","id":1}"#, 2),
            (
                r#"{"method":"SET_PROPERTY","params":["combined",true],"id":2}"#,
                2,
            ),
            (
                r#"{"method":"SUBSCRIBE","params":"btcusdt@trade","id":3}"#,
                2,
            ),
            ("not json", 3),
        ] {
            let out = control(frame, &mut s);
            assert_eq!(out.len(), 1, "{frame}");
            assert_eq!(text_of(&out[0])["code"], code, "{frame}");
        }
    }

    /// Binance's own per-connection limit. A stream already held does not count twice.
    #[test]
    fn streams_past_the_per_connection_limit_are_refused() {
        let names: Vec<Value> = (0..MAX_STREAMS)
            .map(|i| Value::from(format!("s{i}@trade")))
            .collect();
        let mut s = Subs::default();
        assert!(s.apply(&names, true).is_empty());
        assert_eq!(
            s.apply(&[Value::from("s0@trade"), Value::from("extra@trade")], true),
            ["extra@trade"]
        );
        assert_eq!(s.len(), MAX_STREAMS);
    }

    /// Nautilus's Rust clients unsubscribe their refused `@depth` stream on stop, which must not
    /// draw a second refusal.
    #[test]
    fn an_unsubscribe_of_an_unserved_stream_is_acknowledged() {
        let mut s = subs(&["btcusdt@bookTicker"]);
        let out = control(
            r#"{"method":"UNSUBSCRIBE","params":["btcusdt@depth@0ms"],"id":6}"#,
            &mut s,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            text_of(&out[0]),
            serde_json::json!({"result": null, "id": 6})
        );
        assert!(s.has(Kind::BookTicker, "BTCUSDT"));
    }

    // --- frames ---

    /// Every field Nautilus's futures parser requires, at the instrument's precision.
    #[test]
    fn a_futures_book_ticker_carries_every_field_nautilus_requires() {
        let v = parse(&book_ticker(&usdsm_quote(), Engine::UsdM, Framing::Raw));
        assert_eq!(
            v,
            serde_json::json!({
                "e": "bookTicker", "u": 0, "E": TS_MS, "T": TS_MS, "s": "BTCUSDT",
                "b": "61825.40", "B": "5.512", "a": "61825.50", "A": "9.879"
            })
        );
    }

    /// Binance's spot frame names neither its event type nor its event time.
    #[test]
    fn a_spot_book_ticker_is_binances_spot_shape() {
        let v = parse(&book_ticker(&spot_quote(), Engine::Spot, Framing::Raw));
        assert_eq!(
            v,
            serde_json::json!({
                "u": 0, "T": TS_MS, "s": "BTCUSDT",
                "b": "82783.38000000", "B": "0.00008000", "a": "82783.39000000", "A": "1.50000000"
            })
        );
    }

    /// A trimmed `100.5` beside `101` would fail Nautilus's Python `QuoteTick` (see [`fixed`]).
    #[test]
    fn bid_and_ask_share_one_precision() {
        let q = quote(6, "BTCUSDT", (100.5, 1.0), (101.0, 2.25), (-1, -2));
        let v = parse(&book_ticker(&q, Engine::UsdM, Framing::Raw));
        assert_eq!(
            (v["b"].as_str(), v["a"].as_str()),
            (Some("100.5"), Some("101.0"))
        );
        assert_eq!(
            (v["B"].as_str(), v["A"].as_str()),
            (Some("1.00"), Some("2.25"))
        );
    }

    #[test]
    fn a_non_negative_exponent_renders_whole_numbers() {
        assert_eq!(fixed(1200.0, 2), "1200");
        assert_eq!(fixed(5.0, 0), "5");
        assert_eq!(fixed(0.0, -3), "0.000");
    }

    /// A quote with no wire values renders at Binance's finest precision rather than as integers.
    #[test]
    fn a_quote_without_wire_values_renders_at_eight_decimals() {
        let mut q = usdsm_quote();
        q.wire = QuoteWire::default();
        let v = parse(&book_ticker(&q, Engine::UsdM, Framing::Raw));
        assert_eq!(v["b"], "61825.40000000");
    }

    /// The feed's `0` means no venue time, which Nautilus would date to 1970.
    #[test]
    fn a_missing_venue_time_becomes_the_send_time() {
        let mut q = usdsm_quote();
        q.source_ts_ns = 0;
        let v = parse(&book_ticker(&q, Engine::UsdM, Framing::Raw));
        assert!(v["T"].as_u64().unwrap() > TS_MS);
    }

    /// One execution per frame; `m` is true when the seller was the aggressor.
    #[test]
    fn an_agg_trade_is_one_execution() {
        let v = parse(&trade(
            &print(6, Side::Sell),
            Kind::AggTrade,
            true,
            (-2, -3),
            Framing::Raw,
        ));
        assert_eq!(
            v,
            serde_json::json!({
                "e": "aggTrade", "E": TS_MS, "s": "BTCUSDT", "a": TRADE_ID, "p": "61825.50",
                "q": "0.002", "f": TRADE_ID, "l": TRADE_ID, "T": TS_MS, "m": true
            })
        );
    }

    #[test]
    fn a_trade_carries_the_raw_trade_id() {
        let v = parse(&trade(
            &print(8, Side::Buy),
            Kind::Trade,
            false,
            (-8, -8),
            Framing::Raw,
        ));
        assert_eq!(
            v,
            serde_json::json!({
                "e": "trade", "E": TS_MS, "s": "BTCUSDT", "t": TRADE_ID,
                "p": "61825.50000000", "q": "0.00200000", "T": TS_MS, "m": false
            })
        );
    }

    /// `/stream` wraps each event under the stream name Binance would use.
    #[test]
    fn the_combined_framing_names_the_stream() {
        let v = parse(&book_ticker(
            &usdsm_quote(),
            Engine::UsdM,
            Framing::Combined,
        ));
        assert_eq!(v["stream"], "btcusdt@bookTicker");
        assert_eq!(
            v["data"],
            parse(&book_ticker(&usdsm_quote(), Engine::UsdM, Framing::Raw))
        );
    }

    /// Both engines list `BTCUSDT`: the path's Source ID is the only thing that keeps them apart.
    #[test]
    fn another_engines_symbol_never_reaches_this_one() {
        let s = subs(&["btcusdt@bookTicker", "btcusdt@aggTrade", "btcusdt@trade"]);
        let cat = catalog(-2, -3);
        let spot = FeedMessage::Quote(spot_quote());
        let usdsm = FeedMessage::Quote(usdsm_quote());
        assert!(frames(&spot, Engine::UsdM, Framing::Raw, &s, &cat).is_empty());
        assert!(frames(&usdsm, Engine::Spot, Framing::Raw, &s, &cat).is_empty());
        assert_eq!(
            frames(&usdsm, Engine::UsdM, Framing::Raw, &s, &cat).len(),
            1
        );
        let spot_print = FeedMessage::Trade(print(8, Side::Buy));
        assert!(frames(&spot_print, Engine::UsdM, Framing::Raw, &s, &cat).is_empty());
    }

    /// A trade carries no exponents, so it renders at its instrument's from the catalog.
    #[test]
    fn a_trade_renders_at_its_instruments_precision() {
        let s = subs(&["btcusdt@aggTrade", "btcusdt@trade"]);
        let m = FeedMessage::Trade(print(6, Side::Buy));
        let out = frames(&m, Engine::UsdM, Framing::Raw, &s, &catalog(-2, -3));
        let kinds: Vec<Kind> = out.iter().map(|(k, _)| *k).collect();
        assert_eq!(kinds, [Kind::AggTrade, Kind::Trade]);
        assert_eq!(parse(&out[0].1)["p"], "61825.50");
        assert_eq!(parse(&out[0].1)["q"], "0.002");

        let empty: InstrumentSnapshot = Default::default();
        let unknown = frames(&m, Engine::UsdM, Framing::Raw, &s, &empty);
        assert_eq!(parse(&unknown[0].1)["p"], "61825.50000000");
    }

    /// Binance's schema has no "unknown" aggressor, and `m` is read directionally. Counted, so a
    /// publisher stamping none is not mistaken for a quiet tape.
    #[test]
    fn a_print_with_no_known_aggressor_is_dropped_and_counted() {
        let dropped = metrics()
            .binance_sink_dropped
            .with_label_values(&["unknown_aggressor"]);
        let before = dropped.get();
        let s = subs(&["btcusdt@aggTrade"]);
        let m = FeedMessage::Trade(print(6, Side::Unknown));
        assert!(frames(&m, Engine::UsdM, Framing::Raw, &s, &catalog(-2, -3)).is_empty());
        assert_eq!(dropped.get() - before, 1);
    }

    // --- golden frames (pinned here, parsed as Nautilus parses them by tests/binance_sink_shapes.rs) ---

    #[test]
    fn golden_frames_match_the_committed_fixtures() {
        let cat = catalog(-2, -3);
        let mut spot_print = print(8, Side::Buy);
        spot_print.price = 82783.38;
        spot_print.size = 0.00008;
        for (frame, fixture, name) in [
            (
                book_ticker(&usdsm_quote(), Engine::UsdM, Framing::Raw),
                include_str!("../../tests/fixtures/binance_usdsm_bookticker_golden.json"),
                "binance_usdsm_bookticker_golden.json",
            ),
            (
                trade(
                    &print(6, Side::Sell),
                    Kind::AggTrade,
                    true,
                    trade_exponents(&cat, &print(6, Side::Sell)),
                    Framing::Raw,
                ),
                include_str!("../../tests/fixtures/binance_usdsm_aggtrade_golden.json"),
                "binance_usdsm_aggtrade_golden.json",
            ),
            (
                book_ticker(&spot_quote(), Engine::Spot, Framing::Combined),
                include_str!("../../tests/fixtures/binance_spot_bookticker_golden.json"),
                "binance_spot_bookticker_golden.json",
            ),
            (
                trade(&spot_print, Kind::Trade, false, (-8, -8), Framing::Combined),
                include_str!("../../tests/fixtures/binance_spot_trade_golden.json"),
                "binance_spot_trade_golden.json",
            ),
        ] {
            assert_eq!(
                frame,
                fixture.trim_end(),
                "regenerate tests/fixtures/{name} from this renderer"
            );
        }
    }

    // --- over a socket ---

    async fn spawn() -> (
        broadcast::Sender<Arc<FeedMessage>>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let srv = tokio::spawn(serve(listener, tx.clone(), catalog(-2, -3)));
        (tx, addr, srv)
    }

    async fn next_text<S>(ws: &mut S, within: Duration) -> Option<Value>
    where
        S: StreamExt<Item = std::result::Result<WsMessage, WsError>> + Unpin,
    {
        timeout(within, async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => return parse(&t),
                    Some(Ok(_)) => continue,
                    other => panic!("stream ended: {other:?}"),
                }
            }
        })
        .await
        .ok()
    }

    const WAIT: Duration = Duration::from_secs(2);
    const QUIET: Duration = Duration::from_millis(300);

    /// `/usdsm` acknowledges, then sends its own engine's quote bare and nothing of spot's.
    #[tokio::test]
    async fn a_usdsm_client_receives_its_engines_quotes_and_nothing_else() {
        let (tx, addr, srv) = spawn().await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/usdsm"))
            .await
            .unwrap();
        ws.send(text(
            r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}"#.into(),
        ))
        .await
        .unwrap();
        assert_eq!(
            next_text(&mut ws, WAIT).await.unwrap(),
            serde_json::json!({"result": null, "id": 1})
        );
        let _ = tx.send(Arc::new(FeedMessage::Quote(spot_quote())));
        let _ = tx.send(Arc::new(FeedMessage::Quote(usdsm_quote())));
        let v = next_text(&mut ws, WAIT).await.unwrap();
        assert_eq!(
            (v["e"].as_str(), v["b"].as_str()),
            (Some("bookTicker"), Some("61825.40"))
        );
        assert!(next_text(&mut ws, QUIET).await.is_none());
        srv.abort();
    }

    /// The Python client's form: the first stream in the URL, no `SUBSCRIBE`, combined frames.
    #[tokio::test]
    async fn a_stream_named_in_the_url_streams_combined_frames() {
        let (tx, addr, srv) = spawn().await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/spot/stream?streams=btcusdt@trade"
        ))
        .await
        .unwrap();
        let _ = tx.send(Arc::new(FeedMessage::Trade(print(8, Side::Buy))));
        let v = next_text(&mut ws, WAIT).await.unwrap();
        assert_eq!(v["stream"], "btcusdt@trade");
        assert_eq!(v["data"]["t"], TRADE_ID);
        srv.abort();
    }

    /// A stream in the URL this sink cannot serve is refused in-band, not at the handshake.
    #[tokio::test]
    async fn an_unservable_stream_in_the_url_is_refused_after_the_handshake() {
        let (_tx, addr, srv) = spawn().await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/usdsm/stream?streams=btcusdt@depth@0ms"
        ))
        .await
        .unwrap();
        let v = next_text(&mut ws, WAIT).await.unwrap();
        assert_eq!(v["code"], 2);
        assert!(v.get("id").is_none());
        srv.abort();
    }

    #[tokio::test]
    async fn a_path_naming_no_engine_fails_the_handshake_with_404() {
        let (_tx, addr, srv) = spawn().await;
        let Err(WsError::Http(response)) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/stream")).await
        else {
            panic!("the handshake must fail")
        };
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = String::from_utf8(response.body().clone().unwrap_or_default()).unwrap();
        assert!(body.contains("/usdsm") && body.contains("/spot"), "{body}");
        srv.abort();
    }

    /// Crossing the inbound cap ends the connection with the reason, then a `Close`.
    #[tokio::test]
    async fn crossing_the_inbound_rate_limit_ends_the_connection() {
        let (_tx, addr, srv) = spawn().await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/spot"))
            .await
            .unwrap();
        for i in 0..=600 {
            let frame = format!(r#"{{"method":"SUBSCRIBE","params":[],"id":{i}}}"#);
            if ws.send(text(frame)).await.is_err() {
                break;
            }
        }
        let (limited, closed) = timeout(Duration::from_secs(5), async {
            let mut limited = false;
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => limited |= parse(&t)["code"] == -1003,
                    Some(Ok(WsMessage::Close(_))) => return (limited, true),
                    Some(Ok(_)) => continue,
                    _ => return (limited, false),
                }
            }
        })
        .await
        .expect("must answer within the timeout");
        assert!(limited && closed);
        srv.abort();
    }
}
