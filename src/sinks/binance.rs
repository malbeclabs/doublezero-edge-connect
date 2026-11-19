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
        Arc, OnceLock,
    },
    time::{Duration, Instant},
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
        Message as WsMessage, Utf8Bytes,
    },
    WebSocketStream,
};
use tracing::{info, warn};

use super::{
    http::parse_query_params,
    hyperliquid::{
        finish_close, inbound_allowed, inbound_limits, json, ms_or_now, ClientGuard,
        CLOSE_HANDSHAKE_TIMEOUT, HANDSHAKE_TIMEOUT, HEARTBEAT, IDLE_TIMEOUT, MAX_CLIENTS,
        PREPARED_CAPACITY,
    },
};
use crate::{
    ingest::sources::{BINANCE_SPOT_SOURCE_ID, BINANCE_USDSM_SOURCE_ID},
    metrics::metrics,
    model::{FeedMessage, NormalizedQuote, NormalizedTrade, QuoteWire, Side, TradeWire},
};

/// Binance's own per-connection stream limit.
const MAX_STREAMS: usize = 1024;

/// Longest symbol accepted in a stream name, which is retained per subscription.
const MAX_SYMBOL_LEN: usize = 32;

/// Pause after a failed accept, so a persistent error such as `EMFILE` cannot spin a worker.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

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
    for (key, names) in parse_query_params(query.unwrap_or_default()) {
        // Binance also takes `timeUnit`; ignoring it would serve milliseconds to a client expecting
        // microseconds.
        if key != "streams" {
            return Err((
                StatusCode::BAD_REQUEST,
                "unsupported query parameter: only streams= is supported\n",
            ));
        }
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

/// A wire integer at its exponent, trailing zeros kept: the instrument's declared precision, as
/// Binance formats it. Nautilus's Python client takes precision from the string: its `QuoteTick`
/// refuses a bid and ask of different precision, and its risk engine an order more precise than the
/// instrument.
fn fixed(mantissa: i128, exponent: i8) -> String {
    if exponent >= 0 {
        return match mantissa {
            0 => "0".into(),
            m => format!("{m}{}", "0".repeat(exponent.unsigned_abs().into())),
        };
    }
    let dp = usize::from(exponent.unsigned_abs());
    let digits = format!("{:0>width$}", mantissa.unsigned_abs(), width = dp + 1);
    let (int, frac) = digits.split_at(digits.len() - dp);
    let sign = if mantissa < 0 { "-" } else { "" };
    format!("{sign}{int}.{frac}")
}

/// A value from an input with no wire integers, none of them Binance's today, at Binance's finest
/// precision.
fn unscaled(v: f64) -> String {
    format!("{v:.8}")
}

/// Bid price, bid size, ask price and ask size.
fn quote_values(q: &NormalizedQuote) -> [String; 4] {
    let (w, scaled) = (&q.wire, [q.bid, q.bid_size, q.ask, q.ask_size]);
    // An all-zero wire is also a real empty book at integer precision, so only values it could not
    // have produced mark a quote that carries none.
    if *w == QuoteWire::default() && scaled.iter().any(|v| *v != 0.0) {
        return scaled.map(unscaled);
    }
    [
        fixed(w.bid_price.into(), w.price_exponent),
        fixed(w.bid_qty.into(), w.qty_exponent),
        fixed(w.ask_price.into(), w.price_exponent),
        fixed(w.ask_qty.into(), w.qty_exponent),
    ]
}

/// Price and quantity.
fn trade_values(t: &NormalizedTrade) -> [String; 2] {
    let (w, scaled) = (&t.wire, [t.price, t.size]);
    if *w == TradeWire::default() && scaled.iter().any(|v| *v != 0.0) {
        return scaled.map(unscaled);
    }
    [
        fixed(w.price.into(), w.price_exponent),
        fixed(w.qty.into(), w.qty_exponent),
    ]
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
    let [b, bid_qty, a, ask_qty] = quote_values(q);
    let time = ms_or_now(q.source_ts_ns);
    let futures = engine == Engine::UsdM;
    let data = BookTicker {
        e: futures.then_some("bookTicker"),
        u: 0,
        event_time: futures.then_some(time),
        transaction_time: time,
        s: &q.symbol,
        b,
        bid_qty,
        a,
        ask_qty,
    };
    framed(framing, &q.symbol, Kind::BookTicker, &data)
}

fn trade(t: &NormalizedTrade, kind: Kind, buyer_is_maker: bool, framing: Framing) -> String {
    let time = ms_or_now(t.source_ts_ns);
    let [p, q] = trade_values(t);
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

enum Event {
    Quote(NormalizedQuote),
    /// With Binance's `m`: whether the buyer was the maker.
    Trade(NormalizedTrade, bool),
}

/// One Binance quote or print shared by every client. Each `(kind, framing)` is rendered at most
/// once, by the first client that sends it, so N clients cost one render and a stream nobody
/// subscribed to costs none.
struct Prepared {
    engine: Engine,
    event: Event,
    rendered: [OnceLock<Utf8Bytes>; 6],
}

impl Prepared {
    /// `None` for anything but a quote or print from one of Binance's two engines.
    fn new(msg: &FeedMessage) -> Option<Self> {
        let id = match msg {
            FeedMessage::Quote(q) => q.source_id,
            FeedMessage::Trade(t) => t.source_id,
            _ => return None,
        };
        let engine = [Engine::UsdM, Engine::Spot]
            .into_iter()
            .find(|e| e.source_id() == id)?;
        let event = match msg {
            FeedMessage::Quote(q) => Event::Quote(q.clone()),
            // `m` is the one field a consumer reads directionally, and Binance's schema has no
            // "unknown", so such a print is dropped rather than guessed.
            FeedMessage::Trade(t) => match t.aggressor_side {
                Side::Buy => Event::Trade(t.clone(), false),
                Side::Sell => Event::Trade(t.clone(), true),
                Side::Unknown => {
                    metrics()
                        .binance_sink_dropped
                        .with_label_values(&["unknown_aggressor"])
                        .inc();
                    return None;
                }
            },
            _ => return None,
        };
        Some(Self {
            engine,
            event,
            rendered: Default::default(),
        })
    }

    /// The frames one client is owed: the streams it subscribed to, on its own engine. Another
    /// Source ID is another engine, never sent here even under the same symbol.
    fn frames(&self, engine: Engine, framing: Framing, subs: &Subs) -> Vec<(Kind, Utf8Bytes)> {
        if self.engine != engine {
            return Vec::new();
        }
        let (symbol, kinds): (&str, &[Kind]) = match &self.event {
            Event::Quote(q) => (&q.symbol, &[Kind::BookTicker]),
            Event::Trade(t, _) => (&t.symbol, &[Kind::AggTrade, Kind::Trade]),
        };
        kinds
            .iter()
            .filter(|k| subs.has(**k, symbol))
            .map(|&kind| {
                let slot = &self.rendered[kind as usize * 2 + framing as usize];
                let frame = slot.get_or_init(|| {
                    match &self.event {
                        Event::Quote(q) => book_ticker(q, engine, framing),
                        Event::Trade(t, m) => trade(t, kind, *m, framing),
                    }
                    .into()
                });
                (kind, frame.clone())
            })
            .collect()
    }
}

/// Filters the broadcast to Binance's quotes and prints once for every client, so other feeds'
/// traffic wakes none of them.
async fn prepare_loop(
    mut backbone: broadcast::Receiver<Arc<FeedMessage>>,
    out: broadcast::Sender<Arc<Prepared>>,
) {
    loop {
        match backbone.recv().await {
            Ok(m) => {
                if out.receiver_count() == 0 {
                    continue;
                }
                if let Some(p) = Prepared::new(&m) {
                    let _ = out.send(Arc::new(p));
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("binance sink prepare stage lagged, dropped {n}");
                metrics()
                    .binance_sink_dropped
                    .with_label_values(&["prepare_lagged"])
                    .inc();
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
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
pub async fn serve(listener: TcpListener, tx: broadcast::Sender<Arc<FeedMessage>>) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));
    let (prepared, _) = broadcast::channel::<Arc<Prepared>>(PREPARED_CAPACITY);
    tokio::spawn(prepare_loop(tx.subscribe(), prepared.clone()));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!("binance sink accept failed: {e}");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
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
        let prepared = prepared.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, prepared).await {
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

async fn serve_client(stream: TcpStream, prepared: broadcast::Sender<Arc<Prepared>>) -> Result<()> {
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
        send(&mut write, refused_streams(&refused), IDLE_TIMEOUT).await?;
    }
    // Only now, so nothing broadcast during the handshake reaches the client as if it were live.
    let mut rx = prepared.subscribe();
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval(HEARTBEAT);
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Close(_))) | None => break,
                // Every frame is charged, whatever its kind, so none can bypass the cap.
                Some(Ok(frame)) => {
                    last_seen = Instant::now();
                    if !inbound_allowed(&mut win_start, &mut win_count) {
                        return end_rate_limited(&mut write, &mut read).await;
                    }
                    match frame {
                        WsMessage::Text(request) => {
                            for answer in control(&request, &mut subs) {
                                send(&mut write, answer, IDLE_TIMEOUT).await?;
                            }
                        }
                        WsMessage::Ping(p) => send(&mut write, WsMessage::Pong(p), IDLE_TIMEOUT).await?,
                        _ => {}
                    }
                }
                Some(Err(e)) => return Err(e.into()),
            },

            _ = hb.tick() => {
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    let _ = send(&mut write, WsMessage::Close(None), CLOSE_HANDSHAKE_TIMEOUT).await;
                    break;
                }
                send(&mut write, WsMessage::Ping(Vec::new().into()), IDLE_TIMEOUT).await?;
            },

            msg = rx.recv() => match msg {
                Ok(p) => {
                    for (kind, frame) in p.frames(engine, framing, &subs) {
                        metrics().binance_sink_messages.with_label_values(&[kind.name()]).inc();
                        send(&mut write, WsMessage::Text(frame), IDLE_TIMEOUT).await?;
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

/// Bounded, because a peer that stops reading leaves a send pending for good, and with it this
/// connection's heartbeat and idle reap: its client slot would never be released.
async fn send<S>(write: &mut S, msg: WsMessage, deadline: Duration) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(deadline, write.send(msg))
        .await
        .map_err(|_| anyhow::anyhow!("client stopped reading"))??;
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
    let reason = refusal(-1003, "Too many requests: inbound rate limit exceeded");
    send(write, reason, CLOSE_HANDSHAKE_TIMEOUT).await?;
    send(write, WsMessage::Close(None), CLOSE_HANDSHAKE_TIMEOUT).await?;
    finish_close(read, CLOSE_HANDSHAKE_TIMEOUT).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{net::TcpListener, sync::broadcast, time::timeout};
    use tokio_tungstenite::tungstenite::Error as WsError;

    use super::*;
    use crate::ingest::codec_common::apply_exponent;

    const VENUE: &str = "BINANCE";
    /// 2023-11-14T22:13:20.123Z, so `E`/`T` carry a millisecond fraction.
    const TS_NS: u64 = 1_700_000_000_123_000_000;
    const TS_MS: u64 = 1_700_000_000_123;
    const TRADE_ID: u64 = 6_778_484_593;

    /// A quote as the top-of-book decoder emits it from wire `(price, qty)` integers per side.
    fn quote(
        source_id: u16,
        symbol: &str,
        bid: (i64, u64),
        ask: (i64, u64),
        (price_exponent, qty_exponent): (i8, i8),
    ) -> NormalizedQuote {
        NormalizedQuote {
            venue: VENUE.into(),
            source_name: VENUE.into(),
            source_id,
            symbol: symbol.into(),
            bid: apply_exponent(bid.0, price_exponent),
            bid_size: apply_exponent(bid.1 as i64, qty_exponent),
            bid_n: 0,
            ask: apply_exponent(ask.0, price_exponent),
            ask_size: apply_exponent(ask.1 as i64, qty_exponent),
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
                bid_price: bid.0,
                bid_qty: bid.1,
                ask_price: ask.0,
                ask_qty: ask.1,
                price_exponent,
                qty_exponent,
            },
        }
    }

    fn usdsm_quote() -> NormalizedQuote {
        quote(
            6,
            "BTCUSDT",
            (6_182_540, 5_512),
            (6_182_550, 9_879),
            (-2, -3),
        )
    }

    fn spot_quote() -> NormalizedQuote {
        let (bid, ask) = ((8_278_338_000_000, 8_000), (8_278_339_000_000, 150_000_000));
        let mut q = quote(8, "BTCUSDT", bid, ask, (-8, -8));
        q.category = "spot".into();
        q
    }

    /// `61825.50` x `0.002` at USD-M's `(-2, -3)`.
    fn print(source_id: u16, side: Side) -> NormalizedTrade {
        traded(source_id, side, (6_182_550, 2), (-2, -3))
    }

    /// A print as the top-of-book decoder emits it from wire `(price, qty)` integers.
    fn traded(
        source_id: u16,
        side: Side,
        (price, qty): (i64, u64),
        (price_exponent, qty_exponent): (i8, i8),
    ) -> NormalizedTrade {
        NormalizedTrade {
            venue: VENUE.into(),
            source_name: VENUE.into(),
            source_id,
            symbol: "BTCUSDT".into(),
            channel: 0,
            instrument_id: 7,
            category: "usdsm".into(),
            price: apply_exponent(price, price_exponent),
            size: apply_exponent(qty as i64, qty_exponent),
            aggressor_side: side,
            trade_id: TRADE_ID,
            cumulative_volume: 0.0,
            source_ts_ns: TS_NS,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            wire: TradeWire {
                price,
                qty,
                price_exponent,
                qty_exponent,
            },
        }
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

    /// What one broadcast message yields for a client, through the shared stage.
    fn frames(
        msg: &FeedMessage,
        engine: Engine,
        framing: Framing,
        subs: &Subs,
    ) -> Vec<(Kind, String)> {
        Prepared::new(msg).map_or_else(Vec::new, |p| {
            p.frames(engine, framing, subs)
                .into_iter()
                .map(|(k, f)| (k, f.to_string()))
                .collect()
        })
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

    /// As an HTTP server would, so a client whose URL builder escapes `@` and `/` is not refused.
    #[test]
    fn streams_in_the_url_are_percent_decoded() {
        let r = route(
            "/spot/stream",
            Some("streams=btcusdt%40trade%2Fethusdt%40bookTicker"),
        )
        .unwrap();
        assert_eq!(r.streams, ["btcusdt@trade", "ethusdt@bookTicker"]);
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
        let q = quote(6, "BTCUSDT", (1_005, 100), (1_010, 225), (-1, -2));
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
    fn wire_integers_render_at_their_exponent() {
        assert_eq!(fixed(5, -3), "0.005");
        assert_eq!(fixed(0, -3), "0.000");
        assert_eq!(fixed(-15, -1), "-1.5");
        assert_eq!(fixed(12, 2), "1200");
        assert_eq!(fixed(5, 0), "5");
        assert_eq!(fixed(0, 2), "0");
    }

    /// Past ~15 significant digits the scaled `f64` rounds; the wire integer does not.
    #[test]
    fn values_past_an_f64s_precision_render_exactly() {
        let big = 10_000_000_000_000_001;
        let q = quote(8, "BTCUSDT", (1, big), (2, big), (-8, -8));
        assert_ne!(format!("{:.8}", q.bid_size), "100000000.00000001");
        let v = parse(&book_ticker(&q, Engine::Spot, Framing::Raw));
        assert_eq!(v["B"], "100000000.00000001");
        let t = traded(8, Side::Buy, (1, big), (-8, -8));
        let v = parse(&trade(&t, Kind::Trade, false, Framing::Raw));
        assert_eq!(v["q"], "100000000.00000001");
    }

    /// An all-zero wire is also a real quote: an empty book at integer precision.
    #[test]
    fn an_empty_book_at_integer_precision_renders_from_the_wire() {
        let q = quote(6, "BTCUSDT", (0, 0), (0, 0), (0, 0));
        assert_eq!(q.wire, QuoteWire::default(), "fixture sanity");
        let v = parse(&book_ticker(&q, Engine::UsdM, Framing::Raw));
        assert_eq!((v["b"].as_str(), v["B"].as_str()), (Some("0"), Some("0")));
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
        let t = traded(8, Side::Buy, (6_182_550_000_000, 200_000), (-8, -8));
        let v = parse(&trade(&t, Kind::Trade, false, Framing::Raw));
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

    /// N clients sharing a stream cost one render, not N.
    #[test]
    fn one_render_serves_every_client() {
        let s = subs(&["btcusdt@bookTicker"]);
        let p = Prepared::new(&FeedMessage::Quote(usdsm_quote())).unwrap();
        let first = p.frames(Engine::UsdM, Framing::Raw, &s);
        let second = p.frames(Engine::UsdM, Framing::Raw, &s);
        assert_eq!(first[0].1.as_str().as_ptr(), second[0].1.as_str().as_ptr());
    }

    /// Other feeds never reach a client, so their traffic wakes none of them.
    #[test]
    fn only_binance_quotes_and_prints_are_forwarded() {
        let mut hl = usdsm_quote();
        hl.source_id = 1;
        assert!(Prepared::new(&FeedMessage::Quote(hl)).is_none());
        assert!(Prepared::new(&FeedMessage::Quote(usdsm_quote())).is_some());
        assert!(Prepared::new(&FeedMessage::Trade(print(8, Side::Buy))).is_some());
    }

    /// Both engines list `BTCUSDT`: the path's Source ID is the only thing that keeps them apart.
    #[test]
    fn another_engines_symbol_never_reaches_this_one() {
        let s = subs(&["btcusdt@bookTicker", "btcusdt@aggTrade", "btcusdt@trade"]);
        let spot = FeedMessage::Quote(spot_quote());
        let usdsm = FeedMessage::Quote(usdsm_quote());
        assert!(frames(&spot, Engine::UsdM, Framing::Raw, &s).is_empty());
        assert!(frames(&usdsm, Engine::Spot, Framing::Raw, &s).is_empty());
        assert_eq!(frames(&usdsm, Engine::UsdM, Framing::Raw, &s).len(), 1);
        let spot_print = FeedMessage::Trade(print(8, Side::Buy));
        assert!(frames(&spot_print, Engine::UsdM, Framing::Raw, &s).is_empty());
    }

    /// At the exponents it was scaled with, which a later definition cannot change; a print with no
    /// wire values at Binance's finest precision.
    #[test]
    fn a_trade_renders_at_its_own_precision() {
        let s = subs(&["btcusdt@aggTrade", "btcusdt@trade"]);
        let m = FeedMessage::Trade(traded(6, Side::Buy, (15, 15), (-3, -3)));
        let out = frames(&m, Engine::UsdM, Framing::Raw, &s);
        let kinds: Vec<Kind> = out.iter().map(|(k, _)| *k).collect();
        assert_eq!(kinds, [Kind::AggTrade, Kind::Trade]);
        assert_eq!(parse(&out[0].1)["p"], "0.015");

        let mut t = print(6, Side::Buy);
        t.wire = TradeWire::default();
        let out = frames(&FeedMessage::Trade(t), Engine::UsdM, Framing::Raw, &s);
        assert_eq!(parse(&out[0].1)["p"], "61825.50000000");
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
        assert!(frames(&m, Engine::UsdM, Framing::Raw, &s).is_empty());
        assert_eq!(dropped.get() - before, 1);
    }

    // --- golden frames (pinned here, parsed as Nautilus parses them by tests/binance_sink_shapes.rs) ---

    #[test]
    fn golden_frames_match_the_committed_fixtures() {
        let spot_print = traded(8, Side::Buy, (8_278_338_000_000, 8_000), (-8, -8));
        for (frame, fixture, name) in [
            (
                book_ticker(&usdsm_quote(), Engine::UsdM, Framing::Raw),
                include_str!("../../tests/fixtures/binance_usdsm_bookticker_golden.json"),
                "binance_usdsm_bookticker_golden.json",
            ),
            (
                trade(&print(6, Side::Sell), Kind::AggTrade, true, Framing::Raw),
                include_str!("../../tests/fixtures/binance_usdsm_aggtrade_golden.json"),
                "binance_usdsm_aggtrade_golden.json",
            ),
            (
                book_ticker(&spot_quote(), Engine::Spot, Framing::Combined),
                include_str!("../../tests/fixtures/binance_spot_bookticker_golden.json"),
                "binance_spot_bookticker_golden.json",
            ),
            (
                trade(&spot_print, Kind::Trade, false, Framing::Combined),
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
        let srv = tokio::spawn(serve(listener, tx.clone()));
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
        // The client subscribes only once its handshake and URL streams are done, so publish until
        // a print reaches it.
        let v = timeout(WAIT, async {
            loop {
                let _ = tx.send(Arc::new(FeedMessage::Trade(print(8, Side::Buy))));
                if let Some(v) = next_text(&mut ws, Duration::from_millis(20)).await {
                    return v;
                }
            }
        })
        .await
        .unwrap();
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

    /// Flood past the inbound cap with `frame`, then report whether the rate-limit refusal arrived
    /// and a `Close` followed it.
    async fn flood_past_the_cap(frame: impl Fn() -> WsMessage) -> (bool, bool) {
        let (_tx, addr, srv) = spawn().await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/spot"))
            .await
            .unwrap();
        for _ in 0..=600 {
            if ws.send(frame()).await.is_err() {
                break;
            }
        }
        let out = timeout(Duration::from_secs(5), async {
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
        srv.abort();
        out
    }

    /// Crossing the inbound cap ends the connection with the reason, then a `Close`, whatever kind
    /// of frame crossed it: one the cap skipped could be sent without limit.
    #[tokio::test]
    async fn every_kind_of_inbound_frame_counts_against_the_rate_limit() {
        let subscribe = || text(r#"{"method":"SUBSCRIBE","params":[],"id":1}"#.into());
        let binary = || WsMessage::Binary(vec![0u8; 16].into());
        let pong = || WsMessage::Pong(Vec::new().into());
        for (kind, frame) in [
            ("text", &subscribe as &dyn Fn() -> WsMessage),
            ("binary", &binary),
            ("pong", &pong),
        ] {
            assert_eq!(flood_past_the_cap(frame).await, (true, true), "{kind}");
        }
    }

    /// A sink that never accepts a frame: a peer whose receive window is full.
    struct Stalled;

    impl futures_util::Sink<WsMessage> for Stalled {
        type Error = std::io::Error;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
        fn start_send(
            self: std::pin::Pin<&mut Self>,
            _: WsMessage,
        ) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
    }

    /// A peer that stops reading must not hold its client slot: the send gives up at its deadline.
    #[tokio::test]
    async fn a_send_to_a_peer_that_stopped_reading_gives_up() {
        let deadline = Duration::from_millis(50);
        let sent = timeout(
            Duration::from_secs(5),
            send(&mut Stalled, WsMessage::Ping(Vec::new().into()), deadline),
        )
        .await
        .expect("send must return at its deadline, not hang");
        assert!(sent.is_err());
    }
}
