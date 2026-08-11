//! Hyperliquid-compatible WebSocket sink: re-serve our reconstructed order book and tape in
//! **Hyperliquid's own schema**, so an existing Hyperliquid client consumes edge-connect by pointing
//! its WebSocket endpoint here. Off by default (`--hl-ws-bind`).
//!
//! This is a **rendering, not a second pipeline**: it holds no ingest state, adds no dedup identity
//! and influences no arbitration. It reads the same broadcast `sinks::ws` reads plus the shared
//! `BookSnapshot` the arbiter maintains, and nothing about it belongs in PROTOCOL.md — that document
//! is the contract for our normalized protocol, and this speaks someone else's.
//!
//! Two references define the wire, and they disagree in one place. **NautilusTrader v1.227.0**
//! (`crates/adapters/hyperliquid/src/websocket/messages.rs`) is what actually parses our bytes for
//! `l2Book`/`trades`, so it wins for anything it parses. **DoubleZero's own Hyperliquid publisher**
//! (`malbeclabs/hyperliquid`, `app/publisher/server/src`) defines `l4Book`, the `nLevels` extension
//! and the significant-figure arithmetic, which are absent from Nautilus entirely.
//!
//! Scoped to the Hyperliquid venue: `coin` is our `symbol`, and a message from any other venue is
//! never rendered.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};

use crate::{
    metrics::metrics,
    model::{
        now_ns, BookAccumulator, BookAction, BookSide, BookSnapshot, FeedMessage, NormalizedBook,
        NormalizedTrade, ReplayScope, Side,
    },
};

/// The one venue this sink renders. Hyperliquid's schema names an instrument by `coin`, which is our
/// `symbol`; a message from another venue has no `coin` to be and is dropped before any rendering.
const VENUE: &str = "HYPERLIQUID";

/// `nLevels` when the subscription omits it, and the ceiling it is clamped to — both from the
/// publisher's `types/subscription.rs`.
const DEFAULT_LEVELS: usize = 20;
const MAX_LEVELS: usize = 100;

/// Longest `coin` accepted on a subscription. The wire symbol is a 16-byte field, so this is slack
/// above anything real; it exists because the string is retained per subscription.
const MAX_COIN_LEN: usize = 32;

/// Client limits and liveness. Deliberately constants rather than flags: the sink is a compatibility
/// rendering with no operator-tunable behaviour, and every value here mirrors the normalized sink's
/// shipped default.
const MAX_CLIENTS: usize = 64;
const MAX_SUBS: usize = 256;
const HEARTBEAT: Duration = Duration::from_secs(20);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest client text echoed back in an error frame, so a hostile client cannot amplify its own
/// input into our output.
const MAX_ECHOED_ERROR: usize = 200;

/// The fields Hyperliquid's schema requires and an MBO feed cannot supply. The wire carries an order
/// book, not the venue's account model: there are no counterparties on a print, no transaction hash,
/// and no block height. Nulling the ones that are nullable and zeroing the rest keeps the frames
/// parseable by a client written against the publisher, which is the whole point of the sink; a
/// consumer must not read meaning into them.
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// One client's subscriptions: which channel for which coin. `n_levels` is normalized here (the
/// default applied, the ceiling clamped), so the value carried is the one actually served.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum Sub {
    #[serde(rename_all = "camelCase")]
    L2Book {
        coin: String,
        n_sig_figs: Option<u32>,
        mantissa: Option<u32>,
        n_levels: usize,
    },
    #[serde(rename_all = "camelCase")]
    L4Book { coin: String },
    #[serde(rename_all = "camelCase")]
    Trades { coin: String },
}

impl Sub {
    fn coin(&self) -> &str {
        match self {
            Sub::L2Book { coin, .. } | Sub::L4Book { coin } | Sub::Trades { coin } => coin,
        }
    }
}

/// The `l2Book` view a subscription asked for: the significant-figure bucket, its mantissa
/// refinement, and how deep to publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct L2View {
    n_sig_figs: Option<u32>,
    mantissa: Option<u32>,
    n_levels: usize,
}

/// One recognized inbound control frame.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Control {
    Subscribe(Sub),
    Unsubscribe(Sub),
    /// `{"method":"ping"}` — NautilusTrader sends this every 30s and parses only `{"channel":"pong"}`
    /// in reply, so answering with the publisher's error envelope would log an error per heartbeat
    /// for the life of the session.
    Ping,
}

/// As received, before defaults and validation: every optional field is still absent-or-null.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum SubIn {
    #[serde(rename_all = "camelCase")]
    L2Book {
        coin: String,
        n_sig_figs: Option<u32>,
        mantissa: Option<u32>,
        n_levels: Option<usize>,
    },
    #[serde(rename_all = "camelCase")]
    L4Book { coin: String },
    #[serde(rename_all = "camelCase")]
    Trades { coin: String },
}

#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "camelCase")]
enum Inbound {
    Subscribe { subscription: SubIn },
    Unsubscribe { subscription: SubIn },
    Ping,
}

/// Parse one inbound control frame. `Err` carries the message for Hyperliquid's error envelope —
/// either the frame did not parse, or its subscription asked for an aggregation that does not exist.
///
/// An out-of-range `nSigFigs`/`mantissa` is **rejected, not coerced**: a silently-substituted bucket
/// produces a book whose prices are plausible and not the venue's, which is worse for a consumer
/// than being told no. `nLevels` is our own extension and only truncates, so it clamps.
pub(crate) fn parse_control(text: &str) -> Result<Control, String> {
    let inbound = serde_json::from_str::<Inbound>(text).map_err(|_| {
        let mut shown: String = text.chars().take(MAX_ECHOED_ERROR).collect();
        if shown.len() < text.len() {
            shown.push('…');
        }
        format!("Error parsing JSON into valid websocket request: {shown}")
    })?;
    Ok(match inbound {
        Inbound::Ping => Control::Ping,
        Inbound::Subscribe { subscription } => Control::Subscribe(normalize(subscription)?),
        Inbound::Unsubscribe { subscription } => Control::Unsubscribe(normalize(subscription)?),
    })
}

fn normalize(s: SubIn) -> Result<Sub, String> {
    let check = |coin: String| {
        if coin.is_empty() || coin.len() > MAX_COIN_LEN {
            return Err(format!("Invalid subscription: bad coin {coin:?}"));
        }
        Ok(coin)
    };
    match s {
        SubIn::Trades { coin } => Ok(Sub::Trades { coin: check(coin)? }),
        SubIn::L4Book { coin } => Ok(Sub::L4Book { coin: check(coin)? }),
        SubIn::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            n_levels,
        } => {
            let coin = check(coin)?;
            if let Some(n) = n_sig_figs {
                if !(2..=5).contains(&n) {
                    return Err(format!("Invalid subscription: nSigFigs {n} not in 2..=5"));
                }
            }
            // `mantissa` refines the bucket in the fifth significant digit, so it is meaningless at a
            // coarser `nSigFigs` and the publisher rejects the pair rather than ignoring it.
            match (n_sig_figs, mantissa) {
                (_, None) => {}
                (Some(5), Some(2 | 5)) => {}
                (_, Some(m)) => {
                    return Err(format!(
                        "Invalid subscription: mantissa {m} requires nSigFigs 5 and must be 2 or 5"
                    ))
                }
            }
            Ok(Sub::L2Book {
                coin,
                n_sig_figs,
                mantissa,
                n_levels: n_levels.unwrap_or(DEFAULT_LEVELS).min(MAX_LEVELS),
            })
        }
    }
}

// --- the wire shapes ---

#[derive(Serialize)]
struct Envelope<T> {
    channel: &'static str,
    data: T,
}

#[derive(Serialize)]
struct Level {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Serialize)]
struct L2Data<'a> {
    coin: &'a str,
    time: u64,
    /// `[bids, asks]`, each best-first. Nautilus deserializes this into a fixed 2-element array.
    levels: [Vec<Level>; 2],
}

/// The publisher's `L4Order`. Nine of its fields describe a Hyperliquid account order and have no
/// counterpart on the market-by-order wire — see [`ZERO_ADDRESS`].
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct L4Order<'a> {
    user: Option<&'a str>,
    coin: &'a str,
    side: &'static str,
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    trigger_condition: &'static str,
    is_trigger: bool,
    trigger_px: &'static str,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: &'static str,
    tif: Option<&'static str>,
    cloid: Option<&'static str>,
}

/// Externally tagged, exactly as the publisher's `L4Book` is: `{"Snapshot":{…}}` / `{"Updates":{…}}`
/// is the discriminator, and there is no `isSnapshot` flag anywhere on the channel.
#[derive(Serialize)]
enum L4Data<'a> {
    Snapshot {
        coin: &'a str,
        time: u64,
        height: u64,
        levels: [Vec<L4Order<'a>>; 2],
    },
    Updates(L4Updates<'a>),
}

#[derive(Serialize)]
struct L4Updates<'a> {
    time: u64,
    height: u64,
    /// Always empty: the publisher fills this from the node's order-status stream, which the wire we
    /// decode does not carry.
    order_statuses: [u8; 0],
    book_diffs: Vec<OrderDiffEntry<'a>>,
}

#[derive(Serialize)]
struct OrderDiffEntry<'a> {
    user: &'static str,
    oid: u64,
    px: String,
    coin: &'a str,
    raw_book_diff: RawDiff,
}

/// The publisher's `OrderDiff`, minus its `update{origSz,newSz}` variant: our changes carry an
/// order's **absolute** resulting quantity and no prior one, so `origSz` could only be fabricated.
/// `new` already means "this order now rests at this price for this size", which is exactly what a
/// change asserts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum RawDiff {
    New { sz: String },
    Remove,
}

// --- rendering ---

/// Format a price or size the way the venue does: plain decimal, no trailing zeros, never an
/// exponent. `px`/`sz` are strings a consumer parses, so `1e2` or `100.50` would break compatibility
/// even though both are numerically right. Mirrors the publisher's `Px::to_str`.
fn num(v: f64) -> String {
    let s = format!("{v:.8}");
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

/// Hyperliquid's `time` is milliseconds and has no "unknown" encoding, so our `0` sentinel becomes
/// the send time rather than the epoch — a consumer that timestamps from this field would otherwise
/// date every message to 1970.
fn ms_or_now(source_ts_ns: u64) -> u64 {
    if source_ts_ns == 0 {
        now_ns() / 1_000_000
    } else {
        source_ts_ns / 1_000_000
    }
}

/// Round `px` into its significant-figure bucket. **Bids round down and asks round up**, both away
/// from the mid, so aggregation never invents a better price than the book holds.
///
/// Mirrors the publisher's `bucket()`, including working on the `10^-8` fixed-point integer: the
/// bucket increment is `mantissa * 10^(digits - nSigFigs)` over that integer, which is what makes
/// "significant figures" well defined at every price magnitude.
fn aggregate_price(px: f64, is_bid: bool, n_sig_figs: Option<u32>, mantissa: Option<u32>) -> f64 {
    let Some(n) = n_sig_figs else { return px };
    let scaled = (px * 1e8).round();
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        return px;
    }
    let v = scaled as u64;
    let digits = if v == 0 { 1 } else { v.ilog10() + 1 };
    let inc = u64::from(mantissa.unwrap_or(1)).saturating_mul(10u64.pow(digits.saturating_sub(n)));
    let bucketed = if is_bid {
        (v / inc).checked_mul(inc)
    } else {
        v.div_ceil(inc).checked_mul(inc)
    };
    match bucketed {
        Some(b) => b as f64 / 1e8,
        None => px,
    }
}

/// Bucket one already-best-first side and merge the levels that collide, summing size and order
/// count. Input order is preserved, so a merged level keeps the position of the best price in it.
fn bucket_side(
    levels: &[crate::model::CountedLevel],
    is_bid: bool,
    view: L2View,
) -> Vec<(f64, f64, u32)> {
    let mut out: Vec<(f64, f64, u32)> = Vec::with_capacity(levels.len().min(view.n_levels));
    for &(px, sz, n) in levels {
        let px = aggregate_price(px, is_bid, view.n_sig_figs, view.mantissa);
        match out.last_mut() {
            Some(last) if last.0 == px => {
                last.1 += sz;
                last.2 += n;
            }
            _ => {
                if out.len() == view.n_levels {
                    break;
                }
                out.push((px, sz, n));
            }
        }
    }
    out
}

/// Render one market's whole book as an `l2Book` frame. `l2Book` is snapshot-per-update, not deltas:
/// Nautilus clears and rebuilds from every frame, so the caller must only ever hand this an
/// accumulator that is [`BookAccumulator::baselined`].
pub(crate) fn render_l2book(
    acc: &BookAccumulator,
    coin: &str,
    view: L2View,
    time_ms: u64,
) -> String {
    let (bids, asks) = acc.price_fold();
    let level = |(px, sz, n): (f64, f64, u32)| Level {
        px: num(px),
        sz: num(sz),
        n,
    };
    let data = L2Data {
        coin,
        time: time_ms,
        levels: [
            bucket_side(&bids, true, view)
                .into_iter()
                .map(level)
                .collect(),
            bucket_side(&asks, false, view)
                .into_iter()
                .map(level)
                .collect(),
        ],
    };
    json(Envelope {
        channel: "l2Book",
        data,
    })
}

fn side_code(side: BookSide) -> &'static str {
    match side {
        BookSide::Bid => "B",
        _ => "A",
    }
}

fn l4_order<'a>(coin: &'a str, side: BookSide, px: f64, sz: f64, oid: u64, ts: u64) -> L4Order<'a> {
    L4Order {
        user: None,
        coin,
        side: side_code(side),
        limit_px: num(px),
        sz: num(sz),
        oid,
        timestamp: ts,
        trigger_condition: "N/A",
        is_trigger: false,
        trigger_px: "0",
        is_position_tpsl: false,
        reduce_only: false,
        order_type: "Limit",
        tif: None,
        cloid: None,
    }
}

/// Render the whole resting book, order by order with the venue's own ids — what an `l4Book`
/// subscriber receives before any diff, and what re-baselines it afterwards (the channel has no
/// clear, so a producer re-baseline becomes another snapshot).
pub(crate) fn render_l4book_snapshot(
    acc: &BookAccumulator,
    key: &(Arc<str>, u32, u32),
    coin: &str,
) -> String {
    let b = acc.to_book(&key.0, key.1, key.2, ReplayScope::Orders);
    let time = ms_or_now(b.source_ts_ns);
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    for c in &b.changes {
        // Skips the leading `clear` and any price-aggregated level: neither names an order, and
        // `oid: 0` reads to an L3 consumer as "aggregate me", silently degrading its book to L2.
        if c.order_id == 0 {
            continue;
        }
        let order = l4_order(coin, c.side, c.price, c.size, c.order_id, time);
        if matches!(c.side, BookSide::Bid) {
            bids.push(order);
        } else {
            asks.push(order);
        }
    }
    json(Envelope {
        channel: "l4Book",
        data: L4Data::Snapshot {
            coin,
            time,
            height: 0,
            levels: [bids, asks],
        },
    })
}

/// Render one incremental batch as `l4Book` order diffs. `None` when the batch is for another venue
/// or coin, or when it carries no order-level change at all.
pub(crate) fn render_l4book_diff(b: &NormalizedBook, coin: &str) -> Option<String> {
    if b.venue.as_ref() != VENUE || b.symbol.as_ref() != coin {
        return None;
    }
    let book_diffs: Vec<OrderDiffEntry> = b
        .changes
        .iter()
        .filter(|c| c.order_id != 0)
        .map(|c| OrderDiffEntry {
            user: ZERO_ADDRESS,
            oid: c.order_id,
            px: num(c.price),
            coin,
            // A zero size is how an order-level producer says the order is gone; a consumer that
            // rested it would hold a phantom forever.
            raw_book_diff: if c.action == BookAction::Delete || c.size == 0.0 {
                RawDiff::Remove
            } else {
                RawDiff::New { sz: num(c.size) }
            },
        })
        .collect();
    if book_diffs.is_empty() {
        return None;
    }
    Some(json(Envelope {
        channel: "l4Book",
        data: L4Data::Updates(L4Updates {
            time: ms_or_now(b.source_ts_ns),
            height: 0,
            order_statuses: [],
            book_diffs,
        }),
    }))
}

#[derive(Serialize)]
struct TradeData<'a> {
    coin: &'a str,
    side: &'static str,
    px: String,
    sz: String,
    hash: &'static str,
    time: u64,
    tid: u64,
    users: [&'static str; 2],
}

/// Render one print in Hyperliquid's trade envelope. `None` for another venue or coin.
///
/// A trade whose aggressor we do not know is dropped rather than guessed: the side is the only field
/// on this channel a consumer acts on directionally, and Hyperliquid's schema has no "unknown".
pub(crate) fn render_trade(t: &NormalizedTrade, coin: &str) -> Option<String> {
    if t.venue.as_ref() != VENUE || t.symbol.as_ref() != coin {
        return None;
    }
    let side = match t.aggressor_side {
        Side::Buy => "B",
        Side::Sell => "A",
        Side::Unknown => return None,
    };
    Some(json(Envelope {
        channel: "trades",
        // An array, not an object: `WsTradeData` is deserialized as `Vec<_>` by the consumer.
        data: [TradeData {
            coin,
            side,
            px: num(t.price),
            sz: num(t.size),
            hash: ZERO_HASH,
            time: ms_or_now(t.source_ts_ns),
            tid: t.trade_id,
            users: [ZERO_ADDRESS, ZERO_ADDRESS],
        }],
    }))
}

/// Serialize a frame we own. Our own types cannot fail to serialize; an empty object rather than a
/// panic keeps a hypothetical failure from taking a client's task down.
fn json<T: Serialize>(v: T) -> String {
    serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string())
}

fn error_frame(message: &str) -> WsMessage {
    WsMessage::Text(
        json(Envelope {
            channel: "error",
            data: message,
        })
        .into(),
    )
}

// --- the server ---

/// Bind the listener up front so the caller decides what a bind failure means: a taken port must
/// disable this sink, never take the process (and the DoubleZero tunnel) down with it.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "Hyperliquid-compatible WebSocket sink listening");
    Ok(listener)
}

/// Releases the client slot and gauge on drop, so a panic inside a client task cannot leak either.
struct ClientGuard {
    clients: Arc<AtomicUsize>,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.clients.fetch_sub(1, Ordering::SeqCst);
        metrics().hl_sink_clients.dec();
    }
}

/// The accept loop. Each client gets its own broadcast receiver: unlike the normalized sink there is
/// no serialize-once stage, because two clients can ask for different `nSigFigs`/`nLevels` views of
/// the same book and so share no rendered bytes.
pub async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<Arc<FeedMessage>>,
    books: BookSnapshot,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));
    loop {
        let (stream, peer) = listener.accept().await?;
        if clients.fetch_add(1, Ordering::SeqCst) >= MAX_CLIENTS {
            clients.fetch_sub(1, Ordering::SeqCst);
            warn!(%peer, max = MAX_CLIENTS, "hl sink at max clients; rejecting connection");
            drop(stream);
            continue;
        }
        metrics().hl_sink_clients.inc();
        let guard = ClientGuard {
            clients: clients.clone(),
        };
        let rx = tx.subscribe();
        let books = books.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, rx, books).await {
                warn!(%peer, "hl sink client ended: {e}");
            }
        });
    }
}

fn sent(channel: &'static str) {
    metrics()
        .hl_sink_messages
        .with_label_values(&[channel])
        .inc();
}

/// The current full state of one market on the channel `sub` asked for, or `None` when the market is
/// not this venue's, not the subscribed coin, or not [`BookAccumulator::baselined`].
///
/// That last gate is load-bearing on **both** book channels: an `l2Book` frame replaces a consumer's
/// book wholesale and an `l4Book` snapshot claims completeness, so an accumulator seeded mid-stream —
/// holding only the levels that have moved since — must be withheld rather than published as whole.
fn full_state(
    key: &(Arc<str>, u32, u32),
    acc: &BookAccumulator,
    sub: &Sub,
) -> Option<(&'static str, String)> {
    if key.0.as_ref() != VENUE || acc.symbol().as_ref() != sub.coin() || !acc.baselined() {
        return None;
    }
    match sub {
        Sub::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            n_levels,
        } => {
            let view = L2View {
                n_sig_figs: *n_sig_figs,
                mantissa: *mantissa,
                n_levels: *n_levels,
            };
            let time = ms_or_now(acc.source_ts_ns());
            Some(("l2Book", render_l2book(acc, coin, view, time)))
        }
        Sub::L4Book { coin } => Some(("l4Book", render_l4book_snapshot(acc, key, coin))),
        Sub::Trades { .. } => None,
    }
}

/// Bootstrap `sub` from current state — every market of this venue whose symbol is the subscribed
/// coin. Scans the market map, so it is for a subscribe or a recovery, never the steady stream.
fn bootstrap(books: &BookSnapshot, sub: &Sub) -> Vec<(&'static str, String)> {
    let guard = crate::model::lock(books);
    guard
        .iter()
        .filter_map(|(key, acc)| full_state(key, acc, sub))
        .collect()
}

/// Every frame one broadcast message produces for `sub`. Filtering on venue and coin happens before
/// any rendering, which is the expensive part, and the book channels resolve the market by its own
/// `(venue, channel, instrument_id)` key rather than rescanning the map per message.
fn render(m: &FeedMessage, sub: &Sub, books: &BookSnapshot) -> Vec<(&'static str, String)> {
    match (m, sub) {
        (FeedMessage::Trade(t), Sub::Trades { coin }) => render_trade(t, coin)
            .map(|f| vec![("trades", f)])
            .unwrap_or_default(),
        (FeedMessage::Book(b), Sub::L2Book { coin, .. } | Sub::L4Book { coin })
            if b.venue.as_ref() == VENUE && b.symbol.as_ref() == coin.as_str() =>
        {
            // A `Clear`-led batch is a producer re-baseline. `l4Book` has no clear, so it becomes
            // another snapshot; `l2Book` is snapshot-per-update and needs no special case.
            let rebaseline = b
                .changes
                .first()
                .is_some_and(|c| c.action == BookAction::Clear);
            if matches!(sub, Sub::L4Book { .. }) && !rebaseline {
                return render_l4book_diff(b, coin)
                    .map(|f| vec![("l4Book", f)])
                    .unwrap_or_default();
            }
            let key = (b.venue.clone(), b.channel, b.instrument_id);
            let guard = crate::model::lock(books);
            guard
                .get(&key)
                .and_then(|acc| full_state(&key, acc, sub))
                .map(|f| vec![f])
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<Arc<FeedMessage>>,
    books: BookSnapshot,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();
    let mut subs: Vec<Sub> = Vec::new();
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval(HEARTBEAT);

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Text(txt))) => {
                    last_seen = Instant::now();
                    match parse_control(&txt) {
                        Ok(Control::Ping) => write.send(WsMessage::Text(
                            json(Envelope { channel: "pong", data: serde_json::Value::Null }).into(),
                        )).await?,
                        Ok(Control::Subscribe(sub)) => {
                            if subs.len() >= MAX_SUBS {
                                write.send(error_frame("Invalid subscription: max subscriptions reached")).await?;
                                continue;
                            }
                            // Only a *new* subscription bootstraps. A repeat adds no scope, and
                            // rendering anyway would let a client loop O(book) work — taken under the
                            // mutex the ingest emit path shares — without ever reaching `MAX_SUBS`.
                            let added = !subs.contains(&sub);
                            write.send(subscription_response("subscribe", &sub)).await?;
                            if added {
                                for (channel, frame) in bootstrap(&books, &sub) {
                                    sent(channel);
                                    write.send(WsMessage::Text(frame.into())).await?;
                                }
                                subs.push(sub);
                            }
                        }
                        Ok(Control::Unsubscribe(sub)) => {
                            subs.retain(|s| s != &sub);
                            write.send(subscription_response("unsubscribe", &sub)).await?;
                        }
                        Err(message) => write.send(error_frame(&message)).await?,
                    }
                }
                Some(Ok(WsMessage::Ping(p))) => { last_seen = Instant::now(); write.send(WsMessage::Pong(p)).await?; }
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
                    for sub in &subs {
                        for (channel, frame) in render(&m, sub, &books) {
                            sent(channel);
                            write.send(WsMessage::Text(frame.into())).await?;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("hl sink client lagged, dropped {n}");
                    // `l4Book` is incremental, so a dropped batch leaves this client's book
                    // permanently wrong. (`l2Book` self-heals on the next frame; re-bootstrapping it
                    // here just makes that immediate.)
                    for sub in &subs {
                        for (channel, frame) in bootstrap(&books, sub) {
                            sent(channel);
                            write.send(WsMessage::Text(frame.into())).await?;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}

/// The publisher acknowledges a sub/unsub by echoing the client's own frame back. The echo carries
/// the **normalized** subscription, so a client that omitted `nLevels` is told the depth it is
/// actually being served.
fn subscription_response(method: &'static str, sub: &Sub) -> WsMessage {
    #[derive(Serialize)]
    struct Echo<'a> {
        method: &'static str,
        subscription: &'a Sub,
    }
    WsMessage::Text(
        json(Envelope {
            channel: "subscriptionResponse",
            data: Echo {
                method,
                subscription: sub,
            },
        })
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{net::TcpListener, sync::broadcast, time::timeout};

    use super::*;
    use crate::model::{BookChange, NormalizedBook};

    fn l2_view(n_sig_figs: Option<u32>, mantissa: Option<u32>, n_levels: usize) -> L2View {
        L2View {
            n_sig_figs,
            mantissa,
            n_levels,
        }
    }

    /// A `NormalizedBook` of order-level changes, `Clear`-led so the accumulator baselines it.
    fn order_book(orders: Vec<(BookSide, f64, f64, u64)>) -> NormalizedBook {
        let mut changes = vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }];
        changes.extend(
            orders
                .into_iter()
                .map(|(side, price, size, order_id)| BookChange {
                    action: BookAction::Update,
                    side,
                    price,
                    size,
                    order_id,
                }),
        );
        book_with(changes)
    }

    fn book_with(changes: Vec<BookChange>) -> NormalizedBook {
        NormalizedBook {
            venue: VENUE.into(),
            source: VENUE.into(),
            source_id: 1,
            symbol: "BTC".into(),
            channel: 0,
            instrument_id: 7,
            changes,
            snapshot: false,
            last: true,
            source_ts_ns: 1_700_000_000_000_000_000,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    fn accumulated(orders: Vec<(BookSide, f64, f64, u64)>) -> BookAccumulator {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&order_book(orders));
        acc
    }

    fn key() -> (Arc<str>, u32, u32) {
        (Arc::from(VENUE), 0, 7)
    }

    fn normalized_trade(
        venue: &str,
        symbol: &str,
        price: f64,
        size: f64,
        buy: bool,
        trade_id: u64,
    ) -> NormalizedTrade {
        NormalizedTrade {
            venue: venue.into(),
            source: venue.into(),
            source_id: 1,
            symbol: symbol.into(),
            price,
            size,
            aggressor_side: if buy { Side::Buy } else { Side::Sell },
            trade_id,
            cumulative_volume: 0.0,
            source_ts_ns: 1_700_000_000_000_000_000,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    // --- Task 1: the subscription protocol ---

    /// The exact subscription frame NautilusTrader's Rust client sends. If this stops parsing, a
    /// stock Nautilus trader pointed at us silently receives nothing.
    #[test]
    fn parses_the_nautilus_l2book_subscription() {
        let f = r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":5}}"#;
        let Ok(Control::Subscribe(Sub::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            n_levels,
        })) = parse_control(f)
        else {
            panic!("must parse as an l2Book subscribe")
        };
        assert_eq!(coin, "BTC");
        assert_eq!(n_sig_figs, Some(5));
        assert_eq!(mantissa, Some(5));
        assert_eq!(
            n_levels, 20,
            "Hyperliquid's default when the field is absent"
        );
    }

    /// `nSigFigs` and `mantissa` are optional; absent means full precision.
    #[test]
    fn l2book_precision_fields_are_optional() {
        let f = r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}"#;
        let Ok(Control::Subscribe(Sub::L2Book {
            n_sig_figs,
            mantissa,
            ..
        })) = parse_control(f)
        else {
            panic!()
        };
        assert_eq!(n_sig_figs, None);
        assert_eq!(mantissa, None);
    }

    /// `nLevels` is the publisher's documented extension, capped at 100.
    #[test]
    fn n_levels_is_honoured_and_capped() {
        let f =
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nLevels":500}}"#;
        let Ok(Control::Subscribe(Sub::L2Book { n_levels, .. })) = parse_control(f) else {
            panic!()
        };
        assert_eq!(n_levels, 100, "clamped, not rejected");
    }

    #[test]
    fn parses_l4book_and_trades_and_rejects_unknown() {
        assert!(matches!(
            parse_control(
                r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#
            ),
            Ok(Control::Subscribe(Sub::L4Book { .. }))
        ));
        assert!(matches!(
            parse_control(
                r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#
            ),
            Ok(Control::Subscribe(Sub::Trades { .. }))
        ));
        assert!(parse_control(
            r#"{"method":"subscribe","subscription":{"type":"candle","coin":"BTC"}}"#
        )
        .is_err());
        assert!(matches!(
            parse_control(r#"{"method":"ping"}"#),
            Ok(Control::Ping)
        ));
    }

    /// An aggregation that does not exist is refused rather than coerced to the nearest one that
    /// does: a substituted bucket yields prices that look like the venue's and are not.
    #[test]
    fn an_impossible_aggregation_is_rejected() {
        for f in [
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":1}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":6}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","mantissa":5}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":4,"mantissa":5}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":3}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":""}}"#,
        ] {
            assert!(parse_control(f).is_err(), "must reject {f}");
        }
    }

    /// The error frame quotes the client's own text, so a huge frame must not be echoed whole.
    #[test]
    fn a_long_unparseable_frame_is_truncated_in_the_error() {
        let long = "x".repeat(10_000);
        let Err(message) = parse_control(&long) else {
            panic!("must not parse")
        };
        assert!(message.len() < MAX_ECHOED_ERROR + 100);
    }

    /// Nautilus heartbeats with `{"method":"ping"}` and parses only `{"channel":"pong"}`.
    #[test]
    fn ping_is_answered_with_the_pong_channel() {
        let f = json(Envelope {
            channel: "pong",
            data: serde_json::Value::Null,
        });
        assert_eq!(f, r#"{"channel":"pong","data":null}"#);
    }

    // --- Task 2: l2Book ---

    /// The envelope and field shapes NautilusTrader parses: `channel`/`data`, `levels` as
    /// [bids, asks], `px`/`sz` as strings, `n` as the order count at that price.
    #[test]
    fn l2book_renders_the_shape_nautilus_parses() {
        let acc = accumulated(vec![
            (BookSide::Bid, 100.5, 5.0, 1),
            (BookSide::Bid, 100.5, 3.0, 2),
            (BookSide::Bid, 99.0, 1.0, 3),
            (BookSide::Ask, 101.0, 2.0, 4),
        ]);

        let json = render_l2book(&acc, "BTC", l2_view(None, None, 20), 1_700_000_000_000);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["channel"], "l2Book");
        assert_eq!(v["data"]["coin"], "BTC");
        assert_eq!(v["data"]["time"], 1_700_000_000_000u64);

        let bids = &v["data"]["levels"][0];
        assert_eq!(bids[0]["px"], "100.5", "price is a string");
        assert_eq!(bids[0]["sz"], "8", "two orders at one price aggregate");
        assert_eq!(bids[0]["n"], 2, "and the order count comes with it");
        assert_eq!(bids[1]["px"], "99");
        assert_eq!(v["data"]["levels"][1][0]["px"], "101");
    }

    /// Levels are capped by the subscription's `nLevels`, best-first on each side.
    #[test]
    fn l2book_truncates_to_n_levels_best_first() {
        let acc = accumulated(
            (0u32..30)
                .map(|i| (BookSide::Bid, 100.0 - f64::from(i), 1.0, u64::from(i) + 1))
                .collect(),
        );
        let json = render_l2book(&acc, "BTC", l2_view(None, None, 5), 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["data"]["levels"][0].as_array().unwrap().len(), 5);
        assert_eq!(v["data"]["levels"][0][0]["px"], "100", "best bid first");
    }

    /// A whole number renders with no decimal point and a fraction with no trailing zeros — the
    /// venue's own formatting, and the only form a string-parsing consumer is guaranteed to accept.
    #[test]
    fn numbers_render_as_plain_decimal_strings() {
        assert_eq!(num(100.0), "100");
        assert_eq!(num(100.5), "100.5");
        assert_eq!(num(0.0), "0");
        assert_eq!(num(0.00000001), "0.00000001");
        assert_eq!(num(1_000_000.0), "1000000", "no exponent");
    }

    // --- Task 3: nSigFigs / mantissa ---

    /// Significant-figure aggregation merges levels that round to the same bucket, and the merged
    /// level carries the summed size and the summed order count.
    #[test]
    fn n_sig_figs_merges_colliding_levels() {
        let acc = accumulated(vec![
            (BookSide::Bid, 12345.0, 1.0, 1),
            (BookSide::Bid, 12346.0, 2.0, 2),
            (BookSide::Bid, 12300.0, 4.0, 3),
        ]);
        let json = render_l2book(&acc, "BTC", l2_view(Some(3), None, 20), 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let bids = v["data"]["levels"][0].as_array().unwrap();
        assert_eq!(
            bids.len(),
            1,
            "12345, 12346 and 12300 all round to 12300 at 3 sig figs"
        );
        assert_eq!(bids[0]["sz"], "7");
        assert_eq!(bids[0]["n"], 3);
    }

    /// No aggregation requested means full precision — every distinct price is its own level.
    #[test]
    fn absent_n_sig_figs_is_full_precision() {
        let acc = accumulated(vec![
            (BookSide::Bid, 12345.0, 1.0, 1),
            (BookSide::Bid, 12346.0, 2.0, 2),
        ]);
        let json = render_l2book(&acc, "BTC", l2_view(None, None, 20), 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["data"]["levels"][0].as_array().unwrap().len(), 2);
    }

    /// Bids round down and asks round up, so aggregation never invents a price better than the book
    /// holds — the inside market can only widen. Values from the publisher's own arithmetic.
    #[test]
    fn aggregation_rounds_away_from_the_mid() {
        assert_eq!(aggregate_price(106_217.0, true, Some(5), None), 106_210.0);
        assert_eq!(aggregate_price(106_217.0, false, Some(5), None), 106_220.0);
        // `mantissa` widens the bucket to multiples of 5 in the fifth significant digit.
        assert_eq!(
            aggregate_price(106_217.0, true, Some(5), Some(5)),
            106_200.0
        );
        assert_eq!(
            aggregate_price(106_217.0, false, Some(5), Some(5)),
            106_250.0
        );
    }

    /// A price with fewer significant digits than requested is left alone rather than mangled.
    #[test]
    fn aggregation_of_a_small_price_is_a_no_op() {
        assert_eq!(aggregate_price(0.5, true, Some(5), None), 0.5);
        assert_eq!(aggregate_price(0.0, true, Some(5), None), 0.0);
    }

    // --- Task 4: l4Book ---

    /// Subscribing to l4Book gets the whole book first, order by order, with the venue's real ids.
    /// The envelope is the publisher's externally-tagged `{"Snapshot":{…}}`.
    #[test]
    fn l4book_subscribe_sends_the_whole_book_with_order_ids() {
        let acc = accumulated(vec![
            (BookSide::Bid, 100.0, 5.0, 11),
            (BookSide::Ask, 101.0, 2.0, 22),
        ]);
        let v: serde_json::Value =
            serde_json::from_str(&render_l4book_snapshot(&acc, &key(), "BTC")).unwrap();
        assert_eq!(v["channel"], "l4Book");
        let snap = &v["data"]["Snapshot"];
        assert_eq!(snap["coin"], "BTC");
        assert_eq!(snap["time"], 1_700_000_000_000u64);
        let bids = snap["levels"][0].as_array().unwrap();
        assert_eq!(
            bids[0]["oid"], 11,
            "the venue's order id, not a synthesized one"
        );
        assert_eq!(
            bids[0]["limitPx"], "100",
            "the publisher names it limitPx, not px"
        );
        assert_eq!(bids[0]["sz"], "5");
        assert_eq!(bids[0]["side"], "B");
        assert_eq!(snap["levels"][1][0]["oid"], 22);
        assert_eq!(snap["levels"][1][0]["side"], "A");
    }

    /// After the snapshot, each incremental book message becomes an order diff.
    #[test]
    fn l4book_forwards_order_diffs_after_the_snapshot() {
        let b = book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }]);
        let v: serde_json::Value =
            serde_json::from_str(&render_l4book_diff(&b, "BTC").unwrap()).unwrap();
        let up = &v["data"]["Updates"];
        assert_eq!(up["book_diffs"][0]["oid"], 11);
        assert_eq!(up["book_diffs"][0]["px"], "100");
        assert_eq!(up["book_diffs"][0]["raw_book_diff"]["new"]["sz"], "3");
        assert_eq!(up["order_statuses"].as_array().unwrap().len(), 0);
    }

    /// A gone order is `remove`, whether the producer said so with a delete or with a zero size.
    /// Rendering it as a resting order of size zero leaves a phantom in the consumer's book.
    #[test]
    fn l4book_renders_a_gone_order_as_remove() {
        for change in [
            BookChange {
                action: BookAction::Delete,
                side: BookSide::Bid,
                price: 100.0,
                size: 0.0,
                order_id: 11,
            },
            BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 100.0,
                size: 0.0,
                order_id: 11,
            },
        ] {
            let b = book_with(vec![change]);
            let v: serde_json::Value =
                serde_json::from_str(&render_l4book_diff(&b, "BTC").unwrap()).unwrap();
            assert_eq!(
                v["data"]["Updates"]["book_diffs"][0]["raw_book_diff"],
                "remove"
            );
        }
    }

    /// A message for another venue or coin renders nothing — the sink is Hyperliquid-scoped.
    #[test]
    fn l4book_ignores_other_venues_and_coins() {
        let mut b = book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }]);
        assert!(render_l4book_diff(&b, "ETH").is_none());
        b.venue = "PHOENIX".into();
        assert!(render_l4book_diff(&b, "BTC").is_none());
    }

    /// A price-aggregated change carries no order identity, and `oid: 0` reads to an L3 consumer as
    /// "aggregate me" — silently degrading its book to L2. It is skipped, not emitted as zero.
    #[test]
    fn l4book_never_emits_a_zero_order_id() {
        let b = book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 0,
        }]);
        assert!(render_l4book_diff(&b, "BTC").is_none());
    }

    // --- Task 5: trades ---

    /// Our normalized trade becomes Hyperliquid's trade envelope: string price and size, the
    /// aggressor side as the venue spells it, and the venue's own trade id.
    #[test]
    fn trades_render_the_hyperliquid_envelope() {
        let t = normalized_trade(VENUE, "BTC", 100.5, 2.0, true, 424_242);
        let v: serde_json::Value = serde_json::from_str(&render_trade(&t, "BTC").unwrap()).unwrap();
        assert_eq!(v["channel"], "trades");
        let d = &v["data"][0];
        assert_eq!(d["coin"], "BTC");
        assert_eq!(d["px"], "100.5");
        assert_eq!(d["sz"], "2");
        assert_eq!(d["side"], "B", "Hyperliquid spells the aggressor side B/A");
        assert_eq!(d["tid"], 424_242u64);
        assert_eq!(d["time"], 1_700_000_000_000u64);
        assert_eq!(d["users"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn a_sell_aggressor_is_spelled_a() {
        let t = normalized_trade(VENUE, "BTC", 1.0, 1.0, false, 1);
        let v: serde_json::Value = serde_json::from_str(&render_trade(&t, "BTC").unwrap()).unwrap();
        assert_eq!(v["data"][0]["side"], "A");
    }

    #[test]
    fn trades_from_another_venue_render_nothing() {
        let t = normalized_trade("PHOENIX", "BTC", 1.0, 1.0, true, 1);
        assert!(render_trade(&t, "BTC").is_none());
    }

    /// Hyperliquid's schema has no "unknown" aggressor, and `side` is the one field on this channel a
    /// consumer acts on directionally — so a print without one is dropped rather than guessed.
    #[test]
    fn a_trade_with_an_unknown_aggressor_is_dropped() {
        let mut t = normalized_trade(VENUE, "BTC", 1.0, 1.0, true, 1);
        t.aggressor_side = Side::Unknown;
        assert!(render_trade(&t, "BTC").is_none());
    }

    // --- Task 7: streaming ---

    /// A market accumulated mid-stream holds only the levels that have moved since. Publishing it on
    /// either book channel would tell the client those are the whole book — `l2Book` because every
    /// frame replaces the consumer's book wholesale, `l4Book` because a snapshot claims completeness.
    #[test]
    fn a_market_that_never_baselined_is_not_published() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 1.0,
            order_id: 1,
        }]));
        assert!(!acc.baselined());
        let books = Arc::new(Mutex::new(HashMap::from([(key(), acc)])));
        assert!(bootstrap(
            &books,
            &Sub::L2Book {
                coin: "BTC".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: 20
            }
        )
        .is_empty());
        assert!(bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }).is_empty());
    }

    async fn spawn(
        books: HashMap<(Arc<str>, u32, u32), BookAccumulator>,
    ) -> (
        broadcast::Sender<Arc<FeedMessage>>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let srv = tokio::spawn(serve(listener, tx.clone(), Arc::new(Mutex::new(books))));
        (tx, addr, srv)
    }

    async fn next_text<S>(ws: &mut S, within: Duration) -> Option<String>
    where
        S: StreamExt<Item = std::result::Result<WsMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        timeout(within, async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => return t.to_string(),
                    Some(Ok(_)) => continue,
                    other => panic!("stream ended: {other:?}"),
                }
            }
        })
        .await
        .ok()
    }

    /// End to end over a real socket: subscribe, receive the acknowledgement and the l4Book
    /// snapshot, then a diff for a subsequent book message — and nothing for a coin the client did
    /// not subscribe to.
    #[tokio::test]
    async fn a_client_receives_its_subscription_and_nothing_else() {
        let books = HashMap::from([(key(), accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]))]);
        let (tx, addr, srv) = spawn(books).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#.into(),
        ))
        .await
        .unwrap();

        let ack = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let ack: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack["channel"], "subscriptionResponse");
        assert_eq!(ack["data"]["method"], "subscribe");
        assert_eq!(ack["data"]["subscription"]["type"], "l4Book");

        let snap = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let snap: serde_json::Value = serde_json::from_str(&snap).unwrap();
        assert_eq!(snap["data"]["Snapshot"]["levels"][0][0]["oid"], 11);

        // An unsubscribed coin, then the subscribed one: exactly one further frame must arrive, and
        // it must be the BTC diff.
        let mut other = book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 1.0,
            size: 1.0,
            order_id: 99,
        }]);
        other.symbol = "ETH".into();
        let _ = tx.send(Arc::new(FeedMessage::Book(other)));
        let _ = tx.send(Arc::new(FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }]))));

        let diff = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let diff: serde_json::Value = serde_json::from_str(&diff).unwrap();
        assert_eq!(diff["data"]["Updates"]["book_diffs"][0]["oid"], 11);
        assert_eq!(
            diff["data"]["Updates"]["book_diffs"][0]["raw_book_diff"]["new"]["sz"],
            "3"
        );
        assert!(next_text(&mut ws, Duration::from_millis(300))
            .await
            .is_none());
        srv.abort();
    }

    /// An unrecognized frame is answered with Hyperliquid's error envelope, and a ping with a pong.
    #[tokio::test]
    async fn unknown_frames_get_the_error_envelope() {
        let (_tx, addr, srv) = spawn(HashMap::new()).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        ws.send(WsMessage::Text("not json".into())).await.unwrap();
        let f = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["channel"], "error");
        assert!(
            v["data"].is_string(),
            "the publisher's error data is a bare string"
        );

        ws.send(WsMessage::Text(r#"{"method":"ping"}"#.into()))
            .await
            .unwrap();
        let f = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&f).unwrap()["channel"],
            "pong"
        );
        srv.abort();
    }

    /// A `Clear`-led batch is a producer re-baseline. `l4Book` has no clear, so an l4 subscriber must
    /// receive a fresh snapshot rather than diffs that leave its stale orders resting forever.
    #[tokio::test]
    async fn a_rebaseline_becomes_an_l4book_snapshot() {
        let books = HashMap::from([(key(), accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]))]);
        let (tx, addr, srv) = spawn(books).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#.into(),
        ))
        .await
        .unwrap();
        next_text(&mut ws, Duration::from_secs(2)).await.unwrap(); // ack
        next_text(&mut ws, Duration::from_secs(2)).await.unwrap(); // snapshot

        let _ = tx.send(Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            5.0,
            11,
        )]))));
        let f = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&f).unwrap();
        assert!(
            v["data"]["Snapshot"].is_object(),
            "a re-baseline must re-snapshot, got {f}"
        );
        srv.abort();
    }

    /// A trades subscriber receives prints and no book frames.
    #[tokio::test]
    async fn a_trades_subscriber_receives_only_prints() {
        let (tx, addr, srv) = spawn(HashMap::new()).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#.into(),
        ))
        .await
        .unwrap();
        next_text(&mut ws, Duration::from_secs(2)).await.unwrap(); // ack

        let _ = tx.send(Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            1.0,
            1,
        )]))));
        let _ = tx.send(Arc::new(FeedMessage::Trade(normalized_trade(
            VENUE, "BTC", 100.5, 2.0, true, 7,
        ))));
        let f = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["channel"], "trades");
        assert_eq!(v["data"][0]["tid"], 7u64);
        srv.abort();
    }

    /// The golden fixture `tests/hyperliquid_sink_shapes.rs` pins is generated from this renderer,
    /// so the pin tracks what the sink actually emits rather than what we wrote down.
    #[test]
    fn golden_l2book_frame_matches_the_committed_fixture() {
        let acc = accumulated(vec![
            (BookSide::Bid, 100.5, 5.0, 1),
            (BookSide::Bid, 100.5, 3.0, 2),
            (BookSide::Bid, 99.0, 1.0, 3),
            (BookSide::Ask, 101.0, 2.0, 4),
        ]);
        let frame = render_l2book(&acc, "BTC", l2_view(None, None, 20), 1_700_000_000_000);
        assert_eq!(
            frame,
            include_str!("../../tests/fixtures/hl_l2book_golden.json").trim_end(),
            "regenerate tests/fixtures/hl_l2book_golden.json from this renderer"
        );
    }
}
