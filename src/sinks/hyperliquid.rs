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
        now_ns, BookAccumulator, BookAction, BookSide, BookSnapshot, CountedLevel, FeedMessage,
        NormalizedBook, NormalizedTrade, ReplayScope, Side,
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
const MAX_INBOUND_PER_MIN: u32 = 600;
const HEARTBEAT: Duration = Duration::from_secs(20);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on an inbound frame. Control frames here are tens of bytes; tungstenite's 64 MiB default would
/// let `MAX_CLIENTS` peers buffer gigabytes before a single byte is parsed. Read-path only, so the
/// sink's own large `l4Book` snapshots are unaffected.
fn inbound_limits() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(64 * 1024))
        .max_frame_size(Some(64 * 1024))
}

/// A market in the shared replay map: `(venue, channel, instrument_id)`.
type MarketKey = (Arc<str>, u32, u32);

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
enum Sub {
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

/// The `l2Book` view a subscription asked for: the significant-figure bucket, its mantissa
/// refinement, and how deep to publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct L2View {
    n_sig_figs: Option<u32>,
    mantissa: Option<u32>,
    n_levels: usize,
}

/// One recognized inbound control frame.
#[derive(Debug, PartialEq, Eq)]
enum Control {
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
fn parse_control(text: &str) -> Result<Control, String> {
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
            // coarser `nSigFigs` and the venue rejects the pair rather than ignoring it. `1` is the
            // identity bucket the venue accepts; our own publisher refuses it, so we are the laxer.
            match (n_sig_figs, mantissa) {
                (_, None) => {}
                (Some(5), Some(1 | 2 | 5)) => {}
                (_, Some(m)) => {
                    return Err(format!(
                        "Invalid subscription: mantissa {m} must be 1, 2 or 5 at nSigFigs 5"
                    ))
                }
            }
            // Zero is refused rather than clamped up: it renders as an empty book on every frame,
            // which an `l2Book` consumer applies as "the book is now empty" and then holds forever
            // with nothing to explain it.
            if n_levels == Some(0) {
                return Err("Invalid subscription: nLevels must be > 0".to_string());
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
    // `normalize` bounds `n` to 2..=5, which bounds the exponent to 18 — but the fallback keeps a
    // caller that skipped validation from panicking in debug and wrapping in release.
    let Some(pow) = 10u64.checked_pow(digits.saturating_sub(n)) else {
        return px;
    };
    let inc = u64::from(mantissa.unwrap_or(1)).saturating_mul(pow);
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
fn bucket_side(levels: &[CountedLevel], is_bid: bool, view: L2View) -> Vec<CountedLevel> {
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
/// Nautilus clears and rebuilds from every frame, so the caller must only ever hand this the folded
/// levels of a market that is [`BookAccumulator::baselined`].
///
/// Takes the fold rather than the accumulator because the fold is O(resting orders) and independent
/// of the view: one is computed per market per message and shared by every subscription of it.
fn render_l2book(
    bids: &[CountedLevel],
    asks: &[CountedLevel],
    coin: &str,
    view: L2View,
    time_ms: u64,
) -> String {
    let level = |(px, sz, n): (f64, f64, u32)| Level {
        px: num(px),
        sz: num(sz),
        n,
    };
    let data = L2Data {
        coin,
        time: time_ms,
        levels: [
            bucket_side(bids, true, view)
                .into_iter()
                .map(level)
                .collect(),
            bucket_side(asks, false, view)
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

fn l4_order<'a>(coin: &'a str, side: BookSide, px: f64, sz: f64, oid: u64) -> L4Order<'a> {
    L4Order {
        user: None,
        coin,
        side: side_code(side),
        limit_px: num(px),
        sz: num(sz),
        oid,
        // Not the book's event time: that is one instant shared by every order in a snapshot, which a
        // consumer ranking queue priority or ageing orders would read as a real placement time. The
        // wire carries no per-order timestamp, so 0 says so.
        timestamp: 0,
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
///
/// Takes the materialized order set rather than the accumulator, so the caller can produce it under
/// the shared lock and release before any of this runs.
fn render_l4book_snapshot(b: &NormalizedBook, coin: &str) -> String {
    let time = ms_or_now(b.source_ts_ns);
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    for c in &b.changes {
        // Skips the leading `clear` and any price-aggregated level: neither names an order, and
        // `oid: 0` reads to an L3 consumer as "aggregate me", silently degrading its book to L2.
        if c.order_id == 0 {
            continue;
        }
        let order = l4_order(coin, c.side, c.price, c.size, c.order_id);
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
fn render_l4book_diff(b: &NormalizedBook, coin: &str) -> Option<String> {
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
fn render_trade(t: &NormalizedTrade, coin: &str) -> Option<String> {
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
        // Never propagate an accept error: this task's `Err` reaches `main`'s `select!` and would
        // exit the process — tunnel, receivers and every other sink with it — over a transient
        // `ECONNABORTED`/`EMFILE` on an opt-in compatibility port. That is the same outcome the
        // non-fatal bind above exists to avoid.
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!("hl sink accept failed: {e}");
                continue;
            }
        };
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

/// Whether this market's accumulated book may be published as full state on either book channel.
///
/// `baselined` is load-bearing on **both**: an `l2Book` frame replaces a consumer's book wholesale
/// and an `l4Book` snapshot claims completeness, so an accumulator seeded mid-stream — holding only
/// the levels that have moved since — must be withheld rather than published as whole. `is_order_level`
/// is what keeps that honest for a market whose changes are price-aggregated: this sink reads only the
/// order population, so such a market would render as an *empty* book, telling the consumer to discard
/// levels the bridge holds.
fn publishable(key: &MarketKey, acc: &BookAccumulator, coin: &str) -> bool {
    key.0.as_ref() == VENUE
        && acc.symbol().as_ref() == coin
        && acc.baselined()
        && acc.is_order_level()
}

/// Copy what a render needs out of one market, under the shared lock and no longer — the arbiter's
/// emit path takes this same mutex on every published batch. `take` must be the minimum each channel
/// needs (`price_fold` for `l2Book`, `to_book` for `l4Book`) and never the accumulator itself, whose
/// clone is O(resting orders); the decimal formatting and the JSON run after the guard drops.
fn take_market<T>(
    books: &BookSnapshot,
    key: &MarketKey,
    coin: &str,
    take: impl FnOnce(&BookAccumulator) -> T,
) -> Option<T> {
    let guard = crate::model::lock(books);
    guard
        .get(key)
        .filter(|acc| publishable(key, acc, coin))
        .map(take)
}

/// The same copy-out for every market of this venue matching `coin`. Scans the map, so it is for a
/// subscribe or a recovery — never the steady stream, which resolves one market by its own key.
fn take_markets<T>(
    books: &BookSnapshot,
    coin: &str,
    take: impl Fn(&MarketKey, &BookAccumulator) -> T,
) -> Vec<T> {
    let guard = crate::model::lock(books);
    guard
        .iter()
        .filter(|(key, acc)| publishable(key, acc, coin))
        .map(|(key, acc)| take(key, acc))
        .collect()
}

fn l2_view(n_sig_figs: Option<u32>, mantissa: Option<u32>, n_levels: usize) -> L2View {
    L2View {
        n_sig_figs,
        mantissa,
        n_levels,
    }
}

/// Bootstrap `sub` from current state.
fn bootstrap(books: &BookSnapshot, sub: &Sub) -> Vec<(&'static str, String)> {
    match sub {
        // Prints are point-in-time: there is nothing to bootstrap, and no reason to take the lock.
        Sub::Trades { .. } => Vec::new(),
        Sub::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            n_levels,
        } => {
            let view = l2_view(*n_sig_figs, *mantissa, *n_levels);
            take_markets(books, coin, |_, acc| {
                (acc.price_fold(), ms_or_now(acc.source_ts_ns()))
            })
            .into_iter()
            .map(|((bids, asks), time)| ("l2Book", render_l2book(&bids, &asks, coin, view, time)))
            .collect()
        }
        Sub::L4Book { coin } => take_markets(books, coin, |key, acc| {
            acc.to_book(&key.0, key.1, key.2, ReplayScope::Orders)
        })
        .into_iter()
        .map(|b| ("l4Book", render_l4book_snapshot(&b, coin)))
        .collect(),
    }
}

/// Every frame one broadcast message produces across a client's subscriptions.
///
/// Filtering on venue and coin happens before any rendering, and the book state is resolved **once**
/// per message: one lock acquisition, one `price_fold` shared by every `l2Book` subscription of that
/// market (the fold is O(resting orders) and independent of the view, so folding per subscription
/// would let one client multiply a 44k-order book by `MAX_SUBS`).
fn frames(m: &FeedMessage, subs: &[Sub], books: &BookSnapshot) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    match m {
        FeedMessage::Trade(t) => {
            for sub in subs {
                if let Sub::Trades { coin } = sub {
                    out.extend(render_trade(t, coin).map(|f| ("trades", f)));
                }
            }
        }
        FeedMessage::Book(b) if b.venue.as_ref() == VENUE => {
            let coin = b.symbol.as_ref();
            let l2 = subs
                .iter()
                .any(|s| matches!(s, Sub::L2Book { coin: c, .. } if c == coin));
            let l4 = subs
                .iter()
                .any(|s| matches!(s, Sub::L4Book { coin: c } if c == coin));
            // A `Clear`-led batch is a producer re-baseline. `l4Book` has no clear, so it becomes
            // another snapshot; `l2Book` is snapshot-per-update and needs no special case.
            let rebaseline = b
                .changes
                .first()
                .is_some_and(|c| c.action == BookAction::Clear);
            let key = (b.venue.clone(), b.channel, b.instrument_id);
            // Only these two cases need book state; an ordinary `l4Book` diff is rendered from the
            // batch alone and takes no lock.
            let (fold, snapshot) = (l2 || (l4 && rebaseline))
                .then(|| {
                    take_market(books, &key, coin, |acc| {
                        (
                            l2.then(|| (acc.price_fold(), ms_or_now(acc.source_ts_ns()))),
                            (l4 && rebaseline)
                                .then(|| acc.to_book(&key.0, key.1, key.2, ReplayScope::Orders)),
                        )
                    })
                })
                .flatten()
                .unwrap_or((None, None));
            for sub in subs {
                match sub {
                    Sub::L2Book {
                        coin: c,
                        n_sig_figs,
                        mantissa,
                        n_levels,
                    } if c == coin => {
                        if let Some(((bids, asks), time)) = &fold {
                            let view = l2_view(*n_sig_figs, *mantissa, *n_levels);
                            out.push(("l2Book", render_l2book(bids, asks, c, view, *time)));
                        }
                    }
                    Sub::L4Book { coin: c } if c == coin => {
                        if rebaseline {
                            out.extend(
                                snapshot
                                    .as_ref()
                                    .map(|b| ("l4Book", render_l4book_snapshot(b, c))),
                            );
                        } else {
                            out.extend(render_l4book_diff(b, c).map(|f| ("l4Book", f)));
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
}

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<Arc<FeedMessage>>,
    books: BookSnapshot,
) -> Result<()> {
    // The client slot is taken before this point, and the idle reaper only starts after it, so a peer
    // that connects and never handshakes would hold one of `MAX_CLIENTS` indefinitely.
    let ws = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_async_with_config(stream, Some(inbound_limits())),
    )
    .await
    .map_err(|_| anyhow::anyhow!("handshake timed out"))??;
    let (mut write, mut read) = ws.split();
    let mut subs: Vec<Sub> = Vec::new();
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval(HEARTBEAT);
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Text(txt))) => {
                    last_seen = Instant::now();
                    // Inbound rate limit (per rolling minute). This is what bounds the cost of a
                    // control frame: a subscribe renders a market's whole book, so without a cap an
                    // `unsubscribe`/`subscribe` loop is an output amplifier — the `added` guard below
                    // only suppresses an identical *repeat*.
                    if win_start.elapsed() >= Duration::from_secs(60) {
                        win_start = Instant::now();
                        win_count = 0;
                    }
                    win_count += 1;
                    if win_count > MAX_INBOUND_PER_MIN {
                        write.send(error_frame("inbound rate limit exceeded")).await?;
                        break;
                    }
                    match parse_control(&txt) {
                        Ok(Control::Ping) => write.send(WsMessage::Text(
                            json(Envelope { channel: "pong", data: serde_json::Value::Null }).into(),
                        )).await?,
                        Ok(Control::Subscribe(sub)) => {
                            if subs.len() >= MAX_SUBS {
                                write.send(error_frame("Invalid subscription: max subscriptions reached")).await?;
                                continue;
                            }
                            // Only a *new* subscription bootstraps: a repeat adds no scope, and
                            // rendering a whole book again for it is free work a client could loop
                            // without ever reaching `MAX_SUBS`. The rate limit above is what bounds
                            // the unsubscribe-then-resubscribe variant this cannot see.
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
                    for (channel, frame) in frames(&m, &subs, &books) {
                        sent(channel);
                        write.send(WsMessage::Text(frame.into())).await?;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("hl sink client lagged, dropped {n}");
                    // `l4Book` only: it is incremental, so a dropped batch leaves this client's book
                    // permanently wrong. `l2Book` self-heals on its next frame, and re-rendering it
                    // here would spend the most work on the one path where the client is already
                    // behind — which is what makes it lag again.
                    for sub in subs.iter().filter(|s| matches!(s, Sub::L4Book { .. })) {
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

    /// The order set the sink copies out under the lock, in the form the renderer receives it.
    fn order_set(acc: &BookAccumulator) -> NormalizedBook {
        acc.to_book(&key().0, key().1, key().2, ReplayScope::Orders)
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
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nLevels":0}}"#,
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":""}}"#,
        ] {
            assert!(parse_control(f).is_err(), "must reject {f}");
        }
    }

    /// Hyperliquid accepts a `mantissa` of 1, 2 or 5, and only at `nSigFigs` 5.
    #[test]
    fn every_legal_mantissa_is_accepted_and_the_rest_refused() {
        for m in [1, 2, 5] {
            let f = format!(
                r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":{m}}}}}"#
            );
            let Ok(Control::Subscribe(Sub::L2Book { mantissa, .. })) = parse_control(&f) else {
                panic!("mantissa {m} must be accepted")
            };
            assert_eq!(mantissa, Some(m));
        }
        for m in [0, 3, 4, 6, 10] {
            let f = format!(
                r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":{m}}}}}"#
            );
            assert!(parse_control(&f).is_err(), "mantissa {m} must be refused");
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

        let (bids, asks) = acc.price_fold();
        let json = render_l2book(
            &bids,
            &asks,
            "BTC",
            l2_view(None, None, 20),
            1_700_000_000_000,
        );
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
        let (bids, asks) = acc.price_fold();
        let json = render_l2book(&bids, &asks, "BTC", l2_view(None, None, 5), 0);
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
        let (bids, asks) = acc.price_fold();
        let json = render_l2book(&bids, &asks, "BTC", l2_view(Some(3), None, 20), 0);
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
        let (bids, asks) = acc.price_fold();
        let json = render_l2book(&bids, &asks, "BTC", l2_view(None, None, 20), 0);
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
            serde_json::from_str(&render_l4book_snapshot(&order_set(&acc), "BTC")).unwrap();
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

    /// An order carries no timestamp on the market-by-order wire. Stamping the book's event time
    /// would give every order in a snapshot the same plausible, wrong placement time — which a
    /// consumer ranking queue priority reads as real.
    #[test]
    fn l4book_orders_carry_no_fabricated_timestamp() {
        let acc = accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]);
        let v: serde_json::Value =
            serde_json::from_str(&render_l4book_snapshot(&order_set(&acc), "BTC")).unwrap();
        assert_eq!(v["data"]["Snapshot"]["levels"][0][0]["timestamp"], 0);
        assert_eq!(
            v["data"]["Snapshot"]["levels"][0][0]["user"],
            serde_json::Value::Null
        );
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

    /// This sink reads only the accumulator's *order* population, so a price-aggregated market would
    /// render as an empty book — telling an `l2Book` consumer to discard levels the bridge holds.
    /// Withhold it instead.
    #[test]
    fn a_price_aggregated_market_is_not_published() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book_with(vec![
            BookChange {
                action: BookAction::Clear,
                side: BookSide::Both,
                price: 0.0,
                size: 0.0,
                order_id: 0,
            },
            BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 100.0,
                size: 1.0,
                order_id: 0,
            },
        ]));
        assert!(acc.baselined() && !acc.is_order_level());
        let books = Arc::new(Mutex::new(HashMap::from([(key(), acc)])));
        assert!(bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }).is_empty());
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
    }

    /// An order-level market that empties is still order-level. Derived from the current population
    /// instead, an emptied book stops publishing on both channels — permanently for `l4Book`, whose
    /// snapshot is only sent once this gate passes.
    #[test]
    fn an_emptied_order_book_keeps_publishing_on_both_channels() {
        let mut acc = accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]);
        acc.apply(&book_with(vec![BookChange {
            action: BookAction::Delete,
            side: BookSide::Bid,
            price: 100.0,
            size: 0.0,
            order_id: 11,
        }]));
        assert!(acc.price_fold().0.is_empty(), "the book is now empty");
        assert!(acc.baselined() && acc.is_order_level());

        acc.apply(&book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 101.0,
            size: 2.0,
            order_id: 12,
        }]));
        let books = Arc::new(Mutex::new(HashMap::from([(key(), acc)])));
        assert_eq!(
            bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }).len(),
            1
        );
        assert_eq!(
            bootstrap(
                &books,
                &Sub::L2Book {
                    coin: "BTC".into(),
                    n_sig_figs: None,
                    mantissa: None,
                    n_levels: 20
                }
            )
            .len(),
            1
        );
    }

    /// One book message serves every subscription of that market — and the `l2Book` fold behind them
    /// is computed once, not once per subscription, so a client cannot multiply a whole book by
    /// `MAX_SUBS`.
    #[test]
    fn one_book_message_serves_every_subscription_of_the_market() {
        let books = Arc::new(Mutex::new(HashMap::from([(
            key(),
            accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]),
        )])));
        let subs = vec![
            Sub::L2Book {
                coin: "BTC".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: 20,
            },
            Sub::L4Book { coin: "BTC".into() },
            Sub::Trades { coin: "BTC".into() },
            Sub::L2Book {
                coin: "ETH".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: 20,
            },
        ];
        let m = FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }]));
        let out = frames(&m, &subs, &books);
        assert_eq!(
            out.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
            vec!["l2Book", "l4Book"]
        );
    }

    /// `Arbiter::apply_book_replay` takes this same mutex on every published batch, so only the
    /// copy-out may run under it — the formatting and the JSON, both O(book), must not. Pinned by
    /// holding the map for the whole render: a render that reached for it would time out here.
    #[test]
    fn rendering_does_not_hold_the_shared_book_lock() {
        let acc = accumulated(
            (0u32..5_000)
                .map(|i| {
                    (
                        BookSide::Bid,
                        100.0 - f64::from(i) / 100.0,
                        1.0,
                        u64::from(i) + 1,
                    )
                })
                .collect(),
        );
        let books: BookSnapshot = Arc::new(Mutex::new(HashMap::from([(key(), acc)])));

        // Everything the two book channels need, copied out under the lock — never the accumulator.
        let (fold, time) = take_market(&books, &key(), "BTC", |a| {
            (a.price_fold(), ms_or_now(a.source_ts_ns()))
        })
        .unwrap();
        let snapshot = take_market(&books, &key(), "BTC", |a| {
            a.to_book(&key().0, key().1, key().2, ReplayScope::Orders)
        })
        .unwrap();

        // An ingest apply in flight, holding the map for as long as the renders below take.
        let mut held = crate::model::lock(&books);
        held.insert((Arc::from(VENUE), 0, 8), BookAccumulator::new("ETH".into()));

        let (tx, rx) = std::sync::mpsc::channel();
        let rendering = std::thread::spawn(move || {
            let l2 = render_l2book(&fold.0, &fold.1, "BTC", l2_view(None, None, 100), time);
            tx.send((l2, render_l4book_snapshot(&snapshot, "BTC")))
        });
        let (l2, l4) = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("a render must not wait on the shared book lock");
        drop(held);
        rendering.join().unwrap().unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&l2).unwrap()["data"]["levels"][0]
                .as_array()
                .unwrap()
                .len(),
            100
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&l4).unwrap()["data"]["Snapshot"]["levels"]
                [0]
            .as_array()
            .unwrap()
            .len(),
            5_000
        );
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

    /// A control frame is cheap to send and can cost a whole book to answer, so the per-minute cap is
    /// what bounds the amplification. Crossing it ends the connection rather than throttling, which is
    /// what the normalized sink does.
    #[tokio::test]
    async fn crossing_the_inbound_rate_limit_ends_the_connection() {
        let (_tx, addr, srv) = spawn(HashMap::new()).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        for _ in 0..=MAX_INBOUND_PER_MIN {
            if ws
                .send(WsMessage::Text(r#"{"method":"ping"}"#.into()))
                .await
                .is_err()
            {
                break;
            }
        }
        // Drain until the rate-limit error arrives (every frame before it is a pong).
        let limited = timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => {
                        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                        if v["channel"] == "error" {
                            return true;
                        }
                    }
                    Some(Ok(_)) => continue,
                    _ => return false,
                }
            }
        })
        .await
        .expect("must answer within the timeout");
        assert!(limited, "the connection must end with the rate-limit error");
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
        let (bids, asks) = acc.price_fold();
        let frame = render_l2book(
            &bids,
            &asks,
            "BTC",
            l2_view(None, None, 20),
            1_700_000_000_000,
        );
        assert_eq!(
            frame,
            include_str!("../../tests/fixtures/hl_l2book_golden.json").trim_end(),
            "regenerate tests/fixtures/hl_l2book_golden.json from this renderer"
        );
    }

    /// The `l4Book` goldens, pinned the same way. This channel has no Nautilus reader, so the
    /// publisher's own field spellings are the whole contract — and they are **mixed** by derive:
    /// only `L4Order` carries `rename_all`, so `limitPx` sits beside `book_diffs`.
    #[test]
    fn golden_l4book_frames_match_the_committed_fixtures() {
        let acc = accumulated(vec![
            (BookSide::Bid, 100.5, 5.0, 1),
            (BookSide::Bid, 100.5, 3.0, 2),
            (BookSide::Bid, 99.0, 1.0, 3),
            (BookSide::Ask, 101.0, 2.0, 4),
        ]);
        assert_eq!(
            render_l4book_snapshot(&order_set(&acc), "BTC"),
            include_str!("../../tests/fixtures/hl_l4book_snapshot_golden.json").trim_end(),
            "regenerate tests/fixtures/hl_l4book_snapshot_golden.json from this renderer"
        );

        let b = book_with(vec![
            BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 100.5,
                size: 4.0,
                order_id: 1,
            },
            BookChange {
                action: BookAction::Delete,
                side: BookSide::Ask,
                price: 101.0,
                size: 0.0,
                order_id: 4,
            },
        ]);
        assert_eq!(
            render_l4book_diff(&b, "BTC").unwrap(),
            include_str!("../../tests/fixtures/hl_l4book_updates_golden.json").trim_end(),
            "regenerate tests/fixtures/hl_l4book_updates_golden.json from this renderer"
        );
    }
}
