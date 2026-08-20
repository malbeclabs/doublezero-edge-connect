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
use tokio_tungstenite::tungstenite::{Message as WsMessage, Utf8Bytes};
use tracing::{info, warn};

use crate::{
    metrics::metrics,
    model::{
        now_ns, BookAccumulator, BookAction, BookKey, BookSide, BookSnapshot, CountedLevel,
        FeedMessage, NormalizedBook, NormalizedTrade, ReplayScope, Side,
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
/// Capacity of the prepared-frame fan-out. A constant like every other limit here — this sink has no
/// operator-tunable behaviour — and set well above the backbone's own shipped default, so this hop is
/// never the one that drops a batch a client would otherwise have kept.
const PREPARED_CAPACITY: usize = 4096;
/// Shortest interval between one client's `l4Book` re-bootstraps — see `rebootstrap`.
const REBOOTSTRAP_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// Cap on an inbound frame. Control frames here are tens of bytes; tungstenite's 64 MiB default would
/// let `MAX_CLIENTS` peers buffer gigabytes before a single byte is parsed. Read-path only, so the
/// sink's own large `l4Book` snapshots are unaffected.
fn inbound_limits() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(64 * 1024))
        .max_frame_size(Some(64 * 1024))
}

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

/// The publisher's `OrderDiff`, verbatim (`app/publisher/server/src/types/mod.rs`) — the container's
/// `rename_all` spells the variants `new`/`update`/`remove`, and the `Update` variant carries its own,
/// which is what spells its fields `origSz`/`newSz`.
///
/// **The three are not interchangeable.** `new` asserts that an order the recipient does not have is
/// now resting; `update` asserts that one it does have changed size. A partial fill is the second, and
/// rendering it as the first tells a consumer to insert an order it already holds. The publisher's own
/// book builder refuses that outright — `listeners/order_book/state.rs` inserts a `New` only against a
/// matching opening order status and logs "New order did not rest" otherwise, while `Update` goes
/// straight to `modify_sz(oid, coin, newSz)`.
///
/// ⚠️ That builder consumes the Hyperliquid node's raw book diffs, **not** this WebSocket channel, and
/// there is no reference `l4Book` *consumer* in either source this sink is written against. It is the
/// closest statement of the variants' meaning that exists, and it is cited as that rather than as the
/// behaviour of any particular client. What follows from it either way: emit the variant the venue
/// event actually is.
///
/// `origSz` is what **this channel last published** for the order, which is the only prior size the
/// sink can honestly claim (the arbiter can refuse a change that never reached the wire, so a
/// producer-side prior would describe a state no consumer here holds). The publisher's builder ignores
/// the field; it is carried because the schema has it.
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum RawDiff {
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
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

/// Which `raw_book_diff` one change is, given what this channel last published for the order.
fn raw_diff(c: &crate::model::BookChange, published: Option<f64>) -> RawDiff {
    // A zero size is how an order-level producer says the order is gone; a consumer that rested it
    // would hold a phantom forever.
    if c.action == BookAction::Delete || c.size == 0.0 {
        return RawDiff::Remove;
    }
    match published {
        // The consumer already holds this order, so the change is a size change and not a new
        // resting order. See [`RawDiff`].
        Some(prev) => RawDiff::Update {
            orig_sz: num(prev),
            new_sz: num(c.size),
        },
        None => RawDiff::New { sz: num(c.size) },
    }
}

/// Render one incremental batch as `l4Book` order diffs, against `published` — the sizes this
/// channel last sent for the market, read **before** the batch is folded into it. `None` when the
/// batch carries no order-level change at all.
fn render_l4book_diff(b: &NormalizedBook, coin: &str, published: &MarketOrders) -> Option<String> {
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
            raw_book_diff: raw_diff(c, published.size_of(c.order_id)),
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

// --- the shared per-batch stage ---

/// Markets whose published order sizes are tracked, and resting orders per market. Both are wire-keyed
/// (`channel`/`instrument_id` and the venue's order ids), so both are bounded; overflow degrades one
/// market's diffs to `new` rather than growing without limit. `MAX_TRACKED_ORDERS` matches
/// `model::MAX_ACCUMULATED_ORDERS`, the ceiling the shared accumulator already holds itself to.
const MAX_TRACKED_MARKETS: usize = 4096;
const MAX_TRACKED_ORDERS: usize = 1 << 18;

/// What this sink has last published for one market's resting orders — the state `origSz` is a claim
/// about, and the memo that says whether the market is order-level at all.
///
/// Deliberately *this channel's* view rather than a copy of the producer's: the arbiter can refuse a
/// change that never reaches the wire, so a producer-side prior size would describe a book no consumer
/// here holds.
#[derive(Default)]
struct MarketOrders {
    sizes: std::collections::HashMap<u64, f64>,
    /// Whether this market streams order-level changes. Sticky, like the accumulator's own flag: an
    /// order-level book that empties is still order-level, and reading it off the population would
    /// stop a bare `clear` reaching the `l4Book` subscriber it is meant for.
    order_level: bool,
}

impl MarketOrders {
    fn size_of(&self, oid: u64) -> Option<f64> {
        self.sizes.get(&oid).copied()
    }

    /// Fold one published batch in. A `Clear` replaces the population rather than merging into it —
    /// the consumer was just told to discard, so anything left behind would make the next `origSz` a
    /// claim about a book nobody holds.
    fn apply(&mut self, b: &NormalizedBook) {
        for c in &b.changes {
            if c.action == BookAction::Clear {
                self.sizes.clear();
                continue;
            }
            if c.order_id == 0 {
                continue;
            }
            self.order_level = true;
            if c.action == BookAction::Delete || c.size == 0.0 || !c.size.is_finite() {
                self.sizes.remove(&c.order_id);
            } else if self.sizes.len() < MAX_TRACKED_ORDERS || self.sizes.contains_key(&c.order_id)
            {
                self.sizes.insert(c.order_id, c.size);
            }
        }
    }
}

/// One broadcast message with everything shared across clients already computed.
///
/// The stage exists because the per-client alternative takes the **arbiter's** mutex once per client
/// per batch — the same mutex every receiver on every feed takes to emit — and then folds the same
/// 44,598-order market once per client. Both are now paid once per batch, off that lock.
enum Prepared {
    Message(Box<PreparedMessage>),
    /// The stage fell behind the backbone, so every client's `l4Book` book is now wrong. Clients see
    /// their own `Lagged` but not this one, and an incremental channel cannot self-heal without being
    /// told.
    Resync,
}

struct PreparedMessage {
    msg: Arc<FeedMessage>,
    /// The market this batch is for, when it is a `book`.
    key: Option<BookKey>,
    /// Folded price levels and their time, when a client wants `l2Book` for this coin. Shared, not
    /// re-folded per subscription: the fold is independent of the `nSigFigs`/`nLevels` view.
    l2: Option<Arc<L2Fold>>,
    /// The rendered `l4Book` frame, when a client wants `l4Book` for this coin. Identical for every
    /// such client (the channel has no view parameters), so it is rendered once.
    l4: Option<Utf8Bytes>,
}

/// `(bids, asks, time_ms)` — one market's price fold, shared by every `l2Book` subscription of it.
type L2Fold = (Vec<CountedLevel>, Vec<CountedLevel>, u64);

/// How many connected clients want each channel for a coin, so the stage above folds and renders only
/// what someone will read. Refcounted rather than a flag: subscriptions come and go per client.
#[derive(Default, Clone, Copy)]
struct Wants {
    l2: usize,
    l4: usize,
}

type Wanted = Arc<std::sync::Mutex<std::collections::HashMap<String, Wants>>>;

/// One client's claims on [`Wanted`], released on drop so an early return or a panic cannot leave the
/// stage folding a market nobody reads.
struct WantGuard {
    wanted: Wanted,
    held: Vec<(String, bool)>,
}

impl WantGuard {
    fn new(wanted: Wanted) -> Self {
        Self {
            wanted,
            held: Vec::new(),
        }
    }

    fn add(&mut self, coin: &str, l4: bool) {
        let mut w = crate::model::lock(&self.wanted);
        let e = w.entry(coin.to_string()).or_default();
        if l4 {
            e.l4 += 1;
        } else {
            e.l2 += 1;
        }
        drop(w);
        self.held.push((coin.to_string(), l4));
    }

    fn remove(&mut self, coin: &str, l4: bool) {
        let Some(i) = self.held.iter().position(|(c, k)| c == coin && *k == l4) else {
            return;
        };
        self.held.swap_remove(i);
        release(&self.wanted, coin, l4);
    }
}

fn release(wanted: &Wanted, coin: &str, l4: bool) {
    let mut w = crate::model::lock(wanted);
    let Some(e) = w.get_mut(coin) else { return };
    let n = if l4 { &mut e.l4 } else { &mut e.l2 };
    *n = n.saturating_sub(1);
    if e.l2 == 0 && e.l4 == 0 {
        w.remove(coin);
    }
}

impl Drop for WantGuard {
    fn drop(&mut self) {
        for (coin, l4) in std::mem::take(&mut self.held) {
            release(&self.wanted, &coin, l4);
        }
    }
}

fn dropped(reason: &'static str) {
    metrics().hl_sink_dropped.with_label_values(&[reason]).inc();
}

/// The shared stage: one task reads the backbone, does the per-batch work once, and re-broadcasts it.
async fn prepare_loop(
    mut backbone: broadcast::Receiver<Arc<FeedMessage>>,
    out: broadcast::Sender<Arc<Prepared>>,
    books: BookSnapshot,
    wanted: Wanted,
) {
    let mut published: std::collections::HashMap<BookKey, MarketOrders> = Default::default();
    let mut order: std::collections::VecDeque<BookKey> = Default::default();
    loop {
        match backbone.recv().await {
            Ok(m) => {
                // No connected clients → nothing to prepare. The published-size map still has to
                // track, or the first client to connect would be diffed against a book this sink
                // never saw.
                let idle = out.receiver_count() == 0;
                let p = prepare_one(&m, &books, &wanted, &mut published, &mut order, idle);
                if !idle {
                    let _ = out.send(Arc::new(Prepared::Message(Box::new(p))));
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("hl sink prepare stage lagged, dropped {n}");
                dropped("prepare_lagged");
                // The published sizes no longer describe what any client holds — the gap's changes
                // never went out. Re-seeded from the accumulator every client is about to
                // re-bootstrap from, rather than dropped: dropped, the first change to *every*
                // order after the gap claims to be a new one, for orders the re-bootstrapped
                // client demonstrably holds.
                reseed_published(&books, &mut published, &mut order);
                let _ = out.send(Arc::new(Prepared::Resync));
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Rebuild the published-size map from the shared accumulators — the state every client is about to
/// re-bootstrap from. Only ever on a stage gap, so its O(tracked books) cost is not on any hot path.
fn reseed_published(
    books: &BookSnapshot,
    published: &mut std::collections::HashMap<BookKey, MarketOrders>,
    order: &mut std::collections::VecDeque<BookKey>,
) {
    published.clear();
    order.clear();
    let guard = crate::model::lock(books);
    for (key, acc) in guard.iter().filter(|(_, acc)| acc.is_order_level()) {
        if published.len() >= MAX_TRACKED_MARKETS {
            break;
        }
        let mut m = MarketOrders {
            order_level: true,
            ..Default::default()
        };
        m.apply(&acc.to_book(key, ReplayScope::Orders));
        published.insert(key.clone(), m);
        order.push_back(key.clone());
    }
}

fn prepare_one(
    m: &Arc<FeedMessage>,
    books: &BookSnapshot,
    wanted: &Wanted,
    published: &mut std::collections::HashMap<BookKey, MarketOrders>,
    order: &mut std::collections::VecDeque<BookKey>,
    idle: bool,
) -> PreparedMessage {
    let bare = |key| PreparedMessage {
        msg: m.clone(),
        key,
        l2: None,
        l4: None,
    };
    let FeedMessage::Book(b) = &**m else {
        return bare(None);
    };
    if b.venue.as_ref() != VENUE {
        return bare(None);
    }
    let coin = b.symbol.as_ref();
    let key: BookKey = (
        b.venue.clone(),
        b.category.clone(),
        b.channel,
        b.instrument_id,
    );
    let wants = crate::model::lock(wanted)
        .get(coin)
        .copied()
        .unwrap_or_default();
    let rebaseline = b
        .changes
        .first()
        .is_some_and(|c| c.action == BookAction::Clear);

    if !published.contains_key(&key) {
        while published.len() >= MAX_TRACKED_MARKETS {
            match order.pop_front() {
                Some(old) => {
                    published.remove(&old);
                }
                None => break,
            }
        }
        order.push_back(key.clone());
        // Seeded from the shared accumulator, not rebuilt from the feed: a market re-created after
        // an eviction would otherwise read as price-aggregated until its next order-carrying batch,
        // and a bare `clear` in that window would reach no `l4Book` subscriber at all.
        let seed = crate::model::lock(books)
            .get(&key)
            .is_some_and(BookAccumulator::is_order_level);
        published.insert(
            key.clone(),
            MarketOrders {
                order_level: seed,
                ..Default::default()
            },
        );
    }
    let market = published.entry(key.clone()).or_default();
    // Rendered against the pre-batch sizes, then the batch is folded in — `origSz` is what the
    // consumer holds, which is what this channel published *before* this batch.
    let l4_diff = (!idle && wants.l4 > 0 && !rebaseline)
        .then(|| render_l4book_diff(b, coin, market))
        .flatten();
    market.apply(b);
    let order_level = market.order_level;

    // **Rendered from the batch, never from the shared accumulator.** The accumulator is advanced by
    // `Arbiter::publish_book` *before* the message is broadcast, so it already holds batches still
    // queued for a lagging client: that client would get a snapshot containing them and then apply
    // the older diffs on top, resurrecting orders permanently. A `Clear`-led batch is the complete
    // book by construction, and a bare one is the arbiter's degraded "discard this market" — which an
    // `l4Book` subscriber must still hear, since the channel has no clear of its own.
    let l4 = if idle || wants.l4 == 0 {
        None
    } else if rebaseline {
        order_level.then(|| Utf8Bytes::from(render_l4book_snapshot(b, coin)))
    } else {
        l4_diff.map(Utf8Bytes::from)
    };
    // `l2Book` is snapshot-per-update and needs the whole folded market, so it still reads the shared
    // accumulator — once per batch, under one brief lock, shared by every subscription of it.
    let l2 = (!idle && wants.l2 > 0)
        .then(|| take_market(books, &key, coin, BookAccumulator::clone))
        .flatten()
        .map(|acc| {
            metrics().hl_sink_folds.inc();
            let (bids, asks) = acc.price_fold();
            Arc::new((bids, asks, ms_or_now(acc.source_ts_ns())))
        });
    PreparedMessage {
        msg: m.clone(),
        key: Some(key),
        l2,
        l4,
    }
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

/// The accept loop, fed by the shared [`prepare_loop`] stage rather than by the backbone directly:
/// the `l4Book` frame and the `l2Book` price fold are identical for every client that wants them, and
/// paying for either per client puts the arbiter's mutex — the one every receiver on every feed takes
/// to emit — on the sink's fan-out path. Only the `l2Book` *rendering* stays per client, since two
/// subscriptions can ask for different `nSigFigs`/`nLevels` views of one fold.
pub async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<Arc<FeedMessage>>,
    books: BookSnapshot,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));
    let wanted: Wanted = Default::default();
    let (prepared_tx, _rx) = broadcast::channel::<Arc<Prepared>>(PREPARED_CAPACITY);
    tokio::spawn(prepare_loop(
        tx.subscribe(),
        prepared_tx.clone(),
        books.clone(),
        wanted.clone(),
    ));
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
        let rx = prepared_tx.subscribe();
        let books = books.clone();
        let wanted = wanted.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, rx, books, wanted).await {
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
/// and an `l4Book` snapshot claims completeness, so an accumulator seeded partway through — holding only
/// the levels that have moved since — must be withheld rather than published as whole. `is_order_level`
/// is what keeps that honest for a market whose changes are price-aggregated: this sink reads only the
/// order population, so such a market would render as an *empty* book, telling the consumer to discard
/// levels the bridge holds.
fn publishable(key: &BookKey, acc: &BookAccumulator, coin: &str) -> bool {
    key.0.as_ref() == VENUE
        && acc.symbol().as_ref() == coin
        && acc.baselined()
        && acc.is_order_level()
}

/// Copy one market out from under the shared lock — the arbiter's emit path takes this same mutex on
/// every published batch, so nothing that renders may run while it is held.
///
/// `take` is deliberately a **clone and nothing more**. Every rendering step is far more expensive
/// than the copy it would save: measured on the 44,598-order fixture, cloning the accumulator is
/// ~0.45 ms against ~9.1 ms to fold it to price levels and ~5.6 ms to materialize its order set. The
/// closure exists to keep that discipline visible at the call site, not to push work under the guard.
fn take_market<T>(
    books: &BookSnapshot,
    key: &BookKey,
    coin: &str,
    take: impl FnOnce(&BookAccumulator) -> T,
) -> Option<T> {
    let guard = crate::model::lock(books);
    guard
        .get(key)
        .filter(|acc| publishable(key, acc, coin))
        .map(take)
}

/// The same copy-out for every market of this venue matching `coin`, under the same rule. Scans the
/// map, so it is for a subscribe or a recovery — never the steady feed, which resolves one market by
/// its own key.
fn take_markets<T>(
    books: &BookSnapshot,
    coin: &str,
    take: impl Fn(&BookKey, &BookAccumulator) -> T,
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

/// Every market of this venue whose display symbol is `coin`.
///
/// **A `coin` is not an identity.** The wire `symbol` is a truncated 16-byte label, and two distinct
/// `instrument_id`s sharing one is confirmed on captured data (`tests/fixtures/PROVENANCE.md`) — which
/// is why `BookSnapshot` is keyed on the identity and not on the symbol. Hyperliquid's schema carries
/// no channel or instrument field to disambiguate with, so a subscription that resolves to more than
/// one market is refused rather than served two markets' competing snapshots and interleaved updates
/// under one name.
fn markets_for(books: &BookSnapshot, coin: &str) -> Vec<BookKey> {
    take_markets(books, coin, |key, _| key.clone())
}

/// Bootstrap `sub` from current state, scoped to `key` when the client is pinned to one market.
fn bootstrap(
    books: &BookSnapshot,
    sub: &Sub,
    key: Option<&BookKey>,
) -> Vec<(&'static str, String)> {
    let markets = |coin: &str| -> Vec<(BookKey, BookAccumulator)> {
        match key {
            Some(k) => take_market(books, k, coin, BookAccumulator::clone)
                .map(|acc| vec![(k.clone(), acc)])
                .unwrap_or_default(),
            None => take_markets(books, coin, |k, acc| (k.clone(), acc.clone())),
        }
    };
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
            markets(coin)
                .into_iter()
                .map(|(_, acc)| {
                    let (bids, asks) = acc.price_fold();
                    let time = ms_or_now(acc.source_ts_ns());
                    ("l2Book", render_l2book(&bids, &asks, coin, view, time))
                })
                .collect()
        }
        Sub::L4Book { coin } => markets(coin)
            .into_iter()
            .map(|(key, acc)| {
                let b = acc.to_book(&key, ReplayScope::Orders);
                ("l4Book", render_l4book_snapshot(&b, coin))
            })
            .collect(),
    }
}

/// One outbound frame: the channel it counts against, and its bytes.
type Frame = (&'static str, Utf8Bytes);

/// Every frame one prepared message produces across a client's subscriptions.
///
/// Takes no lock and folds nothing — [`prepare_one`] has already done both, once for every client.
/// `pinned` binds each subscribed coin to the one market it was bootstrapped from, so a second market
/// that later takes the same truncated symbol cannot interleave its updates into the first's book
/// (see [`markets_for`]).
fn frames(
    p: &PreparedMessage,
    subs: &[Sub],
    pinned: &mut std::collections::HashMap<String, BookKey>,
) -> Vec<Frame> {
    let mut out = Vec::new();
    match &*p.msg {
        FeedMessage::Trade(t) => {
            for sub in subs {
                if let Sub::Trades { coin } = sub {
                    out.extend(render_trade(t, coin).map(|f| ("trades", Utf8Bytes::from(f))));
                }
            }
        }
        FeedMessage::Book(b) if b.venue.as_ref() == VENUE => {
            let coin = b.symbol.as_ref();
            let Some(key) = p.key.as_ref() else {
                return out;
            };
            // **Only a coin this client is subscribed to may be pinned.** Pinning from the feed
            // regardless would let a market that happened to publish between accept and the
            // subscribe frame claim the coin, and the subscribe path — which resolves it properly
            // against the book map — would then find the pin already taken: an empty bootstrap
            // followed by every real frame silently dropped, for the life of the connection.
            if !subs.iter().any(|s| book_coin(s) == Some(coin)) {
                return out;
            }
            match pinned.get(coin) {
                Some(held) if held != key => {
                    dropped("ambiguous_market");
                    return out;
                }
                Some(_) => {}
                None => {
                    pinned.insert(coin.to_string(), key.clone());
                }
            }
            for sub in subs {
                match sub {
                    Sub::L2Book {
                        coin: c,
                        n_sig_figs,
                        mantissa,
                        n_levels,
                    } if c == coin => {
                        if let Some(fold) = &p.l2 {
                            let view = l2_view(*n_sig_figs, *mantissa, *n_levels);
                            let text = render_l2book(&fold.0, &fold.1, c, view, fold.2);
                            out.push(("l2Book", Utf8Bytes::from(text)));
                        }
                    }
                    Sub::L4Book { coin: c } if c == coin => {
                        out.extend(p.l4.clone().map(|f| ("l4Book", f)));
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
    mut rx: broadcast::Receiver<Arc<Prepared>>,
    books: BookSnapshot,
    wanted: Wanted,
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
    let mut wants = WantGuard::new(wanted);
    // The one market each subscribed coin was resolved to — see `markets_for`.
    let mut pinned: std::collections::HashMap<String, BookKey> = Default::default();
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval(HEARTBEAT);
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;
    let mut last_rebootstrap: Option<Instant> = None;

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Text(txt))) => {
                    last_seen = Instant::now();
                    // Inbound rate limit (per rolling minute). This is what bounds the cost of a
                    // control frame: a subscribe renders a market's whole book, so without a cap an
                    // `unsubscribe`/`subscribe` loop is an output amplifier — the `added` guard below
                    // only suppresses an identical *repeat*.
                    if !inbound_allowed(&mut win_start, &mut win_count) {
                        return end_rate_limited(&mut write).await;
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
                            if added {
                                if let Some(coin) = book_coin(&sub) {
                                    let markets = markets_for(&books, coin);
                                    if markets.len() > 1 {
                                        dropped("ambiguous_market");
                                        write.send(error_frame(
                                            "Invalid subscription: coin is ambiguous, several markets share this symbol",
                                        )).await?;
                                        continue;
                                    }
                                    // Overwrites: this is the authoritative resolution, against
                                    // the book map, and it must win over anything the feed put
                                    // there for an earlier subscription of the same coin.
                                    if let Some(key) = markets.into_iter().next() {
                                        pinned.insert(coin.to_string(), key);
                                    }
                                }
                            }
                            write.send(subscription_response("subscribe", &sub)).await?;
                            if added {
                                // **Claim before reading the bootstrap**, not after writing it.
                                // The stage renders `l4` only for coins something wants, so a want
                                // claimed after the write leaves the whole write of a 44,598-order
                                // snapshot as a window in which every batch reaches this client
                                // with nothing to render — and an incremental channel does not
                                // recover from a gap. Claiming first can instead duplicate a batch
                                // the bootstrap already contains, and those converge: an `update`
                                // carries the order's absolute size and a `remove` is idempotent.
                                if let Some(coin) = book_coin(&sub) {
                                    wants.add(coin, matches!(sub, Sub::L4Book { .. }));
                                }
                                let key = book_coin(&sub).and_then(|c| pinned.get(c)).cloned();
                                let frames = bootstrap(&books, &sub, key.as_ref());
                                subs.push(sub);
                                for (channel, frame) in frames {
                                    sent(channel);
                                    write.send(WsMessage::Text(frame.into())).await?;
                                }
                            }
                        }
                        Ok(Control::Unsubscribe(sub)) => {
                            if subs.contains(&sub) {
                                if let Some(coin) = book_coin(&sub) {
                                    wants.remove(coin, matches!(sub, Sub::L4Book { .. }));
                                }
                            }
                            subs.retain(|s| s != &sub);
                            // The pin goes with the last subscription that held it, so a later
                            // subscribe re-resolves the coin instead of inheriting a stale market.
                            if let Some(coin) = book_coin(&sub) {
                                if !subs.iter().any(|s| book_coin(s) == Some(coin)) {
                                    pinned.remove(coin);
                                }
                            }
                            write.send(subscription_response("unsubscribe", &sub)).await?;
                        }
                        Err(message) => write.send(error_frame(&message)).await?,
                    }
                }
                // Rate-limited exactly as `Text` is. Applied to `Text` alone, a peer holds the
                // connection open indefinitely and drives an unbounded outbound `Pong` stream without
                // ever tripping the cap this sink relies on as its load-bearing client limit.
                Some(Ok(WsMessage::Ping(p))) => {
                    last_seen = Instant::now();
                    if !inbound_allowed(&mut win_start, &mut win_count) {
                        return end_rate_limited(&mut write).await;
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
                Ok(p) => match &*p {
                    Prepared::Message(p) => {
                        for (channel, frame) in frames(p, &subs, &mut pinned) {
                            sent(channel);
                            write.send(WsMessage::Text(frame)).await?;
                        }
                    }
                    // The shared stage lost messages, so this client's `l4Book` book is wrong even
                    // though its own receiver never lagged. Only an `l4Book` subscriber has anything
                    // to lose, and only one may be disconnected for it: the stage's lag is not this
                    // client's fault, and a `trades`/`l2Book` client is unaffected by it.
                    Prepared::Resync if subs.iter().any(|s| matches!(s, Sub::L4Book { .. })) => {
                        if !rebootstrap(&mut write, &books, &subs, &pinned, &mut last_rebootstrap).await? {
                            return end_lagging(&mut write).await;
                        }
                    }
                    Prepared::Resync => {}
                },
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("hl sink client lagged, dropped {n}");
                    dropped("client_lagged");
                    if !rebootstrap(&mut write, &books, &subs, &pinned, &mut last_rebootstrap).await? {
                        return end_lagging(&mut write).await;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}

/// The coin a subscription names, when it is one of the two book channels.
fn book_coin(sub: &Sub) -> Option<&str> {
    match sub {
        Sub::L2Book { coin, .. } | Sub::L4Book { coin } => Some(coin),
        Sub::Trades { .. } => None,
    }
}

/// Charge one inbound frame against the rolling-minute cap, returning whether it is allowed.
fn inbound_allowed(win_start: &mut Instant, win_count: &mut u32) -> bool {
    if win_start.elapsed() >= Duration::from_secs(60) {
        *win_start = Instant::now();
        *win_count = 0;
    }
    *win_count += 1;
    *win_count <= MAX_INBOUND_PER_MIN
}

/// End a connection that crossed the inbound cap. The `Close` is not decoration: dropping the socket
/// straight after the error frame races whatever the peer has not drained, so a real client is
/// disconnected with the one frame that says why still in flight.
async fn end_rate_limited<S>(write: &mut S) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    write
        .send(error_frame("inbound rate limit exceeded"))
        .await?;
    write.send(WsMessage::Close(None)).await?;
    Ok(())
}

/// End a connection that keeps falling behind. See [`rebootstrap`].
async fn end_lagging<S>(write: &mut S) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    write
        .send(error_frame("lagging: reconnect for a fresh book"))
        .await?;
    write.send(WsMessage::Close(None)).await?;
    Ok(())
}

/// Re-bootstrap this client's `l4Book` subscriptions after a gap, returning whether it should stay
/// connected.
///
/// `l4Book` only: it is incremental, so a dropped batch leaves this client's book permanently wrong.
/// `l2Book` self-heals on its next frame.
///
/// **Guarded, because the remedy is also the cause.** A client lags when it cannot keep up, and this
/// hands it the most expensive frame the sink produces — a lock-held clone plus a full order-set
/// materialization, ~5.6 ms per market. Ungated, a client that lags once lags again on the work sent
/// to fix it, forever. A second gap inside the guard window says the client cannot be served at this
/// rate, so it is told to reconnect rather than kept on a book known to be wrong.
async fn rebootstrap<S>(
    write: &mut S,
    books: &BookSnapshot,
    subs: &[Sub],
    pinned: &std::collections::HashMap<String, BookKey>,
    last: &mut Option<Instant>,
) -> Result<bool>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    if last.is_some_and(|t| t.elapsed() < REBOOTSTRAP_MIN_INTERVAL) {
        return Ok(false);
    }
    *last = Some(Instant::now());
    for sub in subs.iter().filter(|s| matches!(s, Sub::L4Book { .. })) {
        let key = book_coin(sub).and_then(|c| pinned.get(c));
        for (channel, frame) in bootstrap(books, sub, key) {
            sent(channel);
            write.send(WsMessage::Text(frame.into())).await?;
        }
    }
    Ok(true)
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

    const TEST_CATEGORY: &str = "perps";

    use crate::model::BookReplay;

    /// The shared replay map a test hands the sink, holding exactly the markets listed.
    fn replay(markets: Vec<(BookKey, BookAccumulator)>) -> BookSnapshot {
        let mut r = BookReplay::default();
        for (k, acc) in markets {
            r.insert(k, acc);
        }
        Arc::new(Mutex::new(r))
    }

    fn book_with(changes: Vec<BookChange>) -> NormalizedBook {
        NormalizedBook {
            venue: VENUE.into(),
            source: VENUE.into(),
            source_id: 1,
            symbol: "BTC".into(),
            channel: 0,
            instrument_id: 7,
            order_level: changes.iter().any(|c| c.order_id != 0),
            changes,
            snapshot: false,
            last: true,
            source_ts_ns: 1_700_000_000_000_000_000,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            category: TEST_CATEGORY.into(),
        }
    }

    fn accumulated(orders: Vec<(BookSide, f64, f64, u64)>) -> BookAccumulator {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&order_book(orders));
        acc
    }

    fn key() -> BookKey {
        (Arc::from(VENUE), Arc::from(TEST_CATEGORY), 0, 7)
    }

    /// The order set the sink copies out under the lock, in the form the renderer receives it.
    fn order_set(acc: &BookAccumulator) -> NormalizedBook {
        acc.to_book(&key(), ReplayScope::Orders)
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
            channel: 0,
            instrument_id: 7,
            cumulative_volume: 0.0,
            source_ts_ns: 1_700_000_000_000_000_000,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            category: TEST_CATEGORY.into(),
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
            serde_json::from_str(&render_l4book_diff(&b, "BTC", &MarketOrders::default()).unwrap())
                .unwrap();
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
            let v: serde_json::Value = serde_json::from_str(
                &render_l4book_diff(&b, "BTC", &MarketOrders::default()).unwrap(),
            )
            .unwrap();
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
        assert!(render_l4book_diff(&b, "ETH", &MarketOrders::default()).is_none());
        b.venue = "PHOENIX".into();
        assert!(render_l4book_diff(&b, "BTC", &MarketOrders::default()).is_none());
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
        assert!(render_l4book_diff(&b, "BTC", &MarketOrders::default()).is_none());
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

    /// A market accumulated partway through holds only the levels that have moved since. Publishing it on
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
        let books = replay(vec![(key(), acc)]);
        assert!(bootstrap(
            &books,
            &Sub::L2Book {
                coin: "BTC".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: 20
            },
            None,
        )
        .is_empty());
        assert!(bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }, None).is_empty());
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
        let books = replay(vec![(key(), acc)]);
        assert!(bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }, None).is_empty());
        assert!(bootstrap(
            &books,
            &Sub::L2Book {
                coin: "BTC".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: 20
            },
            None,
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
        let books = replay(vec![(key(), acc)]);
        assert_eq!(
            bootstrap(&books, &Sub::L4Book { coin: "BTC".into() }, None).len(),
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
                },
                None,
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
        let books = replay(vec![(
            key(),
            accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]),
        )]);
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
        let m = Arc::new(FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }])));
        let wanted: Wanted = Default::default();
        for (coin, l4) in [("BTC", false), ("BTC", true)] {
            let mut w = crate::model::lock(&wanted);
            let e = w.entry(coin.to_string()).or_default();
            if l4 {
                e.l4 += 1;
            } else {
                e.l2 += 1;
            }
        }
        let mut published = Default::default();
        let mut order = Default::default();
        let p = prepare_one(&m, &books, &wanted, &mut published, &mut order, false);
        let out = frames(&p, &subs, &mut Default::default());
        assert_eq!(
            out.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
            vec!["l2Book", "l4Book"]
        );
    }

    /// A book far larger than any view of it: `nLevels` truncates the `l2Book` view while the
    /// `l4Book` snapshot carries every order.
    ///
    /// Deliberately says nothing about the shared book lock. That only the copy-out runs under it is
    /// real and load-bearing (see [`take_market`]), and nothing here can observe it: the renderers
    /// take no `BookSnapshot`, so a clone moved back under the guard would still pass.
    #[test]
    fn a_large_book_truncates_for_l2_and_renders_whole_for_l4() {
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
        let books: BookSnapshot = replay(vec![(key(), acc)]);
        let (fold, time) = take_market(&books, &key(), "BTC", |a| {
            (a.price_fold(), ms_or_now(a.source_ts_ns()))
        })
        .unwrap();
        let snapshot = take_market(&books, &key(), "BTC", |a| {
            a.to_book(&key(), ReplayScope::Orders)
        })
        .unwrap();

        let l2 = render_l2book(&fold.0, &fold.1, "BTC", l2_view(None, None, 100), time);
        let l4 = render_l4book_snapshot(&snapshot, "BTC");
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
        books: Vec<(BookKey, BookAccumulator)>,
    ) -> (
        broadcast::Sender<Arc<FeedMessage>>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let srv = tokio::spawn(serve(listener, tx.clone(), replay(books)));
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
        let books = vec![(key(), accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]))];
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
        let (_tx, addr, srv) = spawn(vec![]).await;
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
        let books = vec![(key(), accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]))];
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
        let (tx, addr, srv) = spawn(vec![]).await;
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

    /// Flood the connection past the inbound cap with `flood`, then report whether the rate-limit
    /// error arrived and whether a WebSocket `Close` followed it.
    async fn flood_past_the_cap(frame: impl Fn() -> WsMessage) -> (bool, bool) {
        let (_tx, addr, srv) = spawn(vec![]).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        for _ in 0..=MAX_INBOUND_PER_MIN {
            if ws.send(frame()).await.is_err() {
                break;
            }
        }
        // Drain until the rate-limit error arrives (every frame before it is a pong), then look for
        // the close that must follow it.
        let out = timeout(Duration::from_secs(5), async {
            let mut errored = false;
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => {
                        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                        if v["channel"] == "error" {
                            errored = true;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) => return (errored, true),
                    Some(Ok(_)) => continue,
                    _ => return (errored, false),
                }
            }
        })
        .await
        .expect("must answer within the timeout");
        srv.abort();
        out
    }

    /// A control frame is cheap to send and can cost a whole book to answer, so the per-minute cap is
    /// what bounds the amplification. Crossing it ends the connection rather than throttling, which is
    /// what the normalized sink does.
    ///
    /// **Item K.** The `Close` is part of ending it: dropping the socket straight after the error
    /// races the frames the peer has not drained, so the one frame that says *why* can be lost
    /// entirely and a real client is disconnected with no reason.
    #[tokio::test]
    async fn crossing_the_inbound_rate_limit_ends_the_connection() {
        let (errored, closed) =
            flood_past_the_cap(|| WsMessage::Text(r#"{"method":"ping"}"#.into())).await;
        assert!(errored, "the connection must end with the rate-limit error");
        assert!(closed, "and with a WebSocket close, not an abrupt drop");
    }

    /// **Item L.** A WebSocket `Ping` is an inbound frame that costs an outbound `Pong`, so it is
    /// charged against the same cap. Applied to `Text` alone, a peer holds the connection open
    /// indefinitely and drives an unbounded `Pong` stream without ever tripping the limit this sink
    /// relies on as its load-bearing client bound.
    #[tokio::test]
    async fn websocket_pings_count_against_the_inbound_rate_limit() {
        let (errored, closed) = flood_past_the_cap(|| WsMessage::Ping(Vec::new().into())).await;
        assert!(errored, "a ping flood must trip the inbound cap");
        assert!(closed);
    }

    /// **Item J.** A coin that resolves to more than one market cannot be served: the schema carries
    /// no channel or instrument field, so both markets' snapshots and updates would arrive under one
    /// name. Refused, rather than silently served one of them.
    #[tokio::test]
    async fn an_ambiguous_coin_subscription_is_refused() {
        let mut second = key();
        second.3 = 8;
        let books = vec![
            (key(), accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)])),
            (second, accumulated(vec![(BookSide::Ask, 200.0, 1.0, 12)])),
        ];
        let (_tx, addr, srv) = spawn(books).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#.into(),
        ))
        .await
        .unwrap();
        let f = next_text(&mut ws, Duration::from_secs(2)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["channel"], "error", "got {f}");
        assert!(v["data"].as_str().unwrap().contains("ambiguous"));
        assert!(
            next_text(&mut ws, Duration::from_millis(300))
                .await
                .is_none(),
            "an ambiguous coin must not be bootstrapped from either market"
        );
        srv.abort();
    }

    // --- the review's findings ---

    /// One market's published order sizes, as the prepare stage would hold them.
    fn published_sizes(orders: &[(u64, f64)]) -> MarketOrders {
        let mut m = MarketOrders {
            order_level: true,
            ..Default::default()
        };
        for &(oid, sz) in orders {
            m.sizes.insert(oid, sz);
        }
        m
    }

    fn diff_json(b: &NormalizedBook, published: &MarketOrders) -> serde_json::Value {
        serde_json::from_str(&render_l4book_diff(b, "BTC", published).unwrap()).unwrap()
    }

    /// **Item I.** A partial fill of an order the consumer already holds is `update{origSz,newSz}`.
    /// Rendered as `new`, the reference apply skips it for want of a matching opening order status
    /// (`listeners/order_book/state.rs`) and the fill is silently lost. An order the channel has never
    /// published stays `new` — there is no prior size to claim.
    #[test]
    fn a_partial_fill_of_a_published_order_is_an_update() {
        let b = book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }]);
        let filled = diff_json(&b, &published_sizes(&[(11, 5.0)]));
        assert_eq!(
            filled["data"]["Updates"]["book_diffs"][0]["raw_book_diff"]["update"],
            serde_json::json!({ "origSz": "5", "newSz": "3" })
        );
        let fresh = diff_json(&b, &published_sizes(&[]));
        assert_eq!(
            fresh["data"]["Updates"]["book_diffs"][0]["raw_book_diff"]["new"]["sz"],
            "3"
        );
    }

    /// A removal stays `remove` whatever the channel last published for the order.
    #[test]
    fn a_removal_is_remove_even_for_a_published_order() {
        let b = book_with(vec![BookChange {
            action: BookAction::Delete,
            side: BookSide::Bid,
            price: 100.0,
            size: 0.0,
            order_id: 11,
        }]);
        assert_eq!(
            diff_json(&b, &published_sizes(&[(11, 5.0)]))["data"]["Updates"]["book_diffs"][0]
                ["raw_book_diff"],
            "remove"
        );
    }

    /// A `Clear` drops the published population: the consumer was told to discard, so the next change
    /// to a formerly-resting order is `new`, not an `update` against a size nobody holds.
    #[test]
    fn a_clear_drops_the_published_sizes() {
        let mut m = published_sizes(&[(11, 5.0)]);
        m.apply(&book_with(vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }]));
        assert_eq!(m.size_of(11), None);
        assert!(m.order_level, "an emptied order book is still order-level");
    }

    /// Everything one broadcast message needs, prepared as the shared stage prepares it.
    fn prepared(
        m: &Arc<FeedMessage>,
        books: &BookSnapshot,
        published: &mut std::collections::HashMap<BookKey, MarketOrders>,
        coins: &[(&str, bool)],
    ) -> PreparedMessage {
        let wanted: Wanted = Default::default();
        {
            let mut w = crate::model::lock(&wanted);
            for (coin, l4) in coins {
                let e = w.entry((*coin).to_string()).or_default();
                if *l4 {
                    e.l4 += 1;
                } else {
                    e.l2 += 1;
                }
            }
        }
        let mut order = Default::default();
        prepare_one(m, books, &wanted, published, &mut order, false)
    }

    /// **Item C.** The `l4Book` re-baseline is rendered from the batch, not from the shared
    /// accumulator. `Arbiter::publish_book` advances that accumulator *before* the message is
    /// broadcast, so it already holds batches still queued for a lagging client: rendered from it, the
    /// client gets a snapshot containing them and then applies the older diffs on top, resurrecting
    /// removed orders permanently.
    #[test]
    fn an_l4book_snapshot_renders_the_batch_not_the_accumulator() {
        // The shared accumulator is ahead of the batch — order 12 has already been folded in.
        let mut acc = accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]);
        acc.apply(&book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Ask,
            price: 101.0,
            size: 1.0,
            order_id: 12,
        }]));
        let books = replay(vec![(key(), acc)]);
        let m = Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            5.0,
            11,
        )])));
        let p = prepared(&m, &books, &mut Default::default(), &[("BTC", true)]);
        let v: serde_json::Value =
            serde_json::from_str(p.l4.as_ref().expect("a re-baseline must snapshot")).unwrap();
        let oids: Vec<u64> = v["data"]["Snapshot"]["levels"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|side| side.as_array().unwrap())
            .map(|o| o["oid"].as_u64().unwrap())
            .collect();
        assert_eq!(
            oids,
            vec![11],
            "the snapshot must be the batch, not the accumulator's later state"
        );
    }

    /// **Item B.** The arbiter's degraded forced re-baseline is a bare `Clear` — no replacement
    /// content. `l4Book` has no clear of its own, so the consumer must still get a snapshot (an empty
    /// one) or it holds every stale order forever and then receives `Updates` for a market it was
    /// never given a `Snapshot` for.
    #[test]
    fn a_bare_clear_still_snapshots_an_l4book_subscriber() {
        let books = replay(vec![(
            key(),
            accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]),
        )]);
        let mut published = Default::default();
        // The market must first be known order-level, exactly as it is on the wire.
        let seed = Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            5.0,
            11,
        )])));
        let _ = prepared(&seed, &books, &mut published, &[("BTC", true)]);

        let bare = Arc::new(FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }])));
        let p = prepared(&bare, &books, &mut published, &[("BTC", true)]);
        let v: serde_json::Value = serde_json::from_str(
            p.l4.as_ref()
                .expect("a bare clear must still tell the consumer to discard"),
        )
        .unwrap();
        assert!(v["data"]["Snapshot"].is_object());
        assert!(v["data"]["Snapshot"]["levels"][0]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(v["data"]["Snapshot"]["levels"][1]
            .as_array()
            .unwrap()
            .is_empty());
    }

    /// A market that has never carried an order id is price-aggregated: this sink reads only the order
    /// population, so an empty `l4Book` snapshot for it would tell the consumer to discard levels the
    /// bridge holds.
    #[test]
    fn a_price_aggregated_clear_is_not_snapshotted() {
        let books = replay(vec![]);
        let bare = Arc::new(FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }])));
        let p = prepared(&bare, &books, &mut Default::default(), &[("BTC", true)]);
        assert!(p.l4.is_none());
    }

    /// **Item M.** The fold is O(resting orders) and is the sink's dominant cost. Paid per client it
    /// also takes the *arbiter's* mutex once per client per batch — the one every receiver on every
    /// feed takes to emit — so 64 clients stall all ingest for tens of milliseconds a batch. One fold
    /// per batch, fanned out.
    #[test]
    fn the_l2_fold_is_paid_once_per_batch_not_once_per_client() {
        let books = replay(vec![(
            key(),
            accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]),
        )]);
        let m = Arc::new(FeedMessage::Book(book_with(vec![BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 100.0,
            size: 3.0,
            order_id: 11,
        }])));
        let before = metrics().hl_sink_folds.get();
        let p = prepared(&m, &books, &mut Default::default(), &[("BTC", false)]);
        let subs = vec![Sub::L2Book {
            coin: "BTC".into(),
            n_sig_figs: None,
            mantissa: None,
            n_levels: 20,
        }];
        for _ in 0..64 {
            let out = frames(&p, &subs, &mut Default::default());
            assert_eq!(out.len(), 1, "every client still gets its own view");
        }
        assert_eq!(
            metrics().hl_sink_folds.get() - before,
            1,
            "64 clients must cost one fold, not 64"
        );
    }

    /// **Item J.** A `coin` is a truncated display label, and two markets sharing one is confirmed on
    /// captured data (`tests/fixtures/PROVENANCE.md`). The schema carries no channel or instrument
    /// field, so once a subscription is bound to a market, a second market taking the same symbol must
    /// not interleave its updates into the first's book.
    #[test]
    fn a_second_market_sharing_the_coin_does_not_interleave() {
        let books = replay(vec![]);
        let subs = vec![Sub::L4Book { coin: "BTC".into() }];
        let mut pinned = Default::default();
        let first = Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            5.0,
            11,
        )])));
        let mut published = Default::default();
        let p = prepared(&first, &books, &mut published, &[("BTC", true)]);
        assert_eq!(frames(&p, &subs, &mut pinned).len(), 1);

        // Same coin, different market.
        let FeedMessage::Book(mut other) = (*first).clone() else {
            unreachable!()
        };
        other.instrument_id = 8;
        let other = Arc::new(FeedMessage::Book(other));
        let p = prepared(&other, &books, &mut published, &[("BTC", true)]);
        assert!(
            frames(&p, &subs, &mut pinned).is_empty(),
            "the second market must not be served under the first's coin"
        );
    }

    /// A `Sink` that just records what was written, so the re-bootstrap guard can be driven without a
    /// socket.
    #[derive(Default)]
    struct Recorder(Vec<WsMessage>);

    impl futures_util::Sink<WsMessage> for Recorder {
        type Error = std::convert::Infallible;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn start_send(
            mut self: std::pin::Pin<&mut Self>,
            item: WsMessage,
        ) -> std::result::Result<(), Self::Error> {
            self.0.push(item);
            Ok(())
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A market must not claim a coin before the client asks for it. Pinned from the feed regardless
    /// of subscriptions, a market that published between accept and the subscribe frame took the coin,
    /// the subscribe path's own resolution could not displace it, and the client then received an empty
    /// bootstrap followed by every real frame dropped — with a `subscriptionResponse` and no error.
    #[test]
    fn an_unsubscribed_coin_is_not_pinned_from_the_feed() {
        let books = replay(vec![]);
        let mut pinned = Default::default();
        let stray = Arc::new(FeedMessage::Book(order_book(vec![(
            BookSide::Bid,
            100.0,
            5.0,
            11,
        )])));
        let p = prepared(&stray, &books, &mut Default::default(), &[("BTC", true)]);
        // No subscriptions yet: the batch is not rendered, and must leave no pin behind.
        assert!(frames(&p, &[], &mut pinned).is_empty());
        assert!(
            pinned.is_empty(),
            "a coin nobody subscribed to must not be bound to a market"
        );
    }

    /// **Item N.** A client lags because it cannot keep up, and the re-bootstrap is the most expensive
    /// frame the sink produces — so re-sending it unguarded is what makes the client lag again. A
    /// second gap inside the guard window means it cannot be served at this rate.
    #[tokio::test]
    async fn a_second_lag_inside_the_guard_window_is_refused() {
        let books = replay(vec![(
            key(),
            accumulated(vec![(BookSide::Bid, 100.0, 5.0, 11)]),
        )]);
        let subs = vec![Sub::L4Book { coin: "BTC".into() }];
        let pinned = Default::default();
        let mut out = Recorder::default();
        let mut last = None;
        assert!(
            rebootstrap(&mut out, &books, &subs, &pinned, &mut last)
                .await
                .unwrap(),
            "the first gap re-bootstraps"
        );
        assert_eq!(out.0.len(), 1);
        assert!(
            !rebootstrap(&mut out, &books, &subs, &pinned, &mut last)
                .await
                .unwrap(),
            "a second gap inside the window must not re-send the whole book"
        );
        assert_eq!(out.0.len(), 1);
    }

    /// Not a regression guard — it asserts nothing — but the figures behind the fan-out above, so a
    /// future reader can re-measure instead of trusting the commit message. Run with
    /// `cargo test --release -- --ignored measure_the_per_client_cost --nocapture`.
    #[test]
    #[ignore = "measurement, not an assertion"]
    fn measure_the_per_client_cost() {
        let n = 44_598u64;
        let acc = accumulated(
            (0..n)
                .map(|i| (BookSide::Bid, 100.0 - (i as f64) / 1000.0, 1.0, i + 1))
                .collect(),
        );
        let books = replay(vec![(key(), acc.clone())]);
        let t = std::time::Instant::now();
        for _ in 0..10 {
            let _ = take_market(&books, &key(), "BTC", BookAccumulator::clone);
        }
        println!("clone (lock-held):   {:?}", t.elapsed() / 10);
        let t = std::time::Instant::now();
        for _ in 0..10 {
            let _ = acc.price_fold();
        }
        println!("price_fold:          {:?}", t.elapsed() / 10);
        let t = std::time::Instant::now();
        for _ in 0..10 {
            let _ = acc.to_book(&key(), ReplayScope::Orders);
        }
        println!("to_book(Orders):     {:?}", t.elapsed() / 10);
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
            // A partial fill of an order this channel has already published — the `update` variant.
            BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 100.5,
                size: 1.5,
                order_id: 2,
            },
        ]);
        assert_eq!(
            render_l4book_diff(&b, "BTC", &published_sizes(&[(2, 3.0)])).unwrap(),
            include_str!("../../tests/fixtures/hl_l4book_updates_golden.json").trim_end(),
            "regenerate tests/fixtures/hl_l4book_updates_golden.json from this renderer"
        );
    }
}
