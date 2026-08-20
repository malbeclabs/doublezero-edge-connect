//! Normalized feed messages re-served over WebSocket to any trading engine.
//! Wire format is engine-agnostic JSON - see PROTOCOL.md.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use serde::{Deserialize, Serialize};

/// The aggressor (taker) side of a trade. Serializes as `"buy"`/`"sell"`/`"unknown"` (the PROTOCOL.md
/// wire values) — a fixed enum rather than an owned `String`, so building a trade allocates nothing
/// for the side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
    Unknown,
}

impl Side {
    /// Map the edge-feed-spec Trade `aggressor_side` wire byte (1=Buy, 2=Sell, 0/other=Unknown).
    pub fn from_code(code: u8) -> Self {
        match code {
            1 => Side::Buy,
            2 => Side::Sell,
            _ => Side::Unknown,
        }
    }
}

/// Return the process-wide interned `Arc<str>` for a static venue name, so the ingest hot path
/// clones a cached `Arc` (a refcount bump) instead of allocating a fresh `String`/`Arc` per message.
/// Venues are a tiny fixed set (a handful of feeds), so the interner is populated during warmup and
/// then read-only. Backed by an `RwLock` so the steady-state path takes only a *shared* read lock
/// (uncontended across the ingest tasks) — the exclusive write lock is taken once per venue, the
/// first time it is seen, not per message.
pub fn venue_arc(venue: &'static str) -> Arc<str> {
    static INTERN: OnceLock<RwLock<HashMap<&'static str, Arc<str>>>> = OnceLock::new();
    intern_static(&INTERN, venue)
}

/// The same interner for a feed **category** (`ingest::feeds::Feed::category`), which the arbiter's
/// tape gate pairs with the venue to key one entry per *universe* rather than per venue. Interned
/// for exactly the reason venues are: that key is built on the trade hot path, once per print, and
/// `Arc::from(&str)` there would allocate and copy per message.
///
/// A separate map from [`venue_arc`]'s on purpose — the two namespaces overlap ("perps" is no
/// venue, but nothing stops a future registry name colliding), and sharing one map would hand out
/// the same `Arc` for both, quietly making a venue and a category equal by pointer for anything
/// that ever compares them.
pub fn category_arc(category: &'static str) -> Arc<str> {
    static INTERN: OnceLock<RwLock<HashMap<&'static str, Arc<str>>>> = OnceLock::new();
    intern_static(&INTERN, category)
}

/// Shared body of the two interners above: a `&'static str` keyed cache of `Arc<str>`, read under a
/// shared lock in steady state and written once per distinct string.
fn intern_static(
    intern: &OnceLock<RwLock<HashMap<&'static str, Arc<str>>>>,
    s: &'static str,
) -> Arc<str> {
    let map = intern.get_or_init(|| RwLock::new(HashMap::new()));
    // Steady state: already interned -> shared read lock, clone the cached `Arc`.
    if let Some(arc) = map.read().unwrap_or_else(|e| e.into_inner()).get(s) {
        return arc.clone();
    }
    // First sighting: take the write lock and insert (re-checking under the lock in case another
    // task interned it in the race window).
    map.write()
        .unwrap_or_else(|e| e.into_inner())
        .entry(s)
        .or_insert_with(|| Arc::from(s))
        .clone()
}

/// Serde default for `source` on payloads written before the field existed. `Arc<str>` has no
/// `Default`, so the field needs an explicit default function rather than `#[serde(default)]`.
pub fn empty_source() -> Arc<str> {
    Arc::from("")
}

/// Default for the producer-side-only `category` field carried by [`NormalizedInstrument`] and
/// [`NormalizedTrade`] (`#[serde(skip)]` — see their docs for why it must never reach the wire).
/// `Arc<str>` has no `Default`, so this needs the same explicit function [`empty_source`] does.
pub fn empty_category() -> Arc<str> {
    Arc::from("")
}

/// A normalized two-sided top-of-book update from any venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedQuote {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    pub symbol: Arc<str>,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    /// Orders/sources at the best bid/ask ("Bid/Ask Source Count" in the edge-feed-spec TOB; the
    /// canonical `bbo_hash` `bid_n`/`ask_n`). 0 when the venue does not report it. Part of the
    /// top-of-book identity, so a change here is a distinct quote even at an unchanged price/size.
    #[serde(default)]
    pub bid_n: u16,
    #[serde(default)]
    pub ask_n: u16,
    /// Venue/source timestamp (nanoseconds since epoch), 0 if unknown.
    pub source_ts_ns: u64,
    /// When the bridge received it (user-space wall clock, nanoseconds since epoch).
    /// Taken *after* frame decode - kept for the kernel-vs-userspace jitter comparison.
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME nanoseconds),
    /// captured in the driver softirq *before* user-space. 0 when unavailable (e.g. the
    /// socket option is unsupported). This is the defendable wire-adjacent arrival time.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (nanoseconds since epoch) sampled by the WS server the instant before this
    /// quote is serialized and written to a subscriber. With `kernel_rx_ts_ns` / `recv_ts_ns`
    /// this decomposes the bridge's internal transit (kernel -> user-space -> WS hand-off).
    /// 0 until the WS server stamps it.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}

/// A normalized trade print (last sale) from any venue. Like [`NormalizedQuote`] it rides the
/// same four latency timestamps; unlike a quote it is a point-in-time event, not full state, so a
/// dropped trade is a missed print (not a stale book) and there is nothing to replay on connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedTrade {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    pub symbol: Arc<str>,
    /// The publisher's `channel_id`: the instrument set this feed carries. Filterable. `0` for a
    /// source whose wire has no channel concept of its own (the public WS backstops) — see
    /// `ingest::public_feeder::resolve_instrument`, which resolves the real value from the edge
    /// catalog instead where one exists.
    #[serde(default)]
    pub channel: u32,
    /// Instrument id, unique within `channel`. Additive alongside `channel` (see its doc): together
    /// they are the identity `history::Key` groups on, closing the gap that let a price-aggregated
    /// venue's mirrored arms (identical instrument set, distinct `channel`) drop every trade rather
    /// than risk misattributing one to the wrong arm.
    #[serde(default)]
    pub instrument_id: u32,
    /// The instrument **universe** this trade's row carries (`ingest::feeds::Feed::category`),
    /// stamped by the emitting processor from its `FrameCtx::category` and read back by
    /// `ingest::reconcile::feed_history` to key `history::Key` on the same grain
    /// `model::BookKey`/`authority::MarketKey` already use. Producer-side only: two disjoint
    /// universes under one Source ID can share `(channel, instrument_id)`, so without this a
    /// history lookup or a channel purge cannot tell which universe a trade or a stored product
    /// belongs to. Never serialized — PROTOCOL.md carries no category, and a consumer has no use
    /// for a producer-side arbitration key.
    #[serde(skip, default = "empty_category")]
    pub category: Arc<str>,
    pub price: f64,
    pub size: f64,
    /// `"buy"`, `"sell"`, or `"unknown"` - the aggressor (taker) side.
    pub aggressor_side: Side,
    /// Venue-assigned trade identifier.
    pub trade_id: u64,
    /// Session cumulative traded volume reported by the venue (decimal), 0 if not provided.
    pub cumulative_volume: f64,
    /// Venue/source timestamp (nanoseconds since epoch), 0 if unknown.
    pub source_ts_ns: u64,
    /// When the bridge received it (user-space wall clock, ns since epoch), after frame decode.
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME ns), 0 when unavailable.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (ns since epoch) stamped by the WS server just before send; 0 until stamped.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}

/// A normalized derived mid price for an instrument (from the Midpoint sibling feed). Like a
/// quote it is full state per instrument (the latest mid), so it self-heals on the next message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedMidpoint {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    pub symbol: Arc<str>,
    pub mid: f64,
    /// How the mid was computed (0 = the instrument's default method).
    pub method: u8,
    /// Quality bits: 0=stale, 1=one-sided, 2=crossed/locked, 3=synthetic.
    pub quality_flags: u8,
    /// Venue timestamp of the underlying book state (ns since epoch), 0 if unknown.
    pub book_ts_ns: u64,
    /// When the publisher computed the mid (ns since epoch), 0 if unknown.
    pub compute_ts_ns: u64,
    /// When the bridge received it (user-space wall clock, ns since epoch), after frame decode.
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME ns), 0 when unavailable.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (ns since epoch) stamped by the WS server just before send; 0 until stamped.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}

/// A normalized order-book depth snapshot, derived in the bridge from the Market-by-Order feed.
/// Each message is the **full** top-N of both sides (not a delta), so - like a quote - it
/// self-heals: a consumer that drops one under backpressure recovers on the next snapshot. Levels
/// are `[price, size]` decimal pairs, best first (bids high->low, asks low->high).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedDepth {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    pub symbol: Arc<str>,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
    /// Timestamp of the latest applied book event (ns since epoch), 0 if unknown.
    pub source_ts_ns: u64,
    /// When the bridge produced this snapshot (user-space wall clock, ns since epoch).
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME ns), 0 when unavailable.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (ns since epoch) stamped by the WS server just before send; 0 until stamped.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}

/// What one entry of a [`NormalizedBook`] batch does to the consumer's book.
///
/// **A `Clear` is the only thing that re-baselines**, which is why a re-baseline is a batch led by
/// one rather than a separate message type: the reference consumer's book dispatcher branches on
/// this action alone and never reads a snapshot flag, so a boolean "this is a snapshot" field would
/// be silently ineffective there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookAction {
    /// Discard the named side(s) before applying the rest of the batch.
    Clear,
    /// Set the level at `price` to `size` (an absolute quantity, not a delta).
    Update,
    /// Remove the level at `price`. `size` is `0`.
    Delete,
}

/// Which side of the book a [`BookChange`] touches. `Both` occurs only on a `Clear`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    Bid,
    Ask,
    Both,
}

/// One book change. `size` is the **absolute** resulting quantity — of the level for a
/// price-aggregated change, of the order for an order-level one — never a delta, so a consumer that
/// misses nothing needs no arithmetic: set it, or remove it when the action is `Delete`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookChange {
    pub action: BookAction,
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
    /// The venue's own order id for an order-level (L3) change, or `0` when the change is
    /// price-aggregated and carries no order identity. Never `0` on a Market-by-Order feed: a
    /// consumer that keys an L3 book by id reads `0` as "aggregate me", silently degrading to L2.
    #[serde(default)]
    pub order_id: u64,
}

/// A batch of price-level changes for one instrument — the incremental order-book product, derived
/// in the bridge from the Market-by-Price feed's snapshot+delta stream.
///
/// **`(venue, channel, instrument_id)` is the identity; `symbol` is a display label.** The wire
/// `symbol` is a fixed 16-byte field the publisher fills by keeping the rightmost 16 bytes of the
/// venue's ticker — silently, with no hash and no length check — so on venues with long tickers
/// distinct markets collide on it and a consumer keying on `symbol` would merge two books.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedBook {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    /// Display label. Not unique in general — see the type docs.
    pub symbol: Arc<str>,
    /// The publisher's `channel_id`: the instrument set this feed carries. Filterable.
    pub channel: u32,
    /// Instrument id, unique within `channel`.
    pub instrument_id: u32,
    /// The instrument **universe** this batch's row carries, stamped by the emitting processor from
    /// its `FrameCtx::category` — the same field, for the same reason, as
    /// [`NormalizedInstrument::category`]: it completes the [`BookKey`] a sink needs to resolve this
    /// batch's market in [`BookSnapshot`]. Never serialized.
    #[serde(skip, default = "empty_category")]
    pub category: Arc<str>,
    /// Whether this market's changes are order-level (each `size` one *order's* absolute quantity,
    /// `order_id` non-zero) rather than price-aggregated. **Never serialized** — it selects the wire
    /// `type` (`order_book` vs `book`) in `sinks::ws::prepare`, which is where the distinction
    /// exists. It cannot be recovered from `changes`: an order-level re-baseline's leading `Clear`
    /// carries `order_id: 0` and a lone clear is a complete message, so content alone cannot tell
    /// the two apart.
    #[serde(skip)]
    pub order_level: bool,
    pub changes: Vec<BookChange>,
    /// Advisory: this batch is part of a rebuild rather than ordinary activity. Deliberately NOT
    /// what re-baselines a consumer — `changes[0].action == Clear` is.
    pub snapshot: bool,
    /// The final batch of a logical book event. **Mandatory** — a buffering consumer wedges
    /// permanently without it, including on a re-baseline that is only a clear.
    pub last: bool,
    /// Timestamp of the latest applied book event (ns since epoch), 0 if unknown.
    pub source_ts_ns: u64,
    /// When the bridge produced this batch (user-space wall clock, ns since epoch).
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME ns), 0 when unavailable.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (ns since epoch) stamped by the WS server just before send; 0 until stamped.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}

/// A normalized instrument definition (so subscribers know precision/venue).
///
/// Carries the same `(channel, instrument_id)` identity pair as [`NormalizedBook`], so a consumer
/// joins a book to its definition on the identity rather than on `symbol` — which collides across
/// markets on venues with long tickers (see the `NormalizedBook` docs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedInstrument {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    pub symbol: Arc<str>,
    /// The publisher's `channel_id`: the instrument set this definition came from. Filterable.
    #[serde(default)]
    pub channel: u32,
    /// Instrument id, unique within `channel`.
    #[serde(default)]
    pub instrument_id: u32,
    /// The instrument **universe** this definition's row carries
    /// (`ingest::feeds::Feed::category`), stamped by the emitting processor from its
    /// `FrameCtx::category`. Part of `InstrumentSnapshot`'s key (see there) for the same reason
    /// `BookKey` already carries it: two disjoint universes under one Source ID can share
    /// `(channel, instrument_id)`, and a category-blind catalog either overwrites one universe's
    /// definition with the other's or, on lookup, resolves the wrong one. Never serialized —
    /// PROTOCOL.md carries no category, and a consumer has no use for a producer-side
    /// arbitration key.
    #[serde(skip, default = "empty_category")]
    pub category: Arc<str>,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// A venue-level feed-health status (the PROTOCOL.md `status` candidate extension). Emitted when
/// the bridge's quote (mktdata) multicast for a venue goes silent past the idle watchdog, and
/// again when quotes recover - so consumers can gray out / restore that source. Carries no symbol
/// (it is about the whole venue feed); consumers ignoring unknown `type`s skip it harmlessly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedStatus {
    pub venue: Arc<str>,
    /// The source this message came from — the registry name for `source_id`. Always equal to
    /// `venue`, which it replaces; `venue` is deprecated and removed at a future break.
    #[serde(default = "empty_source")]
    pub source: Arc<str>,
    /// The wire Source ID, verbatim — passed through unmodified from what the publisher stamped,
    /// or `0` when the feed names no registry row. `source` is that ID's registry name.
    #[serde(default)]
    pub source_id: u16,
    /// `"down"` when the quote feed has gone silent, `"ok"` once quotes flow again.
    pub state: String,
    /// Milliseconds the quote feed has been silent (0 when `state == "ok"`).
    pub stale_ms: u64,
    /// Wall clock (ns since epoch) this status was emitted.
    pub ts_ns: u64,
}

/// The tagged message envelope sent to WebSocket subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FeedMessage {
    Instrument(NormalizedInstrument),
    Quote(NormalizedQuote),
    Trade(NormalizedTrade),
    Midpoint(NormalizedMidpoint),
    Depth(NormalizedDepth),
    Book(NormalizedBook),
    /// The order-level book, as it appears **on the wire only**. Internally there is one book
    /// product — one accumulator, one authority gate, one replay entry — so the whole pipeline
    /// carries [`Self::Book`] and only the serializer distinguishes them. Constructed in
    /// `sinks::ws` at the two points that render JSON, from `NormalizedBook::order_level`; nothing
    /// broadcasts it. A separate `type` is what keeps this additive: PROTOCOL.md's
    /// forward-compatibility rule has consumers ignore unknown *types*, and a v1 consumer that
    /// ignored a new `order_id` *field* on `book` would key order-level changes by price and
    /// silently corrupt its book instead.
    OrderBook(NormalizedBook),
    Status(FeedStatus),
}

impl FeedMessage {
    /// The (venue, symbol) this message is about - used for per-subscriber filtering. A `Status`
    /// is venue-level and carries no symbol, so it reports an empty symbol (the WS server matches
    /// it by venue alone - see `ws_server`).
    pub fn venue_symbol(&self) -> (&str, &str) {
        match self {
            FeedMessage::Instrument(i) => (i.venue.as_ref(), i.symbol.as_ref()),
            FeedMessage::Quote(q) => (q.venue.as_ref(), q.symbol.as_ref()),
            FeedMessage::Trade(t) => (t.venue.as_ref(), t.symbol.as_ref()),
            FeedMessage::Midpoint(m) => (m.venue.as_ref(), m.symbol.as_ref()),
            FeedMessage::Depth(d) => (d.venue.as_ref(), d.symbol.as_ref()),
            FeedMessage::Book(b) | FeedMessage::OrderBook(b) => {
                (b.venue.as_ref(), b.symbol.as_ref())
            }
            FeedMessage::Status(s) => (s.venue.as_ref(), ""),
        }
    }

    /// The `channel_id` this message is about, for per-channel subscription filtering. The
    /// incremental `book` product and the `instrument` definition that scales it carry one; every
    /// other type is venue/symbol-scoped.
    pub fn channel(&self) -> Option<u32> {
        match self {
            FeedMessage::Book(b) | FeedMessage::OrderBook(b) => Some(b.channel),
            FeedMessage::Instrument(i) => Some(i.channel),
            _ => None,
        }
    }
}

/// Latest known instrument definitions, keyed by `(venue, category, channel, instrument_id)`,
/// shared between the receivers (which update it) and the WebSocket server (which replays it to
/// each new subscriber so reference data arrives before quotes - otherwise a client that connects
/// mid-stream sees a quote first and has to guess the price/qty precision).
///
/// The key is [`NormalizedBook`]/[`NormalizedInstrument`]'s wire identity triple **prefixed with
/// the arbitration scope** (`category`), exactly [`BookKey`]/`ingest::authority::MarketKey` —
/// and, as there, that shared grain is load-bearing, not incidental: two disjoint instrument
/// universes under one Source ID have independent id spaces and can collide on `(channel,
/// instrument_id)`. A category-blind key (the triple alone) let one universe's `upsert_instrument`
/// silently overwrite the other's definition, and — since only one survived — no lookup fix on
/// top could have recovered it: `sinks/api.rs`'s market resolution would return whichever universe
/// happened to write last, for every request, regardless of which one the caller actually meant.
/// `category` is never serialized (see [`NormalizedInstrument::category`]) — readers destructure
/// the key exactly as [`BookReplay`] does.
///
/// Within one category the key is still not `(venue, symbol)`: `symbol` is a display label — on
/// the price-aggregated protocol a fixed 16-byte wire field the publisher fills by keeping a
/// ticker's rightmost 16 bytes with no hash and no length check, so two genuinely different
/// markets on a venue with a long ticker can and do collide on it (confirmed against a real
/// capture — see `tests/fixtures/PROVENANCE.md`). It also does NOT distinguish by protocol/feed
/// *within* one category: when one venue is served by multiple feeds of the same universe sharing
/// a channel/instrument id (e.g. Hyperliquid TOB + MBO both reporting `channel=0`, both the
/// registry's default category), both write the same entry (last-writer-wins). Those feeds are
/// expected to agree on precision; `upsert_instrument` in `processor.rs` warns if their exponents
/// diverge.
pub type InstrumentSnapshot =
    Arc<Mutex<HashMap<(Arc<str>, Arc<str>, u32, u32), NormalizedInstrument>>>;

/// Latest order-book `depth` snapshot per `(venue, symbol)`, derived from the Market-by-Order feed
/// and shared with the WebSocket server so it can replay the current book to a newly-connecting
/// subscriber (depth is full state, so one replayed snapshot bootstraps the consumer immediately
/// instead of making it wait for the next periodic one). Updated by the MBO receiver.
pub type DepthSnapshot = Arc<Mutex<HashMap<(Arc<str>, Arc<str>), NormalizedDepth>>>;

/// Accumulated book state for one market, so a connecting or newly-subscribing client can be
/// bootstrapped immediately instead of waiting a full snapshot cycle.
///
/// `depth` is full state, so its replay map stores the last message. `book` is incremental, so the
/// last batch tells a fresh client nothing — this performs the same accumulation a consumer does and
/// materializes a `clear` plus the complete level set on demand. Levels are keyed by the price
/// canonicalized to a `10^-8` fixed-point integer, because `f64` is not `Ord`; the original `f64` is
/// kept alongside so replayed prices are byte-identical to the streamed ones.
#[derive(Debug, Clone)]
pub struct BookAccumulator {
    symbol: Arc<str>,
    bids: std::collections::BTreeMap<i128, (f64, f64)>,
    asks: std::collections::BTreeMap<i128, (f64, f64)>,
    /// Resting orders for an order-level (L3) market as `(is_bid, price_key, price, size)`, keyed by
    /// the venue's order id. Empty for a price-aggregated market, whose changes carry `order_id == 0`
    /// and live in `bids`/`asks`; the two populations never coexist for one market.
    orders: HashMap<u64, (bool, i128, f64, f64)>,
    /// Whether this market is order-level — see [`BookAccumulator::is_order_level`].
    order_level: bool,
    /// Changes of a logical event still awaiting its `last` batch — see [`BookAccumulator::apply`].
    pending: Vec<BookChange>,
    pending_ts_ns: u64,
    source_ts_ns: u64,
    /// Whether a producer re-baseline has been folded in, i.e. whether these levels are the market's
    /// **whole** book rather than only what has changed since accumulation started. See
    /// [`BookAccumulator::baselined`].
    baselined: bool,
    /// The producer's Source ID for this market, retained so `to_book` can stamp a materialized
    /// re-baseline or replay with it. Not derivable here — `to_book`'s `venue` comes from the map
    /// key, but the id is not part of that key.
    source_id: u16,
}

/// One folded price level as `(price, total size, resting order count)`.
pub type CountedLevel = (f64, f64, u32);

/// How much detail a replayed `book` re-baseline carries.
///
/// **Unset follows the market**, which is what a consumer needs by default: a bootstrap of price levels
/// followed by a live stream of order-level changes cannot be reconciled at all — each change carries one
/// *order's* absolute size, and applying it as a level's size corrupts the book — so the granularity has
/// to match what the market streams. Asking for `Levels` on an order-level market is therefore only
/// useful to a consumer that folds the stream itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayScope {
    /// Price levels, each carrying `order_id == 0`.
    Levels,
    /// Every resting order, each carrying the venue's `order_id`.
    Orders,
}

/// Cap on changes buffered for one unterminated logical event. The producer is an unauthenticated
/// datagram source, so a stream that never sets `last` must not grow this without limit; the cap sits
/// far above any real market's full-book rebuild, and overflowing it desynchronizes the accumulator
/// rather than silently dropping changes from a book still claimed to be complete.
const MAX_PENDING_CHANGES: usize = 8192;

/// Cap on resting orders one market's replay state holds — the same ceiling `ingest::book`'s own
/// `MAX_ORDERS_PER_BOOK` puts on the producer, so a well-behaved market never reaches it.
const MAX_ACCUMULATED_ORDERS: usize = 1 << 18;

/// Return type of [`BookAccumulator::top_levels`]: `(bids, asks, true bid count, true ask count)`
/// — named so clippy's `type_complexity` lint doesn't flag the bare tuple at the signature.
pub type TopLevels = (Vec<(f64, f64)>, Vec<(f64, f64)>, usize, usize);

impl BookAccumulator {
    pub fn new(symbol: Arc<str>) -> Self {
        Self {
            symbol,
            bids: std::collections::BTreeMap::new(),
            asks: std::collections::BTreeMap::new(),
            orders: HashMap::new(),
            order_level: false,
            pending: Vec::new(),
            pending_ts_ns: 0,
            source_ts_ns: 0,
            baselined: false,
            source_id: 0,
        }
    }

    /// Whether these levels are the market's complete book, so materializing them as a re-baseline is
    /// honest. False until a producer re-baseline (a `Clear` of both sides) has been folded in: an
    /// accumulator seeded mid-stream holds only the levels that have moved since, and publishing that
    /// as `snapshot: true` would tell a consumer to discard the levels it is missing.
    pub fn baselined(&self) -> bool {
        self.baselined
    }

    /// Buffer one broadcast batch, and fold the whole logical event into the book on its `last`
    /// batch.
    ///
    /// Honoring `last` is what keeps [`BookAccumulator::to_book`] honest: it stamps its output
    /// `last: true`, so if the levels could be half of a multi-batch rebuild a replayed client would
    /// publish a torn book (one side missing, or a crossed inside market) as complete. Buffering is
    /// also exactly what PROTOCOL.md asks a consumer to do, so the bound on this buffer is the
    /// producer's event size — the same one the consumer already pays.
    pub fn apply(&mut self, b: &NormalizedBook) {
        self.symbol = b.symbol.clone();
        self.source_id = b.source_id;
        // 0 is the "unknown" sentinel, never a real time: a batch without one must not blank the
        // last known event time on every subsequent replay.
        if b.source_ts_ns != 0 {
            self.pending_ts_ns = b.source_ts_ns;
        }
        // A non-finite price would saturate the fixed-point key (NaN to 0, inf to i128::MIN/MAX),
        // silently merging unrelated levels into one entry that then lives in the replay map forever.
        self.pending.extend(
            b.changes
                .iter()
                .filter(|c| c.price.is_finite() && c.size.is_finite()),
        );
        if !b.last {
            // The cap guards only an event still waiting for its `last`: an event that outgrows it is
            // abandoned rather than truncated, since keeping the levels it did fold in while dropping
            // the rest would leave a book still claiming to be complete. A *terminated* batch folds
            // below whatever its size, or an order-level snapshot — tens of thousands of orders in one
            // batch — could never baseline.
            if self.pending.len() > MAX_PENDING_CHANGES {
                self.pending.clear();
                self.baselined = false;
            }
            return;
        }
        let mut cleared = (false, false);
        for c in std::mem::take(&mut self.pending) {
            let key = (c.price * 10f64.powi(8)).round() as i128;
            // An order-level change is keyed by its order id, not its price: two orders rest at one
            // price, and only the id says which of them moved. A `Clear` is the exception: it names a
            // *side*, not an order, so it always carries `order_id == 0` and must discard that side of
            // **both** populations — routing it by id would leave every order of a re-baselined-away
            // book resting in the replay map forever.
            if c.order_id != 0 {
                // A `Clear` names a side, so an id on it is a producer bug rather than evidence.
                self.order_level |= !matches!(c.action, BookAction::Clear);
                match c.action {
                    BookAction::Delete => {
                        self.orders.remove(&c.order_id);
                    }
                    // A zero size is how an order-level producer says the order is gone; resting it
                    // would leave a phantom the consumer keeps forever.
                    BookAction::Update if c.size == 0.0 => {
                        self.orders.remove(&c.order_id);
                    }
                    BookAction::Update => {
                        let is_bid = matches!(c.side, BookSide::Bid);
                        self.orders
                            .insert(c.order_id, (is_bid, key, c.price, c.size));
                    }
                    // A `Clear` carrying an order id is a producer bug: it names a side, and acting on
                    // one order would clear neither side. Fall through to the side-scoped arms below.
                    BookAction::Clear => self.clear_side(c.side, &mut cleared),
                }
                continue;
            }
            match (c.action, c.side) {
                (BookAction::Clear, _) => self.clear_side(c.side, &mut cleared),
                (BookAction::Delete, BookSide::Bid) => {
                    self.bids.remove(&key);
                }
                (BookAction::Delete, BookSide::Ask) => {
                    self.asks.remove(&key);
                }
                (BookAction::Update, BookSide::Bid) => {
                    self.bids.insert(key, (c.price, c.size));
                }
                (BookAction::Update, BookSide::Ask) => {
                    self.asks.insert(key, (c.price, c.size));
                }
                // `Both` is only ever a clear; a delete/update on it is a producer bug, not a
                // consumer-visible state, so ignore it rather than guessing a side.
                (_, BookSide::Both) => {}
            }
        }
        // A both-sided clear is the producer re-baselining: from here the levels are the whole book.
        self.baselined |= cleared == (true, true);
        // A producer whose own book overflows its order cap clears it silently, with no `Clear` on the
        // wire, so this population must bound itself rather than trusting a re-baseline to arrive.
        // Overflow abandons it: a book still claiming completeness is worse than none.
        if self.orders.len() > MAX_ACCUMULATED_ORDERS {
            self.orders.clear();
            self.baselined = false;
        }
        self.source_ts_ns = self.pending_ts_ns;
    }

    /// Discard one or both sides of the book, across **both** populations — a `Clear` names a side, and
    /// which population holds that side is a property of the market, not of the change.
    fn clear_side(&mut self, side: BookSide, cleared: &mut (bool, bool)) {
        if matches!(side, BookSide::Bid | BookSide::Both) {
            self.bids.clear();
            self.orders.retain(|_, (is_bid, ..)| !*is_bid);
            cleared.0 = true;
        }
        if matches!(side, BookSide::Ask | BookSide::Both) {
            self.asks.clear();
            self.orders.retain(|_, (is_bid, ..)| *is_bid);
            cleared.1 = true;
        }
    }

    /// Whether this market's book is order-level, i.e. whether its changes name orders rather than
    /// price levels. This is what the `book` bootstrap follows, so a client is bootstrapped at the
    /// same granularity the market streams.
    ///
    /// A property of the market, not of what it currently holds: an order-level book that empties is
    /// still order-level, and reading it off the population would silently stop the Hyperliquid
    /// sink publishing such a market. A fresh accumulator starts unknown, which is what a reset wants.
    pub fn is_order_level(&self) -> bool {
        self.order_level
    }

    /// The market's display label, as last seen on the wire.
    pub fn symbol(&self) -> &Arc<str> {
        &self.symbol
    }

    /// The current best bid (highest price), as `(price, size)`. Cheap — reads the top of the
    /// accumulator's own tree rather than materializing the whole book via [`Self::to_book`], which
    /// a caller that only wants the inside market (e.g. a `best_bid_ask`-style query) should never
    /// have to pay for. Available regardless of [`Self::baselined`]: reporting only the touched top
    /// level as "the best currently known" does not claim completeness the way replaying the whole
    /// book as a re-baseline would.
    ///
    /// ⚠️ **`None` for an order-level market**, which keeps nothing in that tree: its levels exist
    /// only as resting orders, and the order map is unordered, so there is no top to read and any
    /// answer here would be a pass over the whole book taken under a caller's lock. A caller wanting
    /// the inside market of such a market reads its `depth` snapshot instead — see
    /// `sinks/api.rs::best_levels`.
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.values().next_back().copied()
    }

    /// The current best ask (lowest price), as `(price, size)`. See [`Self::best_bid`].
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.values().next().copied()
    }

    /// The accumulator's last-applied event time. A caller that only needs a level slice (see
    /// [`Self::top_levels`]) rather than a full materialization shouldn't have to call
    /// [`Self::to_book`] — which stamps this same value — just to read it.
    pub fn source_ts_ns(&self) -> u64 {
        self.source_ts_ns
    }

    /// Fold the accumulated orders into price levels with the order count at each, bids best-first
    /// then asks best-first. The count is not derivable from a price-aggregated book, which is why
    /// this accumulator is order-keyed at all.
    pub fn price_fold(&self) -> (Vec<CountedLevel>, Vec<CountedLevel>) {
        let mut bids: std::collections::BTreeMap<i128, CountedLevel> = Default::default();
        let mut asks: std::collections::BTreeMap<i128, CountedLevel> = Default::default();
        for &(is_bid, key, price, size) in self.orders.values() {
            let side = if is_bid { &mut bids } else { &mut asks };
            let e = side.entry(key).or_insert((price, 0.0, 0));
            e.1 += size;
            e.2 += 1;
        }
        (
            bids.into_values().rev().collect(),
            asks.into_values().collect(),
        )
    }

    /// Every resting order as `(order_id, is_bid, price, size)`, bids best-first then asks best-first,
    /// ties broken by order id so a replay is byte-identical across runs.
    fn order_set(&self) -> Vec<(u64, bool, f64, f64)> {
        let (mut bids, mut asks): (Vec<_>, Vec<_>) = self
            .orders
            .iter()
            .map(|(&id, &(is_bid, key, price, size))| (id, is_bid, key, price, size))
            .partition(|&(_, is_bid, ..)| is_bid);
        bids.sort_by_key(|&(id, _, key, ..)| (std::cmp::Reverse(key), id));
        asks.sort_by_key(|&(id, _, key, ..)| (key, id));
        bids.into_iter()
            .chain(asks)
            .map(|(id, is_bid, _, price, size)| (id, is_bid, price, size))
            .collect()
    }

    /// The best `n` levels per side, best-first (bids high-to-low, asks low-to-high), plus each
    /// side's **true** level count. Cheap regardless of book size — reads straight off the
    /// accumulator's own `BTreeMap`s and takes at most `n` per side, the same discipline
    /// [`Self::best_bid`]/[`Self::best_ask`] use for the inside market — so a capped caller (e.g.
    /// `sinks/api.rs::book`, which serves at most a fixed per-side cap) never pays for
    /// [`Self::to_book`]'s full materialization of the book's real size just to keep a handful of
    /// rows. The counts are the true, uncapped per-side sizes rather than `min(len, n)`, because a
    /// capped caller needs the real count to tell an honest truncation from a complete book.
    pub fn top_levels(&self, n: usize) -> TopLevels {
        let bids: Vec<(f64, f64)> = self.bids.values().rev().take(n).copied().collect();
        let asks: Vec<(f64, f64)> = self.asks.values().take(n).copied().collect();
        (bids, asks, self.bids.len(), self.asks.len())
    }

    /// Materialize the current state as a re-baseline: `clear` first, then the whole book best-first —
    /// as price levels under [`ReplayScope::Levels`], or as individual orders (each carrying its
    /// `order_id`) under [`ReplayScope::Orders`]. Stamps `snapshot`/`last`, so only call it when
    /// [`Self::baselined`] holds — otherwise it claims completeness for a book that is missing every
    /// level which has not moved.
    pub fn to_book(&self, key: &BookKey, scope: ReplayScope) -> NormalizedBook {
        let mut out = self.to_clear(key);
        let changes = &mut out.changes;
        changes.reserve(self.bids.len() + self.asks.len() + self.orders.len());
        let level = |side: BookSide, price: f64, size: f64, order_id: u64| BookChange {
            action: BookAction::Update,
            side,
            price,
            size,
            order_id,
        };
        // The price-keyed population, which only a price-aggregated market has. Bids descend, asks
        // ascend, so the first of each is the inside market.
        for &(price, size) in self.bids.values().rev() {
            changes.push(level(BookSide::Bid, price, size, 0));
        }
        for &(price, size) in self.asks.values() {
            changes.push(level(BookSide::Ask, price, size, 0));
        }
        match scope {
            ReplayScope::Orders => {
                for (id, is_bid, price, size) in self.order_set() {
                    let side = if is_bid { BookSide::Bid } else { BookSide::Ask };
                    changes.push(level(side, price, size, id));
                }
            }
            ReplayScope::Levels => {
                let (bids, asks) = self.price_fold();
                for (price, size, _) in bids {
                    changes.push(level(BookSide::Bid, price, size, 0));
                }
                for (price, size, _) in asks {
                    changes.push(level(BookSide::Ask, price, size, 0));
                }
            }
        }
        out
    }

    /// This market's header carrying a bare `clear` and nothing else: what tells a consumer to drop the
    /// book. Separate from [`Self::to_book`] rather than a call to it, because a disowning has to be
    /// able to say so *without* materializing the book it stopped vouching for — 44k orders on the
    /// flagship market, allocated only to be thrown away.
    pub fn to_clear(&self, key: &BookKey) -> NormalizedBook {
        let (venue, category, channel, instrument_id) = key;
        NormalizedBook {
            venue: venue.clone(),
            source: venue.clone(),
            source_id: self.source_id,
            symbol: self.symbol.clone(),
            channel: *channel,
            instrument_id: *instrument_id,
            category: category.clone(),
            order_level: self.order_level,
            changes: vec![BookChange {
                action: BookAction::Clear,
                side: BookSide::Both,
                price: 0.0,
                size: 0.0,
                order_id: 0,
            }],
            snapshot: true,
            last: true,
            source_ts_ns: self.source_ts_ns,
            recv_ts_ns: now_ns(),
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }
}

/// Accumulated book state per market, replayed on connect and on each subscribe. Written by the
/// arbiter on the authority gate's admit decision, so it always holds the authoritative arm's book
/// rather than a discarded arm's copy.
///
/// Keyed identically to `ingest::authority::MarketKey` — `(venue, category, channel, instrument_id)`
/// — and that shared grain is load-bearing, not incidental. The gate's own per-market state
/// (`BookMarket`, `StickyAuthority::markets`) carries the arbitration scope because two instrument
/// universes under one Source ID have independent id spaces and can collide on
/// `(channel, instrument_id)`. A narrower key here would (a) merge two unrelated markets into one
/// accumulator, which the replay path then serves to every new client as full state, and (b) break
/// the invariant that a market's accumulators, its replay entry and its authority state are dropped
/// **together** — evicting one universe's market would delete the other's live entry while its
/// `last_admitted` survived, so it would never re-baseline and would stay `!baselined()`, invisible
/// to new clients for the life of the process. The `category` is never serialized: readers
/// destructure the key (`sinks/ws.rs`) and the wire carries only what PROTOCOL.md defines.
///
/// The writer owns two obligations the `depth` replay map already discharges (`arbiter.rs`): purge a
/// market's entry on session reset, or an ended session's book is replayed to a new client as an
/// authoritative re-baseline; and bound the entry count, since the key is wire-supplied.
pub type BookSnapshot = Arc<Mutex<BookReplay>>;

/// A market's replay key: the arbitration scope plus the wire identity. Structurally
/// `ingest::authority::MarketKey`, and required to stay so — see [`BookSnapshot`].
pub type BookKey = (Arc<str>, Arc<str>, u32, u32);

/// The map behind [`BookReplay`], named so a reader can borrow it without respelling the key.
pub type BookMap = HashMap<BookKey, BookAccumulator>;

/// The replay state behind [`BookSnapshot`]: every market's accumulated book, keyed on the full
/// [`BookKey`] (arbitration scope plus wire identity).
///
/// `sinks/ws.rs` iterates. `sinks/api.rs` resolves a market from a `NormalizedInstrument`, which
/// carries `category` (see its doc) alongside the wire identity, so its
/// `feed_kind_for`/`best_levels`/`book` handlers build the full `BookKey` directly and look it up
/// with [`get`](Self::get) in one hash hit — no ambiguity, no scan. A prior version kept a second
/// index from wire identity alone to full key, for a caller with no category to disambiguate with;
/// nothing calls that path anymore (every production lookup already has the category), so it was
/// removed rather than kept live on the hot path for no reader.
#[derive(Default)]
pub struct BookReplay {
    books: BookMap,
}

impl BookReplay {
    /// Replace one market's accumulator (the re-baseline path).
    pub fn insert(&mut self, key: BookKey, acc: BookAccumulator) {
        self.books.insert(key, acc);
    }

    /// The accumulator for `key`, created with `f` when the market is new (the streaming path).
    pub fn entry_or_insert_with(
        &mut self,
        key: &BookKey,
        f: impl FnOnce() -> BookAccumulator,
    ) -> &mut BookAccumulator {
        self.books.entry(key.clone()).or_insert_with(f)
    }

    /// Drop one market. The eviction and session-reset path.
    pub fn remove(&mut self, key: &BookKey) -> Option<BookAccumulator> {
        self.books.remove(key)
    }

    pub fn get(&self, key: &BookKey) -> Option<&BookAccumulator> {
        self.books.get(key)
    }

    pub fn contains_key(&self, key: &BookKey) -> bool {
        self.books.contains_key(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&BookKey, &BookAccumulator)> {
        self.books.iter()
    }

    pub fn len(&self) -> usize {
        self.books.len()
    }

    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    // Deliberately no `forget_channel` (or any other multi-market removal) here. This map is one
    // third of a triple this crate keeps in lockstep — a market's accumulator lives in
    // `ingest::arbiter::Arbiter::book_markets`, its replay entry here, and its
    // `ingest::authority::StickyAuthority::last_admitted` in the arbiter's `books` — and only the
    // arbiter can drop all three together. `Arbiter::reset_book_for_market` /
    // `Arbiter::forget_channel_books` are the seams; a caller reaching in here directly (as an
    // earlier version of the channel-departure purge did) leaves `last_admitted` behind, so a
    // restored channel whose arm is unchanged never re-baselines and a market silently stays
    // hidden from every new client. See those methods' docs.
}

/// Lock a shared `Mutex`, recovering the guard even if a previous holder panicked while holding it.
///
/// Every shared mutex in the ingest path (`InstrumentSnapshot`, `DepthSnapshot`, the arbiter) is
/// held only across panic-free critical sections (`HashMap`/`HashSet` work), so the protected state
/// is always left consistent. Recovering from poisoning rather than `.lock().unwrap()` keeps an
/// **unrelated** panic in one ingest task (e.g. the WS feeder) from cascading into every other
/// source the moment it next takes the lock — the failure-isolation contract.
pub fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Nanoseconds since the Unix epoch, for `recv_ts_ns`.
pub fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Monotonic nanoseconds from `CLOCK_MONOTONIC`. Unlike `std::time::Instant`, the raw
/// `clock_gettime` value is **comparable across processes on the same kernel**, so two
/// collectors (e.g. doublezero-edge-connect and hl-collector) can measure an inter-feed delta immune to
/// NTP steps/slew. Pair with `now_ns()` (wall clock) only to correlate with `source_ts`.
/// Also the arrival clock the shred forwarder stamps per datagram for its cross-group lead-time
/// metric (single process, so monotonic ns are directly comparable and immune to NTP steps).
pub fn now_mono_ns() -> u64 {
    use nix::time::{clock_gettime, ClockId};
    clock_gettime(ClockId::CLOCK_MONOTONIC)
        .map(|ts| (ts.tv_sec() as u64) * 1_000_000_000 + ts.tv_nsec() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `order_id` is additive: a payload written before the field still parses, and an order-level
    /// change round-trips its id. Zero is the price-aggregated sentinel — what Market-by-Price emits
    /// and what a consumer reads as "no order identity".
    #[test]
    fn book_change_order_id_is_additive_and_round_trips() {
        let legacy = r#"{"action":"update","side":"bid","price":1.5,"size":2.0}"#;
        let parsed: BookChange = serde_json::from_str(legacy).expect("legacy payload must parse");
        assert_eq!(
            parsed.order_id, 0,
            "an absent id defaults to the no-identity sentinel"
        );

        let c = BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price: 1.5,
            size: 2.0,
            order_id: 42,
        };
        let round: BookChange = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(round.order_id, 42);
    }

    const TEST_CATEGORY: &str = "testcategory";

    /// The replay key a test's accumulator materializes under.
    fn bkey(venue: &Arc<str>, channel: u32, instrument_id: u32) -> BookKey {
        (venue.clone(), TEST_CATEGORY.into(), channel, instrument_id)
    }

    fn book(changes: Vec<BookChange>, snapshot: bool, last: bool) -> NormalizedBook {
        NormalizedBook {
            venue: "KALSHI".into(),
            source: "KALSHI".into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            category: TEST_CATEGORY.into(),
            order_level: changes.iter().any(|c| c.order_id != 0),
            changes,
            snapshot,
            last,
            source_ts_ns: 1_781_019_263_715_344_015,
            recv_ts_ns: 1_781_019_263_715_501_230,
            kernel_rx_ts_ns: 1_781_019_263_715_300_010,
            ws_send_ts_ns: 0,
        }
    }

    /// The wire shape PROTOCOL.md documents, pinned exactly — field names and the `type` tag are the
    /// contract, so a rename is a breaking change a test must catch.
    #[test]
    fn book_serializes_to_the_documented_shape() {
        let m = FeedMessage::Book(book(
            vec![
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 150.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Delete,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 0.0,
                    order_id: 0,
                },
            ],
            false,
            true,
        ));
        let v: serde_json::Value = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "book");
        assert_eq!(v["venue"], "KALSHI");
        assert_eq!(v["symbol"], "KXBTCPERP");
        assert_eq!(v["channel"], 2);
        assert_eq!(v["instrument_id"], 41);
        assert_eq!(v["snapshot"], false);
        assert_eq!(v["last"], true);
        assert_eq!(v["changes"][0]["action"], "update");
        assert_eq!(v["changes"][0]["side"], "bid");
        assert_eq!(v["changes"][0]["price"], 0.62);
        assert_eq!(v["changes"][0]["size"], 150.0);
        assert_eq!(v["changes"][0]["order_id"], 0);
        assert_eq!(v["changes"][1]["action"], "delete");
        assert_eq!(v["changes"][1]["side"], "ask");
        assert!(v["source_ts_ns"].is_u64() && v["kernel_rx_ts_ns"].is_u64());
    }

    /// A re-baseline is structural: `changes[0].action == "clear"`, because the reference consumer's
    /// book dispatcher branches on the action alone and ignores any snapshot flag. `snapshot: true`
    /// is advisory only, so a consumer must be able to re-baseline from the clear with the flag
    /// stripped.
    #[test]
    fn a_rebaseline_leads_with_a_clear_action() {
        let m = book(
            vec![
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
                    price: 0.62,
                    size: 150.0,
                    order_id: 0,
                },
            ],
            true,
            true,
        );
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["changes"][0]["action"], "clear");
        assert_eq!(v["changes"][0]["side"], "both");
        assert_eq!(v["snapshot"], true);
        assert_eq!(v["last"], true, "mandatory even on a lone clear");
    }

    /// A lone clear on an empty book is a legal message and must still carry `last: true` — omitting
    /// it wedges a buffering consumer permanently.
    #[test]
    fn a_lone_clear_is_a_complete_message() {
        let m = book(
            vec![BookChange {
                action: BookAction::Clear,
                side: BookSide::Both,
                price: 0.0,
                size: 0.0,
                order_id: 0,
            }],
            true,
            true,
        );
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["changes"].as_array().unwrap().len(), 1);
        assert_eq!(v["last"], true);
    }

    #[test]
    fn book_round_trips() {
        let m = FeedMessage::Book(book(
            vec![BookChange {
                action: BookAction::Update,
                side: BookSide::Ask,
                price: 0.63,
                size: 7.5,
                order_id: 0,
            }],
            false,
            false,
        ));
        let back: FeedMessage = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        let FeedMessage::Book(b) = back else { panic!() };
        assert_eq!(b.channel, 2);
        assert_eq!(b.instrument_id, 41);
        assert!(!b.last);
    }

    /// `channel` is on `book` and nothing else, so the filter's channel dimension excludes every
    /// other type (see the ws filter tests).
    #[test]
    fn only_book_reports_a_channel() {
        let b = FeedMessage::Book(book(vec![], false, true));
        assert_eq!(b.channel(), Some(2));
        let q = FeedMessage::Status(FeedStatus {
            venue: "KALSHI".into(),
            source: "KALSHI".into(),
            source_id: 0,
            state: "ok".into(),
            stale_ms: 0,
            ts_ns: 1,
        });
        assert_eq!(q.channel(), None);
    }

    /// The identity triple is what a consumer keys on; `symbol` is a display label, so
    /// `venue_symbol` must still report it for the existing symbol filter to work.
    #[test]
    fn book_reports_its_venue_and_symbol_for_filtering() {
        let b = FeedMessage::Book(book(vec![], false, true));
        assert_eq!(b.venue_symbol(), ("KALSHI", "KXBTCPERP"));
    }

    /// The replay accumulator performs the same operation a consumer does, so a materialized book
    /// must reflect every applied batch: an update moves a level, a delete removes it, and the
    /// output leads with the `clear` that re-baselines the client.
    #[test]
    fn the_accumulator_materializes_the_applied_state() {
        let venue: Arc<str> = "KALSHI".into();
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&book(
            vec![
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
                    price: 0.61,
                    size: 10.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 20.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 30.0,
                    order_id: 0,
                },
            ],
            true,
            true,
        ));
        acc.apply(&book(
            vec![
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 25.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Delete,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 0.0,
                    order_id: 0,
                },
            ],
            false,
            true,
        ));

        let out = acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels);
        assert!(out.snapshot && out.last);
        assert_eq!(out.symbol.as_ref(), "KXBTCPERP");
        assert_eq!(
            out.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                // Bids descend: the inside market first.
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 25.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.61,
                    size: 10.0,
                    order_id: 0,
                },
            ],
            "the deleted ask must not be replayed"
        );
    }

    /// A multi-batch logical event must not be materialized half-applied: `to_book` stamps its
    /// output `last: true`, so a torn level set would be published to a replayed client as complete.
    #[test]
    fn a_batch_awaiting_its_last_is_not_materialized() {
        let venue: Arc<str> = "KALSHI".into();
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        let bid = |price, size| BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price,
            size,
            order_id: 0,
        };

        acc.apply(&book(vec![bid(0.61, 10.0)], false, true));
        assert_eq!(
            acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels)
                .changes
                .len(),
            2
        ); // clear + one bid

        // First half of a rebuild: buffered, not applied.
        acc.apply(&book(
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                bid(0.70, 1.0),
            ],
            true,
            false,
        ));
        let mid = acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels);
        assert_eq!(
            mid.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                bid(0.61, 10.0),
            ],
            "the pre-event state, not the half-cleared one"
        );

        // The closing batch commits the whole event at once.
        acc.apply(&book(vec![bid(0.71, 2.0)], true, true));
        assert_eq!(
            acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels)
                .changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                bid(0.71, 2.0),
                bid(0.70, 1.0),
            ]
        );
    }

    /// A non-finite price saturates the fixed-point level key, so unrelated levels would merge into
    /// one entry that then lives in the replay map forever. Drop the change instead.
    #[test]
    fn non_finite_prices_and_sizes_are_dropped() {
        let venue: Arc<str> = "KALSHI".into();
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&book(
            vec![
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: f64::NAN,
                    size: 1.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: f64::INFINITY,
                    size: 1.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: f64::NAN,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.64,
                    size: 5.0,
                    order_id: 0,
                },
            ],
            false,
            true,
        ));
        let out = acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels);
        assert_eq!(
            out.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.64,
                    size: 5.0,
                    order_id: 0,
                },
            ]
        );
    }

    /// `0` is the "unknown" sentinel for every timestamp on the wire, so a batch without one must not
    /// blank the last known event time on every later replay.
    #[test]
    fn a_zero_source_ts_does_not_blank_the_replayed_event_time() {
        let venue: Arc<str> = "KALSHI".into();
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&book(vec![], false, true));
        assert_eq!(
            acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels)
                .source_ts_ns,
            1_781_019_263_715_344_015
        );

        let mut unknown = book(vec![], false, true);
        unknown.source_ts_ns = 0;
        acc.apply(&unknown);
        assert_eq!(
            acc.to_book(&bkey(&venue, 2, 41), ReplayScope::Levels)
                .source_ts_ns,
            1_781_019_263_715_344_015
        );
    }

    #[test]
    fn a_quote_serializes_both_the_new_and_the_deprecated_source_fields() {
        let q = NormalizedQuote {
            venue: Arc::from("HYPERLIQUID"),
            source: Arc::from("HYPERLIQUID"),
            source_id: 1,
            symbol: Arc::from("SOL"),
            bid: 1.0,
            ask: 2.0,
            bid_size: 3.0,
            ask_size: 4.0,
            bid_n: 0,
            ask_n: 0,
            source_ts_ns: 0,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        };
        let v: serde_json::Value = serde_json::to_value(&q).unwrap();
        assert_eq!(v["venue"], "HYPERLIQUID");
        assert_eq!(v["source"], "HYPERLIQUID");
        assert_eq!(v["source_id"], 1);
    }

    /// A payload written before these fields existed must still deserialize, or every committed
    /// fixture and every older consumer's round-trip breaks.
    #[test]
    fn a_payload_without_the_new_fields_still_deserializes() {
        let json = r#"{"venue":"PHOENIX","symbol":"SOL","bid":1.0,"ask":2.0,
            "bid_size":3.0,"ask_size":4.0,"source_ts_ns":0,"recv_ts_ns":0}"#;
        let q: NormalizedQuote = serde_json::from_str(json).unwrap();
        assert_eq!(&*q.venue, "PHOENIX");
        assert_eq!(&*q.source, "");
        assert_eq!(q.source_id, 0);
    }

    /// A re-baseline and a WS replay both materialize through `to_book`. Both must carry the source id
    /// the producer resolved, not a placeholder — a consumer joining on `source_id` gets nothing from a
    /// book that reports 0.
    #[test]
    fn a_materialized_book_carries_the_accumulated_source_id() {
        let venue: Arc<str> = Arc::from("HYPERLIQUID");
        let mut acc = BookAccumulator::new(Arc::from("SOL"));
        acc.apply(&NormalizedBook {
            venue: venue.clone(),
            source: venue.clone(),
            source_id: 1,
            symbol: Arc::from("SOL"),
            channel: 0,
            instrument_id: 41,
            order_level: false,
            changes: vec![
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
                    size: 5.0,
                    order_id: 0,
                },
            ],
            snapshot: true,
            last: true,
            source_ts_ns: 7,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            category: crate::model::empty_category(),
        });

        let out = acc.to_book(&bkey(&venue, 0, 41), ReplayScope::Levels);
        assert_eq!(
            out.source_id, 1,
            "the id the producer resolved, not a placeholder"
        );
        assert_eq!(out.source, venue);
    }

    // ---- order-level (L3) accumulation ----

    fn order(
        action: BookAction,
        side: BookSide,
        price: f64,
        size: f64,
        order_id: u64,
    ) -> BookChange {
        BookChange {
            action,
            side,
            price,
            size,
            order_id,
        }
    }

    /// The accumulator holds orders, and price levels are a fold over them — including the count per
    /// level, which a price-keyed accumulator structurally cannot produce.
    #[test]
    fn the_accumulator_folds_orders_into_levels_with_counts() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![
                order(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
                order(BookAction::Update, BookSide::Bid, 100.0, 3.0, 2),
                order(BookAction::Update, BookSide::Bid, 99.0, 1.0, 3),
                order(BookAction::Update, BookSide::Ask, 101.0, 2.0, 4),
            ],
            false,
            true,
        ));
        let (bids, asks) = acc.price_fold();
        assert_eq!(
            bids,
            vec![(100.0, 8.0, 2), (99.0, 1.0, 1)],
            "two orders rest at 100, and the level is their sum"
        );
        assert_eq!(asks, vec![(101.0, 2.0, 1)]);
    }

    /// Removing the last order at a price leaves no phantom level behind — by `Delete`, and by the
    /// zero-size `Update` an order-level producer may send instead.
    #[test]
    fn removing_the_last_order_at_a_price_removes_the_level() {
        for gone in [
            order(BookAction::Delete, BookSide::Bid, 100.0, 0.0, 1),
            order(BookAction::Update, BookSide::Bid, 100.0, 0.0, 1),
        ] {
            let mut acc = BookAccumulator::new("BTC".into());
            acc.apply(&book(
                vec![order(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1)],
                false,
                true,
            ));
            acc.apply(&book(vec![gone], false, true));
            assert!(acc.price_fold().0.is_empty());
        }
    }

    /// A one-sided clear drops only that side's orders: clearing bids must not silently delete the
    /// asks a consumer is still holding. The `Clear` carries `order_id: 0` because that is the shape a
    /// producer emits — it names a side, not an order.
    #[test]
    fn a_one_sided_clear_spares_the_other_sides_orders() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![
                order(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
                order(BookAction::Update, BookSide::Ask, 101.0, 2.0, 2),
            ],
            false,
            true,
        ));
        acc.apply(&book(
            vec![order(BookAction::Clear, BookSide::Bid, 0.0, 0.0, 0)],
            false,
            true,
        ));
        let (bids, asks) = acc.price_fold();
        assert!(bids.is_empty());
        assert_eq!(asks, vec![(101.0, 2.0, 1)]);
    }

    /// A re-baseline replaces the book, so the orders the previous one held must be gone. They are
    /// exactly the orders that died while the publisher was gapped — keeping them would replay a
    /// phantom set to every client that connects afterwards, forever.
    #[test]
    fn a_rebaseline_discards_the_previous_order_population() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![
                order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0),
                order(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
                order(BookAction::Update, BookSide::Bid, 99.0, 4.0, 2),
            ],
            true,
            true,
        ));
        acc.apply(&book(
            vec![
                order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0),
                order(BookAction::Update, BookSide::Ask, 101.0, 7.0, 3),
            ],
            true,
            true,
        ));
        let (bids, asks) = acc.price_fold();
        assert!(bids.is_empty(), "the first baseline's orders must be gone");
        assert_eq!(asks, vec![(101.0, 7.0, 1)]);
    }

    /// The replay population bounds itself: a producer whose own book overflows its order cap clears it
    /// with no `Clear` on the wire, so nothing else would ever trim this.
    #[test]
    fn the_accumulated_order_population_is_bounded() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0)],
            true,
            true,
        ));
        for chunk in 0..=(MAX_ACCUMULATED_ORDERS as u64 / 4096) {
            let changes: Vec<_> = (0..4096)
                .map(|i| {
                    let id = chunk * 4096 + i + 1;
                    order(BookAction::Update, BookSide::Bid, id as f64, 1.0, id)
                })
                .collect();
            acc.apply(&book(changes, false, true));
        }
        assert!(acc.orders.len() <= MAX_ACCUMULATED_ORDERS);
    }

    /// A terminated batch folds however large it is: an order-level snapshot is tens of thousands of
    /// changes in one batch, and the unterminated-event cap must not reject it.
    #[test]
    fn a_terminated_batch_larger_than_the_pending_cap_still_baselines() {
        let mut acc = BookAccumulator::new("BTC".into());
        let mut changes = vec![order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0)];
        changes.extend(
            (1..=MAX_PENDING_CHANGES as u64 + 100)
                .map(|id| order(BookAction::Update, BookSide::Bid, id as f64, 1.0, id)),
        );
        let n = changes.len() - 1;
        acc.apply(&book(changes, true, true));
        assert!(
            acc.baselined(),
            "a complete snapshot must count as a baseline"
        );
        assert_eq!(acc.price_fold().0.len(), n);
    }

    /// An event still waiting for its `last` is what the cap bounds: an unterminated stream past it is
    /// abandoned rather than folded as a book claiming to be complete.
    #[test]
    fn an_unterminated_event_past_the_cap_is_abandoned() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0)],
            true,
            true,
        ));
        assert!(acc.baselined());
        let flood: Vec<_> = (1..=MAX_PENDING_CHANGES as u64 + 1)
            .map(|id| order(BookAction::Update, BookSide::Bid, id as f64, 1.0, id))
            .collect();
        acc.apply(&book(flood, false, false));
        assert!(!acc.baselined());
        assert!(acc.price_fold().0.is_empty());
    }

    /// A connecting client is bootstrapped with price levels by default, so an L2 consumer never pays
    /// for a full order population; asking for order scope gets the orders, each with its id.
    #[test]
    fn replay_scope_folds_to_levels_or_materializes_orders() {
        let venue: Arc<str> = "HYPERLIQUID".into();
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book(
            vec![
                order(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0),
                order(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
                order(BookAction::Update, BookSide::Bid, 100.0, 3.0, 2),
            ],
            true,
            true,
        ));

        let levels = acc.to_book(&bkey(&venue, 0, 1), ReplayScope::Levels);
        let updates = |b: &NormalizedBook| -> Vec<BookChange> {
            b.changes
                .iter()
                .filter(|c| c.action != BookAction::Clear)
                .copied()
                .collect()
        };
        let folded = updates(&levels);
        assert_eq!(folded.len(), 1, "two orders at one price fold to one level");
        assert_eq!(folded[0].size, 8.0);
        assert!(
            folded.iter().all(|c| c.order_id == 0),
            "a price level carries no order identity"
        );

        let orders = updates(&acc.to_book(&bkey(&venue, 0, 1), ReplayScope::Orders));
        assert_eq!(
            orders.iter().map(|c| c.order_id).collect::<Vec<_>>(),
            vec![1, 2],
            "every resting order, deterministically ordered"
        );
    }
}
