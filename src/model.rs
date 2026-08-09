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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// One price-level change. `size` is the level's **absolute** resulting quantity, never a delta, so
/// a consumer that misses nothing needs no arithmetic: set it, or remove it when the action is
/// `Delete`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookChange {
    pub action: BookAction,
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
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
            FeedMessage::Book(b) => (b.venue.as_ref(), b.symbol.as_ref()),
            FeedMessage::Status(s) => (s.venue.as_ref(), ""),
        }
    }

    /// The `channel_id` this message is about, for per-channel subscription filtering. The
    /// incremental `book` product and the `instrument` definition that scales it carry one; every
    /// other type is venue/symbol-scoped.
    pub fn channel(&self) -> Option<u32> {
        match self {
            FeedMessage::Book(b) => Some(b.channel),
            FeedMessage::Instrument(i) => Some(i.channel),
            _ => None,
        }
    }
}

/// Latest known instrument definitions, keyed by `(venue, channel, instrument_id)`, shared between
/// the receivers (which update it) and the WebSocket server (which replays it to each new
/// subscriber so reference data arrives before quotes - otherwise a client that connects
/// mid-stream sees a quote first and has to guess the price/qty precision).
///
/// The key is the same identity triple [`NormalizedBook`] and [`NormalizedInstrument`] already carry
/// (see their docs) — **not** `(venue, symbol)`. `symbol` is a display label: on the price-aggregated
/// protocol it is a fixed 16-byte wire field the publisher fills by keeping a ticker's rightmost 16
/// bytes with no hash and no length check, so two genuinely different markets on a venue with a long
/// ticker can and do collide on it (confirmed against a real capture — see
/// `tests/fixtures/PROVENANCE.md`). Keying this map on that label meant the second market's insert
/// silently destroyed the first's entry; the identity triple is unique per market by construction, so
/// both survive. It does NOT distinguish by protocol/feed within one venue: when one venue is served
/// by multiple feeds sharing a channel/instrument id (e.g. Hyperliquid TOB + MBO both reporting
/// `channel=0`), both write the same entry (last-writer-wins). Those feeds are expected to agree on
/// precision; `upsert_instrument` in `processor.rs` warns if their exponents diverge.
pub type InstrumentSnapshot = Arc<Mutex<HashMap<(Arc<str>, u32, u32), NormalizedInstrument>>>;

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

/// Cap on changes buffered for one unterminated logical event. The producer is an unauthenticated
/// datagram source, so a stream that never sets `last` must not grow this without limit; the cap sits
/// far above any real market's full-book rebuild, and overflowing it desynchronizes the accumulator
/// rather than silently dropping changes from a book still claimed to be complete.
const MAX_PENDING_CHANGES: usize = 8192;

impl BookAccumulator {
    pub fn new(symbol: Arc<str>) -> Self {
        Self {
            symbol,
            bids: std::collections::BTreeMap::new(),
            asks: std::collections::BTreeMap::new(),
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
        // An event that outgrows the buffer is abandoned rather than truncated: keeping the levels it
        // did fold in while dropping the rest would leave a book still claiming to be complete.
        if self.pending.len() > MAX_PENDING_CHANGES {
            self.pending.clear();
            self.baselined = false;
            return;
        }
        if !b.last {
            return;
        }
        let mut cleared = (false, false);
        for c in std::mem::take(&mut self.pending) {
            let key = (c.price * 10f64.powi(8)).round() as i128;
            match (c.action, c.side) {
                (BookAction::Clear, BookSide::Bid) => {
                    self.bids.clear();
                    cleared.0 = true;
                }
                (BookAction::Clear, BookSide::Ask) => {
                    self.asks.clear();
                    cleared.1 = true;
                }
                (BookAction::Clear, BookSide::Both) => {
                    self.bids.clear();
                    self.asks.clear();
                    cleared = (true, true);
                }
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
        self.source_ts_ns = self.pending_ts_ns;
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
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.values().next_back().copied()
    }

    /// The current best ask (lowest price), as `(price, size)`. See [`Self::best_bid`].
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.values().next().copied()
    }

    /// Materialize the current state as a re-baseline: `clear` first, then every level best-first.
    /// Stamps `snapshot`/`last`, so only call it when [`Self::baselined`] holds — otherwise it claims
    /// completeness for a book that is missing every level which has not moved.
    pub fn to_book(&self, venue: &Arc<str>, channel: u32, instrument_id: u32) -> NormalizedBook {
        let mut changes = Vec::with_capacity(self.bids.len() + self.asks.len() + 1);
        changes.push(BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
        });
        // Bids descend, asks ascend, so the first of each is the inside market.
        for &(price, size) in self.bids.values().rev() {
            changes.push(BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price,
                size,
            });
        }
        for &(price, size) in self.asks.values() {
            changes.push(BookChange {
                action: BookAction::Update,
                side: BookSide::Ask,
                price,
                size,
            });
        }
        NormalizedBook {
            venue: venue.clone(),
            source: venue.clone(),
            source_id: self.source_id,
            symbol: self.symbol.clone(),
            channel,
            instrument_id,
            changes,
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

/// The wire identity alone — all a consumer-facing reader can name, since neither PROTOCOL.md nor
/// the REST surface exposes the arbitration scope.
type BookIdentity = (Arc<str>, u32, u32);

fn book_identity(key: &BookKey) -> BookIdentity {
    (key.0.clone(), key.2, key.3)
}

/// The replay state behind [`BookSnapshot`]: every market's accumulated book, plus a secondary index
/// from **wire identity** to full key.
///
/// The index exists because the two readers name markets differently. `sinks/ws.rs` iterates, so it
/// needs nothing; `sinks/api.rs` resolves a market from a `NormalizedInstrument`, which carries
/// `(venue, channel, instrument_id)` and no category — and its `/v1/products` and `/v1/best_bid_ask`
/// handlers do that **once per instrument**, so a linear scan of this map turns those endpoints into
/// O(instruments x markets). At the tens of thousands of markets a single feed can carry, that is
/// hundreds of millions of comparisons per request. The index keeps it a hash hit.
///
/// The two maps are mutated **only** through this type's methods, so the index cannot drift from the
/// books it indexes: an eviction drops the accumulator and the index entry in one step, exactly as
/// the arbiter drops the replay entry and the authority's per-market state together. That
/// together-or-not-at-all property is the whole reason this is a struct and not a second map kept
/// beside the first.
///
/// ⚠️ **Residual ambiguity, deliberately not resolved here.** Two universes under one Source ID can
/// carry the same `(channel, instrument_id)`, so one identity can index several keys; the lookup
/// returns the first one indexed. The REST surface has no way to say which universe it means, and
/// **the category is what would resolve it** — a change to that surface, tracked separately. Do not
/// reason from today's `channel_id` ranges happening not to overlap: that separation is a numbering
/// convention owned upstream, it is mid-migration, and nothing here enforces it.
#[derive(Default)]
pub struct BookReplay {
    books: BookMap,
    by_identity: HashMap<BookIdentity, Vec<BookKey>>,
}

impl BookReplay {
    /// Replace one market's accumulator (the re-baseline path).
    pub fn insert(&mut self, key: BookKey, acc: BookAccumulator) {
        self.index(&key);
        self.books.insert(key, acc);
    }

    /// The accumulator for `key`, created with `f` when the market is new (the streaming path).
    pub fn entry_or_insert_with(
        &mut self,
        key: &BookKey,
        f: impl FnOnce() -> BookAccumulator,
    ) -> &mut BookAccumulator {
        self.index(key);
        self.books.entry(key.clone()).or_insert_with(f)
    }

    /// Drop one market: accumulator and index entry together. The eviction and session-reset path.
    pub fn remove(&mut self, key: &BookKey) -> Option<BookAccumulator> {
        let id = book_identity(key);
        if let Some(keys) = self.by_identity.get_mut(&id) {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.by_identity.remove(&id);
            }
        }
        self.books.remove(key)
    }

    pub fn get(&self, key: &BookKey) -> Option<&BookAccumulator> {
        self.books.get(key)
    }

    pub fn contains_key(&self, key: &BookKey) -> bool {
        self.books.contains_key(key)
    }

    /// One market by wire identity, in O(1) — the REST surface's lookup. Reads the *first* key
    /// indexed for that identity and does not fall back to a later one, so a stale index entry shows
    /// up as a missing book rather than being silently papered over.
    pub fn by_identity(
        &self,
        venue: &Arc<str>,
        channel: u32,
        instrument_id: u32,
    ) -> Option<&BookAccumulator> {
        let keys = self
            .by_identity
            .get(&(venue.clone(), channel, instrument_id))?;
        self.books.get(keys.first()?)
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

    fn index(&mut self, key: &BookKey) {
        let keys = self.by_identity.entry(book_identity(key)).or_default();
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }

    /// Distinct wire identities currently indexed, so a test can assert the index holds exactly the
    /// live markets. An entry that outlives its market is both an unbounded leak and — where two
    /// universes collide on one identity — what makes the survivor unreachable.
    #[cfg(test)]
    pub fn identity_index_len(&self) -> usize {
        self.by_identity.values().map(Vec::len).sum()
    }

    /// Drop every market on `(venue, category, channel)` — every `instrument_id` under it —
    /// through [`Self::remove`], so the identity index stays in step exactly as it does on any
    /// other removal path.
    ///
    /// The channel-departure seam: a channel that leaves the ingest floor must not keep replaying a
    /// frozen book (a `Clear` plus stale levels, or a full re-baseline on a new client's connect) to
    /// anyone still asking. Scoped by `category` — unlike `history::Store::forget_channel`, whose
    /// key carries no category (see that method's doc for the resulting over-drop risk) — because
    /// [`BookKey`] already carries it: two universes sharing one Source ID and colliding on
    /// `(channel, instrument_id)` are still distinguished here, so this purge cannot reach into a
    /// live peer universe's market the way the history/catalog purges can.
    pub fn forget_channel(&mut self, venue: &str, category: &str, channel: u32) -> usize {
        let doomed: Vec<BookKey> = self
            .books
            .keys()
            .filter(|k| k.0.as_ref() == venue && k.1.as_ref() == category && k.2 == channel)
            .cloned()
            .collect();
        let mut dropped = 0usize;
        for key in doomed {
            if self.remove(&key).is_some() {
                dropped += 1;
            }
        }
        dropped
    }
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

    fn book(changes: Vec<BookChange>, snapshot: bool, last: bool) -> NormalizedBook {
        NormalizedBook {
            venue: "KALSHI".into(),
            source: "KALSHI".into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
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
                },
                BookChange {
                    action: BookAction::Delete,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 0.0,
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
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 150.0,
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
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.61,
                    size: 10.0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 20.0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 30.0,
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
                },
                BookChange {
                    action: BookAction::Delete,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: 0.0,
                },
            ],
            false,
            true,
        ));

        let out = acc.to_book(&venue, 2, 41);
        assert!(out.snapshot && out.last);
        assert_eq!(out.symbol.as_ref(), "KXBTCPERP");
        assert_eq!(
            out.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0
                },
                // Bids descend: the inside market first.
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.62,
                    size: 25.0
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 0.61,
                    size: 10.0
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
        };

        acc.apply(&book(vec![bid(0.61, 10.0)], false, true));
        assert_eq!(acc.to_book(&venue, 2, 41).changes.len(), 2); // clear + one bid

        // First half of a rebuild: buffered, not applied.
        acc.apply(&book(
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                },
                bid(0.70, 1.0),
            ],
            true,
            false,
        ));
        let mid = acc.to_book(&venue, 2, 41);
        assert_eq!(
            mid.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0
                },
                bid(0.61, 10.0),
            ],
            "the pre-event state, not the half-cleared one"
        );

        // The closing batch commits the whole event at once.
        acc.apply(&book(vec![bid(0.71, 2.0)], true, true));
        assert_eq!(
            acc.to_book(&venue, 2, 41).changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0
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
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: f64::INFINITY,
                    size: 1.0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.63,
                    size: f64::NAN,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.64,
                    size: 5.0,
                },
            ],
            false,
            true,
        ));
        let out = acc.to_book(&venue, 2, 41);
        assert_eq!(
            out.changes,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Ask,
                    price: 0.64,
                    size: 5.0
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
            acc.to_book(&venue, 2, 41).source_ts_ns,
            1_781_019_263_715_344_015
        );

        let mut unknown = book(vec![], false, true);
        unknown.source_ts_ns = 0;
        acc.apply(&unknown);
        assert_eq!(
            acc.to_book(&venue, 2, 41).source_ts_ns,
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
            changes: vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 100.0,
                    size: 5.0,
                },
            ],
            snapshot: true,
            last: true,
            source_ts_ns: 7,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        });

        let out = acc.to_book(&venue, 0, 41);
        assert_eq!(
            out.source_id, 1,
            "the id the producer resolved, not a placeholder"
        );
        assert_eq!(out.source, venue);
    }

    // -----------------------------------------------------------------------------------------
    // BookReplay::forget_channel
    // -----------------------------------------------------------------------------------------

    fn book_key(venue: &str, category: &str, channel: u32, instrument_id: u32) -> BookKey {
        (Arc::from(venue), Arc::from(category), channel, instrument_id)
    }

    /// Dropping a channel drops every market on it, and — because [`BookKey`] carries the
    /// category, unlike `history::Store`'s key — leaves a **peer universe sharing the exact same
    /// `(venue, channel, instrument_id)`** untouched. That collision is real (two categories under
    /// one Source ID can share a `channel_id`/`instrument_id`), so a category-blind filter would
    /// wrongly drop the peer; asserting its survival (not merely the drop) is what proves the scope
    /// is precise.
    #[test]
    fn forgetting_a_channel_spares_a_peer_category_sharing_the_same_identity() {
        let mut replay = BookReplay::default();
        let doomed = book_key("KALSHI", "sports", 10, 41);
        let peer_category = book_key("KALSHI", "perps", 10, 41); // same venue/channel/id, other category
        let peer_channel = book_key("KALSHI", "sports", 11, 41); // same category, other channel
        replay.insert(doomed.clone(), BookAccumulator::new("DOOMED".into()));
        replay.insert(peer_category.clone(), BookAccumulator::new("PEER-CAT".into()));
        replay.insert(peer_channel.clone(), BookAccumulator::new("PEER-CHAN".into()));

        let dropped = replay.forget_channel("KALSHI", "sports", 10);

        assert_eq!(dropped, 1, "exactly the one market on that (venue, category, channel)");
        assert!(!replay.contains_key(&doomed), "the doomed market must be gone");
        assert!(
            replay.contains_key(&peer_category),
            "a peer under a different category sharing the same channel/instrument id must survive"
        );
        assert!(
            replay.contains_key(&peer_channel),
            "a peer under a different channel must survive"
        );
        assert_eq!(
            replay.identity_index_len(),
            2,
            "the identity index must drop the doomed market's entry along with the accumulator"
        );
    }
}
