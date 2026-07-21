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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    let map = INTERN.get_or_init(|| RwLock::new(HashMap::new()));
    // Steady state: the venue is already interned -> shared read lock, clone the cached `Arc`.
    if let Some(arc) = map.read().unwrap_or_else(|e| e.into_inner()).get(venue) {
        return arc.clone();
    }
    // First sighting of this venue: take the write lock and insert (re-checking under the lock in
    // case another task interned it in the race window).
    map.write()
        .unwrap_or_else(|e| e.into_inner())
        .entry(venue)
        .or_insert_with(|| Arc::from(venue))
        .clone()
}

/// A normalized two-sided top-of-book update from any venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedQuote {
    pub venue: Arc<str>,
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
    pub symbol: Arc<str>,
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

/// A normalized instrument definition (so subscribers know precision/venue).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedInstrument {
    pub venue: Arc<str>,
    pub symbol: Arc<str>,
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
            FeedMessage::Status(s) => (s.venue.as_ref(), ""),
        }
    }
}

/// Latest known instrument definitions, keyed by `(venue, symbol)`, shared between the
/// receivers (which update it) and the WebSocket server (which replays it to each new
/// subscriber so reference data arrives before quotes - otherwise a client that connects
/// mid-stream sees a quote first and has to guess the price/qty precision).
///
/// The `(venue, symbol)` key disambiguates the same symbol across *different* venues (e.g.
/// `SOL-PERP` on Hyperliquid vs. Phoenix). It does NOT distinguish by protocol/feed: when one
/// venue is served by multiple feeds (e.g. Hyperliquid TOB + MBO), both write the same entry
/// (last-writer-wins). Those feeds are expected to agree on precision; `upsert_instrument` in
/// `processor.rs` warns if their exponents diverge.
pub type InstrumentSnapshot = Arc<Mutex<HashMap<(Arc<str>, Arc<str>), NormalizedInstrument>>>;

/// Latest order-book `depth` snapshot per `(venue, symbol)`, derived from the Market-by-Order feed
/// and shared with the WebSocket server so it can replay the current book to a newly-connecting
/// subscriber (depth is full state, so one replayed snapshot bootstraps the consumer immediately
/// instead of making it wait for the next periodic one). Updated by the MBO receiver.
pub type DepthSnapshot = Arc<Mutex<HashMap<(Arc<str>, Arc<str>), NormalizedDepth>>>;

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
