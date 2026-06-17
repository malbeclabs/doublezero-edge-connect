//! Normalized feed messages re-served over WebSocket to any trading engine.
//! Wire format is engine-agnostic JSON - see PROTOCOL.md.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

/// A normalized two-sided top-of-book update from any venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedQuote {
    pub venue: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
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
    pub venue: String,
    pub symbol: String,
    pub price: f64,
    pub size: f64,
    /// `"buy"`, `"sell"`, or `"unknown"` - the aggressor (taker) side.
    pub aggressor_side: String,
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
    pub venue: String,
    pub symbol: String,
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
    pub venue: String,
    pub symbol: String,
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
    pub venue: String,
    pub symbol: String,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// A venue-level feed-health status (the PROTOCOL.md `status` candidate extension). Emitted when
/// the bridge's quote (mktdata) multicast for a venue goes silent past the idle watchdog, and
/// again when quotes recover - so consumers can gray out / restore that source. Carries no symbol
/// (it is about the whole venue feed); consumers ignoring unknown `type`s skip it harmlessly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedStatus {
    pub venue: String,
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
            FeedMessage::Instrument(i) => (i.venue.as_str(), i.symbol.as_str()),
            FeedMessage::Quote(q) => (q.venue.as_str(), q.symbol.as_str()),
            FeedMessage::Trade(t) => (t.venue.as_str(), t.symbol.as_str()),
            FeedMessage::Midpoint(m) => (m.venue.as_str(), m.symbol.as_str()),
            FeedMessage::Depth(d) => (d.venue.as_str(), d.symbol.as_str()),
            FeedMessage::Status(s) => (s.venue.as_str(), ""),
        }
    }
}

/// Latest known instrument definitions, keyed by `(venue, symbol)`, shared between the
/// receivers (which update it) and the WebSocket server (which replays it to each new
/// subscriber so reference data arrives before quotes - otherwise a client that connects
/// mid-stream sees a quote first and has to guess the price/qty precision). Keying by venue
/// as well as symbol keeps two feeds that share a symbol (e.g. `SOL-PERP` on different
/// venues) from clobbering each other.
pub type InstrumentSnapshot = Arc<Mutex<HashMap<(String, String), NormalizedInstrument>>>;

/// Latest order-book `depth` snapshot per `(venue, symbol)`, derived from the Market-by-Order feed
/// and shared with the WebSocket server so it can replay the current book to a newly-connecting
/// subscriber (depth is full state, so one replayed snapshot bootstraps the consumer immediately
/// instead of making it wait for the next periodic one). Updated by the MBO receiver.
pub type DepthSnapshot = Arc<Mutex<HashMap<(String, String), NormalizedDepth>>>;

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
/// Provided for standalone collectors / null tests; not referenced by the bridge itself.
#[allow(dead_code)]
pub fn now_mono_ns() -> u64 {
    use nix::time::{clock_gettime, ClockId};
    clock_gettime(ClockId::CLOCK_MONOTONIC)
        .map(|ts| (ts.tv_sec() as u64) * 1_000_000_000 + ts.tv_nsec() as u64)
        .unwrap_or(0)
}
