//! Shared pre-broadcast arbiter: the single emit stage every ingest source funnels through.
//!
//! When several independent sources mirror the same feed — the multicast edge publishers
//! (demultiplexed by source IP so each one's frame-sequence state stays separate, see
//! `receiver`/`processor`) **and** the Hyperliquid public WebSocket feeder ([`crate::ingest::ws_feeder`])
//! — they all converge on one [`Arbiter`] just before the broadcast channel. The arbiter deduplicates
//! the *output* keyed on business identity, so a subscriber sees a clean stream regardless of which
//! source delivered a given update first. Because every source races through the **same** per-`(venue,
//! symbol)` floor, a public-feed copy of an update the edge already emitted collapses into a no-op —
//! and when the edge gaps, the public copy is the first to cross the floor and fills in (the backstop,
//! with no health check; see [`Publisher`]).
//!
//! Two dedup primitives, by message semantics:
//! - quotes ([`StalenessFloor`]): a full-state BBO is a *snapshot*, but two distinct BBOs can share
//!   a `source_ts` (the venue stamps coarsely — block-granular — while the book changes faster), so
//!   one `source_ts` "tick" holds a whole sub-sequence of real top-of-book changes. The catch: the
//!   only trustworthy ordering of those changes is a *single* publisher's own stream. Arrival order
//!   across publishers is corrupted by per-publisher network delay (the `hl-bbo-feed-race` board
//!   shows inter-feed skew over 100 ms), so interleaving two sources inside one tick can serve a
//!   stale sample as the freshest — on a falling price, a slower publisher's older, higher sample
//!   landing last reads as a phantom uptick. So the floor **latches to the leader**: per `(venue,
//!   symbol)` tick it emits only the *leader* (first publisher to open the tick — the lowest-delay
//!   source for it) and drops other publishers' samples at that `source_ts`; the leader is
//!   re-selected each new tick. Output `source_ts` is non-decreasing per key, and within each tick
//!   the emitted series is one publisher's coherent, in-order subsequence.
//! - trades ([`WindowedDedup`]): a trade is a *point-in-time event*, not state, so a floor would lose
//!   prints. It keeps the windowed `trade_id` identity instead: a competing publisher's copy or an
//!   in-window reorder is dropped, but every distinct print is kept. `trade_id == 0` is the "venue
//!   assigned none" sentinel and is forwarded unkeyed — see the `Trade` arm of [`Arbiter::emit`].
//!
//! MBO `depth` reuses the quote's [`StalenessFloor`] as a *third* arm (keyed on [`DepthId`], the
//! full top-N book content): two publishers each reconstruct an independent book and emit full-state
//! snapshots, and the floor collapses the redundant copy exactly as it does redundant BBOs. It
//! diverges from the quote arm in one deliberate way — no `source_ts == 0` bypass; see the `Depth`
//! arm of [`Arbiter::emit`].

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use prometheus::{Histogram, IntCounter};
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    ingest::{
        arm_race::ArmRace,
        authority::{AuthorityConfig, MarketKey, ScopeKey, StickyAuthority, OTHER_ARM},
        feeds::ArbitrationMode,
    },
    metrics::metrics,
    model::{
        self, category_arc, now_mono_ns, now_ns, BookAccumulator, BookAction, BookChange,
        BookSnapshot, DepthSnapshot, FeedMessage, NormalizedBook, NormalizedDepth, NormalizedQuote,
        NormalizedTrade, ReplayScope,
    },
};

/// Default number of recent `trade_id`s remembered per `(venue, symbol)` for cross-source trade
/// dedup. Const for now; promote to config alongside a multi-publisher trade test that can size it.
pub const TRADE_DEDUP_WINDOW: usize = 8192;

/// How long a zero-id tape's owning publisher may go silent before a challenger takes the tape over
/// without being reported as a double-print (see `Arbiter::no_id_owner`). Well past any inter-print
/// gap on a live tape, and well under how long an operator would tolerate a stalled one, so a
/// genuine failover reads as a handover and two concurrent emitters still read as a conflict.
const NO_ID_TAPE_HANDOVER_NS: u64 = 5_000_000_000; // 5s

/// Cap on distinct leader BBOs tracked per `source_ts` tick by the quote floor — a safety bound so a
/// stalled/repeated `source_ts` can't grow the per-tick set without limit. Far above the real
/// per-block max (~hundreds of distinct BBOs share one HL block timestamp), so it never evicts in
/// normal operation.
pub const QUOTE_TICK_CAP: usize = 8192;

/// Cap on distinct leader `depth` snapshots tracked per `source_ts` tick by the depth floor. A book
/// can legitimately emit several full-state snapshots at one venue event timestamp (the `emit_depth`
/// per-frame coalescing splits one `source_ts` across frames; see `MboProcessor::emit_depth`), so the
/// cap sits well above that real per-tick count and never evicts in normal operation.
pub const DEPTH_TICK_CAP: usize = 1024;

/// Reject a quote whose `source_ts` is more than this far ahead of the host wall clock before it can
/// advance the floor. A single bad or hostile public-feed timestamp years in the future would
/// otherwise latch `high_water` ahead and drop every real (now-stamped) quote as stale until restart
/// — wedging the *primary* edge feed for that symbol. The bound caps the worst-case wedge to itself
/// and self-heals; it is generous enough to absorb ordinary clock skew between the venue and host.
const MAX_FUTURE_SKEW_NS: u64 = 5_000_000_000; // 5s

/// Cap on markets whose `book` state the authority gate tracks, evicting oldest-first. The key is
/// wire-supplied `(channel_id, instrument_id)`, so a forged stream must cost evictions rather than
/// memory; the cap sits an order of magnitude above the largest real venue (~1,200 instruments).
const MAX_BOOK_MARKETS: usize = 16_384;

/// Cap on batches withheld from one market while waiting for the new arm to close a logical event.
/// `last` is mandatory in PROTOCOL.md, but a producer that stops setting it — a bug, a truncated
/// frame, a forged source — would otherwise withhold that market from the wire forever. Sized like
/// `model`'s pending-change cap, which desynchronizes the accumulator at the same scale, so the
/// abandoned re-baseline degrades to a bare `clear` rather than to a book claiming completeness.
const MAX_WITHHELD_BATCHES: u32 = 8192;

/// How long a peer's last delivered batch keeps its claim to be serving a market. Past it the peer is
/// treated as not serving, so a recovering arm's re-baseline goes out: a publisher that stops reaching
/// us — host drained, group withdrawn, source forged and then silent — reports nothing on its own
/// behalf, and a claim that never expires would suppress the *only* self-heal this product has for the
/// life of the process. Erring toward publishing is safe (a re-baseline is full state); erring toward
/// suppressing is the wedge.
const PEER_SERVING_NS: u64 = 30_000_000_000; // 30s

/// Cap on order events and tracked orders remembered per order-level market. Two jobs: the count half
/// of the dedup window, and the reach of the resurrection guard (see [`MarketEvents::resting`]).
///
/// Sized against the **product** with [`MAX_BOOK_MARKETS`], not against one market: a per-market cap
/// the size of [`TRADE_DEDUP_WINDOW`] would put the aggregate ceiling in the tens of gigabytes on a
/// wire that mints market keys freely. 1024 events is several seconds of a busy market's order flow at
/// the 250 ms default window, so the time bound is what normally binds; on a market fast enough for the
/// count to bind first, an evicted event costs a redundant emission, not a corrupt book.
const MAX_SEEN_ORDER_EVENTS: usize = 1024;

/// Default order-event dedup window, matching `main.rs`'s `--arb-book-dedup-window-ms` so an arbiter
/// built without the flag races on the same window the shipped binary does.
const DEFAULT_BOOK_DEDUP_WINDOW_NS: u64 = 250_000_000; // 250ms

/// Which ingest source produced an update — the floor's per-tick leader identity. The edge
/// multicast publishers are distinguished by their datagram source IP; the public WebSocket feed is
/// a single logical source with no multicast IP. Two distinct edge publishers therefore race as
/// distinct leaders, while the public feed always races as one [`Publisher::PublicWs`].
///
/// The backstop falls out of this: the edge publishers deliver each `source_ts` tick sub-millisecond
/// while the public copy arrives tens of milliseconds later over the internet, so an edge publisher
/// essentially always opens (leads) a tick and the public copy at that tick is dropped as a
/// non-leader no-op. When the edge feed gaps, no edge publisher opens the next tick, so the public
/// feed's sample is the first to cross the floor — it leads and fills in.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Publisher {
    /// A DZ Edge multicast publisher, identified by its datagram source IP.
    Edge(IpAddr),
    /// The Hyperliquid public WebSocket feeder (a single logical source).
    PublicWs,
}

impl Publisher {
    /// A stable, low-cardinality metric label for the source class. Deliberately collapses the
    /// edge publisher's source IP to `"edge"` — the per-IP identity matters to the floor but would
    /// blow up metric cardinality (and is spoofable), so it is never used as a label.
    pub fn label(self) -> &'static str {
        match self {
            Publisher::Edge(_) => "edge",
            Publisher::PublicWs => "public",
        }
    }
}

/// Canonical BBO scale exponent (`10^-8`), matching the capture service's `bbo_hash`
/// (`malbeclabs/hyperliquid` `StableBBOHash`: `canonicalBBOPriceExp = canonicalBBOQtyExp = -8`).
const CANONICAL_BBO_EXP: i32 = 8;

/// The canonical business identity of a quote at a `source_ts` tick — the components of the spec's
/// `bbo_hash`: bid/ask price + size at the canonical `10^-8` fixed-point scale, plus the source
/// counts `bid_n`/`ask_n`. EXCLUDES `source_ts` (the floor tracks that separately).
///
/// Why canonical `-8` integers and not the raw `f64` bits: sources publish the same economic price
/// in different encodings — the edge feed as `raw * 10^exp`, the public WS as a JSON float — and
/// `raw as f64 * 10^exp` is **not** bit-identical to the parsed float for the same value (`0.1` is
/// inexact in binary). Bit-comparing `f64`s would treat the two as distinct, so the *same* BBO from
/// two sources would not share an identity — silently defeating cross-source dedup. Rounding each
/// value to a fixed `10^-8` integer collapses both encodings to the same canonical key, matching
/// `StableBBOHash`. A change in `bid_n`/`ask_n` (orders/sources at the top) is a distinct BBO.
///
/// The fixed-point integers are `i128`, not `i64`: a float→int cast **saturates**, so with `i64`
/// any value above ~9.2e10 (at the `10^-8` scale) would clamp to `i64::MAX` and two genuinely
/// distinct huge values would collapse to one identity — wrongly deduped. `i128` pushes the
/// saturation bound past ~1.7e30, beyond any representable price/quantity of interest, and agrees
/// with the old `i64` canonicalization for every in-range value, so dedup semantics are otherwise
/// unchanged. (Above ~9e7 the effective grid is the `f64` ULP rather than a true `10^-8` step —
/// inherent to the `f64` inputs; distinct `f64` values still map to distinct integers.)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct QuoteId {
    bid_px: i128,
    bid_sz: i128,
    ask_px: i128,
    ask_sz: i128,
    bid_n: u16,
    ask_n: u16,
}

impl QuoteId {
    /// The canonical content identity of a normalized quote: each BBO `f64` rounded to a `10^-8`
    /// fixed-point integer (so two sources' encodings of the same price collapse), plus the counts.
    pub fn of(q: &NormalizedQuote) -> Self {
        let canon = |x: f64| (x * 10f64.powi(CANONICAL_BBO_EXP)).round() as i128;
        Self {
            bid_px: canon(q.bid),
            bid_sz: canon(q.bid_size),
            ask_px: canon(q.ask),
            ask_sz: canon(q.ask_size),
            bid_n: q.bid_n,
            ask_n: q.ask_n,
        }
    }
}

/// The canonical business identity of a `depth` snapshot at a `source_ts` tick — the full top-N book
/// content, EXCLUDING `source_ts` (the floor tracks that separately). Mirrors [`QuoteId`]: each
/// price/qty `f64` is rounded to a `10^-8` fixed-point integer so two publishers' encodings of the
/// same level collapse to one identity (the same reason `QuoteId` canonicalizes — `raw * 10^exp` is
/// not bit-identical to a parsed float). Two publishers that independently reconstruct the *same*
/// book state at one `source_ts` share this identity; genuinely divergent states differ.
///
/// This matches the `no_business_duplicates` depth oracle, which keys on `venue + symbol +
/// source_ts_ns + bids + asks` — content-inclusive by design. The levels are `i128` for the same
/// saturation guard as [`QuoteId`] — an `i64` cast clamps any qty above ~9.2e10 to one value.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DepthId {
    bids: Vec<(i128, i128)>,
    asks: Vec<(i128, i128)>,
}

/// The content identity of an instrument definition: the precision pair. EXCLUDES `venue`/`symbol`
/// (they are the map key). N publishers mirroring a feed each republish identical definitions on
/// every reference-data burst (~every 8s on the live feed), so un-deduped every WS client receives
/// one copy per publisher of content it already has.
///
/// Unlike quotes and depth this needs no `source_ts` floor: a definition carries no timestamp and
/// is idempotent full state, so content plus a re-announce clock is the whole identity. A genuine
/// precision change differs and re-emits immediately.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct InstrumentId {
    price_exponent: i8,
    qty_exponent: i8,
}

/// How long unchanged instrument content is suppressed before it is re-broadcast.
///
/// Collapsing the mirrored publishers' bursts must NOT make a definition a once-per-process event.
/// A streamed `instrument` can be lost — the WS broadcast is drop-oldest under backpressure
/// (`sinks::ws`) — and the periodic refdata burst is the only thing that heals an established
/// client, since the `InstrumentSnapshot` replay happens on connect only. So the dedup is a rate
/// limit, not a latch: the first copy of a burst wins, its five mirrors collapse, and the content
/// is re-announced on the first burst after this interval. Above the live ~8s burst period so a
/// burst always collapses to one message, but close enough to it that the worst-case repair delay
/// stays the same order as the un-deduped feed's (~16s vs ~8s) — that is the trade this const
/// prices, and lowering the mirror traffic further would buy a slower heal.
const INSTRUMENT_REANNOUNCE_NS: u64 = 15_000_000_000; // 15s

impl DepthId {
    pub fn of(d: &NormalizedDepth) -> Self {
        let canon = |levels: &[[f64; 2]]| -> Vec<(i128, i128)> {
            levels
                .iter()
                .map(|l| {
                    (
                        (l[0] * 10f64.powi(CANONICAL_BBO_EXP)).round() as i128,
                        (l[1] * 10f64.powi(CANONICAL_BBO_EXP)).round() as i128,
                    )
                })
                .collect()
        };
        Self {
            bids: canon(&d.bids),
            asks: canon(&d.asks),
        }
    }
}

/// The outcome of a de-dup admission decision, shared by [`StalenessFloor`] and [`WindowedDedup`].
/// The caller forwards on [`Admit::Emitted`] and drops otherwise; [`Admit::Contest`] additionally
/// reports a *cross-source* head-to-head: another publisher already won this identity, and this is
/// the first losing copy from a different publisher, arriving `lead_ns` after the `winner` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit<P> {
    /// Forwarded: opened a new tick / new content (floor), or a first-seen identity (window).
    /// `opened_tick` is true iff this sample *opened* its identity — the floor's first delivery at
    /// a new `source_ts` tick, as opposed to the leader's later distinct content within the
    /// already-open tick. The window has no tick concept: every first-seen identity is its own
    /// open. One `true` per tick is the once-per-tick win the `dz_*_ticks_won_total` counters
    /// publish (see [`crate::metrics::Metrics::quote_ticks_won`]).
    Emitted { opened_tick: bool },
    /// Dropped with no cross-source contest to report: stale tick, exact repeat, a same-publisher
    /// duplicate, or a subsequent follower at a tick whose contest was already counted.
    Dropped,
    /// Dropped as the first losing copy of a cross-source contest: `winner` delivered this identity
    /// `lead_ns` nanoseconds earlier. Recorded once per identity so the count is one-per-contest.
    Contest { winner: P, lead_ns: u64 },
}

impl<P> Admit<P> {
    /// Whether the message should be forwarded (true only for [`Admit::Emitted`]).
    pub fn emitted(&self) -> bool {
        matches!(self, Admit::Emitted { .. })
    }
}

/// Per-key tick state for the [`StalenessFloor`]: the leader latched at the current `source_ts`
/// tick, when that leader's opening copy arrived (for the cross-source lead-time measure), whether
/// a losing follower has already been counted at this tick, and the leader's distinct content set
/// (FIFO-bounded to `tick_cap`).
struct TickState<V, P> {
    high_water: u64,
    leader: P,
    /// Arrival wall-clock (ns) of the copy that opened this tick — the baseline a later follower's
    /// arrival is compared against to compute the lead.
    leader_arrival_ns: u64,
    /// Set once the first cross-publisher follower at this tick is counted, so additional followers
    /// of the same tick don't inflate the contest count.
    follower_recorded: bool,
    content: HashSet<V>,
    order: VecDeque<V>,
}

/// Per-key **latch-to-leader** floor on `source_ts`. Tracks, per key, the highest `source_ts`
/// emitted, the *leader* publisher latched for that tick, and the set of distinct content the leader
/// has emitted at it. The `source_ts` never goes backwards, and within one tick only the leader is
/// emitted:
/// - `source_ts < high_water` → false (stale: a later tick already passed; any publisher).
/// - `source_ts > high_water` → advance the floor, latch the leader to this publisher, reset the
///   tick's content set, true.
/// - `source_ts == high_water` → false if `publisher != leader` (a non-leader sample at this tick:
///   its arrival order relative to the leader is delay-corrupted and untrustworthy, so it is
///   dropped); otherwise true iff the content is new at the tick (the leader's own exact
///   `(source_ts, content)` repeat is dropped).
///
/// The first sample for a key behaves as the `>` case (it opens the tick and becomes leader). Output
/// `source_ts` is non-decreasing per key; within a tick the emitted series is the leader's coherent,
/// in-order subsequence.
///
/// Memory is O(min(distinct leader content at the current tick, `tick_cap`)) per key. The per-tick
/// content set is FIFO-bounded to `tick_cap` so a stalled or pathologically-repeated `source_ts`
/// (a feed that stops advancing its clock while still publishing) can't grow it without limit; the
/// cap is set far above the real per-block max, so it never evicts in normal operation. (`source_ts
/// == 0`, the "not available" sentinel, is handled by the caller — it must not reach the floor as a
/// real tick, or it would pin `high_water` and drop every non-leader forever.)
pub struct StalenessFloor<K, V, P> {
    /// Per key: the latched tick state (high-water `source_ts`, leader + its arrival, content set).
    state: HashMap<K, TickState<V, P>>,
    /// Cap on distinct leader contents tracked at one tick (safety bound; see type docs).
    tick_cap: usize,
}

impl<K: Eq + Hash, V: Eq + Hash + Clone, P: Eq + Copy> StalenessFloor<K, V, P> {
    pub fn new(tick_cap: usize) -> Self {
        Self {
            state: HashMap::new(),
            tick_cap,
        }
    }

    /// The latch-to-leader decision for this `(source_ts, content)` from `publisher`, arriving at
    /// `arrival_ns` (wall clock). [`Admit::Emitted`] forwards (new tick / new leader content / first
    /// sample); a non-leader sample at the current tick returns [`Admit::Contest`] *the first time*
    /// (reporting how far `leader` led) and [`Admit::Dropped`] thereafter; stale ticks and exact
    /// repeats return [`Admit::Dropped`]. The per-tick content set is FIFO-bounded to `tick_cap`.
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the broadcast
    /// channel, the only delivery step. A no-subscriber send desyncs no one, and a unique quote
    /// dropped by a slow per-client channel is unrecoverable regardless — so there is no failed-send
    /// path on which the floor must avoid advancing.
    pub fn admit(
        &mut self,
        key: K,
        source_ts: u64,
        content: V,
        publisher: P,
        arrival_ns: u64,
    ) -> Admit<P> {
        use std::collections::hash_map::Entry;
        match self.state.entry(key) {
            Entry::Vacant(v) => {
                let mut set = HashSet::new();
                set.insert(content.clone());
                v.insert(TickState {
                    high_water: source_ts,
                    leader: publisher,
                    leader_arrival_ns: arrival_ns,
                    follower_recorded: false,
                    content: set,
                    order: VecDeque::from([content]),
                });
                Admit::Emitted { opened_tick: true }
            }
            Entry::Occupied(mut o) => {
                let st = o.get_mut();
                if source_ts < st.high_water {
                    Admit::Dropped
                } else if source_ts > st.high_water {
                    st.high_water = source_ts;
                    st.leader = publisher;
                    st.leader_arrival_ns = arrival_ns;
                    st.follower_recorded = false;
                    st.content.clear();
                    st.order.clear();
                    st.content.insert(content.clone());
                    st.order.push_back(content);
                    Admit::Emitted { opened_tick: true }
                } else if publisher != st.leader {
                    // A non-leader sample at this tick: its arrival order vs the leader is
                    // delay-corrupted so it is dropped, but the *first* one is a cross-source
                    // contest the leader won — report the lead once (later followers just drop).
                    if st.follower_recorded {
                        Admit::Dropped
                    } else {
                        st.follower_recorded = true;
                        Admit::Contest {
                            winner: st.leader,
                            lead_ns: arrival_ns.saturating_sub(st.leader_arrival_ns),
                        }
                    }
                } else if st.content.insert(content.clone()) {
                    st.order.push_back(content);
                    if st.order.len() > self.tick_cap {
                        if let Some(old) = st.order.pop_front() {
                            st.content.remove(&old);
                        }
                    }
                    Admit::Emitted { opened_tick: false }
                } else {
                    Admit::Dropped
                }
            }
        }
    }

    /// Drop the latched tick state for every key matching `pred`, returning how many entries were
    /// cleared. The next sample for a cleared key behaves as first-seen: it re-opens the tick and
    /// latches a fresh leader, so a legitimately *lower* `source_ts` (a venue that restarted its
    /// event clock at a session boundary) is admitted instead of being dropped as stale forever.
    pub fn reset_where(&mut self, mut pred: impl FnMut(&K) -> bool) -> usize {
        let before = self.state.len();
        self.state.retain(|k, _| !pred(k));
        before - self.state.len()
    }
}

/// One order-level `book` event's venue identity: which order, what happened to it, and the state it
/// ended in. Every publisher of a distributed venue reports these identically for one venue event, so
/// the first arrival is published and the rest collapse. Never the producer's `per_instrument_seq` —
/// that is per publisher and unrelated across arms (measured: three unrelated bases for one execution).
///
/// `size_bits` is part of the *identity*, not merely content compared after the fact: successive
/// partial fills of one order share the id, the action and the resting price and differ only here, so
/// omitting it would collapse the second fill as a duplicate of the first and leave every consumer's
/// book holding a quantity the venue has already reduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OrderEvent {
    order_id: u64,
    action: BookAction,
    price_bits: u64,
    size_bits: u64,
}

/// Why a market must be re-baselined before it can be served again.
const FORCED_DISAGREEMENT: &str = "disagreement";
const FORCED_GUARD_EVICTED: &str = "guard_evicted";

/// One order-level market's cross-publisher race state: the events recently delivered to the wire, and
/// the smallest resting quantity any publisher has claimed for each live order.
///
/// Bounded twice — by [`MAX_SEEN_ORDER_EVENTS`] and by the caller's time window — because the wire is
/// unauthenticated. The time bound is a cost knob; the count bound is not, since it also bounds the
/// resurrection guard, so [`Self::forced`] carries what it costs when it binds.
#[derive(Default)]
struct MarketEvents {
    /// Delivered events mapped to the publisher that delivered each first and when it arrived.
    seen: HashMap<OrderEvent, (Publisher, u64)>,
    /// `seen` in delivery order, oldest first, for both bounds.
    order: VecDeque<OrderEvent>,
    /// Per order this market has published anything for: the smallest resting quantity any publisher
    /// has reported and who reported it, or `None` once the order is gone — a **tombstone**.
    ///
    /// This is the cross-publisher resurrection guard, and this is the only place that can hold it: a
    /// venue never reuses an `order_id`, but `ingest::book`'s per-publisher set cannot see a *peer's*
    /// delete, so a lagging publisher's first and only copy of an `Add` for an order another publisher
    /// already killed passes every check that book makes. Deliberately **not** expired by the time
    /// window — only by [`MAX_SEEN_ORDER_EVENTS`] — so the guard's reach is an order count rather than a
    /// clock, which is what keeps the window a cost knob. When that count binds on an entry a peer could
    /// still be racing, the market re-baselines (see [`Self::track`]) rather than forgetting silently.
    resting: HashMap<u64, Option<(f64, Publisher)>>,
    /// `resting`'s keys in insertion order, oldest first, each with the time it was **delivered** —
    /// `None` for one seeded from a re-baseline. That is what tells an eviction that costs the guard
    /// nothing from one that costs it everything.
    resting_order: VecDeque<(u64, Option<u64>)>,
    /// One-shot: why this market can no longer be served from either arm's deltas, drained by the
    /// caller. Set when the guard cannot answer — the arms disagreed, or a tracked order aged out of
    /// `resting` — because from then on neither publisher's stream is known to describe the book a
    /// consumer holds.
    forced: Option<&'static str>,
}

/// Whether one order event reached the wire, and what its arrival said about the arms.
enum EventVerdict {
    /// First delivery of this venue event: publish it.
    Deliver,
    /// A peer delivered it already: collapse this copy.
    Duplicate,
    /// An order this market has already published as gone. A venue never reuses an id, so this is a
    /// lagging publisher's stale copy — drop it, or every consumer resurrects a dead order.
    Resurrection,
    /// This publisher claims more is resting for the order than a peer already reported, so one of the
    /// two books has drifted. Neither copy is published: the larger rewinds the consumer past a fill
    /// the venue already applied, the smaller lets a forged size mute a real order.
    Disagreement,
}

/// How an order came to be tracked, which is the whole of whether losing it costs the guard anything.
#[derive(Clone, Copy)]
enum Tracked {
    /// Delivered to the wire, with a peer's copy still plausibly in flight for `window_ns`.
    Delivered { at: u64, window_ns: u64 },
    /// Seeded from a re-baseline's own book. Nothing is racing it — the consumer already holds this
    /// state — so evicting it is free. Stamping seeds as deliveries instead would make every book
    /// larger than the cap re-arm, through its own seeding, the re-baseline it just discharged.
    Seeded,
}

impl Tracked {
    fn delivered_at(self) -> Option<u64> {
        match self {
            Tracked::Delivered { at, .. } => Some(at),
            Tracked::Seeded => None,
        }
    }

    /// Whether evicting an entry delivered at `other` loses a guard this delivery still needs.
    fn races_with(self, other: Option<u64>) -> bool {
        let (Tracked::Delivered { at, window_ns }, Some(o)) = (self, other) else {
            return false;
        };
        at.saturating_sub(o) <= window_ns
    }
}

impl MarketEvents {
    /// The race decision for one order-level change from `publisher`, arriving at `arrival_ns`, with
    /// events older than `window_ns` forgotten first.
    fn admit(
        &mut self,
        ev: OrderEvent,
        publisher: Publisher,
        arrival_ns: u64,
        window_ns: u64,
    ) -> EventVerdict {
        self.expire(arrival_ns, window_ns);
        if self.seen.contains_key(&ev) {
            return EventVerdict::Duplicate;
        }
        self.seen.insert(ev, (publisher, arrival_ns));
        self.order.push_back(ev);
        while self.order.len() > MAX_SEEN_ORDER_EVENTS {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        let size = f64::from_bits(ev.size_bits);
        match self.resting.get_mut(&ev.order_id) {
            // Tombstoned. A non-zero size would resurrect it; a repeat of the removal is a no-op the
            // consumer can absorb, so it goes out rather than being silently withheld.
            Some(None) => {
                if size == 0.0 {
                    EventVerdict::Deliver
                } else {
                    EventVerdict::Resurrection
                }
            }
            Some(slot) if size == 0.0 => {
                *slot = None;
                EventVerdict::Deliver
            }
            Some(Some((seen_size, seen_by))) => {
                if size > *seen_size && *seen_by != publisher {
                    // A resting order only shrinks, so one of the two books has missed a fill. Which
                    // one is unknowable here, so the market re-baselines instead of picking.
                    self.force(FORCED_DISAGREEMENT);
                    return EventVerdict::Disagreement;
                }
                if size < *seen_size {
                    *seen_size = size;
                    *seen_by = publisher;
                }
                EventVerdict::Deliver
            }
            None => {
                let slot = (size != 0.0).then_some((size, publisher));
                let how = Tracked::Delivered {
                    at: arrival_ns,
                    window_ns,
                };
                self.track(ev.order_id, slot, how);
                EventVerdict::Deliver
            }
        }
    }

    /// Track one order's resting state, evicting the oldest to stay within [`MAX_SEEN_ORDER_EVENTS`].
    ///
    /// An eviction only *loses* the guard while a peer's copy of that order could still be racing, which
    /// is the horizon `window_ns` bounds everywhere else: past it every copy that was coming has already
    /// arrived, so dropping the entry costs nothing and a book far larger than the cap streams normally.
    /// Inside it the guard has been asked to forget something it is still needed for, and the market
    /// re-baselines rather than reopening the resurrection path.
    fn track(&mut self, order_id: u64, slot: Option<(f64, Publisher)>, how: Tracked) {
        if self.resting.insert(order_id, slot).is_none() {
            self.resting_order.push_back((order_id, how.delivered_at()));
        }
        while self.resting_order.len() > MAX_SEEN_ORDER_EVENTS {
            let Some((old, delivered)) = self.resting_order.pop_front() else {
                break;
            };
            self.resting.remove(&old);
            if how.races_with(delivered) {
                self.force(FORCED_GUARD_EVICTED);
            }
        }
    }

    /// Record why this market must re-baseline. First cause wins: the flag is one-shot and a second
    /// cause before it is drained describes the same unserveable market.
    fn force(&mut self, reason: &'static str) {
        self.forced.get_or_insert(reason);
    }

    /// Re-point the race state at a re-baseline's book.
    ///
    /// The consumer's book is replaced wholesale, so nothing delivered before it can be a duplicate of
    /// anything that follows and the dedup window goes. The **resurrection guard does not**: a venue
    /// still never reuses an order id, so a peer's stale `Add` for an order this snapshot does not
    /// contain must still be refused. `changes` seeds the floor with the snapshot's own orders, so a
    /// later peer claiming more is resting for one of them is still caught as drift.
    fn rebaselined(&mut self, changes: &[BookChange], publisher: Publisher) {
        self.seen.clear();
        self.order.clear();
        self.forced = None;
        for c in changes.iter().filter(|c| c.order_id != 0 && c.size != 0.0) {
            self.track(c.order_id, Some((c.size, publisher)), Tracked::Seeded);
        }
    }

    /// Forget events delivered more than `window_ns` before `now`. `order` is in delivery order, so the
    /// scan stops at the first entry still inside the window.
    fn expire(&mut self, now: u64, window_ns: u64) {
        while let Some(oldest) = self.order.front() {
            let Some(&(_, arrival)) = self.seen.get(oldest) else {
                self.order.pop_front();
                continue;
            };
            if now.saturating_sub(arrival) <= window_ns {
                return;
            }
            let old = self.order.pop_front();
            if let Some(old) = old {
                self.seen.remove(&old);
            }
        }
    }
}

/// Per-key bounded dedup of recently-seen identities. Keeps the most recent `capacity` values per
/// key, so a duplicate from a second publisher (or a reorder within the window) is dropped while
/// memory stays bounded.
///
/// Window correctness depends on the `no_business_duplicates` oracle's assumption that each identity
/// is unique per `(venue, symbol)`: the window must exceed the worst-case number of distinct values
/// between competing publishers' copies of the same value, or a late duplicate re-emits.
/// Per-key window contents: each tracked value mapped to the `(publisher, arrival_ns)` of the copy
/// that first delivered it, plus a FIFO of values for bounded (capacity) eviction.
type DedupSeen<V, P> = (HashMap<V, (P, u64)>, VecDeque<V>);

pub struct WindowedDedup<K, V, P> {
    capacity: usize,
    /// Per key: the most recent `capacity` values and their first-deliverer attribution.
    seen: HashMap<K, DedupSeen<V, P>>,
}

impl<K: Eq + Hash + Clone, V: Eq + Hash + Copy, P: Eq + Copy> WindowedDedup<K, V, P> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            seen: HashMap::new(),
        }
    }

    /// [`Admit::Emitted`] if `value` is first-seen for `key` (recording `publisher`/`arrival_ns`);
    /// otherwise a duplicate still in the window — [`Admit::Contest`] (reporting the lead) when it
    /// comes from a *different* publisher than the one that first delivered it, else
    /// [`Admit::Dropped`] (a same-publisher repeat).
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the
    /// broadcast channel, the only delivery step. A no-subscriber send desyncs no one, and a unique
    /// update dropped by a slow per-client channel is unrecoverable regardless — so there is no
    /// failed-send path on which the cache must avoid advancing.
    pub fn admit(&mut self, key: K, value: V, publisher: P, arrival_ns: u64) -> Admit<P> {
        let (seen, order) = self.seen.entry(key).or_default();
        if let Some(&(winner, winner_arrival_ns)) = seen.get(&value) {
            return if winner == publisher {
                Admit::Dropped
            } else {
                Admit::Contest {
                    winner,
                    lead_ns: arrival_ns.saturating_sub(winner_arrival_ns),
                }
            };
        }
        seen.insert(value, (publisher, arrival_ns));
        order.push_back(value);
        if order.len() > self.capacity {
            if let Some(old) = order.pop_front() {
                seen.remove(&old);
            }
        }
        Admit::Emitted { opened_tick: true }
    }
}

/// The shared emit stage: owns the broadcast `Sender` plus the dedup state, and exposes one
/// `emit(msg, publisher)` entry point every ingest source funnels through. Quotes pass through the
/// per-`(venue, symbol)` latch-to-leader [`StalenessFloor`] (keyed on [`QuoteId`], `P = Publisher`),
/// MBO `depth` through its own latch-to-leader floor (keyed on [`DepthId`] — but with no
/// `source_ts == 0` bypass, see the `Depth` arm), trades through the [`WindowedDedup`] on `trade_id`,
/// and everything else (`Instrument`/`Midpoint`/`Status`) is broadcast unchanged. Wrapped in
/// [`SharedArbiter`] so the multicast receiver tasks and the WS feeder share one instance — hence one
/// floor per `(venue, symbol)`, on which all sources race.
pub struct Arbiter {
    /// The backbone carries `Arc<FeedMessage>` so a per-subscriber delivery is a refcount bump, not
    /// a deep clone of the message's `String`/`Vec`s.
    tx: broadcast::Sender<Arc<FeedMessage>>,
    /// Cross-source dedup for quotes. Deliberately EXEMPT from the session-reset escape hatch the
    /// depth floor gets (see `depths` below): the TOB `source_ts` is epoch block time, monotonic
    /// across sessions by construction, so a session boundary cannot restart it below the latched
    /// high-water — and 0, the "not available" sentinel, bypasses this floor entirely. Revisit if
    /// a venue with a session-scoped quote clock is ever added. The key `(venue, symbol)` is
    /// `Arc<str>` (venues interned via `model::venue_arc`), so building it allocates nothing.
    quotes: StalenessFloor<(Arc<str>, Arc<str>), QuoteId, Publisher>,
    trades: WindowedDedup<(Arc<str>, Arc<str>), u64, Publisher>,
    /// Cross-publisher dedup for MBO `depth`. Each publisher reconstructs its own book (per
    /// `(publisher, instrument)` in [`crate::ingest::processor::MboProcessor`]) and emits full-state
    /// snapshots; this floor collapses the redundant publishers' depth the same way the quote floor
    /// collapses redundant BBOs — latch-to-leader per `(venue, symbol)` tick, keyed on [`DepthId`].
    ///
    /// The floor assumes `source_ts_ns` is monotonic non-decreasing **within a session**; it does
    /// NOT assume monotonicity across session boundaries. If a venue restarts its event clock below
    /// the latched high-water, every later depth would be dropped as stale with no full-state
    /// self-heal (the floor stays latched) — so the MBO processor clears the affected entries on
    /// `EndOfSession` / `InstrumentReset` via [`Arbiter::reset_depth_floor_for_venue`] /
    /// [`Arbiter::reset_depth_floor_for_symbol`], the session-reset escape hatch.
    depths: StalenessFloor<(Arc<str>, Arc<str>), DepthId, Publisher>,
    /// Shared latest-`depth` map the WS server replays on connect, keyed `(venue, symbol)`. Written
    /// here — on the floor's **admit** decision — so the replayed snapshot is always the *leader's*
    /// broadcast book, never a non-leader publisher's (possibly divergent) copy that never crossed
    /// the floor. `None` when no WS replay map is wired (e.g. unit tests that only inspect the
    /// broadcast). The `MboProcessor` still purges this map on book eviction (bounding); it no longer
    /// writes it (single writer = this admit branch). Also purged by `reset_depth_floor_for_*`
    /// (the session-reset escape hatch), so a client connecting across a session boundary is not
    /// replayed the ended session's final book — see those methods' docs.
    depth_replay: Option<DepthSnapshot>,
    /// Last broadcast content per `(venue, channel, instrument_id)` plus the monotonic time it went
    /// out, so mirrored publishers' identical definition bursts collapse to one wire message while
    /// the content is still re-announced every [`INSTRUMENT_REANNOUNCE_NS`]. Keyed on the identity
    /// triple rather than `(venue, symbol)`: the market-by-price wire truncates symbols to 16 bytes
    /// and two instrument ids already collide on one truncation in a live capture, so a
    /// symbol-keyed rate limit would starve a `{"channel":N}` subscriber of a definition another
    /// channel announced first. Bounded by the distinct instrument count, like the
    /// `InstrumentSnapshot` `processor::upsert_instrument` maintains.
    instrument_defs: HashMap<(Arc<str>, u32, u32), (InstrumentId, u64)>,
    /// Per-venue pre-resolved metric children, so `emit` increments a cached handle instead of doing
    /// a `with_label_values` label-map lookup per message (mirrors the `SeqEvents` pattern in the
    /// receiver). Populated lazily on the first message for each venue; venues are a tiny fixed set.
    venue_metrics: HashMap<Arc<str>, VenueMetrics>,
    /// Per-venue arbitration mode, populated at startup from every `FEEDS` row — not from the
    /// *selected* rows: `emit`'s venue comes from the wire SourceID, so a venue can reach the
    /// arbiter without its feed being ingested (a remapped SourceID, or a public feeder, neither of
    /// which is `--feed`-gated). A venue absent from the map arbitrates as `Coordinated`. Keyed on
    /// the registry's `&'static str` rather than the `Arc<str>` the dedup keys use; lookups borrow
    /// as `&str`, so this needs no `venue_arc` interning.
    modes: HashMap<&'static str, ArbitrationMode>,
    /// Who owns each `(venue, symbol)` zero-id tape, and when they last printed. A bypassed
    /// `trade_id == 0` has no window to collapse against, so a *concurrent* second publisher's
    /// zero-id prints are pure duplicates; they are still forwarded (dropping is an authority
    /// decision, not a dedup one) but counted and logged.
    ///
    /// Ownership is not a permanent latch: a challenger takes it over once the incumbent has been
    /// silent for [`NO_ID_TAPE_HANDOVER_NS`]. Tape ownership moves at runtime — a failed arm, an
    /// authority transfer, a reconciler handing the tape from one feed row to another — and latching
    /// the first publisher forever would report every legitimate failover as a double-print for the
    /// life of the process, which is exactly the alert nobody would then trust.
    ///
    /// Bounded like `instrument_defs`: one entry per `(venue, symbol)` that ever carries a zero-id
    /// print, which no live feed does today.
    no_id_owner: HashMap<(Arc<str>, Arc<str>), (Publisher, u64)>,
    /// Which **arm** serves each `Sticky` universe's tape, keyed on `(venue, category)`. See
    /// [`Arbiter::tape_arm_admits`]. One entry per `Sticky` universe that has ever printed.
    ///
    /// Venue alone would let a publisher on one universe mute a publisher on a disjoint one: a
    /// single Source ID can carry universes that mirror nothing, and the gate below drops every
    /// print from an arm that is not the leader. That drop has no bound in practice — the silence
    /// handover only fires once the incumbent stops, and an incumbent streaming its own universe
    /// never does — so the loser's tape goes dark for the life of the process.
    tape_leader: HashMap<(Arc<str>, Arc<str>), TapeLead>,
    /// Whether the zero-id double-print warning has fired; the metric carries the ongoing rate.
    no_id_conflict_logged: bool,
    /// Whether the "batches carry no `last`" warning has fired.
    book_withhold_logged: bool,
    /// Single-arm authority for the incremental `book` product, in **both** arbitration modes. A
    /// `source_ts` tick can hold several deltas, so the per-tick latch the quote floor uses would
    /// interleave two arms inside one logical event; there is no mode in which that is acceptable.
    books: StickyAuthority,
    /// The cross-arm trade matcher: the only producer of the matched leads
    /// [`StickyAuthority::observe_matched_lead`] elects on.
    race: ArmRace,
    /// Per-market `book` state, bounded by [`MAX_BOOK_MARKETS`] with `book_order` as the eviction
    /// queue (oldest first, mirroring `processor::PerPublisher`).
    book_markets: HashMap<MarketKey, BookMarket>,
    book_order: VecDeque<MarketKey>,
    /// Shared accumulated-`book` map the WS server replays on connect, keyed `(venue, channel,
    /// instrument_id)`. Mirrors the serving arm's state: seeded from its accumulator on every
    /// re-baseline, then advanced by each admitted batch. `None` when no replay map is wired.
    book_replay: Option<BookSnapshot>,
    /// Per order-level market, each publisher's standing (see [`PeerState`]). Bounded twice: evicted
    /// alongside `book_markets`, and the inner map only admits arms the authority already counts, so a
    /// spoofed-source flood cannot grow it.
    book_sync: HashMap<MarketKey, HashMap<Publisher, PeerState>>,
    /// Per order-level market, the raced order events (see [`MarketEvents`]). Same bound and eviction.
    book_events: HashMap<MarketKey, MarketEvents>,
    /// How long a delivered order event is remembered (`--arb-book-dedup-window-ms`).
    book_dedup_window_ns: u64,
}

/// Who serves one `Sticky` venue's trade tape, per [`Arbiter::tape_arm_admits`].
struct TapeLead {
    arm: Publisher,
    /// That arm's last admitted print, for the silence handover.
    last_ns: u64,
    /// The book election this tape has already deferred to, so the deferral fires once per election
    /// rather than on every print by the elected arm.
    honored_election: Option<Publisher>,
}

/// One publisher's standing for an order-level market: whether its book is in sync, and when it last
/// delivered a batch. Both are needed — the flag alone cannot tell a healthy peer from a departed one.
#[derive(Default, Clone, Copy)]
struct PeerState {
    synced: bool,
    /// Monotonic time of this publisher's last *published* batch for the market; `0` until it publishes
    /// one, which is what keeps a source that only ever sent reference data from claiming to serve.
    last_batch_ns: u64,
}

/// Per-market state behind the `book` authority gate.
#[derive(Default)]
struct BookMarket {
    /// Set once a batch for this market has carried a non-zero `order_id`. **Derived from content, not
    /// from a report**: it survives nothing (eviction drops it) and is re-established by the next
    /// order-level batch, and a forged sync report cannot use it to divert a price-aggregated market off
    /// the single-arm gate.
    order_level: bool,
    /// **Every** eligible arm's accumulated book, not just the serving one. A transfer re-baselines
    /// the consumer against the new arm's *current* levels, which exist only if its stream was folded
    /// in all along. Bounded by the caller's eligibility check (`StickyAuthority::tracks_arm`).
    arms: HashMap<Publisher, BookAccumulator>,
    /// Set when the serving arm changed. The re-baseline then waits for that arm to close a logical
    /// event, because a `to_book` of a half-applied one goes out stamped `last` as a torn book.
    rebaseline: bool,
    /// Batches withheld waiting for that event boundary. `last` is mandatory on the wire but the wire
    /// is unauthenticated, so the wait is bounded — see [`MAX_WITHHELD_BATCHES`].
    withheld: u32,
    /// When this market last discharged a *forced* re-baseline, which rate-limits the next one. One
    /// datagram can raise the flag and each discharge costs a whole book, so without it the counter is
    /// a lever rather than an observable.
    rebaselined_ns: u64,
}

/// The `dz_arm_authority_transfers_total` reason for an admitted `book` batch, or `None` when nothing
/// moved. `margin` is counted where it happens (the sampler tick), and the venue leader taking a
/// market back from a health override follows either that or a recovery, so it is not attributable
/// here and is deliberately left uncounted rather than mislabelled.
fn transfer_reason(
    prev: Option<Publisher>,
    leader_before: Option<Publisher>,
    leader_after: Option<Publisher>,
    publisher: Publisher,
) -> Option<&'static str> {
    if leader_before.is_none() {
        return Some("initial"); // the venue's first eligible arm
    }
    if leader_before != leader_after {
        return Some("silence"); // only `admit`'s own timeout path moves it
    }
    if leader_after != Some(publisher) && prev != Some(publisher) {
        return Some("health"); // the per-market override took this market
    }
    None
}

/// A `clear`-only re-baseline for one market: what a consumer gets when the gate has no accumulated
/// levels to republish (an evicted market). Legal on its own, and `last` is why that field is
/// mandatory.
fn clear_only(b: &NormalizedBook) -> NormalizedBook {
    NormalizedBook {
        changes: vec![model::BookChange {
            action: model::BookAction::Clear,
            side: model::BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }],
        snapshot: true,
        last: true,
        // Producer-synthesized, like `to_book`'s: the triggering batch's arrival time is not when this
        // message was built.
        recv_ts_ns: now_ns(),
        ..b.clone()
    }
}

/// Index of a [`Publisher`] class into the 2-wide `[edge, public]` metric arrays.
fn pub_idx(p: Publisher) -> usize {
    match p {
        Publisher::Edge(_) => 0,
        Publisher::PublicWs => 1,
    }
}

/// Pre-resolved `dz_*` metric children for one venue. Built once per venue via [`VenueMetrics::new`];
/// every field is a cheap-to-`inc()` handle, so the per-message emit path pays no label lookup.
struct VenueMetrics {
    /// `dz_emit_total{kind}` indexed by kind: quote/trade/instrument/midpoint/depth/status/book.
    emit: [IntCounter; 7],
    /// `dz_quotes_admitted_total{publisher}` / `dz_trades_admitted_total{publisher}`, `[edge, public]`.
    quotes_admitted: [IntCounter; 2],
    trades_admitted: [IntCounter; 2],
    /// `dz_quote_ticks_won_total{publisher}` / `dz_depth_ticks_won_total{publisher}`, `[edge, public]`.
    quote_ticks_won: [IntCounter; 2],
    depth_ticks_won: [IntCounter; 2],
    quotes_dropped: IntCounter,
    trades_dropped: IntCounter,
    trades_no_id: IntCounter,
    trades_no_id_conflict: IntCounter,
    instruments_dropped: IntCounter,
    quotes_future_rejected: IntCounter,
    quotes_no_source_ts: IntCounter,
    /// `dz_depth_admitted_total{publisher}` / `dz_depth_dropped_total{publisher}`, `[edge, public]`.
    depth_admitted: [IntCounter; 2],
    depth_dropped: [IntCounter; 2],
    depth_future_rejected: IntCounter,
    /// `dz_book_dropped_total{publisher}`, `[edge, public]` — the `depth_dropped` shape, keeping the
    /// which-arm-is-losing signal a scalar would lose.
    book_dropped: [IntCounter; 2],
    book_markets_evicted: IntCounter,
    /// `dz_quote_lead_ns{winner,loser}` / `dz_trade_lead_ns{winner,loser}` / `dz_depth_lead_ns
    /// {winner,loser}` indexed `winner_idx * 2 + loser_idx` over `[edge, public]`.
    quote_lead: [Histogram; 4],
    trade_lead: [Histogram; 4],
    depth_lead: [Histogram; 4],
    /// `dz_arm_lead_ns{winner}` indexed `[leader, challenger]` — one matched cross-arm trade pair.
    arm_lead: [Histogram; 2],
}

impl VenueMetrics {
    fn new(venue: &str) -> Self {
        let m = metrics();
        let emit_kind = |k: &str| m.emit.with_label_values(&[venue, k]);
        // Pre-resolve the `[edge, public]` children of a `{venue, publisher}` counter, in `pub_idx`
        // order (edge=0, public=1) so the emit path indexes with `pub_idx(publisher)`.
        let by_pub = |c: &prometheus::IntCounterVec| {
            [
                c.with_label_values(&[venue, "edge"]),
                c.with_label_values(&[venue, "public"]),
            ]
        };
        let lead = |h: &prometheus::HistogramVec| {
            [
                h.with_label_values(&[venue, "edge", "edge"]),
                h.with_label_values(&[venue, "edge", "public"]),
                h.with_label_values(&[venue, "public", "edge"]),
                h.with_label_values(&[venue, "public", "public"]),
            ]
        };
        Self {
            emit: [
                emit_kind("quote"),
                emit_kind("trade"),
                emit_kind("instrument"),
                emit_kind("midpoint"),
                emit_kind("depth"),
                emit_kind("status"),
                emit_kind("book"),
            ],
            quotes_admitted: by_pub(&m.quotes_admitted),
            trades_admitted: by_pub(&m.trades_admitted),
            quote_ticks_won: by_pub(&m.quote_ticks_won),
            depth_ticks_won: by_pub(&m.depth_ticks_won),
            quotes_dropped: m.quotes_dropped.with_label_values(&[venue]),
            trades_dropped: m.trades_dropped.with_label_values(&[venue]),
            trades_no_id: m.trades_no_id.with_label_values(&[venue]),
            trades_no_id_conflict: m.trades_no_id_conflict.with_label_values(&[venue]),
            instruments_dropped: m.instruments_dropped.with_label_values(&[venue]),
            quotes_future_rejected: m.quotes_future_rejected.with_label_values(&[venue]),
            quotes_no_source_ts: m.quotes_no_source_ts.with_label_values(&[venue]),
            depth_admitted: by_pub(&m.depth_admitted),
            depth_dropped: by_pub(&m.depth_dropped),
            depth_future_rejected: m.depth_future_rejected.with_label_values(&[venue]),
            book_dropped: by_pub(&m.book_dropped),
            book_markets_evicted: m.book_markets_evicted.with_label_values(&[venue]),
            quote_lead: lead(&m.quote_lead_ns),
            trade_lead: lead(&m.trade_lead_ns),
            depth_lead: lead(&m.depth_lead_ns),
            arm_lead: [
                m.arm_lead_ns.with_label_values(&[venue, "leader"]),
                m.arm_lead_ns.with_label_values(&[venue, "challenger"]),
            ],
        }
    }
}

/// Kind index into [`VenueMetrics::emit`].
const EMIT_QUOTE: usize = 0;
const EMIT_TRADE: usize = 1;
const EMIT_INSTRUMENT: usize = 2;
const EMIT_MIDPOINT: usize = 3;
const EMIT_DEPTH: usize = 4;
const EMIT_STATUS: usize = 5;
const EMIT_BOOK: usize = 6;

impl Arbiter {
    pub fn new(tx: broadcast::Sender<Arc<FeedMessage>>, trade_window: usize) -> Self {
        Self {
            tx,
            quotes: StalenessFloor::new(QUOTE_TICK_CAP),
            trades: WindowedDedup::new(trade_window),
            depths: StalenessFloor::new(DEPTH_TICK_CAP),
            depth_replay: None,
            instrument_defs: HashMap::new(),
            venue_metrics: HashMap::new(),
            modes: HashMap::new(),
            no_id_owner: HashMap::new(),
            tape_leader: HashMap::new(),
            no_id_conflict_logged: false,
            book_withhold_logged: false,
            books: StickyAuthority::new(AuthorityConfig::DEFAULT),
            race: ArmRace::default(),
            book_markets: HashMap::new(),
            book_order: VecDeque::new(),
            book_replay: None,
            book_sync: HashMap::new(),
            book_events: HashMap::new(),
            book_dedup_window_ns: DEFAULT_BOOK_DEDUP_WINDOW_NS,
        }
    }

    /// How long a delivered order event stays in the racing window (`--arb-book-dedup-window-ms`).
    pub fn set_book_dedup_window(&mut self, window_ns: u64) {
        self.book_dedup_window_ns = window_ns;
    }

    /// Report one publisher's order-level book sync state for a market — the seam the Market-by-Order
    /// processor calls on every [`crate::ingest::book::BookState`] status transition.
    ///
    /// **Contract: report `true` before emitting the re-baseline that follows a snapshot install.** The
    /// suppression below reads these states to decide whether a recovering arm is alone, and an arm that
    /// publishes its full book before saying so would let a simultaneously-recovering peer conclude the
    /// same and wipe the consumer twice.
    ///
    /// Ineligible arms are ignored, exactly as [`StickyAuthority::admit`] ignores them: the report keys
    /// on a spoofable source IP, so without this a forged flood would both grow the map and mint an
    /// unbounded supply of peers whose claim suppresses a real publisher's re-baseline.
    pub fn set_book_synced(&mut self, key: &MarketKey, publisher: Publisher, synced: bool) {
        if self
            .books
            .arm_ordinal(&(key.0.clone(), key.1.clone()), publisher)
            == OTHER_ARM
        {
            return;
        }
        if !self.book_markets.contains_key(key) {
            self.track_book_market(key);
        }
        let peer = self
            .book_sync
            .entry(key.clone())
            .or_default()
            .entry(publisher)
            .or_default();
        peer.synced = synced;
    }

    /// Drop a departed publisher's claim to be serving `venue`'s order-level markets — the seam a
    /// Market-by-Order receiver's registration calls as it exits.
    ///
    /// Departure is the authoritative signal and [`PEER_SERVING_NS`] is only its backstop, for a
    /// publisher that goes quiet without deregistering: a gap-and-recover cycle is sub-second, so the
    /// timer never binds on it, and a suppressed re-baseline is never retried — the surviving arm's
    /// market wedges for the life of the process.
    ///
    /// Scoped to the venue, and to the sync claims only. One publisher host serves several protocols
    /// from one source IP (the tape-arm gate rests on that), so a venue-blind sweep would let an
    /// exiting Market-by-Order receiver tear down the same host's live Market-by-Price state.
    pub fn forget_publisher_books(&mut self, venue: &str, publisher: Publisher) {
        for (key, arms) in self.book_sync.iter_mut() {
            if key.0.as_ref() == venue {
                arms.remove(&publisher);
            }
        }
    }

    /// Install the `--arb-*` arbitration tunables: the single-arm authority config, and the cross-arm
    /// matcher's pairing window. Called once at startup; without it both take
    /// [`AuthorityConfig::DEFAULT`] — the same values `main.rs`'s clap defaults are derived from — so
    /// an arbiter built anywhere still gates `book` rather than interleaving two arms.
    pub fn set_authority(&mut self, cfg: AuthorityConfig, match_window_ns: u64) {
        self.books.set_config(cfg);
        self.race = ArmRace::new(match_window_ns);
    }

    /// Wire the shared WS-replay `book` map so a connecting client is bootstrapped with the serving
    /// arm's accumulated levels. Without it the gate still arbitrates; there is just no replay state.
    pub fn set_book_replay(&mut self, books: BookSnapshot) {
        self.book_replay = Some(books);
    }

    /// The authority, so a test can assert a processor's health reports landed.
    #[cfg(test)]
    pub(crate) fn authority(&self) -> &StickyAuthority {
        &self.books
    }

    /// Whether one arm still claims a synced book for a market, so a test can assert a departure
    /// released it.
    #[cfg(test)]
    pub(crate) fn book_arm_synced(&self, key: &MarketKey, publisher: Publisher) -> bool {
        self.book_sync
            .get(key)
            .and_then(|arms| arms.get(&publisher))
            .is_some_and(|st| st.synced)
    }

    /// Report one arm's book health for a market — the seam the MBP processor calls on every
    /// `PriceBook` status transition, since `books` is private here.
    ///
    /// Health is a **per-market override** on the universe's authority: an arm gapped on one market yields
    /// that market only, and takes it back on its own once the book recovers. Under incremental output
    /// a lost level does not self-heal until the next snapshot, so an unhealthy arm must not serve.
    pub fn set_book_health(&mut self, key: &MarketKey, publisher: Publisher, healthy: bool) {
        self.books.set_health(key, publisher, healthy);
    }

    /// Declare a venue's arbitration mode. Called once per selected feed at startup; a venue's rows
    /// are pinned to one mode by `feeds::tests::arbitration_mode_agrees_across_a_venues_rows`.
    pub fn set_mode(&mut self, venue: &'static str, mode: ArbitrationMode) {
        self.modes.insert(venue, mode);
    }

    fn mode_for(&self, venue: &str) -> ArbitrationMode {
        self.modes
            .get(venue)
            .copied()
            .unwrap_or(ArbitrationMode::Coordinated)
    }

    /// Wire the shared WS-replay `depth` map so the arbiter updates it on each admitted (leader)
    /// depth. The bridge calls this once at startup; without it the depth floor still dedups the
    /// broadcast but maintains no replay snapshot.
    pub fn set_depth_replay(&mut self, depth: DepthSnapshot) {
        self.depth_replay = Some(depth);
    }

    /// The pre-resolved metric children for `venue`, created on first use.
    fn vm(&mut self, venue: &Arc<str>) -> &VenueMetrics {
        self.venue_metrics
            .entry(venue.clone())
            .or_insert_with(|| VenueMetrics::new(venue))
    }

    /// The broadcast sender, so output sinks can `subscribe()` and `Status` can be sent directly
    /// (it carries no business identity to dedup). The backbone carries `Arc<FeedMessage>` so a
    /// per-receiver delivery is a refcount bump, not a deep clone of the message's `String`/`Vec`s.
    pub fn sender(&self) -> &broadcast::Sender<Arc<FeedMessage>> {
        &self.tx
    }

    /// Clear every latched depth-floor entry for `venue` — the session-reset escape hatch, called
    /// by the MBO processor on `EndOfSession` (which carries no instrument id, so the whole venue
    /// resets). Without it a venue that restarts its event clock below the latched high-water
    /// would have every post-session depth dropped as stale, permanently (see the `depths` docs).
    /// The venue's WS-replay `depth` entries are purged in the same step: they hold the ended
    /// session's final books, and (unlike the floor, which the next admitted depth re-opens) the
    /// replay map has no other cleanup for an instrument the new session never re-lists — a client
    /// connecting after the boundary would be served that phantom book indefinitely. Replay
    /// repopulates from the first admitted new-session depth. Cleared floor entries are counted in
    /// `dz_depth_floor_resets_total{venue, reason}`.
    ///
    /// Worst case of a spurious reset (e.g. a forged `EndOfSession` — the source IP is spoofable):
    /// a still-live publisher's next depth re-opens the tick, possibly re-admitting a snapshot at
    /// an already-served `source_ts` — full-state, so consumers self-heal. Strictly better than
    /// the permanent wedge the reset prevents.
    pub fn reset_depth_floor_for_venue(&mut self, venue: &str, reason: &'static str) {
        let cleared = self.depths.reset_where(|(v, _)| v.as_ref() == venue);
        if let Some(replay) = &self.depth_replay {
            model::lock(replay).retain(|(v, _), _| v.as_ref() != venue);
        }
        self.record_floor_resets(venue, reason, cleared);
    }

    /// Clear one `(venue, symbol)` latched depth-floor entry (and its WS-replay entry, for the
    /// same reason as [`Self::reset_depth_floor_for_venue`]) — the per-instrument variant, called
    /// by the MBO processor on `InstrumentReset` (the book re-snapshots, and the post-reset anchor
    /// may carry a lower `source_ts`).
    ///
    /// The floor entry is shared across publishers while `InstrumentReset` arrives per publisher,
    /// so a one-publisher reset also clears a healthy mirror's latch: worst case the resetting
    /// publisher's post-resync depth (stamped `source_ts = 0`, its event clock was dropped with
    /// the book) transiently wins leadership and reaches the wire/replay map until the live
    /// mirror's next event — full-state, self-healing, and strictly better than skipping the
    /// clear (a venue clock restart would wedge the symbol permanently).
    pub fn reset_depth_floor_for_symbol(
        &mut self,
        venue: &str,
        symbol: &str,
        reason: &'static str,
    ) {
        let cleared = self
            .depths
            .reset_where(|(v, s)| v.as_ref() == venue && s.as_ref() == symbol);
        if let Some(replay) = &self.depth_replay {
            model::lock(replay).remove(&(Arc::from(venue), Arc::from(symbol)));
        }
        self.record_floor_resets(venue, reason, cleared);
    }

    /// Record cleared floor entries in `dz_depth_floor_resets_total{venue, reason}` (shared by
    /// both reset variants so a future label-set change is edited once).
    fn record_floor_resets(&self, venue: &str, reason: &'static str, cleared: usize) {
        metrics()
            .depth_floor_resets
            .with_label_values(&[venue, reason])
            .inc_by(cleared as u64);
    }

    /// Fold one arm's batch into that arm's own accumulator for `key`, admitting the market to the
    /// tracked set (evicting the oldest when it is full).
    fn accumulate_book(&mut self, key: &MarketKey, publisher: Publisher, b: &NormalizedBook) {
        if !self.book_markets.contains_key(key) {
            self.track_book_market(key);
        }
        let Some(market) = self.book_markets.get_mut(key) else {
            return;
        };
        market
            .arms
            .entry(publisher)
            .or_insert_with(|| BookAccumulator::new(b.symbol.clone()))
            .apply(b);
    }

    /// Start tracking `key`, evicting the oldest markets to stay within [`MAX_BOOK_MARKETS`].
    ///
    /// Eviction drops the market's accumulators, its replay entry **and** its authority state together
    /// (`StickyAuthority::forget_market`). That pairing is what makes eviction safe: losing
    /// `last_admitted` makes the market's next batch read as a change of serving arm, so it
    /// re-baselines the consumer instead of resuming another arm's delta series on top of its state.
    fn track_book_market(&mut self, key: &MarketKey) {
        while self.book_markets.len() >= MAX_BOOK_MARKETS {
            let Some(old) = self.book_order.pop_front() else {
                break;
            };
            // The same pairing every other drop path uses — `pop_front` above has already taken
            // this key out of `book_order`, which is why eviction calls the leg-dropper directly
            // rather than going through [`Self::reset_books_for_markets`]: that would add a full
            // `book_order` scan per evicted market, on the ingest hot path, to remove a key that
            // is already gone.
            self.drop_market_state(&old);
            self.vm(&old.0).book_markets_evicted.inc();
        }
        self.book_order.push_back(key.clone());
        self.book_markets.insert(key.clone(), BookMarket::default());
    }

    /// Republish `publisher`'s whole book for `key` as a re-baseline — a `clear` plus its complete
    /// current level set — and reset the shared replay entry to that arm's accumulator.
    ///
    /// `None` when the accumulator is not [`BookAccumulator::baselined`]: seeded mid-stream, it holds
    /// only the levels that have moved since, so publishing it as `snapshot` would tell the consumer to
    /// discard every level it is missing. The caller degrades to a bare `clear`, which is incomplete
    /// but says so — and the replay entry carries the same flag, so the WS replay skips it too.
    fn rebaseline_book(&mut self, key: &MarketKey, publisher: Publisher) -> Option<NormalizedBook> {
        let acc = self
            .book_markets
            .get(key)
            .and_then(|m| m.arms.get(&publisher))
            .cloned()?;
        if let Some(replay) = &self.book_replay {
            model::lock(replay).insert(key.clone(), acc.clone());
        }
        // `Orders` scope: this re-baselines the live stream, so it must carry whatever granularity that
        // stream carries, or an order-level consumer is handed levels and then cancels for ids it never
        // saw. A price-aggregated market holds no orders, so the two scopes render identically for it.
        acc.baselined()
            .then(|| acc.to_book(key, ReplayScope::Orders))
    }

    /// Drop one market's raced order-event state — the **session-reset seam for order-level racing**.
    ///
    /// A session boundary restarts the venue's order-id space, so the tombstones and resting-quantity
    /// floors from the ended session would otherwise refuse the new session's legitimately-reused ids.
    /// Deliberately narrower than [`Self::reset_book_for_market`]: it leaves the replay accumulator and
    /// the authority entry alone, because a peer arm that did not see the session end is still serving
    /// this market and tearing its published book down is the failure that variant exists to avoid.
    pub fn reset_book_events_for_market(&mut self, key: &MarketKey) {
        self.book_events.remove(key);
    }

    /// Drop one market's tracked `book` state — the session-reset seam, mirroring
    /// [`Self::reset_depth_floor_for_symbol`]. The authority entry goes with it, so the market's next
    /// batch re-baselines the consumer rather than resuming on state that was just discarded.
    ///
    /// There is deliberately no venue-wide variant: the MBP processor scopes `EndOfSession` to the
    /// emitting arm and channel and reports those markets unhealthy, which hands them to the peer arm
    /// rather than tearing down a live arm's published book.
    pub fn reset_book_for_market(&mut self, key: &MarketKey) {
        // Delegated, not re-implemented: [`Self::reset_books_for_markets`] is the single expression
        // of the drop, so the pairing cannot drift between a single-key and a batch path. The cost
        // is unchanged — this path always scanned `book_order` once (`retain`) — plus one
        // one-element `HashSet`, which is not on any hot path (this is the session-reset seam).
        self.reset_books_for_markets(std::slice::from_ref(key));
    }

    /// The single expression of the **three-way drop**: the per-arm accumulators (`book_markets`),
    /// the shared replay entry and `StickyAuthority`'s `last_admitted`, for a batch of keys —
    /// [`Self::reset_book_for_market`] is one key through
    /// here and [`Self::forget_channel_books`] is a channel's worth.
    ///
    /// The three legs **must** drop together: losing `last_admitted` is what forces the consumer
    /// re-baseline, so a replay entry deleted without it leaves a market that never re-baselines and
    /// stays invisible to every new client. Adding a fourth leg is an edit here (and in
    /// [`Self::drop_market_state`], which eviction shares) rather than a thing to remember in three
    /// places.
    ///
    /// `book_order`'s cleanup is one linear scan over the whole queue, not one scan per key: a
    /// per-key `retain` is O(`book_order.len()`), so a loop over N keys is O(`N *
    /// book_order.len()`), which is real money at [`MAX_BOOK_MARKETS`] scale — shedding most of a
    /// channel's markets (the common floor-narrowing case: a handful of channels admitted out of
    /// dozens) would otherwise stall the **one arbiter mutex**, and therefore all ingest, for tens
    /// of milliseconds to seconds.
    fn reset_books_for_markets(&mut self, keys: &[MarketKey]) {
        if keys.is_empty() {
            return;
        }
        let doomed: HashSet<&MarketKey> = keys.iter().collect();
        for key in keys {
            self.drop_market_state(key);
        }
        self.book_order.retain(|k| !doomed.contains(k));
    }

    /// Drop one market's three paired legs — accumulators, replay entry, `last_admitted` — and
    /// nothing else. Every path that discards a market's `book` state goes through here (batch
    /// reset, single reset via the batch, and eviction), which is what keeps the pairing written
    /// once.
    ///
    /// `book_order` is deliberately **not** touched: it is the eviction queue, not one of the paired
    /// legs, and its callers remove from it differently (eviction has already `pop_front`ed the key;
    /// the reset paths filter the queue once for the whole batch). A residual entry there costs one
    /// wasted `pop_front` at eviction time — [`Self::track_book_market`] tolerates a key that is
    /// already gone — never corruption.
    fn drop_market_state(&mut self, key: &MarketKey) {
        self.book_markets.remove(key);
        self.book_sync.remove(key);
        self.book_events.remove(key);
        if let Some(replay) = &self.book_replay {
            model::lock(replay).remove(key);
        }
        self.books.forget_market(key);
    }

    /// Race one order-level `book` batch across a distributed venue's publishers: publish each venue
    /// event's first arrival and collapse every later copy, so a consumer sees each event once and
    /// always from whichever publisher was fastest for *that event*.
    ///
    /// Two decisions, in order. A `Clear`-led batch is a **re-baseline** and must not race: a publisher
    /// recovering via snapshot would wipe a consumer a healthy peer is serving correctly, so it is
    /// published only when no peer of that market is synced — decided here, in one place, because two
    /// publishers recovering together must not both conclude they are alone. Every other batch is
    /// filtered change by change: the surviving events are republished and the collapsed ones are not.
    /// Filtering rather than passing the batch whole is load-bearing — an already-delivered change
    /// carries the order's *absolute* quantity, so re-delivering it after the wire has moved on would
    /// walk a consumer's order back to a size the venue has already reduced.
    ///
    /// A change with no order identity (`order_id == 0`) is un-collapsable and always published.
    fn emit_order_level_book(&mut self, key: MarketKey, b: &NormalizedBook, publisher: Publisher) {
        let now = now_mono_ns();
        if b.changes
            .first()
            .is_some_and(|c| c.action == BookAction::Clear)
        {
            // A peer protects this market only while it is *both* in sync and actually serving. Reading
            // the flag alone would let a departed publisher — or one that only ever sent reference data —
            // suppress the surviving arm's re-baseline forever, and a re-baseline is this product's only
            // self-heal. It also settles the both-recovering race: whichever arm publishes first records
            // a delivery, so the second sees a serving peer and drops, and neither arm needs to have
            // published before for the first one to get through.
            let peer_serving = self.book_sync.get(&key).is_some_and(|arms| {
                arms.iter().any(|(&p, st)| {
                    p != publisher
                        && st.synced
                        && st.last_batch_ns != 0
                        && now.saturating_sub(st.last_batch_ns) <= PEER_SERVING_NS
                })
            });
            if peer_serving {
                self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
                return;
            }
            self.clear_led_rebaseline(&key, b, publisher, now);
            return;
        }
        if self.book_markets.get(&key).is_some_and(|m| m.rebaseline)
            && !self.serve_forced_rebaseline(&key, b, publisher, now)
        {
            return;
        }
        let window = self.book_dedup_window_ns;
        let events = self.book_events.entry(key.clone()).or_default();
        let mut kept: Vec<BookChange> = Vec::new();
        let (mut deduped, mut disagreed, mut resurrected) = (0u64, 0u64, 0u64);
        for c in &b.changes {
            if c.order_id == 0 {
                kept.push(*c);
                continue;
            }
            let ev = OrderEvent {
                order_id: c.order_id,
                action: c.action,
                price_bits: c.price.to_bits(),
                size_bits: c.size.to_bits(),
            };
            match events.admit(ev, publisher, b.recv_ts_ns, window) {
                EventVerdict::Deliver => kept.push(*c),
                EventVerdict::Disagreement => disagreed += 1,
                EventVerdict::Duplicate => deduped += 1,
                EventVerdict::Resurrection => resurrected += 1,
            }
        }
        let forced = events.forced.take();
        let venue = b.venue.as_ref();
        if deduped > 0 {
            metrics()
                .book_events_deduped
                .with_label_values(&[venue])
                .inc_by(deduped);
        }
        if resurrected > 0 {
            metrics()
                .book_resurrections_dropped
                .with_label_values(&[venue])
                .inc_by(resurrected);
        }
        if disagreed > 0 {
            metrics()
                .mbo_arm_disagreement
                .with_label_values(&[venue])
                .inc_by(disagreed);
        }
        if let Some(reason) = forced {
            // This batch's surviving changes go with it: a `Disagreement` drops one change out of a
            // logical event, and publishing the rest with `last` intact hands the consumer half an
            // event as a whole one. The re-baseline that follows carries the market's whole book.
            self.force_rebaseline(&key, &b.venue, reason);
            self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
            return;
        }
        if kept.is_empty() {
            return;
        }
        let out = if kept.len() == b.changes.len() {
            b.clone()
        } else {
            NormalizedBook {
                changes: kept,
                ..b.clone()
            }
        };
        self.note_book_delivery(&key, b, publisher, now);
        self.publish_book(&key, out);
    }

    /// Publish a producer's own `Clear`-led re-baseline and re-point the market's race state at it.
    fn clear_led_rebaseline(
        &mut self,
        key: &MarketKey,
        b: &NormalizedBook,
        publisher: Publisher,
        now: u64,
    ) {
        self.book_events
            .entry(key.clone())
            .or_default()
            .rebaselined(&b.changes, publisher);
        if let Some(m) = self.book_markets.get_mut(key) {
            m.rebaseline = false;
            m.withheld = 0;
        }
        self.note_book_delivery(key, b, publisher, now);
        self.publish_book(key, b.clone());
    }

    /// Mark a market unserveable until it is re-baselined, counting why.
    fn force_rebaseline(&mut self, key: &MarketKey, venue: &str, reason: &'static str) {
        if let Some(m) = self.book_markets.get_mut(key) {
            m.rebaseline = true;
        }
        metrics()
            .mbo_forced_rebaselines
            .with_label_values(&[venue, reason])
            .inc();
    }

    /// Discharge a forced re-baseline on `publisher`'s batch, returning whether the caller should go on
    /// to publish that batch onto it.
    ///
    /// Ordinary events are withheld meanwhile: while the flag is set neither arm's deltas are known to
    /// describe the book a consumer holds, so publishing them is the guess the flag exists to refuse.
    /// The wait for a completed logical event, and its bound, are the authority path's — a `to_book` of
    /// a half-applied event goes out stamped `last` as a torn book, and `last` is a promise made by an
    /// unauthenticated producer.
    ///
    /// **Republished from the shared replay map, never from an arm's own accumulator.** The replay map
    /// holds what actually reached the wire; an arm's accumulator holds whatever that source sent, and
    /// on an unauthenticated wire republishing it as `snapshot`/`last` would let one forged datagram —
    /// a size claim large enough to be a disagreement — buy the wholesale replacement of a market's
    /// book with a fabricated one. What the flag protects is the consumer, and full state it already
    /// agreed with is what does that.
    ///
    /// Rate-limited to one republish per market per dedup window: a single datagram can raise the flag
    /// and each discharge costs a whole book, both to serialize and in time held on the shared arbiter
    /// mutex. Inside the window the market keeps withholding, so it is a delay and not a mute.
    fn serve_forced_rebaseline(
        &mut self,
        key: &MarketKey,
        b: &NormalizedBook,
        publisher: Publisher,
        now: u64,
    ) -> bool {
        let window = self.book_dedup_window_ns;
        if let Some(m) = self.book_markets.get_mut(key) {
            let too_soon = m.rebaselined_ns != 0 && now.saturating_sub(m.rebaselined_ns) < window;
            if too_soon || (!b.last && m.withheld < MAX_WITHHELD_BATCHES) {
                m.withheld += 1;
                self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
                return false;
            }
            m.rebaseline = false;
            m.withheld = 0;
            m.rebaselined_ns = now;
        }
        let Some(full) = self.rebaseline_from_replay(key) else {
            // Nothing complete to republish: empty the consumer's book and let this batch rebuild onto
            // it, exactly as the single-arm gate degrades. The replay entry goes with it — left in
            // place it would keep claiming completeness and bootstrap a new client with the orders
            // live consumers were just told to discard.
            if let Some(replay) = &self.book_replay {
                model::lock(replay).remove(key);
            }
            self.book_events
                .entry(key.clone())
                .or_default()
                .rebaselined(&[], publisher);
            self.vm(&b.venue).emit[EMIT_BOOK].inc();
            let _ = self.tx.send(Arc::new(FeedMessage::Book(clear_only(b))));
            return true;
        };
        self.book_events
            .entry(key.clone())
            .or_default()
            .rebaselined(&full.changes, publisher);
        self.vm(&b.venue).emit[EMIT_BOOK].inc();
        let _ = self.tx.send(Arc::new(FeedMessage::Book(full)));
        true
    }

    /// The market's book as the wire has delivered it, as a `clear`-led re-baseline. `None` when no
    /// replay map is wired or its accumulator is not [`BookAccumulator::baselined`] — seeded mid-stream
    /// it holds only what has moved since, and publishing that as `snapshot` would tell the consumer to
    /// discard every level it is missing. Materialized under the shared lock rather than cloned out of
    /// it, for the reason `sinks/hyperliquid.rs` does the same.
    fn rebaseline_from_replay(&self, key: &MarketKey) -> Option<NormalizedBook> {
        let replay = self.book_replay.as_ref()?;
        let out = model::lock(replay)
            .get(key)
            .filter(|acc| acc.baselined())
            .map(|acc| acc.to_book(key, ReplayScope::Orders));
        out
    }

    /// Record that `publisher` reached the wire for this market, and mark the market order-level. Both
    /// are read by the next `Clear`-led batch's suppression decision, so both must be written on every
    /// published batch rather than only on a state transition.
    fn note_book_delivery(
        &mut self,
        key: &MarketKey,
        b: &NormalizedBook,
        publisher: Publisher,
        now: u64,
    ) {
        if !self.book_markets.contains_key(key) {
            self.track_book_market(key);
        }
        if b.changes.iter().any(|c| c.order_id != 0) {
            if let Some(m) = self.book_markets.get_mut(key) {
                m.order_level = true;
            }
        }
        self.book_sync
            .entry(key.clone())
            .or_default()
            .entry(publisher)
            .or_default()
            .last_batch_ns = now;
    }

    /// Broadcast one order-level batch and advance the shared replay accumulator with it, so a client
    /// connecting mid-stream is bootstrapped from what actually reached the wire rather than from any
    /// single publisher's copy.
    fn publish_book(&mut self, key: &MarketKey, b: NormalizedBook) {
        if !self.book_markets.contains_key(key) {
            self.track_book_market(key);
        }
        self.apply_book_replay(key, &b);
        self.vm(&b.venue).emit[EMIT_BOOK].inc();
        let _ = self.tx.send(Arc::new(FeedMessage::Book(b)));
    }

    /// Drop **every** tracked `book` market on `(venue, category, channel)` — every
    /// `instrument_id` under it — through [`Self::reset_books_for_markets`], so each one's
    /// accumulator, replay entry and `StickyAuthority::last_admitted` drop together exactly as a
    /// single-market reset does — one batch, so `book_order` is filtered once rather than once per
    /// key (see that method's doc).
    ///
    /// The channel-departure seam (`ingest::reconcile`'s ingest-floor narrowing/removal) — a
    /// different reason from `reset_book_for_market`'s "no venue-wide variant" note above, which is
    /// about *EndOfSession*, a producer-side signal scoped to one arm/channel that must not tear
    /// down a live peer arm's book. This is the opposite case: an operator-driven removal of a
    /// channel this process has stopped ingesting altogether, so every market on it — across every
    /// arm — is meant to go.
    ///
    /// **Never hand-delete from `self.book_replay` (or any caller reaching into a `BookSnapshot`
    /// directly) instead of calling this.** `last_admitted` would be left behind: if the channel is
    /// later restored and the same arm resumes, nothing forces a re-baseline, so
    /// `apply_book_replay` recreates the replay entry with `baselined() == false`, and
    /// `sinks/ws.rs`'s replay path then hides the market from every new client until the arm
    /// happens to emit a `Clear` of its own accord.
    pub fn forget_channel_books(&mut self, venue: &str, category: &str, channel: u32) -> usize {
        let doomed: Vec<MarketKey> = self
            .book_markets
            .keys()
            .filter(|k| k.0.as_ref() == venue && k.1.as_ref() == category && k.2 == channel)
            .cloned()
            .collect();
        let dropped = doomed.len();
        self.reset_books_for_markets(&doomed);
        dropped
    }

    /// Advance the shared replay accumulator with an admitted batch, keeping it in step with the
    /// serving arm's own. Skips markets outside the tracked set, so it inherits the same cap.
    fn apply_book_replay(&mut self, key: &MarketKey, b: &NormalizedBook) {
        let Some(replay) = &self.book_replay else {
            return;
        };
        if !self.book_markets.contains_key(key) {
            return;
        }
        model::lock(replay)
            .entry_or_insert_with(key, || BookAccumulator::new(b.symbol.clone()))
            .apply(b);
    }

    /// Whether a trade from `publisher` is evidence about a book-serving arm.
    ///
    /// Two filters, both load-bearing. **An edge arm only**: the public WS backstop reaches `emit` with
    /// the same trades, decodes them from parsed JSON rather than the arms' shared fixed-point, and
    /// serves no `book` at all — matching it would poison `dz_arm_lead_ns` with edge-vs-public leads
    /// and could hand a venue's books to a source that publishes none. **Already tracked by the
    /// authority**: `observe_matched_lead` creates an arm entry for whatever it is handed, so an
    /// untracked publisher (a peer feed row of the same universe, a forged source IP) would otherwise
    /// spend one of the universe's eight admission slots and could displace a real mirror arm.
    fn race_eligible(&self, scope: &ScopeKey, publisher: Publisher) -> bool {
        matches!(publisher, Publisher::Edge(_)) && self.books.tracks_arm(scope, publisher)
    }

    /// Whether this arm currently serves a `Sticky` **universe**'s tape — one gate per
    /// `(venue, category)`, never one per venue.
    ///
    /// Scope first, because it is what makes every rule below safe to state: a single Source ID can
    /// carry instrument universes that mirror nothing of one another, and this gate exists to pick
    /// one arm out of a set of *mirrors*. Keyed on the venue alone it would instead pick one arm
    /// across disjoint universes and drop the other's whole stream — permanently, since the silence
    /// handover needs the incumbent to stop and an incumbent streaming its own universe never does.
    /// The symptom is empty candles for that universe, indistinguishable from a market that did not
    /// trade. `Feed::category` is the registry's declaration of which rows mirror each other, and it
    /// reaches here as an [`Arbiter::emit`] parameter rather than a wire field (PROTOCOL.md is a
    /// consumer contract; this is producer-side keying).
    ///
    /// The reconciler's row ownership picks which *feed* prints; this picks which *arm* within it, and
    /// the `trade_id == 0` bypass below needs both. A sticky venue's arms share no trade-id space: one
    /// may stamp the sentinel while its peer stamps a real venue id, and that pair meets neither the
    /// bypass's owner latch (keyed on the sentinel) nor [`WindowedDedup`] (keyed on the id). Gating on
    /// arm identity instead of on the id collapses that case and the two-different-real-ids one alike.
    ///
    /// Four rules, each load-bearing:
    ///
    /// - **No dark start.** With no entry the first arm to print leads. A top-of-book-only deployment
    ///   carries no `book` traffic, so `scope_leader` is `None` forever and electing first would drop
    ///   the venue's whole tape.
    /// - **Corroborated beats uncorroborated.** A challenger the authority tracks displaces an
    ///   incumbent it does not, immediately. The incumbent slot is filled by whoever prints first, and
    ///   the wire is unauthenticated: without this an early forged print would hold the tape and mute
    ///   the real arms for as long as it kept printing inside the silence window. (It does not close
    ///   that hole on a venue with no `book` traffic, where the authority tracks nobody — see below.)
    /// - **Defer to the book election.** A challenger the authority has *elected* takes over at once,
    ///   so the tape converges on the arm serving the books. Sound because a publisher host uses one
    ///   source IP for both of a venue's protocols, making arm identity shared across its rows. Honored
    ///   once per election, not per print: re-honoring it on every print would let an elected arm whose
    ///   trade stream is nearly dead reclaim the tape from the healthy peer after each straggler and
    ///   mute it for another window — the two rules would fight and the tape would sawtooth. A silence
    ///   handover marks the election it overrode as spent, which is what closes that loop.
    /// - **Silence handover.** A challenger also takes over after [`NO_ID_TAPE_HANDOVER_NS`] of
    ///   incumbent silence — otherwise an elected arm whose *trade* stream is dead would mute the tape.
    ///
    /// ⚠️ Two limits, both inherited from the unauthenticated wire rather than introduced here. On a
    /// venue with **no `book` traffic** the authority tracks and elects nobody, so a forged source that
    /// prints first holds the tape until it goes quiet for a window — the same primitive
    /// [`StickyAuthority::admit`]'s no-dark-start already exposes for the `book` product, and not
    /// closable without an identity the wire does not carry. And the gate spans a whole **category**:
    /// it assumes the rows sharing one carry mirrored tapes, so if two of them instead sharded prints
    /// between them the non-serving arm's exclusive fills would be dropped — the registry's job is to
    /// give sharded rows distinct categories. `dz_tape_arm_dropped_total` is what makes either visible
    /// — deliberately its own counter, not folded into `dz_trades_dropped_total`, whose steady state
    /// here is the challenger's whole stream.
    ///
    /// The two `books` lookups below read the authority at the **same** `(venue, category)` grain
    /// this gate runs at, so the deferral can only ever name an arm elected on *this* universe. While
    /// [`StickyAuthority`] was venue-wide they could name an arm elected on a disjoint universe — a
    /// stranger to this tape — and hand it prints it never makes.
    ///
    /// Applies to every publisher class uniformly, [`Publisher::PublicWs`] included; no `Sticky` venue
    /// has a public backstop today, and adding one needs this revisited.
    fn tape_arm_admits(
        &mut self,
        t: &NormalizedTrade,
        publisher: Publisher,
        category: &'static str,
    ) -> bool {
        // Interned, so the per-print key is two refcount bumps rather than an allocation. One key for
        // the tape gate and the authority alike: they must not disagree about what a universe is.
        let key: ScopeKey = (t.venue.clone(), category_arc(category));
        let elected = self.books.scope_leader(&key);
        let tracked = self.books.tracks_arm(&key, publisher);
        let Some(lead) = self.tape_leader.get_mut(&key) else {
            self.tape_leader.insert(
                key,
                TapeLead {
                    arm: publisher,
                    last_ns: t.recv_ts_ns,
                    honored_election: None,
                },
            );
            return true;
        };
        let transfer = lead.arm != publisher;
        if transfer {
            let displaces_uncorroborated = tracked && !self.books.tracks_arm(&key, lead.arm);
            let new_election = elected == Some(publisher) && lead.honored_election != elected;
            let silent = t.recv_ts_ns.saturating_sub(lead.last_ns) > NO_ID_TAPE_HANDOVER_NS;
            if !(displaces_uncorroborated || new_election || silent) {
                return false;
            }
            // The election in force as this arm took the tape. A silence handover deliberately
            // overrides the election, so marking it spent here is what stops the arm it named from
            // reclaiming on its next straggler print.
            lead.honored_election = elected;
            metrics()
                .tape_arm_transfers
                .with_label_values(&[t.venue.as_ref()])
                .inc();
        }
        lead.arm = publisher;
        lead.last_ns = t.recv_ts_ns;
        true
    }

    /// Claim the `(venue, symbol)` zero-id tape for `publisher`, returning whether a *concurrent*
    /// second emitter was detected. `Coordinated` venues only — see the `Trade` arm.
    fn claim_no_id_tape(&mut self, t: &NormalizedTrade, publisher: Publisher) -> bool {
        let key = (t.venue.clone(), t.symbol.clone());
        match self.no_id_owner.get_mut(&key) {
            // The owner printing again, or a challenger inheriting a tape that has gone quiet past
            // the handover window: either way one emitter, no conflict.
            Some((owner, last_ns)) => {
                let stale = t.recv_ts_ns.saturating_sub(*last_ns) > NO_ID_TAPE_HANDOVER_NS;
                let concurrent = *owner != publisher && !stale;
                if !concurrent {
                    *owner = publisher;
                    *last_ns = t.recv_ts_ns;
                }
                concurrent
            }
            None => {
                self.no_id_owner.insert(key, (publisher, t.recv_ts_ns));
                false
            }
        }
    }

    /// Pair this trade with the peer arm's copy and hand the signed lead to the authority — the only
    /// producer of the evidence [`StickyAuthority::close_window`] elects on.
    ///
    /// `scope` is the emitting row's `(venue, category)`: the election it feeds is per universe, so a
    /// lead measured between two of one universe's mirrors must not be filed against another's arms.
    fn observe_trade_race(&mut self, scope: &ScopeKey, t: &NormalizedTrade, publisher: Publisher) {
        let Some(m) = self.race.on_trade(
            scope,
            &t.symbol,
            t.price,
            t.size,
            t.aggressor_side,
            publisher,
            t.recv_ts_ns,
        ) else {
            return;
        };
        let Some(leader) = self.books.scope_leader(scope) else {
            return;
        };
        let Some((challenger, lead_ns)) = m.lead_for(leader) else {
            return;
        };
        self.books.observe_matched_lead(scope, challenger, lead_ns);
        // A matched pair has a real winner either way, which is what makes `{winner="challenger"}`
        // reachable — unlike `Admit::Contest`'s structurally non-negative phase.
        self.vm(&t.venue).arm_lead[usize::from(lead_ns < 0)].observe(lead_ns.unsigned_abs() as f64);
    }

    /// Close every elapsed arm-sampling window, then refresh the per-arm gauge and drain the matcher's
    /// unmatched counts. Driven by a periodic task on `--arb-sample-interval-secs`, **never per
    /// message**: `markets_held_all` is O(markets × arms).
    ///
    /// A margin transfer moves venue authority here; each affected market re-baselines on its next
    /// admitted batch rather than in a burst of `O(markets)` clears for markets the new arm may not
    /// even speak for (93 of 1,239 instruments saw any update at all in 39 s on the live sports feed).
    pub fn close_authority_windows(&mut self) {
        for ((venue, _category), _) in self.books.close_window(now_ns()) {
            metrics()
                .arm_transfers
                .with_label_values(&[venue.as_ref(), "margin"])
                .inc();
        }
        // Never `arm_ordinal` on either loop: labelling must not admit. `drain_unmatched`'s keys are
        // every publisher that sent a trade for the scope, so minting here would spend that scope's
        // eight admission slots on sources that never serve a book.
        //
        // The two lookups differ because their key sets do. `markets_held_all` is summed per venue
        // (the gauge is labelled `{venue, arm}`), so its label is resolved across the venue's
        // universes; the matcher is scope-keyed, so its label is the exact per-scope ordinal.
        for (venue, arm, held) in self.books.markets_held_all() {
            let label = self.books.arm_label_in_venue(&venue, arm);
            metrics()
                .arm_markets_held
                .with_label_values(&[venue.as_ref(), label])
                .set(held as i64);
        }
        for ((scope, arm), n) in self.race.drain_unmatched() {
            let label = self.books.arm_label(&scope, arm);
            metrics()
                .arm_unmatched_trades
                .with_label_values(&[scope.0.as_ref(), label])
                .inc_by(n);
        }
    }

    /// Apply the appropriate dedup and broadcast if the message survives it. `publisher` is the
    /// source racing for the quote floor's per-tick leadership; it is ignored for non-quote
    /// messages. The send result is ignored: a no-subscriber send desyncs no one, and a unique
    /// update dropped by a slow per-client channel is unrecoverable regardless.
    ///
    /// `category` is the emitting source's instrument **universe** (`ingest::feeds::Feed::category`,
    /// supplied by the caller because it is a property of the *row*, not of the message). Only the
    /// `Sticky` trade gate reads it, and it is deliberately a parameter rather than a field on the
    /// wire types: those serialize into the WebSocket JSON, which PROTOCOL.md fixes as a consumer
    /// contract, and a consumer has no use for a producer-side arbitration key.
    ///
    /// Metric children are pre-resolved per venue (see [`VenueMetrics`]) so this per-message path
    /// increments a cached handle rather than doing a label-map lookup for each counter.
    pub fn emit(&mut self, msg: FeedMessage, publisher: Publisher, category: &'static str) {
        match &msg {
            FeedMessage::Quote(q) => {
                // `source_ts == 0` is the "not available" sentinel (per CLAUDE.md, never a real
                // time): forward it but never let it touch the floor — as a tick it would pin
                // `high_water` at 0 and drop every later quote as stale.
                if q.source_ts_ns == 0 {
                    let vm = self.vm(&q.venue);
                    vm.quotes_no_source_ts.inc();
                    vm.emit[EMIT_QUOTE].inc();
                    let _ = self.tx.send(Arc::new(msg));
                    return;
                }
                // Reject an implausibly-far-future `source_ts` before it can advance the floor.
                // The floor is shared by the trusted edge and the untrusted public WS; one bad/
                // hostile public timestamp years ahead would otherwise latch `high_water` and drop
                // every real edge quote as stale until restart (see `MAX_FUTURE_SKEW_NS`).
                //
                // Compare against the quote's own arrival wall clock (`recv_ts_ns`, sampled at
                // receive) rather than sampling `now_ns()` again here — one fewer clock read per
                // quote on the hot path. Fall back to a fresh sample only if it was never stamped.
                let now = if q.recv_ts_ns != 0 {
                    q.recv_ts_ns
                } else {
                    now_ns()
                };
                if q.source_ts_ns > now.saturating_add(MAX_FUTURE_SKEW_NS) {
                    self.vm(&q.venue).quotes_future_rejected.inc();
                    return;
                }
                let key = (q.venue.clone(), q.symbol.clone());
                // `recv_ts_ns` is the cross-source-comparable arrival clock (host wall clock,
                // sampled for both the edge receiver and the public WS feeder).
                let decision =
                    self.quotes
                        .admit(key, q.source_ts_ns, QuoteId::of(q), publisher, q.recv_ts_ns);
                let vm = self.vm(&q.venue);
                match decision {
                    Admit::Emitted { opened_tick } => {
                        vm.emit[EMIT_QUOTE].inc();
                        // Attribute the admitted quote to its winning publisher. A rise in
                        // `publisher="public"` is the direct signal of the public backstop filling
                        // an edge gap (in steady state the edge publisher leads every tick).
                        vm.quotes_admitted[pub_idx(publisher)].inc();
                        // Once per tick: the class whose copy opened this `source_ts` won the
                        // tick. Unlike the contest histogram (in-tick head-to-heads only), every
                        // tick counts exactly once — this is the published win-rate primitive.
                        if opened_tick {
                            vm.quote_ticks_won[pub_idx(publisher)].inc();
                        }
                        let _ = self.tx.send(Arc::new(msg));
                    }
                    // A cross-source follower lost this tick: record how far the winner led, on top
                    // of the plain drop count. The losing copy is `publisher` (the non-leader at
                    // this tick) — labelling both ends keeps an edge-vs-edge mirror race out of the
                    // headline edge-vs-public margin (see `quote_lead_ns` docs).
                    Admit::Contest { winner, lead_ns } => {
                        vm.quotes_dropped.inc();
                        vm.quote_lead[pub_idx(winner) * 2 + pub_idx(publisher)]
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        vm.quotes_dropped.inc();
                    }
                }
            }
            FeedMessage::Trade(t) => {
                // Feed the cross-arm matcher BEFORE the `trade_id == 0` bypass returns: that sentinel
                // is exactly what a FIX-sourced arm prints, so a call below it would never see the arm
                // the election exists to judge.
                let scope: ScopeKey = (t.venue.clone(), category_arc(category));
                if self.race_eligible(&scope, publisher) {
                    self.observe_trade_race(&scope, t, publisher);
                }
                // Then the per-universe arm gate, for the same reason the `book` arm has one: a
                // `Sticky` universe's two arms mirror one tape with no shared identity to collapse
                // them on. Scoped by `(venue, category)`, not by venue — see `tape_arm_admits`.
                let sticky = self.mode_for(&t.venue) == ArbitrationMode::Sticky;
                if sticky && !self.tape_arm_admits(t, publisher, category) {
                    metrics()
                        .tape_arm_dropped
                        .with_label_values(&[t.venue.as_ref()])
                        .inc();
                    return;
                }
                // `trade_id == 0` is the "no venue trade id" sentinel (a FIX-sourced print has
                // none). Keying the window on it drops every later print for the key: `0` is
                // inserted once and never ages out (eviction is by insertion order), so every
                // subsequent `0` reads as a same-publisher duplicate. Forward unkeyed instead —
                // correct only while one publisher owns the venue's tape, so a *concurrent* second
                // emitter's prints (pure duplicates, nothing collapses them) are counted and logged
                // rather than silently doubling the tape, while a takeover of a tape that has gone
                // quiet is a handover. Not counted as *admitted*: nothing was, mirroring the
                // `source_ts == 0` quote bypass above.
                if t.trade_id == 0 {
                    // Skipped entirely on a `Sticky` venue: the gate above already enforced one
                    // emitter, and this latch — which knows nothing about the election — would report
                    // a gate-approved handover as a double-print on the one counter that has to stay
                    // trustworthy.
                    let conflict = !sticky && self.claim_no_id_tape(t, publisher);
                    let vm = self.vm(&t.venue);
                    vm.trades_no_id.inc();
                    vm.emit[EMIT_TRADE].inc();
                    if conflict {
                        vm.trades_no_id_conflict.inc();
                    }
                    if conflict && !self.no_id_conflict_logged {
                        self.no_id_conflict_logged = true;
                        warn!(
                            venue = %t.venue,
                            symbol = %t.symbol,
                            "a second publisher is emitting trades with no venue trade id: the \
                             tape is double-printing (one tape owner per venue is the sentinel \
                             bypass's precondition)"
                        );
                    }
                    let _ = self.tx.send(Arc::new(msg));
                    return;
                }
                let key = (t.venue.clone(), t.symbol.clone());
                let decision = self.trades.admit(key, t.trade_id, publisher, t.recv_ts_ns);
                let vm = self.vm(&t.venue);
                match decision {
                    Admit::Emitted { .. } => {
                        vm.emit[EMIT_TRADE].inc();
                        vm.trades_admitted[pub_idx(publisher)].inc();
                        let _ = self.tx.send(Arc::new(msg));
                    }
                    Admit::Contest { winner, lead_ns } => {
                        vm.trades_dropped.inc();
                        vm.trade_lead[pub_idx(winner) * 2 + pub_idx(publisher)]
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        vm.trades_dropped.inc();
                    }
                }
            }
            // Passthrough kinds (no dedup). Enumerated explicitly rather than via a catch-all so a
            // future `FeedMessage` variant is a compile error here, not a silent miss / runtime panic.
            FeedMessage::Instrument(i) => {
                // Mirrored publishers republish identical definitions every refdata burst: the
                // burst's first copy goes out, its mirrors collapse, and unchanged content is
                // re-announced once `INSTRUMENT_REANNOUNCE_NS` has elapsed (a rate limit, not a
                // latch — see that const). A precision change always goes out immediately.
                let id = InstrumentId {
                    price_exponent: i.price_exponent,
                    qty_exponent: i.qty_exponent,
                };
                let now = now_mono_ns();
                let key = (i.venue.clone(), i.channel, i.instrument_id);
                let forward = match self.instrument_defs.get(&key) {
                    Some((prev, last)) => {
                        *prev != id || now.saturating_sub(*last) >= INSTRUMENT_REANNOUNCE_NS
                    }
                    None => true,
                };
                if forward {
                    self.instrument_defs.insert(key, (id, now));
                }
                let vm = self.vm(&i.venue);
                if !forward {
                    vm.instruments_dropped.inc();
                    return;
                }
                vm.emit[EMIT_INSTRUMENT].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
            FeedMessage::Midpoint(mp) => {
                self.vm(&mp.venue).emit[EMIT_MIDPOINT].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
            FeedMessage::Depth(d) => {
                // Reject an implausibly-far-future `source_ts` before it can advance the floor — the
                // book event timestamp is venue/wire data and the source IP is spoofable, so one
                // forged far-future depth would otherwise latch `high_water` ahead and wedge depth
                // for that symbol until restart (mirrors the quote arm; see `MAX_FUTURE_SKEW_NS`).
                if d.source_ts_ns > now_ns().saturating_add(MAX_FUTURE_SKEW_NS) {
                    self.vm(&d.venue).depth_future_rejected.inc();
                    return;
                }
                // DELIBERATE divergence from the quote arm: depth is routed through the floor with
                // **no `source_ts == 0` bypass**. For quotes 0 is the "not available" sentinel that
                // must always forward; for depth 0 is a real state — the initial synced-but-empty
                // book each publisher emits right after its snapshot anchor — and the two publishers'
                // identical empty depths at `source_ts == 0` MUST collapse to one (the
                // content-inclusive depth oracle would otherwise flag them as duplicates). Routing 0
                // through `admit()` makes the non-leader's empty anchor a no-op. No wedge: a real
                // later event has `source_ts > 0` and re-advances the floor; only a perpetually-empty
                // book (no market data at all — nothing to serve) leaves the non-leader dropped, and
                // depth is full-state self-healing so nothing is lost.
                let key = (d.venue.clone(), d.symbol.clone());
                let decision =
                    self.depths
                        .admit(key, d.source_ts_ns, DepthId::of(d), publisher, d.recv_ts_ns);
                match decision {
                    Admit::Emitted { opened_tick } => {
                        let vm = self.vm(&d.venue);
                        vm.emit[EMIT_DEPTH].inc();
                        // Attribute the admitted depth to its winning publisher — the depth mirror of
                        // `quotes_admitted`. A rise for a given source shows which publisher currently
                        // leads the reconstructed book (and, were a public depth backstop ever added,
                        // `publisher="public"` would flag it filling an edge gap).
                        vm.depth_admitted[pub_idx(publisher)].inc();
                        // Once per tick, mirroring the quote arm's win-rate primitive.
                        if opened_tick {
                            vm.depth_ticks_won[pub_idx(publisher)].inc();
                        }
                        // Update the WS-replay snapshot with the leader's admitted book, so a client
                        // connecting mid-stream replays exactly what was broadcast (not a dropped
                        // non-leader's divergent copy).
                        if let Some(replay) = &self.depth_replay {
                            model::lock(replay)
                                .insert((d.venue.clone(), d.symbol.clone()), d.clone());
                        }
                        let _ = self.tx.send(Arc::new(msg));
                    }
                    // A cross-publisher follower lost this depth tick: record how far the winner led
                    // (the depth mirror of `quote_lead_ns`), on top of the drop count attributed to
                    // the losing publisher class (which source is *losing* the book race — the
                    // symmetric counterpart of `depth_admitted`'s winner attribution).
                    Admit::Contest { winner, lead_ns } => {
                        let vm = self.vm(&d.venue);
                        vm.depth_dropped[pub_idx(publisher)].inc();
                        vm.depth_lead[pub_idx(winner) * 2 + pub_idx(publisher)]
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        self.vm(&d.venue).depth_dropped[pub_idx(publisher)].inc();
                    }
                }
            }
            // The single-arm authority gate, in BOTH arbitration modes (no `mode_for` branch): a
            // `source_ts` tick can hold several deltas, so the quote floor's per-tick latch would
            // interleave two arms inside one logical event, and the arms' per-instrument delta series
            // are unrelated by construction — a consumer's book corrupts while every per-arm sequence
            // check the producer ran still passes.
            FeedMessage::Book(b) => {
                // The arbitration scope rides in on the key: one election per instrument universe,
                // never one per venue (see `authority`'s module doc — a venue-wide election drops a
                // disjoint universe's whole book stream).
                let scope: ScopeKey = (b.venue.clone(), category_arc(category));
                let key: MarketKey = (scope.0.clone(), scope.1.clone(), b.channel, b.instrument_id);
                // Eligibility first, exactly as `admit` applies it: an arm past the authority's
                // per-universe cap enters no map here either, so a forged source can neither be
                // served nor evict a real market's state.
                if self.books.arm_ordinal(&scope, publisher) == OTHER_ARM {
                    self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
                    return;
                }
                // An order-level market on a distributed venue races per venue event instead: every
                // publisher stamps the same `order_id`, so best-of-N is on offer here where a
                // price-aggregated book (whose arms share no identity at all) can only elect one arm.
                // Routed on the batch's own content, with the market's memo covering the batches that
                // carry no order id of their own (a bare `clear`) — so an evicted market re-routes on its
                // next order-level batch instead of reverting to the authority for good.
                let order_level = b.changes.iter().any(|c| c.order_id != 0)
                    || self.book_markets.get(&key).is_some_and(|m| m.order_level);
                if order_level && self.mode_for(&b.venue) == ArbitrationMode::Coordinated {
                    self.emit_order_level_book(key, b, publisher);
                    return;
                }
                // Then track the market before admitting it, so the authority's own wire-keyed
                // per-market map is bounded by `MAX_BOOK_MARKETS` too — it has no cap of its own, and
                // the instrument-resolves-to-a-book precondition bounds nothing upstream today.
                if !self.book_markets.contains_key(&key) {
                    self.track_book_market(&key);
                }
                let prev = self.books.last_admitted(&key);
                let leader_before = self.books.scope_leader(&scope);
                let decision = self.books.admit(key.clone(), publisher, b.recv_ts_ns);
                // Accumulate the arm's stream whether or not it was admitted: a transfer republishes
                // the new arm's current levels, which exist only if its copies were folded in all along.
                self.accumulate_book(&key, publisher, b);
                if !decision.emitted() {
                    self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
                    return;
                }
                let leader_after = self.books.scope_leader(&scope);
                if let Some(reason) = transfer_reason(prev, leader_before, leader_after, publisher)
                {
                    metrics()
                        .arm_transfers
                        .with_label_values(&[b.venue.as_ref(), reason])
                        .inc();
                }
                // Anything other than "the arm that last reached the wire for this market" means the
                // consumer's state cannot be assumed to continue from this batch: a serving-arm change,
                // a first admission, or a market whose state was evicted. All three re-baseline.
                let (mut rebaseline_now, mut abandoned) = (false, false);
                if let Some(m) = self.book_markets.get_mut(&key) {
                    m.rebaseline |= prev != Some(publisher);
                    if m.rebaseline {
                        // Wait for the new arm to close a logical event before republishing its book: a
                        // `to_book` of a half-applied one goes out stamped `last` as a torn book.
                        // Bounded, because `last` is a promise made by an unauthenticated producer.
                        if !b.last && m.withheld < MAX_WITHHELD_BATCHES {
                            m.withheld += 1;
                            self.vm(&b.venue).book_dropped[pub_idx(publisher)].inc();
                            return;
                        }
                        abandoned = !b.last;
                        m.rebaseline = false;
                        m.withheld = 0;
                        rebaseline_now = true;
                    }
                }
                if abandoned && !self.book_withhold_logged {
                    self.book_withhold_logged = true;
                    warn!(
                        venue = %b.venue, channel = b.channel, instrument = b.instrument_id,
                        "book batches carry no `last`: re-baselining on an unterminated event"
                    );
                }
                if rebaseline_now {
                    let re = self.rebaseline_book(&key, publisher);
                    self.vm(&b.venue).emit[EMIT_BOOK].inc();
                    match re {
                        // The arm's whole book, this batch included, so it replaces it on the wire.
                        Some(full) => {
                            let _ = self.tx.send(Arc::new(FeedMessage::Book(full)));
                            return;
                        }
                        // Nothing complete to republish: empty the consumer's book and let the batch
                        // below rebuild onto it.
                        None => {
                            let _ = self.tx.send(Arc::new(FeedMessage::Book(clear_only(b))));
                        }
                    }
                } else {
                    self.apply_book_replay(&key, b);
                }
                self.vm(&b.venue).emit[EMIT_BOOK].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
            // `Status` is currently never routed through `emit` — receivers send it straight via
            // `sender()` (see `emit_status`), and no other source produces it — so `dz_emit_total
            // {kind="status"}` is unreachable in practice today. The arm is kept for match
            // exhaustiveness and stays correct if a future source ever emits status through here.
            FeedMessage::Status(s) => {
                self.vm(&s.venue).emit[EMIT_STATUS].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
        }
    }
}

/// Process-wide handle to the one [`Arbiter`]: every multicast receiver task and the WS feeder hold
/// a clone and lock it for the brief admit-decision-plus-send critical section.
pub type SharedArbiter = Arc<Mutex<Arbiter>>;

/// Lock the shared arbiter, recovering the guard even if a previous holder panicked while holding it.
///
/// The emit critical section ([`Arbiter::emit`]) is panic-free by construction — it only does
/// `HashMap`/`HashSet` work and an ignored `broadcast::send` — so the protected dedup state is always
/// left consistent. Recovering from poisoning (rather than `.lock().unwrap()`) therefore keeps an
/// **unrelated** panic in any one ingest task from cascading into every other source: the multicast
/// receivers' hot path stays isolated from a WS-feeder fault, which is the failure-isolation contract.
pub fn lock(arbiter: &SharedArbiter) -> std::sync::MutexGuard<'_, Arbiter> {
    arbiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The category every test that is not *about* categories emits under. Sharing one is the point:
    /// the whole suite must behave exactly as it did before the tape gate was scoped, which is what
    /// makes the two category tests below evidence of the scope change and not of a rewrite.
    const TEST_CATEGORY: &str = "testcategory";

    #[test]
    fn quote_first_sample_admits() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 1000, 0, 1, 0).emitted()); // first for this key always emits (latches leader)
    }

    #[test]
    fn quote_new_tick_admits_and_relatches_leader() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 1000, 0, 1, 0).emitted()); // pub 1 opens tick -> leader
        assert!(f.admit("BTC", 1001, 0, 2, 0).emitted()); // newer tick re-latches to pub 2 (even identical content)
        assert!(f.admit("BTC", 2000, 0, 1, 0).emitted()); // newer tick again, leader back to pub 1
    }

    /// Latch-to-leader within one tick: the leader (first publisher to open the tick) emits its
    /// distinct contents in order; a *second* publisher's sample at the same `source_ts` is dropped
    /// even when its content is new. This is the false-uptick fix — on a falling price, admitting a
    /// slower publisher's older, higher sample as a "fresh" change would serve a stale value as the
    /// latest. Exact leader repeats are also dropped.
    #[test]
    fn quote_latches_to_leader_within_tick() {
        let (a, b) = (1u8, 2u8);
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        // Falling price within tick T; leader A observes 5, 4, 3 in its own (trustworthy) order:
        assert!(f.admit("BTC", 1000, 5, a, 0).emitted()); // opens tick -> A is leader, emit 5
        assert!(!f.admit("BTC", 1000, 5, a, 0).emitted()); // A's exact repeat dropped
        assert!(f.admit("BTC", 1000, 4, a, 0).emitted()); // A's next distinct content kept
        assert!(f.admit("BTC", 1000, 3, a, 0).emitted()); // A's next distinct content kept
                                                          // B (higher delay) samples 39 at the same tick, arriving last. DROPPED even though A never
                                                          // sent 39: its order relative to A is delay-corrupted, so emitting it risks a phantom tick.
        assert!(!f.admit("BTC", 1000, 39, b, 0).emitted());
        // A new tick re-opens the latch; whichever publisher gets there first leads it.
        assert!(f.admit("BTC", 1001, 39, b, 0).emitted()); // B opens the next tick -> B leads, emit
        assert!(!f.admit("BTC", 1001, 2, a, 0).emitted()); // A is now the non-leader at this tick -> dropped
    }

    #[test]
    fn quote_stale_tick_dropped_for_any_publisher() {
        let (a, b) = (1u8, 2u8);
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 2000, 1, a, 0).emitted());
        assert!(!f.admit("BTC", 1999, 9, a, 0).emitted()); // strictly older tick -> stale, dropped
        assert!(!f.admit("BTC", 1999, 9, b, 0).emitted()); // and from the other publisher too
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 2000, 0, 1, 0).emitted());
        assert!(f.admit("ETH", 1000, 0, 1, 0).emitted()); // separate floor + leader per key
        assert!(!f.admit("BTC", 1500, 0, 1, 0).emitted()); // BTC's floor unaffected by ETH
    }

    /// The per-tick content set is FIFO-bounded to `tick_cap` so a stalled `source_ts` can't grow it
    /// without limit. With cap 2, the oldest content is evicted and a recurrence re-admits.
    #[test]
    fn quote_tick_set_is_capacity_bounded() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(2);
        assert!(f.admit("BTC", 1000, 1, 9, 0).emitted()); // tick window {1}
        assert!(f.admit("BTC", 1000, 2, 9, 0).emitted()); // {1,2}
        assert!(f.admit("BTC", 1000, 3, 9, 0).emitted()); // {2,3} — content 1 evicted (cap 2)
        assert!(!f.admit("BTC", 1000, 3, 9, 0).emitted()); // 3 still in the set -> dup dropped
        assert!(f.admit("BTC", 1000, 1, 9, 0).emitted()); // 1 fell out of the cap window -> re-admitted
    }

    /// A new tick's first cross-publisher follower is reported as a `Contest` with the lead time;
    /// later followers of the same tick drop silently (one contest sample per tick).
    #[test]
    fn quote_contest_reports_leader_and_lead_once() {
        let (a, b) = (1u8, 2u8);
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 1000, 5, a, 100).emitted()); // A opens tick at t=100
                                                            // B's first copy of this tick arrives at t=150 -> contest, A led by 50.
        assert_eq!(
            f.admit("BTC", 1000, 9, b, 150),
            Admit::Contest {
                winner: a,
                lead_ns: 50
            }
        );
        // A second B follower of the same tick is just a drop (contest already counted).
        assert_eq!(f.admit("BTC", 1000, 7, b, 170), Admit::Dropped);
    }

    #[test]
    fn trade_new_admitted_repeat_dropped() {
        let mut d: WindowedDedup<&str, u64, u8> = WindowedDedup::new(8);
        assert!(d.admit("BTC", 1, 1, 0).emitted());
        // A competing publisher's copy of the same id -> a cross-source contest (the loser), the
        // first publisher led by the arrival delta.
        assert_eq!(
            d.admit("BTC", 1, 2, 40),
            Admit::Contest {
                winner: 1,
                lead_ns: 40
            }
        );
        assert!(d.admit("BTC", 2, 1, 0).emitted());
    }

    #[test]
    fn trade_keys_independent_and_window_evicts() {
        let mut d: WindowedDedup<&str, u64, u8> = WindowedDedup::new(2);
        assert!(d.admit("BTC", 1, 1, 0).emitted());
        assert!(d.admit("ETH", 1, 1, 0).emitted()); // same id, different key
        assert!(d.admit("BTC", 2, 1, 0).emitted());
        assert!(d.admit("BTC", 3, 1, 0).emitted()); // window {2,3}; id 1 evicted
        assert!(d.admit("BTC", 1, 1, 0).emitted()); // 1 fell out of the window -> treated as new
    }

    use std::net::{IpAddr, Ipv4Addr};

    use crate::model::{BookReplay, NormalizedQuote, NormalizedTrade, Side};

    fn quote(source_ts_ns: u64, bid: f64, ask: f64) -> NormalizedQuote {
        NormalizedQuote {
            venue: "HYPERLIQUID".into(),
            source: "HYPERLIQUID".into(),
            source_id: 0,
            symbol: "BTC".into(),
            bid,
            ask,
            bid_size: 1.0,
            ask_size: 2.0,
            bid_n: 0,
            ask_n: 0,
            source_ts_ns,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// Drain every emitted quote's `(source_ts, bid)` from a receiver.
    fn drain_quotes(rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>) -> Vec<(u64, f64)> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Quote(q) = &*m {
                out.push((q.source_ts_ns, q.bid));
            }
        }
        out
    }

    /// `QuoteId` distinguishes distinct BBOs and equates identical ones, so the floor drops a
    /// source's own exact `(source_ts, content)` republish through the arbiter's emit path.
    #[test]
    fn arbiter_emit_drops_same_source_exact_repeat() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        );
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        ); // exact repeat -> dropped
        a.emit(
            FeedMessage::Quote(quote(1000, 100.5, 101.0)),
            edge,
            TEST_CATEGORY,
        ); // new content same tick -> kept
        assert_eq!(drain_quotes(&mut rx), vec![(1000, 100.0), (1000, 100.5)]);
    }

    /// The backstop in miniature: with the edge publisher leading a tick, the public WS copy of the
    /// same `source_ts` loses the race and is dropped as a non-leader no-op — even though its content
    /// (here) differs. When the edge gaps (no edge sample opens the next tick), the public copy is
    /// the first to cross the floor and is emitted.
    #[test]
    fn arbiter_public_loses_to_edge_then_fills_gap() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        // Steady state: edge opens tick 1000, public's copy at the same tick is dropped.
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        );
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        );
        // Edge gaps: the public feed opens the next tick and fills in.
        a.emit(
            FeedMessage::Quote(quote(1001, 100.2, 101.2)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        );
        assert_eq!(drain_quotes(&mut rx), vec![(1000, 100.0), (1001, 100.2)]);
    }

    /// Trades dedup by `trade_id` through the arbiter regardless of which source delivered them, so
    /// a public copy of an edge trade is a no-op.
    #[test]
    fn arbiter_trade_dedup_across_sources() {
        use crate::model::NormalizedTrade;
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let trade = |id: u64| {
            FeedMessage::Trade(NormalizedTrade {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 0,
                symbol: "BTC".into(),
                channel: 0,
                instrument_id: 0,
                category: "default".into(),
                price: 100.0,
                size: 1.0,
                aggressor_side: Side::Buy,
                trade_id: id,
                cumulative_volume: 0.0,
                source_ts_ns: 1,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            })
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(trade(7), edge, TEST_CATEGORY);
        a.emit(trade(7), Publisher::PublicWs, TEST_CATEGORY); // same id from public -> dropped
        a.emit(trade(8), Publisher::PublicWs, TEST_CATEGORY);
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Trade(t) = &*m {
                ids.push(t.trade_id);
            }
        }
        assert_eq!(ids, vec![7, 8]);
    }

    fn trade(trade_id: u64) -> NormalizedTrade {
        NormalizedTrade {
            venue: "KALSHI".into(),
            source: "KALSHI".into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: 0,
            instrument_id: 0,
            category: "default".into(),
            price: 0.62,
            size: 100.0,
            aggressor_side: Side::Buy,
            trade_id,
            cumulative_volume: 0.0,
            source_ts_ns: 1_000,
            recv_ts_ns: 2_000,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// A FIX-sourced publisher has no venue trade id and stamps every print `trade_id == 0`.
    /// Keying the window on `0` collapses the tape to a single print forever (`0` is inserted
    /// once and never evicted), so `0` must mean "no identity" and bypass the window entirely.
    #[test]
    fn zero_trade_id_bypasses_the_window() {
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let p = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        for _ in 0..5 {
            a.emit(FeedMessage::Trade(trade(0)), p, TEST_CATEGORY);
        }
        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 5, "every zero-id print must be emitted");
    }

    /// A bypassed `0` has no window to collapse against, so a second publisher's zero-id prints
    /// double the tape. They are still forwarded — dropping one is an authority decision, not a
    /// dedup one — but `dz_trades_no_id_conflict_total` reports it, and one publisher's own repeats
    /// never do. Venue is unique to this test; the metrics registry is process-global.
    #[test]
    fn second_publisher_zero_id_tape_is_reported_as_a_conflict() {
        let venue = "NoIdTapeConflict";
        let t = |p| {
            let mut tr = trade(0);
            tr.venue = venue.into();
            (FeedMessage::Trade(tr), p)
        };
        let a1 = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let a2 = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let conflicts = metrics().trades_no_id_conflict.with_label_values(&[venue]);

        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let (m, p) = t(a1);
        a.emit(m, p, TEST_CATEGORY);
        let (m, p) = t(a1);
        a.emit(m, p, TEST_CATEGORY); // the owner's own repeat is not a conflict
        assert_eq!(conflicts.get(), 0);

        let (m, p) = t(a2);
        a.emit(m, p, TEST_CATEGORY);
        assert_eq!(
            conflicts.get(),
            1,
            "second publisher's tape must be reported"
        );

        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(
            seen, 3,
            "a conflicting print is still forwarded, not dropped"
        );
    }

    /// Tape ownership is not a permanent latch. A challenger taking over a tape that has gone quiet
    /// past [`NO_ID_TAPE_HANDOVER_NS`] is a failover — a dead arm, an authority transfer, the
    /// reconciler moving the tape between a venue's feed rows — and reporting that as a double-print
    /// would pin the conflict counter non-zero for the life of the process, on the one signal that
    /// has to stay trustworthy. Venue is unique to this test; the metrics registry is process-global.
    #[test]
    fn a_quiet_zero_id_tape_hands_over_without_a_conflict() {
        let venue = "NoIdTapeHandover";
        let t = |p, recv_ts_ns| {
            let mut tr = trade(0);
            tr.venue = venue.into();
            tr.recv_ts_ns = recv_ts_ns;
            (FeedMessage::Trade(tr), p)
        };
        let a1 = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let a2 = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let conflicts = metrics().trades_no_id_conflict.with_label_values(&[venue]);

        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let (m, p) = t(a1, 1_000);
        a.emit(m, p, TEST_CATEGORY);

        // Exactly at the window: a2 is still a concurrent second emitter, and a rejected challenger
        // must not refresh the incumbent's clock (or a burst of them would hold the tape open).
        let (m, p) = t(a2, 1_000 + NO_ID_TAPE_HANDOVER_NS);
        a.emit(m, p, TEST_CATEGORY);
        assert_eq!(conflicts.get(), 1, "not yet past the window");

        // Past it: a2 inherits the tape...
        let (m, p) = t(a2, 1_001 + NO_ID_TAPE_HANDOVER_NS);
        a.emit(m, p, TEST_CATEGORY);
        assert_eq!(conflicts.get(), 1, "a quiet tape hands over");

        // ...and keeps it, so its own later prints are its own.
        let (m, p) = t(a2, 1_002 + NO_ID_TAPE_HANDOVER_NS);
        a.emit(m, p, TEST_CATEGORY);
        assert_eq!(
            conflicts.get(),
            1,
            "the new owner's prints are not a conflict"
        );

        // The previous owner returning while a2 is live is a conflict again.
        let (m, p) = t(a1, 1_003 + NO_ID_TAPE_HANDOVER_NS);
        a.emit(m, p, TEST_CATEGORY);
        assert_eq!(conflicts.get(), 2, "two live emitters still conflict");
    }

    /// The bypass must not weaken dedup for prints that DO carry an id.
    #[test]
    fn nonzero_trade_id_still_dedupes() {
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let p = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        for _ in 0..5 {
            a.emit(FeedMessage::Trade(trade(77)), p, TEST_CATEGORY);
        }
        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 1, "a repeated id is still a duplicate");
    }

    /// Two byte-for-byte identical quote packets from the *same* multicast publisher collapse to a
    /// single emission: the second is an exact `(source_ts, content)` repeat the floor drops. This
    /// isolates the pure duplicate-packet case (no third distinct quote).
    #[test]
    fn duplicate_quote_packet_from_same_source_emitted_once() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        );
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        ); // identical duplicate -> dropped
        assert_eq!(drain_quotes(&mut rx), vec![(1000, 100.0)]);
    }

    /// The same BBO at the same `source_ts` mirrored by two distinct multicast publishers collapses
    /// to one emission: the first publisher to open the tick leads it, and the second's identical
    /// copy is a non-leader no-op. This is the cross-source duplicate-packet case.
    #[test]
    fn duplicate_quote_from_two_multicast_publishers_emitted_once() {
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            pub_a,
            TEST_CATEGORY,
        ); // A opens the tick -> emit
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            pub_b,
            TEST_CATEGORY,
        ); // B's mirror -> non-leader, dropped
        assert_eq!(drain_quotes(&mut rx), vec![(1000, 100.0)]);
    }

    /// Two identical trade packets (same `trade_id`) from the same source collapse to one emission
    /// via the windowed dedup, regardless of any other field.
    #[test]
    fn duplicate_trade_packet_from_same_source_emitted_once() {
        use crate::model::NormalizedTrade;
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let trade = || {
            FeedMessage::Trade(NormalizedTrade {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 0,
                symbol: "BTC".into(),
                channel: 0,
                instrument_id: 0,
                category: "default".into(),
                price: 100.0,
                size: 1.0,
                aggressor_side: Side::Buy,
                trade_id: 42,
                cumulative_volume: 0.0,
                source_ts_ns: 1,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            })
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(trade(), edge, TEST_CATEGORY);
        a.emit(trade(), edge, TEST_CATEGORY); // identical duplicate -> dropped
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Trade(t) = &*m {
                ids.push(t.trade_id);
            }
        }
        assert_eq!(ids, vec![42]);
    }

    /// A single implausibly-far-future quote (a bad/hostile public timestamp) must NOT advance the
    /// shared floor and wedge the symbol: it is dropped, and a later real edge quote still emits.
    /// (PR review finding: one bad public `time` would otherwise latch `high_water` years ahead and
    /// drop every real edge quote as stale until restart.)
    #[test]
    fn arbiter_future_timestamp_does_not_wedge_the_floor() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        let now = crate::model::now_ns();
        let bogus_future = now + 3_600_000_000_000; // 1h ahead -> rejected before touching the floor
        a.emit(
            FeedMessage::Quote(quote(bogus_future, 1.0, 2.0)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        );
        // The real edge quote (at ~now) is not stale relative to the floor and still emits.
        a.emit(
            FeedMessage::Quote(quote(now, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        );
        assert_eq!(drain_quotes(&mut rx), vec![(now, 100.0)]);
    }

    /// `source_ts == 0` (the "not available" sentinel) bypasses the floor: it is emitted but never
    /// latched, so it can't pin `high_water` at 0 and drop later quotes / non-leaders forever.
    #[test]
    fn arbiter_zero_source_ts_bypasses_floor() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Quote(quote(0, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        ); // bypass -> emitted, floor untouched
        a.emit(
            FeedMessage::Quote(quote(0, 100.5, 101.0)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        ); // also bypass -> emitted
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            edge,
            TEST_CATEGORY,
        ); // real tick still emits
        assert_eq!(
            drain_quotes(&mut rx),
            vec![(0, 100.0), (0, 100.5), (1000, 100.0)]
        );
    }

    /// The canonical `QuoteId` collapses two `f64` encodings of the same economic price — the edge's
    /// `raw * 10^exp` and a parsed public float, which are not bit-identical — onto one identity, so
    /// a cross-source copy dedups. (Raw `f64` bits would treat them as distinct.)
    #[test]
    fn quote_id_canonicalizes_equivalent_float_encodings() {
        let edge_px = 6788_f64 * 10f64.powi(-1); // 678.8 via raw*10^exp
        let parsed_px = 678.8_f64; // 678.8 parsed straight from JSON
        let a = QuoteId::of(&quote(1000, edge_px, 999.0));
        let b = QuoteId::of(&quote(1000, parsed_px, 999.0));
        assert_eq!(
            a, b,
            "same economic price must share one canonical identity"
        );
        // A genuinely different price is still distinct.
        assert_ne!(a, QuoteId::of(&quote(1000, 678.9, 999.0)));
    }

    /// End-to-end through `emit`: a cross-source quote contest must reach the lead-time histogram
    /// (attributed to the right `winner`/`loser` child) and bump the drop counter — not just return
    /// `Admit::Contest`. Keyed on a venue unique to this test so its metric children start at 0 and
    /// no parallel test touches them, so the absolute counts are assertable without `#[serial]`.
    #[test]
    fn arbiter_emit_records_quote_contest_into_lead_histogram() {
        let venue = "ArbiterQuoteContestMetricTest";
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mk = |source_ts: u64, recv: u64, bid: f64| {
            let mut q = quote(source_ts, bid, 101.0);
            q.venue = venue.into();
            q.recv_ts_ns = recv;
            FeedMessage::Quote(q)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        // Edge opens tick 1000 arriving at t=100; the public copy at the same tick arrives at t=150
        // -> contest, edge led the public copy by 50ns.
        a.emit(mk(1000, 100, 100.0), edge, TEST_CATEGORY);
        a.emit(mk(1000, 150, 100.5), Publisher::PublicWs, TEST_CATEGORY);

        let m = metrics();
        let edge_beats_public = m
            .quote_lead_ns
            .with_label_values(&[venue, "edge", "public"]);
        assert_eq!(
            edge_beats_public.get_sample_count(),
            1,
            "the contest must reach the edge-vs-public histogram"
        );
        assert_eq!(
            edge_beats_public.get_sample_sum() as u64,
            50,
            "the observed lead is the arrival delta"
        );
        assert_eq!(
            m.quotes_dropped.with_label_values(&[venue]).get(),
            1,
            "the losing copy must also count as a drop"
        );
        // M1: the mirror-race child stays empty — this contest was edge-vs-public, not edge-vs-edge.
        assert_eq!(
            m.quote_lead_ns
                .with_label_values(&[venue, "edge", "edge"])
                .get_sample_count(),
            0
        );
    }

    /// The edge-vs-edge mirror race lands in its own histogram child (`winner=edge,loser=edge`),
    /// kept out of the headline `loser="public"` margin — the M1 fix.
    #[test]
    fn arbiter_emit_separates_edge_mirror_race_from_public_margin() {
        let venue = "ArbiterMirrorRaceMetricTest";
        let mirror_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mirror_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let mk = |recv: u64, bid: f64| {
            let mut q = quote(1000, bid, 101.0);
            q.venue = venue.into();
            q.recv_ts_ns = recv;
            FeedMessage::Quote(q)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk(100, 100.0), mirror_a, TEST_CATEGORY); // A opens the tick
        a.emit(mk(120, 100.0), mirror_b, TEST_CATEGORY); // B's mirror copy loses by 20ns

        let m = metrics();
        let mirror_race = m.quote_lead_ns.with_label_values(&[venue, "edge", "edge"]);
        assert_eq!(mirror_race.get_sample_count(), 1);
        assert_eq!(mirror_race.get_sample_sum() as u64, 20);
        // The edge-vs-public child is untouched: this was a mirror race.
        assert_eq!(
            m.quote_lead_ns
                .with_label_values(&[venue, "edge", "public"])
                .get_sample_count(),
            0
        );
    }

    /// End-to-end through `emit`: a cross-source trade contest reaches the trade lead-time histogram
    /// and the drop counter (the trade-side mirror of the quote test above).
    #[test]
    fn arbiter_emit_records_trade_contest_into_lead_histogram() {
        use crate::model::NormalizedTrade;
        let venue = "ArbiterTradeContestMetricTest";
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let trade = |recv: u64| {
            FeedMessage::Trade(NormalizedTrade {
                venue: venue.into(),
                source: venue.into(),
                source_id: 0,
                symbol: "BTC".into(),
                channel: 0,
                instrument_id: 0,
                category: "default".into(),
                price: 100.0,
                size: 1.0,
                aggressor_side: Side::Buy,
                trade_id: 7,
                cumulative_volume: 0.0,
                source_ts_ns: 1,
                recv_ts_ns: recv,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            })
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(trade(100), edge, TEST_CATEGORY); // edge delivers id 7 first at t=100
        a.emit(trade(175), Publisher::PublicWs, TEST_CATEGORY); // public's copy loses by 75ns

        let m = metrics();
        let edge_beats_public = m
            .trade_lead_ns
            .with_label_values(&[venue, "edge", "public"]);
        assert_eq!(edge_beats_public.get_sample_count(), 1);
        assert_eq!(edge_beats_public.get_sample_sum() as u64, 75);
        assert_eq!(m.trades_dropped.with_label_values(&[venue]).get(), 1);
    }

    use crate::model::NormalizedDepth;

    fn depth(source_ts_ns: u64, bids: Vec<[f64; 2]>, asks: Vec<[f64; 2]>) -> NormalizedDepth {
        NormalizedDepth {
            venue: "HYPERLIQUID".into(),
            source: "HYPERLIQUID".into(),
            source_id: 0,
            symbol: "BTC".into(),
            bids,
            asks,
            source_ts_ns,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// Drain every emitted depth's `(source_ts, top bid px)` from a receiver (0.0 if no bid).
    fn drain_depths(rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>) -> Vec<(u64, f64)> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Depth(d) = &*m {
                out.push((d.source_ts_ns, d.bids.first().map(|l| l[0]).unwrap_or(0.0)));
            }
        }
        out
    }

    /// The two initial synced-but-empty depths two publishers emit at `source_ts == 0` (the empty
    /// book anchor) collapse to ONE. Unlike the quote arm, `source_ts == 0` is NOT bypassed: the
    /// leader's empty anchor is emitted and the non-leader's identical empty anchor is dropped, so the
    /// content-inclusive depth oracle never sees the duplicate `(0, [], [])`.
    #[test]
    fn arbiter_depth_empty_anchor_at_zero_collapses() {
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Depth(depth(0, vec![], vec![])),
            pub_a,
            TEST_CATEGORY,
        ); // A opens tick 0 -> emit
        a.emit(
            FeedMessage::Depth(depth(0, vec![], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // B's identical anchor -> dropped
           // A real later event re-advances the floor (no wedge from the latched 0 tick).
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        );
        assert_eq!(drain_depths(&mut rx), vec![(0, 0.0), (1000, 100.0)]);
    }

    /// Latch-to-leader for depth: the leader (first publisher to open a tick) emits; a non-leader
    /// publisher's depth at the same `source_ts` is dropped even when its book content differs
    /// (independent reconstructions can diverge at one event ts — the leader's book is served, the
    /// divergent copy is never both-emitted). A new tick re-latches.
    #[test]
    fn arbiter_depth_latches_to_leader_within_tick() {
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_a,
            TEST_CATEGORY,
        ); // A leads tick 1000
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // B mirror -> dropped
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[99.0, 1.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // B divergent same tick -> still dropped (non-leader)
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[101.0, 1.0]], vec![])),
            pub_a,
            TEST_CATEGORY,
        ); // A's own new content same tick -> kept
        a.emit(
            FeedMessage::Depth(depth(1001, vec![[102.0, 1.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // B opens the next tick -> leads, kept
        assert_eq!(
            drain_depths(&mut rx),
            vec![(1000, 100.0), (1000, 101.0), (1001, 102.0)]
        );
    }

    /// A strictly-older depth tick is stale and dropped for any publisher (the floor's `source_ts`
    /// never goes backwards on the wire).
    #[test]
    fn arbiter_depth_stale_tick_dropped() {
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Depth(depth(2000, vec![[100.0, 1.0]], vec![])),
            pub_a,
            TEST_CATEGORY,
        );
        a.emit(
            FeedMessage::Depth(depth(1999, vec![[99.0, 1.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // older tick -> stale, dropped
        assert_eq!(drain_depths(&mut rx), vec![(2000, 100.0)]);
    }

    /// A single implausibly-far-future depth (a forged/hostile source_ts) must NOT advance the floor
    /// and wedge the symbol: it is rejected, and a later real depth still emits.
    #[test]
    fn arbiter_depth_future_timestamp_does_not_wedge_the_floor() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        let now = crate::model::now_ns();
        a.emit(
            FeedMessage::Depth(depth(now + 3_600_000_000_000, vec![[1.0, 1.0]], vec![])),
            edge,
            TEST_CATEGORY,
        ); // 1h ahead -> rejected
        a.emit(
            FeedMessage::Depth(depth(now, vec![[100.0, 1.0]], vec![])),
            edge,
            TEST_CATEGORY,
        );
        assert_eq!(drain_depths(&mut rx), vec![(now, 100.0)]);
    }

    /// End-to-end through `emit`: a cross-publisher depth contest reaches the depth lead-time
    /// histogram and the drop counter (the depth mirror of the quote/trade contest tests). Keyed on a
    /// venue unique to this test so its metric children start at 0 without `#[serial]`.
    #[test]
    fn arbiter_emit_records_depth_contest_into_lead_histogram() {
        let venue = "ArbiterDepthContestMetricTest";
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let mk = |recv: u64, bid: f64| {
            let mut d = depth(1000, vec![[bid, 1.0]], vec![]);
            d.venue = venue.into();
            d.recv_ts_ns = recv;
            FeedMessage::Depth(d)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        // A opens tick 1000 at t=200; B's copy of the same tick arrives at t=290 -> contest, A led 90.
        a.emit(mk(200, 100.0), pub_a, TEST_CATEGORY);
        a.emit(mk(290, 100.5), pub_b, TEST_CATEGORY);

        let m = metrics();
        let a_beats_b = m.depth_lead_ns.with_label_values(&[venue, "edge", "edge"]);
        assert_eq!(a_beats_b.get_sample_count(), 1);
        assert_eq!(a_beats_b.get_sample_sum() as u64, 90);
        // The drop is attributed to the losing publisher's class (here the losing mirror is also
        // "edge") — who is *losing* the book race, the counterpart of depth_admitted's winner.
        assert_eq!(m.depth_dropped.with_label_values(&[venue, "edge"]).get(), 1);
    }

    /// The WS-replay map records the LEADER's admitted book, never a dropped non-leader's divergent
    /// copy. Pins the review fix: the replay snapshot must match what was broadcast, so a client
    /// connecting mid-stream never bootstraps from a book that never crossed the floor.
    #[test]
    fn arbiter_depth_replay_records_leader_not_dropped_nonleader() {
        let pub_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let pub_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, _rx) = broadcast::channel(64);
        let replay: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut a = Arbiter::new(tx, 8);
        a.set_depth_replay(replay.clone());
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_a,
            TEST_CATEGORY,
        ); // A leads tick 1000 -> admitted, recorded
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[99.0, 2.0]], vec![])),
            pub_b,
            TEST_CATEGORY,
        ); // B's divergent copy at same tick -> dropped, must NOT overwrite replay
        let map = model::lock(&replay);
        let entry = map
            .get(&("HYPERLIQUID".into(), "BTC".into()))
            .expect("leader depth recorded in replay map");
        assert_eq!(
            entry.bids,
            vec![[100.0, 1.0]],
            "replay map must hold the leader's book, not the dropped non-leader's"
        );
    }

    /// The floor resets purge the matching WS-replay entries: a client connecting across a
    /// session boundary must not be replayed the ended session's final book — and for an
    /// instrument the new session never re-lists, nothing else would ever remove the entry. The
    /// symbol reset purges exactly its key; the venue reset purges only that venue's entries.
    #[test]
    fn arbiter_depth_floor_reset_purges_replay_entries() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mk = |venue: &str, symbol: &str| {
            let mut d = depth(1000, vec![[100.0, 1.0]], vec![]);
            d.venue = venue.into();
            d.symbol = symbol.into();
            FeedMessage::Depth(d)
        };
        let key =
            |venue: &str, symbol: &str| -> (Arc<str>, Arc<str>) { (venue.into(), symbol.into()) };
        let (tx, _rx) = broadcast::channel(64);
        let replay: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut a = Arbiter::new(tx, 8);
        a.set_depth_replay(replay.clone());
        a.emit(mk("VenueA", "BTC"), edge, TEST_CATEGORY);
        a.emit(mk("VenueA", "ETH"), edge, TEST_CATEGORY);
        a.emit(mk("VenueB", "BTC"), edge, TEST_CATEGORY);
        assert_eq!(model::lock(&replay).len(), 3);

        a.reset_depth_floor_for_symbol("VenueA", "BTC", "instrument_reset");
        {
            let map = model::lock(&replay);
            assert!(!map.contains_key(&key("VenueA", "BTC")), "reset key purged");
            assert!(
                map.contains_key(&key("VenueA", "ETH")),
                "sibling symbol kept"
            );
            assert!(map.contains_key(&key("VenueB", "BTC")), "other venue kept");
        }

        a.reset_depth_floor_for_venue("VenueA", "end_of_session");
        {
            let map = model::lock(&replay);
            assert!(
                !map.contains_key(&key("VenueA", "ETH")),
                "venue's entries purged"
            );
            assert!(map.contains_key(&key("VenueB", "BTC")), "other venue kept");
        }
    }

    /// The session-reset escape hatch: a venue that restarts its event clock below the latched
    /// high-water would have every later depth dropped as stale forever; clearing the venue's floor
    /// entries (what the MBO processor does on `EndOfSession`) re-opens the tick so the lower
    /// `source_ts` is admitted. Also pins the cleared-entry count reaching
    /// `dz_depth_floor_resets_total{venue, reason}` (venue unique to this test).
    #[test]
    fn arbiter_depth_session_reset_readmits_lower_tick() {
        let venue = "ArbiterDepthSessionResetTest";
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mk = |ts: u64, bid: f64| {
            let mut d = depth(ts, vec![[bid, 1.0]], vec![]);
            d.venue = venue.into();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk(5000, 100.0), edge, TEST_CATEGORY); // latches high_water at 5000
        a.emit(mk(100, 99.0), edge, TEST_CATEGORY); // post-restart lower tick -> stale, dropped (the wedge)
        a.reset_depth_floor_for_venue(venue, "end_of_session");
        a.emit(mk(100, 99.0), edge, TEST_CATEGORY); // floor cleared -> re-opens the tick, admitted
        let ts: Vec<u64> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = &*m {
                    out.push(d.source_ts_ns);
                }
            }
            out
        };
        assert_eq!(ts, vec![5000, 100]);
        assert_eq!(
            metrics()
                .depth_floor_resets
                .with_label_values(&[venue, "end_of_session"])
                .get(),
            1,
            "one latched entry was cleared"
        );
    }

    /// A venue-wide floor reset touches only that venue's entries: another venue's latched floor
    /// still drops its stale ticks.
    #[test]
    fn arbiter_depth_session_reset_is_venue_scoped() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mk = |venue: &str, ts: u64| {
            let mut d = depth(ts, vec![[100.0, 1.0]], vec![]);
            d.venue = venue.into();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk("VenueA", 5000), edge, TEST_CATEGORY);
        a.emit(mk("VenueB", 5000), edge, TEST_CATEGORY);
        a.reset_depth_floor_for_venue("VenueA", "end_of_session");
        a.emit(mk("VenueA", 100), edge, TEST_CATEGORY); // cleared -> admitted
        a.emit(mk("VenueB", 100), edge, TEST_CATEGORY); // untouched -> still stale, dropped
        let seen: Vec<(String, u64)> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = &*m {
                    out.push((d.venue.to_string(), d.source_ts_ns));
                }
            }
            out
        };
        assert_eq!(
            seen,
            vec![
                ("VenueA".to_string(), 5000),
                ("VenueB".to_string(), 5000),
                ("VenueA".to_string(), 100),
            ]
        );
    }

    /// The per-symbol reset (what the MBO processor does on `InstrumentReset`) clears only that
    /// `(venue, symbol)` entry: the resetting instrument's lower tick is re-admitted while a
    /// sibling symbol's floor stays latched.
    #[test]
    fn arbiter_depth_symbol_reset_clears_only_that_symbol() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let mk = |symbol: &str, ts: u64| {
            let mut d = depth(ts, vec![[100.0, 1.0]], vec![]);
            d.symbol = symbol.into();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk("BTC", 5000), edge, TEST_CATEGORY);
        a.emit(mk("ETH", 5000), edge, TEST_CATEGORY);
        a.reset_depth_floor_for_symbol("HYPERLIQUID", "BTC", "instrument_reset");
        a.emit(mk("BTC", 100), edge, TEST_CATEGORY); // cleared -> admitted
        a.emit(mk("ETH", 100), edge, TEST_CATEGORY); // untouched -> still stale, dropped
        let seen: Vec<(String, u64)> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = &*m {
                    out.push((d.symbol.to_string(), d.source_ts_ns));
                }
            }
            out
        };
        assert_eq!(
            seen,
            vec![
                ("BTC".to_string(), 5000),
                ("ETH".to_string(), 5000),
                ("BTC".to_string(), 100),
            ]
        );
    }

    /// The canonical fixed-point is `i128`: with `i64`, any qty above ~9.2e10 saturated the
    /// float→int cast to `i64::MAX`, so two genuinely distinct huge quantities collapsed to one
    /// identity and the second was wrongly deduped (issue #66 item 2).
    #[test]
    fn depth_id_distinguishes_quantities_beyond_i64_saturation() {
        let a = DepthId::of(&depth(1000, vec![[100.0, 1.0e11]], vec![]));
        let b = DepthId::of(&depth(1000, vec![[100.0, 2.0e11]], vec![]));
        assert_ne!(a, b, "distinct huge quantities must not collapse");
        // And equal content still shares one identity.
        assert_eq!(a, DepthId::of(&depth(1000, vec![[100.0, 1.0e11]], vec![])));
    }

    /// Same guard for `QuoteId` (it shares the canonicalization convention).
    #[test]
    fn quote_id_distinguishes_sizes_beyond_i64_saturation() {
        let mut qa = quote(1000, 100.0, 101.0);
        qa.bid_size = 1.0e11;
        let mut qb = qa.clone();
        qb.bid_size = 2.0e11;
        assert_ne!(QuoteId::of(&qa), QuoteId::of(&qb));
    }

    /// The floor reports whether an emitted sample *opened* its `source_ts` tick — the
    /// once-per-tick first-delivery signal the tick-won counters publish. The leader's later
    /// in-tick contents emit without re-opening, and a follower's copy never opens a tick it lost.
    #[test]
    fn floor_reports_tick_open_once_per_tick() {
        let (a, b) = (1u8, 2u8);
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert_eq!(
            f.admit("BTC", 1000, 5, a, 0),
            Admit::Emitted { opened_tick: true } // first sample opens the tick
        );
        assert_eq!(
            f.admit("BTC", 1000, 4, a, 10),
            Admit::Emitted { opened_tick: false } // leader's in-tick content: same tick, no open
        );
        assert_eq!(
            f.admit("BTC", 1000, 9, b, 20),
            Admit::Contest {
                winner: a,
                lead_ns: 20
            } // follower never opens a lost tick
        );
        assert_eq!(
            f.admit("BTC", 2000, 9, b, 30),
            Admit::Emitted { opened_tick: true } // next tick: first delivery wins it
        );
    }

    /// The windowed dedup has no tick concept: every first-seen identity is its own open (each
    /// admitted trade IS that event's first delivery), pinned so the shared `Admit` shape is
    /// deliberate.
    #[test]
    fn window_first_seen_is_always_an_open() {
        let mut d: WindowedDedup<&str, u64, u8> = WindowedDedup::new(8);
        assert_eq!(
            d.admit("BTC", 1, 1, 0),
            Admit::Emitted { opened_tick: true }
        );
        assert_eq!(
            d.admit("BTC", 2, 1, 0),
            Admit::Emitted { opened_tick: true }
        );
    }

    fn quote_at(venue: &str, source_ts_ns: u64, bid: f64) -> NormalizedQuote {
        NormalizedQuote {
            venue: venue.into(),
            ..quote(source_ts_ns, bid, bid + 1.0)
        }
    }

    /// Tick-won attribution (`dz_quote_ticks_won_total`): every quote tick counts exactly once,
    /// for the publisher class whose copy arrived first. A mirror's copy and the leader's in-tick
    /// contents don't re-count; a tick the public feed never delivers is still an edge win (the
    /// walkover); a tick the public opens is a public win; the `source_ts == 0` sentinel bypasses
    /// the floor and counts nothing. Venue is unique to this test — the metrics registry is
    /// process-global (see `metrics()` docs).
    #[test]
    fn quote_tick_wins_count_once_per_tick_by_class() {
        let venue = "TickWonQuotes";
        let edge_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let edge_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Quote(quote_at(venue, 1000, 100.0)),
            edge_a,
            TEST_CATEGORY,
        ); // edge opens tick 1000
        a.emit(
            FeedMessage::Quote(quote_at(venue, 1000, 100.5)),
            edge_a,
            TEST_CATEGORY,
        ); // in-tick content: no re-count
        a.emit(
            FeedMessage::Quote(quote_at(venue, 1000, 100.0)),
            edge_b,
            TEST_CATEGORY,
        ); // mirror copy: no count
        a.emit(
            FeedMessage::Quote(quote_at(venue, 1000, 100.0)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        ); // late public copy: no count
        a.emit(
            FeedMessage::Quote(quote_at(venue, 2000, 101.0)),
            Publisher::PublicWs,
            TEST_CATEGORY,
        ); // public opens tick 2000
        a.emit(
            FeedMessage::Quote(quote_at(venue, 3000, 102.0)),
            edge_a,
            TEST_CATEGORY,
        ); // walkover tick 3000
        a.emit(
            FeedMessage::Quote(quote_at(venue, 0, 99.0)),
            edge_a,
            TEST_CATEGORY,
        ); // sentinel: bypass, no count
        let m = crate::metrics::metrics();
        assert_eq!(
            m.quote_ticks_won.with_label_values(&[venue, "edge"]).get(),
            2
        );
        assert_eq!(
            m.quote_ticks_won
                .with_label_values(&[venue, "public"])
                .get(),
            1
        );
    }

    /// The depth mirror (`dz_depth_ticks_won_total`): same once-per-tick attribution on the depth
    /// floor, including the `source_ts == 0` empty-anchor tick (a real tick for depth — no
    /// sentinel bypass), counted once for the class that anchored first.
    #[test]
    fn depth_tick_wins_count_once_per_tick_by_class() {
        let venue = "TickWonDepth";
        let depth_at = |source_ts_ns: u64, bids: Vec<[f64; 2]>| NormalizedDepth {
            venue: venue.into(),
            ..depth(source_ts_ns, bids, vec![])
        };
        let edge_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let edge_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(
            FeedMessage::Depth(depth_at(0, vec![])),
            edge_a,
            TEST_CATEGORY,
        ); // A's empty anchor opens tick 0
        a.emit(
            FeedMessage::Depth(depth_at(0, vec![])),
            edge_b,
            TEST_CATEGORY,
        ); // B's identical anchor: no re-count
        a.emit(
            FeedMessage::Depth(depth_at(1000, vec![[100.0, 1.0]])),
            edge_b,
            TEST_CATEGORY,
        ); // B opens tick 1000
        a.emit(
            FeedMessage::Depth(depth_at(2000, vec![[100.0, 2.0]])),
            Publisher::PublicWs,
            TEST_CATEGORY,
        ); // public opens tick 2000
        let m = crate::metrics::metrics();
        assert_eq!(
            m.depth_ticks_won.with_label_values(&[venue, "edge"]).get(),
            2
        );
        assert_eq!(
            m.depth_ticks_won
                .with_label_values(&[venue, "public"])
                .get(),
            1
        );
    }

    /// Pins the hand-computed `pub_idx(winner) * 2 + pub_idx(loser)` offset (used to index the
    /// `quote_lead`/`trade_lead` `[Histogram; 4]` arrays on the emit path) to the exact
    /// `(winner, loser)` label pair `VenueMetrics::new` builds each slot from. A wrong offset would
    /// silently mislabel a contest metric, so the two orderings must stay locked together here.
    #[test]
    fn lead_histogram_offset_maps_to_expected_label_pair() {
        // Mirrors the label order the `lead` closure in `VenueMetrics::new` constructs the array in.
        let expected = [
            ("edge", "edge"),
            ("edge", "public"),
            ("public", "edge"),
            ("public", "public"),
        ];
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let public = Publisher::PublicWs;
        let label = |p: Publisher| match p {
            Publisher::Edge(_) => "edge",
            Publisher::PublicWs => "public",
        };
        for winner in [edge, public] {
            for loser in [edge, public] {
                let idx = pub_idx(winner) * 2 + pub_idx(loser);
                assert_eq!(
                    expected[idx],
                    (label(winner), label(loser)),
                    "offset {idx} mislabels winner={:?} loser={:?}",
                    label(winner),
                    label(loser),
                );
            }
        }
    }

    /// The dedup key is the identity triple, so an `instrument_id` per symbol is what the test
    /// venue's publisher would send — two symbols sharing an id would legitimately collapse.
    fn instrument(
        instrument_id: u32,
        symbol: &str,
        price_exponent: i8,
        qty_exponent: i8,
    ) -> FeedMessage {
        FeedMessage::Instrument(crate::model::NormalizedInstrument {
            venue: "HYPERLIQUID".into(),
            source: "HYPERLIQUID".into(),
            source_id: 0,
            symbol: symbol.into(),
            channel: 0,
            instrument_id,
            category: "default".into(),
            price_exponent,
            qty_exponent,
        })
    }

    fn drain_instruments(
        rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>,
    ) -> Vec<(String, i8, i8)> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                out.push((i.symbol.to_string(), i.price_exponent, i.qty_exponent));
            }
        }
        out
    }

    /// Mirrored publishers republish identical definitions every refdata burst. Within one
    /// re-announce interval only the burst's first copy reaches the wire; a genuine exponent change
    /// re-emits at once.
    #[test]
    fn arbiter_collapses_duplicate_instrument_definitions() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let peer = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY);
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY); // same publisher's next burst -> dropped
        a.emit(instrument(1, "BTC", -2, -4), peer, TEST_CATEGORY); // mirror's copy -> dropped
        a.emit(instrument(2, "ETH", -2, -4), edge, TEST_CATEGORY); // different symbol -> kept
        a.emit(instrument(1, "BTC", -3, -4), peer, TEST_CATEGORY); // real precision change -> kept
        a.emit(instrument(1, "BTC", -3, -4), edge, TEST_CATEGORY); // ...then deduped at the new content
        assert_eq!(
            drain_instruments(&mut rx),
            vec![
                ("BTC".to_string(), -2, -4),
                ("ETH".to_string(), -2, -4),
                ("BTC".to_string(), -3, -4),
            ]
        );
    }

    /// The collapse is a rate limit, not a once-per-process latch: after the re-announce interval
    /// the same unchanged content goes out again. That periodic burst is the only thing that heals
    /// an established WS client which lost an `instrument` to drop-oldest backpressure — the
    /// `InstrumentSnapshot` replay only covers clients at connect time.
    #[test]
    fn arbiter_reannounces_unchanged_instrument_after_the_interval() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY);
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY); // inside the interval -> collapsed
        assert_eq!(drain_instruments(&mut rx).len(), 1);

        // Backdate the last-broadcast stamp past the interval rather than sleeping 30s.
        for (_, last) in a.instrument_defs.values_mut() {
            *last = last.saturating_sub(INSTRUMENT_REANNOUNCE_NS);
        }
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY);
        assert_eq!(
            drain_instruments(&mut rx),
            vec![("BTC".to_string(), -2, -4)],
            "unchanged content must be re-announced once the interval elapses"
        );
        // ...and the clock restarts, so the next mirror copy collapses again.
        a.emit(instrument(1, "BTC", -2, -4), edge, TEST_CATEGORY);
        assert!(drain_instruments(&mut rx).is_empty());
    }

    // ---- the `book` authority gate ----

    use crate::model::{BookAction, BookChange, BookSide, NormalizedBook};

    const BOOK_CHANNEL: u32 = 2;
    const BOOK_INSTRUMENT: u32 = 41;

    /// The arbitration scope every book test runs in: one venue, one instrument universe.
    fn bscope(venue: &str) -> ScopeKey {
        (Arc::from(venue), TEST_CATEGORY.into())
    }

    /// The authority's key for `(venue, TEST_CATEGORY, BOOK_CHANNEL, instrument)` — what the gate
    /// itself builds for a batch `book()` produces.
    fn mkey(venue: &str, instrument_id: u32) -> MarketKey {
        (
            Arc::from(venue),
            TEST_CATEGORY.into(),
            BOOK_CHANNEL,
            instrument_id,
        )
    }

    fn arm(n: u8) -> Publisher {
        Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
    }

    fn bid(price: f64, size: f64) -> BookChange {
        BookChange {
            action: BookAction::Update,
            side: BookSide::Bid,
            price,
            size,
            order_id: 0,
        }
    }

    fn clear_both() -> BookChange {
        BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }
    }

    /// One batch for `(venue, BOOK_CHANNEL, instrument)`. `recv_ns` is the authority's arrival clock.
    fn book(
        venue: &str,
        instrument_id: u32,
        changes: Vec<BookChange>,
        last: bool,
        recv_ns: u64,
    ) -> FeedMessage {
        FeedMessage::Book(NormalizedBook {
            venue: venue.into(),
            source: venue.into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: BOOK_CHANNEL,
            instrument_id,
            category: TEST_CATEGORY.into(),
            changes,
            snapshot: false,
            last,
            source_ts_ns: 1_000,
            recv_ts_ns: recv_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        })
    }

    /// The producer's opening re-baseline, which is what an MBP processor emits for a market once its
    /// book syncs. Until an arm has sent one, the gate holds only the levels that moved since it
    /// started accumulating and can honestly republish nothing but a bare `clear`.
    fn synced(
        a: &mut Arbiter,
        venue: &str,
        instrument_id: u32,
        publisher: Publisher,
        recv_ns: u64,
    ) {
        a.emit(
            book(venue, instrument_id, vec![clear_both()], true, recv_ns),
            publisher,
            TEST_CATEGORY,
        );
    }

    fn drain_books(rx: &mut broadcast::Receiver<Arc<FeedMessage>>) -> Vec<NormalizedBook> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Book(b) = &*m {
                out.push(b.clone());
            }
        }
        out
    }

    /// Short windows and a low sample floor so a margin transfer is drivable in a test; silence is
    /// disabled so only the path under test can move authority.
    fn sticky_cfg() -> AuthorityConfig {
        AuthorityConfig {
            leader_timeout_ns: u64::MAX,
            sample_interval_ns: 1_000,
            transfer_margin_ns: 1_000,
            transfer_win_rate: 0.8,
            min_window_samples: 5,
        }
    }

    /// Feed `n` matched trade pairs in which `fast` beats `slow` by 50us, through `emit` so the
    /// production call site is what supplies the election's evidence.
    fn race_trades(a: &mut Arbiter, venue: &str, fast: Publisher, slow: Publisher, n: u64) {
        for i in 0..n {
            let mut t = trade(i + 1);
            t.venue = venue.into();
            t.price = 0.60 + i as f64 / 1_000.0;
            t.recv_ts_ns = 10_000 + i * 1_000_000;
            let mut peer = t.clone();
            peer.recv_ts_ns = t.recv_ts_ns + 50_000;
            a.emit(FeedMessage::Trade(t), fast, TEST_CATEGORY);
            a.emit(FeedMessage::Trade(peer), slow, TEST_CATEGORY);
        }
    }

    /// Both arms synced and elected, the wire drained: the steady state every test below starts from.
    fn gated(
        venue: &str,
        cfg: AuthorityConfig,
    ) -> (Arbiter, broadcast::Receiver<Arc<FeedMessage>>) {
        let (tx, mut rx) = broadcast::channel(1024);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_authority(cfg, 1_000_000_000);
        synced(&mut a, venue, BOOK_INSTRUMENT, arm(1), 1_000);
        synced(&mut a, venue, BOOK_INSTRUMENT, arm(2), 1_001);
        let _ = drain_books(&mut rx);
        (a, rx)
    }

    /// The corruption the gate exists to prevent: two arms' delta series for one instrument are
    /// unrelated, so only the authoritative arm's batches may reach the wire.
    #[test]
    fn book_publishes_one_arm_only() {
        let (mut a, mut rx) = gated("KALSHI", AuthorityConfig::default());
        for i in 0..5 {
            let px = 0.40 + i as f64 / 100.0;
            a.emit(
                book("KALSHI", BOOK_INSTRUMENT, vec![bid(px, 10.0)], true, 1_100),
                arm(1),
                TEST_CATEGORY,
            );
            a.emit(
                book("KALSHI", BOOK_INSTRUMENT, vec![bid(px, 99.0)], true, 1_101),
                arm(2),
                TEST_CATEGORY,
            );
        }
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 5, "one arm's batches, not both interleaved");
        assert!(
            out.iter().all(|b| b.changes[0].size == 10.0),
            "every batch must come from the elected arm"
        );
    }

    /// A price-aggregated book keeps the single-arm gate in `Coordinated` mode too: its arms share no
    /// per-event identity to race on, and a `source_ts` tick can hold several deltas, so the quote
    /// floor's per-tick latch would interleave two arms inside one logical event. Only an order-level
    /// market, where every publisher stamps the venue's own `order_id`, races.
    #[test]
    fn book_publishes_one_arm_in_coordinated_mode_too() {
        let (mut a, mut rx) = gated("HYPERLIQUID", AuthorityConfig::default());
        a.set_mode("HYPERLIQUID", ArbitrationMode::Coordinated);
        for _ in 0..3 {
            for (p, size) in [(arm(1), 10.0), (arm(2), 99.0)] {
                a.emit(
                    book(
                        "HYPERLIQUID",
                        BOOK_INSTRUMENT,
                        vec![bid(0.40, size)],
                        true,
                        1_100,
                    ),
                    p,
                    TEST_CATEGORY,
                );
            }
        }
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|b| b.changes[0].size == 10.0));
    }

    /// Authority spans a universe (per `StickyAuthority`), so the arm that won it serves every market
    /// in that universe — including one a challenger got to first.
    #[test]
    fn book_authority_spans_the_universe_across_markets() {
        let venue = "BookVenueWide";
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        for id in [42, 43] {
            // The challenger speaks first for a market the leader has never sent for.
            a.emit(
                book(venue, id, vec![bid(0.40, 99.0)], true, 1_100),
                arm(2),
                TEST_CATEGORY,
            );
            assert!(
                drain_books(&mut rx).is_empty(),
                "market {id}: a challenger must not take a market by getting there first"
            );
            a.emit(
                book(venue, id, vec![bid(0.40, 10.0)], true, 1_101),
                arm(1),
                TEST_CATEGORY,
            );
            // A market's first admitted batch re-baselines too, and this arm has sent no producer
            // re-baseline for it, so the honest re-baseline is a bare clear ahead of the batch.
            let out = drain_books(&mut rx);
            assert_eq!(out.len(), 2, "market {id}");
            assert_eq!(out[0].changes, vec![clear_both()]);
            assert_eq!(out[1].changes, vec![bid(0.40, 10.0)]);
        }
    }

    /// The replay map must hold what was broadcast, so a connecting client is bootstrapped with the
    /// authoritative arm's book and never a discarded arm's divergent copy.
    #[test]
    fn book_replay_accumulates_the_authoritative_arm() {
        let venue = "BookReplayLeader";
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());
        synced(&mut a, venue, BOOK_INSTRUMENT, arm(1), 1_000);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 99.0)], true, 1_101),
            arm(2),
            TEST_CATEGORY,
        );
        let guard = model::lock(&replay);
        let acc = guard
            .get(&mkey(venue, BOOK_INSTRUMENT))
            .expect("the admitted market is in the replay map");
        let full = acc.to_book(&mkey(venue, BOOK_INSTRUMENT), ReplayScope::Orders);
        assert_eq!(
            full.changes[1..].to_vec(),
            vec![bid(0.40, 10.0)],
            "the loser's size must not reach the replay state"
        );
    }

    /// `book` is incremental, so it must never pass through a content floor: an oscillation back to a
    /// previously-seen state is a real event, not a duplicate.
    #[test]
    fn book_never_routes_through_a_content_floor() {
        let venue = "BookNoFloor";
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        for size in [100.0, 0.0, 100.0] {
            a.emit(
                book(venue, BOOK_INSTRUMENT, vec![bid(0.40, size)], true, 1_100),
                arm(1),
                TEST_CATEGORY,
            );
        }
        assert_eq!(
            drain_books(&mut rx)
                .iter()
                .map(|b| b.changes[0].size)
                .collect::<Vec<_>>(),
            vec![100.0, 0.0, 100.0]
        );
    }

    /// **The transfer contract.** A margin transfer hands the market to an arm whose delta series is
    /// unrelated to the state the consumer holds, so the next broadcast must republish that arm's
    /// *whole* book — a `clear` plus every level it holds, not just the batch that triggered it. A
    /// bare clear would leave the consumer knowingly incomplete until the next snapshot rotation.
    #[test]
    fn a_transfer_republishes_the_new_arms_whole_book() {
        let venue = "BookMarginTransfer";
        let transfers = |reason: &str| {
            metrics()
                .arm_transfers
                .with_label_values(&[venue, reason])
                .get()
        };
        let (initial, margin) = (transfers("initial"), transfers("margin"));
        let (mut a, mut rx) = gated(venue, sticky_cfg());

        // arm(1) serves; arm(2)'s copies are dropped from the wire but still accumulated.
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 55.0)], true, 1_101),
            arm(2),
            TEST_CATEGORY,
        );
        assert_eq!(drain_books(&mut rx).len(), 1);

        race_trades(&mut a, venue, arm(2), arm(1), 6);
        a.close_authority_windows();

        // arm(1) has lost the venue; its stream stops reaching the wire.
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.41, 11.0)], true, 2_000),
            arm(1),
            TEST_CATEGORY,
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "the displaced arm is muted"
        );

        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.41, 66.0)], true, 2_100),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 1, "one re-baseline, not a batch plus a clear");
        let re = &out[0];
        assert!(
            re.snapshot && re.last,
            "a re-baseline is one complete event"
        );
        assert_eq!(
            re.changes,
            vec![clear_both(), bid(0.41, 66.0), bid(0.40, 55.0)],
            "clear plus arm(2)'s whole book, best bid first"
        );
        assert_eq!(transfers("initial"), initial + 1);
        assert_eq!(transfers("margin"), margin + 1);
    }

    /// A per-market health override changes the serving arm without moving venue authority, and it
    /// re-baselines the consumer for the same reason a transfer does.
    #[test]
    fn a_health_override_also_rebaselines() {
        let venue = "BookHealthOverride";
        let health = metrics()
            .arm_transfers
            .with_label_values(&[venue, "health"]);
        let before = health.get();
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 55.0)], true, 1_101),
            arm(2),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        a.set_book_health(&key, arm(1), false);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.42, 66.0)], true, 1_200),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].changes,
            vec![clear_both(), bid(0.42, 66.0), bid(0.40, 55.0)]
        );
        assert_eq!(
            a.books.scope_leader(&bscope(venue)),
            Some(arm(1)),
            "the venue is untouched"
        );
        assert_eq!(health.get(), before + 1);
    }

    /// A re-baseline waits for the new arm to close a logical event. Materializing a half-applied one
    /// would publish a torn book stamped `last: true`, which is exactly what that field promises not
    /// to be — so the market stays on its stale (but coherent) state until the boundary.
    #[test]
    fn a_rebaseline_waits_for_the_event_boundary() {
        let venue = "BookRebaselineBoundary";
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);
        a.set_book_health(&key, arm(1), false);

        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.41, 20.0)], false, 1_200),
            arm(2),
            TEST_CATEGORY,
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "nothing goes out mid-event"
        );
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.42, 30.0)], true, 1_201),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].changes[1..].to_vec(),
            vec![bid(0.42, 30.0), bid(0.41, 20.0)],
            "the withheld batch is not lost, it lands in the re-baseline"
        );
    }

    /// `last` is mandatory on the wire, but the wire is unauthenticated: an arm that stops closing
    /// events must not withhold a market from the wire forever. The market re-baselines on the bound
    /// and keeps streaming.
    #[test]
    fn an_unterminated_event_does_not_withhold_forever() {
        let venue = "BookUnterminated";
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);
        a.set_book_health(&key, arm(1), false);

        for i in 0..=MAX_WITHHELD_BATCHES {
            a.emit(
                book(
                    venue,
                    BOOK_INSTRUMENT,
                    vec![bid(0.41, f64::from(i))],
                    false,
                    1_200,
                ),
                arm(2),
                TEST_CATEGORY,
            );
        }
        let out = drain_books(&mut rx);
        assert!(!out.is_empty(), "the market must not be withheld forever");
        assert_eq!(
            out[0].changes[0].action,
            BookAction::Clear,
            "it re-baselines rather than resuming on the old arm's state"
        );
    }

    /// An arm that has never sent a producer re-baseline holds only the levels that moved since it
    /// started accumulating. Publishing that as `snapshot` would tell the consumer to discard every
    /// level it is missing, so the gate degrades to a bare `clear` — incomplete, but honest.
    #[test]
    fn a_mid_stream_arm_rebaselines_with_a_bare_clear() {
        let venue = "BookMidStreamArm";
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        synced(&mut a, venue, BOOK_INSTRUMENT, arm(1), 1_000);
        // arm(2) joins mid-stream: deltas only, no re-baseline of its own.
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 55.0)], true, 1_100),
            arm(2),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        a.set_book_health(&key, arm(1), false);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.41, 66.0)], true, 1_200),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(
            out[0].changes,
            vec![clear_both()],
            "a bare clear, never arm(2)'s partial levels dressed as a snapshot"
        );
        assert_eq!(
            out[1].changes,
            vec![bid(0.41, 66.0)],
            "the batch then rebuilds onto the emptied book"
        );
    }

    /// The public WS backstop reaches `emit` with the same trades but decodes them from parsed JSON
    /// and serves no `book` at all. Admitting it as an arm would poison the lead histogram and could
    /// hand a venue's books to a source that publishes none.
    #[test]
    fn the_public_backstop_is_not_an_election_arm() {
        let venue = "BookPublicNotAnArm";
        let (mut a, _rx) = gated(venue, sticky_cfg());
        race_trades(&mut a, venue, Publisher::PublicWs, arm(1), 20);
        a.close_authority_windows();
        assert!(!a.books.tracks_arm(&bscope(venue), Publisher::PublicWs));
        assert_eq!(a.books.scope_leader(&bscope(venue)), Some(arm(1)));
    }

    /// A trade publisher the authority does not track — a peer feed row of the same venue, or a forged
    /// source IP — must not spend one of the universe's eight admission slots through the metrics path.
    /// Once they are gone a real mirror arm is ineligible and the venue can never fail over.
    #[test]
    fn an_untracked_trade_publisher_never_becomes_an_arm() {
        let venue = "BookForgedTradeSource";
        let (mut a, _rx) = gated(venue, sticky_cfg());
        for n in 20..40u8 {
            let mut t = trade(u64::from(n));
            t.venue = venue.into();
            a.emit(FeedMessage::Trade(t), arm(n), TEST_CATEGORY);
        }
        a.close_authority_windows();
        for n in 20..40u8 {
            assert!(
                !a.books.tracks_arm(&bscope(venue), arm(n)),
                "arm {n} was admitted"
            );
            assert_eq!(
                a.books.arm_label(&bscope(venue), arm(n)),
                crate::ingest::authority::OTHER_ARM
            );
        }
        // ...and a third real arm still gets a slot.
        assert_ne!(
            a.books.arm_ordinal(&bscope(venue), arm(3)),
            crate::ingest::authority::OTHER_ARM
        );
    }

    /// An arm past the authority's per-universe cap enters no per-market map, so a forged source can
    /// neither be served nor evict a real market's book state through the gate.
    #[test]
    fn an_ineligible_arm_creates_no_book_state() {
        let venue = "BookIneligibleArm";
        let (mut a, _rx) = gated(venue, AuthorityConfig::default());
        for n in 3..=8 {
            a.books.arm_ordinal(&bscope(venue), arm(n)); // fill the eight labelled slots
        }
        let before = a.book_markets.len();
        a.emit(
            book(venue, 777, vec![bid(0.40, 10.0)], true, 1_100),
            arm(200),
            TEST_CATEGORY,
        );
        assert_eq!(a.book_markets.len(), before, "no market was tracked for it");
    }

    /// An `Arbiter` with `venue` declared `Sticky` and no book traffic — a top-of-book-only
    /// deployment, where the authority never elects and the tape gate is on its own.
    fn sticky_tape(venue: &'static str) -> (Arbiter, broadcast::Receiver<Arc<FeedMessage>>) {
        let (tx, rx) = broadcast::channel(1024);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_mode(venue, ArbitrationMode::Sticky);
        (a, rx)
    }

    /// One print from `arm` for `venue`, at `recv_ts_ns`.
    fn tape_print(a: &mut Arbiter, venue: &str, p: Publisher, id: u64, recv_ts_ns: u64) {
        let mut t = trade(id);
        t.venue = venue.into();
        t.recv_ts_ns = recv_ts_ns;
        a.emit(FeedMessage::Trade(t), p, TEST_CATEGORY);
    }

    fn drain_trades(rx: &mut broadcast::Receiver<Arc<FeedMessage>>) -> usize {
        let mut n = 0;
        while let Ok(m) = rx.try_recv() {
            if matches!(&*m, FeedMessage::Trade(_)) {
                n += 1;
            }
        }
        n
    }

    /// Both arms stamp the sentinel for one fill. The zero-id latch forwards unconditionally, so
    /// without the arm gate the tape doubles.
    #[test]
    fn sticky_zero_id_prints_from_two_arms_collapse() {
        let venue = "TapeArmZeroId";
        let (mut a, mut rx) = sticky_tape(venue);
        tape_print(&mut a, venue, arm(1), 0, 1_000);
        tape_print(&mut a, venue, arm(2), 0, 1_001);
        assert_eq!(drain_trades(&mut rx), 1);
    }

    /// The case neither the sentinel latch nor the dedup window can collapse: two arms stamping
    /// *different real* ids for one fill. Gating on arm identity rather than on the id is what
    /// covers it.
    #[test]
    fn sticky_distinct_ids_from_two_arms_collapse() {
        let venue = "TapeArmDistinctIds";
        let (mut a, mut rx) = sticky_tape(venue);
        tape_print(&mut a, venue, arm(1), 501, 1_000);
        tape_print(&mut a, venue, arm(2), 902, 1_001);
        assert_eq!(drain_trades(&mut rx), 1);
    }

    /// A dead leader must not mute the tape: past the handover window the challenger takes it, and the
    /// move is counted. Venue is unique to this test; the metrics registry is process-global.
    #[test]
    fn a_silent_tape_arm_hands_over() {
        let venue = "TapeArmSilence";
        let transfers = metrics().tape_arm_transfers.with_label_values(&[venue]);
        let before = transfers.get();
        let (mut a, mut rx) = sticky_tape(venue);
        tape_print(&mut a, venue, arm(1), 1, 1_000);
        // Exactly at the window the incumbent still holds it, and a rejected challenger must not
        // refresh its clock (a burst of them would otherwise hold the tape open forever).
        tape_print(&mut a, venue, arm(2), 2, 1_000 + NO_ID_TAPE_HANDOVER_NS);
        assert_eq!(drain_trades(&mut rx), 1);
        assert_eq!(transfers.get() - before, 0);

        tape_print(&mut a, venue, arm(2), 3, 1_001 + NO_ID_TAPE_HANDOVER_NS);
        tape_print(&mut a, venue, arm(2), 4, 1_002 + NO_ID_TAPE_HANDOVER_NS);
        assert_eq!(
            drain_trades(&mut rx),
            2,
            "the challenger inherits and keeps it"
        );
        assert_eq!(transfers.get() - before, 1);
    }

    /// With books flowing, the tape follows the arm the authority elected — even though the peer
    /// printed first and is well inside the silence window. Driven through the public book path, not
    /// by poking authority state.
    #[test]
    fn the_tape_follows_the_book_election() {
        let venue = "TapeArmFollowsElection";
        let (mut a, mut rx) = gated(venue, sticky_cfg());
        a.set_mode(venue, ArbitrationMode::Sticky);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        assert_eq!(a.books.scope_leader(&bscope(venue)), Some(arm(1)));
        let _ = drain_books(&mut rx);
        let _ = drain_trades(&mut rx);

        tape_print(&mut a, venue, arm(2), 11, 2_000); // the non-elected arm opens the tape
        tape_print(&mut a, venue, arm(1), 12, 2_001); // ...and the elected arm takes it immediately
        tape_print(&mut a, venue, arm(2), 13, 2_002); // ...so the peer is dropped from here on
        assert_eq!(drain_trades(&mut rx), 2);
    }

    /// The gate is first-come, and the wire is unauthenticated — so an arm the authority tracks must
    /// displace one it does not, or an early forged print would mute the real arms for as long as it
    /// kept printing inside the silence window.
    #[test]
    fn a_tracked_arm_displaces_an_untracked_incumbent() {
        let venue = "TapeArmDisplacesSquatter";
        let (mut a, mut rx) = gated(venue, sticky_cfg());
        a.set_mode(venue, ArbitrationMode::Sticky);
        let squatter = arm(200);
        assert!(!a.books.tracks_arm(&bscope(venue), squatter));
        assert!(a.books.tracks_arm(&bscope(venue), arm(1)));

        tape_print(&mut a, venue, squatter, 1, 2_000);
        let _ = drain_trades(&mut rx);
        for i in 0..3 {
            tape_print(&mut a, venue, arm(1), 10 + i, 2_001 + i);
            tape_print(&mut a, venue, squatter, 20 + i, 2_100 + i);
        }
        assert_eq!(drain_trades(&mut rx), 3, "the real arm keeps the tape");
    }

    /// The election deferral fires once per election, not per print. Otherwise an elected arm whose
    /// trade stream is nearly dead would reclaim the tape after every straggler and mute the healthy
    /// peer for another window — the two rules would fight and the tape would sawtooth.
    #[test]
    fn a_straggler_from_the_elected_arm_does_not_reclaim_the_tape() {
        let venue = "TapeArmStraggler";
        let (mut a, mut rx) = gated(venue, sticky_cfg());
        a.set_mode(venue, ArbitrationMode::Sticky);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        assert_eq!(a.books.scope_leader(&bscope(venue)), Some(arm(1)));
        let _ = drain_books(&mut rx);
        let _ = drain_trades(&mut rx);

        // arm(1) opens the tape as the elected arm, then goes quiet and arm(2) inherits it.
        tape_print(&mut a, venue, arm(1), 1, 2_000);
        let t0 = 2_001 + NO_ID_TAPE_HANDOVER_NS;
        tape_print(&mut a, venue, arm(2), 2, t0);
        let _ = drain_trades(&mut rx);

        // One straggler from arm(1) must not take it back; arm(2)'s stream keeps reaching the wire.
        tape_print(&mut a, venue, arm(1), 3, t0 + 1);
        for i in 0..3 {
            tape_print(&mut a, venue, arm(2), 10 + i, t0 + 2 + i);
        }
        assert_eq!(drain_trades(&mut rx), 3);
    }

    /// A gate-approved handover is one emitter by construction, so the zero-id latch — which knows
    /// nothing about the election — must not report it on the counter that has to stay trustworthy.
    #[test]
    fn a_gate_approved_handover_is_not_a_zero_id_conflict() {
        let venue = "TapeArmNoFalseConflict";
        let conflicts = metrics().trades_no_id_conflict.with_label_values(&[venue]);
        let before = conflicts.get();
        let (mut a, _rx) = gated(venue, sticky_cfg());
        a.set_mode(venue, ArbitrationMode::Sticky);
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        tape_print(&mut a, venue, arm(2), 0, 2_000); // opens the tape
        tape_print(&mut a, venue, arm(1), 0, 2_001); // the elected arm takes it over
        for i in 0..5 {
            tape_print(&mut a, venue, arm(1), 0, 2_002 + i);
        }
        assert_eq!(conflicts.get(), before);
    }

    /// An `Arbiter` whose one `Sticky` venue is the venue both category tests print under. No book
    /// traffic, so the authority elects nobody and the tape gate is on its own — the shape of a
    /// deployment where one Source ID carries two universes.
    fn arbiter() -> Arbiter {
        let (tx, _rx) = broadcast::channel(1024);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_mode("KALSHI", ArbitrationMode::Sticky);
        a
    }

    /// One print from `p` for `(venue, category)`, returning whether it reached the wire. The
    /// receiver is subscribed immediately before the emit, so it observes this print alone.
    fn tape_print_in(
        a: &mut Arbiter,
        venue: &str,
        category: &'static str,
        p: Publisher,
        id: u64,
        recv_ts_ns: u64,
    ) -> bool {
        let mut rx = a.sender().subscribe();
        let mut t = trade(id);
        t.venue = venue.into();
        t.recv_ts_ns = recv_ts_ns;
        a.emit(FeedMessage::Trade(t), p, category);
        matches!(rx.try_recv(), Ok(m) if matches!(&*m, FeedMessage::Trade(_)))
    }

    /// The arm gate is per `(venue, category)`. Two publishers on disjoint universes are not
    /// competing for one tape, so neither may mute the other — a venue-wide gate drops the
    /// loser's prints forever, since a continuously-printing incumbent never goes silent.
    #[test]
    fn an_arm_on_another_category_does_not_take_the_tape() {
        let mut a = arbiter();
        tape_print_in(&mut a, "KALSHI", "perps", arm(1), 1, 1_000);
        let admitted = tape_print_in(&mut a, "KALSHI", "sports", arm(2), 2, 1_001);
        assert!(
            admitted,
            "a sports print was dropped by the perps tape leader"
        );
        // ...and the perps arm keeps its own tape rather than being displaced by the sports arm.
        assert!(tape_print_in(&mut a, "KALSHI", "perps", arm(1), 3, 1_002));
    }

    /// Within one category the sticky single-arm gate is unchanged: the second arm is dropped
    /// while the incumbent keeps printing.
    #[test]
    fn a_peer_arm_in_the_same_category_is_still_dropped() {
        let mut a = arbiter();
        assert!(tape_print_in(&mut a, "KALSHI", "perps", arm(1), 1, 1_000));
        assert!(!tape_print_in(&mut a, "KALSHI", "perps", arm(2), 2, 1_001));
    }

    /// The gate is `Sticky`-only, so every `Coordinated` venue keeps the id-keyed behaviour: two arms'
    /// distinct ids both reach the wire.
    #[test]
    fn a_coordinated_venue_is_unaffected_by_the_arm_gate() {
        let venue = "TapeArmCoordinated";
        let (tx, mut rx) = broadcast::channel(1024);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        tape_print(&mut a, venue, arm(1), 501, 1_000);
        tape_print(&mut a, venue, arm(2), 902, 1_001);
        assert_eq!(drain_trades(&mut rx), 2);
    }

    /// A losing arm's batches are attributed to it, keeping the which-arm-is-losing signal a scalar
    /// counter would flatten.
    #[test]
    fn book_dropped_is_attributed_to_the_losing_publisher() {
        let venue = "BookDroppedAttribution";
        let dropped = metrics().book_dropped.with_label_values(&[venue, "edge"]);
        let (mut a, _rx) = gated(venue, AuthorityConfig::default());
        let before = dropped.get();
        for _ in 0..3 {
            a.emit(
                book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 99.0)], true, 1_100),
                arm(2),
                TEST_CATEGORY,
            );
        }
        assert_eq!(dropped.get(), before + 3);
    }

    /// The tracked-market set is bounded: the key is wire-supplied, so a flood of distinct markets
    /// must cost evictions rather than memory — and the authority's own per-market map, which has no
    /// cap of its own, must be evicted with it.
    #[test]
    fn book_markets_are_bounded() {
        let venue = "BookMarketCap";
        let (tx, _rx) = broadcast::channel(1);
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());
        for id in 0..(MAX_BOOK_MARKETS as u32 + 64) {
            a.emit(
                book(venue, id, vec![bid(0.40, 10.0)], true, 1_000),
                arm(1),
                TEST_CATEGORY,
            );
        }
        assert_eq!(a.book_markets.len(), MAX_BOOK_MARKETS);
        assert_eq!(a.book_order.len(), MAX_BOOK_MARKETS);
        assert_eq!(
            model::lock(&replay).len(),
            MAX_BOOK_MARKETS,
            "the replay map inherits the same cap"
        );
        assert_eq!(
            a.books.markets_held_all(),
            vec![(Arc::from(venue), arm(1), MAX_BOOK_MARKETS)],
            "the authority's per-market map is evicted in step"
        );
    }

    /// The WS-replay map is keyed at the **same grain** as the market key, and eviction is what makes
    /// that load-bearing rather than cosmetic. Two universes under one Source ID have independent id
    /// spaces, so they can carry the same `(channel, instrument_id)`. Under a venue-grained replay key
    /// they share one entry, and evicting one universe's market deletes the **other's live** entry
    /// while its `book_markets` and `last_admitted` survive — so it never re-baselines, its rebuilt
    /// entry stays `!baselined()`, and it is invisible to every client that connects from then on.
    #[test]
    fn evicting_one_universes_market_spares_the_others_replay_entry() {
        let venue = "BookEvictionAcrossUniverses";
        let (tx, _rx) = broadcast::channel(1);
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());

        // The same wire identity in two universes, each with a producer re-baseline of its own so
        // both replay entries are complete.
        let doomed: MarketKey = (
            Arc::from(venue),
            "perps".into(),
            BOOK_CHANNEL,
            BOOK_INSTRUMENT,
        );
        let kept: MarketKey = (
            Arc::from(venue),
            "sports".into(),
            BOOK_CHANNEL,
            BOOK_INSTRUMENT,
        );
        for category in ["perps", "sports"] {
            // The producer's opening re-baseline, in this universe (`synced` is TEST_CATEGORY-bound).
            a.emit(
                book(venue, BOOK_INSTRUMENT, vec![clear_both()], true, 1_000),
                arm(1),
                category,
            );
            a.emit(
                book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_001),
                arm(1),
                category,
            );
        }
        assert!(
            model::lock(&replay)
                .get(&kept)
                .is_some_and(|acc| acc.baselined()),
            "both universes start with a complete replay entry"
        );

        // Fill to the cap and one past it. `doomed` was tracked first, so it is the single eviction.
        for id in 0..(MAX_BOOK_MARKETS as u32 - 1) {
            a.emit(
                book(venue, 1_000 + id, vec![bid(0.40, 10.0)], true, 1_100),
                arm(1),
                "perps",
            );
        }
        let guard = model::lock(&replay);
        assert!(
            !guard.contains_key(&doomed),
            "the oldest market was evicted, replay entry included"
        );
        assert!(
            guard.get(&kept).is_some_and(|acc| acc.baselined()),
            "the peer universe's live, complete replay entry must survive that eviction"
        );
    }

    /// An evicted market's replay entry is gone with nothing left behind: a stale entry would have
    /// `/v1/products` report a `market_by_price` book for a market that no longer exists.
    #[test]
    fn an_evicted_market_is_unreachable_by_full_key() {
        let venue = "BookEvictionIndex";
        let (tx, _rx) = broadcast::channel(1);
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());
        for id in 0..(MAX_BOOK_MARKETS as u32 + 1) {
            a.emit(
                book(venue, id, vec![bid(0.40, 10.0)], true, 1_000),
                arm(1),
                TEST_CATEGORY,
            );
        }
        let guard = model::lock(&replay);
        assert_eq!(guard.len(), MAX_BOOK_MARKETS, "one market was evicted");
        assert!(
            guard.get(&mkey(venue, 0)).is_none(),
            "the evicted market must not still be reachable"
        );
        assert!(
            guard.get(&mkey(venue, MAX_BOOK_MARKETS as u32)).is_some(),
            "...while a live one still is"
        );
    }

    /// Eviction drops the authority's record of who served a market, and that is what keeps it safe:
    /// the market's next batch reads as a change of serving arm and re-baselines the consumer instead
    /// of resuming an unrelated delta series on top of its state.
    #[test]
    fn an_evicted_market_rebaselines_rather_than_resuming() {
        let venue = "BookEvictionRebaseline";
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        let (mut a, _rx) = gated(venue, AuthorityConfig::default());
        assert_eq!(a.books.last_admitted(&key), Some(arm(1)));

        a.reset_book_for_market(&key);
        assert_eq!(a.books.last_admitted(&key), None);

        let mut rx = a.sender().subscribe();
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        assert_eq!(
            drain_books(&mut rx)[0].changes[0].action,
            BookAction::Clear,
            "a market with no served-arm record must be re-baselined"
        );
    }

    /// N2: `forget_channel_books` must drop all **three** legs of the pairing — the accumulator
    /// (`book_markets`), the replay entry, and `StickyAuthority::last_admitted` — for every
    /// instrument on the departed `(venue, category, channel)`, and leave a peer instrument on a
    /// different category untouched in all three. Hand-deleting only the replay entry (the bug this
    /// method exists to rule out) would leave `last_admitted` behind, which is exactly what a bare
    /// `contains_key` check on the replay map alone cannot catch — so this asserts `last_admitted`
    /// directly, not just replay-map absence.
    #[test]
    fn forget_channel_books_drops_the_full_pairing_and_spares_untouched_peers() {
        let venue = "BookForgetChannel";
        let (tx, _rx) = broadcast::channel(1);
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());

        // A second channel, same venue/category as the doomed markets — a still-running sibling
        // channel. `book()` always stamps `BOOK_CHANNEL`, so this batch is built by hand.
        const OTHER_CHANNEL: u32 = BOOK_CHANNEL + 1;
        let book_on_channel = |channel: u32, instrument_id: u32, changes: Vec<BookChange>| {
            FeedMessage::Book(NormalizedBook {
                venue: venue.into(),
                source: venue.into(),
                source_id: 0,
                symbol: "KXBTCPERP".into(),
                channel,
                instrument_id,
                category: TEST_CATEGORY.into(),
                changes,
                snapshot: false,
                last: true,
                source_ts_ns: 1_000,
                recv_ts_ns: 1_000,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            })
        };

        // Two instruments on the doomed (venue, "sports", BOOK_CHANNEL), each fully re-baselined.
        for id in [BOOK_INSTRUMENT, BOOK_INSTRUMENT + 1] {
            a.emit(
                book(venue, id, vec![clear_both()], true, 1_000),
                arm(1),
                "sports",
            );
            a.emit(
                book(venue, id, vec![bid(0.40, 10.0)], true, 1_001),
                arm(1),
                "sports",
            );
        }
        // Peer 1: a different **category**, same venue/channel/instrument_id as the first doomed
        // market — the exact collision this crate's docs warn about. Deleting the category term
        // from `forget_channel_books`'s filter would wrongly sweep this one up too.
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![clear_both()], true, 1_000),
            arm(1),
            "perps",
        );
        a.emit(
            book(venue, BOOK_INSTRUMENT, vec![bid(0.50, 5.0)], true, 1_001),
            arm(1),
            "perps",
        );
        // Peer 2: a different **channel**, same venue/category as the doomed markets. Deleting the
        // channel term from the filter would wrongly sweep this still-running sibling channel up
        // too — the failure mode a single-channel fixture cannot express.
        a.emit(
            book_on_channel(OTHER_CHANNEL, BOOK_INSTRUMENT, vec![clear_both()]),
            arm(1),
            "sports",
        );
        a.emit(
            book_on_channel(OTHER_CHANNEL, BOOK_INSTRUMENT, vec![bid(0.60, 7.0)]),
            arm(1),
            "sports",
        );

        let doomed_a: MarketKey = (
            Arc::from(venue),
            "sports".into(),
            BOOK_CHANNEL,
            BOOK_INSTRUMENT,
        );
        let doomed_b: MarketKey = (
            Arc::from(venue),
            "sports".into(),
            BOOK_CHANNEL,
            BOOK_INSTRUMENT + 1,
        );
        let peer_category: MarketKey = (
            Arc::from(venue),
            "perps".into(),
            BOOK_CHANNEL,
            BOOK_INSTRUMENT,
        );
        let peer_channel: MarketKey = (
            Arc::from(venue),
            "sports".into(),
            OTHER_CHANNEL,
            BOOK_INSTRUMENT,
        );

        assert_eq!(
            a.books.last_admitted(&doomed_a),
            Some(arm(1)),
            "fixture sanity"
        );
        assert_eq!(
            a.books.last_admitted(&doomed_b),
            Some(arm(1)),
            "fixture sanity"
        );
        assert_eq!(
            a.books.last_admitted(&peer_category),
            Some(arm(1)),
            "fixture sanity"
        );
        assert_eq!(
            a.books.last_admitted(&peer_channel),
            Some(arm(1)),
            "fixture sanity"
        );
        assert_eq!(
            model::lock(&replay).len(),
            4,
            "fixture sanity: four distinct markets in the replay map"
        );

        let dropped = a.forget_channel_books(venue, "sports", BOOK_CHANNEL);
        assert_eq!(
            dropped, 2,
            "exactly the two sports instruments on that channel"
        );

        // All three legs, for both doomed markets.
        assert!(!a.book_markets.contains_key(&doomed_a));
        assert!(!a.book_markets.contains_key(&doomed_b));
        assert_eq!(
            a.books.last_admitted(&doomed_a),
            None,
            "last_admitted must drop too"
        );
        assert_eq!(
            a.books.last_admitted(&doomed_b),
            None,
            "last_admitted must drop too"
        );
        {
            let guard = model::lock(&replay);
            assert!(!guard.contains_key(&doomed_a));
            assert!(!guard.contains_key(&doomed_b));
        }

        // Both peers survive in all three, checked by name so a category-blind purge and a
        // channel-blind purge each fail on their own peer rather than one masking the other.
        for (name, peer) in [
            ("peer_category", &peer_category),
            ("peer_channel", &peer_channel),
        ] {
            assert!(
                a.book_markets.contains_key(peer),
                "{name} must survive in book_markets"
            );
            assert_eq!(
                a.books.last_admitted(peer),
                Some(arm(1)),
                "{name}'s authority record must survive"
            );
            assert!(
                model::lock(&replay)
                    .get(peer)
                    .is_some_and(|acc| acc.baselined()),
                "{name}'s replay entry must survive, complete"
            );
        }

        // F4: the replay map must drop exactly the two doomed markets' entries and nothing else
        // (the coverage `model::BookReplay::forget_channel`'s own test carried before this seam
        // moved to the arbiter).
        assert_eq!(
            model::lock(&replay).len(),
            2,
            "the replay map must hold exactly the two surviving markets"
        );
    }

    /// A `book` batch on an explicit `channel` (the `book()` helper always stamps `BOOK_CHANNEL`).
    fn book_ch(
        venue: &str,
        channel: u32,
        instrument_id: u32,
        changes: Vec<BookChange>,
    ) -> FeedMessage {
        FeedMessage::Book(NormalizedBook {
            venue: venue.into(),
            source: venue.into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel,
            instrument_id,
            category: TEST_CATEGORY.into(),
            changes,
            snapshot: false,
            last: true,
            source_ts_ns: 1_000,
            recv_ts_ns: 1_000,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        })
    }

    /// Every leg of the book-market pairing, plus the eviction queue, in one comparable value.
    #[derive(Debug, PartialEq)]
    struct BookLegs {
        /// `book_markets` keys, sorted (the per-arm accumulators).
        markets: Vec<MarketKey>,
        /// `book_order` in queue order — not one of the paired legs, but a divergence here would
        /// change which market a later eviction drops.
        order: Vec<MarketKey>,
        /// `StickyAuthority::last_admitted` per seeded key.
        admitted: Vec<(MarketKey, Option<Publisher>)>,
        /// The replay entry per seeded key: present-and-`baselined`, present-and-not, or absent.
        replay: Vec<(MarketKey, Option<bool>)>,
        /// Total entries in the replay map (a leaked entry from a market that should have been
        /// dropped would show up here even if it isn't one of the tracked `keys`).
        replay_len: usize,
    }

    fn book_legs(a: &Arbiter, replay: &crate::model::BookSnapshot, keys: &[MarketKey]) -> BookLegs {
        let mut markets: Vec<MarketKey> = a.book_markets.keys().cloned().collect();
        markets.sort();
        let guard = model::lock(replay);
        BookLegs {
            markets,
            order: a.book_order.iter().cloned().collect(),
            admitted: keys
                .iter()
                .map(|k| (k.clone(), a.books.last_admitted(k)))
                .collect(),
            replay: keys
                .iter()
                .map(|k| (k.clone(), guard.get(k).map(|acc| acc.baselined())))
                .collect(),
            replay_len: guard.len(),
        }
    }

    /// Four markets under one venue, each fully re-baselined by `arm(1)`: two on
    /// `(venue, "sports", BOOK_CHANNEL)`, one on a peer **category** colliding with the first on
    /// `(channel, instrument_id)`, one on a peer **channel**. Returns the arbiter, its replay map
    /// and the four keys in that order.
    fn seeded_books(venue: &str) -> (Arbiter, crate::model::BookSnapshot, Vec<MarketKey>) {
        const OTHER_CHANNEL: u32 = BOOK_CHANNEL + 1;
        let (tx, _rx) = broadcast::channel(1);
        let replay: crate::model::BookSnapshot = Arc::new(Mutex::new(BookReplay::default()));
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_book_replay(replay.clone());

        for (category, channel, id) in [
            ("sports", BOOK_CHANNEL, BOOK_INSTRUMENT),
            ("sports", BOOK_CHANNEL, BOOK_INSTRUMENT + 1),
            ("perps", BOOK_CHANNEL, BOOK_INSTRUMENT),
            ("sports", OTHER_CHANNEL, BOOK_INSTRUMENT),
        ] {
            a.emit(
                book_ch(venue, channel, id, vec![clear_both()]),
                arm(1),
                category,
            );
            a.emit(
                book_ch(venue, channel, id, vec![bid(0.40, 10.0)]),
                arm(1),
                category,
            );
        }

        let keys: Vec<MarketKey> = vec![
            (
                Arc::from(venue),
                "sports".into(),
                BOOK_CHANNEL,
                BOOK_INSTRUMENT,
            ),
            (
                Arc::from(venue),
                "sports".into(),
                BOOK_CHANNEL,
                BOOK_INSTRUMENT + 1,
            ),
            (
                Arc::from(venue),
                "perps".into(),
                BOOK_CHANNEL,
                BOOK_INSTRUMENT,
            ),
            (
                Arc::from(venue),
                "sports".into(),
                OTHER_CHANNEL,
                BOOK_INSTRUMENT,
            ),
        ];
        (a, replay, keys)
    }

    /// The three legs of the book-market drop are expressed **once**: `reset_book_for_market`
    /// delegates to `reset_books_for_markets`. This is the test that keeps them from drifting apart
    /// again — the single-key path and the batch path are run against identical seeded state and
    /// their results must match on every leg (accumulators, `last_admitted`, replay entry + its
    /// identity index) plus the eviction queue. Re-implementing either path so it drops a strict
    /// subset (the historical failure: a replay entry deleted while `last_admitted` survives, so the
    /// market never re-baselines and is invisible to new clients) fails here.
    #[test]
    fn the_single_key_and_the_batch_reset_leave_identical_state() {
        let venue = "BookResetPathAgreement";
        let (mut single, single_replay, keys) = seeded_books(venue);
        let (mut batch, batch_replay, _) = seeded_books(venue);
        let doomed = keys[0].clone();

        // Fixture sanity: the two seeds start identical, and the market about to be dropped really
        // holds all three legs (otherwise the agreement below could be two no-ops agreeing).
        assert_eq!(
            book_legs(&single, &single_replay, &keys),
            book_legs(&batch, &batch_replay, &keys),
            "the two seeds must start identical"
        );
        assert!(single.book_markets.contains_key(&doomed));
        assert_eq!(single.books.last_admitted(&doomed), Some(arm(1)));
        assert!(model::lock(&single_replay).contains_key(&doomed));

        single.reset_book_for_market(&doomed);
        batch.reset_books_for_markets(std::slice::from_ref(&doomed));

        let after_single = book_legs(&single, &single_replay, &keys);
        let after_batch = book_legs(&batch, &batch_replay, &keys);
        assert_eq!(
            after_single, after_batch,
            "the single-key reset and the batch reset must leave identical state on all three legs"
        );

        // ...and the drop actually happened: the doomed market is gone from all three legs and from
        // the eviction queue, while the three untouched peers survive complete.
        assert_eq!(
            after_single.markets.len(),
            3,
            "exactly one market dropped, three survive"
        );
        assert!(!after_single.markets.contains(&doomed));
        assert!(!after_single.order.contains(&doomed));
        assert_eq!(
            after_single.admitted,
            keys.iter()
                .map(|k| (k.clone(), (k != &doomed).then(|| arm(1))))
                .collect::<Vec<_>>(),
            "only the doomed market's last_admitted may drop"
        );
        assert_eq!(
            after_single.replay,
            keys.iter()
                .map(|k| (k.clone(), (k != &doomed).then_some(true)))
                .collect::<Vec<_>>(),
            "only the doomed market's replay entry may drop, and the peers stay baselined"
        );
        assert_eq!(
            after_single.replay_len, 3,
            "the replay map must drop exactly the doomed market's key"
        );
    }

    /// The health seam reaches the authority for a market that has delivered no `book` yet — the MBP
    /// processor reports a `PriceBook` transition before its first batch, and dropping that would let
    /// an arm serve a book it has already declared gapped. The map it grows is bounded there, by
    /// `MAX_TRACKED_MARKETS`.
    #[test]
    fn book_health_reaches_the_authority_before_the_first_batch() {
        let venue = "BookHealthEarly";
        let (mut a, mut rx) = gated(venue, AuthorityConfig::default());
        let fresh: MarketKey = mkey(venue, 999);
        a.set_book_health(&fresh, arm(1), false);
        a.emit(
            book(venue, 999, vec![bid(0.40, 10.0)], true, 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "an arm known gapped here must not serve this market"
        );
    }

    /// The election has one producer: matched cross-arm trade pairs. Observable as a transfer, since
    /// nothing else here can move authority (silence is disabled).
    #[test]
    fn matched_trades_feed_the_election() {
        let venue = "BookMatchedTrades";
        let (mut a, _rx) = gated(venue, sticky_cfg());
        race_trades(&mut a, venue, arm(2), arm(1), 6);
        a.close_authority_windows();
        assert_eq!(a.books.scope_leader(&bscope(venue)), Some(arm(2)));
    }

    /// The one placement error that would make the whole election inert for the venue it was built
    /// for: a FIX-sourced arm stamps every print `trade_id == 0`, and that branch returns early.
    #[test]
    fn the_zero_id_tape_still_feeds_the_matcher() {
        let venue = "BookZeroIdMatcher";
        let (mut a, _rx) = gated(venue, sticky_cfg());
        for i in 0..6u64 {
            let mut t = trade(0);
            t.venue = venue.into();
            t.price = 0.60 + i as f64 / 1_000.0;
            t.recv_ts_ns = 10_000 + i * 1_000_000;
            let mut peer = t.clone();
            peer.recv_ts_ns = t.recv_ts_ns + 50_000;
            a.emit(FeedMessage::Trade(t), arm(2), TEST_CATEGORY);
            a.emit(FeedMessage::Trade(peer), arm(1), TEST_CATEGORY);
        }
        a.close_authority_windows();
        assert_eq!(
            a.books.scope_leader(&bscope(venue)),
            Some(arm(2)),
            "the zero-id tape must still be matched"
        );
    }

    // ---- order-level (L3) book racing ----

    /// The venue most racing tests use. The two that read `dz_mbo_arm_disagreement_total` name their
    /// own instead: that counter is process-global, so sharing a label would make their before/after
    /// deltas depend on test execution order.
    const L3_VENUE: &str = "HYPERLIQUID";

    /// One order-level change: the resting state of `order_id` after some venue event.
    fn order(action: BookAction, order_id: u64, price: f64, size: f64) -> BookChange {
        BookChange {
            action,
            side: BookSide::Bid,
            price,
            size,
            order_id,
        }
    }

    /// An arbiter whose one market is order-level and racing: `Coordinated` mode, and both arms
    /// registered as synced exactly as the Market-by-Order processor reports them.
    fn racing(
        venue: &'static str,
        arms: &[Publisher],
    ) -> (Arbiter, broadcast::Receiver<Arc<FeedMessage>>) {
        let (tx, rx) = broadcast::channel(1024);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        a.set_mode(venue, ArbitrationMode::Coordinated);
        // Wired exactly as `main.rs` wires it: a forced re-baseline republishes from this map.
        a.set_book_replay(Arc::new(std::sync::Mutex::new(BookReplay::default())));
        let key: MarketKey = mkey(venue, BOOK_INSTRUMENT);
        for &p in arms {
            a.set_book_synced(&key, p, true);
        }
        (a, rx)
    }

    /// One order-level batch for the market under test.
    fn l3_batch(venue: &str, changes: Vec<BookChange>, recv_ns: u64) -> FeedMessage {
        let FeedMessage::Book(mut b) = book(venue, BOOK_INSTRUMENT, changes, true, recv_ns) else {
            unreachable!()
        };
        b.symbol = "BTC".into();
        FeedMessage::Book(b)
    }

    fn disagreements(venue: &str) -> u64 {
        metrics()
            .mbo_arm_disagreement
            .with_label_values(&[venue])
            .get()
    }

    /// Two publishers of a distributed venue deliver the same venue events. The first copy of each is
    /// published and the rest collapse, so a consumer sees each event once and always from whichever
    /// publisher was fastest for that event.
    #[test]
    fn order_events_collapse_across_publishers_keeping_first_arrival() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1), arm(2)]);
        let ev = |oid, size| vec![order(BookAction::Update, oid, 100.0, size)];

        a.emit(
            l3_batch(L3_VENUE, ev(7, 10.0), 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            l3_batch(L3_VENUE, ev(7, 10.0), 1_100),
            arm(2),
            TEST_CATEGORY,
        ); // the slower publisher's copy
        a.emit(l3_batch(L3_VENUE, ev(8, 4.0), 1_200), arm(2), TEST_CATEGORY); // this one arm 2 won
        a.emit(l3_batch(L3_VENUE, ev(8, 4.0), 1_300), arm(1), TEST_CATEGORY);

        let books = drain_books(&mut rx);
        assert_eq!(
            books.len(),
            2,
            "each venue event reaches the wire exactly once"
        );
        assert_eq!(books[0].changes[0].order_id, 7);
        assert_eq!(books[1].changes[0].order_id, 8);
    }

    /// Successive partial fills of one order share the id, action and resting price and differ only in
    /// the quantity left. Each is its own venue event: collapsing the second as a duplicate of the
    /// first would leave every consumer holding a quantity the venue has already reduced.
    #[test]
    fn successive_partial_fills_of_one_order_all_reach_the_wire() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1), arm(2)]);
        for (i, remaining) in [10.0, 6.0, 3.0].iter().enumerate() {
            let recv = 1_000 + i as u64;
            let batch = vec![order(BookAction::Update, 7, 100.0, *remaining)];
            a.emit(
                l3_batch(L3_VENUE, batch.clone(), recv),
                arm(1),
                TEST_CATEGORY,
            );
            a.emit(l3_batch(L3_VENUE, batch, recv + 1), arm(2), TEST_CATEGORY);
        }
        let sizes: Vec<f64> = drain_books(&mut rx)
            .iter()
            .map(|b| b.changes[0].size)
            .collect();
        assert_eq!(sizes, vec![10.0, 6.0, 3.0]);
    }

    /// A batch whose events are part new and part already delivered is republished with only the new
    /// ones. Passing it whole would re-deliver an order's absolute quantity after the wire moved on,
    /// walking the consumer's order back to a size the venue already reduced.
    #[test]
    fn a_partly_duplicate_batch_publishes_only_its_new_events() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1), arm(2)]);
        a.emit(
            l3_batch(
                L3_VENUE,
                vec![order(BookAction::Update, 7, 100.0, 6.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            l3_batch(
                L3_VENUE,
                vec![order(BookAction::Update, 7, 100.0, 3.0)],
                1_001,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        // The slower arm's batch carries the already-published 6.0 alongside a genuinely new order.
        a.emit(
            l3_batch(
                L3_VENUE,
                vec![
                    order(BookAction::Update, 7, 100.0, 6.0),
                    order(BookAction::Update, 9, 99.0, 1.0),
                ],
                1_002,
            ),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0]
                .changes
                .iter()
                .map(|c| (c.order_id, c.size))
                .collect::<Vec<_>>(),
            vec![(9, 1.0)],
            "the stale copy of order 7 must not walk the consumer back to 6.0"
        );
    }

    /// A resting order only ever shrinks, so a publisher claiming more is resting than a peer already
    /// reported has missed a fill. The disagreement is counted rather than silently absorbed; what
    /// happens to the market afterwards is
    /// [`a_size_disagreement_forces_a_rebaseline_rather_than_a_guess`].
    #[test]
    fn a_content_disagreement_is_counted() {
        const VENUE: &str = "BookDriftCounted";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let before = disagreements(VENUE);
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 6.0)], 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        // arm 2 missed the fill that took this order to 6, so it thinks 8 is resting.
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 8.0)], 1_001),
            arm(2),
            TEST_CATEGORY,
        );
        assert_eq!(drain_books(&mut rx).len(), 1);
        assert_eq!(disagreements(VENUE), before + 1);
    }

    fn forced_rebaselines(venue: &str, reason: &str) -> u64 {
        metrics()
            .mbo_forced_rebaselines
            .with_label_values(&[venue, reason])
            .get()
    }

    /// Two publishers disagreeing about a resting order's size proves one of them has drifted, and
    /// which is unknowable here. Publishing either is a guess — the larger rewinds the consumer past a
    /// fill the venue already applied, the smaller lets a forged size mute a real order — so the market
    /// stops being served from deltas and re-baselines instead.
    #[test]
    fn a_size_disagreement_forces_a_rebaseline_rather_than_a_guess() {
        const VENUE: &str = "BookDriftRebaseline";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let (before_dis, before_forced) = (
            disagreements(VENUE),
            forced_rebaselines(VENUE, FORCED_DISAGREEMENT),
        );
        // Arm 1 installs its book, so the gate has a complete one to republish.
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 7, 100.0, 6.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        // Arm 2 missed the fill that took order 7 to 6, so it claims 8 is resting.
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 8.0)], 1_100),
            arm(2),
            TEST_CATEGORY,
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "neither publisher's claim may reach the wire"
        );
        assert_eq!(disagreements(VENUE), before_dis + 1);
        assert_eq!(
            forced_rebaselines(VENUE, FORCED_DISAGREEMENT),
            before_forced + 1
        );

        // The next completed logical event re-baselines rather than resuming the delta stream, then
        // lands on top of it — the batch is published, just not onto a book nobody vouches for.
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 9, 99.0, 1.0)], 1_200),
            arm(1),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].changes[0].action, BookAction::Clear);
        assert!(out[0].last, "a re-baseline must terminate its own event");
        assert_eq!(out[1].changes[0].order_id, 9);
    }

    /// A forged source can raise a disagreement against any real order for the price of one datagram,
    /// so the re-baseline that follows must republish what the wire agreed on and never the raising
    /// arm's own book — otherwise the cheapest input on the wire buys wholesale replacement of a
    /// market's book with a fabricated one.
    #[test]
    fn a_forced_rebaseline_republishes_the_wire_not_an_arms_own_book() {
        const VENUE: &str = "BookForgedInjection";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 1, 100.0, 5.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 2, 99.0, 4.0)], 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        // The forged arm's own snapshot is suppressed while a peer serves, then it claims ten times the
        // resting quantity the wire holds for a real order.
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 999, 1.0, 7.0)],
                1_200,
            ),
            arm(2),
            TEST_CATEGORY,
        );
        a.emit(
            l3_batch(
                VENUE,
                vec![order(BookAction::Update, 1, 100.0, 50.0)],
                1_300,
            ),
            arm(2),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        // And discharges the flag it raised.
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 3, 98.0, 1.0)], 1_400),
            arm(2),
            TEST_CATEGORY,
        );
        let out = drain_books(&mut rx);
        let published: Vec<(u64, f64)> = out
            .iter()
            .flat_map(|b| b.changes.iter().map(|c| (c.order_id, c.size)))
            .collect();
        assert_eq!(
            published,
            vec![(0, 0.0), (1, 5.0), (2, 4.0), (3, 1.0)],
            "the wire's own book, then the triggering batch onto it"
        );
    }

    /// A book far larger than the guard's cap must stay proportional: each published change costs one
    /// input change. Re-baselining republishes the whole book, so a mechanism that re-arms itself —
    /// through its own seeding, say — turns a 44k-order market into a republish every other batch, and
    /// no test whose book fits inside the cap can see it.
    #[test]
    fn a_book_larger_than_the_guard_stays_proportional_to_its_input() {
        const VENUE: &str = "BookOverCapProportional";
        const ORDERS: u64 = MAX_SEEN_ORDER_EVENTS as u64 * 2;
        let (mut a, mut rx) = racing(VENUE, &[arm(1)]);
        a.set_book_dedup_window(1_000);
        let before = forced_rebaselines(VENUE, FORCED_GUARD_EVICTED);
        let mut install = vec![clear_both()];
        install.extend((1..=ORDERS).map(|oid| order(BookAction::Update, oid, 100.0, 1.0)));
        a.emit(l3_batch(VENUE, install, 1_000), arm(1), TEST_CATEGORY);
        // Spaced well outside the window, so nothing a peer could be racing is ever evicted: every
        // force here would be the seeding of the install re-arming the guard against itself.
        for oid in 1..=ORDERS {
            a.emit(
                l3_batch(
                    VENUE,
                    vec![order(BookAction::Update, oid, 100.0, 2.0)],
                    2_000 + oid * 10_000,
                ),
                arm(1),
                TEST_CATEGORY,
            );
        }
        let published: usize = drain_books(&mut rx).iter().map(|b| b.changes.len()).sum();
        let key: MarketKey = mkey(VENUE, BOOK_INSTRUMENT);
        assert_eq!(
            forced_rebaselines(VENUE, FORCED_GUARD_EVICTED),
            before,
            "a re-baseline's own seeding must not re-arm the eviction that discharged it"
        );
        assert!(!a.book_markets[&key].rebaseline);
        assert!(
            published <= (ORDERS as usize + 1) * 2,
            "{published} changes published for {ORDERS} input changes"
        );
    }

    /// The cross-publisher guard is bounded, so a burst larger than the cap ages an order out while a
    /// peer's copy of it could still be racing. That must not silently reopen the resurrection path:
    /// the market degrades to a re-baseline instead.
    #[test]
    fn evicting_a_tracked_order_marks_the_market_for_rebaseline() {
        const VENUE: &str = "BookGuardEvicted";
        let (mut a, mut rx) = racing(VENUE, &[arm(1)]);
        let before = forced_rebaselines(VENUE, FORCED_GUARD_EVICTED);
        for oid in 1..=(MAX_SEEN_ORDER_EVENTS as u64 + 1) {
            a.emit(
                l3_batch(
                    VENUE,
                    vec![order(BookAction::Update, oid, 100.0, 1.0)],
                    1_000,
                ),
                arm(1),
                TEST_CATEGORY,
            );
        }
        let _ = drain_books(&mut rx);
        let key: MarketKey = mkey(VENUE, BOOK_INSTRUMENT);
        assert!(
            a.book_markets[&key].rebaseline,
            "a lost guard entry must stop the market being served from deltas"
        );
        assert_eq!(
            forced_rebaselines(VENUE, FORCED_GUARD_EVICTED),
            before + 1,
            "and it must be separable from a disagreement in production"
        );
    }

    /// Evicting an order no peer could still be racing costs the guard nothing, so a book far larger
    /// than the cap streams normally instead of re-baselining on every order it adds.
    #[test]
    fn evicting_an_order_past_the_race_horizon_does_not_force_a_rebaseline() {
        const VENUE: &str = "BookGuardAged";
        let (mut a, mut rx) = racing(VENUE, &[arm(1)]);
        a.set_book_dedup_window(1_000);
        let before = forced_rebaselines(VENUE, FORCED_GUARD_EVICTED);
        for oid in 1..=(MAX_SEEN_ORDER_EVENTS as u64 + 64) {
            a.emit(
                l3_batch(
                    VENUE,
                    vec![order(BookAction::Update, oid, 100.0, 1.0)],
                    oid * 10_000,
                ),
                arm(1),
                TEST_CATEGORY,
            );
        }
        let _ = drain_books(&mut rx);
        let key: MarketKey = mkey(VENUE, BOOK_INSTRUMENT);
        assert!(!a.book_markets[&key].rebaseline);
        assert_eq!(forced_rebaselines(VENUE, FORCED_GUARD_EVICTED), before);
    }

    /// A re-baseline replaces the consumer's book, so prior *events* are no longer duplicates — but the
    /// record of which orders are dead must survive it, or a peer's stale `Add` resurrects one.
    #[test]
    fn a_rebaseline_keeps_the_resurrection_guard() {
        const VENUE: &str = "BookRebaselineGuard";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        a.set_book_dedup_window(1_000);
        let add = vec![order(BookAction::Update, 7, 100.0, 6.0)];
        a.emit(l3_batch(VENUE, add.clone(), 1_000), arm(1), TEST_CATEGORY);
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Delete, 7, 100.0, 0.0)], 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        // Arm 1 gaps and recovers; its snapshot does not contain the dead order 7.
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 9, 99.0, 1.0)],
                1_200,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        // The slow arm's only copy of the add, long after both.
        a.emit(l3_batch(VENUE, add, 9_000_000), arm(2), TEST_CATEGORY);
        assert!(
            drain_books(&mut rx).is_empty(),
            "a re-baseline must not reopen the resurrection path"
        );
    }

    /// A re-baseline's own orders seed the resting floor, so a peer claiming more than the snapshot
    /// holds is still caught as drift rather than read as an unseen order.
    #[test]
    fn a_rebaseline_seeds_the_guard_with_its_own_orders() {
        const VENUE: &str = "BookRebaselineSeed";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let before = disagreements(VENUE);
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 9, 99.0, 5.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 9, 99.0, 8.0)], 1_100),
            arm(2),
            TEST_CATEGORY,
        );
        assert!(drain_books(&mut rx).is_empty());
        assert_eq!(disagreements(VENUE), before + 1);
    }

    /// A publisher that has gone away must not hold a market's re-baseline hostage. Its departure is
    /// known the moment its receiver deregisters; waiting out [`PEER_SERVING_NS`] wedges the surviving
    /// arm, because a suppressed re-baseline is never retried.
    #[test]
    fn a_departed_publisher_does_not_suppress_a_peers_rebaseline() {
        const VENUE: &str = "BookDepartedPeer";
        let (mut a, mut rx) = racing(VENUE, &[arm(1)]);
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 1, 100.0, 1.0)], 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        a.forget_publisher_books(VENUE, arm(1));
        a.emit(
            l3_batch(
                VENUE,
                vec![clear_both(), order(BookAction::Update, 7, 100.0, 6.0)],
                1_100,
            ),
            arm(2),
            TEST_CATEGORY,
        );
        assert_eq!(
            drain_books(&mut rx).len(),
            1,
            "the departed arm's serving claim went with its receiver"
        );
    }

    /// A shrinking sequence across two arms is an ordinary race, not drift: whichever arm delivers the
    /// smaller remainder first simply won that event.
    #[test]
    fn an_interleaved_race_is_not_a_disagreement() {
        const VENUE: &str = "BookRaceNotDrift";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let before = disagreements(VENUE);
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 6.0)], 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 3.0)], 1_001),
            arm(2),
            TEST_CATEGORY,
        );
        assert_eq!(drain_books(&mut rx).len(), 2);
        assert_eq!(disagreements(VENUE), before);
    }

    /// With one live publisher the racing path is a pass-through: no stall, no waiting for a peer.
    #[test]
    fn a_single_publisher_streams_unimpeded() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1)]);
        for oid in 1..=5u64 {
            a.emit(
                l3_batch(
                    L3_VENUE,
                    vec![order(BookAction::Update, oid, 100.0, 1.0)],
                    1_000 + oid,
                ),
                arm(1),
                TEST_CATEGORY,
            );
        }
        assert_eq!(drain_books(&mut rx).len(), 5);
    }

    /// A late copy of an order the market has already published as **gone** is refused however long
    /// after the fact it arrives — the guard order-level racing rests on. The peer's own book still
    /// holds the order (it has not processed the cancel yet), so nothing upstream of the merge point can
    /// see this: only here, where the two publishers share an identity, is it visible.
    #[test]
    fn a_late_copy_cannot_resurrect_a_deleted_order() {
        const VENUE: &str = "BookResurrection";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let before = metrics()
            .book_resurrections_dropped
            .with_label_values(&[VENUE])
            .get();
        a.set_book_dedup_window(1_000);
        let add = vec![order(BookAction::Update, 7, 100.0, 6.0)];
        a.emit(l3_batch(VENUE, add.clone(), 1_000), arm(1), TEST_CATEGORY);
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Delete, 7, 100.0, 0.0)], 1_100),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        // The lagging arm's own copy of the add, far past the dedup window.
        a.emit(l3_batch(VENUE, add, 9_000_000), arm(2), TEST_CATEGORY);
        assert!(
            drain_books(&mut rx).is_empty(),
            "a dead order must not be re-added"
        );
        assert_eq!(
            metrics()
                .book_resurrections_dropped
                .with_label_values(&[VENUE])
                .get(),
            before + 1
        );
    }

    /// A repeat of the removal itself is not a resurrection: it is a no-op the consumer absorbs, so it
    /// goes out rather than being silently withheld.
    #[test]
    fn a_repeated_removal_is_not_treated_as_a_resurrection() {
        const VENUE: &str = "BookRepeatDelete";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        a.set_book_dedup_window(1_000);
        let gone = vec![order(BookAction::Delete, 7, 100.0, 0.0)];
        a.emit(
            l3_batch(VENUE, vec![order(BookAction::Update, 7, 100.0, 6.0)], 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        a.emit(l3_batch(VENUE, gone.clone(), 1_100), arm(1), TEST_CATEGORY);
        let _ = drain_books(&mut rx);
        a.emit(l3_batch(VENUE, gone, 9_000_000), arm(2), TEST_CATEGORY);
        assert_eq!(drain_books(&mut rx).len(), 1);
    }

    /// A copy arriving past the window for a **live** order is a redundant emission at worst, which is
    /// why the window is a cost knob rather than a correctness parameter.
    #[test]
    fn a_copy_past_the_window_re_emits_rather_than_corrupting() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1), arm(2)]);
        a.set_book_dedup_window(1_000);
        let ev = vec![order(BookAction::Update, 7, 100.0, 6.0)];
        a.emit(l3_batch(L3_VENUE, ev.clone(), 1_000), arm(1), TEST_CATEGORY);
        a.emit(l3_batch(L3_VENUE, ev.clone(), 1_500), arm(2), TEST_CATEGORY); // inside the window -> collapsed
        assert_eq!(drain_books(&mut rx).len(), 1);
        a.emit(l3_batch(L3_VENUE, ev, 9_000), arm(2), TEST_CATEGORY); // past it -> re-emitted
        assert_eq!(drain_books(&mut rx).len(), 1);
    }

    /// A recovering publisher must not wipe a consumer that a healthy peer is serving, so its
    /// `Clear`-led re-baseline is dropped while a peer is both synced and actually reaching the wire.
    #[test]
    fn a_rebaseline_is_suppressed_while_a_peer_is_serving() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1)]);
        let key: MarketKey = mkey(L3_VENUE, BOOK_INSTRUMENT);
        a.set_book_synced(&key, arm(2), false);
        // Arm 1 is serving: a claim from an arm that has published nothing does not suppress.
        a.emit(
            l3_batch(
                L3_VENUE,
                vec![order(BookAction::Update, 1, 100.0, 1.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        let _ = drain_books(&mut rx);

        a.emit(
            l3_batch(
                L3_VENUE,
                vec![clear_both(), order(BookAction::Update, 7, 100.0, 6.0)],
                1_100,
            ),
            arm(2),
            TEST_CATEGORY,
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "arm 1 is serving this market"
        );
    }

    /// A publisher that reports itself synced and then never reaches the wire — drained host, withdrawn
    /// group, a forged source that went quiet — must not suppress the surviving arm's re-baseline. A
    /// re-baseline is this product's only self-heal, so a claim that never expires wedges the market for
    /// the life of the process.
    #[test]
    fn a_claim_from_a_publisher_that_never_serves_does_not_suppress() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1)]);
        let key: MarketKey = mkey(L3_VENUE, BOOK_INSTRUMENT);
        a.set_book_synced(&key, arm(2), true);
        let _ = drain_books(&mut rx);

        a.emit(
            l3_batch(
                L3_VENUE,
                vec![clear_both(), order(BookAction::Update, 7, 100.0, 6.0)],
                1_000,
            ),
            arm(1),
            TEST_CATEGORY,
        );
        assert_eq!(drain_books(&mut rx).len(), 1);
    }

    /// A market evicted from the tracked set must not silently revert to the single-arm authority: its
    /// next order-level batch re-establishes the routing from its own content.
    #[test]
    fn an_evicted_market_still_routes_as_order_level() {
        const VENUE: &str = "BookEvictedRouting";
        let (mut a, mut rx) = racing(VENUE, &[arm(1), arm(2)]);
        let key: MarketKey = mkey(VENUE, BOOK_INSTRUMENT);
        a.reset_book_for_market(&key);
        let _ = drain_books(&mut rx);

        let ev = vec![order(BookAction::Update, 7, 100.0, 6.0)];
        a.emit(l3_batch(VENUE, ev.clone(), 1_000), arm(1), TEST_CATEGORY);
        a.emit(l3_batch(VENUE, ev, 1_100), arm(2), TEST_CATEGORY);
        assert_eq!(
            drain_books(&mut rx).len(),
            1,
            "the peer's copy must still collapse, not be gated by arm election"
        );
    }

    /// With every publisher recovering there is nothing to protect, so the re-baseline goes out — and
    /// exactly once, however many publishers recover together, because the arm that publishes reports
    /// itself synced before doing so.
    #[test]
    fn simultaneous_recoveries_produce_exactly_one_rebaseline() {
        let (mut a, mut rx) = racing(L3_VENUE, &[]);
        let key: MarketKey = mkey(L3_VENUE, BOOK_INSTRUMENT);
        a.set_book_synced(&key, arm(1), false);
        a.set_book_synced(&key, arm(2), false);
        let _ = drain_books(&mut rx);

        let rebaseline = vec![clear_both(), order(BookAction::Update, 7, 100.0, 6.0)];
        // Each arm reports itself synced as it installs its snapshot, then publishes.
        a.set_book_synced(&key, arm(1), true);
        a.emit(
            l3_batch(L3_VENUE, rebaseline.clone(), 1_000),
            arm(1),
            TEST_CATEGORY,
        );
        a.set_book_synced(&key, arm(2), true);
        a.emit(l3_batch(L3_VENUE, rebaseline, 1_001), arm(2), TEST_CATEGORY);
        assert_eq!(drain_books(&mut rx).len(), 1);
    }

    /// The racing window is bounded by event count as well as by time, so a publisher that stalls the
    /// clock cannot grow one market's state without limit.
    #[test]
    fn the_racing_window_is_bounded_by_event_count() {
        let (mut a, mut rx) = racing(L3_VENUE, &[arm(1)]);
        for oid in 0..(MAX_SEEN_ORDER_EVENTS as u64 + 512) {
            a.emit(
                l3_batch(
                    L3_VENUE,
                    vec![order(BookAction::Update, oid + 1, 100.0, 1.0)],
                    1_000,
                ),
                arm(1),
                TEST_CATEGORY,
            );
        }
        let _ = drain_books(&mut rx);
        let key: MarketKey = mkey(L3_VENUE, BOOK_INSTRUMENT);
        let events = &a.book_events[&key];
        assert!(events.seen.len() <= MAX_SEEN_ORDER_EVENTS);
        assert!(events.resting.len() <= MAX_SEEN_ORDER_EVENTS);
    }

    /// A market evicted from the tracked set takes its racing state with it, or the two maps keyed by
    /// wire-supplied ids would grow past the cap `book_markets` enforces.
    #[test]
    fn eviction_drops_the_racing_state_too() {
        let (mut a, _rx) = racing(L3_VENUE, &[arm(1)]);
        for id in 0..(MAX_BOOK_MARKETS as u32 + 8) {
            let key: MarketKey = mkey(L3_VENUE, id);
            a.set_book_synced(&key, arm(1), true);
        }
        assert_eq!(a.book_markets.len(), MAX_BOOK_MARKETS);
        assert!(a.book_sync.len() <= MAX_BOOK_MARKETS);
        assert!(a.book_events.len() <= MAX_BOOK_MARKETS);
    }
}
