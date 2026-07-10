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
//!   in-window reorder is dropped, but every distinct print is kept.
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

use tokio::sync::broadcast;

use crate::{
    metrics::metrics,
    model::{self, now_ns, DepthSnapshot, FeedMessage, NormalizedDepth, NormalizedQuote},
};

/// Default number of recent `trade_id`s remembered per `(venue, symbol)` for cross-source trade
/// dedup. Const for now; promote to config alongside a multi-publisher trade test that can size it.
pub const TRADE_DEDUP_WINDOW: usize = 8192;

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
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    tx: broadcast::Sender<FeedMessage>,
    /// Cross-source dedup for quotes. Deliberately EXEMPT from the session-reset escape hatch the
    /// depth floor gets (see `depths` below): the TOB `source_ts` is epoch block time, monotonic
    /// across sessions by construction, so a session boundary cannot restart it below the latched
    /// high-water — and 0, the "not available" sentinel, bypasses this floor entirely. Revisit if
    /// a venue with a session-scoped quote clock is ever added.
    quotes: StalenessFloor<(String, String), QuoteId, Publisher>,
    trades: WindowedDedup<(String, String), u64, Publisher>,
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
    depths: StalenessFloor<(String, String), DepthId, Publisher>,
    /// Shared latest-`depth` map the WS server replays on connect, keyed `(venue, symbol)`. Written
    /// here — on the floor's **admit** decision — so the replayed snapshot is always the *leader's*
    /// broadcast book, never a non-leader publisher's (possibly divergent) copy that never crossed
    /// the floor. `None` when no WS replay map is wired (e.g. unit tests that only inspect the
    /// broadcast). The `MboProcessor` still purges this map on book eviction (bounding); it no longer
    /// writes it (single writer = this admit branch). Also purged by `reset_depth_floor_for_*`
    /// (the session-reset escape hatch), so a client connecting across a session boundary is not
    /// replayed the ended session's final book — see those methods' docs.
    depth_replay: Option<DepthSnapshot>,
}

impl Arbiter {
    pub fn new(tx: broadcast::Sender<FeedMessage>, trade_window: usize) -> Self {
        Self {
            tx,
            quotes: StalenessFloor::new(QUOTE_TICK_CAP),
            trades: WindowedDedup::new(trade_window),
            depths: StalenessFloor::new(DEPTH_TICK_CAP),
            depth_replay: None,
        }
    }

    /// Wire the shared WS-replay `depth` map so the arbiter updates it on each admitted (leader)
    /// depth. The bridge calls this once at startup; without it the depth floor still dedups the
    /// broadcast but maintains no replay snapshot.
    pub fn set_depth_replay(&mut self, depth: DepthSnapshot) {
        self.depth_replay = Some(depth);
    }

    /// The broadcast sender, so output sinks can `subscribe()` and `Status` can be sent directly
    /// (it carries no business identity to dedup).
    pub fn sender(&self) -> &broadcast::Sender<FeedMessage> {
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
        let cleared = self.depths.reset_where(|(v, _)| v == venue);
        if let Some(replay) = &self.depth_replay {
            model::lock(replay).retain(|(v, _), _| v != venue);
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
        let cleared = self.depths.reset_where(|(v, s)| v == venue && s == symbol);
        if let Some(replay) = &self.depth_replay {
            model::lock(replay).remove(&(venue.to_string(), symbol.to_string()));
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

    /// Apply the appropriate dedup and broadcast if the message survives it. `publisher` is the
    /// source racing for the quote floor's per-tick leadership; it is ignored for non-quote
    /// messages. The send result is ignored: a no-subscriber send desyncs no one, and a unique
    /// update dropped by a slow per-client channel is unrecoverable regardless.
    pub fn emit(&mut self, msg: FeedMessage, publisher: Publisher) {
        let m = metrics();
        match &msg {
            FeedMessage::Quote(q) => {
                // `source_ts == 0` is the "not available" sentinel (per CLAUDE.md, never a real
                // time): forward it but never let it touch the floor — as a tick it would pin
                // `high_water` at 0 and drop every later quote as stale.
                if q.source_ts_ns == 0 {
                    m.quotes_no_source_ts.with_label_values(&[&q.venue]).inc();
                    m.emit.with_label_values(&[q.venue.as_str(), "quote"]).inc();
                    let _ = self.tx.send(msg);
                    return;
                }
                // Reject an implausibly-far-future `source_ts` before it can advance the floor.
                // The floor is shared by the trusted edge and the untrusted public WS; one bad/
                // hostile public timestamp years ahead would otherwise latch `high_water` and drop
                // every real edge quote as stale until restart (see `MAX_FUTURE_SKEW_NS`).
                if q.source_ts_ns > now_ns().saturating_add(MAX_FUTURE_SKEW_NS) {
                    m.quotes_future_rejected
                        .with_label_values(&[&q.venue])
                        .inc();
                    return;
                }
                let key = (q.venue.clone(), q.symbol.clone());
                // `recv_ts_ns` is the cross-source-comparable arrival clock (host wall clock,
                // sampled for both the edge receiver and the public WS feeder).
                match self.quotes.admit(
                    key,
                    q.source_ts_ns,
                    QuoteId::of(q),
                    publisher,
                    q.recv_ts_ns,
                ) {
                    Admit::Emitted { opened_tick } => {
                        m.emit.with_label_values(&[q.venue.as_str(), "quote"]).inc();
                        // Attribute the admitted quote to its winning publisher. A rise in
                        // `publisher="public"` is the direct signal of the public backstop filling
                        // an edge gap (in steady state the edge publisher leads every tick).
                        m.quotes_admitted
                            .with_label_values(&[q.venue.as_str(), publisher.label()])
                            .inc();
                        // Once per tick: the class whose copy opened this `source_ts` won the
                        // tick. Unlike the contest histogram (in-tick head-to-heads only), every
                        // tick counts exactly once — this is the published win-rate primitive.
                        if opened_tick {
                            m.quote_ticks_won
                                .with_label_values(&[q.venue.as_str(), publisher.label()])
                                .inc();
                        }
                        let _ = self.tx.send(msg);
                    }
                    // A cross-source follower lost this tick: record how far the winner led, on top
                    // of the plain drop count. The losing copy is `publisher` (the non-leader at
                    // this tick) — labelling both ends keeps an edge-vs-edge mirror race out of the
                    // headline edge-vs-public margin (see `quote_lead_ns` docs).
                    Admit::Contest { winner, lead_ns } => {
                        m.quotes_dropped.with_label_values(&[&q.venue]).inc();
                        m.quote_lead_ns
                            .with_label_values(&[
                                q.venue.as_str(),
                                winner.label(),
                                publisher.label(),
                            ])
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        m.quotes_dropped.with_label_values(&[&q.venue]).inc();
                    }
                }
            }
            FeedMessage::Trade(t) => {
                let key = (t.venue.clone(), t.symbol.clone());
                match self.trades.admit(key, t.trade_id, publisher, t.recv_ts_ns) {
                    Admit::Emitted { .. } => {
                        m.emit.with_label_values(&[t.venue.as_str(), "trade"]).inc();
                        m.trades_admitted
                            .with_label_values(&[t.venue.as_str(), publisher.label()])
                            .inc();
                        let _ = self.tx.send(msg);
                    }
                    Admit::Contest { winner, lead_ns } => {
                        m.trades_dropped.with_label_values(&[&t.venue]).inc();
                        m.trade_lead_ns
                            .with_label_values(&[
                                t.venue.as_str(),
                                winner.label(),
                                publisher.label(),
                            ])
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        m.trades_dropped.with_label_values(&[&t.venue]).inc();
                    }
                }
            }
            // Passthrough kinds (no dedup). Enumerated explicitly rather than via a catch-all so a
            // future `FeedMessage` variant is a compile error here, not a silent miss / runtime panic.
            FeedMessage::Instrument(i) => {
                m.emit
                    .with_label_values(&[i.venue.as_str(), "instrument"])
                    .inc();
                let _ = self.tx.send(msg);
            }
            FeedMessage::Midpoint(mp) => {
                m.emit
                    .with_label_values(&[mp.venue.as_str(), "midpoint"])
                    .inc();
                let _ = self.tx.send(msg);
            }
            FeedMessage::Depth(d) => {
                // Reject an implausibly-far-future `source_ts` before it can advance the floor — the
                // book event timestamp is venue/wire data and the source IP is spoofable, so one
                // forged far-future depth would otherwise latch `high_water` ahead and wedge depth
                // for that symbol until restart (mirrors the quote arm; see `MAX_FUTURE_SKEW_NS`).
                if d.source_ts_ns > now_ns().saturating_add(MAX_FUTURE_SKEW_NS) {
                    m.depth_future_rejected.with_label_values(&[&d.venue]).inc();
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
                match self.depths.admit(
                    key,
                    d.source_ts_ns,
                    DepthId::of(d),
                    publisher,
                    d.recv_ts_ns,
                ) {
                    Admit::Emitted { opened_tick } => {
                        m.emit.with_label_values(&[d.venue.as_str(), "depth"]).inc();
                        // Attribute the admitted depth to its winning publisher — the depth mirror of
                        // `quotes_admitted`. A rise for a given source shows which publisher currently
                        // leads the reconstructed book (and, were a public depth backstop ever added,
                        // `publisher="public"` would flag it filling an edge gap).
                        m.depth_admitted
                            .with_label_values(&[d.venue.as_str(), publisher.label()])
                            .inc();
                        // Once per tick, mirroring the quote arm's win-rate primitive.
                        if opened_tick {
                            m.depth_ticks_won
                                .with_label_values(&[d.venue.as_str(), publisher.label()])
                                .inc();
                        }
                        // Update the WS-replay snapshot with the leader's admitted book, so a client
                        // connecting mid-stream replays exactly what was broadcast (not a dropped
                        // non-leader's divergent copy).
                        if let Some(replay) = &self.depth_replay {
                            model::lock(replay)
                                .insert((d.venue.clone(), d.symbol.clone()), d.clone());
                        }
                        let _ = self.tx.send(msg);
                    }
                    // A cross-publisher follower lost this depth tick: record how far the winner led
                    // (the depth mirror of `quote_lead_ns`), on top of the drop count attributed to
                    // the losing publisher class (which source is *losing* the book race — the
                    // symmetric counterpart of `depth_admitted`'s winner attribution).
                    Admit::Contest { winner, lead_ns } => {
                        m.depth_dropped
                            .with_label_values(&[d.venue.as_str(), publisher.label()])
                            .inc();
                        m.depth_lead_ns
                            .with_label_values(&[
                                d.venue.as_str(),
                                winner.label(),
                                publisher.label(),
                            ])
                            .observe(lead_ns as f64);
                    }
                    Admit::Dropped => {
                        m.depth_dropped
                            .with_label_values(&[d.venue.as_str(), publisher.label()])
                            .inc();
                    }
                }
            }
            // `Status` is currently never routed through `emit` — receivers send it straight via
            // `sender()` (see `emit_status`), and no other source produces it — so `dz_emit_total
            // {kind="status"}` is unreachable in practice today. The arm is kept for match
            // exhaustiveness and stays correct if a future source ever emits status through here.
            FeedMessage::Status(s) => {
                m.emit
                    .with_label_values(&[s.venue.as_str(), "status"])
                    .inc();
                let _ = self.tx.send(msg);
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

    use crate::model::NormalizedQuote;

    fn quote(source_ts_ns: u64, bid: f64, ask: f64) -> NormalizedQuote {
        NormalizedQuote {
            venue: "Hyperliquid".to_string(),
            symbol: "BTC".to_string(),
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
    fn drain_quotes(rx: &mut broadcast::Receiver<FeedMessage>) -> Vec<(u64, f64)> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Quote(q) = m {
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
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge);
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge); // exact repeat -> dropped
        a.emit(FeedMessage::Quote(quote(1000, 100.5, 101.0)), edge); // new content same tick -> kept
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
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge);
        a.emit(
            FeedMessage::Quote(quote(1000, 100.0, 101.0)),
            Publisher::PublicWs,
        );
        // Edge gaps: the public feed opens the next tick and fills in.
        a.emit(
            FeedMessage::Quote(quote(1001, 100.2, 101.2)),
            Publisher::PublicWs,
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
                venue: "Hyperliquid".to_string(),
                symbol: "BTC".to_string(),
                price: 100.0,
                size: 1.0,
                aggressor_side: "buy".to_string(),
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
        a.emit(trade(7), edge);
        a.emit(trade(7), Publisher::PublicWs); // same id from public -> dropped
        a.emit(trade(8), Publisher::PublicWs);
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Trade(t) = m {
                ids.push(t.trade_id);
            }
        }
        assert_eq!(ids, vec![7, 8]);
    }

    /// Two byte-for-byte identical quote packets from the *same* multicast publisher collapse to a
    /// single emission: the second is an exact `(source_ts, content)` repeat the floor drops. This
    /// isolates the pure duplicate-packet case (no third distinct quote).
    #[test]
    fn duplicate_quote_packet_from_same_source_emitted_once() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge);
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge); // identical duplicate -> dropped
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
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), pub_a); // A opens the tick -> emit
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), pub_b); // B's mirror -> non-leader, dropped
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
                venue: "Hyperliquid".to_string(),
                symbol: "BTC".to_string(),
                price: 100.0,
                size: 1.0,
                aggressor_side: "buy".to_string(),
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
        a.emit(trade(), edge);
        a.emit(trade(), edge); // identical duplicate -> dropped
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Trade(t) = m {
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
        );
        // The real edge quote (at ~now) is not stale relative to the floor and still emits.
        a.emit(FeedMessage::Quote(quote(now, 100.0, 101.0)), edge);
        assert_eq!(drain_quotes(&mut rx), vec![(now, 100.0)]);
    }

    /// `source_ts == 0` (the "not available" sentinel) bypasses the floor: it is emitted but never
    /// latched, so it can't pin `high_water` at 0 and drop later quotes / non-leaders forever.
    #[test]
    fn arbiter_zero_source_ts_bypasses_floor() {
        let edge = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(FeedMessage::Quote(quote(0, 100.0, 101.0)), edge); // bypass -> emitted, floor untouched
        a.emit(
            FeedMessage::Quote(quote(0, 100.5, 101.0)),
            Publisher::PublicWs,
        ); // also bypass -> emitted
        a.emit(FeedMessage::Quote(quote(1000, 100.0, 101.0)), edge); // real tick still emits
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
            q.venue = venue.to_string();
            q.recv_ts_ns = recv;
            FeedMessage::Quote(q)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        // Edge opens tick 1000 arriving at t=100; the public copy at the same tick arrives at t=150
        // -> contest, edge led the public copy by 50ns.
        a.emit(mk(1000, 100, 100.0), edge);
        a.emit(mk(1000, 150, 100.5), Publisher::PublicWs);

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
            q.venue = venue.to_string();
            q.recv_ts_ns = recv;
            FeedMessage::Quote(q)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk(100, 100.0), mirror_a); // A opens the tick
        a.emit(mk(120, 100.0), mirror_b); // B's mirror copy loses by 20ns

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
                venue: venue.to_string(),
                symbol: "BTC".to_string(),
                price: 100.0,
                size: 1.0,
                aggressor_side: "buy".to_string(),
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
        a.emit(trade(100), edge); // edge delivers id 7 first at t=100
        a.emit(trade(175), Publisher::PublicWs); // public's copy loses by 75ns

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
            venue: "Hyperliquid".to_string(),
            symbol: "BTC".to_string(),
            bids,
            asks,
            source_ts_ns,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// Drain every emitted depth's `(source_ts, top bid px)` from a receiver (0.0 if no bid).
    fn drain_depths(rx: &mut broadcast::Receiver<FeedMessage>) -> Vec<(u64, f64)> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Depth(d) = m {
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
        a.emit(FeedMessage::Depth(depth(0, vec![], vec![])), pub_a); // A opens tick 0 -> emit
        a.emit(FeedMessage::Depth(depth(0, vec![], vec![])), pub_b); // B's identical anchor -> dropped
                                                                     // A real later event re-advances the floor (no wedge from the latched 0 tick).
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_b,
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
        ); // A leads tick 1000
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[100.0, 1.0]], vec![])),
            pub_b,
        ); // B mirror -> dropped
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[99.0, 1.0]], vec![])),
            pub_b,
        ); // B divergent same tick -> still dropped (non-leader)
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[101.0, 1.0]], vec![])),
            pub_a,
        ); // A's own new content same tick -> kept
        a.emit(
            FeedMessage::Depth(depth(1001, vec![[102.0, 1.0]], vec![])),
            pub_b,
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
        );
        a.emit(
            FeedMessage::Depth(depth(1999, vec![[99.0, 1.0]], vec![])),
            pub_b,
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
        ); // 1h ahead -> rejected
        a.emit(
            FeedMessage::Depth(depth(now, vec![[100.0, 1.0]], vec![])),
            edge,
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
            d.venue = venue.to_string();
            d.recv_ts_ns = recv;
            FeedMessage::Depth(d)
        };
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        // A opens tick 1000 at t=200; B's copy of the same tick arrives at t=290 -> contest, A led 90.
        a.emit(mk(200, 100.0), pub_a);
        a.emit(mk(290, 100.5), pub_b);

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
        ); // A leads tick 1000 -> admitted, recorded
        a.emit(
            FeedMessage::Depth(depth(1000, vec![[99.0, 2.0]], vec![])),
            pub_b,
        ); // B's divergent copy at same tick -> dropped, must NOT overwrite replay
        let map = model::lock(&replay);
        let entry = map
            .get(&("Hyperliquid".to_string(), "BTC".to_string()))
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
            d.venue = venue.to_string();
            d.symbol = symbol.to_string();
            FeedMessage::Depth(d)
        };
        let key = |venue: &str, symbol: &str| (venue.to_string(), symbol.to_string());
        let (tx, _rx) = broadcast::channel(64);
        let replay: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut a = Arbiter::new(tx, 8);
        a.set_depth_replay(replay.clone());
        a.emit(mk("VenueA", "BTC"), edge);
        a.emit(mk("VenueA", "ETH"), edge);
        a.emit(mk("VenueB", "BTC"), edge);
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
            d.venue = venue.to_string();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk(5000, 100.0), edge); // latches high_water at 5000
        a.emit(mk(100, 99.0), edge); // post-restart lower tick -> stale, dropped (the wedge)
        a.reset_depth_floor_for_venue(venue, "end_of_session");
        a.emit(mk(100, 99.0), edge); // floor cleared -> re-opens the tick, admitted
        let ts: Vec<u64> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = m {
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
            d.venue = venue.to_string();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk("VenueA", 5000), edge);
        a.emit(mk("VenueB", 5000), edge);
        a.reset_depth_floor_for_venue("VenueA", "end_of_session");
        a.emit(mk("VenueA", 100), edge); // cleared -> admitted
        a.emit(mk("VenueB", 100), edge); // untouched -> still stale, dropped
        let seen: Vec<(String, u64)> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = m {
                    out.push((d.venue, d.source_ts_ns));
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
            d.symbol = symbol.to_string();
            FeedMessage::Depth(d)
        };
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(mk("BTC", 5000), edge);
        a.emit(mk("ETH", 5000), edge);
        a.reset_depth_floor_for_symbol("Hyperliquid", "BTC", "instrument_reset");
        a.emit(mk("BTC", 100), edge); // cleared -> admitted
        a.emit(mk("ETH", 100), edge); // untouched -> still stale, dropped
        let seen: Vec<(String, u64)> = {
            let mut out = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let FeedMessage::Depth(d) = m {
                    out.push((d.symbol, d.source_ts_ns));
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
            venue: venue.to_string(),
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
        a.emit(FeedMessage::Quote(quote_at(venue, 1000, 100.0)), edge_a); // edge opens tick 1000
        a.emit(FeedMessage::Quote(quote_at(venue, 1000, 100.5)), edge_a); // in-tick content: no re-count
        a.emit(FeedMessage::Quote(quote_at(venue, 1000, 100.0)), edge_b); // mirror copy: no count
        a.emit(
            FeedMessage::Quote(quote_at(venue, 1000, 100.0)),
            Publisher::PublicWs,
        ); // late public copy: no count
        a.emit(
            FeedMessage::Quote(quote_at(venue, 2000, 101.0)),
            Publisher::PublicWs,
        ); // public opens tick 2000
        a.emit(FeedMessage::Quote(quote_at(venue, 3000, 102.0)), edge_a); // walkover tick 3000
        a.emit(FeedMessage::Quote(quote_at(venue, 0, 99.0)), edge_a); // sentinel: bypass, no count
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
            venue: venue.to_string(),
            ..depth(source_ts_ns, bids, vec![])
        };
        let edge_a = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let edge_b = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        let (tx, _rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, 8);
        a.emit(FeedMessage::Depth(depth_at(0, vec![])), edge_a); // A's empty anchor opens tick 0
        a.emit(FeedMessage::Depth(depth_at(0, vec![])), edge_b); // B's identical anchor: no re-count
        a.emit(
            FeedMessage::Depth(depth_at(1000, vec![[100.0, 1.0]])),
            edge_b,
        ); // B opens tick 1000
        a.emit(
            FeedMessage::Depth(depth_at(2000, vec![[100.0, 2.0]])),
            Publisher::PublicWs,
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
}
