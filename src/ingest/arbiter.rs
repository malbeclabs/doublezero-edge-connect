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

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use prometheus::{Histogram, IntCounter};
use tokio::sync::broadcast;

use crate::{
    metrics::metrics,
    model::{now_ns, FeedMessage, NormalizedQuote},
};

/// Default number of recent `trade_id`s remembered per `(venue, symbol)` for cross-source trade
/// dedup. Const for now; promote to config alongside a multi-publisher trade test that can size it.
pub const TRADE_DEDUP_WINDOW: usize = 8192;

/// Cap on distinct leader BBOs tracked per `source_ts` tick by the quote floor — a safety bound so a
/// stalled/repeated `source_ts` can't grow the per-tick set without limit. Far above the real
/// per-block max (~hundreds of distinct BBOs share one HL block timestamp), so it never evicts in
/// normal operation.
pub const QUOTE_TICK_CAP: usize = 8192;

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
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct QuoteId {
    bid_px: i64,
    bid_sz: i64,
    ask_px: i64,
    ask_sz: i64,
    bid_n: u16,
    ask_n: u16,
}

impl QuoteId {
    /// The canonical content identity of a normalized quote: each BBO `f64` rounded to a `10^-8`
    /// fixed-point integer (so two sources' encodings of the same price collapse), plus the counts.
    pub fn of(q: &NormalizedQuote) -> Self {
        let canon = |x: f64| (x * 10f64.powi(CANONICAL_BBO_EXP)).round() as i64;
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

/// The outcome of a de-dup admission decision, shared by [`StalenessFloor`] and [`WindowedDedup`].
/// The caller forwards on [`Admit::Emitted`] and drops otherwise; [`Admit::Contest`] additionally
/// reports a *cross-source* head-to-head: another publisher already won this identity, and this is
/// the first losing copy from a different publisher, arriving `lead_ns` after the `winner` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit<P> {
    /// Forwarded: opened a new tick / new content (floor), or a first-seen identity (window).
    Emitted,
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
        matches!(self, Admit::Emitted)
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
                Admit::Emitted
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
                    Admit::Emitted
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
                    Admit::Emitted
                } else {
                    Admit::Dropped
                }
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
        Admit::Emitted
    }
}

/// The shared emit stage: owns the broadcast `Sender` plus the dedup state, and exposes one
/// `emit(msg, publisher)` entry point every ingest source funnels through. Quotes pass through the
/// per-`(venue, symbol)` latch-to-leader [`StalenessFloor`] (keyed on [`QuoteId`], `P = Publisher`),
/// trades through the [`WindowedDedup`] on `trade_id`, and everything else
/// (`Instrument`/`Midpoint`/`Depth`/`Status`) is broadcast unchanged. Wrapped in
/// [`SharedArbiter`] so the multicast receiver tasks and the WS feeder share one instance — hence one
/// floor per `(venue, symbol)`, on which all sources race.
pub struct Arbiter {
    tx: broadcast::Sender<Arc<FeedMessage>>,
    quotes: StalenessFloor<(Arc<str>, Arc<str>), QuoteId, Publisher>,
    trades: WindowedDedup<(Arc<str>, Arc<str>), u64, Publisher>,
    /// Per-venue pre-resolved metric children, so `emit` increments a cached handle instead of doing
    /// a `with_label_values` label-map lookup per message (mirrors the `SeqEvents` pattern in the
    /// receiver). Populated lazily on the first message for each venue; venues are a tiny fixed set.
    venue_metrics: HashMap<Arc<str>, VenueMetrics>,
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
    /// `dz_emit_total{kind}` indexed by kind: quote/trade/instrument/midpoint/depth/status.
    emit: [IntCounter; 6],
    /// `dz_quotes_admitted_total{publisher}` / `dz_trades_admitted_total{publisher}`, `[edge, public]`.
    quotes_admitted: [IntCounter; 2],
    trades_admitted: [IntCounter; 2],
    quotes_dropped: IntCounter,
    trades_dropped: IntCounter,
    quotes_future_rejected: IntCounter,
    quotes_no_source_ts: IntCounter,
    /// `dz_quote_lead_ns{winner,loser}` / `dz_trade_lead_ns{winner,loser}` indexed
    /// `winner_idx * 2 + loser_idx` over `[edge, public]`.
    quote_lead: [Histogram; 4],
    trade_lead: [Histogram; 4],
}

impl VenueMetrics {
    fn new(venue: &str) -> Self {
        let m = metrics();
        let emit_kind = |k: &str| m.emit.with_label_values(&[venue, k]);
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
            ],
            quotes_admitted: [
                m.quotes_admitted.with_label_values(&[venue, "edge"]),
                m.quotes_admitted.with_label_values(&[venue, "public"]),
            ],
            trades_admitted: [
                m.trades_admitted.with_label_values(&[venue, "edge"]),
                m.trades_admitted.with_label_values(&[venue, "public"]),
            ],
            quotes_dropped: m.quotes_dropped.with_label_values(&[venue]),
            trades_dropped: m.trades_dropped.with_label_values(&[venue]),
            quotes_future_rejected: m.quotes_future_rejected.with_label_values(&[venue]),
            quotes_no_source_ts: m.quotes_no_source_ts.with_label_values(&[venue]),
            quote_lead: lead(&m.quote_lead_ns),
            trade_lead: lead(&m.trade_lead_ns),
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

impl Arbiter {
    pub fn new(tx: broadcast::Sender<Arc<FeedMessage>>, trade_window: usize) -> Self {
        Self {
            tx,
            quotes: StalenessFloor::new(QUOTE_TICK_CAP),
            trades: WindowedDedup::new(trade_window),
            venue_metrics: HashMap::new(),
        }
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

    /// Apply the appropriate dedup and broadcast if the message survives it. `publisher` is the
    /// source racing for the quote floor's per-tick leadership; it is ignored for non-quote
    /// messages. The send result is ignored: a no-subscriber send desyncs no one, and a unique
    /// update dropped by a slow per-client channel is unrecoverable regardless.
    ///
    /// Metric children are pre-resolved per venue (see [`VenueMetrics`]) so this per-message path
    /// increments a cached handle rather than doing a label-map lookup for each counter.
    pub fn emit(&mut self, msg: FeedMessage, publisher: Publisher) {
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
                    Admit::Emitted => {
                        vm.emit[EMIT_QUOTE].inc();
                        // Attribute the admitted quote to its winning publisher. A rise in
                        // `publisher="public"` is the direct signal of the public backstop filling
                        // an edge gap (in steady state the edge publisher leads every tick).
                        vm.quotes_admitted[pub_idx(publisher)].inc();
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
                let key = (t.venue.clone(), t.symbol.clone());
                let decision = self.trades.admit(key, t.trade_id, publisher, t.recv_ts_ns);
                let vm = self.vm(&t.venue);
                match decision {
                    Admit::Emitted => {
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
                self.vm(&i.venue).emit[EMIT_INSTRUMENT].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
            FeedMessage::Midpoint(mp) => {
                self.vm(&mp.venue).emit[EMIT_MIDPOINT].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
            FeedMessage::Depth(d) => {
                self.vm(&d.venue).emit[EMIT_DEPTH].inc();
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

    use crate::model::{NormalizedQuote, Side};

    fn quote(source_ts_ns: u64, bid: f64, ask: f64) -> NormalizedQuote {
        NormalizedQuote {
            venue: "Hyperliquid".into(),
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
                venue: "Hyperliquid".into(),
                symbol: "BTC".into(),
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
        a.emit(trade(7), edge);
        a.emit(trade(7), Publisher::PublicWs); // same id from public -> dropped
        a.emit(trade(8), Publisher::PublicWs);
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Trade(t) = &*m {
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
                venue: "Hyperliquid".into(),
                symbol: "BTC".into(),
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
        a.emit(trade(), edge);
        a.emit(trade(), edge); // identical duplicate -> dropped
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
            q.venue = venue.into();
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
            q.venue = venue.into();
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
                venue: venue.into(),
                symbol: "BTC".into(),
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
}
