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
    /// Per key: (high_water source_ts, leader publisher at that tick, leader's content set + FIFO
    /// insertion order at it — the deque bounds the set to `tick_cap`).
    state: HashMap<K, (u64, P, HashSet<V>, VecDeque<V>)>,
    /// Cap on distinct leader contents tracked at one tick (safety bound; see type docs).
    tick_cap: usize,
}

impl<K: Eq + Hash, V: Eq + Hash + Clone, P: Eq> StalenessFloor<K, V, P> {
    pub fn new(tick_cap: usize) -> Self {
        Self {
            state: HashMap::new(),
            tick_cap,
        }
    }

    /// True (recording it) iff this `(source_ts, content)` from `publisher` should be emitted under
    /// the latch-to-leader floor (see the type docs for the three cases). The per-tick content set is
    /// FIFO-bounded to `tick_cap`.
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the broadcast
    /// channel, the only delivery step. A no-subscriber send desyncs no one, and a unique quote
    /// dropped by a slow per-client channel is unrecoverable regardless — so there is no failed-send
    /// path on which the floor must avoid advancing.
    pub fn admit(&mut self, key: K, source_ts: u64, content: V, publisher: P) -> bool {
        use std::collections::hash_map::Entry;
        match self.state.entry(key) {
            Entry::Vacant(v) => {
                let mut set = HashSet::new();
                set.insert(content.clone());
                v.insert((source_ts, publisher, set, VecDeque::from([content])));
                true
            }
            Entry::Occupied(mut o) => {
                let (high_water, leader, set, order) = o.get_mut();
                if source_ts < *high_water {
                    false
                } else if source_ts > *high_water {
                    *high_water = source_ts;
                    *leader = publisher;
                    set.clear();
                    order.clear();
                    set.insert(content.clone());
                    order.push_back(content);
                    true
                } else if publisher != *leader {
                    false
                } else if set.insert(content.clone()) {
                    order.push_back(content);
                    if order.len() > self.tick_cap {
                        if let Some(old) = order.pop_front() {
                            set.remove(&old);
                        }
                    }
                    true
                } else {
                    false
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
pub struct WindowedDedup<K, V> {
    capacity: usize,
    seen: HashMap<K, (HashSet<V>, VecDeque<V>)>,
}

impl<K: Eq + Hash + Clone, V: Eq + Hash + Copy> WindowedDedup<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            seen: HashMap::new(),
        }
    }

    /// True if `value` has not been seen recently for `key` (recording it); false if it is a
    /// duplicate still inside the window.
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the
    /// broadcast channel, the only delivery step. A no-subscriber send desyncs no one, and a unique
    /// update dropped by a slow per-client channel is unrecoverable regardless — so there is no
    /// failed-send path on which the cache must avoid advancing.
    pub fn is_new(&mut self, key: K, value: V) -> bool {
        let (set, order) = self.seen.entry(key).or_default();
        if !set.insert(value) {
            return false;
        }
        order.push_back(value);
        if order.len() > self.capacity {
            if let Some(old) = order.pop_front() {
                set.remove(&old);
            }
        }
        true
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
    tx: broadcast::Sender<FeedMessage>,
    quotes: StalenessFloor<(String, String), QuoteId, Publisher>,
    trades: WindowedDedup<(String, String), u64>,
}

impl Arbiter {
    pub fn new(tx: broadcast::Sender<FeedMessage>, trade_window: usize) -> Self {
        Self {
            tx,
            quotes: StalenessFloor::new(QUOTE_TICK_CAP),
            trades: WindowedDedup::new(trade_window),
        }
    }

    /// The broadcast sender, so output sinks can `subscribe()` and `Status` can be sent directly
    /// (it carries no business identity to dedup).
    pub fn sender(&self) -> &broadcast::Sender<FeedMessage> {
        &self.tx
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
                    m.emit.with_label_values(&[&q.venue, "quote"]).inc();
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
                if self
                    .quotes
                    .admit(key, q.source_ts_ns, QuoteId::of(q), publisher)
                {
                    m.emit.with_label_values(&[&q.venue, "quote"]).inc();
                    // Attribute the admitted quote to its winning publisher. A rise in
                    // `publisher="public"` is the direct signal of the public backstop filling an
                    // edge gap (in steady state the edge publisher leads every tick).
                    m.quotes_admitted
                        .with_label_values(&[&q.venue, publisher.label()])
                        .inc();
                    let _ = self.tx.send(msg);
                } else {
                    m.quotes_dropped.with_label_values(&[&q.venue]).inc();
                }
            }
            FeedMessage::Trade(t) => {
                let key = (t.venue.clone(), t.symbol.clone());
                if self.trades.is_new(key, t.trade_id) {
                    m.emit.with_label_values(&[&t.venue, "trade"]).inc();
                    let _ = self.tx.send(msg);
                } else {
                    m.trades_dropped.with_label_values(&[&t.venue]).inc();
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
                m.emit.with_label_values(&[d.venue.as_str(), "depth"]).inc();
                let _ = self.tx.send(msg);
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
        assert!(f.admit("BTC", 1000, 0, 1)); // first for this key always emits (and latches leader)
    }

    #[test]
    fn quote_new_tick_admits_and_relatches_leader() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 1000, 0, 1)); // pub 1 opens tick -> leader
        assert!(f.admit("BTC", 1001, 0, 2)); // newer tick re-latches to pub 2 (even identical content)
        assert!(f.admit("BTC", 2000, 0, 1)); // newer tick again, leader back to pub 1
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
        assert!(f.admit("BTC", 1000, 5, a)); // opens tick -> A is leader, emit 5
        assert!(!f.admit("BTC", 1000, 5, a)); // A's exact repeat dropped
        assert!(f.admit("BTC", 1000, 4, a)); // A's next distinct content kept
        assert!(f.admit("BTC", 1000, 3, a)); // A's next distinct content kept
                                             // B (higher delay) samples 39 at the same tick, arriving last. DROPPED even though A never
                                             // sent 39: its order relative to A is delay-corrupted, so emitting it risks a phantom tick.
        assert!(!f.admit("BTC", 1000, 39, b));
        // A new tick re-opens the latch; whichever publisher gets there first leads it.
        assert!(f.admit("BTC", 1001, 39, b)); // B opens the next tick -> B leads, emit
        assert!(!f.admit("BTC", 1001, 2, a)); // A is now the non-leader at this tick -> dropped
    }

    #[test]
    fn quote_stale_tick_dropped_for_any_publisher() {
        let (a, b) = (1u8, 2u8);
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 2000, 1, a));
        assert!(!f.admit("BTC", 1999, 9, a)); // strictly older tick -> stale, dropped
        assert!(!f.admit("BTC", 1999, 9, b)); // and from the other publisher too
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(64);
        assert!(f.admit("BTC", 2000, 0, 1));
        assert!(f.admit("ETH", 1000, 0, 1)); // separate floor + leader per key
        assert!(!f.admit("BTC", 1500, 0, 1)); // BTC's floor unaffected by ETH
    }

    /// The per-tick content set is FIFO-bounded to `tick_cap` so a stalled `source_ts` can't grow it
    /// without limit. With cap 2, the oldest content is evicted and a recurrence re-admits.
    #[test]
    fn quote_tick_set_is_capacity_bounded() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new(2);
        assert!(f.admit("BTC", 1000, 1, 9)); // tick window {1}
        assert!(f.admit("BTC", 1000, 2, 9)); // {1,2}
        assert!(f.admit("BTC", 1000, 3, 9)); // {2,3} — content 1 evicted (cap 2)
        assert!(!f.admit("BTC", 1000, 3, 9)); // 3 still in the set -> dup dropped
        assert!(f.admit("BTC", 1000, 1, 9)); // 1 fell out of the cap window -> re-admitted
    }

    #[test]
    fn trade_new_admitted_repeat_dropped() {
        let mut d: WindowedDedup<&str, u64> = WindowedDedup::new(8);
        assert!(d.is_new("BTC", 1));
        assert!(!d.is_new("BTC", 1)); // competing publisher's copy
        assert!(d.is_new("BTC", 2));
    }

    #[test]
    fn trade_keys_independent_and_window_evicts() {
        let mut d: WindowedDedup<&str, u64> = WindowedDedup::new(2);
        assert!(d.is_new("BTC", 1));
        assert!(d.is_new("ETH", 1)); // same id, different key
        assert!(d.is_new("BTC", 2));
        assert!(d.is_new("BTC", 3)); // window {2,3}; id 1 evicted
        assert!(d.is_new("BTC", 1)); // 1 fell out of the window -> treated as new
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
}
