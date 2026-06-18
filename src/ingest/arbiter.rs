//! Multi-publisher output dedup primitives.
//!
//! When several independent publishers mirror the same feed, the bridge demultiplexes them by
//! source IP (so each publisher's frame-sequence state stays separate — see `receiver`/`processor`)
//! but deduplicates the *output*, so a subscriber sees a clean stream regardless of which publisher
//! delivered a given update first. These primitives are keyed on business identity and therefore
//! operate on the merged stream — they need no per-publisher demux themselves.
//!
//! Two primitives, by message semantics:
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
};

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
/// Memory is O(distinct leader content at the current tick) per key — bounded by how fast the book
/// moves within one `source_ts`; no fixed window needed.
pub struct StalenessFloor<K, V, P> {
    /// Per key: (high_water source_ts, leader publisher at that tick, leader's content set at it).
    state: HashMap<K, (u64, P, HashSet<V>)>,
}

impl<K: Eq + Hash, V: Eq + Hash, P> Default for StalenessFloor<K, V, P> {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash, V: Eq + Hash, P: Eq> StalenessFloor<K, V, P> {
    pub fn new() -> Self {
        Self::default()
    }

    /// True (recording it) iff this `(source_ts, content)` from `publisher` should be emitted under
    /// the latch-to-leader floor (see the type docs for the three cases).
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
                set.insert(content);
                v.insert((source_ts, publisher, set));
                true
            }
            Entry::Occupied(mut o) => {
                let (high_water, leader, set) = o.get_mut();
                if source_ts < *high_water {
                    false
                } else if source_ts > *high_water {
                    *high_water = source_ts;
                    *leader = publisher;
                    set.clear();
                    set.insert(content);
                    true
                } else if publisher != *leader {
                    false
                } else {
                    set.insert(content)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_first_sample_admits() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 1000, 0, 1)); // first for this key always emits (and latches leader)
    }

    #[test]
    fn quote_new_tick_admits_and_relatches_leader() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new();
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
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new();
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
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 2000, 1, a));
        assert!(!f.admit("BTC", 1999, 9, a)); // strictly older tick -> stale, dropped
        assert!(!f.admit("BTC", 1999, 9, b)); // and from the other publisher too
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut f: StalenessFloor<&str, u8, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 2000, 0, 1));
        assert!(f.admit("ETH", 1000, 0, 1)); // separate floor + leader per key
        assert!(!f.admit("BTC", 1500, 0, 1)); // BTC's floor unaffected by ETH
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
}
