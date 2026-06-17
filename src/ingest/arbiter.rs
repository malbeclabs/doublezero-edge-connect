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
//!   a `source_ts` (the venue stamps at millisecond granularity while the book changes faster), and
//!   each is a real top-of-book observation, not noise. A per-`(venue, symbol)` `source_ts` *floor*
//!   keeps every distinct `(source_ts, content)` observation but never goes back to a `source_ts` it
//!   has passed: a strictly-older BBO from a lagging publisher is stale (the market moved on) and is
//!   dropped, as is an exact `(source_ts, content)` duplicate (incl. the other publisher's copy), but
//!   a *new* BBO at the current `source_ts` is kept. Output `source_ts` is non-decreasing per key
//!   (not strictly increasing — content can change within a tick). This matches the
//!   `hl-bbo-feed-race` board's `(symbol, source_ts, bbo_hash)` identity. A strict high-watermark
//!   instead over-drops: it discards real intra-tick BBO changes, not just laggard replays.
//! - trades ([`WindowedDedup`]): a trade is a *point-in-time event*, not state, so a floor would lose
//!   prints. It keeps the windowed `trade_id` identity instead: a competing publisher's copy or an
//!   in-window reorder is dropped, but every distinct print is kept.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

/// Per-key staleness floor on `source_ts`. Tracks the highest `source_ts` emitted per key plus the
/// set of distinct content seen *at that tick*. Admits every distinct `(source_ts, content)` while
/// the `source_ts` never goes backwards: a strictly-older sample is stale (dropped); a new tick
/// resets the per-tick content set and admits; an equal-tick sample admits only if its content is
/// new (an exact `(source_ts, content)` duplicate is dropped). Output is `source_ts`-non-decreasing.
///
/// Memory is O(distinct content at the current tick) per key — bounded by how fast the book moves
/// within one `source_ts`; no fixed window needed.
pub struct StalenessFloor<K, V> {
    /// Per key: (high_water source_ts, set of content seen at exactly high_water).
    state: HashMap<K, (u64, HashSet<V>)>,
}

impl<K: Eq + Hash, V: Eq + Hash> Default for StalenessFloor<K, V> {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash, V: Eq + Hash> StalenessFloor<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// True (recording it) if this `(source_ts, content)` is a fresh observation for `key`:
    /// - `source_ts < high_water` → false (stale: a tick already passed — the laggard).
    /// - `source_ts > high_water` → advance the floor, reset the tick's content set to this one, true.
    /// - `source_ts == high_water` → true iff this content is new at the tick (else exact duplicate).
    ///
    /// The first sample for a key behaves as the `>` case.
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the broadcast
    /// channel, the only delivery step. A no-subscriber send desyncs no one, and a unique quote
    /// dropped by a slow per-client channel is unrecoverable regardless — so there is no failed-send
    /// path on which the floor must avoid advancing.
    pub fn admit(&mut self, key: K, source_ts: u64, content: V) -> bool {
        use std::collections::hash_map::Entry;
        match self.state.entry(key) {
            Entry::Vacant(v) => {
                let mut set = HashSet::new();
                set.insert(content);
                v.insert((source_ts, set));
                true
            }
            Entry::Occupied(mut o) => {
                let (high_water, set) = o.get_mut();
                if source_ts < *high_water {
                    false
                } else if source_ts > *high_water {
                    *high_water = source_ts;
                    set.clear();
                    set.insert(content);
                    true
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
        let mut f: StalenessFloor<&str, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 1000, 0)); // first for this key always emits
    }

    #[test]
    fn quote_new_tick_admits() {
        let mut f: StalenessFloor<&str, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 1000, 0));
        assert!(f.admit("BTC", 1001, 0)); // newer tick, even with identical content
        assert!(f.admit("BTC", 2000, 0));
    }

    /// A strictly-older `source_ts` is dropped (stale laggard). An exact `(source_ts, content)`
    /// duplicate is dropped. But a NEW content at the *current* `source_ts` is KEPT — this is the
    /// real intra-tick BBO change the old strict watermark wrongly discarded.
    #[test]
    fn quote_stale_and_exact_dup_dropped_but_intra_tick_change_kept() {
        let mut f: StalenessFloor<&str, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 2000, 1)); // first
        assert!(!f.admit("BTC", 2000, 1)); // exact (ts, content) dup dropped
        assert!(f.admit("BTC", 2000, 2)); // NEW content at the SAME tick -> KEPT (watermark dropped this)
        assert!(!f.admit("BTC", 2000, 1)); // content 1 still in the current tick's set -> dup dropped
        assert!(!f.admit("BTC", 1999, 9)); // strictly older -> stale, dropped (even with new content)
        assert!(f.admit("BTC", 2001, 1)); // new tick: content set resets, so content 1 admits again
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut f: StalenessFloor<&str, u8> = StalenessFloor::new();
        assert!(f.admit("BTC", 2000, 0));
        assert!(f.admit("ETH", 1000, 0)); // separate floor per key
        assert!(!f.admit("BTC", 1500, 0)); // BTC's floor unaffected by ETH
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
