//! Multi-publisher output dedup primitives.
//!
//! When several independent publishers mirror the same feed, the bridge demultiplexes them by
//! source IP (so each publisher's frame-sequence state stays separate — see `receiver`/`processor`)
//! but deduplicates the *output*, so a subscriber sees a clean stream regardless of which publisher
//! delivered a given update first. These primitives are keyed on business identity and therefore
//! operate on the merged stream — they need no per-publisher demux themselves.
//!
//! Two primitives, by message semantics:
//! - quotes ([`Watermark`]): a full-state BBO is a *snapshot*. A per-`(venue, symbol)` `source_ts`
//!   high-watermark admits only strictly-newer samples, so an older BBO from a lagging publisher
//!   (the market has moved on) is dropped and the output is freshest-only and monotonic. This also
//!   drops exact duplicates and any same-`source_ts` distinct copy — accepted for a full-state feed.
//! - trades ([`WindowedDedup`]): a trade is a *point-in-time event*, not state, so freshest-wins
//!   would lose prints. It keeps the windowed `trade_id` identity instead: a competing publisher's
//!   copy or an in-window reorder is dropped, but every distinct print is kept.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

/// Per-key freshest-wins high-watermark on a monotonic `source_ts`. For a full-state feed (quotes),
/// only a strictly-newer sample carries information: an older one is stale (the market moved on) and
/// an equal-`source_ts` one adds nothing, so both are dropped. Output is monotonic per key.
pub struct Watermark<K> {
    last: HashMap<K, u64>,
}

impl<K: Eq + Hash> Default for Watermark<K> {
    fn default() -> Self {
        Self {
            last: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash> Watermark<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// True (recording it) if `source_ts` is the newest yet seen for `key`; false if it is equal to
    /// or older than the high-watermark. The first sample for a key always admits. Dropping an equal
    /// `source_ts` means a same-`source_ts` distinct quote is intentionally suppressed — acceptable
    /// for a full-state feed where the goal is freshest-monotonic output.
    pub fn admit(&mut self, key: K, source_ts: u64) -> bool {
        use std::collections::hash_map::Entry;
        match self.last.entry(key) {
            Entry::Vacant(v) => {
                v.insert(source_ts);
                true
            }
            Entry::Occupied(mut o) => {
                if source_ts > *o.get() {
                    o.insert(source_ts);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_first_sample_admits() {
        let mut w: Watermark<&str> = Watermark::new();
        assert!(w.admit("BTC", 1000)); // first for this key always emits
    }

    #[test]
    fn quote_strictly_increasing_admits() {
        let mut w: Watermark<&str> = Watermark::new();
        assert!(w.admit("BTC", 1000));
        assert!(w.admit("BTC", 1001));
        assert!(w.admit("BTC", 2000));
    }

    /// An equal or older `source_ts` is dropped. Note that an equal `source_ts` with *distinct*
    /// content is intentionally dropped too: this is a full-state feed, so we want freshest-monotonic
    /// output and accept losing a same-instant distinct sample.
    #[test]
    fn quote_equal_or_older_source_ts_dropped() {
        let mut w: Watermark<&str> = Watermark::new();
        assert!(w.admit("BTC", 2000));
        assert!(!w.admit("BTC", 2000)); // equal: same-source_ts distinct content also dropped
        assert!(!w.admit("BTC", 1999)); // older: stale, market moved on
        assert!(w.admit("BTC", 2001)); // newer again: admits
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut w: Watermark<&str> = Watermark::new();
        assert!(w.admit("BTC", 2000));
        assert!(w.admit("ETH", 1000)); // separate high-watermark per key
        assert!(!w.admit("BTC", 1500)); // BTC's watermark unaffected by ETH
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
