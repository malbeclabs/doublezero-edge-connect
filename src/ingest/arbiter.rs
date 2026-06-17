//! Multi-publisher output dedup primitives.
//!
//! When several independent publishers mirror the same feed, the bridge demultiplexes them by
//! source IP (so each publisher's frame-sequence state stays separate — see `receiver`/`processor`)
//! but deduplicates the *output* on business identity, so a subscriber sees each logical update
//! once regardless of which publisher delivered it first. These primitives are identity-keyed and
//! therefore operate on the merged stream — they need no per-publisher demux themselves.
//!
//! [`WindowedDedup`] suppresses a value whose identity `V` was seen recently for a key `K`, keeping
//! the most recent `capacity` identities per key (bounded memory). Two specializations:
//! - trades: `V = u64` (venue `trade_id`) — a competing publisher's copy or an in-window reorder is
//!   dropped.
//! - quotes: `V = `[`QuoteId`] — the windowed BBO identity `(source_ts, bid/ask prices+sizes,
//!   exponents)`. This matches the `no_business_duplicates` oracle's quote key and the Grafana
//!   board's identity join, so it collapses a duplicate `(content, source_ts)` even at real
//!   inter-publisher skew (up to the window), unlike an adjacent-only content fingerprint.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

/// Windowed BBO identity for cross-publisher quote dedup. Carries the **venue source timestamp**
/// plus the raw fixed-point fields **and the instrument exponents**: this is exactly the oracle's
/// quote key, so two publishers' copies of the same update collapse, while a fresh `source_ts` (a
/// genuinely new top-of-book sample) is kept. The exponents guard against a registry change that
/// would otherwise false-dedup two distinct prices sharing a raw int.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QuoteId {
    pub source_ts: u64,
    pub bid_price_raw: i64,
    pub bid_qty_raw: u64,
    pub ask_price_raw: i64,
    pub ask_qty_raw: u64,
    pub price_exponent: i8,
    pub qty_exponent: i8,
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

    fn qid(source_ts: u64, bid_price: i64, bid_qty: u64, ask_price: i64, ask_qty: u64) -> QuoteId {
        QuoteId {
            source_ts,
            bid_price_raw: bid_price,
            bid_qty_raw: bid_qty,
            ask_price_raw: ask_price,
            ask_qty_raw: ask_qty,
            price_exponent: -2,
            qty_exponent: 0,
        }
    }

    #[test]
    fn quote_identical_identity_collapses() {
        let mut d: WindowedDedup<&str, QuoteId> = WindowedDedup::new(8);
        assert!(d.is_new("BTC", qid(1000, 100, 5, 101, 8)));
        assert!(!d.is_new("BTC", qid(1000, 100, 5, 101, 8))); // competing publisher's identical copy
        assert!(d.is_new("BTC", qid(1000, 100, 6, 101, 8))); // bid size changed -> new identity
    }

    /// THE behavior change vs. the old content fingerprint: same BBO content but a fresh
    /// `source_ts` is a genuinely new sample and must be kept, not collapsed.
    #[test]
    fn quote_same_content_different_source_ts_both_kept() {
        let mut d: WindowedDedup<&str, QuoteId> = WindowedDedup::new(8);
        assert!(d.is_new("BTC", qid(1000, 100, 5, 101, 8)));
        assert!(d.is_new("BTC", qid(2000, 100, 5, 101, 8))); // identical content, later source_ts
    }

    #[test]
    fn quote_exponent_change_is_not_a_duplicate() {
        let mut d: WindowedDedup<&str, QuoteId> = WindowedDedup::new(8);
        let a = qid(1000, 100, 5, 101, 8);
        let mut shifted = a;
        shifted.price_exponent = -3; // same raw ints + source_ts, different scale -> distinct price
        assert!(d.is_new("BTC", a));
        assert!(d.is_new("BTC", shifted)); // only catchable because we carry the exponent
    }

    #[test]
    fn quote_keys_are_independent() {
        let mut d: WindowedDedup<&str, QuoteId> = WindowedDedup::new(8);
        assert!(d.is_new("BTC", qid(1000, 100, 5, 101, 8)));
        assert!(d.is_new("ETH", qid(1000, 100, 5, 101, 8)));
    }

    #[test]
    fn quote_window_evicts_oldest() {
        let mut d: WindowedDedup<&str, QuoteId> = WindowedDedup::new(2);
        let a = qid(1, 1, 1, 1, 1);
        assert!(d.is_new("BTC", a));
        assert!(d.is_new("BTC", qid(2, 1, 1, 1, 1)));
        assert!(d.is_new("BTC", qid(3, 1, 1, 1, 1))); // window now {2,3}; `a` evicted
        assert!(d.is_new("BTC", a)); // `a` fell out of the window -> treated as new
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
