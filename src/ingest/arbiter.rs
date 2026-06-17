//! Multi-publisher output dedup primitives.
//!
//! When several independent publishers mirror the same feed, the bridge demultiplexes them by
//! source IP (so each publisher's frame-sequence state stays separate — see `receiver`/`processor`)
//! but deduplicates the *output* on business content, so a subscriber sees each logical update
//! once regardless of which publisher delivered it first. These primitives are content-/id-keyed
//! and therefore operate on the merged stream — they need no per-publisher demux themselves.
//!
//! - [`BboDedup`] suppresses a quote whose top-of-book content matches the last one emitted for a
//!   key. Adapted from the Hyperliquid publisher's per-coin `WireBboQuote` fingerprint — NOT a 1:1
//!   port: we carry the instrument exponents and fingerprint *pre*-conversion (the bridge applies
//!   the exponent later, at JSON emit), where the publisher bakes the exponent into the raw int at
//!   fingerprint time and so needs no exponent field.
//! - [`TradeDeduper`] drops a trade whose venue `trade_id` was seen recently for a key. Trades are
//!   point-in-time, so they dedup by identity, not by content fingerprint.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
};

/// Per-coin wire-level BBO fingerprint for dedup. Compares the raw fixed-point fields **plus the
/// instrument exponents** and EXCLUDES every timestamp: the same BBO with a fresh `source_ts` is a
/// duplicate, and a registry/exponent change must not false-dedup two distinct prices that share a
/// raw int.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireBbo {
    pub update_flags: u8,
    pub bid_price_raw: i64,
    pub bid_qty_raw: u64,
    pub ask_price_raw: i64,
    pub ask_qty_raw: u64,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// Last-emitted BBO fingerprint per key. [`Self::should_emit`] reports whether an incoming BBO
/// differs from the last emitted for `key` (recording it when so); identical content — a redundant
/// republish or a competing publisher's duplicate — is suppressed.
pub struct BboDedup<K> {
    last: HashMap<K, WireBbo>,
}

impl<K: Eq + Hash> Default for BboDedup<K> {
    fn default() -> Self {
        Self {
            last: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash> BboDedup<K> {
    /// True (recording it) if `bbo` differs from the last emitted for `key`; false if identical.
    ///
    /// Records on the emit *decision*: in this gateway "emit" == handing the message to the
    /// broadcast channel, the only delivery step. A no-subscriber send desyncs no one, and a unique
    /// quote dropped by a slow per-client channel is unrecoverable regardless (quotes are not in a
    /// replay map) — so there is no failed-send path on which the cache must avoid advancing.
    pub fn should_emit(&mut self, key: K, bbo: WireBbo) -> bool {
        if self.last.get(&key) == Some(&bbo) {
            return false;
        }
        self.last.insert(key, bbo);
        true
    }
}

/// Per-key bounded dedup of venue trade IDs. Keeps the most recent `capacity` IDs per key, so a
/// duplicate trade from a second publisher (or a reorder within the window) is dropped while memory
/// stays bounded.
///
/// Window correctness depends on the `no_business_duplicates` oracle's assumption that `trade_id`
/// is globally unique per `(venue, symbol)`: the window must exceed the worst-case number of trades
/// between competing publishers' copies of the same trade, or a late duplicate re-emits.
pub struct TradeDeduper<K> {
    capacity: usize,
    seen: HashMap<K, (HashSet<u64>, VecDeque<u64>)>,
}

impl<K: Eq + Hash + Clone> TradeDeduper<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            seen: HashMap::new(),
        }
    }

    /// True if `trade_id` has not been seen recently for `key` (recording it); false if it is a
    /// duplicate still inside the window.
    pub fn is_new(&mut self, key: K, trade_id: u64) -> bool {
        let (set, order) = self.seen.entry(key).or_default();
        if !set.insert(trade_id) {
            return false;
        }
        order.push_back(trade_id);
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

    fn bbo(bid_price: i64, bid_qty: u64, ask_price: i64, ask_qty: u64) -> WireBbo {
        WireBbo {
            update_flags: 0,
            bid_price_raw: bid_price,
            bid_qty_raw: bid_qty,
            ask_price_raw: ask_price,
            ask_qty_raw: ask_qty,
            price_exponent: -2,
            qty_exponent: 0,
        }
    }

    #[test]
    fn bbo_first_emits_identical_suppressed_changed_emits() {
        let mut d: BboDedup<&str> = BboDedup::default();
        assert!(d.should_emit("BTC", bbo(100, 5, 101, 8)));
        assert!(!d.should_emit("BTC", bbo(100, 5, 101, 8))); // identical republish/duplicate
        assert!(d.should_emit("BTC", bbo(100, 6, 101, 8))); // bid size changed
    }

    #[test]
    fn bbo_raw_price_change_at_fixed_exponent_emits() {
        let mut d: BboDedup<&str> = BboDedup::default();
        assert!(d.should_emit("BTC", bbo(100, 5, 101, 8)));
        assert!(d.should_emit("BTC", bbo(102, 5, 101, 8))); // bid price moved, same exponent
    }

    #[test]
    fn bbo_exponent_change_is_not_a_duplicate() {
        let mut d: BboDedup<&str> = BboDedup::default();
        assert!(d.should_emit("BTC", bbo(100, 5, 101, 8)));
        let mut shifted = bbo(100, 5, 101, 8);
        shifted.price_exponent = -3; // same raw ints, different scale -> distinct price
        assert!(d.should_emit("BTC", shifted)); // only catchable because we carry the exponent
    }

    #[test]
    fn bbo_keys_are_independent() {
        let mut d: BboDedup<&str> = BboDedup::default();
        assert!(d.should_emit("BTC", bbo(100, 5, 101, 8)));
        assert!(d.should_emit("ETH", bbo(100, 5, 101, 8)));
    }

    #[test]
    fn trade_new_admitted_repeat_dropped() {
        let mut d: TradeDeduper<&str> = TradeDeduper::new(8);
        assert!(d.is_new("BTC", 1));
        assert!(!d.is_new("BTC", 1)); // competing publisher's copy
        assert!(d.is_new("BTC", 2));
    }

    #[test]
    fn trade_keys_independent_and_window_evicts() {
        let mut d: TradeDeduper<&str> = TradeDeduper::new(2);
        assert!(d.is_new("BTC", 1));
        assert!(d.is_new("ETH", 1)); // same id, different key
        assert!(d.is_new("BTC", 2));
        assert!(d.is_new("BTC", 3)); // window {2,3}; id 1 evicted
        assert!(d.is_new("BTC", 1)); // 1 fell out of the window -> treated as new
    }
}
