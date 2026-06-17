//! Bounded, prefer-valid shred deduplication for the single forwarder task.
//!
//! Window of recently-seen slots keyed by `(slot, shred_index, shred_type)`. A key is recorded only
//! once a **verified** copy has been forwarded (its "winner"); an unverified/invalid copy leaves the
//! key open so a later valid contender can still win. This is the coupling the issue calls for: the
//! dedup decision must never let a forged first-arriving copy lock out the real one.
//!
//! Eviction is a cheap range drop: the window keeps only slots within `window_slots` of the highest
//! slot seen, so memory is bounded by (window_slots × shreds-per-slot) regardless of uptime.

use std::collections::{BTreeMap, BTreeSet};

use super::parse::ShredType;

/// What the forwarder should do with the current datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Forward,
    Drop,
}

pub struct DedupWindow {
    window_slots: u64,
    /// slot -> set of `(index, type)` keys that already have a forwarded winner.
    slots: BTreeMap<u64, BTreeSet<(u32, ShredType)>>,
    /// Highest slot observed; the eviction horizon trails it by `window_slots`.
    tip: u64,
}

impl DedupWindow {
    pub fn new(window_slots: u64) -> Self {
        Self {
            window_slots,
            slots: BTreeMap::new(),
            tip: 0,
        }
    }

    /// Number of slots currently tracked (for tests / observability).
    pub fn tracked_slots(&self) -> usize {
        self.slots.len()
    }

    /// The prefer-valid decision for one shred. `verify` is only called when the key has no winner
    /// yet *and* the leader is known — so signature work stays proportional to unique shreds, and a
    /// duplicate of an already-forwarded shred is dropped without any ed25519 cost.
    ///
    /// `leader_known == false` (schedule not yet loaded, or slot outside the cached epoch) fails
    /// **open**: we forward the datagram but record no winner, so we never silently drop traffic we
    /// simply cannot judge yet.
    pub fn decide<F: FnMut() -> bool>(
        &mut self,
        slot: u64,
        index: u32,
        ty: ShredType,
        leader_known: bool,
        verify: &mut F,
    ) -> Action {
        self.advance(slot);
        let key = (index, ty);
        if self.slots.get(&slot).is_some_and(|s| s.contains(&key)) {
            return Action::Drop; // duplicate of an already-forwarded winner: no sig check
        }
        if !leader_known {
            return Action::Forward; // can't judge -> fail open, don't record a winner
        }
        if verify() {
            // Record the winner unless the slot is already past the eviction horizon (a very late
            // shred for an evicted slot is forwarded but not tracked).
            if slot >= self.horizon() {
                self.slots.entry(slot).or_default().insert(key);
            }
            Action::Forward
        } else {
            Action::Drop // invalid copy: leave the key open for a later valid contender
        }
    }

    /// Advance the tip to the newest slot seen and evict everything below the horizon.
    fn advance(&mut self, slot: u64) {
        if slot > self.tip {
            self.tip = slot;
            let horizon = self.horizon();
            // Drop every tracked slot strictly below the horizon (cheap range split off the front).
            self.slots = self.slots.split_off(&horizon);
        }
    }

    /// Lowest slot still inside the window.
    fn horizon(&self) -> u64 {
        self.tip.saturating_sub(self.window_slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: ShredType = ShredType::Data;
    const CODE: ShredType = ShredType::Code;

    // A verify closure that counts calls and returns a fixed verdict, so tests can assert *whether*
    // the signature was even checked (the "no sigverify on a loser" requirement).
    struct Verifier {
        verdict: bool,
        calls: usize,
    }
    impl Verifier {
        fn new(verdict: bool) -> Self {
            Self { verdict, calls: 0 }
        }
        fn closure(&mut self) -> impl FnMut() -> bool + '_ {
            move || {
                self.calls += 1;
                self.verdict
            }
        }
    }

    #[test]
    fn same_shred_twice_forwards_once_and_skips_second_verify() {
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(true);
        assert_eq!(
            w.decide(10, 0, DATA, true, &mut v.closure()),
            Action::Forward
        );
        // Second copy (e.g. from the retransmit group) is a duplicate: dropped, no verify call.
        assert_eq!(w.decide(10, 0, DATA, true, &mut v.closure()), Action::Drop);
        assert_eq!(v.calls, 1, "the duplicate must not be signature-checked");
    }

    #[test]
    fn bad_signature_dropped_good_forwarded() {
        let mut w = DedupWindow::new(100);
        let mut bad = Verifier::new(false);
        assert_eq!(w.decide(5, 1, DATA, true, &mut bad.closure()), Action::Drop);

        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(5, 2, DATA, true, &mut good.closure()),
            Action::Forward
        );
    }

    #[test]
    fn prefer_valid_bad_copy_first_then_good_copy_wins() {
        let mut w = DedupWindow::new(100);
        // Forged/corrupt copy arrives first: dropped, key left open.
        let mut bad = Verifier::new(false);
        assert_eq!(w.decide(7, 3, DATA, true, &mut bad.closure()), Action::Drop);
        // The real copy arrives later: it is still allowed to win and forward.
        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(7, 3, DATA, true, &mut good.closure()),
            Action::Forward
        );
        assert_eq!(good.calls, 1);
        // A third copy is now a duplicate of the winner: dropped without a check.
        let mut third = Verifier::new(true);
        assert_eq!(
            w.decide(7, 3, DATA, true, &mut third.closure()),
            Action::Drop
        );
        assert_eq!(third.calls, 0);
    }

    #[test]
    fn unknown_leader_fails_open_without_verifying() {
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(false);
        assert_eq!(
            w.decide(1, 0, DATA, false, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(v.calls, 0, "no leader -> no sig check, just forward");
        // No winner recorded, so a later copy is still judged on its own merits.
        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(1, 0, DATA, true, &mut good.closure()),
            Action::Forward
        );
    }

    #[test]
    fn index_and_type_are_part_of_the_key() {
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(true);
        // Same slot+index but different type, and same slot+type but different index, are distinct.
        assert_eq!(
            w.decide(2, 0, DATA, true, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(
            w.decide(2, 0, CODE, true, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(
            w.decide(2, 1, DATA, true, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(v.calls, 3);
    }

    #[test]
    fn old_slots_are_evicted_and_memory_stays_bounded() {
        let mut w = DedupWindow::new(10);
        let mut v = Verifier::new(true);
        for slot in 0..1000u64 {
            w.decide(slot, 0, DATA, true, &mut v.closure());
        }
        // Only slots within `window_slots` of the tip survive.
        assert!(
            w.tracked_slots() <= 11,
            "window must stay bounded, got {}",
            w.tracked_slots()
        );
        // An ancient slot is gone: it no longer counts as a winner, so a fresh copy re-verifies.
        let mut again = Verifier::new(true);
        assert_eq!(
            w.decide(0, 0, DATA, true, &mut again.closure()),
            Action::Forward
        );
    }
}
