//! Bounded, prefer-valid shred deduplication for the single forwarder task.
//!
//! Window of recently-seen slots keyed by `(slot, shred_index, shred_type, fingerprint)`. A key is
//! recorded only once a **verified** copy has been forwarded (its "winner"); an unverified/invalid
//! copy leaves the key open so a later valid contender can still win. This is the coupling the issue
//! calls for: the dedup decision must never let a forged first-arriving copy lock out the real one.
//!
//! The trailing `fingerprint` discriminates content. In **sigverify** mode it is a fixed `0`
//! (content-agnostic — the signature, not the bytes, picks the winner, so a forged different-byte
//! copy collapses onto the same key and is dropped on verify). In **dedup-only** mode it is a
//! content hash over the datagram minus any trailing retransmitter signature (rewritten per turbine
//! path; the caller strips it before hashing), so copies of the same shred share a key while a
//! same-`(slot, index, type)` shred with different signed content still forwards (loss-averse:
//! without sigverify we can't tell which copy is valid).
//!
//! Eviction is a cheap range drop: the window keeps only slots within `window_slots` of the highest
//! slot seen, so memory is bounded by (window_slots × shreds-per-slot) regardless of uptime.

use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    net::Ipv4Addr,
};

use super::parse::ShredType;

/// Cheap, **deterministic** content fingerprint of a byte region, used as the dedup discriminator in
/// **dedup-only** mode so copies sharing that region collapse. The caller passes the datagram minus
/// any trailing retransmitter signature, so cross-turbine-path copies of the same shred fingerprint
/// the same. `DefaultHasher` has a fixed seed (unlike `RandomState`), so identical bytes always
/// fingerprint the same. This is not a security boundary — forgery protection is the sigverify
/// mode's job — so a fast non-crypto hash is the right tool; collisions inside a bounded slot window
/// are astronomically unlikely and the worst case is merely one extra forwarded copy.
pub fn fingerprint(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

/// What the forwarder should do with the current datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Forward,
    /// Dropped with no cross-group contest to report: an invalid copy, or a duplicate from the same
    /// group that already forwarded this shred (a within-group retransmit).
    Drop,
    /// Dropped as a duplicate from a *different* group than the one that first forwarded this shred:
    /// `winner_group` beat this copy by `lead_ns` nanoseconds. The head-to-head cross-group win.
    DropDuplicate {
        winner_group: Ipv4Addr,
        lead_ns: u64,
    },
}

/// Winners recorded for one slot: each `(index, type, fingerprint)` key mapped to the
/// `(group, arrival_ns)` of the copy that first forwarded it — for cross-group win attribution and
/// the lead-time measure. The `fingerprint` discriminates content: a per-datagram content hash in
/// dedup-only mode (so only byte-identical copies share a key) and a fixed `0` in sigverify mode
/// (content-agnostic, so the signature alone decides the winner).
type SlotWinners = BTreeMap<(u32, ShredType, u64), (Ipv4Addr, u64)>;

pub struct DedupWindow {
    window_slots: u64,
    /// Recently-seen slots, each to the keys already forwarded for it (see [`SlotWinners`]).
    slots: BTreeMap<u64, SlotWinners>,
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
    /// yet — so signature work stays proportional to unique shreds, and a duplicate of an
    /// already-forwarded shred is dropped without any ed25519 cost.
    ///
    /// The caller owns the "can't judge" policy: sigverify mode drops a shred with no known leader
    /// before reaching here (fail closed), and dedup-only mode passes a trivially-true `verify`. So
    /// `decide` only ever sees shreds it should judge on the `verify` verdict.
    #[allow(clippy::too_many_arguments)]
    pub fn decide<F: FnMut() -> bool>(
        &mut self,
        slot: u64,
        index: u32,
        ty: ShredType,
        fingerprint: u64,
        group: Ipv4Addr,
        arrival_ns: u64,
        verify: &mut F,
    ) -> Action {
        let key = (index, ty, fingerprint);
        if let Some(&(winner_group, winner_arrival_ns)) =
            self.slots.get(&slot).and_then(|s| s.get(&key))
        {
            // Duplicate of an already-forwarded winner: dropped without a sig check. A copy from a
            // *different* group is a cross-group contest the winner won — report the lead; a
            // same-group retransmit is just a plain drop.
            return if winner_group == group {
                Action::Drop
            } else {
                Action::DropDuplicate {
                    winner_group,
                    lead_ns: arrival_ns.saturating_sub(winner_arrival_ns),
                }
            };
        }
        if verify() {
            self.record_winner(slot, key, group, arrival_ns);
            Action::Forward
        } else {
            Action::Drop // invalid copy: leave the key open for a later valid contender
        }
    }

    /// Record a verified winner, advancing the tip and evicting old slots. The tip is advanced
    /// **only here**, from verified shreds — never from the raw datagram slot — so a forged shred
    /// carrying a far-future slot can't poison the eviction horizon and silently disable dedup. A
    /// very late shred for an already-evicted slot is forwarded (it won the verify) but not tracked.
    fn record_winner(
        &mut self,
        slot: u64,
        key: (u32, ShredType, u64),
        group: Ipv4Addr,
        arrival_ns: u64,
    ) {
        if slot > self.tip {
            self.tip = slot;
            let horizon = self.horizon();
            // Drop every tracked slot strictly below the horizon (cheap range split off the front).
            self.slots = self.slots.split_off(&horizon);
        }
        if slot >= self.horizon() {
            self.slots
                .entry(slot)
                .or_default()
                .insert(key, (group, arrival_ns));
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
    // A single source group for the legacy tests: with one group, every duplicate is a same-group
    // retransmit (`Action::Drop`), so these assertions are unaffected by cross-group attribution.
    const G: Ipv4Addr = Ipv4Addr::new(239, 0, 0, 1);

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
            w.decide(10, 0, DATA, 0, G, 0, &mut v.closure()),
            Action::Forward
        );
        // Second copy (e.g. from the retransmit group) is a duplicate: dropped, no verify call.
        assert_eq!(
            w.decide(10, 0, DATA, 0, G, 0, &mut v.closure()),
            Action::Drop
        );
        assert_eq!(v.calls, 1, "the duplicate must not be signature-checked");
    }

    #[test]
    fn bad_signature_dropped_good_forwarded() {
        let mut w = DedupWindow::new(100);
        let mut bad = Verifier::new(false);
        assert_eq!(
            w.decide(5, 1, DATA, 0, G, 0, &mut bad.closure()),
            Action::Drop
        );

        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(5, 2, DATA, 0, G, 0, &mut good.closure()),
            Action::Forward
        );
    }

    #[test]
    fn prefer_valid_bad_copy_first_then_good_copy_wins() {
        let mut w = DedupWindow::new(100);
        // Forged/corrupt copy arrives first: dropped, key left open.
        let mut bad = Verifier::new(false);
        assert_eq!(
            w.decide(7, 3, DATA, 0, G, 0, &mut bad.closure()),
            Action::Drop
        );
        // The real copy arrives later: it is still allowed to win and forward.
        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(7, 3, DATA, 0, G, 0, &mut good.closure()),
            Action::Forward
        );
        assert_eq!(good.calls, 1);
        // A third copy is now a duplicate of the winner: dropped without a check.
        let mut third = Verifier::new(true);
        assert_eq!(
            w.decide(7, 3, DATA, 0, G, 0, &mut third.closure()),
            Action::Drop
        );
        assert_eq!(third.calls, 0);
    }

    #[test]
    fn index_and_type_are_part_of_the_key() {
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(true);
        // Same slot+index but different type, and same slot+type but different index, are distinct.
        assert_eq!(
            w.decide(2, 0, DATA, 0, G, 0, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(
            w.decide(2, 0, CODE, 0, G, 0, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(
            w.decide(2, 1, DATA, 0, G, 0, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(v.calls, 3);
    }

    #[test]
    fn distinct_fingerprints_on_the_same_key_all_forward() {
        // Dedup-only's loss-averse rule: two shreds sharing (slot, index, type) but carrying
        // *different* content (different fingerprint) are not the same shred, so both forward.
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(true);
        assert_eq!(
            w.decide(4, 0, DATA, 0xAAAA, G, 0, &mut v.closure()),
            Action::Forward
        );
        assert_eq!(
            w.decide(4, 0, DATA, 0xBBBB, G, 0, &mut v.closure()),
            Action::Forward,
            "a different-content copy of the same (slot, index, type) must still forward"
        );
        // A byte-identical copy (same fingerprint) of the first is the duplicate we suppress.
        assert_eq!(
            w.decide(4, 0, DATA, 0xAAAA, G, 0, &mut v.closure()),
            Action::Drop,
            "an identical-content copy is the duplicate that should be dropped"
        );
    }

    #[test]
    fn old_slots_are_evicted_and_memory_stays_bounded() {
        let mut w = DedupWindow::new(10);
        let mut v = Verifier::new(true);
        for slot in 0..1000u64 {
            w.decide(slot, 0, DATA, 0, G, 0, &mut v.closure());
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
            w.decide(0, 0, DATA, 0, G, 0, &mut again.closure()),
            Action::Forward
        );
    }

    #[test]
    fn forged_far_future_slot_does_not_poison_the_eviction_horizon() {
        // A real shred establishes a winner near slot 10.
        let mut w = DedupWindow::new(100);
        let mut good = Verifier::new(true);
        assert_eq!(
            w.decide(10, 0, DATA, 0, G, 0, &mut good.closure()),
            Action::Forward
        );

        // An attacker injects a parseable shred claiming a far-future slot. Its signature is invalid
        // (verify -> false), so it is dropped — and crucially it must NOT advance the eviction tip,
        // which would otherwise evict slot 10's winner and let its duplicates through undeduped.
        let mut forged = Verifier::new(false);
        assert_eq!(
            w.decide(u64::MAX, 0, DATA, 0, G, 0, &mut forged.closure()),
            Action::Drop
        );

        // Slot 10's winner is still tracked: a duplicate is dropped without a signature check.
        let mut dup = Verifier::new(true);
        assert_eq!(
            w.decide(10, 0, DATA, 0, G, 0, &mut dup.closure()),
            Action::Drop
        );
        assert_eq!(dup.calls, 0, "dedup must still suppress the duplicate");
    }

    /// A duplicate from a *different* group than the winner is a cross-group contest: dropped as
    /// `DropDuplicate` reporting the winner group and how far it led. A same-group retransmit stays a
    /// plain `Drop`, and neither loser is signature-checked.
    #[test]
    fn cross_group_duplicate_reports_winner_and_lead() {
        let group_a = Ipv4Addr::new(239, 0, 0, 1);
        let group_b = Ipv4Addr::new(239, 0, 0, 2);
        let mut w = DedupWindow::new(100);
        let mut v = Verifier::new(true);
        // Group A delivers first at t=1000 and forwards (the only signature check).
        assert_eq!(
            w.decide(10, 0, DATA, 0, group_a, 1000, &mut v.closure()),
            Action::Forward
        );
        // Group B's copy arrives at t=1250: it lost the race by 250ns.
        assert_eq!(
            w.decide(10, 0, DATA, 0, group_b, 1250, &mut v.closure()),
            Action::DropDuplicate {
                winner_group: group_a,
                lead_ns: 250
            }
        );
        // A same-group (A) retransmit is a plain drop, not a cross-group contest.
        assert_eq!(
            w.decide(10, 0, DATA, 0, group_a, 1300, &mut v.closure()),
            Action::Drop
        );
        assert_eq!(v.calls, 1, "only the first copy is signature-checked");
    }
}
