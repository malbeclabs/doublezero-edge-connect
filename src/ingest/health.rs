//! Shared receiver liveness, aggregated to the venue-level feed health `PROTOCOL.md` promises.
//!
//! One venue is served by N receivers - one per publisher per protocol (see `ingest::feeds`). The
//! wire `status` message and `dz_feed_up` are **venue**-level, so neither may flip just because a
//! single publisher wedged: a venue is down only when EVERY registered quote-bearing receiver for
//! it is down. Per-publisher detail lives in `dz_receiver_up{venue,kind,publisher}` instead.
//!
//! Two rules make the aggregate honest:
//!
//! - **Only quote-bearing protocols count** ([`carries_venue_status`]). PROTOCOL.md defines
//!   `status` as the *quote* feed's health, so a depth-only Market-by-Order receiver must neither
//!   declare a venue down on its own nor mask a total quote outage by staying up.
//! - **The edge is computed and published in one critical section.** Every mutator takes an
//!   `on_edge` callback invoked while the lock is still held, so two receivers crossing opposite
//!   edges concurrently publish in aggregate order. Returning the edge and letting the caller
//!   publish afterwards would let the later transition be overwritten by the earlier one, latching
//!   the wire `status` at the negation of the real state.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::ingest::feeds::FeedKind;

/// Identity of one receiver: `(venue, kind, base port)`. Same shape as `reconcile::FeedKey`.
pub type ReceiverKey = (&'static str, FeedKind, u16);

/// Whether a receiver of this protocol counts toward the venue-level `status` / `dz_feed_up`.
///
/// PROTOCOL.md's `status` is the health of the venue's **quote** stream (`stale_ms` is documented as
/// "milliseconds the quote feed had been silent"), which Top-of-Book carries and Market-by-Order
/// does not — MBO is re-served as `depth`. Counting MBO would break the contract in both
/// directions: a wedged depth mirror would report a venue outage while quotes flow, and a live one
/// would mask a total quote outage.
fn carries_venue_status(kind: FeedKind) -> bool {
    match kind {
        FeedKind::TopOfBook | FeedKind::Midpoint => true,
        FeedKind::MarketByOrder => false,
    }
}

/// Shared liveness of every running receiver, aggregated per venue. Cheap to clone via
/// [`SharedFeedHealth`]; every mutation is off the hot path (only watchdog edges touch it).
#[derive(Default)]
pub struct FeedHealth {
    /// Registered receivers -> whether each is currently up. A receiver absent from this map is
    /// not running and does not count toward its venue's aggregate.
    up: Mutex<HashMap<ReceiverKey, bool>>,
}

/// Handle cloned into each receiver task and held by the reconciler.
pub type SharedFeedHealth = Arc<FeedHealth>;

/// Whether any registered **quote-bearing** receiver for `venue` is up, falling back to any
/// registered receiver when the venue has no quote-bearing row at all (a hypothetical depth-only
/// venue would otherwise read permanently down and fire the headline alert forever).
fn venue_up_in(map: &HashMap<ReceiverKey, bool>, venue: &str) -> bool {
    let (mut has_carrier, mut carrier_up, mut any_up) = (false, false, false);
    for ((v, kind, _), up) in map.iter() {
        if *v != venue {
            continue;
        }
        any_up |= *up;
        if carries_venue_status(*kind) {
            has_carrier = true;
            carrier_up |= *up;
        }
    }
    if has_carrier {
        carrier_up
    } else {
        any_up
    }
}

impl FeedHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock, recovering from a poisoned mutex. The critical section is `HashMap` work plus the
    /// caller's `on_edge` (a metric write and a broadcast send — no `.await`, no syscall), so the
    /// map is always left consistent; recovering keeps an unrelated panic in one receiver from
    /// cascading into every other venue's health reporting (the same reasoning as `arbiter::lock`).
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ReceiverKey, bool>> {
        self.up.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Apply `mutate` to the map and invoke `on_edge(venue_up)` — still holding the lock — iff the
    /// venue aggregate flipped. The single place the edge is decided, so publication can never be
    /// reordered against the transition that caused it.
    fn with_edge(
        &self,
        venue: &str,
        mutate: impl FnOnce(&mut HashMap<ReceiverKey, bool>),
        on_edge: impl FnOnce(bool),
    ) {
        let mut map = self.lock();
        let was = venue_up_in(&map, venue);
        mutate(&mut map);
        let now = venue_up_in(&map, venue);
        if was != now {
            on_edge(now);
        }
    }

    /// Mark a starting receiver as up, publishing the venue edge if this raised the aggregate (a
    /// receiver respawned into a down venue). Called once at receiver setup. `true` rather than
    /// "unknown" keeps the pre-existing healthy-until-proven-silent semantics: the idle watchdog
    /// takes the venue down within `IDLE_REJOIN` if no data actually arrives.
    pub fn register(&self, key: ReceiverKey, on_edge: impl FnOnce(bool)) {
        self.with_edge(
            key.0,
            |m| {
                m.insert(key, true);
            },
            on_edge,
        );
    }

    /// Forget a stopped receiver (aborted by the reconciler, or exited on its own), publishing the
    /// venue edge if it was the last one up. Without this a receiver that was down when it stopped
    /// would pin its venue down forever.
    pub fn deregister(&self, key: ReceiverKey, on_edge: impl FnOnce(bool)) {
        self.with_edge(
            key.0,
            |m| {
                m.remove(&key);
            },
            on_edge,
        );
    }

    /// Whether any registered quote-bearing receiver for `venue` is up. A venue with no registered
    /// receivers is not up (nothing is serving it).
    pub fn venue_up(&self, venue: &str) -> bool {
        venue_up_in(&self.lock(), venue)
    }

    /// Record `key`'s liveness, publishing the venue edge if the aggregate flipped — so one
    /// `status` transition fires per venue change rather than one per receiver.
    pub fn set(&self, key: ReceiverKey, up: bool, on_edge: impl FnOnce(bool)) {
        self.with_edge(
            key.0,
            |m| {
                m.insert(key, up);
            },
            on_edge,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const V: &str = "TestVenue";
    fn key(base_port: u16) -> ReceiverKey {
        (V, FeedKind::TopOfBook, base_port)
    }

    /// `Some(venue_up)` if the mutation flipped the venue aggregate, else `None` — the shape the
    /// production callers see through their `on_edge` closure.
    fn set(h: &FeedHealth, k: ReceiverKey, up: bool) -> Option<bool> {
        let edge = Cell::new(None);
        h.set(k, up, |v| edge.set(Some(v)));
        edge.get()
    }
    fn register(h: &FeedHealth, k: ReceiverKey) -> Option<bool> {
        let edge = Cell::new(None);
        h.register(k, |v| edge.set(Some(v)));
        edge.get()
    }
    fn deregister(h: &FeedHealth, k: ReceiverKey) -> Option<bool> {
        let edge = Cell::new(None);
        h.deregister(k, |v| edge.set(Some(v)));
        edge.get()
    }

    #[test]
    fn first_registration_makes_the_venue_up() {
        let h = FeedHealth::new();
        assert!(!h.venue_up(V), "unknown venue is not up");
        assert_eq!(register(&h, key(9101)), Some(true), "venue edge to up");
        assert!(h.venue_up(V));
        assert_eq!(register(&h, key(9201)), None, "already up: no second edge");
    }

    /// The whole point: one wedged publisher must NOT take the venue down while a peer streams.
    #[test]
    fn venue_stays_up_while_any_publisher_is_up() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, key(9201));
        assert_eq!(
            set(&h, key(9101), false),
            None,
            "no venue edge: 9201 still up"
        );
        assert!(h.venue_up(V));
        assert_eq!(
            set(&h, key(9201), false),
            Some(false),
            "last publisher down -> venue edge to down"
        );
        assert!(!h.venue_up(V));
    }

    #[test]
    fn recovery_of_any_publisher_raises_the_venue_once() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, key(9201));
        set(&h, key(9101), false);
        set(&h, key(9201), false);
        assert_eq!(
            set(&h, key(9101), true),
            Some(true),
            "venue edge back to up"
        );
        assert_eq!(set(&h, key(9201), true), None, "already up: no second edge");
    }

    #[test]
    fn repeated_same_state_reports_no_edge() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        assert_eq!(set(&h, key(9101), true), None);
        assert_eq!(set(&h, key(9101), false), Some(false));
        assert_eq!(set(&h, key(9101), false), None);
    }

    /// A respawned receiver raises the venue, and it does so **on the edge** — otherwise the peer's
    /// later genuine recovery reports no edge and the wire `status` stays "down" forever.
    #[test]
    fn respawn_into_a_down_venue_publishes_the_up_edge() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        set(&h, key(9101), false);
        assert!(!h.venue_up(V));
        assert_eq!(
            register(&h, key(9201)),
            Some(true),
            "respawn raises the venue"
        );
        assert_eq!(set(&h, key(9101), true), None, "peer recovery: already up");
    }

    /// A deregistered (aborted/exited) receiver must not hold its venue down forever, and losing
    /// the last up receiver is a venue-down edge.
    #[test]
    fn deregister_drops_a_down_receiver_from_the_aggregate() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, key(9201));
        set(&h, key(9101), false);
        assert_eq!(deregister(&h, key(9101)), None, "9201 still up: no edge");
        assert!(h.venue_up(V), "only the live, up receiver counts");
        assert_eq!(set(&h, key(9201), false), Some(false));
        // Deregistering the last receiver leaves no receivers: the venue is not "up".
        assert_eq!(deregister(&h, key(9201)), None, "already down: no edge");
        assert!(!h.venue_up(V));
    }

    #[test]
    fn deregistering_the_last_up_receiver_is_a_down_edge() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        assert_eq!(deregister(&h, key(9101)), Some(false));
        assert!(!h.venue_up(V));
    }

    /// Venues are independent aggregates.
    #[test]
    fn venues_are_isolated() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, ("Other", FeedKind::TopOfBook, 9101));
        assert_eq!(set(&h, key(9101), false), Some(false));
        assert!(h.venue_up("Other"), "other venue unaffected");
    }

    /// A depth-only (MBO) receiver is not a quote-bearing carrier: it must neither take the venue
    /// down on its own nor mask a total outage of the venue's quote publishers.
    #[test]
    fn depth_only_receivers_are_excluded_from_the_venue_aggregate() {
        let mbo = (V, FeedKind::MarketByOrder, 10101);
        let h = FeedHealth::new();
        register(&h, key(9101));
        assert_eq!(
            register(&h, mbo),
            None,
            "MBO does not raise an already-up venue"
        );

        // A wedged MBO mirror is not a venue outage while TOB streams.
        assert_eq!(set(&h, mbo, false), None);
        assert!(h.venue_up(V));

        // ...and a live MBO must not mask the quote feed going fully silent.
        set(&h, mbo, true);
        assert_eq!(
            set(&h, key(9101), false),
            Some(false),
            "all quote publishers down -> venue down even though MBO is up"
        );
        assert!(!h.venue_up(V));
    }

    /// A venue with only depth-only receivers falls back to counting them, so it doesn't read
    /// permanently down (which would fire the headline `dz_feed_up == 0` alert forever).
    #[test]
    fn a_depth_only_venue_falls_back_to_its_registered_receivers() {
        let h = FeedHealth::new();
        let mbo = ("DepthOnly", FeedKind::MarketByOrder, 10101);
        assert_eq!(register(&h, mbo), Some(true));
        assert!(h.venue_up("DepthOnly"));
        assert_eq!(set(&h, mbo, false), Some(false));
    }
}
