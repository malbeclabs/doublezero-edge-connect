//! Shared receiver liveness, aggregated to the venue-level feed health `PROTOCOL.md` promises.
//!
//! One venue is served by N receivers - one per publisher per protocol (see `ingest::feeds`). The
//! wire `status` message and `dz_feed_up` are **venue**-level, so neither may flip just because a
//! single publisher wedged: a venue is down only when EVERY registered receiver for it is down.
//! Per-publisher detail lives in `dz_receiver_up{venue,kind,publisher}` instead.
//!
//! [`FeedHealth::set`] returns the venue-level *edge* (and only the edge), so the caller emits a
//! `status` transition exactly once per venue change rather than once per receiver.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::ingest::feeds::FeedKind;

/// Identity of one receiver: `(venue, kind, publisher)`. Same shape as `reconcile::FeedKey`.
pub type ReceiverKey = (&'static str, FeedKind, &'static str);

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

impl FeedHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock, recovering from a poisoned mutex. The critical section is `HashMap` work only, so the
    /// map is always left consistent; recovering keeps an unrelated panic in one receiver from
    /// cascading into every other venue's health reporting (the same reasoning as
    /// `arbiter::lock`).
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ReceiverKey, bool>> {
        self.up.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Mark a starting receiver as up. Called once at receiver setup.
    pub fn register(&self, key: ReceiverKey) {
        self.lock().insert(key, true);
    }

    /// Forget a stopped receiver (aborted by the reconciler, or exited on its own). Without this a
    /// receiver that was down when it stopped would pin its venue down forever.
    pub fn deregister(&self, key: ReceiverKey) {
        self.lock().remove(&key);
    }

    /// Whether any registered receiver for `venue` is up. A venue with no registered receivers is
    /// not up (nothing is serving it).
    pub fn venue_up(&self, venue: &str) -> bool {
        self.lock().iter().any(|((v, _, _), up)| *v == venue && *up)
    }

    /// Record `key`'s liveness. Returns `Some(venue_up)` iff the **venue** aggregate flipped as a
    /// result, so the caller emits one `status` transition per venue change rather than one per
    /// receiver; `None` means no venue-visible change.
    pub fn set(&self, key: ReceiverKey, up: bool) -> Option<bool> {
        let mut map = self.lock();
        let venue = key.0;
        let was = map.iter().any(|((v, _, _), u)| *v == venue && *u);
        map.insert(key, up);
        let now = map.iter().any(|((v, _, _), u)| *v == venue && *u);
        (was != now).then_some(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: &str = "TestVenue";
    fn key(pubname: &'static str) -> ReceiverKey {
        (V, FeedKind::TopOfBook, pubname)
    }

    #[test]
    fn first_registration_makes_the_venue_up() {
        let h = FeedHealth::new();
        assert!(!h.venue_up(V), "unknown venue is not up");
        h.register(key("p1"));
        assert!(h.venue_up(V));
    }

    /// The whole point: one wedged publisher must NOT take the venue down while a peer streams.
    #[test]
    fn venue_stays_up_while_any_publisher_is_up() {
        let h = FeedHealth::new();
        h.register(key("p1"));
        h.register(key("p2"));
        assert_eq!(h.set(key("p1"), false), None, "no venue edge: p2 still up");
        assert!(h.venue_up(V));
        assert_eq!(
            h.set(key("p2"), false),
            Some(false),
            "last publisher down -> venue edge to down"
        );
        assert!(!h.venue_up(V));
    }

    #[test]
    fn recovery_of_any_publisher_raises_the_venue_once() {
        let h = FeedHealth::new();
        h.register(key("p1"));
        h.register(key("p2"));
        h.set(key("p1"), false);
        h.set(key("p2"), false);
        assert_eq!(h.set(key("p1"), true), Some(true), "venue edge back to up");
        assert_eq!(h.set(key("p2"), true), None, "already up: no second edge");
    }

    #[test]
    fn repeated_same_state_reports_no_edge() {
        let h = FeedHealth::new();
        h.register(key("p1"));
        assert_eq!(h.set(key("p1"), true), None);
        assert_eq!(h.set(key("p1"), false), Some(false));
        assert_eq!(h.set(key("p1"), false), None);
    }

    /// A deregistered (aborted/exited) receiver must not hold its venue down forever.
    #[test]
    fn deregister_drops_a_down_receiver_from_the_aggregate() {
        let h = FeedHealth::new();
        h.register(key("p1"));
        h.register(key("p2"));
        h.set(key("p1"), false);
        h.deregister(key("p1"));
        assert!(h.venue_up(V), "only the live, up receiver counts");
        assert_eq!(h.set(key("p2"), false), Some(false));
        // Deregistering the last receiver leaves no receivers: the venue is not "up".
        h.deregister(key("p2"));
        assert!(!h.venue_up(V));
    }

    /// Different protocols of one venue share the aggregate (a wedged MBO alone is not a venue
    /// outage), and other venues are independent.
    #[test]
    fn aggregate_spans_kinds_and_isolates_venues() {
        let h = FeedHealth::new();
        h.register((V, FeedKind::TopOfBook, "p1"));
        h.register((V, FeedKind::MarketByOrder, "p1"));
        h.register(("Other", FeedKind::TopOfBook, "p1"));
        assert_eq!(h.set((V, FeedKind::MarketByOrder, "p1"), false), None);
        assert!(h.venue_up(V), "TOB still up -> venue up");
        assert!(h.venue_up("Other"));
        assert_eq!(h.set((V, FeedKind::TopOfBook, "p1"), false), Some(false));
        assert!(h.venue_up("Other"), "other venue unaffected");
    }
}
