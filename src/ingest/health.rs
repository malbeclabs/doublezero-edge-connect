//! Shared receiver liveness, aggregated to the venue-level feed health `PROTOCOL.md` promises.
//!
//! One venue is served by N receivers - one per publisher per protocol (see `ingest::feeds`). The
//! wire `status` message and `dz_feed_up` are **venue**-level, so neither may flip just because a
//! single publisher wedged: a venue is down only when EVERY registered quote-bearing receiver for
//! it is down. Per-publisher detail lives in `dz_receiver_up{venue,category,kind,publisher}`
//! instead.
//!
//! "Venue" here is a [`StatusKey`], not the bare registry name: several Source IDs may share one
//! name (two matching engines of one exchange), and PROTOCOL.md promises each its own `status`. A
//! row that declares its `source_id` aggregates under that ID alone, so one live engine cannot mask
//! the other's outage; a row that declares none keeps the name-level `(venue, None)` key.
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
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::ingest::feeds::FeedKind;

/// Identity of one receiver: `(venue, category, kind, base port)`. **The same tuple** as
/// `reconcile::FeedKey`, which is what lets the reconciler pass its own keys to [`FeedHealth::liveness`]
/// directly. The category rides along because `(venue, kind)` is no longer unique: one Source ID can
/// carry disjoint instrument universes, and two rows of the same kind under one venue would otherwise
/// share a liveness entry and report each other's health.
pub type ReceiverKey = (&'static str, &'static str, FeedKind, u16);

/// What the venue-level aggregate is keyed on: the row's `venue` plus its declared Source ID
/// (`Feed::source_id`), or `None` for a row that declares none. Deliberately **not** part of
/// [`ReceiverKey`], which has to stay the reconciler's `FeedKey`; each receiver states its status
/// key once, at [`FeedHealth::register`].
pub type StatusKey = (&'static str, Option<u16>);

/// A receiver's liveness for the purpose of tape ownership, **ordered best first**: the derived
/// `Ord` is what `reconcile::tape_owners` sorts on ahead of feed-kind rank, so the variant order
/// here is load-bearing and not cosmetic.
///
/// `Unregistered` sits between the two on purpose. It is not `Up` — a row that never binds must not
/// outrank the peer that is streaming — and it is not `Down` either, or a cold start (where nothing
/// has bound yet) would demote every row at once and the fallback to rank would never happen. See
/// [`FeedHealth::liveness`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TapeLiveness {
    /// Registered and delivering.
    Up,
    /// Never registered: spawned but its sockets have not bound yet, or they never will.
    Unregistered,
    /// Registered and known silent.
    Down,
}

/// Whether a receiver of this protocol counts toward the venue-level `status` / `dz_feed_up`.
///
/// PROTOCOL.md's `status` is the health of the venue's **quote** feed (`stale_ms` is documented as
/// "milliseconds the quote feed had been silent"), which Top-of-Book carries and the two book
/// protocols do not — Market-by-Order is re-served as `depth`, Market-by-Price as `book`. Counting
/// either would break the contract in both directions: a wedged book mirror would report a venue
/// outage while quotes flow, and a live one would mask a total quote outage.
fn carries_venue_status(kind: FeedKind) -> bool {
    match kind {
        FeedKind::TopOfBook | FeedKind::Midpoint => true,
        FeedKind::MarketByOrder | FeedKind::MarketByPrice => false,
    }
}

/// Shared liveness of every running receiver, aggregated per venue. Cheap to clone via
/// [`SharedFeedHealth`]; every mutation is off the hot path (only watchdog edges touch it).
#[derive(Default)]
pub struct FeedHealth {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Registered receivers -> whether each is currently up. A receiver absent from this map is
    /// not running and does not count toward its venue's aggregate.
    up: HashMap<ReceiverKey, bool>,
    /// Each receiver's [`StatusKey`], recorded at [`FeedHealth::register`]. Never removed: a
    /// receiver's row does not change under it, and the map is bounded by the registry.
    status_of: HashMap<ReceiverKey, StatusKey>,
    /// Status keys that have had a quote-bearing receiver registered at some point in this
    /// process's life. **Sticky on purpose**: carriers leave `up` when their task stops (abort,
    /// exit, bind error), and without this a venue whose every quote receiver had exited would fall
    /// back to its depth-only receivers and publish `status: ok` with zero quotes flowing - the
    /// masking this module's contract forbids. Never removed, so bounded by the registry.
    carrier_keys: HashSet<StatusKey>,
}

impl State {
    /// `key`'s status key, falling back to the name level for a receiver never registered (only
    /// [`FeedHealth::set`] on an unregistered key reaches that, which production never does).
    fn status_key(&self, key: &ReceiverKey) -> StatusKey {
        self.status_of.get(key).copied().unwrap_or((key.0, None))
    }
}

/// Handle cloned into each receiver task and held by the reconciler.
pub type SharedFeedHealth = Arc<FeedHealth>;

/// Whether any registered **quote-bearing** receiver under `status` is up, falling back to any
/// registered receiver when this process has never run a quote-bearing one for it (a depth-only
/// venue, or an MBO-only `--publisher-port` selection, would otherwise read permanently down and
/// fire the headline alert forever).
///
/// The fallback is gated on `carrier_keys`, not on what is in `up` right now: a carrier that
/// *stopped* must keep the venue honest rather than hand the aggregate to a depth-only peer.
fn status_up_in(state: &State, status: StatusKey) -> bool {
    let (mut carrier_up, mut any_up) = (false, false);
    for (key, up) in state.up.iter() {
        if state.status_key(key) != status {
            continue;
        }
        any_up |= *up;
        if carries_venue_status(key.2) {
            carrier_up |= *up;
        }
    }
    if state.carrier_keys.contains(&status) {
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
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Apply `mutate` to the map and invoke `on_edge(up)` — still holding the lock — iff the
    /// aggregate for `key`'s status key flipped. The single place the edge is decided, so
    /// publication can never be reordered against the transition that caused it.
    fn with_edge(
        &self,
        key: &ReceiverKey,
        mutate: impl FnOnce(&mut State),
        on_edge: impl FnOnce(bool),
    ) {
        let mut state = self.lock();
        let status = state.status_key(key);
        let was = status_up_in(&state, status);
        mutate(&mut state);
        let now = status_up_in(&state, status);
        if was != now {
            on_edge(now);
        }
    }

    /// Mark a starting receiver as up, publishing the venue edge if this raised the aggregate (a
    /// receiver respawned into a down venue). Called once at receiver setup. `true` rather than
    /// "unknown" keeps the pre-existing healthy-until-proven-silent semantics: the idle watchdog
    /// takes the venue down within `IDLE_REJOIN` if no data actually arrives.
    ///
    /// `source_id` is the receiver's row's declared Source ID (`Feed::source_id`), which picks the
    /// [`StatusKey`] this receiver aggregates under for the rest of its life.
    pub fn register(&self, key: ReceiverKey, source_id: Option<u16>, on_edge: impl FnOnce(bool)) {
        let status = (key.0, source_id);
        // Recorded before `with_edge` reads it, so the edge is decided under the key it belongs to.
        self.lock().status_of.insert(key, status);
        self.with_edge(
            &key,
            |s| {
                s.up.insert(key, true);
                if carries_venue_status(key.2) {
                    s.carrier_keys.insert(status);
                }
            },
            on_edge,
        );
    }

    /// Forget a stopped receiver (aborted by the reconciler, or exited on its own), publishing the
    /// venue edge if it was the last one up. Without this a receiver that was down when it stopped
    /// would pin its venue down forever.
    pub fn deregister(&self, key: ReceiverKey, on_edge: impl FnOnce(bool)) {
        self.with_edge(
            &key,
            |s| {
                s.up.remove(&key);
            },
            on_edge,
        );
    }

    /// Whether any registered quote-bearing receiver under `status` is up. A key with no
    /// registered receivers is not up (nothing is serving it).
    pub fn status_up(&self, status: StatusKey) -> bool {
        status_up_in(&self.lock(), status)
    }

    /// The health of the engine behind wire Source ID `source_id`, named `venue`: its own
    /// [`StatusKey`] when a registered row declares that ID, else the name-level `(venue, None)`
    /// aggregate every undeclared row reports under. What a per-product `status` reads, since a
    /// product carries its own Source ID.
    pub fn source_up(&self, venue: &str, source_id: u16) -> bool {
        let state = self.lock();
        let declared = state
            .status_of
            .values()
            .find(|(v, id)| *v == venue && *id == Some(source_id));
        match declared {
            Some(&status) => status_up_in(&state, status),
            None => state
                .status_of
                .values()
                .find(|(v, id)| *v == venue && id.is_none())
                .is_some_and(|&status| status_up_in(&state, status)),
        }
    }

    /// This receiver's liveness as the reconciler's tape ownership orders it — a **three**-state
    /// answer, because "registered and down" and "never registered" are different facts and
    /// collapsing either into the other breaks a different case.
    ///
    /// Folding `Unregistered` into `Up` (what a plain `is_down` did) lets a receiver that never
    /// binds hold rank 0 forever: it returns `Err`, is reaped and respawned every tick without ever
    /// registering, so its key never becomes `Some(false)` while it outranks the peer that is
    /// actually streaming — and the venue's tape goes silent indefinitely. Folding it into `Down`
    /// instead bounces the tape on every activation, and leaves a cold start — where no row has
    /// bound yet — with no owner at all.
    pub fn liveness(&self, key: &ReceiverKey) -> TapeLiveness {
        match self.lock().up.get(key) {
            Some(true) => TapeLiveness::Up,
            Some(false) => TapeLiveness::Down,
            None => TapeLiveness::Unregistered,
        }
    }

    /// Record `key`'s liveness, publishing the venue edge if the aggregate flipped — so one
    /// `status` transition fires per venue change rather than one per receiver.
    pub fn set(&self, key: ReceiverKey, up: bool, on_edge: impl FnOnce(bool)) {
        self.with_edge(
            &key,
            |s| {
                s.up.insert(key, up);
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
    /// One category throughout: these tests are about the venue aggregate, which the category does
    /// not enter into — it is only there to keep two rows of one kind under a venue distinct.
    const C: &str = "testcategory";
    fn key(base_port: u16) -> ReceiverKey {
        (V, C, FeedKind::TopOfBook, base_port)
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
        h.register(k, None, |v| edge.set(Some(v)));
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
        assert!(!h.status_up((V, None)), "unknown venue is not up");
        assert_eq!(register(&h, key(9101)), Some(true), "venue edge to up");
        assert!(h.status_up((V, None)));
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
        assert!(h.status_up((V, None)));
        assert_eq!(
            set(&h, key(9201), false),
            Some(false),
            "last publisher down -> venue edge to down"
        );
        assert!(!h.status_up((V, None)));
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
        assert!(!h.status_up((V, None)));
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
        assert!(h.status_up((V, None)), "only the live, up receiver counts");
        assert_eq!(set(&h, key(9201), false), Some(false));
        // Deregistering the last receiver leaves no receivers: the venue is not "up".
        assert_eq!(deregister(&h, key(9201)), None, "already down: no edge");
        assert!(!h.status_up((V, None)));
    }

    #[test]
    fn deregistering_the_last_up_receiver_is_a_down_edge() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        assert_eq!(deregister(&h, key(9101)), Some(false));
        assert!(!h.status_up((V, None)));
    }

    /// Venues are independent aggregates.
    #[test]
    fn venues_are_isolated() {
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, ("Other", C, FeedKind::TopOfBook, 9101));
        assert_eq!(set(&h, key(9101), false), Some(false));
        assert!(h.status_up(("Other", None)), "other venue unaffected");
    }

    /// A depth-only (MBO) receiver is not a quote-bearing carrier: it must neither take the venue
    /// down on its own nor mask a total outage of the venue's quote publishers.
    #[test]
    fn depth_only_receivers_are_excluded_from_the_venue_aggregate() {
        let mbo = (V, C, FeedKind::MarketByOrder, 10101);
        let h = FeedHealth::new();
        register(&h, key(9101));
        assert_eq!(
            register(&h, mbo),
            None,
            "MBO does not raise an already-up venue"
        );

        // A wedged MBO mirror is not a venue outage while TOB streams.
        assert_eq!(set(&h, mbo, false), None);
        assert!(h.status_up((V, None)));

        // ...and a live MBO must not mask the quote feed going fully silent.
        set(&h, mbo, true);
        assert_eq!(
            set(&h, key(9101), false),
            Some(false),
            "all quote publishers down -> venue down even though MBO is up"
        );
        assert!(!h.status_up((V, None)));
    }

    /// A **stopped** quote carrier must not hand the venue aggregate to a depth-only peer. All the
    /// quote receivers wedging and then exiting (abort, panic, bind error) used to erase the venue's
    /// carrier status, letting the live MBO receiver satisfy the fallback and publish `status: ok`
    /// with zero quotes flowing.
    #[test]
    fn a_deregistered_carrier_does_not_let_depth_mask_a_quote_outage() {
        let mbo = (V, C, FeedKind::MarketByOrder, 10101);
        let h = FeedHealth::new();
        register(&h, key(9101));
        register(&h, mbo);

        assert_eq!(set(&h, key(9101), false), Some(false), "quotes silent");
        assert_eq!(
            deregister(&h, key(9101)),
            None,
            "the carrier exiting is not a recovery"
        );
        assert!(
            !h.status_up((V, None)),
            "depth-only receivers left: venue stays down"
        );
    }

    /// A venue with only depth-only receivers falls back to counting them, so it doesn't read
    /// permanently down (which would fire the headline `dz_feed_up == 0` alert forever).
    #[test]
    fn a_depth_only_venue_falls_back_to_its_registered_receivers() {
        let h = FeedHealth::new();
        let mbo = ("DepthOnly", C, FeedKind::MarketByOrder, 10101);
        assert_eq!(register(&h, mbo), Some(true));
        assert!(h.status_up(("DepthOnly", None)));
        assert_eq!(set(&h, mbo, false), Some(false));
    }

    /// Two engines under one registry name, each row declaring its own Source ID — the Binance
    /// shape. Same venue, same kind, same base port; only the category tells the receivers apart.
    fn engine(category: &'static str) -> ReceiverKey {
        ("TwoEngines", category, FeedKind::TopOfBook, 30001)
    }
    fn register_as(h: &FeedHealth, k: ReceiverKey, id: Option<u16>) -> Option<bool> {
        let edge = Cell::new(None);
        h.register(k, id, |v| edge.set(Some(v)));
        edge.get()
    }

    /// The point of the status key: one live engine must not mask the other's outage.
    #[test]
    fn engines_sharing_a_name_keep_their_own_status() {
        let h = FeedHealth::new();
        let (perp, spot) = (engine("usdsm"), engine("spot"));
        assert_eq!(register_as(&h, perp, Some(6)), Some(true));
        assert_eq!(
            register_as(&h, spot, Some(8)),
            Some(true),
            "each engine raises its own key"
        );

        assert_eq!(set(&h, spot, false), Some(false), "spot's own edge fires");
        assert!(!h.status_up(("TwoEngines", Some(8))));
        assert!(h.status_up(("TwoEngines", Some(6))), "perp unaffected");

        assert!(!h.source_up("TwoEngines", 8));
        assert!(h.source_up("TwoEngines", 6));
    }

    /// A row that declares no ID keeps the name-level key, and a product whose ID no row declares
    /// reads that aggregate — today's behaviour for every existing row.
    #[test]
    fn an_undeclared_row_reports_at_the_name_level() {
        let h = FeedHealth::new();
        let k = ("NameLevel", C, FeedKind::TopOfBook, 9001);
        assert_eq!(register_as(&h, k, None), Some(true));
        assert!(h.status_up(("NameLevel", None)));
        assert!(h.source_up("NameLevel", 1), "any ID under the name");
        assert!(h.source_up("NameLevel", 7));
        set(&h, k, false);
        assert!(!h.source_up("NameLevel", 1));
    }

    /// The reconciler passes its own `FeedKey` to `liveness`; the status key must not have changed
    /// what that takes.
    #[test]
    fn liveness_still_takes_the_reconcilers_key() {
        let h = FeedHealth::new();
        let key: crate::ingest::reconcile::FeedKey = engine("usdsm");
        assert_eq!(h.liveness(&key), TapeLiveness::Unregistered);
        register_as(&h, key, Some(6));
        assert_eq!(h.liveness(&key), TapeLiveness::Up);
    }
}
