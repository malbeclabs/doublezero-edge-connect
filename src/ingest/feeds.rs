//! The registry of DoubleZero Edge feeds the bridge ingests — the **types**; the rows themselves
//! are data, loaded at startup from a document (see [`crate::ingest::registry`]).
//!
//! Each feed is one multicast group mapped to exactly one venue, plus the **protocol** it speaks
//! ([`FeedKind`]) and the **publishers** mirroring it ([`FeedPublisher`]), each with its own port
//! block. The bridge spawns one receiver per `(venue, category, kind, publisher)`; consumers then
//! filter by `venue` over the WebSocket (see PROTOCOL.md subscriptions). To ingest another venue's
//! feed, add a row to `registry.json` (or to the document the deployment supplies) — no code
//! changes are needed, and no rebuild if the document is supplied at runtime.

use std::{net::Ipv4Addr, sync::OnceLock};

use tracing::warn;

use crate::ingest::registry;

/// Which edge-feed-spec protocol a feed speaks. Selects the datagram magic + decoder + receiver
/// processor the bridge uses for it. See https://github.com/malbeclabs/edge-feed-spec.
// `Midpoint`/`MarketByOrder`/`MarketByPrice` are matched on by the receiver but only *constructed*
// by FEEDS rows, added once their live multicast endpoints are known - hence the dead_code allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum FeedKind {
    /// Top-of-Book & Trades (datagram magic `0x445A`): best bid/ask quotes + trade prints.
    TopOfBook,
    /// Midpoint (datagram magic `0x4D44`): a single derived mid price per instrument.
    Midpoint,
    /// Market-by-Order (datagram magic `0x4444`): full L3 order book with snapshot+delta recovery.
    MarketByOrder,
    /// Market-by-Price (datagram magic `0x4442`): the price-aggregated book with snapshot+delta
    /// recovery, re-served as the incremental `book` product.
    MarketByPrice,
}

impl FeedKind {
    /// Stable, low-cardinality label for the metrics `kind` dimension and log fields.
    pub fn label(self) -> &'static str {
        match self {
            FeedKind::TopOfBook => "tob",
            FeedKind::Midpoint => "midpoint",
            FeedKind::MarketByOrder => "mbo",
            FeedKind::MarketByPrice => "mbp",
        }
    }
}

/// The multicast ports a feed splits its messages across. Every protocol uses a `mktdata` port
/// (the data feed the liveness watchdog tracks) and a `refdata` port (instrument defs +
/// manifest); Market-by-Order adds a dedicated `snapshot` port for its in-band book recovery.
/// A loopback demo that carries everything on one port is expressed as `mktdata == refdata`.
// `ThreePort` is constructed by Market-by-Order FEEDS rows (added with their endpoints).
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum FeedPorts {
    /// Top-of-Book and Midpoint: market data + reference data.
    TwoPort { mktdata: u16, refdata: u16 },
    /// Market-by-Order: market data (deltas/trades) + reference data + snapshot recovery feed.
    ThreePort {
        mktdata: u16,
        refdata: u16,
        snapshot: u16,
    },
}

impl FeedPorts {
    /// The market-data port (quotes / midpoints / order deltas) - the one the liveness watchdog
    /// tracks, since reference/snapshot ports keep ticking even when market data is wedged.
    pub fn mktdata(&self) -> u16 {
        match *self {
            FeedPorts::TwoPort { mktdata, .. } | FeedPorts::ThreePort { mktdata, .. } => mktdata,
        }
    }
    /// The reference-data port (instrument definitions + manifest).
    pub fn refdata(&self) -> u16 {
        match *self {
            FeedPorts::TwoPort { refdata, .. } | FeedPorts::ThreePort { refdata, .. } => refdata,
        }
    }
    /// The snapshot-recovery port (Market-by-Order only), if any.
    pub fn snapshot(&self) -> Option<u16> {
        match *self {
            FeedPorts::ThreePort { snapshot, .. } => Some(snapshot),
            FeedPorts::TwoPort { .. } => None,
        }
    }
}

/// One publisher mirroring a feed: the port block it publishes on.
///
/// Independent publishers mirror one venue's feed so subscribers can race them (see
/// `ingest::arbiter`). Two deployment models exist and both are supported:
///
/// - **Distinct port blocks per publisher** (what the live Hyperliquid fleet does, on arbitrary
///   base ports — see the `FEEDS` docs): one `FeedPublisher` row per publisher, one receiver task
///   each, and each task sees exactly one source IP.
/// - **Shared port block** (all publishers to one `(group, port)`): a single `FeedPublisher` row,
///   one receiver task, and that task sees N source IPs.
///
/// Either way the *publisher identity* the arbiter races on is the datagram source IP, never the
/// port — so the dedup path is identical. The operator-facing identity is the
/// [`base port`](FeedPublisher::base_port): what `--publisher-port` selects and what the
/// `publisher` metric label carries. Deliberately a port and not a host name — the port block is
/// the publisher property this protocol actually defines.
#[derive(Debug, Clone, Copy)]
pub struct FeedPublisher {
    /// The port block this publisher sends on.
    pub ports: FeedPorts,
    /// The channel id this publisher's block was **derived** from (`port = base + channel_id`), or
    /// `None` when the document wrote the block out verbatim.
    ///
    /// **What it records is the document's *form*, not the feed's semantics.** `Some` means the row
    /// used the `derived` shape and this block came from that channel; `None` means the row used
    /// `explicit`. Nothing more.
    ///
    /// **What it is used for** is the channel filter ([`crate::ingest::channel_filter`]): `Some` is
    /// exactly the condition under which declining a channel is free, because that channel has a
    /// socket of its own and never binding it makes the kernel discard its traffic before userspace.
    /// That much the form does license.
    ///
    /// ⚠️ **It is not the semantic discriminator, and the channel filter's refusal message assumes
    /// it is.**
    /// The question the refusal actually answers — does `channel_id` partition markets, or identify
    /// mirrors of one complete universe? — is a property of the *feed*, and this field only proxies
    /// it. The schema permits an `explicit` row that writes per-channel blocks out at `base + id` by
    /// hand: every publisher would get `None`, narrowing it would be refused, and the refusal would
    /// tell the operator that "each publisher carries the complete instrument universe", which is
    /// **false about that row**. The proper home for that fact is a field on the document row, which
    /// is where it should move if such a row ever appears; recording it per publisher here is a
    /// convenience that happens to coincide with the truth for every row published today.
    ///
    /// Deliberately recorded rather than recomputed from `base_port() - base`: the base is a
    /// property of the document's derived block and is gone by the time these rows are `'static`,
    /// and re-deriving it (by, say, taking the minimum base port) would mint a channel id for a flat
    /// row too — turning the coincidence above into a guarantee of being wrong.
    pub channel: Option<u8>,
    /// A short human label for this channel (e.g. `"sports.nfl"`), carried verbatim from the
    /// document's `derived.channels` published set when that entry supplied one. `None` for every publisher
    /// today: the built-in document ships with no labels (the upstream inventory that owns them is
    /// still being made reachable at runtime), and an `explicit` block has no channel concept to
    /// label at all.
    ///
    /// **Display only.** It is never used for lookup, matching or identity — the channel **id** is
    /// the only contract — and it is not accepted anywhere a channel id is expected. A `range`
    /// entry may never carry one (a range names many channels, not one), which the schema enforces
    /// structurally: only the single-id shape has a `label` field at all, so a `label`
    /// written on a `range` entry lands in that entry's unknown-keys map and is warned about like any
    /// other unrecognised key, never applied.
    pub label: Option<&'static str>,
}

impl FeedPublisher {
    /// This publisher's stable identity within its feed: the market-data (base) port of its block.
    /// Used for the `publisher` metric label, log fields and the reconciler's task key.
    pub fn base_port(&self) -> u16 {
        self.ports.mktdata()
    }
}

/// How the bridge resolves two publishers mirroring one venue.
///
/// Both modes hold exactly one authoritative publisher per key; what differs is when authority
/// transfers. `Coordinated` re-latches every tick, because the publishers stamp a venue clock that
/// is comparable between them. `Sticky` cannot: its paths carry no shared coordinate — no stable
/// entry id, no per-entry venue timestamp, and the transport's own send time is not the venue's —
/// and a content hash is no substitute, since a level oscillating 100 -> 0 -> 100 emits
/// byte-identical updates and collapsing those leaves a subscriber holding 0 at a price that has
/// liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitrationMode {
    /// Comparable venue clock: latch to the tick's leader, re-latch every tick.
    Coordinated,
    /// No comparable coordinate: elect one path and hold it, transferring only on a health verdict,
    /// on silence, or on a sustained speed margin.
    Sticky,
}

#[derive(Debug, Clone, Copy)]
pub struct Feed {
    /// Venue name stamped on every instrument and message from this feed. Matches the
    /// edge-feed-spec source registry name (e.g. "HYPERLIQUID" for SourceID 1).
    pub venue: &'static str,
    /// The instrument universe this row carries. Rows sharing a `(venue, category)` are mirrors
    /// of one another and arbitrate as one; rows differing in it carry disjoint universes and
    /// must never contest each other's tape or book. `topology.md` §2 calls this the category.
    ///
    /// Necessary because a single `Source ID` — and therefore a single `venue` — can span
    /// universes that share no instrument. Without it, the venue-wide tape gate elects one
    /// publisher across both and drops the other universe's prints entirely, which surfaces as
    /// empty candles indistinguishable from a market that did not trade.
    pub category: &'static str,
    /// The DoubleZero multicast group **code** for this feed's group (e.g. "tiredsolid",
    /// "scottsdale") — the identifier `doublezero status`/`multicast group list` report. The
    /// subscription reconciler matches this against the host's subscribed `S:<code>` entries to
    /// decide whether to activate the feed. Multiple feeds (e.g. a venue's TOB + MBO) can share one
    /// code, since they ride the same multicast group.
    pub code: &'static str,
    /// Which edge-feed-spec protocol this feed speaks (selects decoder + processor).
    pub kind: FeedKind,
    /// Multicast group for the feed.
    pub group: Ipv4Addr,
    /// The publishers mirroring this feed, each with its own port block. One receiver task is
    /// spawned per entry. A feed whose publishers all share one port block lists exactly one
    /// entry (see [`FeedPublisher`]).
    pub publishers: &'static [FeedPublisher],
    /// Whether this feed *can* carry the venue's `trade` tape — the registry's declaration of intent,
    /// **not a runtime gate**: nothing reads it on the emit path, and setting it `false` on a
    /// tape-ranked kind suppresses nothing. Which claiming row actually serves the tape is the
    /// reconciler's decision (`reconcile::tape_owners`), because a venue's rows are separately
    /// subscription-gated and the tape must survive on whichever subset is up.
    /// `emit_trades_agrees_with_the_tape_ownership_rule` is what ties the declaration to that
    /// ranking, so a disagreeing row fails the build rather than behaving unexpectedly.
    pub emit_trades: bool,
    /// How this venue's mirrored publishers are arbitrated. Declared per row but consumed per
    /// venue, so a venue's rows must agree (pinned by `arbitration_mode_agrees_across_a_venues_rows`).
    pub arbitration: ArbitrationMode,
    /// A second publisher mirrors this row's whole published set on the **same ports**, stamping every
    /// wire `channel_id` raised by this amount (`publisher_offset` in the document, a row-level
    /// field — an `explicit` row can declare it exactly like a `derived` one) — so the socket
    /// bound for channel `N` also receives datagrams stamped `N + offset`. `None` for every row with
    /// no such mirror.
    ///
    /// **Consumer-facing identity only.** Ingest subtracts this from a wire channel id at the
    /// point a message becomes catalog/history/book identity (`DatagramCtx::canonical_channel`), so
    /// the mirror's `N + offset` and the base publisher's `N` are one market — one catalog entry,
    /// one book, one history series — to everything downstream of that point. It must **never**
    /// be applied to producer-side state (books, sequence tracking, reset counts, snapshot
    /// cycles): those stay keyed on the raw wire channel id precisely because the two paths are
    /// separately sequenced, and collapsing that would corrupt book recovery.
    ///
    /// Ports are unaffected: the mirror sends to the identical port block, so this never factors
    /// into `FeedPublisher::base_port()` or the derived-port arithmetic.
    pub mirror_offset: Option<u8>,
}

/// Every feed row known to the bridge, resolved once at startup from the registry document.
///
/// Deliberately **not** `pub`: a consumer reading the backing storage directly would pin itself to
/// whatever set happened to be compiled in, and a row that only ever arrives from a supplied
/// document would go missing with nothing to show for it. Keeping this private turns that into a
/// compile error and leaves [`feeds()`] the one entry point.
static FEEDS: OnceLock<&'static [Feed]> = OnceLock::new();

/// The winning install's provenance (source/version/row+receiver counts) — the same figures
/// [`registry::Loaded::log_resolved`] logs once at startup, kept here so a running process can
/// report them (`/v1/status`'s `registry` block) without re-reading logs. Set alongside [`FEEDS`]
/// and never afterward, for the identical reason: a losing install must not overwrite the winner's
/// provenance any more than it overwrites its rows.
static REGISTRY_INFO: OnceLock<registry::RegistryInfo> = OnceLock::new();

/// Resolve the registry document from `source` and install it.
///
/// Called once from `main` before any receiver spawns. A repeat call is ignored rather than
/// swapping the set under running receivers — books and reference data are keyed to the topology
/// in effect when they were built.
///
/// The resolved registry is announced **after** the install wins, never before: a losing install
/// that had already logged "feed registry resolved" would leave a breadcrumb naming a document the
/// process then discarded, which is worse than no breadcrumb at all. A loser's rows stay leaked —
/// a one-off in a case that should not happen, and cheaper than making the install fallible.
pub async fn init(source: registry::Source) -> Result<(), registry::RegistryError> {
    let loaded = registry::load(source).await?;
    let info = loaded.info();
    if FEEDS.set(loaded.rows).is_ok() {
        let _ = REGISTRY_INFO.set(info);
        loaded.log_resolved();
    } else {
        warn!(
            source = loaded.origin(),
            "feed registry was already installed; this document was discarded"
        );
    }
    Ok(())
}

/// Install the compiled-in document if nothing is installed yet. Idempotent and synchronous.
///
/// The entry point for tests and for any embedding with no document to supply. Panics only if the
/// compiled-in document is itself invalid, which is a build-time defect.
pub fn init_built_in() {
    let _ = FEEDS.get_or_init(|| {
        let loaded =
            registry::load_built_in().expect("the built-in feed registry document is valid");
        let _ = REGISTRY_INFO.set(loaded.info());
        loaded.rows
    });
}

/// The resolved registry's provenance, for `/v1/status`'s `registry` block. `None` only before
/// [`init`]/[`init_built_in`] has run — never true once this process is serving requests, since the
/// registry resolves before any receiver or sink spawns.
pub fn registry_info() -> Option<&'static registry::RegistryInfo> {
    REGISTRY_INFO.get()
}

/// Every feed row known to the bridge. The one entry point — callers never touch the backing
/// storage directly.
///
/// Reading before [`init`] is a programming error, not a runtime condition: silently falling back
/// to the built-in document would let a misordered startup ingest a different feed set than the
/// operator supplied, with nothing to show for it.
///
/// **In this crate's own test build only**, the built-in document installs itself on first read.
/// A unit test has no `main` to call [`init`], and requiring every test that *transitively* reaches
/// this to install it first is an ordering dependency waiting to break — the offending test passes
/// for exactly as long as some other test in the same binary happens to run before it, and fails
/// the day someone runs it alone. Those tests therefore pin the **built-in document**, which is the
/// right subject: it is the copy every deployment falls back to when a fetch fails, so a slip in it
/// is a production defect and not a fixture typo. The binary and the integration tests link this
/// without `cfg(test)` and keep the strict rule.
pub fn feeds() -> &'static [Feed] {
    #[cfg(test)]
    init_built_in();
    FEEDS.get().copied().expect("feeds::init was not called")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::registry::sports_channel_ids;

    /// Leagues 10-29, sport channels 39-48, catch-all 49 — 31 in total. 30-38 are reserved for
    /// promoting a competition out of a tag channel and are not published; 63 and 120 were
    /// retired on 2026-08-08 and are never reissued. An id here that the publisher does not
    /// send binds a socket that stays silent, which reads as a dead publisher.
    #[test]
    fn the_sports_published_set_is_thirty_one_channels() {
        let ids = sports_channel_ids();
        assert_eq!(ids.len(), 31, "published set size changed: {ids:?}");
        assert_eq!(ids.first().copied(), Some(10));
        assert_eq!(ids.last().copied(), Some(49));
        for reserved in 30..=38 {
            assert!(
                !ids.contains(&reserved),
                "reserved id {reserved} is published"
            );
        }
        for retired in [63u8, 120] {
            assert!(!ids.contains(&retired), "retired id {retired} was reissued");
        }
    }

    /// A sports port is exactly `base + channel_id` on all three port roles. This is the property the
    /// publisher's own `validate_port_scheme` asserts, and the one a subscriber must match exactly
    /// or it joins the right group and hears silence.
    ///
    /// The bases are what the publishers are configured with, confirmed against the deployment
    /// inventory on 2026-08-09. `33000/43000` is the **top-of-book sibling's** base; a
    /// market-by-price row built on it joins the right group and receives nothing — which reads as
    /// a dead publisher, not as a misconfiguration.
    #[test]
    fn sports_ports_are_the_base_plus_the_channel_id() {
        let row = feeds()
            .iter()
            .find(|f| f.venue == "KALSHI" && f.category == "sports")
            .expect("no sports row");
        assert_eq!(row.code, "edge-kalshi-sports-mbp");
        assert_eq!(row.group, Ipv4Addr::new(233, 84, 178, 20));
        assert_eq!(row.kind, FeedKind::MarketByPrice);
        assert_eq!(row.publishers.len(), 31);

        for (p, id) in row.publishers.iter().zip(sports_channel_ids()) {
            let FeedPorts::ThreePort {
                mktdata,
                refdata,
                snapshot,
            } = p.ports
            else {
                panic!("sports publishers bind three ports");
            };
            assert_eq!(mktdata, 34000 + u16::from(id), "mktdata for channel {id}");
            assert_eq!(refdata, 44000 + u16::from(id), "refdata for channel {id}");
            assert_eq!(snapshot, 54000 + u16::from(id), "snapshot for channel {id}");
        }
    }

    /// Base ports identify a receiver task, so a collision would silently merge two channels'
    /// state machines into one task key.
    #[test]
    fn every_base_port_is_unique_within_a_feed() {
        for f in feeds() {
            let mut seen = std::collections::HashSet::new();
            for p in f.publishers {
                assert!(
                    seen.insert(p.base_port()),
                    "{} {:?} repeats base port {}",
                    f.venue,
                    f.kind,
                    p.base_port()
                );
            }
        }
    }

    /// Rows sharing a `(venue, category)` are mirrors and arbitrate as one; rows differing in it
    /// carry disjoint universes and must never contest each other. Uniqueness is on the triple —
    /// `(venue, kind)` alone cannot express two universes under one Source ID.
    #[test]
    fn venue_category_kind_triples_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for f in feeds() {
            assert!(
                seen.insert((f.venue, f.category, f.kind)),
                "duplicate (venue, category, kind): {} {} {:?}",
                f.venue,
                f.category,
                f.kind
            );
        }
    }

    /// Every row declares a category. An empty one would silently collapse into whatever other
    /// row also left it empty, re-creating the venue-wide contest this field exists to prevent.
    #[test]
    fn every_feed_declares_a_category() {
        for f in feeds() {
            assert!(
                !f.category.is_empty(),
                "{} {:?} has no category",
                f.venue,
                f.kind
            );
        }
    }

    #[test]
    fn every_feed_has_a_group_code() {
        // The reconciler matches `code` against `doublezero status` subscriptions, so every row
        // must carry one. Both Hyperliquid rows share the group `tiredsolid`; Phoenix is `scottsdale`.
        for f in feeds() {
            assert!(!f.code.is_empty(), "{} {:?} has no code", f.venue, f.kind);
        }
        // Keyed on `(venue, category, kind)` and not on venue alone: Lashay's two rows ride
        // *different* groups, which is what makes tape ownership a runtime decision in the first
        // place.
        for f in feeds() {
            let expected = match (f.venue, f.category, f.kind) {
                ("HYPERLIQUID", "perps", FeedKind::TopOfBook | FeedKind::MarketByOrder) => {
                    "tiredsolid"
                }
                ("PHOENIX", "spot", FeedKind::TopOfBook) => "scottsdale",
                ("KALSHI", "perps", FeedKind::TopOfBook) => "edge-kalshi-perps-tob",
                ("KALSHI", "perps", FeedKind::MarketByPrice) => "edge-kalshi-perps-mbp",
                ("KALSHI", "sports", FeedKind::MarketByPrice) => "edge-kalshi-sports-mbp",
                other => panic!("unexpected feed {other:?}"),
            };
            assert_eq!(f.code, expected, "{} {:?} has wrong code", f.venue, f.kind);
        }
    }

    #[test]
    fn hyperliquid_has_tob_and_mbo() {
        let kinds: std::collections::HashSet<FeedKind> = feeds()
            .iter()
            .filter(|f| f.venue.eq_ignore_ascii_case("hyperliquid"))
            .map(|f| f.kind)
            .collect();
        assert!(kinds.contains(&FeedKind::TopOfBook));
        assert!(kinds.contains(&FeedKind::MarketByOrder));
    }

    #[test]
    fn hyperliquid_tob_emits_trades() {
        let hl = feeds()
            .iter()
            .find(|f| f.venue == "HYPERLIQUID" && f.kind == FeedKind::TopOfBook)
            .unwrap();
        assert!(hl.emit_trades);
    }

    /// `emit_trades` is the row's static *capability* claim; which claiming row actually serves the
    /// tape is the reconciler's runtime decision (`reconcile::tape_owners`). The two must agree, in
    /// both directions:
    ///
    /// - a row claiming trades on a kind the ranking never admits would have a flag stuck false and
    ///   silently emit nothing — a venue with no tape at all if it is the only claimant;
    /// - a row not claiming trades on a rankable kind would be handed the tape and print anyway,
    ///   which is exactly the double-print the invariant exists to prevent.
    ///
    /// The invariant itself — at most one tape emitter per venue at any moment, which is what
    /// licenses the `trade_id == 0` bypass in `arbiter::emit` — is enforced at runtime by
    /// `tape_owners` (one row per venue) and the arbiter's per-venue tape leader (one path within it),
    /// with `dz_tape_owner_changes_total` / `dz_tape_path_transfers_total` reporting the moves.
    #[test]
    fn emit_trades_agrees_with_the_tape_ownership_rule() {
        for f in feeds() {
            assert_eq!(
                f.emit_trades,
                crate::ingest::reconcile::tape_rank_is_some(f.kind),
                "{} {:?}: emit_trades disagrees with the tape ranking",
                f.venue,
                f.kind
            );
        }
    }

    /// A venue's paths are the same hosts whatever protocol they speak, so every row for a venue
    /// must declare the same arbitration mode. Disagreement would make the arbiter's per-venue mode
    /// depend on which row registered last.
    #[test]
    fn arbitration_mode_agrees_across_a_venues_rows() {
        let mut modes = std::collections::HashMap::new();
        for f in feeds() {
            if let Some(prev) = modes.insert(f.venue, f.arbitration) {
                assert_eq!(
                    prev, f.arbitration,
                    "{} declares two arbitration modes",
                    f.venue
                );
            }
        }
    }

    /// The venues that predate arbitration modes race on a comparable venue clock and must keep
    /// doing so — the mode is a seam, not a behavior change. Scoped by exclusion rather than
    /// asserting over all of `FEEDS`, because `Sticky` exists precisely so a venue whose paths carry
    /// no shared clock can declare it; a new such venue is the feature working, not a regression.
    #[test]
    fn existing_venues_are_coordinated() {
        for f in feeds().iter().filter(|f| f.venue != "KALSHI") {
            assert_eq!(f.arbitration, ArbitrationMode::Coordinated, "{}", f.venue);
        }
    }

    #[test]
    fn port_accessors_cover_both_shapes() {
        let two = FeedPorts::TwoPort {
            mktdata: 1,
            refdata: 2,
        };
        assert_eq!(two.mktdata(), 1);
        assert_eq!(two.refdata(), 2);
        assert_eq!(two.snapshot(), None);

        let three = FeedPorts::ThreePort {
            mktdata: 1,
            refdata: 2,
            snapshot: 3,
        };
        assert_eq!(three.mktdata(), 1);
        assert_eq!(three.refdata(), 2);
        assert_eq!(three.snapshot(), Some(3));
    }

    /// Every feed must list at least one publisher, else it would bind nothing and silently
    /// contribute no data. Enforced for real in `registry::feed_from` (`RegistryError::
    /// EmptyPublishers`) now that a document can be supplied at runtime; this stays as coverage of
    /// the built-in document over the same path.
    #[test]
    fn every_feed_has_at_least_one_publisher() {
        for f in feeds() {
            assert!(
                !f.publishers.is_empty(),
                "{} {:?} lists no publishers",
                f.venue,
                f.kind
            );
        }
    }

    /// Base ports are the `publisher` metric label and the reconciler's task-key component, so they
    /// must be unique within a feed (a duplicate would collapse two receivers into one task key and
    /// merge their metrics).
    #[test]
    fn publisher_base_ports_unique_within_a_feed() {
        for f in feeds() {
            let mut seen = std::collections::HashSet::new();
            for p in f.publishers {
                assert!(
                    seen.insert(p.base_port()),
                    "{} {:?} has duplicate publisher base port {}",
                    f.venue,
                    f.kind,
                    p.base_port()
                );
            }
        }
    }

    /// No two receivers may bind the same `(group, port)`. Two sockets on one `(group, port)` land
    /// in the same `SO_REUSEPORT` set, so the kernel splits that group's datagrams arbitrarily
    /// between them — each receiver then sees a random subset of publishers, duplicating reference
    /// data and scrambling per-publisher metrics. This is the invariant `bind_multicast`'s
    /// bind-to-GROUP comment protects at the group level, extended to ports.
    #[test]
    fn group_port_pairs_are_globally_unique() {
        let mut seen = std::collections::HashSet::new();
        for f in feeds() {
            for p in f.publishers {
                let mut ports = vec![p.ports.mktdata(), p.ports.refdata()];
                if let Some(s) = p.ports.snapshot() {
                    ports.push(s);
                }
                for port in ports {
                    assert!(
                        seen.insert((f.group, port)),
                        "{} {:?} publisher {} reuses (group {}, port {})",
                        f.venue,
                        f.kind,
                        p.base_port(),
                        f.group,
                        port
                    );
                }
            }
        }
    }

    /// The Hyperliquid fleet mirrors one venue across eleven publishers - six DoubleZero hosts,
    /// four partners and one unattributed. Pins the count so a dropped row is caught, and pins the
    /// exact base-port set: the registry previously held only the six in-house hosts, which is the
    /// bug this list fixes, so "some publishers present" is not a strong enough assertion.
    #[test]
    fn hyperliquid_lists_the_whole_publisher_fleet() {
        let base_ports = |kind: FeedKind| -> Vec<u16> {
            let f = feeds()
                .iter()
                .find(|f| f.venue == "HYPERLIQUID" && f.kind == kind)
                .unwrap();
            let mut v: Vec<u16> = f.publishers.iter().map(|p| p.base_port()).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            base_ports(FeedKind::TopOfBook),
            vec![9001, 9011, 9101, 9201, 9301, 9401, 9501, 9601, 9701, 9801, 9901]
        );
        assert_eq!(
            base_ports(FeedKind::MarketByOrder),
            vec![10001, 10011, 10101, 10201, 10301, 10401, 10501, 10601, 10701, 10801, 10901]
        );
    }

    /// Within a publisher's block the offsets follow the publisher implementation: `+1`/`+2` on every
    /// Hyperliquid and Phoenix block and on `edge-kalshi-perps-tob`, `+10000`/`+20000` on `edge-kalshi-perps-mbp` (and on the
    /// sports market-by-price feed: 33010/43010/53010). The base port is free-form since v0.7, so this
    /// spacing is the only structural rule left — it is what an unseen block may be derived from
    /// (10901/10903 were derived this way from 10902), and a row that breaks *both* schemes is a
    /// transcription error rather than a new layout.
    ///
    /// Scoped **per row**, not per venue: Lashay's two rows legitimately use different schemes, and
    /// framing it by scheme rather than by a venue carve-out is what keeps a later Kalshi row from
    /// re-tripping it. Lashay's exact blocks are pinned by `the_kalshi_rows_expand_consistently_with_the_document`, so
    /// widening this one loses nothing.
    #[test]
    fn publisher_blocks_use_a_known_layout() {
        const SCHEMES: [u16; 2] = [1, 10_000];
        for f in feeds() {
            let mut feed_scheme = None;
            for p in f.publishers {
                let mkt = p.ports.mktdata();
                let scheme = SCHEMES
                    .iter()
                    .copied()
                    .find(|s| {
                        p.ports.refdata() == mkt + s
                            && p.ports.snapshot().is_none_or(|snap| snap == mkt + 2 * s)
                    })
                    .unwrap_or_else(|| {
                        panic!("{} {:?} block {mkt}: unknown port layout", f.venue, f.kind)
                    });
                // One publisher role serves a feed, so a block spaced differently from its peers is
                // a typo in that row and not a second deployment.
                assert_eq!(
                    *feed_scheme.get_or_insert(scheme),
                    scheme,
                    "{} {:?} block {mkt}: mixed port layouts within one feed",
                    f.venue,
                    f.kind
                );
            }
        }
    }

    /// ⚠️ **This test does NOT verify the deployment, and its previous name claimed it did.** It
    /// asserts the built-in document against literals written beside it — the code agreeing with
    /// itself. It catches a *later* edit that moves a value; it cannot catch a value that was wrong
    /// when it was written, because both sides come from the same transcription.
    ///
    /// That distinction is not theoretical. On 2026-08-09 **all three** rows in this registry were
    /// found provisioned on ports no publisher sends to — each authored one port block off, carrying a
    /// sibling row's ports — and this test passed throughout, green, under its old name and its old
    /// claim. Two of the three errors predated the branch entirely.
    ///
    /// A wrong value here activates nothing and says nothing: `doublezero status` reports no
    /// matching code, or the socket binds and stays silent, and the only symptom is a permanently
    /// zero `dz_receiver_up` — indistinguishable from a quiet publisher.
    ///
    /// **The external check is a packet capture**, and the procedure is recorded in the
    /// `PORT PROVENANCE` block in `registry.json`. Run it when a row is added or a port moves. A
    /// unit test cannot reach the wire, so this one is deliberately modest about what it pins:
    /// document → expansion consistency, nothing more.
    #[test]
    fn the_kalshi_rows_expand_consistently_with_the_document() {
        // Scoped by category as well as kind: the venue now carries two market-by-price rows on
        // disjoint universes, so `find` on the kind alone would silently pick whichever the
        // document happens to list first.
        let row = |kind| {
            feeds()
                .iter()
                .find(|f| f.venue == "KALSHI" && f.category == "perps" && f.kind == kind)
        };

        let tob = row(FeedKind::TopOfBook).expect("Lashay top-of-book row");
        assert_eq!(tob.code, "edge-kalshi-perps-tob");
        assert_eq!(tob.group, Ipv4Addr::new(233, 84, 178, 3));
        assert_eq!(tob.publishers.len(), 1);
        assert_eq!(tob.publishers[0].ports.mktdata(), 31000);
        assert_eq!(tob.publishers[0].ports.refdata(), 41000);
        assert_eq!(tob.publishers[0].ports.snapshot(), None);

        let mbp = row(FeedKind::MarketByPrice).expect("Lashay market-by-price row");
        assert_eq!(mbp.code, "edge-kalshi-perps-mbp");
        assert_eq!(mbp.group, Ipv4Addr::new(233, 84, 178, 4));
        assert_eq!(mbp.publishers.len(), 1);
        assert_eq!(mbp.publishers[0].ports.mktdata(), 32000);
        assert_eq!(mbp.publishers[0].ports.refdata(), 42000);
        assert_eq!(mbp.publishers[0].ports.snapshot(), Some(52000));

        // Both claim the tape (each group is gated on its own), and both race stickily.
        for f in [tob, mbp] {
            assert!(f.emit_trades, "{:?} must claim the tape", f.kind);
            assert_eq!(f.arbitration, ArbitrationMode::Sticky);
        }
    }

    /// Hyperliquid's publishers each serve both feeds, so the two rows must list the same number of
    /// blocks - a publisher onboarded to one feed and forgotten on the other is the failure this
    /// catches, and it shows up in production as a venue missing depth for one mirror.
    ///
    /// Deliberately a count and not a port relation. Today every MBO block happens to sit 1000
    /// above its TOB peer, but base ports are allocated per channel and that relation is not
    /// guaranteed (see the `FEEDS` docs); asserting it would fail the build for a legitimately
    /// arbitrary block and invite inventing a `10N01` row that isn't on the wire - the exact error
    /// this registry exists to avoid. Pairing a block to its peer needs operator identity, which
    /// the registry deliberately does not record.
    #[test]
    fn both_hyperliquid_feeds_list_every_publisher() {
        let count = |kind: FeedKind| -> usize {
            feeds()
                .iter()
                .find(|f| f.venue == "HYPERLIQUID" && f.kind == kind)
                .unwrap()
                .publishers
                .len()
        };
        assert_eq!(
            count(FeedKind::TopOfBook),
            count(FeedKind::MarketByOrder),
            "Hyperliquid TOB and MBO must list the same publishers"
        );
    }

    #[test]
    fn feed_kind_labels_are_stable() {
        assert_eq!(FeedKind::TopOfBook.label(), "tob");
        assert_eq!(FeedKind::Midpoint.label(), "midpoint");
        assert_eq!(FeedKind::MarketByOrder.label(), "mbo");
        assert_eq!(FeedKind::MarketByPrice.label(), "mbp");
    }
}
