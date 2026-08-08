//! Hardcoded registry of DoubleZero Edge feeds the bridge ingests.
//!
//! Each feed is one multicast group mapped to exactly one venue, plus the **protocol** it speaks
//! ([`FeedKind`]) and the **publishers** mirroring it ([`FeedPublisher`]), each with its own port
//! block. The bridge spawns one receiver per `(venue, kind, publisher)`; consumers then filter by
//! `venue` over the WebSocket (see PROTOCOL.md subscriptions). To ingest another venue's feed, add
//! a `Feed` row below - no other code changes are needed.

use std::net::Ipv4Addr;

/// Which edge-feed-spec protocol a feed speaks. Selects the frame magic + decoder + receiver
/// processor the bridge uses for it. See https://github.com/malbeclabs/edge-feed-spec.
// `Midpoint`/`MarketByOrder`/`MarketByPrice` are matched on by the receiver but only *constructed*
// by FEEDS rows, added once their live multicast endpoints are known - hence the dead_code allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum FeedKind {
    /// Top-of-Book & Trades (frame magic `0x445A`): best bid/ask quotes + trade prints.
    TopOfBook,
    /// Midpoint (frame magic `0x4D44`): a single derived mid price per instrument.
    Midpoint,
    /// Market-by-Order (frame magic `0x4444`): full L3 order book with snapshot+delta recovery.
    MarketByOrder,
    /// Market-by-Price (frame magic `0x4442`): the price-aggregated book with snapshot+delta
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
/// (the data stream the liveness watchdog tracks) and a `refdata` port (instrument defs +
/// manifest); Market-by-Order adds a dedicated `snapshot` port for its in-band book recovery.
/// A loopback demo that carries everything on one port is expressed as `mktdata == refdata`.
// `ThreePort` is constructed by Market-by-Order FEEDS rows (added with their endpoints).
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum FeedPorts {
    /// Top-of-Book and Midpoint: market data + reference data.
    TwoPort { mktdata: u16, refdata: u16 },
    /// Market-by-Order: market data (deltas/trades) + reference data + snapshot recovery stream.
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
/// Independent publishers mirror one venue's stream so subscribers can race them (see
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
/// is comparable between them. `Sticky` cannot: its arms carry no shared coordinate — no stable
/// entry id, no per-entry venue timestamp, and the transport's own send time is not the venue's —
/// and a content hash is no substitute, since a level oscillating 100 -> 0 -> 100 emits
/// byte-identical updates and collapsing those leaves a subscriber holding 0 at a price that has
/// liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitrationMode {
    /// Comparable venue clock: latch to the tick's leader, re-latch every tick.
    Coordinated,
    /// No comparable coordinate: elect one arm and hold it, transferring only on a health verdict,
    /// on silence, or on a sustained speed margin.
    Sticky,
}

#[derive(Debug, Clone, Copy)]
pub struct Feed {
    /// Venue name stamped on every instrument and message from this feed. Matches the
    /// edge-feed-spec source registry name (e.g. "Hyperliquid" for SourceID 1).
    pub venue: &'static str,
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
}

/// All feeds known to the bridge: DZ Edge feeds, one multicast group per venue, each mirrored by
/// one or more publishers ([`FeedPublisher`]).
///
/// Group, ports **and publisher count** all vary per venue. Hyperliquid mirrors one group across
/// eleven publishers, each with its own port block; Phoenix runs a single publisher. Don't assume
/// any of it - confirm against the venue's deployment.
///
/// **Base ports follow no arithmetic rule.** The v0.7 publisher takes an arbitrary `mkt_port` per
/// channel, so a block can sit anywhere (9011 is base+10, not base+N*100). The one guarantee is the
/// spacing *within* a block, which follows the publisher implementation: `+1`/`+2` for the
/// Hyperliquid role, `+10000`/`+20000` for the Lashay one. `publisher_blocks_use_a_known_layout`
/// pins both. Derive a block from its market-data port; never derive the market-data port from an
/// index.
///
/// Sibling-protocol feeds (Midpoint) are added here once their live multicast groups/ports are
/// known; until then they are absent rather than carrying guessed endpoints.
pub const FEEDS: &[Feed] = &[
    // Confirmed on-wire (group-bound capture) plus the publisher fleet's port blocks:
    //
    //   - `tiredsolid` 233.84.178.15 -> Hyperliquid, eleven publishers.
    //   - `scottsdale` 233.84.178.18 -> Phoenix, a single publisher on 9201/9202.
    //
    // The authoritative fleet list is the feed-capture recorder inventory in the private infra
    // repo, NOT the publisher deployment inventory - the latter covers only a subset of the hosts
    // on the group. Sourcing this table from the deployment inventory alone is what left five
    // blocks unbound and the bridge ingesting about a third of the group's datagrams.
    //
    // The venue is still resolved per message from the wire SourceID (see processor.rs), so the
    // `venue` below is only the default for unregistered SourceIDs (the SourceID-3 Hyperliquid
    // superset on tiredsolid). Each publisher gets its own receiver + reference-data state.
    Feed {
        venue: "Hyperliquid",
        code: "tiredsolid",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 15),
        publishers: &[
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9001,
                    refdata: 9002,
                },
            },
            // On the wire but in no inventory; its MBO peer is the group's highest-volume
            // depth publisher. Owner still to be established - do not drop the row.
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9011,
                    refdata: 9012,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9101,
                    refdata: 9102,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9201,
                    refdata: 9202,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9301,
                    refdata: 9302,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9401,
                    refdata: 9402,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9501,
                    refdata: 9502,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9601,
                    refdata: 9602,
                },
            },
            // Registry-only: silent across every capture taken so far.
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9701,
                    refdata: 9702,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9801,
                    refdata: 9802,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9901,
                    refdata: 9902,
                },
            },
        ],
        emit_trades: true,
        arbitration: ArbitrationMode::Coordinated,
    },
    // Hyperliquid Market-by-Order on the same `tiredsolid` group, one port block per publisher
    // (paired with the TOB row above). Depth-only: TOB owns this venue's trades.
    //
    // Unconfirmed ports and how each one fails, since the failure is NOT uniform: `dz_receiver_up`
    // is driven by the idle watchdog, which tracks the **mktdata** port only.
    //   - wrong mktdata  -> no datagrams -> `dz_receiver_up == 0`. Visible.
    //   - wrong refdata  -> `RefDataState` never resolves a definition and every processor gates
    //                       emission on one, so the receiver reads healthy and emits nothing.
    //   - wrong snapshot -> books stay `Recovering` forever, so the publisher contributes no depth
    //                       while still reading healthy.
    // The only signal for the latter two is `dz_datagrams_received_total{role}` pinned at 0.
    // Silently-failing ports here: 10903 (derived from an observed 10902) and the whole 10701
    // block (registry-only, never seen on the wire). Everything else was decoded from a capture.
    Feed {
        venue: "Hyperliquid",
        code: "tiredsolid",
        kind: FeedKind::MarketByOrder,
        group: Ipv4Addr::new(233, 84, 178, 15),
        publishers: &[
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10001,
                    refdata: 10002,
                    snapshot: 10003,
                },
            },
            // Wire-confirmed. Peer of TOB 9011 (owner still to be established).
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10011,
                    refdata: 10012,
                    snapshot: 10013,
                },
            },
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10101,
                    refdata: 10102,
                    snapshot: 10103,
                },
            },
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10201,
                    refdata: 10202,
                    snapshot: 10203,
                },
            },
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10301,
                    refdata: 10302,
                    snapshot: 10303,
                },
            },
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10401,
                    refdata: 10402,
                    snapshot: 10403,
                },
            },
            // Wire-confirmed.
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10501,
                    refdata: 10502,
                    snapshot: 10503,
                },
            },
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10601,
                    refdata: 10602,
                    snapshot: 10603,
                },
            },
            // Registry-only: not yet seen on the wire.
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10701,
                    refdata: 10702,
                    snapshot: 10703,
                },
            },
            // Wire-confirmed.
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10801,
                    refdata: 10802,
                    snapshot: 10803,
                },
            },
            // Only the refdata port was seen; mktdata/snapshot derived from the block layout.
            FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 10901,
                    refdata: 10902,
                    snapshot: 10903,
                },
            },
        ],
        emit_trades: false,
        arbitration: ArbitrationMode::Coordinated,
    },
    Feed {
        venue: "Phoenix",
        code: "scottsdale",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 18),
        publishers: &[FeedPublisher {
            ports: FeedPorts::TwoPort {
                mktdata: 9201,
                refdata: 9202,
            },
        }],
        emit_trades: true,
        arbitration: ArbitrationMode::Coordinated,
    },
    // Lashay perps, two separately-gated groups: `lashay-1` carries top of book and `lashay-2` the
    // market-by-price book. Both claim the tape — a host holding only `lashay-2` must still serve
    // `trade` — and which of them prints is the reconciler's runtime decision (see `Feed::emit_trades`).
    //
    // ⚠️ These `code` values are the *intended* names. The live deployment still carries the old ones
    // until the group rename lands, so until then `doublezero status --json` reports no match, the
    // reconciler never activates these rows, and the failure mode is silent: no warning, no failed
    // bind, just a receiver that never starts.
    //
    // One `FeedPublisher` per row: the two arms share a port block and are distinguished only by
    // datagram source IP (the shared-block model in `FeedPublisher`'s docs). The market-by-price
    // block is spaced `+10000` / `+20000`, not the `+1` / `+2` the Hyperliquid publisher uses.
    Feed {
        venue: "Lashay",
        code: "lashay-1",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 3),
        publishers: &[FeedPublisher {
            ports: FeedPorts::TwoPort {
                mktdata: 7576,
                refdata: 7577,
            },
        }],
        emit_trades: true,
        arbitration: ArbitrationMode::Sticky,
    },
    Feed {
        venue: "Lashay",
        code: "lashay-2",
        kind: FeedKind::MarketByPrice,
        group: Ipv4Addr::new(233, 84, 178, 4),
        publishers: &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 31000,
                refdata: 41000,
                snapshot: 51000,
            },
        }],
        emit_trades: true,
        arbitration: ArbitrationMode::Sticky,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_kind_pairs_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for f in FEEDS {
            assert!(
                seen.insert((f.venue, f.kind)),
                "duplicate (venue, kind): {} {:?}",
                f.venue,
                f.kind
            );
        }
    }

    #[test]
    fn every_feed_has_a_group_code() {
        // The reconciler matches `code` against `doublezero status` subscriptions, so every row
        // must carry one. Both Hyperliquid rows share the group `tiredsolid`; Phoenix is `scottsdale`.
        for f in FEEDS {
            assert!(!f.code.is_empty(), "{} {:?} has no code", f.venue, f.kind);
        }
        // Keyed on `(venue, kind)` and not on venue alone: Lashay's two rows ride *different*
        // groups, which is what makes tape ownership a runtime decision in the first place.
        for f in FEEDS {
            let expected = match (f.venue, f.kind) {
                ("Hyperliquid", FeedKind::TopOfBook | FeedKind::MarketByOrder) => "tiredsolid",
                ("Phoenix", FeedKind::TopOfBook) => "scottsdale",
                ("Lashay", FeedKind::TopOfBook) => "lashay-1",
                ("Lashay", FeedKind::MarketByPrice) => "lashay-2",
                other => panic!("unexpected feed {other:?}"),
            };
            assert_eq!(f.code, expected, "{} {:?} has wrong code", f.venue, f.kind);
        }
    }

    #[test]
    fn hyperliquid_has_tob_and_mbo() {
        let kinds: std::collections::HashSet<FeedKind> = FEEDS
            .iter()
            .filter(|f| f.venue.eq_ignore_ascii_case("hyperliquid"))
            .map(|f| f.kind)
            .collect();
        assert!(kinds.contains(&FeedKind::TopOfBook));
        assert!(kinds.contains(&FeedKind::MarketByOrder));
    }

    #[test]
    fn hyperliquid_tob_emits_trades() {
        let hl = FEEDS
            .iter()
            .find(|f| f.venue == "Hyperliquid" && f.kind == FeedKind::TopOfBook)
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
    /// `tape_owners` (one row per venue) and the arbiter's per-venue tape leader (one arm within it),
    /// with `dz_tape_owner_changes_total` / `dz_tape_arm_transfers_total` reporting the moves.
    #[test]
    fn emit_trades_agrees_with_the_tape_ownership_rule() {
        for f in FEEDS {
            assert_eq!(
                f.emit_trades,
                crate::ingest::reconcile::tape_rank_is_some(f.kind),
                "{} {:?}: emit_trades disagrees with the tape ranking",
                f.venue,
                f.kind
            );
        }
    }

    /// A venue's arms are the same hosts whatever protocol they speak, so every row for a venue
    /// must declare the same arbitration mode. Disagreement would make the arbiter's per-venue mode
    /// depend on which row registered last.
    #[test]
    fn arbitration_mode_agrees_across_a_venues_rows() {
        let mut modes = std::collections::HashMap::new();
        for f in FEEDS {
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
    /// asserting over all of `FEEDS`, because `Sticky` exists precisely so a venue whose arms carry
    /// no shared clock can declare it; a new such venue is the feature working, not a regression.
    #[test]
    fn existing_venues_are_coordinated() {
        for f in FEEDS.iter().filter(|f| f.venue != "Lashay") {
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
    /// contribute no data.
    #[test]
    fn every_feed_has_at_least_one_publisher() {
        for f in FEEDS {
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
        for f in FEEDS {
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
        for f in FEEDS {
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
            let f = FEEDS
                .iter()
                .find(|f| f.venue == "Hyperliquid" && f.kind == kind)
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
    /// Hyperliquid and Phoenix block and on `lashay-1`, `+10000`/`+20000` on `lashay-2` (and on the
    /// sports market-by-price feed: 33010/43010/53010). The base port is free-form since v0.7, so this
    /// spacing is the only structural rule left — it is what an unseen block may be derived from
    /// (10901/10903 were derived this way from 10902), and a row that breaks *both* schemes is a
    /// transcription error rather than a new layout.
    ///
    /// Scoped **per row**, not per venue: Lashay's two rows legitimately use different schemes, and
    /// framing it by scheme rather than by a venue carve-out is what keeps `lashay-3`/`lashay-4` from
    /// re-tripping it. Lashay's exact blocks are pinned by `lashay_rows_match_the_deployment`, so
    /// widening this one loses nothing.
    #[test]
    fn publisher_blocks_use_a_known_layout() {
        const SCHEMES: [u16; 2] = [1, 10_000];
        for f in FEEDS {
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

    /// The Lashay rows are **inert until the upstream group rename lands**: `doublezero status`
    /// reports no matching code, so the reconciler never activates them and the only symptom of a
    /// wrong value here would be a permanently-zero `dz_receiver_up`. Pin the deployment exactly so
    /// a transcription slip fails the build instead.
    #[test]
    fn lashay_rows_match_the_deployment() {
        let row = |kind| FEEDS.iter().find(|f| f.venue == "Lashay" && f.kind == kind);

        let tob = row(FeedKind::TopOfBook).expect("Lashay top-of-book row");
        assert_eq!(tob.code, "lashay-1");
        assert_eq!(tob.group, Ipv4Addr::new(233, 84, 178, 3));
        assert_eq!(tob.publishers.len(), 1);
        assert_eq!(tob.publishers[0].ports.mktdata(), 7576);
        assert_eq!(tob.publishers[0].ports.refdata(), 7577);
        assert_eq!(tob.publishers[0].ports.snapshot(), None);

        let mbp = row(FeedKind::MarketByPrice).expect("Lashay market-by-price row");
        assert_eq!(mbp.code, "lashay-2");
        assert_eq!(mbp.group, Ipv4Addr::new(233, 84, 178, 4));
        assert_eq!(mbp.publishers.len(), 1);
        assert_eq!(mbp.publishers[0].ports.mktdata(), 31000);
        assert_eq!(mbp.publishers[0].ports.refdata(), 41000);
        assert_eq!(mbp.publishers[0].ports.snapshot(), Some(51000));

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
            FEEDS
                .iter()
                .find(|f| f.venue == "Hyperliquid" && f.kind == kind)
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
