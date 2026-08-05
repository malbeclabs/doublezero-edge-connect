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
// `Midpoint`/`MarketByOrder` are matched on by the receiver but only *constructed* by FEEDS rows,
// which are added once their live multicast endpoints are known - hence the dead_code allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum FeedKind {
    /// Top-of-Book & Trades (frame magic `0x445A`): best bid/ask quotes + trade prints.
    TopOfBook,
    /// Midpoint (frame magic `0x4D44`): a single derived mid price per instrument.
    Midpoint,
    /// Market-by-Order (frame magic `0x4444`): full L3 order book with snapshot+delta recovery.
    MarketByOrder,
}

impl FeedKind {
    /// Stable, low-cardinality label for the metrics `kind` dimension and log fields.
    pub fn label(self) -> &'static str {
        match self {
            FeedKind::TopOfBook => "tob",
            FeedKind::Midpoint => "midpoint",
            FeedKind::MarketByOrder => "mbo",
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

/// One publisher mirroring a feed: the port block it publishes on, plus a stable name.
///
/// Independent publishers mirror one venue's stream so subscribers can race them (see
/// `ingest::arbiter`). Two deployment models exist and both are supported:
///
/// - **Distinct port blocks per publisher** (what the live Hyperliquid fleet does — host index N
///   publishes on base + N*100): one `FeedPublisher` row per publisher, one receiver task each,
///   and each task sees exactly one source IP.
/// - **Shared port block** (all publishers to one `(group, port)`): a single `FeedPublisher` row,
///   one receiver task, and that task sees N source IPs.
///
/// Either way the *publisher identity* the arbiter races on is the datagram source IP, never the
/// port — so the dedup path is identical. `name` exists only for metric labels, log fields and the
/// reconciler's task key; it is operator-facing configuration, never read from the wire.
#[derive(Debug, Clone, Copy)]
pub struct FeedPublisher {
    /// Stable, low-cardinality name for the `publisher` metric label and log fields. Unique within
    /// a feed. Mirrors the ansible host that publishes the block, shortened (`aws-tyo-2` ==
    /// `aws-tyo-hl-mainnet2`) so an operator can correlate a metric back to a host.
    pub name: &'static str,
    /// The port block this publisher sends on.
    pub ports: FeedPorts,
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
    /// Whether this feed emits `trade` messages. A venue carried by both TOB and MBO would
    /// otherwise double-emit the same trades; TOB owns trades, MBO is depth-only.
    pub emit_trades: bool,
}

/// All feeds known to the bridge: DZ Edge feeds, one multicast group per venue, each mirrored by
/// one or more publishers ([`FeedPublisher`]).
///
/// Group, ports **and publisher count** all vary per venue. Hyperliquid runs a six-host fleet on
/// one group with a distinct port block per host (base + host_index*100); Phoenix runs a single
/// publisher. Don't assume any of it - confirm against the venue's ansible inventory.
///
/// Sibling-protocol feeds (Midpoint) are added here once their live multicast groups/ports are
/// known; until then they are absent rather than carrying guessed endpoints.
pub const FEEDS: &[Feed] = &[
    // Confirmed on-wire (group-bound capture) plus the publisher fleet from
    // hyperliquid/infra/ansible/inventory/{hosts.ini,host_vars/*/main.yml}:
    //
    //   - `tiredsolid` 233.84.178.15 -> Hyperliquid, six publishers, TOB on 9x01/9x02 and MBO on
    //     10x01/10x02/10x03 where x is the host index (mainnet4=0, mainnet1=1, mainnet2=2,
    //     mainnet3=3, lat=4, gcp=6).
    //   - `scottsdale` 233.84.178.18 -> Phoenix, a single publisher on 9201/9202.
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
                name: "aws-tyo-4",
                ports: FeedPorts::TwoPort {
                    mktdata: 9001,
                    refdata: 9002,
                },
            },
            FeedPublisher {
                name: "aws-tyo-1",
                ports: FeedPorts::TwoPort {
                    mktdata: 9101,
                    refdata: 9102,
                },
            },
            FeedPublisher {
                name: "aws-tyo-2",
                ports: FeedPorts::TwoPort {
                    mktdata: 9201,
                    refdata: 9202,
                },
            },
            FeedPublisher {
                name: "aws-tyo-3",
                ports: FeedPorts::TwoPort {
                    mktdata: 9301,
                    refdata: 9302,
                },
            },
            FeedPublisher {
                name: "lat-tyo-1",
                ports: FeedPorts::TwoPort {
                    mktdata: 9401,
                    refdata: 9402,
                },
            },
            FeedPublisher {
                name: "gcp-tyo-1",
                ports: FeedPorts::TwoPort {
                    mktdata: 9601,
                    refdata: 9602,
                },
            },
        ],
        emit_trades: true,
    },
    // Hyperliquid Market-by-Order on the same `tiredsolid` group, one port block per publisher
    // (paired with the TOB row above). Depth-only: TOB owns this venue's trades.
    Feed {
        venue: "Hyperliquid",
        code: "tiredsolid",
        kind: FeedKind::MarketByOrder,
        group: Ipv4Addr::new(233, 84, 178, 15),
        publishers: &[
            FeedPublisher {
                name: "aws-tyo-4",
                ports: FeedPorts::ThreePort {
                    mktdata: 10001,
                    refdata: 10002,
                    snapshot: 10003,
                },
            },
            FeedPublisher {
                name: "aws-tyo-1",
                ports: FeedPorts::ThreePort {
                    mktdata: 10101,
                    refdata: 10102,
                    snapshot: 10103,
                },
            },
            FeedPublisher {
                name: "aws-tyo-2",
                ports: FeedPorts::ThreePort {
                    mktdata: 10201,
                    refdata: 10202,
                    snapshot: 10203,
                },
            },
            FeedPublisher {
                name: "aws-tyo-3",
                ports: FeedPorts::ThreePort {
                    mktdata: 10301,
                    refdata: 10302,
                    snapshot: 10303,
                },
            },
            FeedPublisher {
                name: "lat-tyo-1",
                ports: FeedPorts::ThreePort {
                    mktdata: 10401,
                    refdata: 10402,
                    snapshot: 10403,
                },
            },
            FeedPublisher {
                name: "gcp-tyo-1",
                ports: FeedPorts::ThreePort {
                    mktdata: 10601,
                    refdata: 10602,
                    snapshot: 10603,
                },
            },
        ],
        emit_trades: false,
    },
    Feed {
        venue: "Phoenix",
        code: "scottsdale",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 18),
        publishers: &[FeedPublisher {
            name: "lon-1",
            ports: FeedPorts::TwoPort {
                mktdata: 9201,
                refdata: 9202,
            },
        }],
        emit_trades: true,
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
        for f in FEEDS {
            let expected = match f.venue {
                "Hyperliquid" => "tiredsolid",
                "Phoenix" => "scottsdale",
                other => panic!("unexpected venue {other}"),
            };
            assert_eq!(f.code, expected, "{} has wrong code", f.venue);
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

    /// Publisher names are the `publisher` metric label and the reconciler's task-key component,
    /// so they must be unique within a feed (a duplicate would collapse two receivers into one
    /// task key and merge their metrics).
    #[test]
    fn publisher_names_unique_within_a_feed() {
        for f in FEEDS {
            let mut seen = std::collections::HashSet::new();
            for p in f.publishers {
                assert!(
                    seen.insert(p.name),
                    "{} {:?} has duplicate publisher name {}",
                    f.venue,
                    f.kind,
                    p.name
                );
                assert!(
                    !p.name.is_empty(),
                    "{} {:?} has an empty publisher name",
                    f.venue,
                    f.kind
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
                        p.name,
                        f.group,
                        port
                    );
                }
            }
        }
    }

    /// The Hyperliquid fleet mirrors one venue across six hosts on the +100-per-host port scheme.
    /// Pins the count so a dropped row is caught, and pins one known block end-to-end.
    #[test]
    fn hyperliquid_lists_the_whole_publisher_fleet() {
        for kind in [FeedKind::TopOfBook, FeedKind::MarketByOrder] {
            let f = FEEDS
                .iter()
                .find(|f| f.venue == "Hyperliquid" && f.kind == kind)
                .unwrap();
            assert_eq!(f.publishers.len(), 6, "{kind:?} publisher count");
        }
        let tob = FEEDS
            .iter()
            .find(|f| f.venue == "Hyperliquid" && f.kind == FeedKind::TopOfBook)
            .unwrap();
        let p = tob
            .publishers
            .iter()
            .find(|p| p.name == "aws-tyo-2")
            .unwrap();
        assert_eq!(p.ports.mktdata(), 9201);
        assert_eq!(p.ports.refdata(), 9202);
    }

    #[test]
    fn feed_kind_labels_are_stable() {
        assert_eq!(FeedKind::TopOfBook.label(), "tob");
        assert_eq!(FeedKind::Midpoint.label(), "midpoint");
        assert_eq!(FeedKind::MarketByOrder.label(), "mbo");
    }
}
