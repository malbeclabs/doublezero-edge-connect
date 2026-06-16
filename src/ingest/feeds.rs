//! Hardcoded registry of DoubleZero Edge feeds the bridge ingests.
//!
//! Each feed is one multicast group mapped to exactly one venue, plus the **protocol** it speaks
//! ([`FeedKind`]) and the **ports** it splits its messages across ([`FeedPorts`]). The bridge
//! spawns one receiver per selected feed; consumers then filter by `venue` over the WebSocket
//! (see PROTOCOL.md subscriptions). To ingest another venue's feed, add a `Feed` row below - no
//! other code changes are needed.

use std::net::Ipv4Addr;

/// Which edge-feed-spec protocol a feed speaks. Selects the frame magic + decoder + receiver
/// processor the bridge uses for it. See https://github.com/malbeclabs/edge-feed-spec.
// `Midpoint`/`MarketByOrder` are matched on by the receiver but only *constructed* by FEEDS rows,
// which are added once their live multicast endpoints are known - hence the dead_code allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum FeedKind {
    /// Top-of-Book & Trades (frame magic `0x445A`): best bid/ask quotes + trade prints.
    TopOfBook,
    /// Midpoint (frame magic `0x4D44`): a single derived mid price per instrument.
    Midpoint,
    /// Market-by-Order (frame magic `0x4444`): full L3 order book with snapshot+delta recovery.
    MarketByOrder,
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

#[derive(Debug, Clone, Copy)]
pub struct Feed {
    /// Venue name stamped on every instrument and message from this feed. Matches the
    /// edge-feed-spec source registry name (e.g. "Hyperliquid" for SourceID 1).
    pub venue: &'static str,
    /// Which edge-feed-spec protocol this feed speaks (selects decoder + processor).
    pub kind: FeedKind,
    /// Multicast group for the feed.
    pub group: Ipv4Addr,
    /// The ports the feed splits its messages across.
    pub ports: FeedPorts,
}

/// All feeds known to the bridge: DZ Edge feeds, one multicast group per venue. Both the group
/// *and* the ports vary per venue: Hyperliquid publishes on the 93xx family, Phoenix on 92xx.
/// Don't assume ports are shared - confirm them against the live publisher.
///
/// Sibling-protocol feeds (Midpoint / Market-by-Order) are added here once their live multicast
/// groups/ports are known; until then they are absent rather than carrying guessed endpoints.
pub const FEEDS: &[Feed] = &[
    // Confirmed on-wire (group-bound capture). The two subscribed DZ multicast groups each carry
    // one venue's Top-of-Book on mktdata 9201 / refdata 9202:
    //
    //   - `tiredsolid` 233.84.178.15 -> Hyperliquid. SourceID 3 (the Hyperliquid superset: core
    //     perps + HIP-3 builder DEXs, builder kept in the symbol) plus SourceID 1.
    //   - `scottsdale` 233.84.178.18 -> Phoenix. SourceID 2.
    //
    // The venue is still resolved per message from the wire SourceID (see processor.rs), so the
    // `venue` below is only the default for unregistered SourceIDs (the SourceID-3 superset on
    // tiredsolid). Each feed gets its own receiver + reference-data state, keyed by group address.
    Feed {
        venue: "Hyperliquid",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 15),
        ports: FeedPorts::TwoPort {
            mktdata: 9201,
            refdata: 9202,
        },
    },
    Feed {
        venue: "Phoenix",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(233, 84, 178, 18),
        ports: FeedPorts::TwoPort {
            mktdata: 9201,
            refdata: 9202,
        },
    },
];

/// Resolve a comma/space-free venue name (case-insensitive) to its feed.
pub fn by_venue(venue: &str) -> Option<&'static Feed> {
    FEEDS.iter().find(|f| f.venue.eq_ignore_ascii_case(venue))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venues_are_unique_and_resolvable() {
        let mut seen = std::collections::HashSet::new();
        for f in FEEDS {
            assert!(seen.insert(f.venue), "duplicate venue {}", f.venue);
            assert!(by_venue(f.venue).is_some());
        }
        assert!(by_venue("hyperLIQUID").is_some()); // case-insensitive
        assert!(by_venue("nope").is_none());
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
}
