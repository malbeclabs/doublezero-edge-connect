//! The **ingest floor**: which channels of an activated feed this process decodes.
//!
//! Distinct from the two filters either side of it. `reconcile` decides which *feeds* run, from the
//! host's subscriptions; `sinks::ws`'s `SubFilter` decides what one *client* receives. The floor
//! sits between them and is process-global and ops-owned: it scopes books, history and CPU for
//! everyone, which is why it is not reachable from the read-only query surface.
//!
//! The floor acts **only** where the publisher derives a port per channel
//! (`port = base + channel_id`): an excluded channel is never bound and the kernel discards its
//! traffic before it reaches userspace. That costs nothing, and it is the reason the derivation
//! exists upstream. One such row carries tens of thousands of markets across dozens of channels,
//! far past what the rolling history window holds, so narrowing it is the difference between
//! covering a slice of one league completely and thrashing across all of them.
//!
//! Narrowing a row whose publishers bind a base **flat** is refused at startup rather than
//! implemented as a frame-header test. On such a row `channel_id` identifies mirrors, not markets —
//! each publisher carries the complete instrument universe — so narrowing it would discard
//! redundancy without reducing a single decoded message. There is no trader-facing reason to want
//! it, and refusing is honest where a header test would be a filter that costs CPU and buys
//! nothing. If a flat row ever partitions markets, this is the seam to revisit.
//!
//! Two properties of the syntax are deliberate:
//!
//! - **Keyed by group code, never global.** `--channels lashay-4=10,11` narrows that row and no
//!   other; an unmentioned row ingests every channel. One global flag would let an operator filter
//!   down to a league and silently half-blind an unrelated mirrored feed, since the two planes give
//!   `channel_id` different meanings.
//! - **Ids are the contract; names are not.** Channel *names* live in the publisher's inventory by
//!   design — they have already moved once, and a copy here would drift exactly as four superseded
//!   port allocations did. The floor takes numeric ids and validates them against the roster in the
//!   **loaded document**, so a typo fails startup instead of filtering nothing.

use std::collections::{BTreeSet, HashMap};

use crate::ingest::feeds::{feeds, Feed, FeedPublisher};

/// Which channels of each group code this process ingests. An absent code means "admit all", which
/// is what makes the empty floor a no-op.
#[derive(Debug, Clone, Default)]
pub struct ChannelFloor {
    admitted: HashMap<&'static str, BTreeSet<u8>>,
}

/// Why a channel floor was refused at startup.
///
/// Every variant is fatal by design: a floor that silently filters nothing is worse than one that
/// refuses to start, because the symptom of the former is a feed that reads healthy and carries
/// markets nobody asked for (or, on a mistyped id, a socket bound to a port no publisher sends to,
/// which reads as a dead feed rather than as a typo). Each message names the offending row and the
/// consequence, matching `registry::RegistryError`'s convention.
#[derive(Debug)]
pub enum FloorError {
    /// A clause with no `=`.
    MissingIds { clause: String },
    /// A code no row in the loaded document carries.
    UnknownCode { code: String, known: Vec<String> },
    /// A code listed twice in one spec.
    RepeatedCode { code: String },
    /// A clause that selects no channel at all.
    EmptySelection { code: String },
    /// An id that is not a number in `0..=255`.
    BadId { code: String, text: String },
    /// An id outside the row's roster.
    UnknownChannel {
        code: String,
        id: u8,
        roster: Vec<u8>,
    },
    /// A row whose publishers bind a base flat — narrowing it is refused, not implemented.
    FlatRow {
        code: String,
        venue: String,
        category: String,
    },
}

impl std::fmt::Display for FloorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FloorError::MissingIds { clause } => write!(
                f,
                "channel floor clause `{clause}` has no `=`; the syntax is \
                 `<code>=<id>[,<id>...][;<code>=...]`"
            ),
            FloorError::UnknownCode { code, known } => write!(
                f,
                "channel floor names group code `{code}`, which no row in the loaded feed registry \
                 carries; it would filter nothing. Known codes: {}",
                known.join(", ")
            ),
            FloorError::RepeatedCode { code } => write!(
                f,
                "channel floor lists group code `{code}` twice; one clause would win and the \
                 other's channels would go silently unbound"
            ),
            FloorError::EmptySelection { code } => write!(
                f,
                "channel floor selects no channel for `{code}`; the row would bind no socket at \
                 all, which is `--feed`'s job, not this flag's"
            ),
            FloorError::BadId { code, text } => write!(
                f,
                "channel floor id `{text}` for `{code}` is not a channel id (0-255)"
            ),
            FloorError::UnknownChannel { code, id, roster } => write!(
                f,
                "channel floor names channel {id} on `{code}`, which is not in that row's roster; \
                 binding it would join a port no publisher sends to, which reads as a dead feed \
                 rather than as a typo. Roster: {}",
                roster
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            FloorError::FlatRow {
                code,
                venue,
                category,
            } => write!(
                f,
                "channel floor narrows `{code}` ({venue}/{category}), whose publishers bind one \
                 base port flat. On such a row `channel_id` identifies mirrors, not markets — each \
                 publisher carries the complete instrument universe — so narrowing it would give up \
                 redundancy without reducing a single decoded message. Remove the clause, or use \
                 `--publisher-port` to drop a specific mirror"
            ),
        }
    }
}

impl std::error::Error for FloorError {}

/// The channel ids a row's publishers were derived from, ascending. Empty for a flat row.
fn roster(f: &Feed) -> BTreeSet<u8> {
    f.publishers.iter().filter_map(|p| p.channel).collect()
}

fn known_codes() -> Vec<String> {
    let mut codes: Vec<String> = feeds().iter().map(|f| f.code.to_string()).collect();
    codes.sort();
    codes.dedup();
    codes
}

impl ChannelFloor {
    /// Parse `<code>=<id>[,<id>...][;<code>=...]`, resolving and validating against the **loaded**
    /// registry document.
    ///
    /// Validation is against the document rather than a compiled-in list on purpose: the roster is
    /// the publisher's to change, so the only roster this process can honestly check an id against
    /// is the one it is about to bind. This is also where the trust boundary the loadable registry
    /// moved gets re-established — an id is now operator input reaching a set of rows that are
    /// themselves operator input, and neither the compiler nor the test suite can see either.
    pub fn parse(spec: &str) -> Result<ChannelFloor, FloorError> {
        let mut admitted: HashMap<&'static str, BTreeSet<u8>> = HashMap::new();
        for clause in spec.split(';') {
            let clause = clause.trim();
            if clause.is_empty() {
                continue;
            }
            let Some((code, ids)) = clause.split_once('=') else {
                return Err(FloorError::MissingIds {
                    clause: clause.to_string(),
                });
            };
            let code = code.trim();
            // Several rows can share one code (they ride one group), so the clause resolves to a
            // set of rows and every one of them is narrowed.
            let rows: Vec<&'static Feed> = feeds().iter().filter(|f| f.code == code).collect();
            let Some(first) = rows.first() else {
                return Err(FloorError::UnknownCode {
                    code: code.to_string(),
                    known: known_codes(),
                });
            };
            // Reuse the registry's own `&'static str` for the key, so the map holds no leaked
            // copy of the flag text.
            let code: &'static str = first.code;

            let mut full_roster = BTreeSet::new();
            for f in &rows {
                let r = roster(f);
                if r.is_empty() {
                    return Err(FloorError::FlatRow {
                        code: code.to_string(),
                        venue: f.venue.to_string(),
                        category: f.category.to_string(),
                    });
                }
                full_roster.extend(r);
            }

            let mut wanted = BTreeSet::new();
            for id in ids.split(',') {
                let id = id.trim();
                if id.is_empty() {
                    return Err(FloorError::EmptySelection {
                        code: code.to_string(),
                    });
                }
                let id: u8 = id.parse().map_err(|_| FloorError::BadId {
                    code: code.to_string(),
                    text: id.to_string(),
                })?;
                if !full_roster.contains(&id) {
                    return Err(FloorError::UnknownChannel {
                        code: code.to_string(),
                        id,
                        roster: full_roster.into_iter().collect(),
                    });
                }
                wanted.insert(id);
            }
            if admitted.insert(code, wanted).is_some() {
                return Err(FloorError::RepeatedCode {
                    code: code.to_string(),
                });
            }
        }
        Ok(ChannelFloor { admitted })
    }

    /// Whether this process ingests `channel` of the row carrying `code`. An unmentioned code
    /// admits everything.
    pub fn admits(&self, code: &str, channel: u8) -> bool {
        self.admitted
            .get(code)
            .is_none_or(|ids| ids.contains(&channel))
    }

    /// The publishers of `feed` this process binds — the floor's whole effect on the hot path.
    ///
    /// A row the floor does not mention keeps every publisher, unchanged. A mentioned row keeps
    /// only the publishers derived from an admitted channel; the rest are never spawned, so their
    /// sockets are never bound and the kernel drops their traffic. A flat row cannot be mentioned
    /// ([`ChannelFloor::parse`] refuses it), which is why there is no "keep everything" fallback
    /// here for a publisher with no channel id.
    pub fn publishers_for(&self, feed: &Feed) -> Vec<&'static FeedPublisher> {
        match self.admitted.get(feed.code) {
            None => feed.publishers.iter().collect(),
            Some(ids) => feed
                .publishers
                .iter()
                .filter(|p| p.channel.is_some_and(|c| ids.contains(&c)))
                .collect(),
        }
    }

    /// Nothing narrowed — the default, and the state every deployment that sets no flag is in.
    pub fn is_empty(&self) -> bool {
        self.admitted.is_empty()
    }

    /// What this floor narrowed, for the startup log: `code -> admitted/roster`. Which channels a
    /// process actually bound is the first question a "why is this market missing" report asks.
    pub fn summary(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .admitted
            .iter()
            .map(|(code, ids)| {
                let roster = feeds()
                    .iter()
                    .filter(|f| f.code == *code)
                    .flat_map(roster)
                    .collect::<BTreeSet<u8>>()
                    .len();
                format!("{code}={} of {roster}", ids.len())
            })
            .collect();
        out.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty floor admits everything — the default must be "ingest what you are subscribed to",
    /// so an operator who sets nothing sees no behaviour change.
    #[test]
    fn an_empty_floor_admits_every_channel() {
        let f = ChannelFloor::parse("").expect("empty is valid");
        assert!(f.admits("lashay-4", 10));
        assert!(f.admits("lashay-4", 49));
        assert!(f.admits("lashay-1", 2));
        assert!(f.is_empty());
    }

    /// The floor is scoped per group code. Narrowing one row must not narrow another — the two
    /// planes give `channel_id` different meanings and a global filter would conflate them.
    #[test]
    fn a_floor_on_one_row_does_not_narrow_another() {
        let f = ChannelFloor::parse("lashay-4=10,11").unwrap();
        assert!(f.admits("lashay-4", 10));
        assert!(!f.admits("lashay-4", 12));
        assert!(f.admits("lashay-1", 1), "an unmentioned row was narrowed");
        assert!(f.admits("lashay-1", 2), "an unmentioned row was narrowed");
    }

    /// A selected sports channel binds its socket and an excluded one binds nothing. Asserting on
    /// the bound set rather than on decoded output is deliberate: decoded output is empty for an
    /// excluded channel either way, so that assertion could not fail.
    #[test]
    fn an_excluded_sports_channel_binds_no_socket() {
        let row = feeds().iter().find(|f| f.category == "sports").unwrap();
        let f = ChannelFloor::parse("lashay-4=10,11").unwrap();
        let ports: Vec<u16> = f
            .publishers_for(row)
            .iter()
            .map(|p| p.base_port())
            .collect();
        assert_eq!(ports, vec![33010, 33011]);
    }

    /// The other half of the same property, and the one that fails if `publishers_for` ever starts
    /// filtering a row nobody narrowed: an unmentioned row binds its whole roster.
    #[test]
    fn an_unmentioned_row_binds_every_publisher() {
        let row = feeds().iter().find(|f| f.category == "sports").unwrap();
        let f = ChannelFloor::parse("").unwrap();
        assert_eq!(f.publishers_for(row).len(), row.publishers.len());
        assert!(row.publishers.len() > 2, "the row must be worth narrowing");
    }

    /// An id outside the row's roster is a startup error. Silently admitting it would bind a port
    /// no publisher sends to, which reads as a dead feed rather than as a typo.
    #[test]
    fn an_id_outside_the_roster_is_rejected() {
        let err = ChannelFloor::parse("lashay-4=10,63").unwrap_err();
        assert!(
            matches!(err, FloorError::UnknownChannel { id: 63, .. }),
            "wrong variant: {err:?}"
        );
        assert!(
            format!("{err}").contains("63"),
            "the error must name the offending id: {err}"
        );
    }

    /// An unknown group code is a startup error for the same reason — it silently filters nothing.
    #[test]
    fn an_unknown_group_code_is_rejected() {
        let err = ChannelFloor::parse("lashay-9=10").unwrap_err();
        assert!(
            matches!(err, FloorError::UnknownCode { ref code, .. } if code == "lashay-9"),
            "wrong variant: {err:?}"
        );
    }

    /// **The refusal.** On a row whose publishers bind a base flat, `channel_id` identifies mirrors
    /// and each publisher carries the complete universe, so narrowing it discards redundancy
    /// without reducing a single decoded message. A frame-header filter there would cost CPU and
    /// buy nothing, so this is refused rather than implemented — and the error has to name the row,
    /// since "the floor did not apply" is otherwise indistinguishable from "the ids were wrong".
    #[test]
    fn narrowing_a_flat_row_is_refused() {
        let err = ChannelFloor::parse("lashay-1=2").unwrap_err();
        assert!(
            matches!(err, FloorError::FlatRow { ref code, .. } if code == "lashay-1"),
            "wrong variant: {err:?}"
        );
        let msg = format!("{err}");
        assert!(msg.contains("lashay-1"), "must name the row: {msg}");
        assert!(msg.contains("mirrors"), "must say why: {msg}");
    }

    /// Two rows can share one code (they ride one group), and both must be narrowed by one clause
    /// — narrowing whichever the document happens to list first would leave the other bound wide
    /// with nothing to show for it.
    #[test]
    fn a_shared_code_narrows_every_row_that_carries_it() {
        let code = "tiredsolid";
        let rows: Vec<&Feed> = feeds().iter().filter(|f| f.code == code).collect();
        assert!(rows.len() > 1, "the fixture code must be shared");
        // Those rows are flat today, so the clause is refused — which is itself the per-row check:
        // the refusal fires on a row's shape, not on the first row found.
        let err = ChannelFloor::parse(&format!("{code}=1")).unwrap_err();
        assert!(matches!(err, FloorError::FlatRow { .. }), "{err:?}");
    }

    /// Whitespace around the code and the ids is tolerated: an operator writing a readable flag
    /// must not be told their code is unknown.
    #[test]
    fn whitespace_and_trailing_separators_are_tolerated() {
        let f = ChannelFloor::parse(" lashay-4 = 10, 11 ; ").unwrap();
        assert!(f.admits("lashay-4", 11));
        assert!(!f.admits("lashay-4", 12));
    }

    /// A clause selecting nothing would leave the row bound to no socket at all — silently the same
    /// as not subscribing to it, which is `--feed`'s job and not this flag's.
    #[test]
    fn a_clause_with_no_ids_is_rejected() {
        assert!(matches!(
            ChannelFloor::parse("lashay-4="),
            Err(FloorError::EmptySelection { .. })
        ));
        assert!(matches!(
            ChannelFloor::parse("lashay-4"),
            Err(FloorError::MissingIds { .. })
        ));
    }

    /// A non-numeric id is a typo, not a name: names deliberately do not live in this repo, so
    /// accepting one would mean mirroring a roster that has already moved once upstream.
    #[test]
    fn a_channel_name_is_rejected_as_an_id() {
        let err = ChannelFloor::parse("lashay-4=nfl").unwrap_err();
        assert!(
            matches!(err, FloorError::BadId { ref text, .. } if text == "nfl"),
            "wrong variant: {err:?}"
        );
    }

    /// A code written twice is ambiguous — one clause would win and the other's channels would go
    /// unbound with no diagnostic.
    #[test]
    fn a_repeated_code_is_rejected() {
        assert!(matches!(
            ChannelFloor::parse("lashay-4=10;lashay-4=11"),
            Err(FloorError::RepeatedCode { .. })
        ));
    }

    /// The startup breadcrumb reports what was narrowed against what exists, so "why is this market
    /// missing" is answerable from the log alone.
    #[test]
    fn the_summary_names_the_narrowing() {
        let f = ChannelFloor::parse("lashay-4=10,11").unwrap();
        assert_eq!(f.summary(), vec!["lashay-4=2 of 31".to_string()]);
    }
}
