//! The **channel filter**: which channels of an activated feed this process decodes.
//!
//! Distinct from the two filters either side of it. `reconcile` decides which *feeds* run, from the
//! host's subscriptions; `sinks::ws`'s `SubFilter` decides what one *client* receives. The channel
//! filter sits between them and is process-global and ops-owned: it scopes books, history and CPU
//! for everyone, which is why it is not reachable from the read-only query surface.
//!
//! It is an **allowlist**, not a threshold — the old name, "floor", implied a minimum, which is
//! backwards: it restricts a set, and every unmentioned feed keeps admitting everything.
//!
//! The channel filter acts **only** where the publisher derives a port per channel
//! (`port = base + channel_id`): an excluded channel is never bound and the kernel discards its
//! traffic before it reaches userspace. That costs nothing, and it is the reason the derivation
//! exists upstream. One such feed carries tens of thousands of markets across dozens of channels,
//! far past what the rolling history window holds, so narrowing it is the difference between
//! covering a slice of one league completely and thrashing across all of them.
//!
//! Narrowing a feed whose publishers bind a base **flat** is refused at startup rather than
//! implemented as a datagram-header test. On such a feed `channel_id` identifies mirrors, not markets —
//! each publisher carries the complete instrument universe — so narrowing it would discard
//! redundancy without reducing a single decoded message. There is no trader-facing reason to want
//! it, and refusing is honest where a header test would be a filter that costs CPU and buys
//! nothing. If a flat feed ever partitions markets, this is the seam to revisit.
//!
//! Two properties of the syntax are deliberate:
//!
//! - **Keyed by group code, never global.** `--channels edge-kalshi-sports-mbp=10,11` narrows that feed and no
//!   other; an unmentioned feed ingests every channel. One global flag would let an operator filter
//!   down to a league and silently half-blind an unrelated mirrored feed, since the two planes give
//!   `channel_id` different meanings.
//! - **Ids are the contract; names are not.** Channel *names* live in the publisher's inventory by
//!   design — they have already moved once, and a copy here would drift exactly as four superseded
//!   port allocations did. The channel filter takes numeric ids and validates them against the
//!   published set in the **loaded document**, so a typo fails startup instead of filtering nothing.

use std::collections::{BTreeSet, HashMap};

use crate::ingest::feeds::{feeds, Feed, FeedPublisher};

/// Which channels of each group code this process ingests. An absent code means "admit all", which
/// is what makes the empty channel filter a no-op.
#[derive(Debug, Clone, Default)]
pub struct ChannelFilter {
    admitted: HashMap<&'static str, Selection>,
}

/// What one clause resolved to: the admitted ids, and how many channels the code's feeds carry in
/// total.
///
/// The denominator is recorded here at parse time rather than looked up again in
/// [`ChannelFilter::summary`], so both halves of the ratio come from the same feed set at the same
/// instant. Reading it back out of `feeds()` later made the summary a statement about two different
/// things — and would have been silently wrong for a channel filter parsed against any other feed set.
#[derive(Debug, Clone)]
struct Selection {
    ids: BTreeSet<u8>,
    published_set_size: usize,
}

/// Why a channel filter was refused at startup.
///
/// Every variant is fatal by design: a channel filter that silently filters nothing is worse than
/// one that refuses to start, because the symptom of the former is a feed that reads healthy and
/// carries markets nobody asked for (or, on a mistyped id, a socket bound to a port no publisher
/// sends to, which reads as a dead feed rather than as a typo). Each message names the offending
/// feed and the consequence, matching `registry::RegistryError`'s convention.
#[derive(Debug)]
pub enum FilterError {
    /// A clause with no `=`.
    MissingIds { clause: String },
    /// A code no feed in the loaded document carries.
    UnknownCode { code: String, known: Vec<String> },
    /// A code listed twice in one spec.
    RepeatedCode { code: String },
    /// A clause that selects no channel at all.
    EmptySelection { code: String },
    /// An id that is not a number in `0..=255`.
    BadId { code: String, text: String },
    /// An id outside the published set of one of the feeds carrying the code.
    ///
    /// Carries the offending **feed**, not just the code: one clause narrows every feed that
    /// carries its code, so the id has to be legal for all of them, and "not in the published set" is only
    /// actionable if the operator is told which feed.
    UnknownChannel {
        code: String,
        venue: String,
        category: String,
        id: u8,
        published_set: Vec<u8>,
    },
    /// A feed whose publishers bind a base flat — narrowing it is refused, not implemented.
    FlatRow {
        code: String,
        venue: String,
        category: String,
    },
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::MissingIds { clause } => write!(
                f,
                "channel filter clause `{clause}` has no `=`; the syntax is \
                 `<code>=<id>[,<id>...][;<code>=...]`"
            ),
            FilterError::UnknownCode { code, known } => write!(
                f,
                "channel filter names group code `{code}`, which no feed in the loaded feed \
                 registry carries; it would filter nothing. Known codes: {}",
                known.join(", ")
            ),
            FilterError::RepeatedCode { code } => write!(
                f,
                "channel filter lists group code `{code}` twice; one clause would win and the \
                 other's channels would go silently unbound"
            ),
            FilterError::EmptySelection { code } => write!(
                f,
                "channel filter selects no channel for `{code}`; the feed would bind no socket at \
                 all, which is `--feed`'s job, not this flag's"
            ),
            FilterError::BadId { code, text } => write!(
                f,
                "channel filter id `{text}` for `{code}` is not a channel id (0-255)"
            ),
            FilterError::UnknownChannel {
                code,
                venue,
                category,
                id,
                published_set,
            } => write!(
                f,
                "channel filter names channel {id} on `{code}`, which is not in the published set of \
                 {venue}/{category}. Every feed carrying a code is narrowed by the same clause, so \
                 an id must be in every one of their published sets: an id in none of them binds a port no \
                 publisher sends to (which reads as a dead feed rather than as a typo), and an id \
                 in only some leaves the remaining feeds binding no socket at all. Published set of \
                 {venue}/{category}: {}",
                published_set
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            FilterError::FlatRow {
                code,
                venue,
                category,
            } => write!(
                f,
                "channel filter narrows `{code}` ({venue}/{category}), whose publishers bind one \
                 base port flat. On such a feed `channel_id` identifies mirrors, not markets — each \
                 publisher carries the complete instrument universe — so narrowing it would give up \
                 redundancy without reducing a single decoded message. Remove the clause, or use \
                 `--publisher-port` to drop a specific mirror"
            ),
        }
    }
}

impl std::error::Error for FilterError {}

/// The channel ids a feed's publishers were derived from, ascending. Empty for a flat feed.
fn published_set(f: &Feed) -> BTreeSet<u8> {
    f.publishers.iter().filter_map(|p| p.channel).collect()
}

fn known_codes(feeds: &[Feed]) -> Vec<String> {
    let mut codes: Vec<String> = feeds.iter().map(|f| f.code.to_string()).collect();
    codes.sort();
    codes.dedup();
    codes
}

impl ChannelFilter {
    /// Parse `<code>=<id>[,<id>...][;<code>=...]`, resolving and validating against the **loaded**
    /// registry document.
    ///
    /// Validation is against the document rather than a compiled-in list on purpose: the published set is
    /// the publisher's to change, so the only published set this process can honestly check an id against
    /// is the one it is about to bind. This is also where the trust boundary the loadable registry
    /// moved gets re-established — an id is now operator input reaching a set of feeds that are
    /// themselves operator input, and neither the compiler nor the test suite can see either.
    pub fn parse(spec: &str) -> Result<ChannelFilter, FilterError> {
        Self::parse_within(feeds(), spec)
    }

    /// The parse, against an explicit feed set.
    ///
    /// Separate from [`ChannelFilter::parse`] so the tests can supply their own feeds. The property
    /// that decides this function's hardest case — one code spanning several **derived** feeds with
    /// **different** published sets — is not expressible in the built-in document, and a test that cannot
    /// construct it cannot fail on it. That is not hypothetical: validating against the union of the
    /// published sets passed every test written against the built-in document while leaving a feed bound to
    /// nothing.
    fn parse_within(feed_rows: &'static [Feed], spec: &str) -> Result<ChannelFilter, FilterError> {
        let mut admitted: HashMap<&'static str, Selection> = HashMap::new();
        for clause in spec.split(';') {
            let clause = clause.trim();
            if clause.is_empty() {
                continue;
            }
            let Some((code, ids)) = clause.split_once('=') else {
                return Err(FilterError::MissingIds {
                    clause: clause.to_string(),
                });
            };
            let code = code.trim();
            // Several feeds can share one code (they ride one group), so the clause resolves to a
            // set of feeds and every one of them is narrowed by it.
            let matching: Vec<&'static Feed> =
                feed_rows.iter().filter(|f| f.code == code).collect();
            let Some(first) = matching.first() else {
                return Err(FilterError::UnknownCode {
                    code: code.to_string(),
                    known: known_codes(feed_rows),
                });
            };
            // Reuse the registry's own `&'static str` for the key, so the map holds no leaked
            // copy of the flag text.
            let code: &'static str = first.code;

            // Per feed, never merged. The flat-feed refusal has to see each feed's own shape, and
            // the id check below has to see each feed's own published set: an id legal for one feed and
            // not another is not a partial narrowing, it is a feed narrowed to **zero**
            // publishers — which binds nothing, and, if it is the only enabled feed, takes the WS
            // sink and the query API down with it (they come up only when a market-data feed is
            // running).
            let mut published_sets: Vec<(&'static Feed, BTreeSet<u8>)> = Vec::new();
            for f in &matching {
                let r = published_set(f);
                if r.is_empty() {
                    return Err(FilterError::FlatRow {
                        code: code.to_string(),
                        venue: f.venue.to_string(),
                        category: f.category.to_string(),
                    });
                }
                published_sets.push((f, r));
            }

            let mut wanted = BTreeSet::new();
            for id in ids.split(',') {
                let id = id.trim();
                // An empty token is a stray or trailing separator, skipped exactly as an empty
                // clause is between `;`s. Selecting nothing at all is still refused, below — but as
                // `EmptySelection`, which is what it is, rather than as a comma complaint.
                if id.is_empty() {
                    continue;
                }
                let id: u8 = id.parse().map_err(|_| FilterError::BadId {
                    code: code.to_string(),
                    text: id.to_string(),
                })?;
                for (f, r) in &published_sets {
                    if !r.contains(&id) {
                        return Err(FilterError::UnknownChannel {
                            code: code.to_string(),
                            venue: f.venue.to_string(),
                            category: f.category.to_string(),
                            id,
                            published_set: r.iter().copied().collect(),
                        });
                    }
                }
                wanted.insert(id);
            }
            if wanted.is_empty() {
                return Err(FilterError::EmptySelection {
                    code: code.to_string(),
                });
            }
            // Every id is in every feed's published set and there is at least one, so each of these feeds
            // keeps at least one publisher by construction. That is the invariant the per-feed
            // check above exists to establish, not a coincidence of the ids an operator happened to
            // pick.
            let published_set_size = published_sets
                .iter()
                .flat_map(|(_, r)| r.iter().copied())
                .collect::<BTreeSet<u8>>()
                .len();
            let selection = Selection {
                ids: wanted,
                published_set_size,
            };
            if admitted.insert(code, selection).is_some() {
                return Err(FilterError::RepeatedCode {
                    code: code.to_string(),
                });
            }
        }
        Ok(ChannelFilter { admitted })
    }

    /// Whether this process ingests `channel` of the feed carrying `code`. An unmentioned code
    /// admits everything.
    pub fn admits(&self, code: &str, channel: u8) -> bool {
        self.admitted
            .get(code)
            .is_none_or(|s| s.ids.contains(&channel))
    }

    /// The publishers of `feed` this process binds — the channel filter's whole effect on the hot
    /// path.
    ///
    /// A feed the channel filter does not mention keeps every publisher, unchanged. A mentioned
    /// feed keeps only the publishers derived from an admitted channel; the rest are never spawned,
    /// so their sockets are never bound and the kernel drops their traffic. A flat feed cannot be
    /// mentioned ([`ChannelFilter::parse`] refuses it), which is why there is no "keep everything"
    /// fallback here for a publisher with no channel id.
    pub fn publishers_for(&self, feed: &Feed) -> Vec<&'static FeedPublisher> {
        match self.admitted.get(feed.code) {
            None => feed.publishers.iter().collect(),
            Some(s) => feed
                .publishers
                .iter()
                .filter(|p| p.channel.is_some_and(|c| s.ids.contains(&c)))
                .collect(),
        }
    }

    /// Nothing narrowed — the default, and the state every deployment that sets no flag is in.
    pub fn is_empty(&self) -> bool {
        self.admitted.is_empty()
    }

    /// What this channel filter narrowed, for the startup log: `code=admitted of published set`. Which
    /// channels a process actually bound is the first question a "why is this market missing"
    /// report asks.
    ///
    /// Both halves of the ratio come from the feed set this channel filter was **parsed against**,
    /// recorded at that moment: a denominator re-read from `feeds()` here would describe a
    /// different feed set from the numerator.
    pub fn summary(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .admitted
            .iter()
            .map(|(code, s)| format!("{code}={} of {}", s.ids.len(), s.published_set_size))
            .collect();
        out.sort();
        out
    }

    /// The group codes this channel filter narrows. Lets the caller check them against a feed
    /// selection it made for other reasons — a clause naming a feed that `--feed`/`--publisher-port`
    /// already excluded is legal but filters nothing, and silence there is the same invisible no-op
    /// this module refuses everywhere else.
    pub fn codes(&self) -> Vec<&'static str> {
        let mut v: Vec<&'static str> = self.admitted.keys().copied().collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::feeds::FeedPorts;

    /// An empty channel filter admits everything — the default must be "ingest what you are
    /// subscribed to", so an operator who sets nothing sees no behaviour change.
    #[test]
    fn an_empty_filter_admits_every_channel() {
        let f = ChannelFilter::parse("").expect("empty is valid");
        assert!(f.admits("edge-kalshi-sports-mbp", 10));
        assert!(f.admits("edge-kalshi-sports-mbp", 49));
        assert!(f.admits("edge-kalshi-perps-tob", 2));
        assert!(f.is_empty());
    }

    /// The channel filter is scoped per group code. Narrowing one feed must not narrow another —
    /// the two planes give `channel_id` different meanings and a global filter would conflate them.
    #[test]
    fn a_filter_on_one_feed_does_not_narrow_another() {
        let f = ChannelFilter::parse("edge-kalshi-sports-mbp=10,11").unwrap();
        assert!(f.admits("edge-kalshi-sports-mbp", 10));
        assert!(!f.admits("edge-kalshi-sports-mbp", 12));
        assert!(
            f.admits("edge-kalshi-perps-tob", 1),
            "an unmentioned feed was narrowed"
        );
        assert!(
            f.admits("edge-kalshi-perps-tob", 2),
            "an unmentioned feed was narrowed"
        );
    }

    /// A selected sports channel binds its socket and an excluded one binds nothing. Asserting on
    /// the bound set rather than on decoded output is deliberate: decoded output is empty for an
    /// excluded channel either way, so that assertion could not fail.
    #[test]
    fn an_excluded_sports_channel_binds_no_socket() {
        let feed = feeds().iter().find(|f| f.category == "sports").unwrap();
        let f = ChannelFilter::parse("edge-kalshi-sports-mbp=10,11").unwrap();
        let ports: Vec<u16> = f
            .publishers_for(feed)
            .iter()
            .map(|p| p.base_port())
            .collect();
        assert_eq!(ports, vec![34010, 34011]);
    }

    /// The other half of the same property, and the one that fails if `publishers_for` ever starts
    /// filtering a feed nobody narrowed: an unmentioned feed binds its whole published set.
    #[test]
    fn an_unmentioned_feed_binds_every_publisher() {
        let feed = feeds().iter().find(|f| f.category == "sports").unwrap();
        let f = ChannelFilter::parse("").unwrap();
        assert_eq!(f.publishers_for(feed).len(), feed.publishers.len());
        assert!(
            feed.publishers.len() > 2,
            "the feed must be worth narrowing"
        );
    }

    /// An id outside the feed's published set is a startup error. Silently admitting it would bind a port
    /// no publisher sends to, which reads as a dead feed rather than as a typo.
    #[test]
    fn an_id_outside_the_published_set_is_rejected() {
        let err = ChannelFilter::parse("edge-kalshi-sports-mbp=10,63").unwrap_err();
        assert!(
            matches!(err, FilterError::UnknownChannel { id: 63, .. }),
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
        let err = ChannelFilter::parse("edge-kalshi-nonesuch=10").unwrap_err();
        assert!(
            matches!(err, FilterError::UnknownCode { ref code, .. } if code == "edge-kalshi-nonesuch"),
            "wrong variant: {err:?}"
        );
    }

    /// **The refusal.** On a feed whose publishers bind a base flat, `channel_id` identifies mirrors
    /// and each publisher carries the complete universe, so narrowing it discards redundancy
    /// without reducing a single decoded message. A datagram-header filter there would cost CPU and
    /// buy nothing, so this is refused rather than implemented — and the error has to name the
    /// feed, since "the channel filter did not apply" is otherwise indistinguishable from "the ids
    /// were wrong".
    #[test]
    fn narrowing_a_flat_feed_is_refused() {
        let err = ChannelFilter::parse("edge-kalshi-perps-tob=2").unwrap_err();
        assert!(
            matches!(err, FilterError::FlatRow { ref code, .. } if code == "edge-kalshi-perps-tob"),
            "wrong variant: {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("edge-kalshi-perps-tob"),
            "must name the feed: {msg}"
        );
        assert!(msg.contains("mirrors"), "must say why: {msg}");
    }

    // -------------------------------------------------------------------------------------------
    // One code, several feeds
    //
    // The built-in document cannot express this: its one derived feed has a unique code, so every
    // test written against it is satisfied by an implementation that consults only the first
    // matching feed. These build their own feeds for exactly that reason — the property under test
    // is "every feed that carries the code", and a fixture where the feeds are indistinguishable
    // measures neither of them.
    // -------------------------------------------------------------------------------------------

    /// A derived feed with the given published set, leaked so it can join a `&'static [Feed]`.
    fn derived_row(code: &'static str, category: &'static str, channels: &[u8]) -> Feed {
        let pubs: Vec<FeedPublisher> = channels
            .iter()
            .map(|&id| FeedPublisher {
                ports: FeedPorts::ThreePort {
                    mktdata: 33000 + u16::from(id),
                    refdata: 43000 + u16::from(id),
                    snapshot: 53000 + u16::from(id),
                },
                channel: Some(id),
                label: None,
            })
            .collect();
        row(code, category, Box::leak(pubs.into_boxed_slice()))
    }

    /// A flat feed: one block, no channel id.
    fn flat_row(code: &'static str, category: &'static str) -> Feed {
        static PUBS: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::TwoPort {
                mktdata: 7576,
                refdata: 7577,
            },
            channel: None,
            label: None,
        }];
        row(code, category, PUBS)
    }

    fn row(
        code: &'static str,
        category: &'static str,
        publishers: &'static [FeedPublisher],
    ) -> Feed {
        Feed {
            venue: "KALSHI",
            category,
            code,
            kind: crate::ingest::feeds::FeedKind::MarketByPrice,
            group: std::net::Ipv4Addr::new(233, 84, 178, 20),
            publishers,
            emit_trades: true,
            arbitration: crate::ingest::feeds::ArbitrationMode::Sticky,
            mirror_offset: None,
        }
    }

    fn rows(rows: Vec<Feed>) -> &'static [Feed] {
        Box::leak(rows.into_boxed_slice())
    }

    /// **The union bug.** One clause narrows every feed carrying its code, so an id legal for one
    /// feed and absent from another does not narrow the second — it empties it. That feed then
    /// binds zero sockets, and if it is the only enabled feed the WS sink and query API go down
    /// with it, since both activate only while a market-data feed is running.
    ///
    /// Validating against the union of the published sets accepted exactly this. The refusal must name the
    /// feed it would have emptied, or "not in the published set" is unactionable when several feeds share a
    /// code.
    #[test]
    fn an_id_missing_from_one_feed_of_a_shared_code_is_refused() {
        let doc = rows(vec![
            derived_row("shared", "sports", &[10, 11]),
            derived_row("shared", "extras", &[10, 12]),
        ]);

        // 11 is in the first feed's published set and not the second's. Under the union rule this parsed
        // cleanly and left `extras` with no publisher at all.
        let err = ChannelFilter::parse_within(doc, "shared=11").unwrap_err();
        assert!(
            matches!(&err, FilterError::UnknownChannel { id: 11, category, .. } if category == "extras"),
            "wrong variant, or it named the wrong feed: {err:?}"
        );
        assert!(
            format!("{err}").contains("extras"),
            "the error must name the feed it would empty: {err}"
        );

        // And the symmetric direction, so this is not passing because the second feed is special.
        let err = ChannelFilter::parse_within(doc, "shared=12").unwrap_err();
        assert!(
            matches!(&err, FilterError::UnknownChannel { id: 12, category, .. } if category == "sports"),
            "wrong variant, or it named the wrong feed: {err:?}"
        );
    }

    /// The legal case, and the property the test above is the negative of: an id every feed
    /// carries narrows **all** of them, each to exactly that channel — and every feed keeps a
    /// publisher, which is the invariant the per-feed check buys.
    #[test]
    fn a_shared_code_narrows_every_feed_that_carries_it() {
        let doc = rows(vec![
            derived_row("shared", "sports", &[10, 11]),
            derived_row("shared", "extras", &[10, 12]),
        ]);
        let f = ChannelFilter::parse_within(doc, "shared=10").unwrap();
        for r in doc {
            let ports: Vec<u16> = f.publishers_for(r).iter().map(|p| p.base_port()).collect();
            assert_eq!(
                ports,
                vec![33010],
                "{}: every feed carrying the code must be narrowed, and none emptied",
                r.category
            );
        }
    }

    /// The flat-feed refusal is per feed too: a code spanning a derived feed **and** a flat one is
    /// refused, naming the flat one. Ordered with the derived feed first, so an implementation that
    /// inspects only the first match accepts it.
    #[test]
    fn a_flat_feed_sharing_a_code_with_a_derived_one_is_still_refused() {
        let doc = rows(vec![
            derived_row("shared", "sports", &[10, 11]),
            flat_row("shared", "mirrored"),
        ]);
        let err = ChannelFilter::parse_within(doc, "shared=10").unwrap_err();
        assert!(
            matches!(&err, FilterError::FlatRow { category, .. } if category == "mirrored"),
            "wrong variant, or it named the wrong feed: {err:?}"
        );
    }

    /// The denominator in the startup summary comes from the feeds the channel filter was parsed
    /// against, not from a later lookup in `feeds()` — which for this channel filter would report
    /// the built-in document's codes and find none of them.
    #[test]
    fn the_summary_denominator_follows_the_feeds_it_was_parsed_against() {
        let doc = rows(vec![
            derived_row("shared", "sports", &[10, 11]),
            derived_row("shared", "extras", &[10, 12]),
        ]);
        let f = ChannelFilter::parse_within(doc, "shared=10").unwrap();
        // Three distinct ids across the two feeds (10, 11, 12), one admitted.
        assert_eq!(f.summary(), vec!["shared=1 of 3".to_string()]);
        assert_eq!(f.codes(), vec!["shared"]);
    }

    /// Whitespace around the code and the ids is tolerated: an operator writing a readable flag
    /// must not be told their code is unknown. A trailing separator is tolerated **on both
    /// separators** — a stray `,` used to be fatal while a stray `;` was skipped, which is one
    /// syntax behaving as two.
    #[test]
    fn whitespace_and_trailing_separators_are_tolerated() {
        let f = ChannelFilter::parse(" edge-kalshi-sports-mbp = 10, 11 ; ").unwrap();
        assert!(f.admits("edge-kalshi-sports-mbp", 11));
        assert!(!f.admits("edge-kalshi-sports-mbp", 12));

        let trailing_comma = ChannelFilter::parse("edge-kalshi-sports-mbp=10,11,").unwrap();
        assert_eq!(trailing_comma.summary(), f.summary());
    }

    /// A clause selecting nothing would leave the feed bound to no socket at all — silently the
    /// same as not subscribing to it, which is `--feed`'s job and not this flag's. A clause of
    /// nothing but separators reports the same thing, rather than complaining about the comma.
    #[test]
    fn a_clause_with_no_ids_is_rejected() {
        assert!(matches!(
            ChannelFilter::parse("edge-kalshi-sports-mbp=,"),
            Err(FilterError::EmptySelection { .. })
        ));
        assert!(matches!(
            ChannelFilter::parse("edge-kalshi-sports-mbp="),
            Err(FilterError::EmptySelection { .. })
        ));
        assert!(matches!(
            ChannelFilter::parse("edge-kalshi-sports-mbp"),
            Err(FilterError::MissingIds { .. })
        ));
    }

    /// A non-numeric id is a typo, not a name: names deliberately do not live in this repo, so
    /// accepting one would mean mirroring a published set that has already moved once upstream.
    #[test]
    fn a_channel_name_is_rejected_as_an_id() {
        let err = ChannelFilter::parse("edge-kalshi-sports-mbp=nfl").unwrap_err();
        assert!(
            matches!(err, FilterError::BadId { ref text, .. } if text == "nfl"),
            "wrong variant: {err:?}"
        );
    }

    /// A code written twice is ambiguous — one clause would win and the other's channels would go
    /// unbound with no diagnostic.
    #[test]
    fn a_repeated_code_is_rejected() {
        assert!(matches!(
            ChannelFilter::parse("edge-kalshi-sports-mbp=10;edge-kalshi-sports-mbp=11"),
            Err(FilterError::RepeatedCode { .. })
        ));
    }

    /// The startup breadcrumb reports what was narrowed against what exists, so "why is this
    /// market missing" is answerable from the log alone.
    #[test]
    fn the_summary_names_the_narrowing() {
        let f = ChannelFilter::parse("edge-kalshi-sports-mbp=10,11").unwrap();
        assert_eq!(
            f.summary(),
            vec!["edge-kalshi-sports-mbp=2 of 31".to_string()]
        );
    }
}
