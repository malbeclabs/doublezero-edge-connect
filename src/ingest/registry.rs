//! The feed registry as **data**, not code.
//!
//! Which group carries which feed, on which ports, with which channel roster, is the publisher's
//! to decide and it changes without our involvement — upstream reallocated it four times in dated
//! specs, each reversing the last. Compiling those numbers in makes every such change a rebuild,
//! and makes a stale copy invisible: a wrong port binds a socket that stays silent, and a wrong
//! group code activates nothing, with no warning either way.
//!
//! So the document is supplied to the container at runtime, from one of three sources, in
//! precedence order. This is also the seam the DoubleZero ledger drops into: it becomes a fourth
//! [`Source`] and nothing else here changes.
//!
//! The parsed document is **leaked once into `'static`** at startup. The registry is immutable and
//! process-lived, so this allocates once and never grows, and it is what keeps the seam free
//! downstream: `FeedKey`, every metric label and every `&'static str` field on [`Feed`] stay
//! exactly as they were. There is deliberately no hot reload — a feed set that changed under a
//! running receiver would leave books and reference data keyed to a topology no longer in effect.

use std::{net::Ipv4Addr, path::PathBuf, time::Duration};

use serde::Deserialize;
use tracing::{info, warn};

use crate::ingest::{
    feeds::{ArbitrationMode, Feed, FeedKind, FeedPorts, FeedPublisher},
    sources,
};

/// The document compiled in, so the container runs standalone and always has a fallback.
const BUILT_IN: &str = include_str!("registry.json");

/// The schema version this build understands.
///
/// Checked rather than ignored: the row structs reject unknown fields, so a document written for a
/// later schema would otherwise fail with a field-level error that reads like a typo. Adding a
/// field upstream is a version bump, and an older binary refusing it loudly is the point — half a
/// row applied silently is exactly the invisible drift this module exists to prevent.
const SUPPORTED_VERSION: u32 = 1;

/// How long to wait on a registry fetch before falling back to the built-in document. Bounded
/// because this runs on the startup path: an unreachable registry host must not hang the boot.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the registry document comes from, in precedence order.
#[derive(Debug, Clone)]
pub enum Source {
    /// `--feed-registry-url` — fetched once at startup. The precursor to reading the ledger.
    Url(String),
    /// `--feed-registry <path>` — a bind-mounted document.
    File(PathBuf),
    /// The document compiled in via `include_str!`, so the container runs standalone.
    BuiltIn,
}

impl Source {
    /// Resolve the two CLI flags to a source. A URL wins over a path; neither set means built-in.
    pub fn from_flags(url: &str, path: &str) -> Source {
        if !url.is_empty() {
            Source::Url(url.to_string())
        } else if !path.is_empty() {
            Source::File(PathBuf::from(path))
        } else {
            Source::BuiltIn
        }
    }
}

/// Why a registry document was refused. Every variant here is **fatal**: it means the document we
/// read is wrong, not missing. A document we could not read at all never reaches these — that path
/// warns and falls back to [`BUILT_IN`], because a container that will not boot over a briefly
/// unreachable registry host is worse than one running a slightly stale document, and the built-in
/// copy is by construction last-known-good.
#[derive(Debug)]
pub enum RegistryError {
    Parse(serde_json::Error),
    UnsupportedVersion(u32),
    NoFeeds,
    EmptyField {
        venue: String,
        field: &'static str,
    },
    UnknownVenue(String),
    EmptyRoster {
        venue: String,
        category: String,
    },
    BadRange {
        venue: String,
        lo: u8,
        hi: u8,
    },
    DuplicateBasePort {
        venue: String,
        category: String,
        port: u16,
    },
    PortOverflow {
        venue: String,
        base: u16,
        id: u8,
    },
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Parse(e) => write!(f, "feed registry does not parse: {e}"),
            RegistryError::UnsupportedVersion(v) => write!(
                f,
                "feed registry is version {v}; this build understands {SUPPORTED_VERSION}"
            ),
            RegistryError::NoFeeds => write!(f, "feed registry lists no feeds"),
            RegistryError::EmptyField { venue, field } => {
                write!(f, "{venue}: `{field}` is empty")
            }
            RegistryError::UnknownVenue(v) => write!(
                f,
                "venue `{v}` resolves to no Source ID; its messages would be dropped and its \
                 status stream would go unrecorded"
            ),
            RegistryError::EmptyRoster { venue, category } => {
                write!(f, "{venue}/{category}: derived roster is empty")
            }
            RegistryError::BadRange { venue, lo, hi } => {
                write!(f, "{venue}: channel range [{lo}, {hi}] is descending")
            }
            RegistryError::DuplicateBasePort {
                venue,
                category,
                port,
            } => write!(f, "{venue}/{category}: repeats base port {port}"),
            RegistryError::PortOverflow { venue, base, id } => {
                write!(f, "{venue}: port base {base} + channel {id} overflows u16")
            }
        }
    }
}

impl std::error::Error for RegistryError {}

// -------------------------------------------------------------------------------------------
// The wire model
// -------------------------------------------------------------------------------------------
//
// Deliberately a separate set of types from `feeds::*` rather than serde derives on the domain
// enums: the JSON spellings are a contract with an upstream document, and deriving on `FeedKind`
// would make a Rust variant rename a silent change to that contract.

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    /// Documentation carried in the document itself, since JSON has no comments. Never read.
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
    feeds: Vec<FeedRow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedRow {
    venue: String,
    category: String,
    code: String,
    kind: WireKind,
    group: Ipv4Addr,
    emit_trades: bool,
    arbitration: WireArbitration,
    publishers: Publishers,
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum WireKind {
    TopOfBook,
    Midpoint,
    MarketByOrder,
    MarketByPrice,
}

impl From<WireKind> for FeedKind {
    fn from(k: WireKind) -> FeedKind {
        match k {
            WireKind::TopOfBook => FeedKind::TopOfBook,
            WireKind::Midpoint => FeedKind::Midpoint,
            WireKind::MarketByOrder => FeedKind::MarketByOrder,
            WireKind::MarketByPrice => FeedKind::MarketByPrice,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum WireArbitration {
    Coordinated,
    Sticky,
}

impl From<WireArbitration> for ArbitrationMode {
    fn from(a: WireArbitration) -> ArbitrationMode {
        match a {
            WireArbitration::Coordinated => ArbitrationMode::Coordinated,
            WireArbitration::Sticky => ArbitrationMode::Sticky,
        }
    }
}

/// How a row lists its publishers: port blocks verbatim, or a channel roster to expand.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Publishers {
    /// One entry per publisher, ports written out.
    Explicit(Vec<PortBlock>),
    /// A channel roster plus per-plane bases; one publisher per channel at `base + id`.
    Derived(Derived),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortBlock {
    mktdata: u16,
    refdata: u16,
    /// Present only for the protocols with an in-band snapshot stream (market-by-order and
    /// market-by-price). A two-port block leaves it out rather than repeating a plane.
    #[serde(default)]
    snapshot: Option<u16>,
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Derived {
    channels: Vec<ChannelSpec>,
    ports: PortBases,
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ChannelSpec {
    /// An inclusive `[lo, hi]` span.
    Range([u8; 2]),
    /// A single channel id.
    Id(u8),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortBases {
    mktdata: u16,
    refdata: u16,
    /// Optional because the top-of-book plane binds a **pair** of ports — there is no in-band
    /// snapshot stream on it, and the `5xxxx` slot is left unallocated rather than reused so the
    /// leading digit keeps naming the traffic class.
    #[serde(default)]
    snapshot: Option<u16>,
}

// -------------------------------------------------------------------------------------------
// Loading
// -------------------------------------------------------------------------------------------

/// Resolve, validate and install the registry, returning the leaked `'static` rows.
///
/// Async only because the URL source fetches; the file and built-in sources never await. A fetch
/// or read failure warns and falls back to the built-in document. A document that *parses* but
/// fails validation is fatal — that is a wrong document, not a missing one, and a wrong registry
/// ingests the wrong feeds silently.
pub async fn load(source: Source) -> Result<&'static [Feed], RegistryError> {
    let (origin, text) = match &source {
        Source::Url(url) => match fetch(url).await {
            Ok(t) => (format!("url {url}"), t),
            Err(e) => {
                warn!(%url, error = %e, "feed registry fetch failed; using the built-in document");
                (format!("built-in (fetch of {url} failed)"), BUILT_IN.to_string())
            }
        },
        Source::File(path) => match std::fs::read_to_string(path) {
            Ok(t) => (format!("file {}", path.display()), t),
            Err(e) => {
                warn!(path = %path.display(), error = %e,
                      "feed registry read failed; using the built-in document");
                (
                    format!("built-in (read of {} failed)", path.display()),
                    BUILT_IN.to_string(),
                )
            }
        },
        Source::BuiltIn => ("built-in".to_string(), BUILT_IN.to_string()),
    };
    build(&text, &origin)
}

/// The built-in document, parsed. The fallback path and the one tests use.
pub fn load_built_in() -> Result<&'static [Feed], RegistryError> {
    build(BUILT_IN, "built-in")
}

async fn fetch(url: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()?
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}

/// Parse, validate, expand and leak. The single place a document becomes feed rows.
fn build(text: &str, origin: &str) -> Result<&'static [Feed], RegistryError> {
    let doc: Document = serde_json::from_str(text).map_err(RegistryError::Parse)?;
    if doc.version != SUPPORTED_VERSION {
        return Err(RegistryError::UnsupportedVersion(doc.version));
    }
    if doc.feeds.is_empty() {
        return Err(RegistryError::NoFeeds);
    }

    let mut rows = Vec::with_capacity(doc.feeds.len());
    for row in &doc.feeds {
        rows.push(feed_from(row)?);
    }

    // Which registry a process is running is the first question any drift investigation asks, and
    // it must not require guessing from behaviour.
    info!(
        source = origin,
        version = doc.version,
        rows = rows.len(),
        receivers = rows.iter().map(|f: &Feed| f.publishers.len()).sum::<usize>(),
        "feed registry resolved"
    );
    Ok(Box::leak(rows.into_boxed_slice()))
}

fn feed_from(row: &FeedRow) -> Result<Feed, RegistryError> {
    for (field, value) in [
        ("venue", &row.venue),
        ("category", &row.category),
        ("code", &row.code),
    ] {
        if value.is_empty() {
            return Err(RegistryError::EmptyField {
                venue: row.venue.clone(),
                field,
            });
        }
    }
    // A venue `source_id_of` does not resolve is dropped by `receiver::record_revealed`, so its
    // `status` stream goes unrecorded — a row that ingests but never reports.
    if sources::source_id_of(&row.venue).is_none() {
        return Err(RegistryError::UnknownVenue(row.venue.clone()));
    }

    let publishers = match &row.publishers {
        Publishers::Explicit(blocks) => blocks.iter().map(block_to_publisher).collect(),
        Publishers::Derived(d) => expand(row, d)?,
    };

    // Base ports are the `publisher` metric label and the reconciler's task-key component, so a
    // duplicate would silently merge two publishers' state machines into one receiver task.
    let mut seen = std::collections::HashSet::new();
    for p in &publishers {
        if !seen.insert(p.base_port()) {
            return Err(RegistryError::DuplicateBasePort {
                venue: row.venue.clone(),
                category: row.category.clone(),
                port: p.base_port(),
            });
        }
    }

    Ok(Feed {
        venue: leak(&row.venue),
        category: leak(&row.category),
        code: leak(&row.code),
        kind: row.kind.into(),
        group: row.group,
        publishers: Box::leak(publishers.into_boxed_slice()),
        emit_trades: row.emit_trades,
        arbitration: row.arbitration.into(),
    })
}

fn block_to_publisher(b: &PortBlock) -> FeedPublisher {
    FeedPublisher {
        ports: ports(b.mktdata, b.refdata, b.snapshot),
    }
}

fn ports(mktdata: u16, refdata: u16, snapshot: Option<u16>) -> FeedPorts {
    match snapshot {
        Some(snapshot) => FeedPorts::ThreePort {
            mktdata,
            refdata,
            snapshot,
        },
        None => FeedPorts::TwoPort { mktdata, refdata },
    }
}

/// Expand a derived roster into one [`FeedPublisher`] per channel, ascending.
///
/// A channel is an independent state machine (its own `Reset Count`, sequence series, manifest seq
/// and snapshot cycle), so one channel is one receiver task with its own processor state and books
/// — the same shape a mirrored publisher already has. Ports are `base + id` on every plane, which
/// is the arithmetic the publisher itself asserts; a subscriber that computes it differently joins
/// the right group and hears silence.
fn expand(row: &FeedRow, d: &Derived) -> Result<Vec<FeedPublisher>, RegistryError> {
    let ids = roster_ids(row, &d.channels)?;
    let p = &d.ports;
    ids.iter()
        .map(|&id| {
            let off = u16::from(id);
            let plane = |base: u16| {
                base.checked_add(off).ok_or(RegistryError::PortOverflow {
                    venue: row.venue.clone(),
                    base,
                    id,
                })
            };
            Ok(FeedPublisher {
                ports: ports(
                    plane(p.mktdata)?,
                    plane(p.refdata)?,
                    p.snapshot.map(plane).transpose()?,
                ),
            })
        })
        .collect()
}

/// Flatten ranges and singletons into a deduped, ascending id list.
///
/// Deduped rather than rejected on repeat: the same id reaching the expander twice would produce
/// two publishers on one port block, and the roster is a human-edited list where a singleton
/// overlapping a range is a plausible edit, not a corrupt document.
fn roster_ids(row: &FeedRow, channels: &[ChannelSpec]) -> Result<Vec<u8>, RegistryError> {
    let mut ids = Vec::new();
    for spec in channels {
        match *spec {
            ChannelSpec::Range([lo, hi]) => {
                if lo > hi {
                    return Err(RegistryError::BadRange {
                        venue: row.venue.clone(),
                        lo,
                        hi,
                    });
                }
                ids.extend(lo..=hi);
            }
            ChannelSpec::Id(id) => ids.push(id),
        }
    }
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Err(RegistryError::EmptyRoster {
            venue: row.venue.clone(),
            category: row.category.clone(),
        });
    }
    Ok(ids)
}

/// Leak one document string into `'static`. Runs once per field at startup over a handful of rows.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The sports channel roster, read from the **document** rather than from the expanded rows.
///
/// A test helper, not a source of truth: reading the document is what lets the port tests assert
/// the `base + id` derivation instead of merely echoing whatever the expander produced.
#[cfg(test)]
pub(crate) fn sports_channel_ids() -> Vec<u8> {
    let doc: Document = serde_json::from_str(BUILT_IN).expect("built-in document parses");
    let row = doc
        .feeds
        .iter()
        .find(|f| f.category == "sports")
        .expect("built-in document has no sports row");
    match &row.publishers {
        Publishers::Derived(d) => roster_ids(row, &d.channels).expect("sports roster"),
        Publishers::Explicit(_) => panic!("the sports row must carry a derived roster"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_with(feeds: &str) -> String {
        format!(r#"{{"version":1,"feeds":[{feeds}]}}"#)
    }

    const SPORTS_ROW: &str = r#"{
        "venue":"KALSHI","category":"sports","code":"c","kind":"MarketByPrice",
        "group":"233.84.178.20","emit_trades":true,"arbitration":"Sticky",
        "publishers":{"derived":{
            "channels":[{"range":[10,12]},{"id":49}],
            "ports":{"mktdata":33000,"refdata":43000,"snapshot":53000}}}}"#;

    #[test]
    fn built_in_document_loads() {
        let rows = load_built_in().expect("built-in document is valid");
        assert!(!rows.is_empty());
    }

    /// Ranges and singletons flatten to one ascending, deduped list, and every plane is offset by
    /// the id — the property the whole derived form exists for.
    #[test]
    fn derived_rows_expand_to_base_plus_id() {
        let rows = build(&doc_with(SPORTS_ROW), "test").unwrap();
        let ports: Vec<(u16, u16, Option<u16>)> = rows[0]
            .publishers
            .iter()
            .map(|p| (p.ports.mktdata(), p.ports.refdata(), p.ports.snapshot()))
            .collect();
        assert_eq!(
            ports,
            vec![
                (33010, 43010, Some(53010)),
                (33011, 43011, Some(53011)),
                (33012, 43012, Some(53012)),
                (33049, 43049, Some(53049)),
            ]
        );
    }

    /// No snapshot base means a two-port block, not a snapshot plane silently aliased onto another.
    #[test]
    fn a_derived_row_without_a_snapshot_base_binds_two_ports() {
        let row = SPORTS_ROW.replace(r#","snapshot":53000"#, "");
        let rows = build(&doc_with(&row), "test").unwrap();
        assert_eq!(rows[0].publishers[0].ports.snapshot(), None);
    }

    /// A version this build does not understand is refused rather than half-applied.
    #[test]
    fn an_unsupported_version_is_fatal() {
        let doc = doc_with(SPORTS_ROW).replace(r#""version":1"#, r#""version":2"#);
        assert!(matches!(
            build(&doc, "test"),
            Err(RegistryError::UnsupportedVersion(2))
        ));
    }

    /// A misspelled field must not be silently ignored: `emit_trads` defaulting to false is a
    /// muted tape with nothing to show for it.
    #[test]
    fn an_unknown_field_is_fatal() {
        let row = SPORTS_ROW.replace(r#""emit_trades""#, r#""emit_trads""#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::Parse(_))
        ));
    }

    /// A venue with no Source ID ingests but never reports — `record_revealed` drops it.
    #[test]
    fn an_unresolvable_venue_is_fatal() {
        let row = SPORTS_ROW.replace(r#""venue":"KALSHI""#, r#""venue":"NOPE""#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::UnknownVenue(v)) if v == "NOPE"
        ));
    }

    #[test]
    fn an_empty_code_is_fatal() {
        let row = SPORTS_ROW.replace(r#""code":"c""#, r#""code":"""#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::EmptyField { field: "code", .. })
        ));
    }

    #[test]
    fn an_empty_roster_is_fatal() {
        let row = SPORTS_ROW.replace(r#"{"range":[10,12]},{"id":49}"#, "");
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::EmptyRoster { .. })
        ));
    }

    #[test]
    fn a_descending_range_is_fatal() {
        let row = SPORTS_ROW.replace(r#"{"range":[10,12]}"#, r#"{"range":[12,10]}"#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::BadRange { lo: 12, hi: 10, .. })
        ));
    }

    /// `base + id` past `u16::MAX` must fail loudly rather than wrap onto an unrelated port.
    #[test]
    fn a_port_overflow_is_fatal() {
        let row = SPORTS_ROW.replace(r#""mktdata":33000"#, r#""mktdata":65530"#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::PortOverflow { .. })
        ));
    }

    /// Two publishers on one base port collapse into one reconciler task key and one metric child.
    #[test]
    fn a_duplicate_base_port_within_a_row_is_fatal() {
        let row = r#"{
            "venue":"PHOENIX","category":"spot","code":"c","kind":"TopOfBook",
            "group":"233.84.178.18","emit_trades":true,"arbitration":"Coordinated",
            "publishers":{"explicit":[
                {"mktdata":9201,"refdata":9202},{"mktdata":9201,"refdata":9302}]}}"#;
        assert!(matches!(
            build(&doc_with(row), "test"),
            Err(RegistryError::DuplicateBasePort { port: 9201, .. })
        ));
    }

    /// A singleton that also falls inside a range yields one publisher, not two on one port block.
    #[test]
    fn overlapping_roster_entries_dedup() {
        let row = SPORTS_ROW.replace(r#"{"id":49}"#, r#"{"id":11}"#);
        let rows = build(&doc_with(&row), "test").unwrap();
        assert_eq!(rows[0].publishers.len(), 3);
    }

    #[test]
    fn a_document_with_no_feeds_is_fatal() {
        assert!(matches!(
            build(r#"{"version":1,"feeds":[]}"#, "test"),
            Err(RegistryError::NoFeeds)
        ));
    }

    /// A missing file warns and falls back rather than refusing to boot: the built-in copy is by
    /// construction last-known-good, and a registry host blip must not take the tunnel down.
    #[tokio::test]
    async fn an_unreadable_file_falls_back_to_the_built_in_document() {
        let rows = load(Source::File(PathBuf::from("/nonexistent/registry.json")))
            .await
            .expect("fallback, not an error");
        assert_eq!(rows.len(), load_built_in().unwrap().len());
    }

    #[test]
    fn flags_resolve_in_precedence_order() {
        assert!(matches!(Source::from_flags("", ""), Source::BuiltIn));
        assert!(matches!(Source::from_flags("", "/p"), Source::File(_)));
        assert!(matches!(Source::from_flags("http://x", "/p"), Source::Url(_)));
    }
}
