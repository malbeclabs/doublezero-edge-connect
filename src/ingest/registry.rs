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
/// This is for a change that reinterprets what a field *means*, not for one that adds a field —
/// additive changes are handled by ignoring and reporting unknown keys, so they never bump this and
/// never reach a rejection. A document whose version this build does not know is one whose existing
/// fields it may read wrongly, which is why it is refused rather than applied: under a URL source
/// that refusal degrades to the built-in copy, under a file it is fatal (see [`load`]).
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

/// Why a registry document was refused: it means the document we read is **wrong**, not missing.
///
/// Whether that is fatal depends on where the document came from, and [`load`] is where that is
/// decided — a bind-mounted file or the built-in copy dies here, a URL degrades to the built-in
/// copy with a warning. Unknown *keys* are deliberately not in this enum: they are reported by
/// [`warn_unknown`] and ignored, so an additive upstream change never lands on this path at all.
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
    /// Two rows share a `(venue, category, kind)` identity.
    DuplicateRow {
        venue: String,
        category: String,
        kind: &'static str,
    },
    /// A venue's rows declare more than one arbitration mode.
    ArbitrationDisagreement {
        venue: String,
    },
    /// A row's `emit_trades` contradicts whether its kind can ever own a tape.
    EmitTradesDisagrees {
        venue: String,
        category: String,
        kind: &'static str,
    },
    /// Two receivers would bind the same `(group, port)`.
    DuplicateGroupPort {
        venue: String,
        group: Ipv4Addr,
        port: u16,
    },
    /// A publisher's port block does not match the plane count its protocol binds.
    PortShape {
        venue: String,
        category: String,
        kind: &'static str,
        base_port: u16,
        expected: u8,
        found: u8,
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
            RegistryError::DuplicateRow {
                venue,
                category,
                kind,
            } => write!(
                f,
                "{venue}/{category}: two `{kind}` rows share one identity; the second would be \
                 dropped by feed selection and never bound"
            ),
            RegistryError::ArbitrationDisagreement { venue } => write!(
                f,
                "{venue}: rows declare two arbitration modes; the venue's mode is keyed by venue \
                 alone, so it would depend on document order"
            ),
            RegistryError::EmitTradesDisagrees {
                venue,
                category,
                kind,
            } => write!(
                f,
                "{venue}/{category}: `emit_trades` disagrees with whether `{kind}` can own a tape"
            ),
            RegistryError::DuplicateGroupPort { venue, group, port } => write!(
                f,
                "{venue}: two receivers would bind ({group}, {port}); the kernel would split that \
                 group's datagrams arbitrarily between them"
            ),
            RegistryError::PortShape {
                venue,
                category,
                kind,
                base_port,
                expected,
                found,
            } => write!(
                f,
                "{venue}/{category}: a `{kind}` publisher binds {expected} ports, but block \
                 {base_port} lists {found}. A missing `snapshot` leaves the book in recovery \
                 forever, so the feed connects and produces nothing; an unexpected one binds a \
                 socket the protocol never fills"
            ),
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
//
// **No `deny_unknown_fields` anywhere in this module** — the same rule `doublezero-edge/src/types.rs`
// states for the REST types, for the same reason. A field a newer publisher adds must be ignored by
// an older binary, not rejected: the registry is infrastructure that moves underneath a running
// fleet, and rejecting an additive change would take every process down at its next restart. What
// `deny_unknown_fields` was actually buying — a misspelled key not silently defaulting — is bought
// instead by capturing the leftovers in [`Unknown`] and warning about each one by path.

/// Keys the schema does not know, captured rather than rejected so they can be reported.
///
/// `BTreeMap` so the warnings come out in a stable order; a `#[serde(flatten)]` field collects
/// exactly the keys no declared field claimed, which means it needs no maintenance as fields are
/// added.
type Unknown = std::collections::BTreeMap<String, serde_json::Value>;

/// Warn about every key the schema did not recognise, naming its path in the document.
///
/// A `warn` and not an error: an unknown key is either an upstream addition this build predates
/// (harmless, and refusing it is what would crash-loop a fleet) or a typo (harmless to the loader,
/// but it means the author's intent was silently dropped — which is exactly what must not be
/// invisible).
fn warn_unknown(path: &str, unknown: &Unknown) {
    for key in unknown.keys() {
        warn!(key = %format!("{path}.{key}"), "feed registry: unrecognised key, ignored");
    }
}

#[derive(Debug, Deserialize)]
struct Document {
    version: u32,
    /// Documentation carried in the document itself, since JSON has no comments. Never read.
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
    feeds: Vec<FeedRow>,
    #[serde(flatten)]
    unknown: Unknown,
}

#[derive(Debug, Deserialize)]

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
    #[serde(flatten)]
    unknown: Unknown,
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
#[serde(rename_all = "snake_case")]
enum Publishers {
    /// One entry per publisher, ports written out.
    Explicit(Vec<PortBlock>),
    /// A channel roster plus per-plane bases; one publisher per channel at `base + id`.
    Derived(Derived),
}

#[derive(Debug, Deserialize)]
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
    #[serde(flatten)]
    unknown: Unknown,
}

#[derive(Debug, Deserialize)]
struct Derived {
    channels: Vec<ChannelSpec>,
    ports: PortBases,
    #[serde(default)]
    #[allow(dead_code)]
    notes: serde_json::Value,
    #[serde(flatten)]
    unknown: Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChannelSpec {
    /// An inclusive `[lo, hi]` span.
    Range([u8; 2]),
    /// A single channel id.
    Id(u8),
}

#[derive(Debug, Deserialize)]
struct PortBases {
    mktdata: u16,
    refdata: u16,
    /// Optional because the top-of-book plane binds a **pair** of ports — there is no in-band
    /// snapshot stream on it, and the `5xxxx` slot is left unallocated rather than reused so the
    /// leading digit keeps naming the traffic class.
    #[serde(default)]
    snapshot: Option<u16>,
    #[serde(flatten)]
    unknown: Unknown,
}

// -------------------------------------------------------------------------------------------
// Loading
// -------------------------------------------------------------------------------------------

/// A resolved registry: the leaked rows plus what they came from.
///
/// The origin and version ride along rather than being logged inside [`build`] because the caller
/// installs the rows and the install can lose a race — logging "registry resolved" for a document
/// that was then discarded is precisely the breadcrumb a drift investigation must not be given.
pub struct Loaded {
    pub rows: &'static [Feed],
    origin: String,
    version: u32,
}

impl Loaded {
    /// Announce the installed registry. Which one a process is running is the first question any
    /// drift investigation asks, and it must not require guessing from behaviour.
    pub fn log_resolved(&self) {
        info!(
            source = self.origin,
            version = self.version,
            rows = self.rows.len(),
            receivers = self.rows.iter().map(|f| f.publishers.len()).sum::<usize>(),
            "feed registry resolved"
        );
    }

    /// What the rows came from, for a caller that needs to say so without logging.
    pub fn origin(&self) -> &str {
        &self.origin
    }
}

/// Resolve and validate the registry, leaking the rows into `'static`.
///
/// Async only because the URL source fetches; the file and built-in sources never await.
///
/// **A rejected document degrades or refuses depending on where it came from, and the asymmetry is
/// the whole point:**
///
/// - [`Source::Url`] — *any* failure (unreachable host, malformed body, a `version` this build
///   predates, a validation error) warns and falls back to the built-in copy. A remote registry is
///   infrastructure that moves underneath a running fleet, and because resolution happens only at
///   startup, refusing would not kill the fleet when the document changed — it would kill each
///   process at its next reschedule, hours later and far from the cause. A host that is *up* and
///   serving one new field must not be worse than a host that is down.
/// - [`Source::File`] and [`Source::BuiltIn`] — fatal. A bind-mounted file is an operator's explicit
///   instruction about this one container, so a wrong one must not run; and a built-in copy that
///   does not load is a build defect.
///
/// The built-in copy is by construction last-known-good, which is what makes the fallback safe.
pub async fn load(source: Source) -> Result<Loaded, RegistryError> {
    match &source {
        Source::Url(url) => {
            let fallback = |reason: &str| {
                build(BUILT_IN, &format!("built-in ({reason})"))
                    .expect("the built-in feed registry document is valid")
            };
            match fetch(url).await {
                Err(e) => {
                    warn!(%url, error = %e,
                          "feed registry fetch failed; falling back to the built-in document");
                    Ok(fallback(&format!("fetch of {url} failed")))
                }
                Ok(text) => match build(&text, &format!("url {url}")) {
                    Ok(loaded) => Ok(loaded),
                    Err(e) => {
                        warn!(%url, error = %e,
                              "fetched feed registry was rejected; falling back to the built-in \
                               document");
                        Ok(fallback(&format!("url {url} rejected")))
                    }
                },
            }
        }
        Source::File(path) => match std::fs::read_to_string(path) {
            // A file that cannot be *read* is still the missing-registry case, not the wrong one.
            Err(e) => {
                warn!(path = %path.display(), error = %e,
                      "feed registry read failed; falling back to the built-in document");
                build(
                    BUILT_IN,
                    &format!("built-in (read of {} failed)", path.display()),
                )
            }
            Ok(text) => build(&text, &format!("file {}", path.display())),
        },
        Source::BuiltIn => load_built_in(),
    }
}

/// The built-in document, parsed. The fallback path and the one tests use.
pub fn load_built_in() -> Result<Loaded, RegistryError> {
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
fn build(text: &str, origin: &str) -> Result<Loaded, RegistryError> {
    let doc: Document = serde_json::from_str(text).map_err(RegistryError::Parse)?;
    if doc.version != SUPPORTED_VERSION {
        return Err(RegistryError::UnsupportedVersion(doc.version));
    }
    if doc.feeds.is_empty() {
        return Err(RegistryError::NoFeeds);
    }
    report_unknown_keys(&doc);

    let mut rows = Vec::with_capacity(doc.feeds.len());
    for row in &doc.feeds {
        rows.push(feed_from(row)?);
    }
    check_cross_row_invariants(&rows)?;

    Ok(Loaded {
        rows: Box::leak(rows.into_boxed_slice()),
        origin: origin.to_string(),
        version: doc.version,
    })
}

/// Walk the document warning about every key the schema did not claim.
fn report_unknown_keys(doc: &Document) {
    warn_unknown("$", &doc.unknown);
    for (i, row) in doc.feeds.iter().enumerate() {
        let at = format!("$.feeds[{i}]");
        warn_unknown(&at, &row.unknown);
        match &row.publishers {
            Publishers::Explicit(blocks) => {
                for (j, b) in blocks.iter().enumerate() {
                    warn_unknown(&format!("{at}.publishers.explicit[{j}]"), &b.unknown);
                }
            }
            Publishers::Derived(d) => {
                warn_unknown(&format!("{at}.publishers.derived"), &d.unknown);
                warn_unknown(&format!("{at}.publishers.derived.ports"), &d.ports.unknown);
            }
        }
    }
}

/// The invariants that span rows, and so cannot be checked while building one.
///
/// These were previously asserted only by `#[cfg(test)]` tests over the built-in document. That was
/// sufficient while the registry was compiled in — a violation could not reach a running process
/// without failing the build first — and stopped being sufficient the moment a document could be
/// supplied at runtime, because a supplied document that violates any of them loads and runs.
/// Checking them here puts the built-in document through the same path an operator's document takes;
/// the tests that pin them still pass, now as coverage of this code rather than as a parallel
/// assertion.
/// How many ports one publisher of this protocol binds.
///
/// A real property of the protocols rather than a convention: market-by-order and market-by-price
/// recover their books from an **in-band snapshot stream** on a dedicated port, and top-of-book and
/// midpoint have no such stream — which is why the `5xxxx` slot is left unallocated for them rather
/// than reused, so the leading digit keeps naming the traffic class.
fn planes_for(kind: FeedKind) -> u8 {
    match kind {
        FeedKind::TopOfBook | FeedKind::Midpoint => 2,
        FeedKind::MarketByOrder | FeedKind::MarketByPrice => 3,
    }
}

fn check_cross_row_invariants(rows: &[Feed]) -> Result<(), RegistryError> {
    let mut triples = std::collections::HashSet::new();
    let mut modes: std::collections::HashMap<&str, ArbitrationMode> = std::collections::HashMap::new();
    let mut group_ports = std::collections::HashSet::new();

    for f in rows {
        // `(venue, category, kind)` is the identity of a row. A duplicate is silently dropped by
        // `main`'s feed selection, so the second row's publishers would never be bound.
        if !triples.insert((f.venue, f.category, f.kind)) {
            return Err(RegistryError::DuplicateRow {
                venue: f.venue.to_string(),
                category: f.category.to_string(),
                kind: f.kind.label(),
            });
        }

        // Scoped per **venue**, not per `(venue, category)`: the arbiter's mode map is keyed by
        // venue alone (`Arbiter::set_mode`), so two rows that disagree resolve last-write-wins and
        // the venue's arbitration then depends on document order. Enforcing it at the granularity
        // the consumer actually uses is what makes that unreachable.
        match modes.entry(f.venue) {
            std::collections::hash_map::Entry::Occupied(e) if *e.get() != f.arbitration => {
                return Err(RegistryError::ArbitrationDisagreement {
                    venue: f.venue.to_string(),
                })
            }
            std::collections::hash_map::Entry::Occupied(_) => {}
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(f.arbitration);
            }
        }

        // `emit_trades` is a capability claim the reconciler's ranking has to agree with, in both
        // directions: claiming trades on a kind the ranking never admits mutes a venue that has no
        // other claimant, and not claiming them on a rankable kind gets the row handed the tape and
        // printing anyway — the double print the one-emitter invariant exists to prevent.
        if f.emit_trades != crate::ingest::reconcile::tape_rank_is_some(f.kind) {
            return Err(RegistryError::EmitTradesDisagrees {
                venue: f.venue.to_string(),
                category: f.category.to_string(),
                kind: f.kind.label(),
            });
        }

        let expected_planes = planes_for(f.kind);
        for p in f.publishers {
            // The plane count is a property of the protocol, so a block that does not match it is a
            // wrong block. This is also what keeps a *typo* in an optional key from failing silently:
            // with no `deny_unknown_fields`, a misspelled `snapshot` is absorbed into the unknown map
            // and the block quietly becomes two-port — a market-by-price row that then binds no
            // snapshot socket, never syncs its book, and serves nothing while reading healthy. That
            // is the exact silent failure this registry exists to eliminate, so it dies at startup.
            let found = if p.ports.snapshot().is_some() { 3 } else { 2 };
            if found != expected_planes {
                return Err(RegistryError::PortShape {
                    venue: f.venue.to_string(),
                    category: f.category.to_string(),
                    kind: f.kind.label(),
                    base_port: p.base_port(),
                    expected: expected_planes,
                    found,
                });
            }

            // Two sockets on one `(group, port)` land in the same `SO_REUSEPORT` set, so the kernel
            // splits that group's datagrams arbitrarily between them: each receiver sees a random
            // subset of publishers, duplicating reference data and scrambling per-publisher metrics.
            let mut ports = vec![p.ports.mktdata(), p.ports.refdata()];
            if let Some(s) = p.ports.snapshot() {
                ports.push(s);
            }
            for port in ports {
                if !group_ports.insert((f.group, port)) {
                    return Err(RegistryError::DuplicateGroupPort {
                        venue: f.venue.to_string(),
                        group: f.group,
                        port,
                    });
                }
            }
        }
    }
    Ok(())
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
/// Deduped rather than rejected on repeat: the same id reaching the expander twice would produce two
/// publishers on one port block, and the roster is a human-edited list where a singleton overlapping
/// a range is a plausible edit, not a corrupt document. The dedup is **reported** rather than
/// swallowed — silently accepting it would hide the one case where it is not benign, an author who
/// wrote the id twice meaning two different channels and got one.
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
    let listed = ids.len();
    ids.dedup();
    if ids.len() != listed {
        warn!(
            venue = row.venue,
            category = row.category,
            listed,
            distinct = ids.len(),
            "feed registry: channel roster repeats ids; the duplicates were collapsed"
        );
    }
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn doc_with(feeds: &str) -> String {
        format!(r#"{{"version":1,"feeds":[{feeds}]}}"#)
    }

    const SPORTS_ROW: &str = r#"{
        "venue":"KALSHI","category":"sports","code":"c","kind":"MarketByPrice",
        "group":"233.84.178.20","emit_trades":true,"arbitration":"Sticky",
        "publishers":{"derived":{
            "channels":[{"range":[10,12]},{"id":49}],
            "ports":{"mktdata":33000,"refdata":43000,"snapshot":53000}}}}"#;

    /// A second row on the same venue, disjoint in every dimension the invariants key on.
    const PERPS_ROW: &str = r#"{
        "venue":"KALSHI","category":"perps","code":"d","kind":"TopOfBook",
        "group":"233.84.178.3","emit_trades":true,"arbitration":"Sticky",
        "publishers":{"explicit":[{"mktdata":7576,"refdata":7577}]}}"#;

    /// Serve one fixed body over HTTP on an ephemeral port, forever. Returns the URL.
    ///
    /// Hand-rolled rather than pulling in a test HTTP server: the loader only ever issues a plain
    /// GET and reads the body, so a fixed response is a complete oracle for it.
    async fn serve(body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        format!("http://{addr}/registry.json")
    }

    fn built_in_row_count() -> usize {
        load_built_in().unwrap().rows.len()
    }

    /// The fallback actually happened — not merely "no error", which an empty registry would also
    /// satisfy. Both halves matter: the origin says built-in, and the rows *are* the built-in rows.
    fn assert_fell_back(loaded: &Loaded) {
        assert!(
            loaded.origin().starts_with("built-in"),
            "origin does not name the built-in document: {}",
            loaded.origin()
        );
        assert_eq!(
            loaded.rows.len(),
            built_in_row_count(),
            "fell back to something that is not the built-in registry"
        );
    }

    #[test]
    fn built_in_document_loads() {
        let loaded = load_built_in().expect("built-in document is valid");
        assert!(!loaded.rows.is_empty());
    }

    /// Ranges and singletons flatten to one ascending, deduped list, and every plane is offset by
    /// the id — the property the whole derived form exists for.
    #[test]
    fn derived_rows_expand_to_base_plus_id() {
        let loaded = build(&doc_with(SPORTS_ROW), "test").unwrap();
        let ports: Vec<(u16, u16, Option<u16>)> = loaded.rows[0]
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
    ///
    /// On a **top-of-book** row, because that is the protocol the two-plane shape is legal for — the
    /// derived form has to support it for the sibling row that arrives with its own publisher.
    #[test]
    fn a_derived_row_without_a_snapshot_base_binds_two_ports() {
        let row = SPORTS_ROW
            .replace(r#","snapshot":53000"#, "")
            .replace(r#""kind":"MarketByPrice""#, r#""kind":"TopOfBook""#);
        let loaded = build(&doc_with(&row), "test").unwrap();
        assert_eq!(loaded.rows[0].publishers[0].ports.snapshot(), None);
        assert_eq!(loaded.rows[0].publishers.len(), 4, "the row still expanded");
    }

    // ---------------------------------------------------------------------------------------
    // Forward compatibility: an additive upstream change must not stop an older binary
    // ---------------------------------------------------------------------------------------

    /// A key this build does not know is **ignored and reported**, never rejected. The registry is
    /// infrastructure that moves underneath a running fleet, so a publisher adding a field must not
    /// take every process down at its next restart. Same rule `doublezero-edge/src/types.rs` states.
    #[test]
    fn an_unrecognised_key_is_ignored_at_every_level() {
        let row = SPORTS_ROW
            .replace(r#""venue":"KALSHI""#, r#""venue":"KALSHI","future_row_field":1"#)
            .replace(
                r#""ports":{"mktdata":33000"#,
                r#""ports":{"future_port_field":true,"mktdata":33000"#,
            );
        let doc = doc_with(&row).replace(r#""version":1"#, r#""version":1,"future_doc_field":[]"#);
        let loaded = build(&doc, "test").expect("an additive change must still load");
        assert_eq!(loaded.rows.len(), 1);
        assert_eq!(loaded.rows[0].publishers.len(), 4, "the row still expanded");
    }

    /// The one thing `deny_unknown_fields` was buying stays bought a different way: a *required*
    /// field that is missing (because it was misspelled) is still fatal, so a typo cannot quietly
    /// default. `emit_trades` defaulting to false would be a muted tape with nothing to show for it.
    #[test]
    fn a_misspelled_required_field_is_still_fatal() {
        let row = SPORTS_ROW.replace(r#""emit_trades""#, r#""emit_trads""#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::Parse(_))
        ));
    }

    // ---------------------------------------------------------------------------------------
    // Where a rejected document is fatal, and where it degrades
    // ---------------------------------------------------------------------------------------

    /// A URL-sourced document that does not parse falls back rather than erroring. A remote registry
    /// is infrastructure; refusing here would crash-loop a fleet through the *success* path.
    #[tokio::test]
    async fn an_unparseable_url_document_falls_back() {
        let url = serve("this is not json").await;
        let loaded = load(Source::Url(url)).await.expect("fallback, not an error");
        assert_fell_back(&loaded);
    }

    /// Same for a `version` this build predates — otherwise the version gate is the identical trap
    /// one layer up, and no upstream change, additive or versioned, would be tolerable.
    #[tokio::test]
    async fn a_future_version_from_a_url_falls_back() {
        let url = serve(r#"{"version":99,"feeds":[]}"#).await;
        let loaded = load(Source::Url(url)).await.expect("fallback, not an error");
        assert_fell_back(&loaded);
    }

    /// And for a document that parses but violates an invariant.
    #[tokio::test]
    async fn an_invalid_url_document_falls_back() {
        let url = serve(r#"{"version":1,"feeds":[]}"#).await;
        let loaded = load(Source::Url(url)).await.expect("fallback, not an error");
        assert_fell_back(&loaded);
    }

    /// A URL that *is* good is used, so the fallback tests above are not passing vacuously.
    #[tokio::test]
    async fn a_valid_url_document_is_used() {
        let url = serve(
            r#"{"version":1,"feeds":[{
                "venue":"PHOENIX","category":"spot","code":"c","kind":"TopOfBook",
                "group":"233.84.178.18","emit_trades":true,"arbitration":"Coordinated",
                "publishers":{"explicit":[{"mktdata":9201,"refdata":9202}]}}]}"#,
        )
        .await;
        let loaded = load(Source::Url(url)).await.expect("valid document");
        assert!(loaded.origin().starts_with("url "), "{}", loaded.origin());
        assert_eq!(loaded.rows.len(), 1);
    }

    /// A bind-mounted file is an operator's explicit instruction about this one container, so a
    /// wrong one must **not** run — the asymmetry with the URL source is the whole point.
    #[tokio::test]
    async fn an_unparseable_file_document_is_fatal() {
        let path = std::env::temp_dir().join("dz-registry-bad.json");
        std::fs::write(&path, "this is not json").unwrap();
        let err = load(Source::File(path.clone())).await;
        std::fs::remove_file(&path).ok();
        assert!(matches!(err, Err(RegistryError::Parse(_))));
    }

    /// A file that cannot be *read* is the missing case, not the wrong one: warn and fall back.
    #[tokio::test]
    async fn an_unreadable_file_falls_back_to_the_built_in_document() {
        let loaded = load(Source::File(PathBuf::from("/nonexistent/registry.json")))
            .await
            .expect("fallback, not an error");
        assert_fell_back(&loaded);
    }

    /// The built-in copy failing to load is a build defect, so it stays fatal at `build`.
    #[test]
    fn an_unsupported_version_is_fatal_when_not_from_a_url() {
        let doc = doc_with(SPORTS_ROW).replace(r#""version":1"#, r#""version":2"#);
        assert!(matches!(
            build(&doc, "test"),
            Err(RegistryError::UnsupportedVersion(2))
        ));
    }

    // ---------------------------------------------------------------------------------------
    // Per-row validation
    // ---------------------------------------------------------------------------------------

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
        let loaded = build(&doc_with(&row), "test").unwrap();
        assert_eq!(loaded.rows[0].publishers.len(), 3);
    }

    #[test]
    fn a_document_with_no_feeds_is_fatal() {
        assert!(matches!(
            build(r#"{"version":1,"feeds":[]}"#, "test"),
            Err(RegistryError::NoFeeds)
        ));
    }

    // ---------------------------------------------------------------------------------------
    // Cross-row invariants
    //
    // Enforced here rather than only by tests over the built-in document: a *supplied* document
    // that violates any of these otherwise loads and runs, and each one fails silently downstream.
    // ---------------------------------------------------------------------------------------

    /// Two rows sharing `(venue, category, kind)`: feed selection dedups on that triple, so the
    /// second row's publishers would be dropped and never bound.
    #[test]
    fn a_duplicate_row_identity_is_fatal() {
        let twin = SPORTS_ROW
            .replace(r#""code":"c""#, r#""code":"other""#)
            .replace(r#""group":"233.84.178.20""#, r#""group":"233.84.178.21""#);
        assert!(matches!(
            build(&doc_with(&format!("{SPORTS_ROW},{twin}")), "test"),
            Err(RegistryError::DuplicateRow { .. })
        ));
    }

    /// A venue's rows must agree on arbitration: the arbiter's mode map is keyed by venue alone, so
    /// disagreement resolves last-write-wins and the venue's mode depends on document order.
    #[test]
    fn disagreeing_arbitration_within_a_venue_is_fatal() {
        let other = PERPS_ROW.replace(r#""arbitration":"Sticky""#, r#""arbitration":"Coordinated""#);
        assert!(matches!(
            build(&doc_with(&format!("{SPORTS_ROW},{other}")), "test"),
            Err(RegistryError::ArbitrationDisagreement { .. })
        ));
    }

    /// `emit_trades` must agree with the tape ranking in both directions. A depth-only kind claiming
    /// the tape has a flag stuck false and silently emits nothing.
    #[test]
    fn emit_trades_disagreeing_with_the_tape_ranking_is_fatal() {
        let row = SPORTS_ROW.replace(r#""kind":"MarketByPrice""#, r#""kind":"MarketByOrder""#);
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::EmitTradesDisagrees { .. })
        ));
    }

    /// Two receivers on one `(group, port)` land in the same `SO_REUSEPORT` set, so the kernel
    /// splits that group's datagrams arbitrarily between them.
    #[test]
    fn a_reused_group_port_across_rows_is_fatal() {
        let clash = PERPS_ROW
            .replace(r#""group":"233.84.178.3""#, r#""group":"233.84.178.20""#)
            .replace(r#"{"mktdata":7576,"refdata":7577}"#, r#"{"mktdata":33010,"refdata":7577}"#);
        assert!(matches!(
            build(&doc_with(&format!("{SPORTS_ROW},{clash}")), "test"),
            Err(RegistryError::DuplicateGroupPort { port: 33010, .. })
        ));
    }

    /// A book protocol without its snapshot port is refused at startup rather than binding a
    /// two-port block that never syncs.
    ///
    /// This is the structural close on the one sharp edge of dropping `deny_unknown_fields`: a
    /// *misspelled optional* key is absorbed into the unknown map and warned, so a typo'd `snapshot`
    /// would otherwise silently produce this exact block — a feed that connects, reads healthy and
    /// serves nothing, which is far harder to diagnose than a startup error.
    #[test]
    fn a_market_by_price_row_without_a_snapshot_port_is_fatal() {
        let row = SPORTS_ROW.replace(r#","snapshot":53000"#, "");
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::PortShape {
                kind: "mbp",
                expected: 3,
                found: 2,
                base_port: 33010,
                ..
            })
        ));
    }

    /// And the same typo the other way: a top-of-book row carries no in-band snapshot stream, so a
    /// third port would bind a socket the protocol never fills.
    #[test]
    fn a_top_of_book_row_with_a_snapshot_port_is_fatal() {
        let row = PERPS_ROW.replace(
            r#"{"mktdata":7576,"refdata":7577}"#,
            r#"{"mktdata":7576,"refdata":7577,"snapshot":7578}"#,
        );
        assert!(matches!(
            build(&doc_with(&row), "test"),
            Err(RegistryError::PortShape {
                kind: "tob",
                expected: 2,
                found: 3,
                ..
            })
        ));
    }

    /// The two rows above are otherwise legal together, so the four tests are not passing because
    /// any two-row document is rejected.
    #[test]
    fn two_disjoint_rows_on_one_venue_load() {
        let loaded = build(&doc_with(&format!("{SPORTS_ROW},{PERPS_ROW}")), "test").unwrap();
        assert_eq!(loaded.rows.len(), 2);
    }

    #[test]
    fn flags_resolve_in_precedence_order() {
        assert!(matches!(Source::from_flags("", ""), Source::BuiltIn));
        assert!(matches!(Source::from_flags("", "/p"), Source::File(_)));
        assert!(matches!(
            Source::from_flags("http://x", "/p"),
            Source::Url(_)
        ));
    }
}
