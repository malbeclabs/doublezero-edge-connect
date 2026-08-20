//! The `Source ID` -> registry-name mirror.
//!
//! `edge-feed-spec/sources/spec.md` is the sole authority for this allocation; nothing here decides
//! it. A Source ID identifies the source whose order book a price was derived from, IDs are stable
//! and are never reused, and the wire value is authoritative — a publisher stamping the wrong ID is
//! a publisher defect fixed at the publisher, reported here as-is and never substituted.
//!
//! The mapping arrives with the **feed registry document** (`registry.rs`'s optional `sources`
//! block), so assigning a venue is a document republish rather than a code change and a release.
//! [`BUILT_IN`] is the compiled-in fallback for a document that carries no block — which is not
//! hypothetical, since adding the block bumps no schema version and an older document is legal.

use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

/// One `Source ID` -> registry-name assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceAssignment {
    pub id: u16,
    pub name: &'static str,
}

/// The compiled-in mirror, used when the resolved document carries no `sources` block.
const BUILT_IN: [SourceAssignment; 3] = [
    SourceAssignment {
        id: 1,
        name: "HYPERLIQUID",
    },
    SourceAssignment {
        id: 2,
        name: "PHOENIX",
    },
    SourceAssignment {
        id: 3,
        name: KALSHI,
    },
];

/// The assignments in force, installed once by `ingest::feeds::init` alongside the feed rows.
static INSTALLED: OnceLock<&'static [SourceAssignment]> = OnceLock::new();

/// Install the document's assignments. Called once from `feeds::init`, before any receiver spawns;
/// a repeat call is ignored for the same reason the feed rows' is — a venue name that changed under
/// a running receiver would leave books and reference data keyed to a mapping no longer in effect.
pub fn install(assignments: &'static [SourceAssignment]) -> bool {
    INSTALLED.set(assignments).is_ok()
}

/// The assignments to resolve against: the document's if it carried a block, else [`BUILT_IN`].
pub fn assignments() -> &'static [SourceAssignment] {
    INSTALLED.get().copied().unwrap_or(&BUILT_IN)
}

/// Map a wire `Source ID` to its registered source name.
///
/// Returns `None` only for IDs the document assigns no row. Names are **uppercase**, which is the
/// form that reaches consumers: this is what `venue`/`source_name` carry on the WebSocket and what every
/// `venue=` metric label holds, so it is also the form a product identifier like `HYPERLIQUID:BTC`
/// composes from.
pub fn source_name(source_id: u16) -> Option<&'static str> {
    assignments()
        .iter()
        .find(|a| a.id == source_id)
        .map(|a| a.name)
}

/// Source ID 3's registry name. The pre-launch codename it used to also answer to is gone: the
/// ledger re-registered the groups under their `edge-kalshi-*` codes, so nothing feeds the old name
/// in on input either.
const KALSHI: &str = "KALSHI";

/// Map a registry source *name* back to its `Source ID`, exactly — the inverse of [`source_name`].
///
/// This is what lets a resolved source carry a numeric identity a consumer can join against the
/// registry, and what `receiver::record_revealed` tests a wire venue against before recording it.
pub fn source_id_of(source: &str) -> Option<u16> {
    assignments()
        .iter()
        .find(|a| a.name == source)
        .map(|a| a.id)
}

/// Cap on distinct synthesized labels for unregistered Source IDs. Bounded like every other
/// per-key map in this crate so a garbage or hostile wire cannot grow it without limit.
pub const MAX_UNREGISTERED_SOURCES: usize = 64;

/// Which branch [`label_in`] took, so the caller owns the metrics.
pub(crate) enum LabelOutcome {
    Existing,
    New,
    Capped,
}

/// Resolve or synthesize a label within `map`. Pure with respect to the caller's state — the
/// process-global map lives in `source_label`, so tests can exercise the cap in isolation
/// instead of racing each other through one shared static.
pub(crate) fn label_in(
    map: &mut HashMap<u16, &'static str>,
    source_id: u16,
) -> (&'static str, LabelOutcome) {
    if let Some(label) = map.get(&source_id) {
        return (label, LabelOutcome::Existing);
    }
    if map.len() >= MAX_UNREGISTERED_SOURCES {
        return ("UNREGISTERED", LabelOutcome::Capped);
    }
    let leaked: &'static str = Box::leak(format!("SOURCE_{source_id}").into_boxed_str());
    map.insert(source_id, leaked);
    (leaked, LabelOutcome::New)
}

/// The label to stamp as `source_name`/`venue` for a Source ID. Total — `venue` is never blank.
///
/// A registered ID yields its registry name. An unregistered one yields a stable synthesized
/// `SOURCE_<id>`, distinct per ID: the arbiter keys dedup on `(venue, symbol)`, so collapsing
/// distinct unregistered sources into one label would merge unrelated markets. Past
/// [`MAX_UNREGISTERED_SOURCES`] distinct IDs, everything shares `UNREGISTERED`.
///
/// Labels are leaked deliberately — `venue` is `&'static str` throughout the ingest hot path, and
/// the leak is bounded by the cap.
pub fn source_label(source_id: u16) -> &'static str {
    if let Some(name) = source_name(source_id) {
        return name;
    }
    static UNREGISTERED: OnceLock<RwLock<HashMap<u16, &'static str>>> = OnceLock::new();
    let map = UNREGISTERED.get_or_init(|| RwLock::new(HashMap::new()));
    if let Some(label) = map
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&source_id)
    {
        return label;
    }
    let mut w = map.write().unwrap_or_else(|e| e.into_inner());
    if let Some(label) = w.get(&source_id) {
        return label;
    }
    let (label, outcome) = label_in(&mut w, source_id);
    match outcome {
        LabelOutcome::Existing => {}
        LabelOutcome::New => crate::metrics::metrics().unregistered_sources.inc(),
        LabelOutcome::Capped => crate::metrics::metrics()
            .unregistered_source_labels_capped
            .inc(),
    }
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_ids_map_to_their_names() {
        assert_eq!(source_name(1), Some("HYPERLIQUID"));
        assert_eq!(source_name(2), Some("PHOENIX"));
    }

    /// All three assigned production IDs resolve. The wire value is authoritative — a publisher
    /// stamping the wrong ID is a publisher defect, fixed at the publisher; this crate reports what
    /// the wire says and never substitutes a row's own venue for it.
    #[test]
    fn every_assigned_id_resolves() {
        assert_eq!(source_name(1), Some("HYPERLIQUID"));
        assert_eq!(source_name(2), Some("PHOENIX"));
        assert_eq!(source_name(3), Some(KALSHI));
    }

    /// Registry names are uppercase, so a consumer composing `SOURCE:SYMBOL` never has to case-fold
    /// what the wire gave it.
    #[test]
    fn registry_names_are_uppercase() {
        for id in 0u16..1024 {
            if let Some(name) = source_name(id) {
                assert_eq!(name, name.to_uppercase(), "id {id} is not uppercase");
            }
        }
    }

    /// A label is always available, so `venue` is never blank. Registered ids only, so this
    /// exercises the real `source_label` entry point without touching the process-global
    /// unregistered-id map (see `label_in` below for that logic, tested against injected state).
    #[test]
    fn assigned_ids_label_as_their_name() {
        assert_eq!(source_label(1), "HYPERLIQUID");
        assert_eq!(source_label(3), KALSHI);
    }

    /// The pre-launch codename resolves to nothing now that the ledger no longer serves it, so a
    /// document or operator string still carrying it is refused rather than silently accepted under
    /// a second name for the same ID.
    #[test]
    fn only_the_registry_name_resolves_to_id_3() {
        assert_eq!(source_id_of(KALSHI), Some(3));
        assert_eq!(source_id_of("LASHAY"), None);
    }

    /// The public entry point's unregistered branch: read-lock miss, write-lock re-check,
    /// delegation, and the counter wiring. `label_in`'s own tests use local maps, so without this
    /// the whole locking wrapper is uncovered. Deliberately does NOT loop toward the cap (that is
    /// exactly what caused the original race) and uses an id no other test touches, so this is the
    /// only test in the suite that contacts the global map and cannot race `label_in`'s tests.
    #[test]
    fn source_label_synthesizes_and_memoizes_an_unregistered_id() {
        let first = source_label(54321);
        assert_eq!(first, "SOURCE_54321");
        assert_eq!(
            source_label(54321),
            first,
            "second call must return the same leaked label"
        );
    }

    /// Distinct unregistered ids must get distinct labels: the arbiter keys dedup on
    /// `(venue, symbol)`, so a shared label would merge unrelated markets into one bucket.
    /// Drives `label_in` with its own `HashMap` rather than `source_label`'s process-global
    /// static, so this can't race another test filling the shared cap (#write-up in review).
    #[test]
    fn unregistered_ids_get_distinct_stable_labels() {
        let mut map = HashMap::new();
        let (a, outcome_a) = label_in(&mut map, 900);
        assert_eq!(a, "SOURCE_900");
        assert!(matches!(outcome_a, LabelOutcome::New));
        let (b, outcome_b) = label_in(&mut map, 901);
        assert_ne!(a, b);
        assert!(matches!(outcome_b, LabelOutcome::New));
        let (again, outcome_again) = label_in(&mut map, 900);
        assert_eq!(again, a, "same id must return the same label every time");
        assert!(matches!(outcome_again, LabelOutcome::Existing));
    }

    /// Past the cap they share one label — bounded state, like every other per-key map here. Own
    /// `HashMap` for the same reason as above: filling it to the cap must not affect any other
    /// test's view of the (otherwise shared) unregistered-id map.
    #[test]
    fn unregistered_labels_are_bounded() {
        let mut map = HashMap::new();
        let (first, _) = label_in(&mut map, 1000);
        for id in 1001..(1000 + MAX_UNREGISTERED_SOURCES as u16) {
            label_in(&mut map, id);
        }
        assert_eq!(map.len(), MAX_UNREGISTERED_SOURCES);
        let (over_cap, outcome) = label_in(&mut map, 60000);
        assert_eq!(over_cap, "UNREGISTERED");
        assert!(matches!(outcome, LabelOutcome::Capped));
        // An id assigned before the cap keeps its own label rather than collapsing.
        let (still_first, outcome_first) = label_in(&mut map, 1000);
        assert_eq!(still_first, first);
        assert!(matches!(outcome_first, LabelOutcome::Existing));
    }

    #[test]
    fn unassigned_and_reserved_ids_are_unmapped() {
        assert_eq!(source_name(0), None);
        assert_eq!(source_name(9999), None);
    }

    #[test]
    fn names_map_back_to_their_registry_ids() {
        assert_eq!(source_id_of("HYPERLIQUID"), Some(1));
        assert_eq!(source_id_of("PHOENIX"), Some(2));
        assert_eq!(source_id_of("Nonesuch"), None);
    }

    /// Every name the forward table yields must round-trip back to the same id, or the two tables
    /// have drifted apart.
    #[test]
    fn forward_and_reverse_tables_agree() {
        for id in 0u16..1024 {
            if let Some(name) = source_name(id) {
                assert_eq!(
                    source_id_of(name),
                    Some(id),
                    "id {id} ({name}) does not round-trip"
                );
            }
        }
    }
}
