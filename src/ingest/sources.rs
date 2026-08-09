//! Transcription of the upstream source registry
//! (`edge-feed-spec/sources/spec.md`) — the canonical `Source ID` allocation.
//!
//! A Source ID identifies the source whose order book a price was derived from. IDs are stable and
//! are never reused. This module is the **only** place the registry is mirrored; add a row here when
//! upstream assigns a new production ID (1-1023).

use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

/// Map a wire `Source ID` to its registered source name.
///
/// Returns `None` only for IDs with no registry row. The wire value is authoritative: a publisher
/// stamping the wrong ID is a publisher defect and is reported as-is, never substituted.
pub fn source_name(source_id: u16) -> Option<&'static str> {
    match source_id {
        1 => Some("Hyperliquid"),
        2 => Some("Phoenix"),
        3 => Some("Lashay"),
        _ => None,
    }
}

/// Map a registry source *name* back to its `Source ID`.
///
/// Unlike [`source_name`], this covers every assigned row including the one whose ID is ambiguous on
/// the wire: names are unique in the registry even where an ID is overloaded. This is what lets a
/// resolved source carry a numeric identity a consumer can join against the registry.
pub fn source_id_of(source: &str) -> Option<u16> {
    match source {
        "Hyperliquid" => Some(1),
        "Phoenix" => Some(2),
        "Lashay" => Some(3),
        _ => None,
    }
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

/// The label to stamp as `source`/`venue` for a Source ID. Total — `venue` is never blank.
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

/// The short, CLI-facing name for a source — the registry's `Short Name` column.
///
/// Defaults to the uppercase of the registry name, which is correct for every row whose short name
/// is just its name. Add an explicit arm here for any row where upstream assigns something else.
pub fn short_name(source: &str) -> String {
    #[allow(clippy::match_single_binding)]
    match source {
        // Explicit overrides from the registry's Short Name column go here.
        _ => source.to_uppercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_ids_map_to_their_names() {
        assert_eq!(source_name(1), Some("Hyperliquid"));
        assert_eq!(source_name(2), Some("Phoenix"));
    }

    /// The registry assigns this ID, so it resolves. That two publishers currently stamp it is a
    /// publisher defect, fixed at the publisher; this crate reports what the wire says.
    #[test]
    fn every_assigned_id_resolves() {
        assert_eq!(source_name(1), Some("Hyperliquid"));
        assert_eq!(source_name(2), Some("Phoenix"));
        assert_eq!(source_name(3), Some("Lashay"));
    }

    /// A label is always available, so `venue` is never blank. Registered ids only, so this
    /// exercises the real `source_label` entry point without touching the process-global
    /// unregistered-id map (see `label_in` below for that logic, tested against injected state).
    #[test]
    fn assigned_ids_label_as_their_name() {
        assert_eq!(source_label(1), "Hyperliquid");
        assert_eq!(source_label(3), "Lashay");
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
        assert_eq!(source_id_of("Hyperliquid"), Some(1));
        assert_eq!(source_id_of("Phoenix"), Some(2));
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

    /// The default short name is the uppercase of the registry name; an explicit row overrides it.
    #[test]
    fn short_name_defaults_to_uppercase() {
        assert_eq!(short_name("Hyperliquid"), "HYPERLIQUID");
        assert_eq!(short_name("Phoenix"), "PHOENIX");
    }

    #[test]
    fn short_name_handles_an_unregistered_source() {
        assert_eq!(short_name("Nonesuch"), "NONESUCH");
    }
}
