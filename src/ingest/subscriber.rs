//! Reference-data subscriber state machine (DoubleZero Edge reference-data supplement).
//!
//! Collects instrument definitions tagged with the latest `ManifestSummary` sequence,
//! decides `ready()`, and resets on a channel reset.
//!
//! The state machine is identical across the edge-feed-spec protocols (Top-of-Book, Midpoint,
//! Market-by-Order), which differ only in their instrument-definition *layout*, so it is generic
//! over any definition type that implements [`InstrumentDef`] (its key + manifest sequence).

use std::collections::HashMap;

/// The two fields the reference-data state machine needs from any protocol's instrument
/// definition: its numeric key, and the manifest sequence it was published under. The processors
/// read the rest (symbol, exponents) from the concrete type.
pub trait InstrumentDef {
    fn id(&self) -> u32;
    fn manifest_seq(&self) -> u16;
}

/// Wraparound-safe u16 ordering: true if `b` is after `a`.
pub fn is_later(b: u16, a: u16) -> bool {
    let diff = b.wrapping_sub(a);
    diff != 0 && diff < 32768
}

pub struct RefDataState<D> {
    pub valid: bool,
    pub latest_seq: u16,
    pub expected_count: u32,
    pub last_reset_count: u8,
    pub defs: HashMap<u32, D>,
}

impl<D: InstrumentDef> Default for RefDataState<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: InstrumentDef> RefDataState<D> {
    pub fn new() -> Self {
        // Not `#[derive(Default)]`: that would impose `D: Default`, which the definition types
        // don't (and needn't) implement - only the `HashMap` needs defaulting.
        Self {
            valid: false,
            latest_seq: 0,
            expected_count: 0,
            last_reset_count: 0,
            defs: HashMap::new(),
        }
    }

    pub fn on_frame(&mut self, reset_count: u8) {
        if reset_count != self.last_reset_count {
            self.valid = false;
            self.latest_seq = 0;
            self.expected_count = 0;
            self.last_reset_count = reset_count;
            self.defs.clear();
        }
    }

    pub fn on_manifest(&mut self, valid: bool, manifest_seq: u16, instrument_count: u32) {
        if !valid {
            self.valid = false;
            self.latest_seq = 0;
            self.expected_count = 0;
            self.defs.clear();
            return;
        }
        if !self.valid || is_later(manifest_seq, self.latest_seq) {
            self.valid = true;
            self.latest_seq = manifest_seq;
            self.expected_count = instrument_count;
            self.defs.clear();
        }
    }

    pub fn on_instrument_definition(&mut self, def: D) {
        if self.valid && def.manifest_seq() == self.latest_seq {
            self.defs.insert(def.id(), def);
        }
    }

    /// True once the *whole* instrument set for the current manifest epoch is known. Quote
    /// emission no longer gates on this (it gates per instrument via [`Self::definition`]); kept
    /// as the documented "full set complete" invariant and exercised by the tests.
    #[allow(dead_code)]
    pub fn ready(&self) -> bool {
        self.valid && self.defs.len() as u32 == self.expected_count
    }

    pub fn definition(&self, instrument_id: u32) -> Option<&D> {
        self.defs.get(&instrument_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::codec::InstrumentDefinition;

    fn defn(iid: u32, seq: u16) -> InstrumentDefinition {
        InstrumentDefinition {
            instrument_id: iid,
            symbol: format!("SYM{iid}"),
            price_exponent: -2,
            qty_exponent: -2,
            manifest_seq: seq,
        }
    }

    #[test]
    fn ready_after_all_defs() {
        let mut s = RefDataState::new();
        s.on_manifest(true, 1, 2);
        assert!(!s.ready());
        s.on_instrument_definition(defn(10, 1));
        s.on_instrument_definition(defn(20, 1));
        assert!(s.ready());
    }

    #[test]
    fn definition_resolves_per_instrument_before_full_set() {
        // The quote path gates on `definition(id)`, not `ready()`: a known instrument's
        // definition must resolve even while the set is incomplete, so its quotes flow without
        // waiting for the rest of the burst (the fragility that wedged the live Phoenix feed).
        let mut s = RefDataState::new();
        s.on_manifest(true, 1, 3); // expect 3, only one has arrived
        s.on_instrument_definition(defn(10, 1));
        assert!(!s.ready(), "set is incomplete");
        assert!(
            s.definition(10).is_some(),
            "known instrument resolves despite !ready()"
        );
        assert!(s.definition(20).is_none(), "unknown instrument still drops");
    }

    #[test]
    fn reset_clears_state() {
        let mut s = RefDataState::new();
        s.on_manifest(true, 1, 1);
        s.on_instrument_definition(defn(10, 1));
        assert!(s.ready());
        s.on_frame(1);
        assert!(!s.valid);
    }

    #[test]
    fn is_later_wraps() {
        assert!(is_later(0, 65535));
        assert!(!is_later(65535, 0));
        assert!(!is_later(4, 4));
    }
}
