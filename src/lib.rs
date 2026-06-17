//! Library surface for `doublezero-edge-connect`.
//!
//! The binary (`src/main.rs`) is a thin wrapper over this library. Exposing the ingest pipeline,
//! wire model, and output sinks as a lib lets dev tooling and integration tests reuse the codecs
//! and types directly — e.g. `examples/pcap2frames.rs`, which decodes captured frames through the
//! same `ingest::codec` the bridge uses (so the converter doubles as a codec-offset validator
//! against the live feed).

pub mod ingest;
pub mod model;
pub mod sinks;
