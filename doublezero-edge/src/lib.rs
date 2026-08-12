//! `doublezero-edge`: an agent-facing, read-only CLI over a running `doublezero-edge-connect`
//! container's `/v1` HTTP market-data API. See `main.rs` for the command surface and
//! `sinks/api.rs` in the bridge crate for the API this talks to.
//!
//! This is a library target purely so the integration tests under `tests/` can exercise each
//! module directly (golden-fixture rendering, `--jq`, `key==value` parsing) without shelling out
//! to the built binary for everything — `main.rs` stays a thin wrapper over these modules.

pub mod channels;
pub mod client;
pub mod diagnose;
pub mod endpoint;
pub mod jq;
pub mod params;
pub mod render;
pub mod types;
