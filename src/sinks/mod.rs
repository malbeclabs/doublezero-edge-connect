//! Output sinks: consumers of the `FeedMessage` broadcast produced by `crate::ingest`. Each runs
//! off the hot path (a slow/failed sink can never stall ingest) and is independently enableable at
//! runtime:
//!   - [`ws`]     - WebSocket server (PROTOCOL.md wire contract); on by default, off if `--ws-bind` empty.
//!
//! Sink activation is uniform: a sink is active when its key config value is non-empty/present.
//! The WS sink just ships a non-empty default bind, so it is on unless explicitly cleared. A new
//! output feature is added here as a sibling module + a spawn in `main.rs`.

pub mod ws;
