//! Output sinks: consumers of the `FeedMessage` broadcast produced by `crate::ingest`. Each runs
//! off the hot path (a slow/failed sink can never stall ingest) and is independently enableable at
//! runtime:
//!   - [`ws`]      - WebSocket server (PROTOCOL.md wire contract); on by default, off if `--ws-bind` empty.
//!   - [`hyperliquid`] - the same data in **Hyperliquid's own schema** for an existing Hyperliquid
//!     client; off by default, on when `--hl-ws-bind` is set. A rendering, not a second protocol —
//!     see `docs/output-sinks.md`, never PROTOCOL.md.
//!   - [`metrics`] - Prometheus metrics HTTP endpoint; off by default, on when `--metrics-bind` is set.
//!
//! Sink activation is uniform: a sink is active when its key config value is non-empty/present.
//! The WS sink just ships a non-empty default bind, so it is on unless explicitly cleared; the
//! metrics endpoint ships an empty default, so it is off unless a bind is given. A new output
//! feature is added here as a sibling module + a spawn in `main.rs`.
//!
//! Note: [`metrics`] is the one "sink" that does not consume the broadcast — it serves the metric
//! registry on demand, off the hot path. It lives here because it is an independently-enableable
//! output feature wired the same way (sibling module + spawn in `main.rs`).

pub mod hyperliquid;
pub mod metrics;
pub mod ws;
