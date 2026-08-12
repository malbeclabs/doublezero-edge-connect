//! Output sinks: consumers of the `FeedMessage` broadcast produced by `crate::ingest`. Each runs
//! off the hot path (a slow/failed sink can never stall ingest) and is independently enableable at
//! runtime:
//!   - [`ws`]      - WebSocket server (PROTOCOL.md wire contract); on by default, off if `--ws-bind` empty.
//!   - [`metrics`] - Prometheus metrics HTTP endpoint; off by default, on when `--metrics-bind` is set.
//!   - [`api`]     - read-only `/v1` JSON query API over the rolling history window + snapshots.
//!   - [`admin`]   - the one mutation path (runtime channel-filter changes); on by default, at
//!     loopback (`--admin-bind`/`DZ_ADMIN_BIND` default `127.0.0.1:9098`), off when set empty.
//!     Deliberately separate from [`api`] so `/v1` stays provably read-only.
//!
//! Sink activation is uniform: a sink is active when its key config value is non-empty/present.
//! The WS sink just ships a non-empty default bind, so it is on unless explicitly cleared; the
//! metrics/admin endpoints ship an empty default, so they are off unless a bind is given. A new
//! output feature is added here as a sibling module + a spawn in `main.rs`.
//!
//! Note: [`metrics`], [`api`] and [`admin`] are the "sinks" that do not consume the broadcast — each
//! serves (or, for `admin` alone, mutates) already-computed/shared state on demand, off the hot
//! path. They live here because they are independently-enableable output features wired the same
//! way (sibling module + spawn in `main.rs`).
//!
//! [`http`] is not a sink either — it is the hand-rolled HTTP/1.1 scaffolding (accept loop, request
//! parsing, response writing) that [`metrics`], [`api`], [`admin`] and any other on-demand HTTP sink
//! build their handler on top of, kept here so the crate has exactly one such responder rather than
//! one per sink.

pub mod admin;
pub mod api;
pub mod http;
pub mod metrics;
pub mod ws;
