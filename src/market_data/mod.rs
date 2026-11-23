//! Response semantics for an upcoming public market-data quote server: freshness, cache policy,
//! the Problem Details catalog, origin headers and near-match instrument hints.
//!
//! Pure functions only. Nothing calls them yet; the server that renders responses with them
//! arrives separately and is off by default.

pub mod near_match;
pub mod response;
