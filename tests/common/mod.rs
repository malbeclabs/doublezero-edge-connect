//! Shared E2E harness: replay golden frames into the bridge over loopback multicast,
//! collect its WebSocket output, and assert the protocol contract.
// A test binary that uses only part of this module (e.g. `dedup.rs` uses `assertions` but not
// `bridge`/`ws_client`) would otherwise trip `-Dwarnings` on the unused items.
#![allow(dead_code)]
pub mod assertions;
pub mod bridge;
pub mod replay;
pub mod ws_client;
pub mod ws_input;
