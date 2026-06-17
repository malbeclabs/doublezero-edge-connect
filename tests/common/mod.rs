//! Shared E2E harness: replay golden frames into the bridge over loopback multicast,
//! collect its WebSocket output, and assert the protocol contract.
pub mod assertions;
pub mod bridge;
pub mod replay;
pub mod ws_client;
