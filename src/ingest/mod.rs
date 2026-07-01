//! Multicast ingest pipeline (always on): bind each selected feed's DZ Edge multicast group,
//! decode the binary edge-feed-spec frames (Top-of-Book, Midpoint, Market-by-Order), drive the
//! reference-data state machine, and produce normalized `FeedMessage`s onto the shared broadcast
//! that the output sinks (`crate::sinks`) consume. This half has no knowledge of how the data is
//! re-served.

pub mod arbiter;
pub mod book;
pub mod codec;
pub mod codec_common;
pub mod codec_mbo;
pub mod codec_midpoint;
pub mod feeds;
pub mod phoenix_feeder;
pub mod processor;
pub mod public_feeder;
pub mod receiver;
pub mod reconcile;
pub mod subscriber;
pub mod subscriptions;
pub mod ws_feeder;
