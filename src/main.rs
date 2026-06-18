//! doublezero-edge-connect - DoubleZero Edge multicast -> normalized WebSocket bridge.
//!
//! Binds each configured DZ Edge feed's multicast group, decodes the binary Top-of-Book
//! frames, and re-serves normalized quotes over a WebSocket that any trading engine can
//! subscribe to. One feed maps to one venue (see `ingest/feeds.rs`); the bridge ingests every
//! selected feed at once and consumers filter by venue over the WebSocket (PROTOCOL.md).
//! Run it on a host connected to DZ Edge (the `doublezero1` interface) so consumers never
//! have to bind multicast themselves.

mod ingest;
mod model;
mod shred;
mod sinks;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Result};
use clap::Parser;
use tokio::{sync::broadcast, task::JoinSet};
use tracing::{info, warn};

use ingest::feeds;

#[derive(Parser, Debug)]
#[command(
    name = "doublezero-edge-connect",
    about = "DoubleZero Edge multicast -> WebSocket bridge"
)]
struct Args {
    /// Venue(s) to ingest, by name, repeatable (e.g. `--feed Hyperliquid`). Each name must
    /// be a known feed (see `feeds.rs`). Omit to ingest ALL known feeds.
    #[arg(long = "feed", env = "DZ_FEEDS", value_delimiter = ',')]
    feeds: Vec<String>,

    /// Interface to join the groups on - a name (e.g. "doublezero1") or an IPv4 address.
    /// Names are resolved to their IPv4 (as in edge-multicast-ref).
    #[arg(long, env = "DZ_IFACE", default_value = "doublezero1")]
    iface: String,

    /// Kernel socket receive buffer (SO_RCVBUF) in bytes, per feed socket.
    #[arg(long, env = "DZ_RECV_BUF", default_value_t = 8_388_608)]
    recv_buf: usize,

    /// WebSocket server bind address for consumers to connect to. The WS sink is on by default;
    /// pass an empty value (`--ws-bind ""`) to disable it.
    #[arg(long, env = "WS_BIND", default_value = "0.0.0.0:8081")]
    ws_bind: String,

    /// Server heartbeat (WS Ping) interval in seconds.
    #[arg(long, env = "WS_HEARTBEAT_SECS", default_value_t = 20)]
    ws_heartbeat_secs: u64,

    /// Close a client that sends no frame (incl. auto-Pong) for this many seconds.
    #[arg(long, env = "WS_IDLE_TIMEOUT_SECS", default_value_t = 60)]
    ws_idle_timeout_secs: u64,

    /// Max concurrent WebSocket clients; new connections beyond are rejected.
    #[arg(long, env = "WS_MAX_CLIENTS", default_value_t = 64)]
    ws_max_clients: usize,

    /// Max subscriptions per client.
    #[arg(long, env = "WS_MAX_SUBS", default_value_t = 256)]
    ws_max_subs: usize,

    /// Max inbound (control) messages per client per minute before disconnect.
    #[arg(long, env = "WS_MAX_INBOUND_PER_MIN", default_value_t = 600)]
    ws_max_inbound_per_min: u32,

    /// Broadcast buffer capacity (backpressure: a slow client drops the oldest beyond this).
    #[arg(long, env = "WS_BROADCAST_CAPACITY", default_value_t = 4096)]
    ws_broadcast_capacity: usize,

    /// Shred forwarder: only join discovered multicast groups whose `code` starts with this
    /// prefix (`doublezero multicast group list`). Excludes unrelated groups (e.g. jito-shredstream).
    #[arg(long, env = "DZ_SHRED_CODE_PREFIX", default_value = "edge-solana-")]
    shred_code_prefix: String,

    /// Shred forwarder: UDP port the `edge-solana-*` groups publish on (all share one port).
    #[arg(long, env = "DZ_SHRED_PORT", default_value_t = 7733)]
    shred_port: u16,

    /// Shred forwarder: local destination(s) every shred datagram is fanned out to, repeatable
    /// (`host:port`). Defaults to the Jito shredstream-proxy local-listener convention.
    #[arg(
        long = "shred-forward",
        env = "DZ_SHRED_FORWARD",
        value_delimiter = ',',
        default_value = "127.0.0.1:20000"
    )]
    shred_forward: Vec<String>,

    /// Shred forwarder: explicit source group(s) `GROUP:PORT`, repeatable. Overrides discovery
    /// entirely (for tests/edge cases). When set, the shred forwarder runs even without the CLI.
    #[arg(long = "shred-source", env = "DZ_SHRED_SOURCES", value_delimiter = ',')]
    shred_sources: Vec<String>,
}

/// Resolve the `--feed` selection to a list of feeds: empty selection means all known feeds.
fn select_feeds(selection: &[String]) -> Result<Vec<&'static feeds::Feed>> {
    if selection.is_empty() {
        return Ok(feeds::FEEDS.iter().collect());
    }
    let mut chosen = Vec::new();
    for name in selection {
        let matches: Vec<&'static feeds::Feed> = feeds::FEEDS
            .iter()
            .filter(|f| f.venue.eq_ignore_ascii_case(name))
            .collect();
        if matches.is_empty() {
            let known: Vec<&str> = feeds::FEEDS.iter().map(|f| f.venue).collect();
            bail!("unknown feed '{name}'; known feeds: {}", known.join(", "));
        }
        chosen.extend(matches);
    }
    Ok(chosen)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let args = Args::parse();
    info!(?args, "starting doublezero-edge-connect");

    let enabled = select_feeds(&args.feeds)?;
    info!(feeds = ?enabled.iter().map(|f| f.venue).collect::<Vec<_>>(), "ingesting feeds");

    let (tx, _rx) = broadcast::channel::<model::FeedMessage>(args.ws_broadcast_capacity);
    let instruments: model::InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let depth: model::DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));

    // WebSocket sink: on by default; disable it by passing an empty `--ws-bind`.
    let ws = if args.ws_bind.is_empty() {
        info!("WebSocket sink disabled (empty --ws-bind)");
        None
    } else {
        let ws_cfg = sinks::ws::WsConfig {
            heartbeat: std::time::Duration::from_secs(args.ws_heartbeat_secs),
            idle_timeout: std::time::Duration::from_secs(args.ws_idle_timeout_secs),
            max_clients: args.ws_max_clients,
            max_subs: args.ws_max_subs,
            max_inbound_per_min: args.ws_max_inbound_per_min,
        };
        Some(tokio::spawn(sinks::ws::run(
            args.ws_bind.clone(),
            tx.clone(),
            instruments.clone(),
            depth.clone(),
            ws_cfg,
        )))
    };

    // Shred forwarder: activate-on-discovery. Runs iff an explicit `--shred-source` is given or
    // discovery finds ≥1 `edge-solana-*` group; otherwise it stays off (no enable flag). Invalid
    // source/destination addresses fail fast here, before anything is spawned.
    let shred_sources = shred::resolve_sources(
        &args.shred_sources,
        &args.shred_code_prefix,
        args.shred_port,
    )?;
    let shred = if shred_sources.is_empty() {
        info!("shred forwarder disabled (no --shred-source and discovery found no groups)");
        None
    } else {
        let shred_cfg = shred::ShredConfig {
            iface: args.iface.clone(),
            recv_buf: args.recv_buf,
            sources: shred_sources,
            forward: shred::parse_forwards(&args.shred_forward)?,
        };
        info!(sources = ?shred_cfg.sources, forward = ?shred_cfg.forward, "shred forwarder enabled");
        Some(tokio::spawn(shred::run(shred_cfg)))
    };

    // One receiver task per feed; all publish onto the shared broadcast tagged with the
    // feed's venue, and the WS server fans them out to consumers (who filter by venue).
    let mut receivers = JoinSet::new();
    for feed in enabled {
        info!(venue = feed.venue, kind = ?feed.kind, group = %feed.group,
              mktdata_port = feed.ports.mktdata(), refdata_port = feed.ports.refdata(),
              snapshot_port = ?feed.ports.snapshot(), "starting feed receiver");
        receivers.spawn(ingest::receiver::run_feed(
            *feed,
            args.iface.clone(),
            args.recv_buf,
            tx.clone(),
            instruments.clone(),
            depth.clone(),
        ));
    }

    // Exit if the WS server (when enabled) or any feed receiver returns (they loop forever). When
    // the WS sink is disabled, that arm is a never-resolving future so the process is driven by the
    // receivers alone.
    tokio::select! {
        r = async { match ws {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r??,
        // The shred forwarder is an optional add-on; a shred-side failure (e.g. the forwarder
        // failing to bind its send socket, or a task panic) must NOT take the market-data bridge
        // down with it. Log the outcome and degrade this arm to `pending()` so the rest of the
        // process keeps running. (Receiver bind failures are already retried, not propagated.)
        () = async { match shred {
            Some(handle) => {
                match handle.await {
                    Ok(Ok(())) => warn!("shred forwarder exited cleanly; market-data bridge continues"),
                    Ok(Err(e)) => warn!(error = %e, "shred forwarder failed; market-data bridge continues"),
                    Err(e) => warn!(error = %e, "shred forwarder task panicked; market-data bridge continues"),
                }
                std::future::pending::<()>().await
            }
            None => std::future::pending::<()>().await,
        } } => {},
        Some(r) = receivers.join_next() => r??,
    }
    Ok(())
}
