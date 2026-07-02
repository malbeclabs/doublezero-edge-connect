//! doublezero-edge-connect - DoubleZero Edge multicast -> normalized WebSocket bridge.
//!
//! Binds each configured DZ Edge feed's multicast group, decodes the binary Top-of-Book
//! frames, and re-serves normalized quotes over a WebSocket that any trading engine can
//! subscribe to. One feed maps to one venue (see `ingest/feeds.rs`); the bridge ingests every
//! selected feed at once and consumers filter by venue over the WebSocket (PROTOCOL.md).
//! Run it on a host connected to DZ Edge (the `doublezero1` interface) so consumers never
//! have to bind multicast themselves.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Result};
use clap::Parser;
use tokio::sync::broadcast;
use tracing::{info, warn};

use doublezero_edge_connect::{ingest, metrics, model, shred, sinks};
use ingest::{
    arbiter::{Arbiter, SharedArbiter, TRADE_DEDUP_WINDOW},
    feeds,
};

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

    /// Shred forwarder: opt-out kill switch. The forwarder is otherwise **automatic** — it
    /// activates whenever `doublezero multicast group list` reports an `edge-solana-*` group (which
    /// a mainnet access pass always makes discoverable), and forwards the shred firehose to
    /// `--shred-forward`. Set this to force it off regardless of discovery (e.g. when no consumer
    /// listens on the forward target). Default off: behaviour is unchanged unless you set it.
    #[arg(
        long = "shred-forward-disable",
        env = "DZ_SHRED_DISABLE",
        default_value_t = false
    )]
    shred_disable: bool,

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

    /// Shred forwarder: deduplication mode — the single selector for forwarder behaviour.
    /// `dedup` (default) forwards one copy of each shred with no sigverify or RPC; `sigverify`
    /// additionally ed25519-verifies that copy against its slot leader (and requires
    /// `--shred-rpc-url`); `none` forwards every datagram (duplicates and all).
    #[arg(
        long = "shred-dedup-mode",
        env = "DZ_SHRED_DEDUP_MODE",
        value_enum,
        default_value_t = shred::DedupMode::Dedup
    )]
    shred_dedup_mode: shred::DedupMode,

    /// Shred forwarder: Solana JSON-RPC endpoint for the leader schedule. Required (and consumed)
    /// only by `--shred-dedup-mode sigverify`; ignored (with a warning) in any other mode.
    #[arg(long = "shred-rpc-url", env = "DZ_SHRED_RPC_URL")]
    shred_rpc_url: Option<String>,

    /// Shred forwarder: dedup window depth in slots. Keys older than this many slots behind the tip
    /// are evicted, bounding memory. Used in `dedup` and `sigverify` modes.
    #[arg(
        long = "shred-dedup-window-slots",
        env = "DZ_SHRED_DEDUP_WINDOW_SLOTS",
        default_value_t = 512
    )]
    shred_dedup_window_slots: u64,

    /// Coins to subscribe on the Hyperliquid **public** WebSocket input feeder, repeatable/
    /// comma-separated (e.g. `--ws-input-coins BTC,ETH`). This is the backstop arbitrage source: it
    /// races the public feed against the DZ Edge multicast in the shared arbiter, so the edge wins in
    /// steady state and the public copy fills in only when the edge gaps. Empty (the default) leaves
    /// the feeder off.
    #[arg(long = "ws-input-coins", env = "WS_INPUT_COINS", value_delimiter = ',')]
    ws_input_coins: Vec<String>,

    /// URL for the public WS input feeder. Defaults to Hyperliquid's public endpoint; override to
    /// point the feeder at a local mock (e.g. in tests).
    #[arg(
        long = "ws-input-url",
        env = "WS_INPUT_URL",
        default_value = "wss://api.hyperliquid.xyz/ws"
    )]
    ws_input_url: String,

    /// Phoenix market symbols to back on the **public-API** trade feeder, repeatable/comma-separated
    /// (bare tickers, e.g. `--phoenix-ws-input-markets SOL,BTC`). Phoenix uses the same symbol on the
    /// edge and public feeds (edge `instrument_id == public assetId`), so these are both the public
    /// subscribe symbols and the edge symbols. This backstop races Phoenix's public trades against the
    /// DZ Edge Phoenix multicast in the shared arbiter (deduped on trade_id), so the edge wins in
    /// steady state and the public copy fills in only when the edge gaps. Trades only — no quote
    /// backstop. Empty (the default) leaves the feeder off.
    #[arg(
        long = "phoenix-ws-input-markets",
        env = "PHOENIX_WS_INPUT_MARKETS",
        value_delimiter = ','
    )]
    phoenix_ws_input_markets: Vec<String>,

    /// URL for the Phoenix public WS trade feeder. Defaults to Phoenix's public endpoint; override to
    /// point the feeder at a local mock (e.g. in tests).
    #[arg(
        long = "phoenix-ws-input-url",
        env = "PHOENIX_WS_INPUT_URL",
        default_value = "wss://perp-api.phoenix.trade/v1/ws"
    )]
    phoenix_ws_input_url: String,

    /// Prometheus metrics HTTP endpoint bind address (e.g. `127.0.0.1:9090`). Off by default
    /// (opt-in): empty means no endpoint is exposed. Metrics are recorded regardless; this only
    /// controls whether they can be scraped at `GET /metrics`. No TLS — terminate at a proxy.
    #[arg(long = "metrics-bind", env = "METRICS_BIND", default_value = "")]
    metrics_bind: String,

    /// How often (seconds) the subscription reconciler re-reads `doublezero status` and reconciles
    /// which market-data receivers, the WebSocket sink, and shred sources are active. Subscriptions
    /// change rarely, so the default is coarse.
    #[arg(
        long = "subscription-refresh-secs",
        env = "DZ_SUBSCRIPTION_REFRESH_SECS",
        default_value_t = 30
    )]
    subscription_refresh_secs: u64,

    /// Disable subscription-driven activation and force the static always-on model: run every
    /// selected feed's receiver + the WS sink (if `--ws-bind` is set) from startup, and resolve
    /// shred sources once. The same fallback kicks in automatically when the `doublezero` CLI is
    /// absent (running from source).
    #[arg(
        long = "subscription-gating-disable",
        env = "DZ_SUBSCRIPTION_GATING_DISABLE",
        default_value_t = false
    )]
    subscription_gating_disable: bool,
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
    // RUST_LOG, when set, is honored verbatim. Unset, we default to a quiet base of `warn`
    // (so noisy dependency chatter stays out of the container log, which the json-file driver
    // caps on disk) while keeping our own crate at `info` for startup/operational breadcrumbs.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("warn,doublezero_edge_connect=info")
            }),
        )
        .init();

    let args = Args::parse();
    info!(?args, "starting doublezero-edge-connect");

    let enabled = select_feeds(&args.feeds)?;
    info!(feeds = ?enabled.iter().map(|f| f.venue).collect::<Vec<_>>(), "ingesting feeds");

    // Force the metrics registry to initialize up front (registering the process collector and all
    // metric families) so the very first recorded sample lands in a ready registry, whether or not
    // the scrape endpoint below is enabled.
    metrics::metrics();

    let (tx, _rx) = broadcast::channel::<model::FeedMessage>(args.ws_broadcast_capacity);
    // The shared pre-broadcast arbiter: every ingest source (each multicast receiver and the WS
    // feeder) emits through this one instance, so cross-source duplicates collapse on one
    // per-(venue, symbol) floor before fan-out. Output sinks subscribe to `tx` directly.
    let instruments: model::InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let depth: model::DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let arbiter: SharedArbiter = {
        let mut a = Arbiter::new(tx.clone(), TRADE_DEDUP_WINDOW);
        // The arbiter updates the WS-replay depth map on each admitted (leader) depth, so a
        // reconnecting client replays the broadcast book, not a dropped non-leader's copy.
        a.set_depth_replay(depth.clone());
        Arc::new(Mutex::new(a))
    };

    // WebSocket sink config. The sink itself is activated by the subscription reconciler (below),
    // not here: it comes up only when a market-data feed is actually subscribed, and its listener is
    // bound non-fatally (a taken port disables the sink but never crash-loops the tunnel). An empty
    // `--ws-bind` disables it outright.
    if args.ws_bind.is_empty() {
        info!("WebSocket sink disabled (empty --ws-bind)");
    }
    let ws_cfg = sinks::ws::WsConfig {
        heartbeat: std::time::Duration::from_secs(args.ws_heartbeat_secs),
        idle_timeout: std::time::Duration::from_secs(args.ws_idle_timeout_secs),
        max_clients: args.ws_max_clients,
        max_subs: args.ws_max_subs,
        max_inbound_per_min: args.ws_max_inbound_per_min,
    };

    // Prometheus metrics endpoint: off by default (opt-in via `--metrics-bind`). Recording is always
    // on; this only exposes the registry over HTTP for scraping.
    let metrics_srv = if args.metrics_bind.is_empty() {
        info!("metrics endpoint disabled (empty --metrics-bind)");
        None
    } else {
        info!(bind = %args.metrics_bind, "metrics endpoint enabled");
        Some(tokio::spawn(sinks::metrics::run(args.metrics_bind.clone())))
    };

    // Shred-forwarder parameters. Sources are NOT resolved here anymore — the subscription
    // reconciler derives them (from the host's subscribed `edge-solana-*` groups, or an explicit
    // `--shred-source` override) and restarts the forwarder when they change. Validate the pieces up
    // front (pure parse, no I/O) so a bad `--shred-forward`/mode/window fails fast.
    let shred_forward = shred::parse_forwards(&args.shred_forward)?;
    let shred_explicit_sources = shred::parse_sources(&args.shred_sources)?;
    if !args.shred_disable {
        let mode = args.shred_dedup_mode;
        // The mode is the single source of truth: sigverify needs an RPC URL, and an RPC URL set in
        // any other mode is ignored (warn rather than silently promote — the user chose the mode).
        if mode == shred::DedupMode::Sigverify && args.shred_rpc_url.is_none() {
            bail!("--shred-dedup-mode sigverify requires --shred-rpc-url (DZ_SHRED_RPC_URL)");
        }
        if mode != shred::DedupMode::Sigverify && args.shred_rpc_url.is_some() {
            warn!(
                ?mode,
                "--shred-rpc-url is set but ignored (only --shred-dedup-mode sigverify uses it)"
            );
        }
        // A zero window evicts everything immediately, defeating dedup; reject it up front rather
        // than silently forwarding every duplicate.
        if mode != shred::DedupMode::None && args.shred_dedup_window_slots == 0 {
            bail!("--shred-dedup-window-slots must be > 0 unless --shred-dedup-mode is none");
        }
    }
    let shred_params = ingest::reconcile::ShredParams {
        disabled: args.shred_disable,
        explicit_sources: shred_explicit_sources,
        code_prefix: args.shred_code_prefix.clone(),
        port: args.shred_port,
        forward: shred_forward,
        mode: args.shred_dedup_mode,
        rpc_url: args.shred_rpc_url.clone(),
        dedup_window_slots: args.shred_dedup_window_slots,
    };

    // Public WS input feeder: off unless `--ws-input-coins` is non-empty (the source/sink activation
    // convention). It emits through the same shared arbiter as the multicast receivers, so the public
    // feed races the edge per (venue, symbol) tick and backstops it. Failure-isolated: it reconnects
    // internally and never returns, so its churn can't touch the multicast hot path.
    let ws_input = if args.ws_input_coins.is_empty() {
        info!("public WS input feeder disabled (no --ws-input-coins)");
        None
    } else {
        info!(coins = ?args.ws_input_coins, url = %args.ws_input_url,
              "starting public WS input feeder");
        Some(tokio::spawn(ingest::ws_feeder::run(
            args.ws_input_url.clone(),
            args.ws_input_coins.clone(),
            arbiter.clone(),
            instruments.clone(),
        )))
    };

    // Phoenix public-API trade feeder: off unless `--phoenix-ws-input-markets` is non-empty. Same
    // shape as the HL feeder — its own failure-isolated task emitting through the shared arbiter, so
    // public trades race the edge Phoenix multicast (deduped on trade_id) and backstop it.
    let phoenix_ws_input = if args.phoenix_ws_input_markets.is_empty() {
        info!("Phoenix public WS trade feeder disabled (no --phoenix-ws-input-markets)");
        None
    } else {
        info!(markets = ?args.phoenix_ws_input_markets, url = %args.phoenix_ws_input_url,
              "starting Phoenix public WS trade feeder");
        Some(tokio::spawn(ingest::phoenix_feeder::run(
            args.phoenix_ws_input_url.clone(),
            args.phoenix_ws_input_markets.clone(),
            arbiter.clone(),
            instruments.clone(),
        )))
    };

    // The subscription reconciler owns market-data receivers, the WebSocket sink, and the shred
    // forwarder: it polls `doublezero status` and activates/deactivates them as the host's
    // subscriptions change (default-on with fail-open; `--subscription-gating-disable` forces the
    // static always-on model). It loops forever, so its arm resolves only on a task panic.
    let reconciler = tokio::spawn(
        ingest::reconcile::Reconciler::new(ingest::reconcile::ReconcilerConfig {
            tx: tx.clone(),
            arbiter,
            instruments,
            depth,
            enabled,
            iface: args.iface.clone(),
            recv_buf: args.recv_buf,
            refresh: std::time::Duration::from_secs(args.subscription_refresh_secs),
            gating_disabled: args.subscription_gating_disable,
            ws_bind: args.ws_bind.clone(),
            ws_cfg,
            shred: shred_params,
        })
        .run(),
    );

    // The reconciler and the (independent, config-gated) public feeders + metrics endpoint all loop
    // forever; the process exits only if one of them panics or the metrics server fails to bind.
    tokio::select! {
        r = reconciler => r??,
        r = async { match ws_input {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r?,
        r = async { match phoenix_ws_input {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r?,
        // The metrics endpoint (when enabled) loops forever; its arm resolves only on a bind/accept
        // failure or a task panic.
        r = async { match metrics_srv {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r??,
    }
    Ok(())
}
