//! doublezero-edge-connect - DoubleZero Edge multicast -> normalized WebSocket bridge.
//!
//! Binds each configured DZ Edge feed's multicast group, decodes the binary Top-of-Book
//! datagrams, and re-serves normalized quotes over a WebSocket that any trading engine can
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

use doublezero_edge_connect::{history, ingest, metrics, model, shred, sinks};
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

    /// Publisher(s) to ingest within each selected feed, by **base port** — the market-data port of
    /// the publisher's block, repeatable (e.g. `--publisher-port 9201`). Each port must be the base
    /// port of some selected feed's publisher (see `feeds.rs`). Omit to ingest EVERY publisher of
    /// every selected feed. Use this to cap ingest cost (each publisher is a full receiver, and for
    /// MBO a full independent book) or to exclude a misbehaving publisher. Base ports are unique
    /// within a feed but **not across feeds** (two venues can both publish on 9201), so pair this
    /// with `--feed` to scope the narrowing to one venue.
    #[arg(
        long = "publisher-port",
        env = "DZ_PUBLISHER_PORTS",
        value_delimiter = ','
    )]
    publisher_ports: Vec<u16>,

    /// Channels to ingest, scoped per group code: `edge-kalshi-sports-mbp=10,11;edge-kalshi-perps-tob=2`. An unmentioned
    /// feed ingests every channel. Ids are the contract and are validated against the feed's
    /// roster at startup; channel *names* are not mirrored here — they live in the publisher's
    /// inventory by design and have already moved once. Use `doublezero-edge channels list` to
    /// see what a bound channel actually contains.
    ///
    /// Only feeds whose publisher derives a port per channel can be narrowed: there the excluded
    /// channel's socket is never bound and the kernel discards its traffic. Narrowing a feed whose
    /// publishers bind one base port flat is refused at startup — `channel_id` identifies mirrors
    /// there, not markets (see `ingest::channel_filter`).
    ///
    /// Validated against the whole registry, not against the `--feed`/`--publisher-port` selection:
    /// a clause naming a feed those already excluded is legal, filters nothing, and is warned about
    /// at startup.
    #[arg(long, env = "DZ_CHANNELS", default_value = "")]
    channels: String,

    /// URL to fetch the feed registry document from at startup. The precursor to reading it from
    /// the DoubleZero ledger. On failure the built-in document is used and a warning is logged — a
    /// container that will not boot because a registry host blipped is worse than one running a
    /// slightly stale document. A document that *parses* but fails validation is fatal.
    #[arg(long, env = "DZ_FEED_REGISTRY_URL", default_value = "")]
    feed_registry_url: String,

    /// Path to a feed registry document, for a bind-mounted file. Ignored when
    /// `--feed-registry-url` is set.
    #[arg(long, env = "DZ_FEED_REGISTRY", default_value = "")]
    feed_registry: String,

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

    /// Bind address for the Hyperliquid-compatible WebSocket sink, e.g. `0.0.0.0:8082`. Off by
    /// default (empty disables it). This serves Hyperliquid's own schema, not the protocol in
    /// PROTOCOL.md — point an existing Hyperliquid client's WebSocket endpoint at it. Not
    /// subscription-gated. No TLS; terminate at a proxy if exposed.
    #[arg(long = "hl-ws-bind", env = "HL_WS_BIND", default_value = "")]
    hl_ws_bind: String,

    /// Prometheus metrics HTTP endpoint bind address (e.g. `127.0.0.1:9090`). Off by default
    /// (opt-in): empty means no endpoint is exposed. Metrics are recorded regardless; this only
    /// controls whether they can be scraped at `GET /metrics`. No TLS — terminate at a proxy.
    #[arg(long = "metrics-bind", env = "METRICS_BIND", default_value = "")]
    metrics_bind: String,

    /// Query API bind address (`GET /v1/...`). Loopback by default: under host networking a wildcard
    /// bind is genuinely network-reachable and this API has no authentication. Empty disables it.
    #[arg(
        long = "api-bind",
        env = "DZ_API_BIND",
        default_value = "127.0.0.1:9099"
    )]
    api_bind: String,

    /// Admin surface bind address (`GET`/`POST /admin/channels`) — the one mutation path in this
    /// crate: it lets `--channels`/`DZ_CHANNELS` be replaced at runtime. **On by default, at
    /// loopback** (`127.0.0.1:9098`): the exposure is accepted on the condition that the default
    /// never reaches past this host. Set empty to disable it outright. This surface carries **no
    /// authentication**, so — like `--api-bind` — under host networking a wildcard bind is genuinely
    /// network-reachable; if you override this, stay on loopback (e.g. `127.0.0.1:<port>`), never a
    /// bare wildcard. Loopback alone does not stop a web page open in a browser on this host from
    /// POSTing a form here, so `POST` also requires an `X-DZ-Admin-Request` header (any value — a
    /// form post cannot set it). Deliberately separate from `--api-bind`'s `/v1`, which must stay
    /// provably read-only.
    ///
    /// It also serves `GET /admin/diagnostics`, which is unauthenticated like the rest of this
    /// surface and reports this host's device/metro names, subscribed group codes and their
    /// multicast IPs, every configured bind, and the feed-registry URL. On the loopback default
    /// that is the same audience that could already run `doublezero status`; a non-loopback bind
    /// hands it to anyone who can reach the port.
    #[arg(
        long = "admin-bind",
        env = "DZ_ADMIN_BIND",
        default_value = "127.0.0.1:9098"
    )]
    admin_bind: String,

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

    /// Seconds between path re-election samples for `Sticky` venues. Longer holds a slower path
    /// longer; shorter risks flapping authority, which re-baselines every consumer's book.
    #[arg(
        long = "arb-sample-interval-secs",
        env = "DZ_ARB_SAMPLE_INTERVAL_SECS",
        default_value_t = ARB.sample_interval_ns / 1_000_000_000
    )]
    arb_sample_interval_secs: u64,

    /// Microseconds a challenger must beat the authoritative path by, on median, to take authority.
    #[arg(
        long = "arb-transfer-margin-us",
        env = "DZ_ARB_TRANSFER_MARGIN_US",
        default_value_t = ARB.transfer_margin_ns / 1_000
    )]
    arb_transfer_margin_us: u64,

    /// Fraction of a window's contested samples the challenger must also lead, 0.0-1.0.
    /// Independent of the margin, so a heavy tail alone cannot carry a transfer.
    #[arg(
        long = "arb-transfer-win-rate",
        env = "DZ_ARB_TRANSFER_WIN_RATE",
        default_value_t = ARB.transfer_win_rate,
        value_parser = parse_win_rate
    )]
    arb_transfer_win_rate: f64,

    /// Seconds of leader silence after which a healthy challenger takes authority. Measured
    /// venue-wide, against the leader's last message on any market — not per market, or a market
    /// quieter than this would hand authority back and forth on every update.
    #[arg(
        long = "arb-leader-timeout-secs",
        env = "DZ_ARB_LEADER_TIMEOUT_SECS",
        default_value_t = ARB.leader_timeout_ns / 1_000_000_000
    )]
    arb_leader_timeout_secs: u64,

    /// Matched cross-path samples a path needs in a window before its speed is judged at all. Below
    /// this the window is ignored, so a handful of lucky matches cannot move a venue.
    #[arg(
        long = "arb-min-window-samples",
        env = "DZ_ARB_MIN_WINDOW_SAMPLES",
        default_value_t = ARB.min_window_samples as u64,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    arb_min_window_samples: u64,

    /// Seconds a path's trade waits for the peer path's copy of the same print before it counts as
    /// unmatched. Must exceed the worst plausible inter-path lead and stay well under the interval
    /// between repeats of one identical trade.
    #[arg(
        long = "arb-match-window-secs",
        env = "DZ_ARB_MATCH_WINDOW_SECS",
        default_value_t = 5,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    arb_match_window_secs: u64,

    /// Milliseconds a delivered order-level book event is remembered so a slower publisher's copy is
    /// recognized as a duplicate. A removed order id is never re-added regardless (`ingest::book`),
    /// so this does not gate resurrection — but set below the paths' separation it turns a lagging
    /// copy of a partially-filled order into a false size disagreement, which costs a forced
    /// re-baseline and the batches withheld behind it. A per-market count cap of 1024 events bounds
    /// the reach independently, so values much above a second are inert on a busy market.
    #[arg(
        long = "arb-book-dedup-window-ms",
        env = "DZ_ARB_BOOK_DEDUP_WINDOW_MS",
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    arb_book_dedup_window_ms: u64,

    /// Seconds of venue time behind a channel's newest stamp an order-level book event may be and
    /// still be admitted — and, by the same value, how long a removed order is remembered so a
    /// lagging publisher's stale add for it can be refused. Set below the paths' worst separation and
    /// a returning link's backlog is published as live; set far above it and every removal inside the
    /// window is held (~4,000/s per publisher on the flagship channel, against a process-wide ceiling
    /// of 1,048,576 entries).
    #[arg(
        long = "arb-book-retention-secs",
        env = "DZ_ARB_BOOK_RETENTION_SECS",
        default_value_t = BOOK_GUARD.retention_ns / 1_000_000_000,
        value_parser = clap::value_parser!(u64).range(1..=86_400)
    )]
    arb_book_retention_secs: u64,

    /// Seconds a batch's venue stamp may be ahead of its channel's newest and still advance it. Past
    /// it the advance is refused but the batch is still served, so a single bad stamp cannot carry the
    /// frontier years forward and refuse every real event behind it. Must stay comfortably below
    /// `--arb-book-retention-secs`, or one accepted jump puts the whole channel outside the window.
    #[arg(
        long = "arb-book-ts-jump-secs",
        env = "DZ_ARB_BOOK_TS_JUMP_SECS",
        default_value_t = BOOK_GUARD.max_ts_jump_ns / 1_000_000_000,
        value_parser = clap::value_parser!(u64).range(1..=86_400)
    )]
    arb_book_ts_jump_secs: u64,

    /// Seconds a channel's newest venue stamp may fail to **move** before it is re-seated from the
    /// batches actually arriving. A real tradeoff: below the paths' worst separation (2.77 s measured
    /// at p99.99) it fires on ordinary jitter, and above it, it bounds both how long a stuck frontier
    /// grows the removed population unforgotten and how long a market whose only surviving path is
    /// behind that frontier can be dark.
    #[arg(
        long = "arb-book-reseat-secs",
        env = "DZ_ARB_BOOK_RESEAT_SECS",
        default_value_t = BOOK_GUARD.reseat_after_ns / 1_000_000_000,
        value_parser = clap::value_parser!(u64).range(1..=86_400)
    )]
    arb_book_reseat_secs: u64,
}

/// The single source of the `--arb-*` defaults, so the values a test-built arbiter arbitrates on and
/// the ones `--help` advertises cannot drift apart.
const ARB: ingest::authority::AuthorityConfig = ingest::authority::AuthorityConfig::DEFAULT;

/// The same, for the resurrection guard's venue-time tunables.
const BOOK_GUARD: ingest::arbiter::BookGuardConfig = ingest::arbiter::BookGuardConfig::DEFAULT;

/// A win rate outside `0.0..=1.0` silently disables one of the two transfer conditions (above 1.0
/// no challenger ever clears it, below 0.0 every one does), and `NaN` compares false against both.
/// Reject it at startup rather than shipping a knob that reads as set but does nothing.
fn parse_win_rate(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("`{s}` is not a number"))?;
    if (0.0..=1.0).contains(&v) {
        Ok(v)
    } else {
        Err(format!("`{s}` is outside 0.0-1.0"))
    }
}

/// Resolve the `--feed` selection to a list of feeds: empty selection means all known feeds.
fn select_feeds(selection: &[String]) -> Result<Vec<&'static feeds::Feed>> {
    if selection.is_empty() {
        return Ok(feeds::feeds().iter().collect());
    }
    let mut chosen = Vec::new();
    // Dedup on `(venue, category, kind)` — the identity of a FEED ROW, unique across the registry
    // (see `feeds::tests::venue_category_kind_triples_are_unique`), so a repeated `--feed` name
    // selects each row once. The category is load-bearing, not decoration: one venue can carry two
    // rows of the same kind on disjoint universes, and dedup on `(venue, kind)` would drop the
    // second of them silently. The reconciler's own key is finer (it adds the publisher);
    // narrowing the publisher set is `filter_publishers`'s job, not this function's.
    let mut seen = std::collections::HashSet::new();
    for name in selection {
        let matches: Vec<&'static feeds::Feed> = feeds::feeds()
            .iter()
            .filter(|f| f.venue.eq_ignore_ascii_case(name))
            .collect();
        if matches.is_empty() {
            let known: Vec<&str> = feeds::feeds().iter().map(|f| f.venue).collect();
            bail!("unknown feed '{name}'; known feeds: {}", known.join(", "));
        }
        for f in matches {
            if seen.insert((f.venue, f.category, f.kind)) {
                chosen.push(f);
            }
        }
    }
    Ok(chosen)
}

/// Narrow each feed's publisher list to the `selection` of base ports, dropping feeds left with no
/// publisher. An empty selection keeps every publisher of every feed. Errors if a port matches no
/// publisher of any selected feed, so a typo fails fast rather than silently ingesting nothing.
fn filter_publishers(
    feeds: Vec<&'static feeds::Feed>,
    selection: &[u16],
) -> Result<Vec<feeds::Feed>> {
    if selection.is_empty() {
        return Ok(feeds.into_iter().copied().collect());
    }
    let mut unmatched: std::collections::HashSet<u16> = selection.iter().copied().collect();
    let mut out = Vec::new();
    for f in feeds {
        // `publishers` is a `&'static` slice, so the narrowed set has to be leaked to keep the
        // `Feed`'s `&'static [FeedPublisher]` shape. This runs once at startup over a handful of
        // rows, never on the hot path.
        let kept: Vec<feeds::FeedPublisher> = f
            .publishers
            .iter()
            .filter(|p| {
                let hit = selection.contains(&p.base_port());
                if hit {
                    unmatched.remove(&p.base_port());
                }
                hit
            })
            .copied()
            .collect();
        if kept.is_empty() {
            continue;
        }
        out.push(feeds::Feed {
            publishers: Box::leak(kept.into_boxed_slice()),
            ..*f
        });
    }
    if !unmatched.is_empty() {
        let mut known: Vec<u16> = feeds::feeds()
            .iter()
            .flat_map(|f| f.publishers.iter().map(|p| p.base_port()))
            .collect();
        known.sort_unstable();
        known.dedup();
        let mut missing: Vec<u16> = unmatched.into_iter().collect();
        missing.sort_unstable();
        bail!(
            "base port(s) {} are not publishers of the selected feed(s); base ports across all \
             feeds: {}",
            join_ports(&missing),
            join_ports(&known)
        );
    }
    Ok(out)
}

fn join_ports(ports: &[u16]) -> String {
    ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Fail startup if the channel filter, applied to the already `--feed`/`--publisher-port`-narrowed
/// `enabled` set, admits zero publishers of some enabled feed.
///
/// `ChannelFilter::parse` validates a clause against the **whole** registry, so a spec valid there
/// can still cross with `--feed`/`--publisher-port`'s narrowing to leave a feed with nothing
/// admitted: e.g. `--publisher-port` keeps only one derived channel's publisher, and `--channels`
/// then names a different (individually valid) channel on that same code. The existing loop above
/// this call only warns when a clause's *code* is absent from `enabled` entirely — it says nothing
/// when the code is present but every one of its admitted ids was narrowed away by
/// `--publisher-port`. The result is a feed silently left with no publisher, which takes the WS
/// sink, query API and history feeder down with it if it was the only market-data feed running —
/// with no warning at all. A channel filter that silently admits nothing is worse than one that
/// refuses to start.
fn check_channel_filter_covers_enabled(
    enabled: &[feeds::Feed],
    filter: &ingest::channel_filter::ChannelFilter,
) -> Result<()> {
    for f in enabled {
        if filter.publishers_for(f).is_empty() {
            bail!(
                "channel filter admits no publisher of enabled feed {} ({}, code {}); \
                 --publisher-port and --channels together leave it with zero publishers - \
                 narrow one of them less aggressively",
                f.venue,
                f.category,
                f.code
            );
        }
    }
    Ok(())
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

    // Resolve the feed registry before anything reads it. The rows are data supplied to the
    // container, not compiled-in constants: which group carries which feed, on which ports, is the
    // publisher's to change and it must not need a rebuild here.
    //
    // A rejected document degrades or refuses depending on where it came from. A `--feed-registry-url`
    // failure of any kind — unreachable, malformed, a version this build predates, a validation error
    // — warns and falls back to the built-in copy, which is by construction last-known-good: a remote
    // registry is infrastructure that can move underneath a running fleet, and since this resolves
    // only at startup, refusing would not kill the fleet when the document changed but each process
    // at its next reschedule, far from the cause. A `--feed-registry` file is an operator's explicit
    // instruction about this one container, so a document that was read but rejected is fatal here.
    feeds::init(ingest::registry::Source::from_flags(
        &args.feed_registry_url,
        &args.feed_registry,
    ))
    .await?;

    let enabled = filter_publishers(select_feeds(&args.feeds)?, &args.publisher_ports)?;

    // The channel filter, parsed once here against the registry that was just resolved and fatal
    // on any error: a channel filter that silently filters nothing is worse than one that refuses
    // to start, since the symptom is a process quietly ingesting markets nobody asked for. It is
    // handed to the reconciler as an *input* to the desired receiver set — `reconcile` stays the
    // single activation authority.
    let filter = ingest::channel_filter::ChannelFilter::parse(&args.channels)?;
    if !filter.is_empty() {
        info!(channels = ?filter.summary(), "channel filter active (excluded channels bind no socket)");
        // The channel filter validates against the whole registry, but `--feed`/`--publisher-port`
        // narrow what this process runs. A clause naming a feed those already excluded is legal and
        // filters nothing — not fatal, since the operator gave two explicit instructions and the
        // narrower one simply wins, but it must not be silent: an unbound channel and an unbound
        // feed look identical from outside.
        for code in filter.codes() {
            if !enabled.iter().any(|f| f.code == code) {
                warn!(
                    code,
                    "channel filter names a group code that --feed/--publisher-port already \
                     excluded; the clause filters nothing"
                );
            }
        }
    }
    // A code present in `enabled` can still end up with zero admitted publishers once
    // `--publisher-port` and `--channels` are combined - the warn loop above only catches a code
    // absent entirely. Fatal, not a warning: see the function's docs.
    check_channel_filter_covers_enabled(&enabled, &filter)?;

    info!(
        feeds = ?enabled.iter().map(|f| (f.venue, f.kind.label(), filter.publishers_for(f).len())).collect::<Vec<_>>(),
        "ingesting feeds"
    );

    // Wrapped here (after the plain-value startup logging above, which wants a snapshot, not a
    // guard) so the reconciler and the admin surface (below) share one instance to swap at runtime:
    // a `POST /admin/channels` replaces its contents in place, and the reconciler reads a fresh
    // clone every tick (see `ingest::reconcile::Reconciler::filter`).
    let filter: Arc<Mutex<ingest::channel_filter::ChannelFilter>> = Arc::new(Mutex::new(filter));

    // Force the metrics registry to initialize up front (registering the process collector and all
    // metric families) so the very first recorded sample lands in a ready registry, whether or not
    // the scrape endpoint below is enabled.
    metrics::metrics();

    // The backbone carries `Arc<FeedMessage>`: a per-subscriber delivery is a refcount bump, not a
    // deep clone of the message's owned `String`/`Vec` fields (see `arbiter`/`sinks::ws`).
    let (tx, _rx) = broadcast::channel::<Arc<model::FeedMessage>>(args.ws_broadcast_capacity);
    // The shared pre-broadcast arbiter: every ingest source (each multicast receiver and the WS
    // feeder) emits through this one instance, so cross-source duplicates collapse on one
    // per-(venue, symbol) floor before fan-out. Output sinks subscribe to `tx` directly.
    let instruments: model::InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let depth: model::DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
    let books: model::BookSnapshot = Arc::new(Mutex::new(model::BookReplay::default()));
    // Rolling one-hour trade history behind the `/v1` query API. Built once here (like the three
    // snapshot maps above) so it survives the API sink's own activate/deactivate cycles - a
    // subscription blip that briefly takes the sink down must not reset the window it comes back up
    // to. The reconciler owns feeding it (only while the sink is up) and reading it (the API sink).
    let history: Arc<Mutex<history::Store>> = Arc::new(Mutex::new(history::Store::new()));
    // Single-path authority tunables for `Sticky` venues, plus the cross-path matcher's pairing window,
    // validated here at startup and handed to the arbiter.
    let authority_cfg = ingest::authority::AuthorityConfig {
        leader_timeout_ns: args.arb_leader_timeout_secs.saturating_mul(1_000_000_000),
        sample_interval_ns: args.arb_sample_interval_secs.saturating_mul(1_000_000_000),
        transfer_margin_ns: args.arb_transfer_margin_us.saturating_mul(1_000),
        transfer_win_rate: args.arb_transfer_win_rate,
        min_window_samples: args.arb_min_window_samples as usize,
    };
    // The resurrection guard's venue-time tunables. Validated by the type that owns the bounds, not
    // here: both ways of getting them wrong read as a channel-wide book outage with no metric saying
    // why, which is a typo with the same signature as an attack.
    let book_guard = ingest::arbiter::BookGuardConfig {
        retention_ns: args.arb_book_retention_secs * 1_000_000_000,
        max_ts_jump_ns: args.arb_book_ts_jump_secs * 1_000_000_000,
        reseat_after_ns: args.arb_book_reseat_secs * 1_000_000_000,
    };
    if let Err(why) = book_guard.validate() {
        bail!(why);
    }
    let arbiter: SharedArbiter = {
        let mut a = Arbiter::new(tx.clone(), TRADE_DEDUP_WINDOW);
        // Every registry venue, not just the selected ones: a message's venue comes from the wire
        // SourceID, so a venue can reach the arbiter without its own feed being ingested.
        for f in ingest::feeds::feeds() {
            a.set_mode(f.venue, f.arbitration);
        }
        // The arbiter updates the WS-replay depth map on each admitted (leader) depth, so a
        // reconnecting client replays the broadcast book, not a dropped non-leader's copy.
        a.set_depth_replay(depth.clone());
        a.set_book_replay(books.clone());
        a.set_authority(
            authority_cfg,
            args.arb_match_window_secs.saturating_mul(1_000_000_000),
        );
        a.set_book_dedup_window(args.arb_book_dedup_window_ms.saturating_mul(1_000_000));
        a.set_book_guard(book_guard);
        Arc::new(Mutex::new(a))
    };

    // The path sampler: closes each elapsed re-election window (a margin transfer moves venue
    // authority here), refreshes the O(markets × paths) gauges and drains the matcher's unmatched
    // counts. Off the emit path entirely, so a slow sweep never touches ingest latency.
    let sampler = {
        let arbiter = arbiter.clone();
        // A fraction of the window, not the window itself: the authority closes a window only once
        // `sample_interval_ns` has really elapsed, so ticking at exactly that period lets ordinary
        // scheduling jitter push a verdict a whole window late.
        let period = std::time::Duration::from_secs((args.arb_sample_interval_secs / 4).max(1));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                ingest::arbiter::lock(&arbiter).close_authority_windows();
            }
        })
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
        broadcast_capacity: args.ws_broadcast_capacity,
    };

    // Hyperliquid-compatible sink: off by default (opt-in via `--hl-ws-bind`), and not
    // subscription-gated — it is a rendering of whatever the shared broadcast carries, so it has no
    // group of its own to be subscribed to. A bind failure disables it with a warning rather than
    // taking the tunnel down, exactly as the normalized sink's does.
    let hl_sink = if args.hl_ws_bind.is_empty() {
        info!("Hyperliquid-compatible sink disabled (empty --hl-ws-bind)");
        None
    } else {
        match sinks::hyperliquid::bind(&args.hl_ws_bind).await {
            Ok(listener) => {
                let (tx, books) = (tx.clone(), books.clone());
                Some(tokio::spawn(sinks::hyperliquid::serve(listener, tx, books)))
            }
            Err(e) => {
                warn!(bind = %args.hl_ws_bind, "Hyperliquid-compatible sink disabled: {e}");
                None
            }
        }
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

    // Admin surface: the one mutation path in this crate, on by default at loopback
    // (`127.0.0.1:9098`) and disabled only if `--admin-bind` is set empty. Unlike the WS sink and
    // the query API, it is **not** subscription-gated — an operator must be able to inspect or
    // change the channel filter even with nothing currently subscribed — so it is spawned once
    // here, gated only on the bind being non-empty. A taken port is non-fatal, exactly like
    // `ws`/`api`: it disables this surface without taking the tunnel down.
    //
    // It is also where `GET /admin/diagnostics` lives, for the same reason: on a host whose tunnel
    // never came up, no market-data feed is subscribed, so `/v1` is not listening and this is the
    // only surface that can answer why (see `ingest::diagnostics`).
    let diagnostics: ingest::diagnostics::SharedDiagnostics =
        Arc::new(Mutex::new(ingest::diagnostics::DiagnosticsSnapshot {
            refresh_secs: args.subscription_refresh_secs,
            ..Default::default()
        }));
    let admin_srv = if args.admin_bind.is_empty() {
        info!("admin surface disabled (empty --admin-bind)");
        None
    } else {
        match sinks::admin::bind(&args.admin_bind).await {
            Ok(listener) => {
                info!(bind = %args.admin_bind, "admin surface enabled (mutating — no authentication)");
                Some(tokio::spawn(sinks::admin::serve(
                    listener,
                    sinks::admin::AdminConfig {
                        filter: filter.clone(),
                        enabled: enabled.clone(),
                        diagnostics: diagnostics.clone(),
                        binds: sinks::admin::Binds {
                            ws: args.ws_bind.clone(),
                            api: args.api_bind.clone(),
                            admin: args.admin_bind.clone(),
                            metrics: args.metrics_bind.clone(),
                        },
                    },
                )))
            }
            Err(e) => {
                warn!(bind = %args.admin_bind, %e,
                    "admin surface failed to bind (port in use?); staying off");
                None
            }
        }
    };

    // The subscription reconciler owns market-data receivers, the WebSocket sink, and the shred
    // forwarder: it polls `doublezero status` and activates/deactivates them as the host's
    // subscriptions change (default-on with fail-open; `--subscription-gating-disable` forces the
    // static always-on model). It loops forever, so its path resolves only on a task panic.
    let reconciler = tokio::spawn(
        ingest::reconcile::Reconciler::new(ingest::reconcile::ReconcilerConfig {
            tx: tx.clone(),
            arbiter,
            instruments,
            depth,
            books,
            enabled,
            filter,
            iface: args.iface.clone(),
            recv_buf: args.recv_buf,
            refresh: std::time::Duration::from_secs(args.subscription_refresh_secs),
            gating_disabled: args.subscription_gating_disable,
            ws_bind: args.ws_bind.clone(),
            ws_cfg,
            api_bind: args.api_bind.clone(),
            history,
            shred: shred_params,
            diagnostics,
        })
        .run(),
    );

    // The reconciler, the path sampler and the (independent, config-gated) public feeders + metrics
    // endpoint all loop forever; the process exits only if one of them panics or the metrics server
    // fails to bind.
    tokio::select! {
        r = reconciler => r??,
        r = sampler => r?,
        r = async { match ws_input {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r?,
        r = async { match phoenix_ws_input {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r?,
        r = async { match hl_sink {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r??,
        // The metrics endpoint (when enabled) loops forever; its path resolves only on a bind/accept
        // failure or a task panic.
        r = async { match metrics_srv {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r??,
        // The admin surface (when enabled) loops forever; its path resolves only on a task panic or a
        // fatal accept error (a bind failure was already handled non-fatally above).
        r = async { match admin_srv {
            Some(handle) => handle.await,
            None => std::future::pending().await,
        } } => r??,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `main` installs the registry before anything reads it; a unit test has no `main`, so it
    /// installs the built-in document itself. Idempotent, so every test can call it.
    fn registry() {
        feeds::init_built_in();
    }

    #[test]
    fn empty_selection_is_all_feeds() {
        registry();
        let all = select_feeds(&[]).unwrap();
        assert_eq!(all.len(), feeds::feeds().len());
    }

    /// The admin surface defaults on — but only at loopback, never a wildcard: that is the
    /// condition the exposure was accepted under. Same shape as `--api-bind`'s default now, unlike
    /// the old off-by-default policy this replaces.
    #[test]
    fn the_admin_surface_defaults_to_loopback_never_a_wildcard() {
        let bind = Args::parse_from(["x"]).admin_bind;
        assert!(
            bind.starts_with("127.0.0.1:"),
            "admin surface must default to loopback, got {bind:?}"
        );
        assert!(
            !bind.starts_with("0.0.0.0"),
            "admin surface must never default to a wildcard bind, got {bind:?}"
        );
    }

    // The identity that maps 1:1 to a spawned receiver (the reconciler's `FeedKey`). Includes the
    // category: a venue can carry two rows of one kind on disjoint universes, so `(venue, kind)`
    // would collapse them and hide exactly the dedup bug this checks for.
    fn keys(sel: &[&feeds::Feed]) -> Vec<(&'static str, &'static str, feeds::FeedKind)> {
        sel.iter().map(|f| (f.venue, f.category, f.kind)).collect()
    }

    /// Selecting a venue must select **every** row it owns, including two rows of one kind on
    /// disjoint universes — dedup on `(venue, kind)` would silently drop the second.
    #[test]
    fn a_venue_with_two_rows_of_one_kind_selects_both() {
        registry();
        let sel = select_feeds(&["KALSHI".to_string()]).unwrap();
        let mut mbp: Vec<&str> = sel
            .iter()
            .filter(|f| f.kind == feeds::FeedKind::MarketByPrice)
            .map(|f| f.category)
            .collect();
        mbp.sort_unstable();
        // The categories themselves, not a count: a count of 2 would also be satisfied by the same
        // universe selected twice, which is the opposite failure and equally wrong.
        assert_eq!(
            mbp,
            vec!["perps", "sports"],
            "both universes must be selected, each once"
        );
    }

    #[test]
    fn repeated_name_selects_same_as_single() {
        registry();
        let once = select_feeds(&["HYPERLIQUID".to_string()]).unwrap();
        let twice = select_feeds(&["HYPERLIQUID".to_string(), "HYPERLIQUID".to_string()]).unwrap();
        // Repeating a name must spawn the same receivers (same keys, same order) as passing it once.
        assert_eq!(keys(&once), keys(&twice));
        // Hyperliquid maps to >1 row (TOB + MBO), so this actually exercises multi-row dedup.
        assert!(once.len() > 1);
    }

    #[test]
    fn distinct_names_union_without_dup() {
        registry();
        let sel = select_feeds(&[
            "HYPERLIQUID".to_string(),
            "PHOENIX".to_string(),
            "HYPERLIQUID".to_string(),
        ])
        .unwrap();
        // Union of the two distinct venues' rows, each receiver once — no row spawned twice.
        let k = keys(&sel);
        let uniq: std::collections::HashSet<_> = k.iter().collect();
        assert_eq!(uniq.len(), k.len());
        // The repeated "HYPERLIQUID" added nothing beyond the first: selecting both venues equals
        // selecting each once.
        assert_eq!(
            k,
            keys(&select_feeds(&["HYPERLIQUID".to_string(), "PHOENIX".to_string()]).unwrap())
        );
    }

    #[test]
    fn unknown_name_still_errors() {
        registry();
        assert!(select_feeds(&["Nope".to_string()]).is_err());
    }

    #[test]
    fn empty_publisher_selection_keeps_every_publisher() {
        registry();
        let all = filter_publishers(select_feeds(&[]).unwrap(), &[]).unwrap();
        let hl_tob = all
            .iter()
            .find(|f| f.venue == "HYPERLIQUID" && f.kind == feeds::FeedKind::TopOfBook)
            .unwrap();
        // Compare against the registry, not a literal: this test is about the empty selection
        // being a no-op, and a hardcoded count silently turns it into a fleet-size assertion that
        // has to be edited every time a publisher is onboarded (`feeds.rs` already pins the set).
        let registry = feeds::feeds()
            .iter()
            .find(|f| f.venue == "HYPERLIQUID" && f.kind == feeds::FeedKind::TopOfBook)
            .unwrap();
        assert_eq!(hl_tob.publishers.len(), registry.publishers.len());
    }

    #[test]
    fn publisher_selection_narrows_by_base_port() {
        registry();
        let sel = filter_publishers(select_feeds(&[]).unwrap(), &[9201, 9401]).unwrap();
        let hl_tob = sel
            .iter()
            .find(|f| f.venue == "HYPERLIQUID" && f.kind == feeds::FeedKind::TopOfBook)
            .unwrap();
        let ports: Vec<u16> = hl_tob.publishers.iter().map(|p| p.base_port()).collect();
        assert_eq!(ports, vec![9201, 9401]);
    }

    /// A feed left with no matching publisher drops out entirely rather than running with zero
    /// publishers (9401 is a Hyperliquid-only block; Phoenix publishes on 9201).
    #[test]
    fn feeds_without_a_matching_base_port_drop_out() {
        registry();
        let sel = filter_publishers(select_feeds(&[]).unwrap(), &[9401]).unwrap();
        assert!(!sel.iter().any(|f| f.venue == "PHOENIX"));
        assert!(sel.iter().any(|f| f.venue == "HYPERLIQUID"));
    }

    /// Base ports are unique **within** a feed, not across feeds: 9201 is both a Hyperliquid TOB
    /// block and Phoenix's only block, so selecting it keeps a publisher on each. Scoping to one
    /// venue is `--feed`'s job.
    #[test]
    fn base_ports_are_not_unique_across_feeds() {
        registry();
        let sel = filter_publishers(select_feeds(&[]).unwrap(), &[9201]).unwrap();
        let venues: std::collections::HashSet<&str> = sel.iter().map(|f| f.venue).collect();
        assert!(venues.contains("HYPERLIQUID"));
        assert!(venues.contains("PHOENIX"));
        assert!(sel
            .iter()
            .all(|f| f.publishers.iter().all(|p| p.base_port() == 9201)));

        let scoped =
            filter_publishers(select_feeds(&["PHOENIX".to_string()]).unwrap(), &[9201]).unwrap();
        assert!(scoped.iter().all(|f| f.venue == "PHOENIX"));
    }

    #[test]
    fn unknown_publisher_base_port_is_an_error() {
        registry();
        assert!(filter_publishers(select_feeds(&[]).unwrap(), &[1234]).is_err());
    }

    /// The regression this pins: a `--publisher-port` + `--channels` combination that empties an
    /// enabled feed while both, taken alone, are valid against the whole registry. `--publisher-port`
    /// keeps only the sports (`edge-kalshi-sports-mbp`) feed's channel-10 publisher; `--channels edge-kalshi-sports-mbp=11` is a
    /// perfectly valid clause against the full 31-channel roster, but channel 11's publisher was
    /// never in the narrowed `enabled` set to begin with, so it admits nothing.
    #[test]
    fn a_publisher_port_and_channel_filter_combo_that_empties_a_feed_is_refused() {
        registry();
        let sports = feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row");
        let chan10_port = sports
            .publishers
            .iter()
            .find(|p| p.channel == Some(10))
            .expect("channel 10 is in the sports roster")
            .base_port();

        let enabled = filter_publishers(
            select_feeds(&[sports.venue.to_string()]).unwrap(),
            &[chan10_port],
        )
        .unwrap();
        assert_eq!(
            enabled.len(),
            1,
            "only the sports feed's channel-10 publisher should remain"
        );

        // Individually valid against the whole registry (11 is in the sports roster), but it names
        // a channel that --publisher-port already excluded.
        let filter =
            ingest::channel_filter::ChannelFilter::parse("edge-kalshi-sports-mbp=11").unwrap();

        let err = check_channel_filter_covers_enabled(&enabled, &filter)
            .expect_err("a feed left with zero admitted publishers must be refused");
        let msg = err.to_string();
        assert!(msg.contains("edge-kalshi-sports-mbp"), "{msg}");
    }

    /// The same combination when the filter still admits the surviving publisher must pass.
    #[test]
    fn a_channel_filter_that_still_admits_the_narrowed_publisher_is_fine() {
        registry();
        let sports = feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row");
        let chan10_port = sports
            .publishers
            .iter()
            .find(|p| p.channel == Some(10))
            .expect("channel 10 is in the sports roster")
            .base_port();

        let enabled = filter_publishers(
            select_feeds(&[sports.venue.to_string()]).unwrap(),
            &[chan10_port],
        )
        .unwrap();

        let filter =
            ingest::channel_filter::ChannelFilter::parse("edge-kalshi-sports-mbp=10").unwrap();
        assert!(check_channel_filter_covers_enabled(&enabled, &filter).is_ok());
    }
}
