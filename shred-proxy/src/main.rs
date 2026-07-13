//! shred-proxy — receives shreds from DoubleZero's `edge-solana-*` multicast feeds and forwards them
//! deduplicated to a local UDP port.
//!
//! It detects which multicast groups are active by reading the kernel routing table (a group is
//! active if the kernel routes it via the DoubleZero interface), joins only those, and forwards a
//! single copy of each shred (dedup) to the local destination. Meant to run on a host connected to
//! DoubleZero (the `doublezero1` interface) so the local consumer (e.g. Jito shredstream-proxy)
//! doesn't have to bind multicast itself.
//!
//! The forwarder, parser, dedup window and multicast plumbing are reused directly from the
//! `doublezero-edge-connect` library; this binary only adds routing-table detection, the reconciler,
//! and the CLI.

mod net;
mod reconcile;

use std::{io::IsTerminal, net::Ipv4Addr, time::Duration};

use anyhow::{bail, Context, Result};
use clap::Parser;
use doublezero_edge_connect::shred::{self, DedupMode};
use tracing::{info, warn};

/// Default candidate multicast IPs: the edge-solana feeds (shreds + regional retransmitters). The
/// proxy probes the routing table and joins only the ones that are active.
const DEFAULT_CANDIDATES: &str =
    "233.84.178.1,233.84.178.16,233.84.178.14,233.84.178.13,233.84.178.12";

#[derive(Parser, Debug)]
#[command(
    name = "shred-proxy",
    about = "Receives DoubleZero multicast shreds (active groups detected via the routing table) and forwards them deduplicated to a local port"
)]
struct Args {
    /// Candidate multicast IP(s) to probe in the routing table, repeatable/comma-separated. The
    /// proxy joins only those the kernel routes via the DoubleZero interface (--iface).
    #[arg(
        long = "candidate-group",
        env = "DZ_CANDIDATE_GROUPS",
        value_delimiter = ',',
        default_value = DEFAULT_CANDIDATES
    )]
    candidate_groups: Vec<Ipv4Addr>,

    /// UDP port the edge-solana groups publish on (all share one).
    #[arg(long, env = "DZ_PORT", default_value_t = 7733)]
    port: u16,

    /// Explicit source(s) `GROUP:PORT`, repeatable. When set, skips routing-table detection and runs
    /// over this fixed set (tests / manual pin).
    #[arg(long = "source", env = "DZ_SOURCES", value_delimiter = ',')]
    sources: Vec<String>,

    /// Local destination(s) every shred is fanned out to, repeatable (`host:port`).
    #[arg(
        long = "forward",
        env = "DZ_FORWARD",
        value_delimiter = ',',
        default_value = "127.0.0.1:8001"
    )]
    forward: Vec<String>,

    /// Interface to join the groups on — a name (e.g. "doublezero1") or an IPv4. The name is also
    /// what gets matched in the routing-table detection.
    #[arg(long, env = "DZ_IFACE", default_value = "doublezero1")]
    iface: String,

    /// Kernel socket receive buffer (SO_RCVBUF) in bytes, per receiver socket.
    #[arg(long, env = "DZ_RECV_BUF", default_value_t = 8_388_608)]
    recv_buf: usize,

    /// Deduplication mode. `dedup` (default) forwards one copy of each shred; `sigverify` also
    /// verifies that one copy against its slot leader (requires --rpc-url); `none` forwards every
    /// datagram (duplicates included).
    #[arg(
        long = "dedup-mode",
        env = "DZ_DEDUP_MODE",
        value_enum,
        default_value_t = DedupMode::Dedup
    )]
    dedup_mode: DedupMode,

    /// Solana JSON-RPC endpoint used to fetch the leader schedule. Required by (and only used with)
    /// `--dedup-mode sigverify`.
    #[arg(long = "rpc-url", env = "DZ_RPC_URL")]
    rpc_url: Option<String>,

    /// Dedup window depth in slots. Keys older than this many slots behind the tip are evicted,
    /// bounding memory.
    #[arg(
        long = "dedup-window-slots",
        env = "DZ_DEDUP_WINDOW_SLOTS",
        default_value_t = 512
    )]
    dedup_window_slots: u64,

    /// Interval (seconds) to re-probe the routing table for changes in the active groups. Ignored
    /// when explicit sources (--source) are passed.
    #[arg(long = "refresh-secs", env = "DZ_REFRESH_SECS", default_value_t = 30)]
    refresh_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // RUST_LOG, when set, is honored verbatim. Unset, we default to a quiet base of `warn` with this
    // binary AND the reused forwarder library (which logs under `doublezero_edge_connect`) at `info`
    // for startup/operational breadcrumbs.
    //
    // ANSI colours only when stdout is a real terminal: under systemd/journald (a background service)
    // stdout is a pipe, and colour escapes would land in the journal as literal `\x1b[..` garbage.
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "warn,shred_proxy=info,doublezero_edge_connect=info",
                )
            }),
        )
        .init();

    let args = Args::parse();
    info!(?args, "starting shred-proxy");

    // Validate/parse everything up front (pure parse, no I/O) so a bad flag fails fast.
    let forward = shred::parse_forwards(&args.forward)?;
    if forward.is_empty() {
        bail!("--forward cannot be empty: there is nowhere to forward the shreds");
    }
    let explicit_sources = shred::parse_sources(&args.sources)?;
    // The mode is the single source of truth: sigverify needs an RPC URL, and an RPC URL set in any
    // other mode is ignored (warn rather than silently promote — the user chose the mode).
    if args.dedup_mode == DedupMode::Sigverify && args.rpc_url.is_none() {
        bail!("--dedup-mode sigverify requires --rpc-url (DZ_RPC_URL)");
    }
    if args.dedup_mode != DedupMode::Sigverify && args.rpc_url.is_some() {
        warn!(
            mode = ?args.dedup_mode,
            "--rpc-url is set but ignored (only --dedup-mode sigverify uses it)"
        );
    }
    if args.dedup_mode != DedupMode::None && args.dedup_window_slots == 0 {
        bail!("--dedup-window-slots must be > 0 unless --dedup-mode is none");
    }
    if args.candidate_groups.is_empty() && explicit_sources.is_empty() {
        bail!("no candidate groups (--candidate-group) nor explicit sources (--source)");
    }

    let cfg = reconcile::ReconcileConfig {
        candidates: dedup_preserving_order(args.candidate_groups),
        port: args.port,
        explicit_sources,
        forward,
        iface: args.iface.clone(),
        recv_buf: args.recv_buf,
        mode: args.dedup_mode,
        rpc_url: args.rpc_url.clone(),
        dedup_window_slots: args.dedup_window_slots,
        refresh: Duration::from_secs(args.refresh_secs),
    };

    reconcile::run(cfg)
        .await
        .context("shred forwarder reconciler exited")
}

/// Remove repeated candidate IPs preserving first-seen order (in case the user repeats one).
fn dedup_preserving_order(ips: Vec<Ipv4Addr>) -> Vec<Ipv4Addr> {
    let mut seen = std::collections::HashSet::new();
    ips.into_iter().filter(|ip| seen.insert(*ip)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_candidates_parse_to_five_ips() {
        let ips: Vec<Ipv4Addr> = DEFAULT_CANDIDATES
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(ips.len(), 5);
        assert_eq!(ips[0], Ipv4Addr::new(233, 84, 178, 1));
        assert_eq!(ips[1], Ipv4Addr::new(233, 84, 178, 16));
    }

    #[test]
    fn dedup_preserving_order_removes_repeats() {
        let a = Ipv4Addr::new(233, 84, 178, 1);
        let b = Ipv4Addr::new(233, 84, 178, 16);
        assert_eq!(dedup_preserving_order(vec![a, b, a, b, a]), vec![a, b]);
    }
}
