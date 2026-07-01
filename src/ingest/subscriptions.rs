//! Host multicast-subscription detection — the single place all feed activation is derived from.
//!
//! `doublezero status --json` reports exactly the multicast groups **this host** is subscribed to,
//! in its `multicast_groups` field: a comma-separated list of `ROLE:code` entries where `S:` is a
//! subscriber and `P:` a publisher, e.g.
//! `"S:edge-solana-root,S:edge-solana-shreds,S:tiredsolid,S:scottsdale"`. That is the source of
//! truth the reconciler (`crate::ingest::reconcile`) uses to decide which market-data receivers,
//! shred sources, and the WebSocket sink to run.
//!
//! The network-wide `doublezero multicast group list` (what the shred forwarder discovered from
//! before) does **not** reflect per-host subscription, so it can't gate activation — here it's used
//! only to map subscribed group *codes* to their multicast *IPs*, and only for the shred groups
//! (the market-data groups already carry their IP in the `FEEDS` registry).
//!
//! Detection is a sync `std::process::Command` shell-out (soft-fail, mirroring
//! `crate::shred::discovery`); the async reconciler invokes it via `spawn_blocking`.

use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddrV4},
};

use serde::Deserialize;
use tracing::warn;

use crate::{ingest::feeds::Feed, shred::discovery::parse_group_code_ips};

/// Outcome of one detection attempt. The reconciler treats the three cases differently: `Ok` is
/// authoritative (reconcile to it, even when empty), `CliMissing` means **fail open** (no DZ CLI on
/// this host — run the static always-on set), and `Unavailable` is a transient hiccup (skip the
/// tick, keep the current activations rather than flapping everything off).
#[derive(Debug)]
pub enum Detected {
    Ok(HostSubs),
    /// The `doublezero` binary isn't installed/spawnable — e.g. running the bridge from source.
    CliMissing,
    /// The CLI is present but the query failed (non-zero exit, unparseable output).
    Unavailable,
}

/// The host's current multicast subscriptions plus the code→IP map for groups outside the
/// market-data `FEEDS` registry (the shred groups).
#[derive(Debug, Default, Clone)]
pub struct HostSubs {
    /// Group codes this host subscribes to (the `S:` entries of `doublezero status`).
    pub subscribed_codes: HashSet<String>,
    /// code → multicast IP, from `doublezero multicast group list` (activated rows only).
    pub code_ip: HashMap<String, Ipv4Addr>,
}

/// One entry of `doublezero status --json`. Only `multicast_groups` is read; every other field
/// (`response`, `current_device`, …) is ignored. A session with no multicast groups (e.g. IBRL)
/// simply has the field absent/null → treated as no subscriptions.
#[derive(Debug, Deserialize)]
struct StatusEntry {
    #[serde(default)]
    multicast_groups: Option<String>,
}

/// Parse the subscribed group **codes** from `doublezero status --json`. Each `multicast_groups`
/// token is `ROLE:code`; we keep subscriber (`S:`) entries (the bridge only ever receives), strip
/// the role prefix, and tolerate a bare `code` with no prefix. A parse error yields an empty list
/// (soft-fail).
pub fn parse_status_codes(stdout: &[u8]) -> Vec<String> {
    let entries: Vec<StatusEntry> = match serde_json::from_slice(stdout) {
        Ok(e) => e,
        Err(e) => {
            warn!(%e, "could not parse `doublezero status --json`; treating as no subscriptions");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for entry in entries {
        let Some(groups) = entry.multicast_groups else {
            continue;
        };
        for tok in groups.split(',') {
            let tok = tok.trim();
            if tok.is_empty() {
                continue;
            }
            let code = match tok.split_once(':') {
                // subscriber entry -> the code after the role prefix
                Some((role, code)) if role.eq_ignore_ascii_case("s") => code,
                // publisher (or any other role) -> not a receive subscription; skip
                Some(_) => continue,
                // no role prefix (older CLI) -> treat the whole token as a subscribed code
                None => tok,
            };
            if !code.is_empty() {
                out.push(code.to_string());
            }
        }
    }
    out
}

/// Run `doublezero status --json` (always) and `doublezero multicast group list --json-compact`
/// (only when `need_group_ips`, i.e. shred sources aren't explicitly overridden). See [`Detected`]
/// for how the three outcomes are classified.
pub fn detect(need_group_ips: bool) -> Detected {
    let status = match run_cli(&["status", "--json"]) {
        CliOut::Ok(bytes) => bytes,
        CliOut::Missing => return Detected::CliMissing,
        CliOut::Err => return Detected::Unavailable,
    };
    let subscribed_codes: HashSet<String> = parse_status_codes(&status).into_iter().collect();

    // The group list is only needed to resolve shred-group IPs (market-data IPs come from FEEDS).
    // A failure here doesn't invalidate the status-based market-data/WS gating, so it degrades to an
    // empty map (shred sources just won't resolve this tick) rather than an `Unavailable`.
    let code_ip = if need_group_ips {
        match run_cli(&["multicast", "group", "list", "--json-compact"]) {
            CliOut::Ok(bytes) => parse_group_code_ips(&bytes).into_iter().collect(),
            _ => HashMap::new(),
        }
    } else {
        HashMap::new()
    };

    Detected::Ok(HostSubs {
        subscribed_codes,
        code_ip,
    })
}

impl HostSubs {
    /// The subset of `enabled` feeds whose group `code` this host is subscribed to.
    pub fn market_data_feeds<'a>(&self, enabled: &[&'a Feed]) -> Vec<&'a Feed> {
        enabled
            .iter()
            .copied()
            .filter(|f| self.subscribed_codes.contains(f.code))
            .collect()
    }

    /// Subscribed group codes matching `prefix` (the shred groups, `edge-solana-`), resolved to
    /// `ip:port` via the group-list map. Sorted for deterministic diffing. A subscribed group with
    /// no known IP is warned about and skipped.
    pub fn shred_sources(&self, prefix: &str, port: u16) -> Vec<SocketAddrV4> {
        let mut out: Vec<SocketAddrV4> = self
            .subscribed_codes
            .iter()
            .filter(|c| c.starts_with(prefix))
            .filter_map(|code| match self.code_ip.get(code) {
                Some(ip) => Some(SocketAddrV4::new(*ip, port)),
                None => {
                    warn!(%code, "subscribed shred group has no multicast IP in `multicast group list`; skipping");
                    None
                }
            })
            .collect();
        out.sort();
        out
    }
}

/// Result of a single `doublezero` shell-out, distinguishing "binary absent" (fail open) from a
/// runtime error (transient) so [`detect`] can classify the outcome.
enum CliOut {
    Ok(Vec<u8>),
    Err,
    Missing,
}

fn run_cli(args: &[&str]) -> CliOut {
    match std::process::Command::new("doublezero").args(args).output() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => CliOut::Missing,
        Err(e) => {
            warn!(?args, %e, "could not run `doublezero`");
            CliOut::Err
        }
        Ok(o) if !o.status.success() => {
            warn!(
                ?args,
                status = %o.status,
                stderr = %String::from_utf8_lossy(&o.stderr).trim(),
                "`doublezero` exited non-zero"
            );
            CliOut::Err
        }
        Ok(o) => CliOut::Ok(o.stdout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::feeds::FEEDS;

    // A real `doublezero status --json` capture from a host subscribed to both shred and
    // market-data groups (the field that matters is `multicast_groups`).
    const STATUS_JSON: &str = r#"[
      {
        "response": {
          "doublezero_status": {"session_status": "BGP Session Up", "last_session_update": 1782920453},
          "tunnel_name": "doublezero1",
          "user_type": "Multicast"
        },
        "reconciler_enabled": true,
        "current_device": "tyo002-dz002",
        "network": "mainnet-beta",
        "multicast_groups": "S:edge-solana-root,S:edge-solana-retrans-apac,S:edge-solana-shreds,S:tiredsolid,S:scottsdale"
      }
    ]"#;

    fn codes(json: &str) -> HashSet<String> {
        parse_status_codes(json.as_bytes()).into_iter().collect()
    }

    #[test]
    fn parses_subscribed_codes_from_real_status() {
        let got = codes(STATUS_JSON);
        assert_eq!(
            got,
            [
                "edge-solana-root",
                "edge-solana-retrans-apac",
                "edge-solana-shreds",
                "tiredsolid",
                "scottsdale",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn keeps_subscriber_skips_publisher() {
        let json = r#"[{"multicast_groups":"S:tiredsolid,P:mine,s:scottsdale"}]"#;
        // Both S: (case-insensitive) kept, P: dropped.
        assert_eq!(
            codes(json),
            codes(r#"[{"multicast_groups":"S:tiredsolid,S:scottsdale"}]"#)
        );
    }

    #[test]
    fn tolerates_bare_codes_and_whitespace() {
        let json = r#"[{"multicast_groups":" tiredsolid , S:scottsdale ,"}]"#;
        assert_eq!(
            codes(json),
            codes(r#"[{"multicast_groups":"S:tiredsolid,S:scottsdale"}]"#)
        );
    }

    #[test]
    fn entries_without_groups_yield_nothing() {
        // An IBRL session with no multicast_groups field, and an empty string, both -> nothing.
        assert!(parse_status_codes(br#"[{"response":{"user_type":"IBRL"}}]"#).is_empty());
        assert!(parse_status_codes(br#"[{"multicast_groups":""}]"#).is_empty());
        assert!(parse_status_codes(br#"[{"multicast_groups":null}]"#).is_empty());
    }

    #[test]
    fn invalid_json_yields_nothing() {
        assert!(parse_status_codes(b"").is_empty());
        assert!(parse_status_codes(b"not json").is_empty());
        assert!(parse_status_codes(b"{\"multicast_groups\":\"S:x\"}").is_empty());
        // object, not array
    }

    fn subs(codes: &[&str], code_ip: &[(&str, Ipv4Addr)]) -> HostSubs {
        HostSubs {
            subscribed_codes: codes.iter().map(|s| s.to_string()).collect(),
            code_ip: code_ip.iter().map(|(c, ip)| (c.to_string(), *ip)).collect(),
        }
    }

    #[test]
    fn market_data_feeds_match_by_code() {
        let enabled: Vec<&Feed> = FEEDS.iter().collect();

        // Subscribed to Hyperliquid's group only -> both HL rows (TOB + MBO), not Phoenix.
        let hl = subs(&["tiredsolid", "edge-solana-shreds"], &[]);
        let got = hl.market_data_feeds(&enabled);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|f| f.venue == "Hyperliquid"));

        // Subscribed to Phoenix only.
        let px = subs(&["scottsdale"], &[]);
        let got = px.market_data_feeds(&enabled);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].venue, "Phoenix");

        // Shreds-only host -> no market-data feeds.
        let shreds_only = subs(&["edge-solana-shreds", "edge-solana-root"], &[]);
        assert!(shreds_only.market_data_feeds(&enabled).is_empty());
    }

    #[test]
    fn shred_sources_resolve_subscribed_prefix_to_ips() {
        let s = subs(
            &["edge-solana-shreds", "edge-solana-root", "tiredsolid"],
            &[
                ("edge-solana-shreds", Ipv4Addr::new(233, 84, 178, 1)),
                ("edge-solana-root", Ipv4Addr::new(233, 84, 178, 5)),
                ("tiredsolid", Ipv4Addr::new(233, 84, 178, 15)),
            ],
        );
        let got = s.shred_sources("edge-solana-", 7733);
        assert_eq!(
            got,
            vec![
                SocketAddrV4::new(Ipv4Addr::new(233, 84, 178, 1), 7733),
                SocketAddrV4::new(Ipv4Addr::new(233, 84, 178, 5), 7733),
            ]
        );
    }

    #[test]
    fn shred_source_without_ip_is_skipped() {
        // Subscribed to a shred group whose IP the group list didn't provide -> skipped, not panicked.
        let s = subs(&["edge-solana-shreds"], &[]);
        assert!(s.shred_sources("edge-solana-", 7733).is_empty());
    }
}
