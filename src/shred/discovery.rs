//! Source discovery for the shred forwarder: shell out to
//! `doublezero multicast group list --json-compact` and select the `edge-solana-*` multicast
//! groups to join.
//!
//! Mirrors the existing `ip -4 -o addr show` shell-out style in `ingest/receiver.rs`: the
//! `doublezero` CLI is installed and pre-configured in the container. The container is auto-joined
//! only to the groups its access pass grants (incl. exactly one regional retransmit), so binding
//! every matched group is safe — the network delivers only the permitted subset.
//!
//! `--json-compact` is the CLI's machine-readable contract; we deserialize it with serde rather
//! than scraping the human-readable pipe-delimited table (whose column order/width is a
//! presentation detail with no stability guarantee, and whose breakage would be invisible —
//! every row failing to parse just leaves the forwarder silently off).

use std::net::{Ipv4Addr, SocketAddrV4};

use serde::Deserialize;
use tracing::{info, warn};

/// One row of `doublezero multicast group list --json-compact`. Only the fields we act on are
/// declared; serde ignores the rest (`account`, `max_bandwidth`, `publishers`, …). `multicast_ip`
/// is parsed straight into `Ipv4Addr`, so a malformed address fails deserialization (→ empty list,
/// forwarder stays off) rather than yielding a bad source.
#[derive(Debug, Deserialize)]
struct GroupRow {
    code: String,
    multicast_ip: Ipv4Addr,
    status: String,
}

/// Run `doublezero multicast group list --json-compact`, parse the JSON, and return the
/// `(group, port)` sources whose `code` starts with `prefix` and whose `status` is activated.
/// Every matched group is bound on `port`.
///
/// Discovery failures (binary missing, non-zero exit, empty/garbage output, JSON parse error) are
/// treated as "no groups": logged and returned as an empty list, so a host without the CLI simply
/// doesn't run the shred pipeline rather than crashing.
pub fn discover_groups(prefix: &str, port: u16) -> Vec<SocketAddrV4> {
    let output = match std::process::Command::new("doublezero")
        .args(["multicast", "group", "list", "--json-compact"])
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            warn!(prefix, %e, "could not run `doublezero multicast group list`; no shred groups discovered");
            return Vec::new();
        }
    };
    if !output.status.success() {
        warn!(
            prefix,
            status = %output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "`doublezero multicast group list` exited non-zero; no shred groups discovered"
        );
        return Vec::new();
    }
    let groups = parse_group_json(&output.stdout, prefix);
    if groups.is_empty() {
        info!(
            prefix,
            "no activated `edge-solana-*` multicast groups matched discovery"
        );
        return Vec::new();
    }
    groups
        .into_iter()
        .map(|ip| SocketAddrV4::new(ip, port))
        .collect()
}

/// Deserialize the `--json-compact` array and return the `multicast_ip` of every activated row
/// whose `code` starts with `prefix`. A JSON parse error (wrong shape, truncated, non-JSON) is a
/// soft failure: logged and returned as an empty list so the forwarder stays off rather than
/// crashing the process.
pub fn parse_group_json(stdout: &[u8], prefix: &str) -> Vec<Ipv4Addr> {
    let rows: Vec<GroupRow> = match serde_json::from_slice(stdout) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(%e, "could not parse `doublezero multicast group list --json-compact`; no shred groups discovered");
            return Vec::new();
        }
    };
    rows.into_iter()
        .filter(|r| r.code.starts_with(prefix))
        // Skip groups that aren't activated (draining/pending) — only join live ones.
        .filter(|r| r.status.eq_ignore_ascii_case("activated"))
        .map(|r| r.multicast_ip)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors `doublezero multicast group list --json-compact`: a JSON array of group objects.
    // Includes the unrelated `jito-shredstream` group (must be excluded by prefix) and a
    // non-activated `edge-solana-*` group (must be excluded by status).
    const SAMPLE: &str = r#"[
        {"account":"7Gf...","code":"edge-solana-retrans-amer","multicast_ip":"233.84.178.14","max_bandwidth":"150Mbps","publishers":2,"subscribers":32,"status":"activated","owner":"DZf..."},
        {"account":"31f...","code":"edge-solana-shreds","multicast_ip":"233.84.178.1","max_bandwidth":"100Mbps","publishers":759,"subscribers":154,"status":"Activated","owner":"DZj..."},
        {"account":"3eU...","code":"jito-shredstream","multicast_ip":"233.84.178.2","max_bandwidth":"200Mbps","publishers":9,"subscribers":0,"status":"activated","owner":"44N..."}
    ]"#;

    #[test]
    fn selects_edge_solana_excludes_jito() {
        let ips = parse_group_json(SAMPLE.as_bytes(), "edge-solana-");
        assert_eq!(
            ips,
            vec![
                Ipv4Addr::new(233, 84, 178, 14),
                Ipv4Addr::new(233, 84, 178, 1),
            ]
        );
        // jito-shredstream (233.84.178.2) must NOT be selected by the prefix.
        assert!(!ips.contains(&Ipv4Addr::new(233, 84, 178, 2)));
    }

    #[test]
    fn status_filter_excludes_non_activated() {
        // A pending/draining group is skipped even when its code matches the prefix.
        let json = r#"[
            {"code":"edge-solana-shreds","multicast_ip":"233.84.178.1","status":"activated"},
            {"code":"edge-solana-retrans-eu","multicast_ip":"233.84.178.12","status":"pending"}
        ]"#;
        let ips = parse_group_json(json.as_bytes(), "edge-solana-");
        assert_eq!(ips, vec![Ipv4Addr::new(233, 84, 178, 1)]);
    }

    #[test]
    fn case_insensitive_status_is_accepted() {
        // The CLI capitalizes `Activated`; the filter must match regardless of case.
        let json =
            r#"[{"code":"edge-solana-shreds","multicast_ip":"233.84.178.1","status":"Activated"}]"#;
        let ips = parse_group_json(json.as_bytes(), "edge-solana-");
        assert_eq!(ips, vec![Ipv4Addr::new(233, 84, 178, 1)]);
    }

    #[test]
    fn invalid_json_yields_nothing() {
        // Non-JSON, truncated, or wrong-shape output is a soft failure → empty list (forwarder off).
        assert!(parse_group_json(b"", "edge-solana-").is_empty());
        assert!(parse_group_json(b"not json at all", "edge-solana-").is_empty());
        assert!(parse_group_json(b"[{\"code\":\"edge-solana-shreds\"", "edge-solana-").is_empty());
        // Wrong shape (object, not array) is also tolerated.
        assert!(parse_group_json(b"{\"groups\":[]}", "edge-solana-").is_empty());
    }

    #[test]
    fn malformed_ip_fails_the_whole_parse() {
        // A bad `multicast_ip` fails deserialization of the array → empty list (fail-safe, never a
        // bad source). serde aborts the whole parse, which is fine: we'd rather stay off than join
        // a garbage address.
        let json =
            r#"[{"code":"edge-solana-shreds","multicast_ip":"not-an-ip","status":"activated"}]"#;
        assert!(parse_group_json(json.as_bytes(), "edge-solana-").is_empty());
    }

    #[test]
    fn empty_array_yields_nothing() {
        assert!(parse_group_json(b"[]", "edge-solana-").is_empty());
    }

    #[test]
    fn prefix_is_honored() {
        // A narrower prefix selects only the leader group.
        let ips = parse_group_json(SAMPLE.as_bytes(), "edge-solana-shreds");
        assert_eq!(ips, vec![Ipv4Addr::new(233, 84, 178, 1)]);
    }
}
