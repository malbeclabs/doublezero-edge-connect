//! Multicast group **code → IP** lookup: shell out to `doublezero multicast group list
//! --json-compact` and map each activated group's code to its multicast IP.
//!
//! This is a helper for `crate::ingest::subscriptions`, which decides *what this host is subscribed
//! to* from `doublezero status` and then needs the multicast IP for the subscribed **shred** groups
//! (the market-data groups carry their IP in the `FEEDS` registry, so only shred groups need this
//! lookup). The group list is network-wide and does NOT reflect per-host subscription — it's used
//! purely as a code→IP directory here.
//!
//! `--json-compact` is the CLI's machine-readable contract; we deserialize it with serde rather
//! than scraping the human-readable pipe-delimited table (whose column order/width is a
//! presentation detail with no stability guarantee, and whose breakage would be invisible).

use std::net::Ipv4Addr;

use serde::Deserialize;
use tracing::warn;

/// One row of `doublezero multicast group list --json-compact`. Only the fields we act on are
/// declared; serde ignores the rest (`account`, `max_bandwidth`, `publishers`, …). `multicast_ip`
/// is parsed straight into `Ipv4Addr`, so a malformed address fails deserialization (→ empty list)
/// rather than yielding a bad source.
#[derive(Debug, Deserialize)]
struct GroupRow {
    code: String,
    multicast_ip: Ipv4Addr,
    status: String,
}

/// Deserialize the `--json-compact` array and return `(code, multicast_ip)` for every **activated**
/// row. A JSON parse error (wrong shape, truncated, non-JSON) is a soft failure: logged and returned
/// as an empty list so a bad read leaves callers with no map rather than crashing the process.
pub fn parse_group_code_ips(stdout: &[u8]) -> Vec<(String, Ipv4Addr)> {
    let rows: Vec<GroupRow> = match serde_json::from_slice(stdout) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(%e, "could not parse `doublezero multicast group list --json-compact`; no code->ip map");
            return Vec::new();
        }
    };
    rows.into_iter()
        // Skip groups that aren't activated (draining/pending) — only map live ones.
        .filter(|r| r.status.eq_ignore_ascii_case("activated"))
        .map(|r| (r.code, r.multicast_ip))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Mirrors `doublezero multicast group list --json-compact`: a JSON array of group objects,
    // including a non-activated row (must be excluded by status) and mixed `activated`/`Activated`.
    const SAMPLE: &str = r#"[
        {"account":"7Gf...","code":"edge-solana-retrans-amer","multicast_ip":"233.84.178.14","max_bandwidth":"150Mbps","publishers":2,"subscribers":32,"status":"activated","owner":"DZf..."},
        {"account":"31f...","code":"edge-solana-shreds","multicast_ip":"233.84.178.1","max_bandwidth":"100Mbps","publishers":759,"subscribers":154,"status":"Activated","owner":"DZj..."},
        {"account":"3eU...","code":"tiredsolid","multicast_ip":"233.84.178.15","max_bandwidth":"200Mbps","publishers":9,"subscribers":0,"status":"activated","owner":"44N..."},
        {"account":"9aa...","code":"edge-solana-retrans-eu","multicast_ip":"233.84.178.12","status":"pending"}
    ]"#;

    fn map(json: &str) -> HashMap<String, Ipv4Addr> {
        parse_group_code_ips(json.as_bytes()).into_iter().collect()
    }

    #[test]
    fn maps_activated_codes_to_ips_case_insensitively() {
        let m = map(SAMPLE);
        // All activated rows are present (no prefix filter — codes across families).
        assert_eq!(
            m.get("edge-solana-retrans-amer"),
            Some(&Ipv4Addr::new(233, 84, 178, 14))
        );
        assert_eq!(
            m.get("edge-solana-shreds"),
            Some(&Ipv4Addr::new(233, 84, 178, 1))
        ); // "Activated"
        assert_eq!(m.get("tiredsolid"), Some(&Ipv4Addr::new(233, 84, 178, 15)));
    }

    #[test]
    fn excludes_non_activated() {
        // The `pending` row is skipped.
        assert!(!map(SAMPLE).contains_key("edge-solana-retrans-eu"));
    }

    #[test]
    fn invalid_json_yields_nothing() {
        // Non-JSON, truncated, or wrong-shape output is a soft failure → empty.
        assert!(parse_group_code_ips(b"").is_empty());
        assert!(parse_group_code_ips(b"not json at all").is_empty());
        assert!(parse_group_code_ips(b"[{\"code\":\"edge-solana-shreds\"").is_empty());
        // Wrong shape (object, not array) is also tolerated.
        assert!(parse_group_code_ips(b"{\"groups\":[]}").is_empty());
    }

    #[test]
    fn malformed_ip_fails_the_whole_parse() {
        // A bad `multicast_ip` fails deserialization of the array → empty (fail-safe, never a bad
        // source). serde aborts the whole parse, which is fine: we'd rather have no map.
        let json =
            r#"[{"code":"edge-solana-shreds","multicast_ip":"not-an-ip","status":"activated"}]"#;
        assert!(parse_group_code_ips(json.as_bytes()).is_empty());
    }

    #[test]
    fn empty_array_yields_nothing() {
        assert!(parse_group_code_ips(b"[]").is_empty());
    }
}
