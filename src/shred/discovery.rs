//! Source discovery for the shred forwarder: shell out to `doublezero multicast group list`
//! and select the `edge-solana-*` multicast groups to join.
//!
//! Mirrors the existing `ip -4 -o addr show` shell-out style in `ingest/receiver.rs`: the
//! `doublezero` CLI is installed and pre-configured in the container. The container is auto-joined
//! only to the groups its access pass grants (incl. exactly one regional retransmit), so binding
//! every matched group is safe — the network delivers only the permitted subset.

use std::net::{Ipv4Addr, SocketAddrV4};

use tracing::{info, warn};

/// Run `doublezero multicast group list`, parse the table, and return the `(group, port)` sources
/// whose `code` starts with `prefix`. Every matched group is bound on `port`.
///
/// Discovery failures (binary missing, non-zero exit, empty/garbage output) are treated as "no
/// groups": logged and returned as an empty list, so a host without the CLI simply doesn't run the
/// shred pipeline rather than crashing.
pub fn discover_groups(prefix: &str, port: u16) -> Vec<SocketAddrV4> {
    let output = match std::process::Command::new("doublezero")
        .args(["multicast", "group", "list"])
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
    let stdout = String::from_utf8_lossy(&output.stdout);
    let groups = parse_group_table(&stdout, prefix);
    if groups.is_empty() {
        info!(
            prefix,
            "no `edge-solana-*` multicast groups matched discovery"
        );
        return Vec::new();
    }
    groups
        .into_iter()
        .map(|ip| SocketAddrV4::new(ip, port))
        .collect()
}

/// Parse the pipe-delimited `doublezero multicast group list` table and return the `multicast_ip`
/// of every row whose `code` cell starts with `prefix`.
///
/// The table has a header row and one row per group, each cell `|`-delimited (with a leading space
/// per line). Header, blank, and malformed rows (too few cells, unparseable IP) are skipped so a
/// partial/garbled table never panics or yields a bad source.
pub fn parse_group_table(table: &str, prefix: &str) -> Vec<Ipv4Addr> {
    let mut out = Vec::new();
    for line in table.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        // Columns: account | code | multicast_ip | ... — need at least the first three.
        if cells.len() < 3 {
            continue;
        }
        let code = cells[1];
        if !code.starts_with(prefix) {
            continue; // skips the header row ("code") and unrelated groups (e.g. jito-shredstream)
        }
        match cells[2].parse::<Ipv4Addr>() {
            Ok(ip) => out.push(ip),
            Err(_) => continue, // header's "multicast_ip" literal, or a garbled cell
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
 account                                      | code                     | multicast_ip  | max_bandwidth | publishers | subscribers | status    | owner
 7GfKjAfxZWaLZBKn2KwYQECfrFmSQ3EdBshyjMC4TnJg | edge-solana-retrans-amer | 233.84.178.14 | 150Mbps       | 2          | 32          | activated | DZf...
 31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj | edge-solana-shreds       | 233.84.178.1  | 100Mbps       | 759        | 154         | activated | DZj...
 3eUvZvcpCtsfJ8wqCZvhiyBhbY2Sjn56JcQWpDwsESyX | jito-shredstream         | 233.84.178.2  | 200Mbps       | 9          | 0           | activated | 44N...";

    #[test]
    fn selects_edge_solana_excludes_jito() {
        let ips = parse_group_table(SAMPLE, "edge-solana-");
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
    fn header_row_is_skipped() {
        // The header's code cell is the literal "code" and its ip cell is "multicast_ip" — neither
        // starts with the prefix / parses as an IP, so it never leaks in.
        let ips = parse_group_table(SAMPLE, "edge-solana-");
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn tolerates_blank_and_garbage_lines() {
        let messy = format!("\n\n{SAMPLE}\nnot a table row at all\n   \n|||\n");
        let ips = parse_group_table(&messy, "edge-solana-");
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn empty_table_yields_nothing() {
        assert!(parse_group_table("", "edge-solana-").is_empty());
        assert!(parse_group_table("   \n  \n", "edge-solana-").is_empty());
    }

    #[test]
    fn prefix_is_honored() {
        // A narrower prefix selects only the leader group.
        let ips = parse_group_table(SAMPLE, "edge-solana-shreds");
        assert_eq!(ips, vec![Ipv4Addr::new(233, 84, 178, 1)]);
    }
}
