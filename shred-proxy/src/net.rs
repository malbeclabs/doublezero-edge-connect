//! Active-group detection via the kernel routing table.
//!
//! The multicast join, interface resolution and socket plumbing all live in the bridge crate's
//! library (`doublezero_edge_connect::ingest::receiver`, driven by `shred::run`). The only network
//! logic unique to this binary is deciding **which** candidate groups to join by reading the routing
//! table — the bridge derives that from the `doublezero` CLI instead, which this standalone proxy
//! deliberately avoids depending on.

use std::net::Ipv4Addr;

/// Egress interface (`dev`) the kernel would use to reach `ip`, per `ip route get`. Returns `None`
/// if the command fails, exits non-zero, or the output has no `dev` token.
///
/// The first line of `ip route get 233.84.178.1` looks like
/// `multicast 233.84.178.1 dev doublezero1 src 10.0.0.2 …`; we take the token after `dev`.
fn route_egress_iface(ip: Ipv4Addr) -> Option<String> {
    let out = std::process::Command::new("ip")
        .args(["route", "get", &ip.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = std::str::from_utf8(&out.stdout).ok()?;
    let mut it = s.split_whitespace();
    while let Some(tok) = it.next() {
        if tok == "dev" {
            return it.next().map(|d| d.to_string());
        }
    }
    None
}

/// Of the candidate multicast IPs, the **active** ones: those the kernel routes via the DoubleZero
/// interface (`dz_iface`). On a DoubleZero host a subscribed group routes over the tunnel
/// (`doublezero1`), while an unsubscribed one falls back to the default multicast route (a different
/// interface), so the egress interface is the "active group" discriminator.
///
/// `dz_iface` must be an interface **name** (e.g. `doublezero1`), not an IP: `ip route get` reports
/// the interface by name. When `--iface` is passed as an IP (tests), this detection won't match and
/// the explicit source override should be used instead.
pub fn active_groups(candidates: &[Ipv4Addr], dz_iface: &str) -> Vec<Ipv4Addr> {
    candidates
        .iter()
        .copied()
        .filter(|ip| route_egress_iface(*ip).as_deref() == Some(dz_iface))
        .collect()
}

#[cfg(test)]
mod tests {
    // The `dev` token parsing is tested purely by replicating the `ip route get` format.
    // `route_egress_iface` shells out, so we validate the extraction logic separately here.
    fn dev_from(line: &str) -> Option<String> {
        let mut it = line.split_whitespace();
        while let Some(tok) = it.next() {
            if tok == "dev" {
                return it.next().map(|d| d.to_string());
            }
        }
        None
    }

    #[test]
    fn extracts_dev_from_multicast_route() {
        let line = "multicast 233.84.178.1 dev doublezero1 src 10.0.0.2 uid 1000";
        assert_eq!(dev_from(line).as_deref(), Some("doublezero1"));
    }

    #[test]
    fn extracts_dev_from_default_route() {
        let line = "multicast 233.84.178.1 dev eth1 src 192.168.88.223 rt_offload_failed uid 1000";
        assert_eq!(dev_from(line).as_deref(), Some("eth1"));
    }

    #[test]
    fn no_dev_yields_none() {
        assert_eq!(dev_from("233.84.178.1 via 10.0.0.1"), None);
        assert_eq!(dev_from(""), None);
    }
}
