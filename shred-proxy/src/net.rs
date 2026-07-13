//! Active-group detection via the kernel routing table.
//!
//! The multicast join, interface resolution and socket plumbing all live in the bridge crate's
//! library (`doublezero_edge_connect::ingest::receiver`, driven by `shred::run`). The only network
//! logic unique to this binary is deciding **which** candidate groups to join by reading the routing
//! table — the bridge derives that from the `doublezero` CLI instead, which this standalone proxy
//! deliberately avoids depending on.

use std::{io, net::Ipv4Addr};

/// Parse the egress interface out of an `ip route get` line: the token after `dev`.
///
/// The first line of `ip route get 233.84.178.1` looks like
/// `multicast 233.84.178.1 dev doublezero1 src 10.0.0.2 …`. Pure so it is unit-tested directly (the
/// real probe shells out) — the shared parser means the tests exercise the actual extraction code.
fn parse_dev(route_line: &str) -> Option<&str> {
    let mut it = route_line.split_whitespace();
    while let Some(tok) = it.next() {
        if tok == "dev" {
            return it.next();
        }
    }
    None
}

/// Egress interface (`dev`) the kernel would use to reach `ip`, per `ip route get`.
///
/// Distinguishes "ran, no `dev` token" (`Ok(None)`) from "could not probe" (`Err`): a failure to
/// spawn `ip` or a non-zero exit is a *transient* error, not evidence the group is inactive, so it
/// must not be silently collapsed to `None` (that would tear down live forwarding — see
/// [`active_groups`]).
fn route_egress_iface(ip: Ipv4Addr) -> io::Result<Option<String>> {
    let out = std::process::Command::new("ip")
        .args(["route", "get", &ip.to_string()])
        .output()?;
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "`ip route get {ip}` exited with {}",
            out.status
        )));
    }
    let s = std::str::from_utf8(&out.stdout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(parse_dev(s).map(|d| d.to_string()))
}

/// Of the candidate multicast IPs, the **active** ones: those the kernel routes via the DoubleZero
/// interface (`dz_iface`). On a DoubleZero host a subscribed group routes over the tunnel
/// (`doublezero1`), while an unsubscribed one falls back to the default multicast route (a different
/// interface), so the egress interface is the "active group" discriminator.
///
/// Returns `Err` if **any** candidate probe could not run: a transient `ip` failure is
/// indistinguishable from "not routed here", so rather than fail-empty (which would blank the active
/// set and tear the forwarder down) we surface the error and let the reconciler keep the current
/// activation (fail-open, matching the bridge reconciler).
///
/// `dz_iface` must be an interface **name** (e.g. `doublezero1`), not an IP: `ip route get` reports
/// the interface by name. When `--iface` is passed as an IP (tests), this detection won't match and
/// the explicit source override should be used instead.
pub fn active_groups(candidates: &[Ipv4Addr], dz_iface: &str) -> io::Result<Vec<Ipv4Addr>> {
    let mut active = Vec::new();
    for ip in candidates.iter().copied() {
        if route_egress_iface(ip)?.as_deref() == Some(dz_iface) {
            active.push(ip);
        }
    }
    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::parse_dev;

    #[test]
    fn extracts_dev_from_multicast_route() {
        let line = "multicast 233.84.178.1 dev doublezero1 src 10.0.0.2 uid 1000";
        assert_eq!(parse_dev(line), Some("doublezero1"));
    }

    #[test]
    fn extracts_dev_from_default_route() {
        let line = "multicast 233.84.178.1 dev eth1 src 192.168.88.223 rt_offload_failed uid 1000";
        assert_eq!(parse_dev(line), Some("eth1"));
    }

    #[test]
    fn no_dev_yields_none() {
        assert_eq!(parse_dev("233.84.178.1 via 10.0.0.1"), None);
        assert_eq!(parse_dev(""), None);
    }
}
