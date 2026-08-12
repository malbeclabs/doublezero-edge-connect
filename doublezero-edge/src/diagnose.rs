//! `diagnose` support: pure logic over already-fetched JSON plus its `--output table` renderer,
//! kept out of `main.rs`'s network wiring exactly as `channels.rs` is, so it is unit-testable
//! against a fixture body with no server in the loop.

use serde_json::Value;

use crate::{render, types::*};

/// `--output table` for `diagnose`'s merged body (`{"diagnostics": <GET /admin/diagnostics>,
/// "status": <GET /v1/status, or null>}`). Leads with the verdict: everything below it is evidence
/// for an answer the bridge already computed, and an operator running this command is stuck.
pub fn render_diagnose(body: &Value) -> Result<String, String> {
    let d: DiagnosticsResponse = render::parse(&body["diagnostics"])?;
    let mut out = render_verdict(&d.diagnosis);
    out.push_str("\n\n");
    out.push_str(&render_tunnel(&d.tunnel));
    out.push_str("\n\n");
    out.push_str(&render_subscriptions(&d.subscriptions));
    if !d.registry.source.is_empty() {
        out.push_str("\n\n");
        out.push_str(&render::render_registry_line(&d.registry));
    }
    if let Some(latency) = &d.latency {
        out.push_str("\n\n");
        out.push_str(&render_latency(latency, d.latency_at_unix));
    }
    out.push_str("\n\n");
    out.push_str(&render_activation(&d.activation, &d.binds));

    // `/v1` is fetched best-effort and is expected to be down on exactly the host this command is
    // for, so its absence is reported rather than treated as an error.
    let venues: Vec<VenueStatus> =
        serde_json::from_value(body["status"]["venues"].clone()).unwrap_or_default();
    out.push_str("\n\n");
    if venues.is_empty() {
        out.push_str("venues: (no /v1/status — see the verdict above)");
    } else {
        let rows: Vec<Vec<String>> = venues
            .iter()
            .map(|v| vec![v.venue.clone(), v.status.clone()])
            .collect();
        out.push_str(&render::table(&["VENUE", "STATUS"], &rows));
    }
    Ok(out)
}

fn render_verdict(d: &Diagnosis) -> String {
    format!(
        "diagnosis: {}\nsummary: {}\nremediation: {}",
        d.code, d.summary, d.remediation
    )
}

fn render_tunnel(t: &TunnelBlock) -> String {
    let checked = t
        .checked_at_unix
        .map(|v| v.to_string())
        .unwrap_or_else(|| "never".to_string());
    let mut out = format!(
        "tunnel: detection={}  checked_at_unix={checked}  poll_seconds={}",
        t.detection, t.poll_seconds
    );
    // Only worth a column when it disagrees with `checked_at_unix` — that gap *is* the staleness,
    // and printing it on every healthy host would be a second copy of the same number.
    if let Some(ok_at) = t.last_ok_at_unix.filter(|v| Some(*v) != t.checked_at_unix) {
        out.push_str(&format!(
            "  last_ok_at_unix={ok_at} (the session and subscription data below is from that poll)"
        ));
    }
    if let Some(detail) = &t.detail {
        out.push_str(&format!("\ndetail: {detail}"));
    }
    if t.sessions.is_empty() {
        out.push_str("\n(no sessions reported)");
        return out;
    }
    let rows: Vec<Vec<String>> = t
        .sessions
        .iter()
        .map(|s| {
            vec![
                or_dash(&s.session_status),
                or_dash(&s.tunnel_name),
                or_dash(&s.user_type),
                or_dash(&s.current_device),
                or_dash(&s.lowest_latency_device),
                or_dash(&s.metro),
                or_dash(&s.network),
            ]
        })
        .collect();
    out.push('\n');
    out.push_str(&render::table(
        &[
            "SESSION",
            "TUNNEL",
            "USER_TYPE",
            "DEVICE",
            "LOWEST_LATENCY_DEVICE",
            "METRO",
            "NETWORK",
        ],
        &rows,
    ));
    out
}

fn render_subscriptions(s: &SubscriptionsBlock) -> String {
    format!(
        "subscriptions: market_data=[{}]  shred=[{}]  other=[{}]",
        s.market_data_codes.join(", "),
        s.shred_codes.join(", "),
        s.other_codes.join(", "),
    )
}

/// Sorted nearest-first among reachable devices. Two questions this answers: on a host that is
/// down, whether any device responds at all; on one that is up, whether something is now nearer
/// than the device it is actually on (compare the `DEVICE` column of the tunnel table above).
///
/// Stamped with its own time because the container paces this far coarser than the poll, so these
/// figures are routinely older than the rest of the report and must not read as just-measured.
fn render_latency(rows: &[DeviceLatency], probed_at: Option<u64>) -> String {
    let at = match probed_at {
        Some(v) => format!("  probed_at_unix={v}"),
        None => String::new(),
    };
    if rows.is_empty() {
        return format!("latency:{at} probed, no device answered");
    }
    let mut rows: Vec<&DeviceLatency> = rows.iter().collect();
    rows.sort_by_key(|d| (!d.reachable, d.avg_latency_ns.unwrap_or(u64::MAX)));
    let body: Vec<Vec<String>> = rows
        .iter()
        .map(|d| {
            vec![
                or_dash(&d.device_code),
                or_dash(&d.device_ip),
                if d.reachable { "yes" } else { "no" }.to_string(),
                ms(d.min_latency_ns),
                ms(d.avg_latency_ns),
                ms(d.max_latency_ns),
            ]
        })
        .collect();
    format!(
        "latency: {} device(s){at}\n{}",
        rows.len(),
        render::table(
            &["DEVICE", "IP", "REACHABLE", "MIN_MS", "AVG_MS", "MAX_MS"],
            &body,
        )
    )
}

/// Nanoseconds as milliseconds — the scale a human reads a network path in; the raw ns stay in
/// `--output json`.
fn ms(ns: Option<u64>) -> String {
    match ns {
        Some(v) => format!("{:.3}", v as f64 / 1_000_000.0),
        None => "-".to_string(),
    }
}

fn render_activation(a: &ActivationBlock, b: &BindsBlock) -> String {
    let mut out = format!(
        "activation: ws_on={}  api_on={}  receivers={}  shred_sources={}\n\
         binds: ws={}  api={}  admin={}  metrics={}",
        a.ws_on,
        a.api_on,
        a.receivers.len(),
        a.shred_sources.len(),
        or_unset(&b.ws),
        or_unset(&b.api),
        or_unset(&b.admin),
        or_unset(&b.metrics),
    );
    if a.receivers.is_empty() {
        return out;
    }
    let rows: Vec<Vec<String>> = a
        .receivers
        .iter()
        .map(|r| {
            vec![
                r.venue.clone(),
                r.category.clone(),
                r.kind.clone(),
                r.publisher.to_string(),
                r.liveness.clone(),
            ]
        })
        .collect();
    out.push('\n');
    out.push_str(&render::table(
        &["VENUE", "CATEGORY", "KIND", "PUBLISHER", "LIVENESS"],
        &rows,
    ));
    out
}

fn or_dash(v: &Option<String>) -> String {
    v.clone().unwrap_or_else(|| "-".to_string())
}

fn or_unset(v: &str) -> &str {
    if v.is_empty() {
        "(unset)"
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------------------------
    // render_diagnose
    // -----------------------------------------------------------------------------------------

    fn diagnostics_fixture() -> Value {
        json!({
            "diagnosis": {
                "code": "tunnel_down",
                "summary": "The DoubleZero tunnel is not up — session status: disconnected.",
                "remediation": "Run `doublezero connect multicast` in the container."
            },
            "tunnel": {
                "detection": "ok", "detail": null, "checked_at_unix": 1782920453,
                "poll_seconds": 30,
                "sessions": [{"session_status": "disconnected", "tunnel_name": null,
                              "user_type": null, "current_device": "N/A", "metro": "N/A",
                              "network": "mainnet-beta"}]
            },
            "subscriptions": {"market_data_codes": [], "shred_codes": [], "other_codes": []},
            "activation": {"receivers": [], "ws_on": false, "api_on": false, "shred_sources": []},
            "registry": {"source": "built-in", "version": 3, "rows": 4, "receivers": 9},
            "binds": {"ws": "0.0.0.0:8081", "api": "", "admin": "127.0.0.1:9098", "metrics": ""}
        })
    }

    /// The verdict comes first and in full — an operator running this command is stuck, and the
    /// evidence below is only useful once they have been told the answer.
    #[test]
    fn the_table_leads_with_the_verdict() {
        let out = render_diagnose(&json!({"diagnostics": diagnostics_fixture(), "status": null}))
            .unwrap();
        let first = out.lines().next().unwrap();
        assert_eq!(first, "diagnosis: tunnel_down");
        assert!(
            out.contains("summary: The DoubleZero tunnel is not up"),
            "{out}"
        );
        assert!(
            out.contains("remediation: Run `doublezero connect multicast`"),
            "{out}"
        );
        assert!(out.contains("disconnected"), "the session table: {out}");
        assert!(out.contains("registry: source=built-in"), "{out}");
        assert!(
            out.contains("api=(unset)"),
            "an unset bind must read as such: {out}"
        );
    }

    /// `/v1` being down is the normal case here, so its absence is stated rather than rendered as
    /// an empty venue table that reads like "no venues exist".
    #[test]
    fn a_missing_v1_status_is_reported_not_rendered_as_an_empty_table() {
        let out = render_diagnose(&json!({"diagnostics": diagnostics_fixture(), "status": null}))
            .unwrap();
        assert!(out.contains("no /v1/status"), "{out}");
    }

    #[test]
    fn a_present_v1_status_renders_its_venues() {
        let body = json!({
            "diagnostics": diagnostics_fixture(),
            "status": {"venues": [{"venue": "LASHAY", "status": "online"}]}
        });
        let out = render_diagnose(&body).unwrap();
        assert!(out.contains("LASHAY"), "{out}");
        assert!(out.contains("online"), "{out}");
    }

    /// A bridge one version behind sends fewer blocks; the report must degrade to what it does
    /// carry rather than refuse to render.
    #[test]
    fn a_sparse_diagnostics_body_still_renders() {
        let body = json!({"diagnostics": {"diagnosis": {"code": "pending"}}, "status": null});
        let out = render_diagnose(&body).unwrap();
        assert!(out.starts_with("diagnosis: pending"), "{out}");
        assert!(out.contains("(no sessions reported)"), "{out}");
    }

    /// The `latency` read #133 asks for, off a real capture. Rendered nearest-first and in ms,
    /// because the question on a down host is "is anything reachable, and which is closest".
    #[test]
    fn the_latency_block_renders_nearest_reachable_device_first() {
        let mut d = diagnostics_fixture();
        d["latency_at_unix"] = json!(1782920100);
        d["latency"] = json!([
            {"device_code": "dz-ny7-sw02", "device_ip": "142.215.184.122",
             "min_latency_ns": 18529843, "avg_latency_ns": 23286675,
             "max_latency_ns": 31704187, "reachable": true},
            {"device_code": "dz-ny7-sw01", "device_ip": "137.239.213.192",
             "min_latency_ns": 18656333, "avg_latency_ns": 19979918,
             "max_latency_ns": 21151629, "reachable": true},
            {"device_code": "dz-unreachable", "device_ip": "10.0.0.1",
             "min_latency_ns": null, "avg_latency_ns": null,
             "max_latency_ns": null, "reachable": false}
        ]);
        let out = render_diagnose(&json!({"diagnostics": d, "status": null})).unwrap();

        assert!(out.contains("latency: 3 device(s)"), "{out}");
        assert!(
            out.contains("probed_at_unix=1782920100"),
            "the probe is paced coarser than the poll, so it must date itself: {out}"
        );
        assert!(out.contains("19.980"), "ns must render as ms: {out}");
        let rows: Vec<&str> = out
            .lines()
            .skip_while(|l| !l.starts_with("latency:"))
            .collect();
        let nearest = rows.iter().position(|l| l.contains("dz-ny7-sw01"));
        let further = rows.iter().position(|l| l.contains("dz-ny7-sw02"));
        let dead = rows.iter().position(|l| l.contains("dz-unreachable"));
        assert!(nearest < further, "nearest reachable device first: {out}");
        assert!(further < dead, "unreachable devices sort last: {out}");
        assert!(
            out.contains('-'),
            "a device with no samples renders a dash: {out}"
        );
    }

    /// Not probed and probed-but-silent are different answers: the container skips the probe
    /// entirely once a session is up, and that must not read as "no device answered".
    #[test]
    fn an_unprobed_host_renders_no_latency_block_and_an_empty_probe_says_so() {
        let out = render_diagnose(&json!({"diagnostics": diagnostics_fixture(), "status": null}))
            .unwrap();
        assert!(
            !out.contains("latency:"),
            "absent means never probed: {out}"
        );

        let mut d = diagnostics_fixture();
        d["latency"] = json!([]);
        let out = render_diagnose(&json!({"diagnostics": d, "status": null})).unwrap();
        assert!(out.contains("latency: probed, no device answered"), "{out}");
    }
}
