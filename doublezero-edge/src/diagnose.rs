//! `diagnose` / `connect` / `disconnect` support: pure logic over already-fetched JSON plus their
//! `--output table` renderers, kept out of `main.rs`'s network wiring exactly as `channels.rs` is,
//! so each is unit-testable against a fixture body with no server in the loop.

use serde_json::Value;

use crate::{render, types::*};

/// Has the attempt this process started finished?
///
/// **An absent `last_attempt` means still-running, never finished.** The bridge records the attempt
/// before answering `202`, so after one we are always looking at our own — but a body that carries
/// none at all (an older bridge, a truncated response) must not be read as a completed run, which
/// would report a success nobody observed. An attempt object with no `running` field is treated as
/// finished, so a future bridge that drops the flag ends the wait rather than hanging on it.
pub fn attempt_finished(diagnostics: &Value) -> bool {
    match &diagnostics["last_attempt"] {
        Value::Object(a) => a.get("running") != Some(&Value::Bool(true)),
        _ => false,
    }
}

/// The finished attempt's process exit code, if it reported one.
pub fn attempt_exit_code(diagnostics: &Value) -> Option<i64> {
    diagnostics["last_attempt"]["exit_code"].as_i64()
}

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
    out.push_str("\n\n");
    out.push_str(&render_activation(&d.activation, &d.binds));
    if let Some(attempt) = &d.last_attempt {
        out.push_str("\n\n");
        out.push_str(&render_attempt_block(attempt));
    }

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

/// `--output table` for `connect`/`disconnect`'s merged body (`{"accepted": <the 202 body>,
/// "diagnostics": <the poll's last body, or null under --no-wait>, "timed_out": bool}`).
pub fn render_attempt(body: &Value) -> Result<String, String> {
    let mut out = format!(
        "accepted: {}",
        body["accepted"]["message"]
            .as_str()
            .unwrap_or("the container accepted the request")
    );
    if body["diagnostics"].is_null() {
        out.push_str(
            "\n\nnot waited for (--no-wait). `doublezero-edge diagnose` reports the outcome under \
             last_attempt.",
        );
        return Ok(out);
    }

    let d: DiagnosticsResponse = render::parse(&body["diagnostics"])?;
    out.push_str("\n\n");
    match &d.last_attempt {
        Some(attempt) => out.push_str(&render_attempt_block(attempt)),
        None => out.push_str("attempt: (the container reported none)"),
    }
    if body["timed_out"].as_bool().unwrap_or(false) {
        out.push_str(
            "\n\nstill running when this command gave up waiting; it continues in the \
                      container. Re-run `doublezero-edge diagnose` for its outcome.",
        );
    }
    out.push_str("\n\n");
    out.push_str(&render_verdict(&d.diagnosis));
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

fn render_attempt_block(a: &AttemptBlock) -> String {
    let state = if a.running {
        "running".to_string()
    } else {
        match a.exit_code {
            Some(code) => format!("finished exit_code={code}"),
            None => "finished (no exit code — killed by a signal)".to_string(),
        }
    };
    let mut out = format!(
        "attempt: `{}`  started_at_unix={}  {state}",
        a.command, a.started_at_unix
    );
    if !a.output_tail.trim().is_empty() {
        out.push_str(&format!("\noutput:\n{}", a.output_tail.trim_end()));
    }
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
    // attempt_finished — the polling loop's whole stopping condition.
    // -----------------------------------------------------------------------------------------

    /// The case the brief calls out: after a `202`, a body with no `last_attempt` must read as
    /// still-running. Read as finished, the poll would stop immediately and report an outcome that
    /// had not happened.
    #[test]
    fn an_absent_attempt_reads_as_still_running() {
        assert!(!attempt_finished(&json!({})));
        assert!(!attempt_finished(&json!({"last_attempt": null})));
    }

    #[test]
    fn a_running_attempt_is_not_finished_and_a_stopped_one_is() {
        assert!(!attempt_finished(
            &json!({"last_attempt": {"running": true}})
        ));
        assert!(attempt_finished(
            &json!({"last_attempt": {"running": false, "exit_code": 0}})
        ));
    }

    #[test]
    fn the_exit_code_comes_off_the_finished_attempt() {
        let body = json!({"last_attempt": {"running": false, "exit_code": 3}});
        assert_eq!(attempt_exit_code(&body), Some(3));
        assert_eq!(attempt_exit_code(&json!({})), None);
    }

    // -----------------------------------------------------------------------------------------
    // render_diagnose
    // -----------------------------------------------------------------------------------------

    fn diagnostics_fixture() -> Value {
        json!({
            "diagnosis": {
                "code": "tunnel_down",
                "summary": "The DoubleZero tunnel is not up — session status: disconnected.",
                "remediation": "Run `doublezero-edge connect`."
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
            out.contains("remediation: Run `doublezero-edge connect`."),
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

    // -----------------------------------------------------------------------------------------
    // render_attempt
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_finished_attempt_reports_its_exit_code_output_and_the_new_verdict() {
        let body = json!({
            "accepted": {"accepted": true, "command": "doublezero connect multicast",
                         "message": "`doublezero connect multicast` started."},
            "diagnostics": {
                "diagnosis": {"code": "ok", "summary": "The tunnel is up.", "remediation": "-"},
                "last_attempt": {"command": "doublezero connect multicast",
                                 "started_at_unix": 1782920453, "finished_at_unix": 1782920500,
                                 "running": false, "exit_code": 0, "output_tail": "user activated"}
            },
            "timed_out": false
        });
        let out = render_attempt(&body).unwrap();
        assert!(out.contains("exit_code=0"), "{out}");
        assert!(out.contains("user activated"), "the output tail: {out}");
        assert!(out.contains("diagnosis: ok"), "{out}");
    }

    /// A timeout must never read as a failed run: the attempt is still going in the container.
    #[test]
    fn a_timed_out_attempt_says_it_is_still_running() {
        let body = json!({
            "accepted": {"message": "started"},
            "diagnostics": {
                "diagnosis": {"code": "tunnel_down"},
                "last_attempt": {"command": "doublezero connect multicast",
                                 "started_at_unix": 1, "running": true, "output_tail": ""}
            },
            "timed_out": true
        });
        let out = render_attempt(&body).unwrap();
        assert!(out.contains("running"), "{out}");
        assert!(out.contains("gave up waiting"), "{out}");
    }

    #[test]
    fn no_wait_reports_that_the_outcome_was_not_observed() {
        let body = json!({
            "accepted": {"message": "`doublezero connect multicast` started."},
            "diagnostics": null,
            "timed_out": false
        });
        let out = render_attempt(&body).unwrap();
        assert!(out.contains("--no-wait"), "{out}");
        assert!(out.contains("diagnose"), "{out}");
    }
}
