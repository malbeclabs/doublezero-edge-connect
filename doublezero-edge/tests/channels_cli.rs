//! End-to-end tests for the `channels` command group, driven through the compiled binary — the
//! DZ_ADMIN_BIND-naming failure mode, the merged `channels list` view, and `channels set`'s
//! confirmation gate (required unless `--force`) are all only observable at this level.

mod common;

use std::{
    io::Write,
    process::{Command, Stdio},
};

use common::{bin, mock_server, unreachable_url};

struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

/// Like `cli_integration.rs`'s `run`, but with an explicit stdin payload — `channels set`'s
/// confirmation prompt reads a line from stdin, and the default (unpiped) stdin under a test
/// harness reads as an immediate EOF, which this crate's own confirmation gate must treat as "not
/// confirmed," not hang.
fn run_with_stdin(args: &[&str], stdin_payload: &str) -> Run {
    let mut child = Command::new(bin())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn doublezero-edge");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin_payload.as_bytes())
        .expect("write stdin payload");
    let out = child.wait_with_output().expect("wait for exit");
    Run {
        status: out
            .status
            .code()
            .expect("process exited via a signal, not a status code"),
        stdout: String::from_utf8(out.stdout).expect("stdout was not UTF-8"),
        stderr: String::from_utf8(out.stderr).expect("stderr was not UTF-8"),
    }
}

// -------------------------------------------------------------------------------------------
// DZ_ADMIN_BIND naming: a connection-refused against the admin surface must not read like an
// ordinary "unreachable" failure, since the admin surface is off unless the container sets
// DZ_ADMIN_BIND — otherwise a wrong --admin-url and a container that never enabled the surface
// are indistinguishable.
// -------------------------------------------------------------------------------------------

#[test]
fn channels_list_names_dz_admin_bind_when_the_admin_surface_is_unreachable() {
    let admin_url = unreachable_url();
    let r = run_with_stdin(&["channels", "list", "--admin-url", &admin_url], "");
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("DZ_ADMIN_BIND"),
        "a connection-refused to the admin surface must name DZ_ADMIN_BIND: {}",
        r.stderr
    );
}

#[test]
fn channels_set_names_dz_admin_bind_when_the_admin_surface_is_unreachable() {
    // The preview fetch against --url is irrelevant here (it can fail too, which just disables
    // the preview) — what this pins is that the *admin* unreachable path names DZ_ADMIN_BIND, so
    // force past the confirmation gate straight to the POST.
    let status_url = mock_server(
        "200 OK",
        r#"{"venues":[],"history":{"products":0,"products_at_cap":false,"buckets":0,"bucket_budget":100,"est_bytes":0,"window_seconds":3600,"evicted":0,"late_drops":0},"channels":{"rows":[],"excluded_by_floor":0}}"#,
    );
    let admin_url = unreachable_url();
    let r = run_with_stdin(
        &[
            "--url",
            &status_url,
            "channels",
            "set",
            "lashay-4=10",
            "--admin-url",
            &admin_url,
            "--force",
        ],
        "",
    );
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("DZ_ADMIN_BIND"),
        "a connection-refused to the admin surface must name DZ_ADMIN_BIND: {}",
        r.stderr
    );
}

// -------------------------------------------------------------------------------------------
// channels list: merges the admin floor summary with /v1/status's real per-channel state.
// -------------------------------------------------------------------------------------------

#[test]
fn channels_list_merges_the_admin_floor_summary_with_status_liveness() {
    let admin_url = mock_server(
        "200 OK",
        r#"{"summary":["lashay-4=2 of 31"],"rows":[],"note":"..."}"#,
    );
    let status_url = mock_server(
        "200 OK",
        r#"{"venues":[],"history":{"products":2,"products_at_cap":false,"buckets":2,"bucket_budget":100,"est_bytes":10,"window_seconds":3600,"evicted":0,"late_drops":0},
            "channels":{"rows":[{"venue":"KALSHI","category":"sports","code":"lashay-4","excluded":29,
            "channels":[{"channel":10,"floor_admits":true,"bound":true,"products":412,"label":"sports.nfl"},
                        {"channel":11,"floor_admits":true,"bound":false,"products":0}]}],"excluded_by_floor":29}}"#,
    );
    let r = run_with_stdin(
        &[
            "--url",
            &status_url,
            "channels",
            "list",
            "--admin-url",
            &admin_url,
            "--output",
            "table",
        ],
        "",
    );
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stdout.contains("lashay-4=2 of 31"), "{}", r.stdout);
    assert!(r.stdout.contains("sports.nfl"), "{}", r.stdout);
    assert!(r.stdout.contains("412"), "{}", r.stdout);
}

// -------------------------------------------------------------------------------------------
// channels set: confirmation is required unless --force, and the spec is never applied without
// it.
// -------------------------------------------------------------------------------------------

/// Default (unpiped-equivalent, here explicitly empty) stdin must read as "not confirmed" and
/// exit nonzero, without ever reaching the admin POST — proven by pointing --admin-url at a
/// closed port: if the CLI posted anyway, the connection failure would surface as
/// `admin_api_unreachable` (exit 3) rather than the abort path's exit 1.
#[test]
fn channels_set_aborts_without_force_and_never_posts() {
    let status_url = mock_server(
        "200 OK",
        r#"{"venues":[],"history":{"products":1,"products_at_cap":false,"buckets":1,"bucket_budget":100,"est_bytes":10,"window_seconds":3600,"evicted":0,"late_drops":0},
            "channels":{"rows":[{"venue":"KALSHI","category":"sports","code":"lashay-4","excluded":29,
            "channels":[{"channel":11,"floor_admits":true,"bound":true,"products":287}]}],"excluded_by_floor":29}}"#,
    );
    let admin_url = unreachable_url();
    let r = run_with_stdin(
        &[
            "--url",
            &status_url,
            "channels",
            "set",
            "lashay-4=10",
            "--admin-url",
            &admin_url,
        ],
        "no\n",
    );
    assert_eq!(r.status, 1, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("aborted"),
        "an unconfirmed set must abort, not silently do nothing: {}",
        r.stderr
    );
    assert!(
        !r.stderr.contains("DZ_ADMIN_BIND"),
        "an aborted set must never reach the admin surface at all: {}",
        r.stderr
    );
    // The preview must have named the channel that would be dropped.
    assert!(r.stdout.contains("287"), "{}", r.stdout);
}

/// `--force` skips the confirmation prompt and applies the spec.
#[test]
fn channels_set_with_force_skips_confirmation_and_applies() {
    let status_url = mock_server(
        "200 OK",
        r#"{"venues":[],"history":{"products":1,"products_at_cap":false,"buckets":1,"bucket_budget":100,"est_bytes":10,"window_seconds":3600,"evicted":0,"late_drops":0},
            "channels":{"rows":[],"excluded_by_floor":0}}"#,
    );
    let admin_url = mock_server("200 OK", r#"{"applied":["lashay-4=1 of 31"]}"#);
    let r = run_with_stdin(
        &[
            "--url",
            &status_url,
            "channels",
            "set",
            "lashay-4=10",
            "--admin-url",
            &admin_url,
            "--force",
        ],
        "",
    );
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stdout.contains("lashay-4=1 of 31"), "{}", r.stdout);
}

/// A `400` from the admin `POST` (e.g. the flat-row refusal) is surfaced verbatim, never
/// swallowed or reworded.
#[test]
fn channels_set_surfaces_a_400_from_the_admin_surface_verbatim() {
    let status_url = mock_server(
        "200 OK",
        r#"{"venues":[],"history":{"products":0,"products_at_cap":false,"buckets":0,"bucket_budget":100,"est_bytes":0,"window_seconds":3600,"evicted":0,"late_drops":0},
            "channels":{"rows":[],"excluded_by_floor":0}}"#,
    );
    let admin_url = mock_server(
        "400 Bad Request",
        r#"{"error":"invalid_channel_floor","message":"channel floor narrows `perps1` (LASHAY/perps), whose publishers bind one base port flat.","remediation":"..."}"#,
    );
    let r = run_with_stdin(
        &[
            "--url",
            &status_url,
            "channels",
            "set",
            "perps1=1",
            "--admin-url",
            &admin_url,
            "--force",
        ],
        "",
    );
    assert_eq!(r.status, 1, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("invalid_channel_floor"), "{}", r.stderr);
    assert!(r.stderr.contains("bind one base port flat"), "{}", r.stderr);
}
