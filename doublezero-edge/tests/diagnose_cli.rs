//! End-to-end tests for the admin-surface diagnosis commands — `diagnose`, `connect`,
//! `disconnect` — and for the `/v1` failure they exist to disambiguate: `api_unreachable` (nothing
//! is running) versus `api_inactive` (edge-connect is running, `/v1` is simply not activated).
//! Only observable through the compiled binary, since the probe, its host guard and the exit codes
//! are all decisions `main.rs` makes around the library calls.

mod common;

use std::{
    io::Write,
    process::{Command, Stdio},
    sync::atomic::Ordering,
};

use common::{bin, mock_server, mock_server_sequence, unreachable_url};

struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run(args: &[&str]) -> Run {
    run_with_stdin(args, "")
}

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

/// A `tunnel_down` diagnostics body — the issue's own host: container up, tunnel never came up, so
/// `/v1` is not listening at all.
const TUNNEL_DOWN: &str = r#"{
    "diagnosis": {"code":"tunnel_down",
                  "summary":"The DoubleZero tunnel is not up - session status: disconnected.",
                  "remediation":"Run `doublezero-edge connect`."},
    "tunnel": {"detection":"ok","detail":null,"checked_at_unix":1782920453,"poll_seconds":30,
               "sessions":[{"session_status":"disconnected","tunnel_name":null,"user_type":null,
                            "current_device":"N/A","metro":"N/A","network":"mainnet-beta"}]},
    "subscriptions": {"market_data_codes":[],"shred_codes":[],"other_codes":[]},
    "activation": {"receivers":[],"ws_on":false,"api_on":false,"shred_sources":[]},
    "last_attempt": null,
    "registry": {"source":"built-in","version":3,"rows":4,"receivers":9},
    "binds": {"ws":"0.0.0.0:8081","api":"0.0.0.0:9099","admin":"127.0.0.1:9098","metrics":""}
}"#;

// -------------------------------------------------------------------------------------------
// api_unreachable vs api_inactive: the distinction the whole change is for.
// -------------------------------------------------------------------------------------------

/// `/v1` refuses the connection but the admin surface answers: the container is up and `/v1` is
/// correctly serving nothing. Reporting `api_unreachable` here sends an operator to `docker ps`,
/// which shows a healthy container and no further clue — the dead end being fixed.
#[test]
fn a_dead_v1_with_a_live_admin_surface_reports_api_inactive() {
    let admin_url = mock_server("200 OK", TUNNEL_DOWN);
    let r = run(&[
        "--url",
        &unreachable_url(),
        "--admin-url",
        &admin_url,
        "products",
        "list",
    ]);
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("\"api_inactive\""),
        "a running container must not be reported as unreachable: {}",
        r.stderr
    );
    assert!(
        r.stderr.contains("doublezero-edge diagnose"),
        "the remediation must name the command that explains it: {}",
        r.stderr
    );
    assert!(
        r.stderr.contains("The DoubleZero tunnel is not up"),
        "the probe already knows the verdict; quote it: {}",
        r.stderr
    );
}

/// Neither surface answers: nothing changes, and the container really may be down.
#[test]
fn both_surfaces_dead_keeps_api_unreachable() {
    let r = run(&[
        "--url",
        &unreachable_url(),
        "--admin-url",
        &unreachable_url(),
        "products",
        "list",
    ]);
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("\"api_unreachable\""), "{}", r.stderr);
    assert!(
        !r.stderr.contains("api_inactive"),
        "with nothing answering, claiming the container is running would be a guess: {}",
        r.stderr
    );
    assert!(
        r.stderr.contains("doublezero-edge diagnose"),
        "{}",
        r.stderr
    );
}

/// The host guard. A remote `--url` with the default loopback `--admin-url` must **not** probe: the
/// local container's state is not the remote one's, and reporting it as such is confidently wrong.
/// Proven by the admin mock's own request counter, not just by the message — a probe that happened
/// and was then discarded would still be a request against the wrong host.
#[test]
fn a_remote_url_with_a_loopback_admin_url_never_probes() {
    let (admin_url, served) = mock_server_sequence(vec![("200 OK", TUNNEL_DOWN)]);
    let r = run(&[
        "--url",
        "http://no-such-host.invalid:9099",
        "--admin-url",
        &admin_url,
        "products",
        "list",
    ]);
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("\"api_unreachable\""), "{}", r.stderr);
    assert!(
        !r.stderr.contains("api_inactive"),
        "a loopback admin surface says nothing about a remote bridge: {}",
        r.stderr
    );
    assert_eq!(
        served.load(Ordering::SeqCst),
        0,
        "the probe must not even be issued against a different host"
    );
}

// -------------------------------------------------------------------------------------------
// diagnose
// -------------------------------------------------------------------------------------------

/// A verdict is a successful report, however bad the news: an agent branches on
/// `.diagnostics.diagnosis.code`, and a nonzero exit here would read as "diagnose itself failed".
#[test]
fn diagnose_exits_zero_on_a_tunnel_down_verdict() {
    let admin_url = mock_server("200 OK", TUNNEL_DOWN);
    let r = run(&[
        "--url",
        &unreachable_url(),
        "--admin-url",
        &admin_url,
        "diagnose",
        "--output",
        "table",
    ]);
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stdout.starts_with("diagnosis: tunnel_down"),
        "the table must lead with the verdict: {}",
        r.stdout
    );
    assert!(r.stdout.contains("disconnected"), "{}", r.stdout);
    assert!(
        r.stdout.contains("no /v1/status"),
        "a /v1 that never answered is reported, not silently rendered as no venues: {}",
        r.stdout
    );
}

/// The one failure that is `diagnose`'s own: with no admin surface there is nothing to report.
#[test]
fn diagnose_exits_3_when_the_admin_surface_is_unreachable() {
    let r = run(&[
        "--url",
        &unreachable_url(),
        "--admin-url",
        &unreachable_url(),
        "diagnose",
    ]);
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("admin_api_unreachable"), "{}", r.stderr);
    assert!(r.stderr.contains("DZ_ADMIN_BIND"), "{}", r.stderr);
}

/// The machine-readable path an agent actually uses.
#[test]
fn diagnose_jq_extracts_the_verdict_code() {
    let admin_url = mock_server("200 OK", TUNNEL_DOWN);
    let r = run(&[
        "--url",
        &unreachable_url(),
        "--admin-url",
        &admin_url,
        "--jq",
        ".diagnostics.diagnosis.code",
        "diagnose",
    ]);
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert_eq!(r.stdout.trim(), "\"tunnel_down\"");
}

// -------------------------------------------------------------------------------------------
// connect / disconnect
// -------------------------------------------------------------------------------------------
