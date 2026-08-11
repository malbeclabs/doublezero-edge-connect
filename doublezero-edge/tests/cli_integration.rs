//! End-to-end CLI-level tests, driven through the compiled `doublezero-edge` binary (not the
//! library functions directly) — these are the ones that pin the actual exit codes and stderr/
//! stdout split a caller (an agent shelling out to this tool) actually observes.

mod common;

use std::process::Command;

use common::{bin, mock_server, unreachable_url};

struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run(args: &[&str]) -> Run {
    let out = Command::new(bin())
        .args(args)
        .output()
        .expect("spawn doublezero-edge");
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
// Exit-code mapping per error class.
// -------------------------------------------------------------------------------------------

#[test]
fn a_successful_response_exits_zero_with_json_on_stdout() {
    let url = mock_server(
        "200 OK",
        r#"{"products":[{"product_id":"HYPERLIQUID:BTC","source_id":1,"source":"Hyperliquid","symbol":"BTC","channel":0,"instrument_id":41,"price_increment":"0.01","base_increment":"0.00001","status":"online","feed_kind":"top_of_book"}]}"#,
    );
    let r = run(&["--url", &url, "products", "list"]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert!(r.stdout.contains("HYPERLIQUID:BTC"), "{}", r.stdout);
    assert!(
        r.stderr.is_empty(),
        "a success must not write to stderr: {}",
        r.stderr
    );
}

#[test]
fn a_404_from_the_server_exits_2() {
    let url = mock_server(
        "404 Not Found",
        r#"{"error":"product_not_found","message":"No product \"X\".","remediation":"Run `doublezero-edge products list` to see available products."}"#,
    );
    let r = run(&["--url", &url, "products", "get", "HYPERLIQUID:NOPE"]);
    assert_eq!(r.status, 2, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("product_not_found"), "{}", r.stderr);
    assert!(r.stderr.contains("remediation"), "{}", r.stderr);
    assert!(
        r.stdout.is_empty(),
        "an error must not write to stdout: {}",
        r.stdout
    );
}

#[test]
fn a_400_from_the_server_exits_1() {
    let url = mock_server(
        "400 Bad Request",
        r#"{"error":"invalid_granularity","message":"bad value","remediation":"Use one of: ONE_MINUTE, ..."}"#,
    );
    let r = run(&[
        "--url",
        &url,
        "products",
        "candles",
        "HYPERLIQUID:BTC",
        "granularity==BOGUS",
    ]);
    assert_eq!(r.status, 1, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("invalid_granularity"), "{}", r.stderr);
}

#[test]
fn a_409_ambiguous_response_exits_1() {
    let url = mock_server(
        "409 Conflict",
        r#"{"error":"ambiguous_product","message":"matches more than one market","remediation":"Disambiguate using one of the listed candidates.","candidates":["A#1.1","A#1.2"]}"#,
    );
    let r = run(&["--url", &url, "products", "get", "LASHAY:EAVE-27JAN01-YES"]);
    assert_eq!(r.status, 1, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("ambiguous_product"), "{}", r.stderr);
}

/// A CLI-side usage problem (no product id given) never reaches the network, and must land in the
/// same exit-1 bucket as a server-side validation error.
#[test]
fn a_missing_required_product_id_is_a_usage_error_exiting_1() {
    let r = run(&["products", "get"]);
    assert_eq!(r.status, 1, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("product_id"), "{}", r.stderr);
}

/// The one path with no server response to read at all: the transport failure itself must never
/// leak to the caller as a raw connection-error string — it must be the same remediation-carrying
/// envelope shape a server-side error would produce, and exit 3.
#[test]
fn the_unreachable_api_path_synthesizes_the_remediation_envelope_and_exits_3() {
    let url = unreachable_url();
    let r = run(&["--url", &url, "products", "list"]);
    assert_eq!(r.status, 3, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("\"error\": \"api_unreachable\""),
        "{}",
        r.stderr
    );
    assert!(r.stderr.contains("remediation"), "{}", r.stderr);
    assert!(
        r.stderr.contains(&url),
        "message must name the URL that failed: {}",
        r.stderr
    );
    // The defect this guards against: a raw reqwest/hyper error string (which mentions things
    // like "tcp connect error" or "Connection refused (os error 61)") leaking to the caller
    // instead of the synthesized envelope.
    assert!(
        !r.stderr.to_lowercase().contains("os error"),
        "a raw OS-level connection error must not leak past the synthesized envelope: {}",
        r.stderr
    );
}

// -------------------------------------------------------------------------------------------
// --jq extraction against a real response, end to end.
// -------------------------------------------------------------------------------------------

#[test]
fn jq_extracts_a_nested_field_from_a_real_response() {
    let url = mock_server(
        "200 OK",
        r#"{"trades":[{"time_ns":"1","price":"67000.12","size":"0.5"},{"time_ns":"2","price":"66999.00","size":"1.0"}],"best_bid":"67000.00","best_ask":"67001.50"}"#,
    );
    let r = run(&[
        "--url",
        &url,
        "--jq",
        ".trades[0].price",
        "products",
        "ticker",
        "HYPERLIQUID:BTC",
    ]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert_eq!(r.stdout.trim(), "\"67000.12\"");
}

#[test]
fn jq_streams_one_line_per_element_on_an_iterate() {
    let url = mock_server(
        "200 OK",
        r#"{"products":[{"product_id":"A:X","source_id":1,"source":"A","symbol":"X","channel":0,"instrument_id":1,"price_increment":"1","base_increment":"1","status":"online","feed_kind":"top_of_book"},{"product_id":"A:Y","source_id":1,"source":"A","symbol":"Y","channel":0,"instrument_id":2,"price_increment":"1","base_increment":"1","status":"online","feed_kind":"top_of_book"}]}"#,
    );
    let r = run(&[
        "--url",
        &url,
        "--jq",
        ".products[].product_id",
        "products",
        "list",
    ]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    let lines: Vec<&str> = r.stdout.lines().collect();
    assert_eq!(lines, vec!["\"A:X\"", "\"A:Y\""]);
}

// -------------------------------------------------------------------------------------------
// --template makes no network call and never crashes against --output table.
// -------------------------------------------------------------------------------------------

/// Regression: `--template` prints a small flat parameter doc, not a real endpoint response — it
/// must never be run through the endpoint's strict table renderer, which expects fields (like
/// `status`'s `history`) the template document doesn't carry.
#[test]
fn template_with_output_table_does_not_crash_and_makes_no_request() {
    // No --url is given and the default points at a port nothing is listening on in this test
    // environment; if --template made a request, this would hang or fail as unreachable instead
    // of succeeding immediately.
    let r = run(&[
        "--url",
        &unreachable_url(),
        "status",
        "--output",
        "table",
        "--template",
    ]);
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert_eq!(r.stdout.trim(), "{}");
}

#[test]
fn template_respects_a_jq_filter() {
    let r = run(&[
        "--url",
        &unreachable_url(),
        "products",
        "candles",
        "HYPERLIQUID:BTC",
        "--template",
        "--jq",
        ".granularity",
    ]);
    assert_eq!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stdout.trim().starts_with("\"one of ONE_MINUTE"),
        "{}",
        r.stdout
    );
}

// -------------------------------------------------------------------------------------------
// --help states the tool is read-only (Step 3 of the brief).
// -------------------------------------------------------------------------------------------

#[test]
fn help_long_description_states_the_tool_is_read_only() {
    let out = Command::new(bin()).arg("--help").output().expect("spawn");
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.to_uppercase().contains("READ-ONLY"), "{text}");
}
