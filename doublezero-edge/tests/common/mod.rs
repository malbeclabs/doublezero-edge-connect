//! Test-only scaffolding shared by the CLI-level integration tests: a minimal one-shot-per-request
//! HTTP server (so a test can hand the built binary a canned status + JSON body without needing a
//! real edge-connect container) and a helper to grab a port nothing is listening on.
//!
//! Each integration-test file is its own compiled binary and pulls in this module separately, so a
//! binary that only needs a subset (e.g. `completion_cli.rs`, which needs just `bin`) would
//! otherwise see the rest as dead code under `-D warnings`.
#![allow(dead_code)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
};

/// Bind an ephemeral listener, serve exactly one request with the given status line and JSON
/// body, then stop. Returns the base URL (`http://127.0.0.1:PORT`) to point the CLI at.
///
/// Deliberately not the shared `sinks::http` scaffolding from the bridge crate: this crate must
/// have no dependency (path or otherwise) on that crate, so this test-only stand-in is a handful
/// of lines of `std::net`, not a reused component.
pub fn mock_server(status_line: &'static str, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            serve_one(&mut stream, status_line, body);
        }
    });
    format!("http://{addr}")
}

fn serve_one(stream: &mut TcpStream, status_line: &str, body: &str) {
    // Drain the request head; content doesn't matter for these tests.
    let mut buf = [0u8; 4096];
    let _ = stream.read(&mut buf);
    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// As [`mock_server`], but also hands back the raw bytes of the one request it served — for a
/// test that needs to assert something about what the CLI actually sent (e.g. the query string),
/// not just how it reacted to the response.
pub fn mock_server_capture(
    status_line: &'static str,
    body: &'static str,
) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    (format!("http://{addr}"), rx)
}

/// Serve a fixed sequence of canned `(status_line, body)` responses, one per request, in order —
/// for a test that drives a caller (e.g. `--paginate`) known to issue more than one request and
/// needs to hand each one a different canned page. Returns the base URL and a shared counter of
/// how many requests were actually served, so a test can assert more than one request happened
/// (a page count of 1 would prove nothing about a pagination flag).
pub fn mock_server_sequence(
    responses: Vec<(&'static str, &'static str)>,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let served = Arc::new(AtomicUsize::new(0));
    let served_writer = served.clone();
    thread::spawn(move || {
        for (status_line, body) in responses {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    serve_one(&mut stream, status_line, body);
                    served_writer.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => break,
            }
        }
    });
    (format!("http://{addr}"), served)
}

/// A `127.0.0.1` address nothing is listening on: bind an ephemeral port, then drop the listener
/// immediately so the port closes but the address is still routable — a connection to it fails
/// fast (connection refused) rather than hanging, which is what the "unreachable" tests need.
pub fn unreachable_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    format!("http://{addr}")
}

/// Path to the compiled `doublezero-edge` binary under test.
pub fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_doublezero-edge")
}
