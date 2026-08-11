//! Test-only scaffolding shared by the CLI-level integration tests: a minimal one-shot-per-request
//! HTTP server (so a test can hand the built binary a canned status + JSON body without needing a
//! real edge-connect container) and a helper to grab a port nothing is listening on.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
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
