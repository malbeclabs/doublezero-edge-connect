//! Spawn the real `doublezero-edge-connect` binary as a subprocess for E2E tests.

use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

/// Marker logged by `src/ingest/receiver.rs` once the multicast socket is bound and the group is
/// joined. `wait_ready` blocks until this appears so the caller knows it's safe to send datagrams.
const RECEIVER_BOUND_MARKER: &str = "DZ Edge multicast receiver bound";

/// A running bridge subprocess, joined to its feed's multicast group on loopback,
/// serving WebSocket on `ws_addr`. Killed when dropped.
pub struct Bridge {
    child: Child,
    pub ws_addr: String,
}

impl Bridge {
    /// Spawn the binary for one venue, joining its multicast group on INADDR_ANY (the
    /// default interface) and serving WS on `127.0.0.1:ws_port`. Blocks until both the
    /// WS port accepts connections AND at least one multicast receiver has logged its
    /// ready marker (see `RECEIVER_BOUND_MARKER`), so callers can immediately send
    /// datagrams without a bare sleep.
    ///
    /// `--iface 0.0.0.0` joins on the default interface rather than `lo`. Combined with the
    /// sender's multicast-loopback (see `replay::multicast_sender`), locally-sent datagrams
    /// are delivered without requiring `lo` to carry multicast — the portable choice for
    /// containers and CI. Override with `DZ_E2E_IFACE` if a specific environment needs a
    /// named interface (e.g. `eth0`).
    pub fn spawn(venue: &str, ws_port: u16) -> Self {
        let bin = env!("CARGO_BIN_EXE_doublezero-edge-connect");
        let ws_addr = format!("127.0.0.1:{ws_port}");
        let iface = std::env::var("DZ_E2E_IFACE").unwrap_or_else(|_| "0.0.0.0".to_string());
        let mut child = Command::new(bin)
            .args(["--feed", venue, "--iface", &iface, "--ws-bind", &ws_addr])
            .env("RUST_LOG", "info")
            // Capture stdout to watch for the receiver-bound marker; keep stderr inherited so
            // error/warn lines surface in test output immediately without buffering.
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn doublezero-edge-connect");

        // The reader thread tees each stdout line to the test's stdout and signals via a
        // condvar once the receiver-bound marker is seen. The pipe is consumed by the thread
        // for the lifetime of the process, so it never fills and deadlocks.
        let stdout = child.stdout.take().expect("stdout was piped");
        let ready_flag: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        let ready_flag_clone = Arc::clone(&ready_flag);
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => break,
                };
                // Tee to test stdout so tracing logs remain visible in `cargo test -- --nocapture`.
                println!("{line}");
                if line.contains(RECEIVER_BOUND_MARKER) {
                    let (lock, cvar) = &*ready_flag_clone;
                    *lock.lock().unwrap() = true;
                    cvar.notify_all();
                }
            }
        });

        let bridge = Self { child, ws_addr };
        bridge.wait_ready(Duration::from_secs(10), ready_flag);
        bridge
    }

    fn wait_ready(&self, timeout: Duration, ready_flag: Arc<(Mutex<bool>, Condvar)>) {
        let deadline = Instant::now() + timeout;

        // Poll WS port until it accepts.
        loop {
            if Instant::now() >= deadline {
                panic!("bridge WS {} not ready within {timeout:?}", self.ws_addr);
            }
            if TcpStream::connect(&self.ws_addr).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        // Wait for at least one "receiver bound" log line.
        let (lock, cvar) = &*ready_flag;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (guard, timed_out) = cvar
            .wait_timeout_while(lock.lock().unwrap(), remaining, |ready| !*ready)
            .unwrap();
        drop(guard);
        if timed_out.timed_out() {
            panic!(
                "bridge receiver never logged '{RECEIVER_BOUND_MARKER}' within {timeout:?} \
                 (WS {} accepted but multicast group not yet joined)",
                self.ws_addr
            );
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
