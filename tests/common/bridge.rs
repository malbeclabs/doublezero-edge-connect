//! Spawn the real `doublezero-edge-connect` binary as a subprocess for E2E tests.

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A running bridge subprocess, joined to its feed's multicast group on loopback,
/// serving WebSocket on `ws_addr`. Killed when dropped.
pub struct Bridge {
    child: Child,
    pub ws_addr: String,
}

impl Bridge {
    /// Spawn the binary for one venue, joining its multicast group on INADDR_ANY (the
    /// default interface) and serving WS on `127.0.0.1:ws_port`. Blocks until the WS port
    /// accepts connections.
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
        let child = Command::new(bin)
            .args(["--feed", venue, "--iface", &iface, "--ws-bind", &ws_addr])
            .env("RUST_LOG", "info")
            // Inherit stdio so logs appear in captured test output and the child never
            // blocks on a full pipe (we are not parsing its logs).
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn doublezero-edge-connect");
        let bridge = Self { child, ws_addr };
        bridge.wait_ready(Duration::from_secs(10));
        bridge
    }

    fn wait_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if TcpStream::connect(&self.ws_addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("bridge WS {} not ready within {timeout:?}", self.ws_addr);
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
