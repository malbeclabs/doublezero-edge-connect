# shred-proxy

A standalone service that receives Solana **shreds** from DoubleZero's `edge-solana-*` multicast
feeds, **de-duplicates** them, and forwards a single copy of each shred to a local UDP port where a
Solana validator (or a consumer like Jito's shredstream-proxy) can read them.

The same shred arrives on several overlapping multicast groups, so forwarding verbatim would
multiply the traffic. `shred-proxy` collapses those duplicates and emits exactly one copy per shred
(default destination `127.0.0.1:8001`, override with `--forward`).

This is a **workspace member of [`doublezero-edge-connect`](../)** and reuses the bridge crate's
library directly — the receiver, dedup window, shred parser, sigverify and multicast plumbing all
come from `doublezero_edge_connect::shred`. The only logic unique to this binary is **active-group
detection via the kernel routing table**, the reconciler, and the CLI. It pulls in no `doublezero`
CLI and no `solana-*` crates.

## Install (one-liner)

Downloads a prebuilt static binary from GitHub Releases and installs it as a systemd service that
starts on boot:

```bash
curl -fsSL https://get.doublezero.xyz/shred-proxy | bash
```

Configure straight from the one-liner — any `DZ_*` variable set before the pipe is recorded into
`/etc/default/shred-proxy` on a fresh install:

```bash
DZ_FORWARD=127.0.0.1:20000 DZ_DEDUP_MODE=dedup \
  curl -fsSL https://get.doublezero.xyz/shred-proxy | bash
```

Installer environment variables: `SHRED_PROXY_VERSION` (release tag, default `latest`),
`SHRED_PROXY_REPO` (default `malbeclabs/doublezero-edge-connect`), `SHRED_PROXY_NO_START=1` (install
without starting). Requires **Linux/amd64** and **iproute2** (`ip`) for routing-table detection.

Manage the service:

```bash
journalctl -u shred-proxy -f            # follow logs
systemctl status shred-proxy
sudo nano /etc/default/shred-proxy && sudo systemctl restart shred-proxy
sudo /path/to/packaging/uninstall.sh    # remove (leaves /etc/default/shred-proxy)
```

## Active-group detection

Instead of hardcoding which groups to join, the proxy detects the active ones by reading the kernel
routing table: for each candidate IP it runs `ip route get <group>` and considers it active if the
egress interface (`dev …`) is the configured interface (`--iface`, default `doublezero1`). A
subscribed group routes over the tunnel; an unsubscribed one falls back to the default interface, so
the egress interface is the discriminator. The set is re-probed every `--refresh-secs` (30 by
default) and the forwarder restarts when the active groups change.

## Build from source

From the repository root:

```bash
cargo build --release -p shred-proxy      # binary at target/release/shred-proxy
cargo run --release -p shred-proxy         # run with defaults
```

## Usage

```bash
# Defaults: detect active groups and forward one deduplicated copy to 127.0.0.1:8001
shred-proxy

# Pick a different local destination and interface
shred-proxy --forward 127.0.0.1:20000 --iface doublezero1

# Skip detection and pin explicit sources (tests / manual pin)
shred-proxy --iface 0.0.0.0 --source 239.255.0.1:17733 --forward 127.0.0.1:8001

# Forward everything without deduplicating
shred-proxy --dedup-mode none

# Dedup + verify the forwarded copy against its slot leader (needs an RPC endpoint)
shred-proxy --dedup-mode sigverify --rpc-url https://api.mainnet-beta.solana.com
```

Each flag also has a `DZ_*` environment-variable equivalent:

| Flag | Default | Description |
|------|---------|-------------|
| `--candidate-group` | the 5 edge-solana IPs | Multicast IPs to probe in the routing table |
| `--port` | `7733` | UDP port shared by the groups |
| `--source` | — | Explicit sources `GROUP:PORT`; skips detection |
| `--forward` | `127.0.0.1:8001` | Local fan-out destination(s) |
| `--iface` | `doublezero1` | Interface for the join and for detection matching |
| `--recv-buf` | `8388608` | SO_RCVBUF per receiver socket |
| `--dedup-mode` | `dedup` | `dedup` \| `sigverify` \| `none` |
| `--rpc-url` | — | Solana JSON-RPC for the leader schedule (required by `sigverify`) |
| `--dedup-window-slots` | `512` | Dedup window depth |
| `--refresh-secs` | `30` | Routing-table re-probe interval |

## How it works

```text
N multicast groups → N receiver tasks → bounded queue → 1 forwarder (dedup) → fan-out → M destinations
```

The receiver, dedup and fan-out are the bridge library's `shred::run`. Shreds are loss-tolerant, so
under backpressure the proxy sheds load rather than blocking.

## License

Licensed under the **Apache License 2.0** (see the repository [LICENSE](../LICENSE)).
