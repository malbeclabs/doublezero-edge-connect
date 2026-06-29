# doublezero-edge-connect

**Run one command, get a normalized market-data WebSocket off the DoubleZero Edge.**

```bash
curl -fsSL https://get.doublezero.xyz/connect | bash
```

`doublezero-edge-connect` is the bridge an operator runs to turn the DoubleZero (DZ) Edge
**binary multicast** feeds into something a trading engine can read. It connects to the
[DoubleZero Edge](https://doublezero.xyz/dz-edge) sources you're authorized for, decodes their
little-endian fixed-size frames, drives the reference-data subscriber state machine (so every
quote carries the precision needed to interpret it), and re-serves the result as one normalized,
**engine-agnostic JSON WebSocket** — venue + symbol tagged on every message, with four latency
timestamps for end-to-end measurement.

```
DZ Edge sources ──multicast──▶  doublezero-edge-connect  ──WebSocket (JSON)──▶  your engine
  (binary, 2 ports/venue)         (decode · normalize)        ws://host:8081      (any WS+JSON engine)
```

The binary multicast, the two-port split, and the manifest/precision handshake all stay on this
side of the bridge. The **only** contract a consumer codes against is the WebSocket JSON, fully
specified in **[PROTOCOL.md](PROTOCOL.md)**.

---

## Install

One command prepares the host and runs the bridge container (it bundles `doublezerod` + the
`doublezero` CLI), joins the DoubleZero network, and serves normalized quotes over a WebSocket on
`:8081`. Pick the one-liner for your environment:

```bash
# mainnet-beta (default)
curl -fsSL https://get.doublezero.xyz/connect | bash

# testnet
curl -fsSL https://get.doublezero.xyz/connect-testnet | bash

# devnet (private image — needs DZ_GHCR_TOKEN, see Configure)
curl -fsSL https://get.doublezero.xyz/connect-devnet | bash
```

What the script does:

1. Checks preconditions (Linux/amd64, root or `sudo`).
2. Loads the access secret (a `DZ_`-prefixed token, or a keypair file path) and **verifies its
   access pass onchain before installing anything** — a pure host-side check (no Docker, no CLI)
   over the ledger's public JSON-RPC. If the identity has no access pass for `0.0.0.0` (the any-IP
   wildcard) nor for the host's public IP, it aborts with a descriptive error when that IP was
   given explicitly via `DZ_CLIENT_IP`, and otherwise (the IP was only auto-detected, which can be
   wrong behind NAT) just warns and continues, leaving `doublezero connect` as the real check.
3. Ensures Docker is present (offers to install it) and preps the host kernel/network for the GRE
   tunnel: loads `tun`/`ip_gre`, raises `net.core.rmem_max`, warns about firewalls and
   cloud-provider rules.
4. Runs the bridge container (`--network host`, `NET_ADMIN`/`NET_RAW`, `/dev/net/tun`) and runs
   `doublezero connect multicast`.

> **Attendantless.** The only input is the access secret. Provide it via `DZ_SECRET` to run with
> no prompts; otherwise you're prompted once. Everything else has a default.

Requirements: **Linux/amd64**, GRE connectivity (allow IP protocol 47 at the cloud provider; on
AWS disable the ENI source/dest check), and a host public IP authorized onchain for the chosen
environment. See [scripts/README.md](scripts/README.md) for the full requirements and caveats.

## Configure (override the one-liner)

All configuration is via **environment variables set before the pipe**. No config file:

```bash
DZ_SECRET=DZ_… DZ_NAME=Custom-Container-Name curl -fsSL https://get.doublezero.xyz/connect | bash
```

**Installer variables:**

| Var | Default | Purpose |
|-----|---------|---------|
| `DZ_SECRET` | *(prompted)* | `DZ_`-prefixed base64 token **or** a path to a keypair file. If set, runs non-interactively. A token is injected into the container and never written to host disk; a file is bind-mounted read-only. |
| `DZ_ENV` | per script | `mainnet-beta` \| `testnet` \| `devnet`. |
| `DZ_IMAGE` | per script | Override the container image. |
| `DZ_NAME` | `doublezero-edge-connect` | Container name. |
| `DZ_FEEDS` | *(all)* | Comma-separated venues to narrow ingestion. Does **not** affect Solana shred forwarding. |
| `DZ_SHRED_*` | *(auto)* | Solana shred forwarder config (`DZ_SHRED_DEDUP_MODE`, `DZ_SHRED_FORWARD`, `DZ_SHRED_RPC_URL`, …). Forwarding activates on discovery of `edge-solana-*` groups; these tune it. See [shred forwarding](docs/shred-forwarding.md). |
| `DZ_ASSUME_YES` | `0` | Skip confirmation prompts (e.g. the Docker install prompt). |
| `DZ_CLIENT_IP` | *(auto-detected)* | Override the host public IP used by the access-pass pre-check (set if auto-detection is wrong). |
| `DZ_LEDGER_RPC_URL` | per env | Override the DoubleZero ledger RPC the access-pass pre-check queries. |
| `DZ_GHCR_TOKEN` | — | **devnet only**, required: a GHCR token with `read:packages` (the devnet image is private). |
| `DZ_GHCR_USER` | `malbeclabs` | **devnet only**, optional: the GHCR username for the login. |

**Bridge variables.** The installer relays **any** non-empty bridge env var straight through to
the container, so the bridge is tuned entirely from the one-liner. Common ones: `DZ_IFACE`,
`DZ_RECV_BUF`, `WS_BIND` and the `WS_*` limits, `METRICS_BIND` (turn on the Prometheus `/metrics`
endpoint — off by default), `RUST_LOG`, and the shred forwarder's `DZ_SHRED_*` (notably
`DZ_SHRED_DEDUP_MODE` and `DZ_SHRED_RPC_URL`). The full list with defaults is the `Args`
struct in [`src/main.rs`](src/main.rs); per-feature config lives in the [docs](docs/) (see below).

> **Logging defaults.** Unset, `RUST_LOG` defaults to `warn,doublezero_edge_connect=info`: the
> bridge's own startup/operational lines stay at `info` while noisy dependency chatter is held to
> `warn`. Set `RUST_LOG=debug` for verbose output. The installer also caps the container log on
> disk (json-file driver, ~60 MB ceiling) so it can't fill the host.

> **Limitation:** only **non-empty** values are forwarded, so you can't pass an *empty* override
> (e.g. `WS_BIND=""` to disable the WebSocket sink) through the installer. For that, run a
> hand-written `docker run` — see [Self-hosting](docs/self-hosting.md).

Examples:

```bash
# Testnet, non-interactive:
DZ_SECRET=DZ_… curl -fsSL https://get.doublezero.xyz/connect-testnet | bash

# Verbose logging + a non-default WebSocket port:
RUST_LOG=debug WS_BIND=0.0.0.0:9000 curl -fsSL https://get.doublezero.xyz/connect | bash

# Shred forwarder with sigverify (dedup-only is the default and needs no vars):
DZ_SECRET=DZ_… DZ_SHRED_DEDUP_MODE=sigverify DZ_SHRED_RPC_URL=https://api.mainnet-beta.solana.com \
  curl -fsSL https://get.doublezero.xyz/connect | bash
```

The complete installer reference (every variable, the devnet GHCR login, keypair handling) is in
**[scripts/README.md](scripts/README.md)**.

## Manage

```bash
sudo docker logs -f doublezero-edge-connect                      # bridge + daemon logs
sudo docker exec -it doublezero-edge-connect doublezero status   # tunnel status
sudo docker exec -it doublezero-edge-connect doublezero latency  # device latencies
sudo docker stop doublezero-edge-connect && sudo docker rm doublezero-edge-connect  # disconnect & remove
```

> **No TLS.** The bridge targets a trusted/local network; terminate TLS at a reverse proxy if you
> expose it.

## Consume Edge Feeds
_For Edge Feeds (not solana-shreds)_

Open a WebSocket to `ws://<host>:8081` and read JSON. You receive only the venues you're authorized
for; an optional `subscribe` control frame narrows the stream further:

```json
{"method":"subscribe","subscription":{"venue":"<venue-name>","symbol":"SOL"}}
```

On connect you first get the current instrument definitions (precision), then a stream of quotes.
Any engine that speaks WebSocket + JSON consumes it with a thin (~50-100 line) adapter. The full
wire contract is in **[PROTOCOL.md](PROTOCOL.md)** (see
[Consuming the feed](PROTOCOL.md#consuming-the-feed-any-engine)).

## Documentation

- **[docs/](docs/)** — operating reference:
  [Self-hosting](docs/self-hosting.md) ·
  [Output sinks](docs/output-sinks.md) ·
  [Metrics](docs/metrics.md) ·
  [Input sources](docs/input-sources.md) ·
  [Shred forwarding](docs/shred-forwarding.md)
- **[PROTOCOL.md](PROTOCOL.md)** — the WebSocket JSON contract (v1).
- **[scripts/README.md](scripts/README.md)** — the installer scripts and full env-var reference.
- **[CLAUDE.md](CLAUDE.md)** — architecture and internals.
- **[DoubleZero Edge](https://doublezero.xyz/dz-edge)** · **[docs](https://docs.doublezero.xyz)** ·
  **[edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)** — the product and upstream feed format.
