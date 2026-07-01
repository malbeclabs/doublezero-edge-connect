# Installer scripts

The one-liner installers for `doublezero-edge-connect`. Each is a self-contained
`curl … | bash` script served from `https://get.doublezero.xyz/`. They prep the host, run the
bridge container (which bundles `doublezerod` + the `doublezero` CLI), join the DoubleZero
network, and then serve normalized quotes over a WebSocket (`:8081`).

| Script | Environment | One-liner |
|--------|-------------|-----------|
| `connect.sh` | mainnet-beta (default) | `curl -fsSL https://get.doublezero.xyz/connect \| bash` |
| `connect-testnet.sh` | testnet | `curl -fsSL https://get.doublezero.xyz/connect-testnet \| bash` |
| `connect-devnet.sh` | devnet (private image) | `curl -fsSL https://get.doublezero.xyz/connect-devnet \| bash` |

The three scripts are identical except for the default image/environment and, for devnet, a GHCR
login step (the devnet image is private). Edit them **together** — they are served standalone, so
each must stay self-contained (no shared sourced file).

## What they do

1. Check preconditions (Linux/amd64, root or `sudo`).
2. Load the access secret (a `DZ_`-prefixed token or a keypair file path) and **verify its access
   pass onchain before installing anything** — a pure host-side check (no Docker, no CLI) that
   derives the identity, computes the access-pass PDA, and reads it over the ledger's public
   JSON-RPC. If the identity has no access pass for `0.0.0.0` (the any-IP wildcard) nor for this
   host's public IP, the installer aborts with a descriptive error (instead of failing later at
   `connect`) when that IP was supplied explicitly via `DZ_CLIENT_IP`; if the IP was only
   auto-detected (which can be wrong behind NAT) it warns and continues. The check also degrades to
   a warning (and continues) if the public IP can't be determined, the ledger RPC is unreachable,
   the keypair can't be read/parsed, or `python3` is missing.
3. Ensure Docker is present (offer to install it via `get.docker.com`).
4. Prep the host kernel/network for GRE: load `tun`/`ip_gre`, raise `net.core.rmem_max`, warn
   about active firewalls and cloud provider rules (AWS/GCP/Azure).
5. Run the bridge container (`--network host`, `NET_ADMIN`/`NET_RAW`, `/dev/net/tun`), inject the
   keypair, and run `doublezero connect multicast`.
6. Print connection URLs and management hints.

> **Attendantless:** the only input is the access secret. Provide it via `DZ_SECRET` to run with
> **no prompts at all**; otherwise you're prompted once. Everything else has a default.

## Configuration

All configuration is via environment variables set **before** the pipe, e.g.:

```bash
DZ_SECRET=DZ_… DZ_FEEDS=Hyperliquid curl -fsSL https://get.doublezero.xyz/connect | bash
```

### Installer variables

| Var | Default | Purpose |
|-----|---------|---------|
| `DZ_SECRET` | *(prompted)* | `DZ_`-prefixed base64 token **or** a path to a keypair file. If set, runs non-interactively. A token is injected into the container and never written to host disk; a file is bind-mounted read-only. |
| `DZ_ENV` | per script | `testnet` \| `devnet` \| `mainnet-beta`. |
| `DZ_IMAGE` | per script | Override the container image. |
| `DZ_NAME` | `doublezero-edge-connect` | Container name. |
| `DZ_FEEDS` | *(all)* | Comma-separated venues to narrow ingestion (e.g. `Hyperliquid,Phoenix`). |
| `DZ_ASSUME_YES` | `0` | Skip confirmation prompts (e.g. the Docker install prompt). |
| `DZ_CLIENT_IP` | *(auto-detected)* | Override the host public IP used by the access-pass pre-check. Set this if auto-detection picks the wrong IP. |
| `DZ_LEDGER_RPC_URL` | per env | Override the DoubleZero ledger JSON-RPC URL the access-pass pre-check queries. |
| `DZ_GHCR_TOKEN` | — | **devnet only**, required: a GHCR token with `read:packages` (the devnet image is private). |
| `DZ_GHCR_USER` | `malbeclabs` | **devnet only**, optional: the GHCR username for the login. |

### Bridge variables (relayed to the container)

The installer relays **any** of the bridge's own env vars that are set straight through to
`docker run`, so this is all the wiring needed to tune the bridge from the one-liner — no
per-feature flags in the script.

Common ones: `DZ_IFACE`, `DZ_RECV_BUF`, `WS_BIND` and the `WS_*` limits, `RUST_LOG`, and the shred
forwarder's `DZ_SHRED_*` (notably `DZ_SHRED_DEDUP_MODE` — `dedup` by default, `sigverify` / `none`
to change — and `DZ_SHRED_RPC_URL` for sigverify mode). See the bridge's `Args` in
[`../src/main.rs`](../src/main.rs) and the sink/shred/input tables in the
[`docs/`](../docs/) reference for the full list and defaults.

> **Limitation:** only **non-empty** values are forwarded, so you can't pass an *empty* override
> (e.g. `WS_BIND=""` to disable the WebSocket sink) through the installer. For that edge case, run
> a hand-written `docker run` instead.

## Examples

```bash
# Mainnet, narrow to one venue, non-interactive
DZ_SECRET=DZ_… DZ_FEEDS=Hyperliquid \
  curl -fsSL https://get.doublezero.xyz/connect | bash

# Testnet, non-interactive
DZ_SECRET=DZ_… \
  curl -fsSL https://get.doublezero.xyz/connect-testnet | bash

# Devnet (private image)
DZ_SECRET=DZ_… DZ_GHCR_TOKEN=ghp_… \
  curl -fsSL https://get.doublezero.xyz/connect-devnet | bash

# More verbose logging + a non-default WebSocket port
RUST_LOG=debug WS_BIND=0.0.0.0:9000 \
  curl -fsSL https://get.doublezero.xyz/connect | bash

# Shred forwarder with sigverify (dedup-only is the default and needs no vars)
DZ_SECRET=DZ_… DZ_SHRED_DEDUP_MODE=sigverify DZ_SHRED_RPC_URL=https://api.mainnet-beta.solana.com \
  curl -fsSL https://get.doublezero.xyz/connect | bash
```

## After install

```bash
sudo docker logs -f doublezero-edge-connect                  # bridge + daemon logs
sudo docker exec -it doublezero-edge-connect doublezero status   # tunnel status
sudo docker exec -it doublezero-edge-connect doublezero latency  # device latencies
sudo docker stop doublezero-edge-connect && sudo docker rm doublezero-edge-connect  # disconnect, stop & remove
```

The bridge serves:
- **WebSocket** `ws://<host>:8081` — normalized quotes (see [PROTOCOL.md](../PROTOCOL.md)).

## Requirements & caveats

- **Linux / amd64 only.** The image is published for amd64; the bridge needs host networking and
  kernel tunnels.
- **GRE connectivity.** On a cloud host you must also allow IP protocol 47 at the provider level
  (and, on AWS, disable the ENI source/dest check) — the script warns but can't fix this for you.
- **Host firewall (default-deny-incoming).** Allowing GRE (protocol 47) + UDP 44880 admits the
  *outer* encapsulated packets, but the kernel decapsulates them and the *inner* multicast UDP
  re-traverses the `INPUT` chain on the tunnel interface (`doublezero1`), where a default-deny
  policy drops it. Also admit that interface, e.g. `sudo ufw allow in on doublezero1` (or place
  `doublezero1` in a firewalld trusted zone). The installer warns about this when it detects an
  active ufw/firewalld.
- **WebSocket port.** The bridge serves the WS sink on `:8081` by default. If that port is already
  in use the installer's preflight flags it and (interactively) offers to pick another port,
  disable the sink (`WS_BIND=""`), or continue; a bind failure is non-fatal either way (the tunnel
  is unaffected).
- **Access pass.** `doublezero connect` requires the host's public IP to be authorized onchain for
  the chosen environment; otherwise the tunnel won't come up. The installer now checks this **up
  front** (step 2): if the identity has no access pass for the host's public IP or `0.0.0.0` it
  aborts before installing anything when the IP was given via `DZ_CLIENT_IP`, and warns-and-continues
  when the IP was only auto-detected. Provision one with
  `doublezero access-pass set --accesspass-type prepaid --user-payer <IDENTITY> --client-ip <IP>`
  (use `--client-ip 0.0.0.0` to allow any IP). Override the detected IP with `DZ_CLIENT_IP` if needed.
- **No TLS.** The bridge targets a trusted/local network; terminate TLS at a reverse proxy if you
  expose it.
