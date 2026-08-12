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

1. Checks preconditions (Linux/amd64, root or `sudo`) — including whether a **host-level
   `doublezerod` already holds UDP `44880`**. The container runs its own client, and the two cannot
   share that port, so the installer offers to stop the host daemon (which disconnects any tunnel it
   owns) and refuses to start rather than leaving you a container that exits seconds later. Answer
   non-interactively with `DZ_STOP_HOST_DAEMON=1|0`.
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
5. Once the container is up, offers to install the **[`doublezero-edge` CLI](#query-market-data-the-doublezero-edge-cli)**
   (a signed, dependency-free package) — naming the Cloudsmith repository and package before
   touching anything. Decline at the prompt, or set `DZ_INSTALL_CLI=0`; `DZ_INSTALL_CLI=1` /
   `DZ_ASSUME_YES=1` accepts non-interactively. Declining, or any package-manager failure, only
   warns — the container is already up either way.

> **Attendantless.** The only input is the access secret. Provide it via `DZ_SECRET` to run with
> no prompts; otherwise you're prompted once. Everything else has a default.

Requirements: **Linux/amd64**, GRE connectivity (allow IP protocol 47 at the cloud provider; on
AWS disable the ENI source/dest check), and a host public IP authorized onchain for the chosen
environment. If the host runs a **default-deny-incoming** firewall, also admit the decapsulated
inner multicast on the tunnel interface (e.g. `sudo ufw allow in on doublezero1`) — allowing GRE
alone isn't enough, since the inner UDP re-traverses `INPUT` on `doublezero1` after decapsulation.
See [scripts/README.md](scripts/README.md) for the full requirements and caveats.

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
| `DZ_PUBLISHER_PORTS` | *(all)* | Comma-separated publisher **base ports** (the market-data port of each block, e.g. `9201`) to narrow which mirrors of each selected feed are ingested. One receiver runs per publisher, so this caps ingest cost on a multi-publisher venue. Base ports are unique within a feed but not across feeds — pair with `DZ_FEEDS` to scope to one venue. |
| `DZ_CHANNELS` | *(all)* | Channels to ingest, scoped per group code (`code=id,id;code=id`, e.g. `lashay-4=10,11`). An unmentioned feed ingests every channel. Only applies to a feed whose publisher derives a port per channel — an excluded channel's socket is never bound, so its traffic never reaches userspace. Ids are validated against the loaded registry at startup; an unknown id or code, or a narrowing of a feed whose publishers share one base port, is refused rather than silently filtering nothing. Can also be changed at runtime — see the admin surface below. |
| `DZ_FEED_REGISTRY_URL` | hosted URL (**image default**) | URL to fetch the feed registry document from at startup — the image sets this to `https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json`. On any failure (unreachable, malformed, an unsupported `version`, a validation error) the built-in document is used instead and a warning is logged — never fatal. See [Feed registry](#feed-registry) below for how to tell which source actually loaded. |
| `DZ_FEED_REGISTRY` | *(built-in)* | Path to a feed registry document, for an air-gapped/locked-down host. The bridge tries `DZ_FEED_REGISTRY_URL` first when it's non-empty, so the installer clears the image's default URL for you whenever you set this without also setting a URL of your own — otherwise the file would be silently shadowed. **This path is read inside the container**, so the installer only forwards it when it can also bind-mount the same file from the host at the identical path (read-only); if the host path doesn't exist it aborts before starting the container rather than passing a path that would silently resolve to nothing. Unlike the URL source, a bad or missing document here is **fatal** at container startup — it is an explicit operator instruction. |
| `DZ_ADMIN_BIND` | `127.0.0.1:9098` | Bind address for the **admin surface** (`GET`/`POST /admin/channels`), the one runtime-mutation path — it lets `DZ_CHANNELS` be replaced without a restart (see [Admin surface](#admin-surface-runtime-channel-changes) below). On by default, at loopback; **this surface has no authentication**, and under the container's host networking a wildcard bind is genuinely reachable off the host, so if you override this, stay on loopback — never a bare wildcard. Set empty to disable it outright. Loopback alone does not stop a browser page on the same host from POSTing here, so `POST` also requires an `X-DZ-Admin-Request` header (any value); `doublezero-edge channels set` sends it automatically. |
| `DZ_SHRED_*` | *(auto)* | Solana shred forwarder config (`DZ_SHRED_DEDUP_MODE`, `DZ_SHRED_FORWARD`, `DZ_SHRED_RPC_URL`, …). Forwarding activates on discovery of `edge-solana-*` groups; these tune it. See [shred forwarding](docs/shred-forwarding.md). |
| `DZ_ASSUME_YES` | `0` | Skip confirmation prompts (e.g. the Docker install prompt) and imply "yes" to the `doublezero-edge` CLI install offer too. |
| `DZ_INSTALL_CLI` | *(prompted)* | Answer the `doublezero-edge` CLI install offer non-interactively: `1` installs, `0` skips. Overridden by `DZ_ASSUME_YES=1`. |
| `DZ_CLIENT_IP` | *(auto-detected)* | Override the host public IP used by the access-pass pre-check (set if auto-detection is wrong). |
| `DZ_LEDGER_RPC_URL` | per env | Override the DoubleZero ledger RPC the access-pass pre-check queries. |
| `DZ_GHCR_TOKEN` | — | **devnet only**, required: a GHCR token with `read:packages` (the devnet image is private). |
| `DZ_GHCR_USER` | `malbeclabs` | **devnet only**, optional: the GHCR username for the login. |

**Bridge variables.** The installer relays **any** non-empty bridge env var straight through to
the container, so the bridge is tuned entirely from the one-liner. Common ones: `DZ_IFACE`,
`DZ_RECV_BUF`, `WS_BIND` and the `WS_*` limits, `METRICS_BIND` (turn on the Prometheus `/metrics`
endpoint — off by default), `DZ_API_BIND` (the read-only `/v1` query API), `DZ_ADMIN_BIND` and
`DZ_CHANNELS` (the runtime-mutable channel filter — see [Admin
surface](#admin-surface-runtime-channel-changes)), `DZ_FEED_REGISTRY`/`DZ_FEED_REGISTRY_URL` (the
feed registry document sources — see the table above), `RUST_LOG`, the shred forwarder's
`DZ_SHRED_*` (notably `DZ_SHRED_DEDUP_MODE` and `DZ_SHRED_RPC_URL`), and the reconciler's
`DZ_SUBSCRIPTION_REFRESH_SECS` / `DZ_SUBSCRIPTION_GATING_DISABLE`. The full list with defaults is
the `Args` struct in [`src/main.rs`](src/main.rs); per-feature config lives in the
[docs](docs/) (see below).

> **Subscription-driven activation.** The bridge only runs the feeds this host is actually
> subscribed to: a reconciler polls `doublezero status` every `DZ_SUBSCRIPTION_REFRESH_SECS`
> (default 30) and activates/deactivates market-data receivers, the shred forwarder, and the
> WebSocket sink as subscriptions change. The **WebSocket sink comes up only when a market-data feed
> is subscribed** — so a shreds-only host serves no WS (and won't collide with an existing `:8081`
> service) with no config. Running from source without the `doublezero` CLI, gating falls open to
> the static always-on behaviour; `DZ_SUBSCRIPTION_GATING_DISABLE=1` forces that model explicitly.
>
> A venue whose feeds ride separate groups is gated per group, and its `trade` tape **follows
> whichever of them is active** — so a host subscribed only to a venue's depth feed still gets a tape,
> and the pick moves without restarting the receiver that keeps it. See
> [Input sources](docs/input-sources.md).

> **Logging defaults.** Unset, `RUST_LOG` defaults to `warn,doublezero_edge_connect=info`: the
> bridge's own startup/operational lines stay at `info` while noisy dependency chatter is held to
> `warn`. Set `RUST_LOG=debug` for verbose output. The installer also caps the container log on
> disk (json-file driver, ~60 MB ceiling) so it can't fill the host.

> **Note:** only **non-empty** values are forwarded, with one exception: `WS_BIND` is forwarded
> whenever it is *set* — including set-but-empty — so `WS_BIND="" curl … | bash` disables the
> WebSocket sink straight from the one-liner. The installer also runs a host-side **port
> preflight**: if the WS port is already taken it warns and (interactively) offers to pick another
> port, disable the sink, or continue. Even if a conflict slips through, a WS bind failure is
> non-fatal — the bridge logs it and keeps running (the tunnel and shred forwarding are
> unaffected). A hand-written `docker run` is still an option — see
> [Self-hosting](docs/self-hosting.md).

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

## Upgrade & remove

The CLI versions independently of the bridge image, so upgrade and remove them separately:

| | Bridge (container) | `doublezero-edge` CLI (package) |
|---|---|---|
| Upgrade | `docker pull` the new image, then re-run the one-liner (or your own `docker run`) | `sudo apt upgrade doublezero-edge` / `sudo dnf upgrade doublezero-edge` |
| Remove | `docker stop`/`rm` the container (see [Manage](#manage)) — leaves the CLI and its Cloudsmith repo config untouched | `sudo apt remove doublezero-edge` / `sudo dnf remove doublezero-edge` — leaves the container and the repo config untouched |

Removing the Cloudsmith repository itself (not just the package) is a normal `apt`/`dnf` source
removal — see your package manager's docs; nothing here manages that file.

> **Version skew is fine.** `/v1` and `/admin` are additive-only and both sides ignore unknown
> fields, so an older CLI against a newer bridge just doesn't see the new field, and a newer CLI
> against an older bridge falls back when a field is absent. Upgrade either half whenever.

## Feed registry

The document that maps venues to multicast groups/ports (`src/ingest/registry.rs`). The image's
`DZ_FEED_REGISTRY_URL` default points at the hosted copy:

```
https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json
```

served fresh at every container start — no rebuild needed to pick up a new venue or port. Override
with your own `DZ_FEED_REGISTRY_URL`, or with a bind-mounted file via `DZ_FEED_REGISTRY` (the
installer clears the default URL for you in that case — see the table above). A host that can't
reach the URL falls back to the built-in copy **silently by design**; the bridge's own startup log
says which source actually won, and the one-liner echoes it for you:

```
==> Feed registry: source="url https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json" version=1 rows=6 receivers=56
```

That line only appears at startup, so `/v1/status` reports it too — which is how you check a
running process, or compare a fleet, without reading logs on each box:

```bash
doublezero-edge status --jq '.registry'
# {"source":"url https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json",
#  "version":1,"rows":3,"receivers":33}
```

A `source` of `built-in` on a host you expected to be using the hosted document means the fetch
failed and it fell back — the string says which.

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

## Query market data (the `doublezero-edge` CLI)

Prefer polling a candle or the current book over consuming the WebSocket stream? The bridge also
serves a read-only `/v1` HTTP API (on by default at `127.0.0.1:9099`, activated whenever a
market-data feed is — see [Output sinks](docs/output-sinks.md#query-api-v1)), and
**[`doublezero-edge`](doublezero-edge/)** is a small CLI client for it, built for scripting and for
an agent to drive directly.

The installer offers it once the bridge is up; on a host that only queries a remote bridge, install
it on its own — it is a signed package in the same repository the DoubleZero client comes from, so
there is no second key to trust:

```bash
sudo apt install doublezero-edge      # or: sudo dnf install doublezero-edge
doublezero-edge status --output table
doublezero-edge products list --jq '.products | length'
doublezero-edge products candles 'KALSHI:KXBTCPERP' granularity==ONE_MINUTE --output table
```

Point it at another host with `--url http://<host>:9099` (or `DOUBLEZERO_EDGE_URL`); it needs no
Docker and no keys. To build from source instead:

```bash
cargo build --release -p doublezero-edge
```

Six commands read `/v1` (all `GET`s), plus a `channels` group that talks to a **separate** surface:

| Command | What it returns |
|---|---|
| `products list` | The product catalog (`limit`/`cursor` query params; `--paginate` follows cursors and accumulates every page) |
| `products get <id>` | One product's identity and registry-derived fields |
| `products ticker <id>` | Recent trades plus best bid/ask |
| `products candles <id>` | OHLCV candles (`granularity`/`limit` query params) |
| `products book <id>` | The order book |
| `products best_bid_ask [<id>]` | Best bid/ask, across every product or (with an id / `product_ids==A,B`) a filtered set |
| `status` | Per-venue feed health, plus `history` (history-store occupancy), `channels` (per-feed channel-filter admission, real bound state and product counts) and `process` (resident memory, CPU time) blocks |
| `channels list` | The channel filter in force, and what it admits/binds per feed (talks to the admin surface, not `/v1`) |
| `channels set <spec>` | Replace the channel filter (same syntax as `--channels`/`DZ_CHANNELS`); previews what would be dropped and asks for confirmation unless `--force` (talks to the admin surface, not `/v1`) |

`<id>` is `SOURCE:SYMBOL` (e.g. `HYPERLIQUID:BTC`); add `#<channel>.<instrument_id>` only if a bare
symbol collides across markets — the CLI reports the candidates when it does. Point it at a
non-default or remote container with `--url` (env `DOUBLEZERO_EDGE_URL`; default
`http://127.0.0.1:9099`):

```bash
doublezero-edge --url http://edge-host:9099 products ticker HYPERLIQUID:BTC
```

**`products`/`status` are read-only, with no exception.** There is no order-placement or mutation
path anywhere in edge-connect for `/v1` to reach, so unlike the trading CLI its surface is modelled
on, neither ever needs a confirmation prompt. **Candles and tickers cover a rolling one hour, held
in memory** — the window does not survive a bridge restart and nothing here is ever written to disk.

**`channels` is the one exception**, and talks to a different, off-by-default surface entirely — see
[Admin surface](#admin-surface-runtime-channel-changes) below.

### Install the CLI alone

Only querying a remote bridge over `--url` and never running the container on this host? Skip the
one-liner and install just the package — a signed, dependency-free deb/rpm — from the same
repository the bridge's own one-liner offers:

```bash
# deb hosts (Ubuntu/Debian):
curl -1sLf https://dl.cloudsmith.io/public/malbeclabs/doublezero-mainnet-beta/setup.deb.sh | sudo -E bash
sudo apt install doublezero-edge

# rpm hosts (Fedora/RHEL): swap setup.deb.sh -> setup.rpm.sh and apt install -> dnf/yum install
```

Testnet package: swap `doublezero-mainnet-beta` for `doublezero-testnet` (mainnet-beta serves
devnet too — the CLI has no ledger coupling).

### Admin surface (runtime channel changes)

Beyond `/v1`, the bridge serves an **admin** surface — `GET`/`POST /admin/channels` — the one
runtime-mutation path in edge-connect, letting the channel filter (`--channels`/
`DZ_CHANNELS`) be replaced without a restart. It is **on by default, at loopback**
(`--admin-bind`/`DZ_ADMIN_BIND` defaults to `127.0.0.1:9098`; set empty to disable it outright) and
deliberately separate from `/v1`, which stays provably read-only.

```bash
./target/release/doublezero-edge-connect --iface doublezero1 --admin-bind 127.0.0.1:9098
doublezero-edge channels list
doublezero-edge channels set 'lashay-4=10,11'
```

`GET` reports the channel filter in force and, per feed, which publishers/channels it admits (not
necessarily **bound** — a feed's group must also be subscribed for an admitted publisher to actually
receive traffic; `status` above reports real liveness). `POST ?channels=<spec>` validates through the
exact same parser `--channels`/`DZ_CHANNELS` uses at startup, so nothing here can admit a feed startup
would have refused, and it takes effect on the reconciler's next pass — which is also what drops a
departing channel's catalog, book and history state, so narrowing the channel filter at runtime is
irreversible within the history window.

> **No authentication.** This surface has none, and under the container's `--network host`, a
> wildcard bind is genuinely reachable from the network, same as `/v1`. **Bind it to loopback**
> (`127.0.0.1:9098`, `doublezero-edge`'s own default), never a bare wildcard, unless you have your
> own network-level access control in front of it. Loopback alone does not stop a web page open in
> a browser on this host from POSTing a form to `/admin/channels` — the request would originate
> from the host itself. `POST` therefore also requires an `X-DZ-Admin-Request` header (any value is
> fine — a form post cannot set an arbitrary header, so requiring one is enough). `doublezero-edge
> channels set` sends it for you; a raw `curl` needs `-H 'X-DZ-Admin-Request: 1'`.

The full flag reference (`--jq`, `--template`, `--output table`) is in `--help`. Note:
`doublezero-edge` builds and runs on macOS as well as Linux; the bridge itself does not (it uses
`SO_TIMESTAMPNS` via `nix` with no `cfg` gate), which is why the CLI is a separate workspace member.

## Standalone shred-proxy

Shreds-only host and don't need the market-data bridge? The **[`shred-proxy/`](shred-proxy/)**
workspace member is a lightweight service that joins the `edge-solana-*` shred feeds, deduplicates,
and forwards to a local UDP port — no Docker, no `doublezero` CLI. Install it with its own one-liner:

```bash
curl -fsSL https://get.doublezero.xyz/shred-proxy | bash
```

It reuses this crate's shred forwarder directly; see **[shred-proxy/README.md](shred-proxy/README.md)**.

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
