# doublezero-edge-connect

**The market data infrastructure crypto never had, now one WebSocket away.**

```bash
curl -fsSL https://get.doublezero.xyz/connect | bash
```

That's the only command you run (testnet: `/connect-testnet`, devnet: `/connect-devnet`). From there everything is automatic: `doublezero-edge-connect`
connects to the [DoubleZero Edge](https://doublezero.xyz/dz-edge) sources you're authorized for,
decodes their binary **multicast** Top-of-Book feeds, and serves them as a single normalized
**WebSocket** that any trading engine can read.

---

## What is DoubleZero Edge?

Institutional finance built dedicated distribution networks for a reason. [DoubleZero](https://doublezero.xyz)
brings that same model to crypto: instead of pulling market data over the public internet,
contributors stitch underutilized private fiber into a global mesh and push packet processing out to
the **edge**, the network hardware where data enters. **DoubleZero Edge** rides that mesh to deliver
market data via **multicast**, the same data-distribution standard traditional finance has used for
decades, now deployed to onchain markets.

The result is the kind of delivery a trading desk actually wants:

- **Reduced variance**: data is published once and replicated *in-network*, not re-sent over N independent connections.
- **Flatter tail latency**: fewer hops, no per-subscriber fan-out from the source.
- **Delivery independent of who you are**: every subscriber on the group receives the same frame at the same time.

## What edge-connect delivers

The Edge feed is *raw*: little-endian fixed-size binary frames on a multicast group, split across two
UDP ports, gated by a reference-data state machine. Great for latency, painful to consume directly.

`doublezero-edge-connect` is the bridge that makes it **trivial to consume from any trading engine**. It:

1. **Connects** to the DZ Edge multicast sources your subscription authorizes, on the `doublezero1` interface.
2. **Decodes** the binary [edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec) Top-of-Book
   frames (validated **byte-for-byte** against the reference Go decoder).
3. **Drives** the reference-data subscriber state machine, so every quote carries the precision you
   need to interpret it.
4. **Re-serves** it all as one normalized, **engine-agnostic JSON WebSocket**: venue + symbol tagged
   on every message, plus four latency timestamps for end-to-end measurement.

```
DZ Edge sources ──multicast──▶  doublezero-edge-connect  ──WebSocket (JSON)──▶  your engine
  (binary, 2 ports/venue)         (decode · normalize)        ws://host:8081      (any WS+JSON engine)
```

The binary multicast, the two-port split, the manifest/precision handshake: all of it stays on this
side of the bridge. The **only** contract you code against is the WebSocket JSON, fully specified in
**[PROTOCOL.md](PROTOCOL.md)**.

## Get started in minutes

No circuit orders. No contract negotiation. No sales call. And no feeds to wire up by hand.

### 1. Subscribe to the sources you need

During purchase you're authorized for specific venues. That authorization is the only thing that
decides which sources you receive; there are no feeds to pick and no flags to set.

### 2. Connect

```bash
curl -fsSL https://doublezero.xyz/install | bash
```

This onboards your host to DoubleZero. From there everything is automatic: the bridge connects to
your authorized sources and starts serving normalized data over the WebSocket. No further commands.

### 3. Consume the feed

Open a WebSocket to `ws://<host>:8081` and read JSON. You receive only the venues you're authorized
for; an optional `subscribe` control frame narrows the stream further:

```json
{"method":"subscribe","subscription":{"venue":"Hyperliquid","symbol":"SOL"}}
```

On connect you first get the current instrument definitions (precision), then a stream of quotes:

```json
{"type":"instrument","venue":"Hyperliquid","symbol":"SOL","price_exponent":-2,"qty_exponent":-2}
{"type":"quote","venue":"Hyperliquid","symbol":"SOL","bid":184.20,"ask":184.21,
 "bid_size":12.5,"ask_size":8.0,"source_ts_ns":...,"kernel_rx_ts_ns":...,
 "recv_ts_ns":...,"ws_send_ts_ns":...}
```

Any engine that speaks WebSocket + JSON consumes it with a thin (~50-100 line) adapter, see
[PROTOCOL.md → Consuming the feed](PROTOCOL.md#consuming-the-feed-any-engine).

---

## Operating the bridge

> Subscribers don't need anything below. Your data is delivered over the WebSocket automatically once
> you're connected. This section is for operators building or self-hosting a bridge instance.

Feeds are defined in [`src/ingest/feeds.rs`](src/ingest/feeds.rs); add a `Feed` row to ingest
another venue.

### From source

```bash
cargo build --release
cargo test                 # codec round-trip + refdata subscriber state machine
cargo clippy --all-targets

./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081
# narrow to specific venues with --feed (repeatable):
./target/release/doublezero-edge-connect --feed Hyperliquid --iface doublezero1
```

A larger kernel receive buffer is recommended for bursty feeds:
`sudo sysctl -w net.core.rmem_max=268435456`.

### In Docker

The image bundles the `doublezerod` daemon: the entrypoint brings the daemon up first, then starts
the bridge. Host networking is required to see the multicast group, plus `NET_ADMIN` and the tun
device for the daemon's GRE tunnel:

```bash
docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
  doublezero-edge-connect
```

Prebuilt images are published to GHCR, one per DoubleZero environment, each layered
on the matching `doublezero` base image:

| Environment | Image | Moving tag |
|---|---|---|
| mainnet-beta | `ghcr.io/malbeclabs/doublezero-edge-connect` | `:mainnet-beta` (= `:latest`) |
| testnet | `ghcr.io/malbeclabs/doublezero-edge-connect` | `:testnet` |
| devnet (private) | `ghcr.io/malbeclabs/doublezero-edge-connect-devnet` | `:latest` |

Release tags (`vX.Y.Z`) additionally publish pinned `:<env>-X.Y.Z` tags; every build
also gets a `:sha-<commit>` tag for precise pinning.

> **No TLS.** edge-connect targets a trusted/local network (the same stance as the DoubleZero
> overlay). Terminate TLS at a reverse proxy if you must expose it.

### Output sinks

The decoded feed is fanned out to one or more **output sinks** (under [`src/sinks/`](src/sinks/)),
each an independent consumer of the internal broadcast running off the ingest hot path, so enabling
one never affects the others and a slow/failed sink can't stall ingest. Every flag also reads from
the env var shown.

| Sink | Default | Enable / disable | Config flags (env) |
|------|---------|------------------|--------------------|
| **WebSocket** (`sinks::ws`) | **on** | on unless `--ws-bind` is empty; `--ws-bind ""` disables it | `--ws-bind` (`WS_BIND`, default `0.0.0.0:8081`) + the `--ws-*` limits |

A sink is active when its key config value is non-empty/present; the WebSocket sink simply ships a
non-empty default bind, so it is on unless you explicitly clear it.

### Solana shred forwarding

Alongside the market-data bridge, an optional **shred forwarder** (under [`src/shred/`](src/shred/))
joins the DoubleZero `edge-solana-*` shred multicast feeds, combines them, and fans each raw datagram
out to one or more **local unicast** UDP destinations (e.g. a Jito shredstream-proxy listener). By
default it forwards **one copy of each shred** (DoubleZero delivers the same shred on several overlapping
multicast groups, so this collapses the duplicate fan-out). `--shred-dedup-mode`
(`DZ_SHRED_DEDUP_MODE`) is the single selector: `dedup` (default), `sigverify` to additionally
signature-verify that one copy (see below), or `none` to forward every datagram. Each destination gets its
own `connect`ed send socket, so an async
ICMP error from a down destination (e.g. nothing listening) stays isolated to that socket and never
drops a datagram bound for a healthy one. `--shred-forward` targets should be local/fast sinks: sends
are sequential per destination, so a slow or remote sink throttles the whole forwarder (and sheds
load); the send sockets pin no egress interface.

It **activates on discovery**: by default it shells out to `doublezero multicast group list
--json-compact` and selects the activated groups whose `code` starts with `--shred-code-prefix`
(default `edge-solana-`), binding each on `--shred-port` (default `7733`). If the CLI is missing,
errors, or finds no matching group, the forwarder stays off. Pass `--shred-source GROUP:PORT`
(repeatable) to override discovery entirely.

**Deduplication.** `--shred-dedup-mode` (`DZ_SHRED_DEDUP_MODE`) picks one of three modes — it is the
**only** method selector:

- **Dedup-only** (`dedup`, the **default**): forward exactly **one copy** of each shred, keyed on
  `(slot, index, type)` in a bounded window — a duplicate of an already-forwarded copy is dropped.
  No leader lookup, no signature work, no RPC. This is the cheap suppressor for the multicast-overlap
  duplicates (forgery protection is moot on the trusted DoubleZero network).
- **Dedup + sigverify** (`sigverify`): same dedup, plus the first copy of each key is
  ed25519-verified against its slot leader before being forwarded. Requires `--shred-rpc-url`
  (`DZ_SHRED_RPC_URL`) — the leader schedule is fetched per epoch from that RPC and cached; an
  invalid copy is dropped but leaves the key open so a later valid copy can still win; a slot whose
  leader isn't known yet (schedule still loading, or outside the cached epoch) fails **open** —
  forwarded but not deduped — so the forwarder never silently drops traffic it can't yet judge. An
  RPC URL set in any other mode is **ignored** (logged at startup); sigverify is never auto-selected.
- **Bare** (`none`): every datagram is forwarded — duplicates and all (the original behaviour).

The `dedup` and `sigverify` modes share the bounded `(slot, index, type)` window
(`--shred-dedup-window-slots`): keys older than that many slots behind the tip are evicted, so memory
is bounded by `window × shreds-per-slot`.

> ⚠️ The shred/merkle byte offsets are transcribed from the agave shred layout and are **not** yet
> validated against a live `edge-solana-*` hexdump (the same caveat as the repo's unvalidated Midpoint/
> MBO codecs). This affects **both** dedup modes: a misparse mis-keys a shred and could over- or
> under-deduplicate (and, in sigverify mode, mis-verify). Confirm the offsets against a captured frame
> before relying on either in production; the forwarder logs a one-time warning when sigverify is on
> and a periodic tally so a systematic misparse (≈100% "invalid") is obvious.

| Flag | Env | Default |
|------|-----|---------|
| `--shred-code-prefix` | `DZ_SHRED_CODE_PREFIX` | `edge-solana-` |
| `--shred-port` | `DZ_SHRED_PORT` | `7733` |
| `--shred-forward` (repeatable) | `DZ_SHRED_FORWARD` | `127.0.0.1:20000` |
| `--shred-source` (repeatable) | `DZ_SHRED_SOURCES` | — (override discovery) |
| `--shred-dedup-mode` | `DZ_SHRED_DEDUP_MODE` | `dedup` (one copy per shred; `sigverify` / `none` to change) |
| `--shred-rpc-url` | `DZ_SHRED_RPC_URL` | — (RPC endpoint; required by `sigverify` mode, ignored otherwise) |
| `--shred-dedup-window-slots` | `DZ_SHRED_DEDUP_WINDOW_SLOTS` | `512` (used in `dedup` or `sigverify` mode) |

Configure everything on the run command — there is no config file. The default `dedup` mode needs no
extra flags; switch modes (and pass any other shred config) inline at install/launch time:

```bash
# From source — default dedup-only, just point it at a destination:
./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081 \
  --shred-forward 127.0.0.1:20000

# Switch to sigverify (needs an RPC); or turn dedup off with --shred-dedup-mode none:
./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081 \
  --shred-dedup-mode sigverify --shred-rpc-url https://api.mainnet-beta.solana.com

# In Docker the same config is passed as env vars on the one-liner:
docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
  -e DZ_SHRED_DEDUP_MODE=sigverify -e DZ_SHRED_RPC_URL=https://api.mainnet-beta.solana.com \
  -e DZ_SHRED_FORWARD=127.0.0.1:20000 \
  doublezero-edge-connect
```

The forwarder reuses `--iface` and `--recv-buf`. Invalid `host:port` / `GROUP:PORT` values fail fast
at startup, and a non-loopback `--shred-forward` target is warned about (it would route raw,
unverified shreds out the default interface, off-box). Shreds are loss-tolerant, so under forwarder
backpressure the **newest** datagram is shed (with a periodic drop-count log) rather than blocking
ingest. Discovery binds every matched group; a group this host isn't actually receiving on simply
stays idle and periodically rejoins (harmless).

Source resolution is **one-shot at startup**: if the `doublezero` CLI isn't ready when the bridge
boots, or a group activates later, those groups aren't picked up until the process restarts (periodic
re-discovery is a follow-up). Once a group is resolved, its receiver survives interface flap via the
rejoin watchdog.

### Input sources

The DZ Edge **multicast** feeds are always-on inputs. A second, optional input is the Hyperliquid
**public** WebSocket feed (`wss://api.hyperliquid.xyz/ws`), which acts as a **backstop**: the edge
feed should win essentially always (that's the product), and the public feed only matters when the
edge feed gaps, stalls, or dies.

Both inputs converge on one shared arbiter that races them per `(venue, symbol)` `source_ts` tick, so
no second dedup stage is needed. In steady state an edge publisher opens each tick first (sub-ms vs.
the public feed's tens of ms over the internet), so the public copy loses the race and is dropped as a
no-op; when the edge gaps, the public copy is the first to cross the floor and fills in. The backstop
needs no health check, and the WebSocket output is identical regardless of which input delivered a
given update.

| Input source | Default | Enable / disable | Config flags (env) |
|--------------|---------|------------------|--------------------|
| **DZ Edge multicast** | **on** | always on | `--feed`/`--iface`/`--recv-buf` (see above) |
| **Hyperliquid public WS** (`ingest::ws_feeder`) | **off** | on when `--ws-input-coins` is non-empty | `--ws-input-coins` (`WS_INPUT_COINS`, e.g. `BTC,ETH`) · `--ws-input-url` (`WS_INPUT_URL`, default `wss://api.hyperliquid.xyz/ws`) |

```bash
# Run the edge multicast feed with the public WS backstop for BTC and ETH:
./target/release/doublezero-edge-connect --feed Hyperliquid --ws-input-coins BTC,ETH
```

The feeder is failure-isolated (its own task with reconnect + exponential backoff; decode/socket
errors are logged and never touch the multicast hot path) and relies on the edge reference data for
precision — it emits a public quote/trade only once that `(venue, symbol)` instrument is known. The
outbound `wss://` client is the one place TLS is used (rustls + bundled webpki roots).

> **Caveat — trade dedup window vs. reconnect lag.** Cross-source trade dedup is a fixed-size
> windowed `trade_id` cache. A long public reconnect can deliver trades whose ids have aged out of
> the window during a high-volume burst, which would re-emit a duplicate trade. Sizing the window
> against the public feed's unbounded-lag failure mode is tracked separately (window-sizing issue);
> until then the window is a compile-time constant.

## Learn more

- **[PROTOCOL.md](PROTOCOL.md)**: the full WebSocket JSON contract (v1).
- **[CLAUDE.md](CLAUDE.md)**: architecture and internals.
- **[DoubleZero Edge](https://doublezero.xyz/dz-edge)** · **[docs](https://docs.doublezero.xyz)**: the product and getting onto the network.
- **[edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)**: the upstream binary feed format.
</content>
