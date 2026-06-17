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

## Learn more

- **[PROTOCOL.md](PROTOCOL.md)**: the full WebSocket JSON contract (v1).
- **[CLAUDE.md](CLAUDE.md)**: architecture and internals.
- **[DoubleZero Edge](https://doublezero.xyz/dz-edge)** · **[docs](https://docs.doublezero.xyz)**: the product and getting onto the network.
- **[edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)**: the upstream binary feed format.
</content>
