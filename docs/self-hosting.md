# Self-hosting (build & run without the one-liner)

> Most operators don't need this. The [one-liner](../README.md#install) prepares the host and
> runs the bridge container for you. This page is for building from source or running the image
> by hand.

Feeds are defined in [`../src/ingest/feeds.rs`](../src/ingest/feeds.rs); add a `Feed` row to
ingest another venue. The full flag/env reference is the `Args` struct in
[`../src/main.rs`](../src/main.rs).

## From source

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

## In Docker

The image bundles the `doublezerod` daemon: the entrypoint brings the daemon up first, then
starts the bridge. Host networking is required to see the multicast group, plus `NET_ADMIN` and
the tun device for the daemon's GRE tunnel:

```bash
docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
  doublezero-edge-connect
```

Any of the bridge's env vars (see [Configure](../README.md#configure-override-the-one-liner))
can be passed with `-e`.

Prebuilt images are published to GHCR, one per DoubleZero environment, each layered on the
matching `doublezero` base image:

| Environment | Image | Moving tag |
|---|---|---|
| mainnet-beta | `ghcr.io/malbeclabs/doublezero-edge-connect` | `:mainnet-beta` (= `:latest`) |
| testnet | `ghcr.io/malbeclabs/doublezero-edge-connect` | `:testnet` |
| devnet (private) | `ghcr.io/malbeclabs/doublezero-edge-connect-devnet` | `:latest` |

Release tags (`vX.Y.Z`) additionally publish pinned `:<env>-X.Y.Z` tags; every build also gets
a `:sha-<commit>` tag for precise pinning.

> **No TLS.** edge-connect targets a trusted/local network (the same stance as the DoubleZero
> overlay). Terminate TLS at a reverse proxy if you must expose it.
