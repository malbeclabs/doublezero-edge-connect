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

For a long-lived, detached deployment, cap the container log on disk so it can't fill the host, and
raise the stop timeout so `docker stop` doesn't `SIGKILL` the entrypoint mid-`doublezero disconnect`
(docker's default is 10s; releasing the tunnel and its onchain session can take longer). The
installer's `docker run` does both for you, but a by-hand run should add them too:

```bash
docker run -d --restart unless-stopped --network host --cap-add NET_ADMIN --device /dev/net/tun \
  --stop-timeout 60 \
  --log-driver json-file --log-opt max-size=20m --log-opt max-file=3 \
  doublezero-edge-connect      # ~60 MB log ceiling (20m x 3 rotated files)
```

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

## Feed registry

The image sets `DZ_FEED_REGISTRY_URL` to the hosted document
(`https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json`); building/running from
source instead gets the `clap` default, which is empty — no network call unless you pass
`--feed-registry-url`/`DZ_FEED_REGISTRY_URL` yourself. Override with a different URL, or with
`--feed-registry <path>`/`DZ_FEED_REGISTRY <path>` (a bind-mounted file, in Docker) — note the
bridge tries the URL first when it's non-empty, so pass an empty `--feed-registry-url ""` alongside
the file if you've also set a URL. A URL that can't be reached or fails validation falls back to
the built-in document silently by design; check `sudo docker logs <container> | grep 'feed
registry resolved'` (or the equivalent for a bare process) to see which source actually loaded.

The document also carries the **`sources` block** — the Source ID → registry-name allocation,
generated from `edge-feed-spec/sources/spec.md`, which stays the authority for it:

```json
"sources": [
  { "id": 1, "name": "HYPERLIQUID" },
  { "id": 2, "name": "PHOENIX" },
  { "id": 3, "name": "KALSHI" }
]
```

A name is emitted verbatim as `venue`/`source_name` on the WebSocket and as every `venue=` metric
label value, so it must be uppercase, and an id or a name may appear only once. The block is
**optional**: adding it bumps no schema version, so a document written before it existed still
loads and resolves against the copy compiled into the binary. A Source ID the block does not assign
is not an error — the wire value is authoritative and gets a distinct synthesized `SOURCE_<id>`
label. Assigning a venue is therefore a republish of this document rather than a new release.
