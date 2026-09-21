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
`--feed-registry-url`/`DZ_FEED_REGISTRY_URL` yourself.

The hosted document is `src/ingest/registry.json` from this repo, published by
`.github/workflows/release.feed-registry.yml` on every change to it on `main`, with an immutable
per-commit copy alongside at `…/feeds/doublezero-edge-feeds-<sha>.json` to pin or roll back to. It
is published only after the loader that reads it has validated it, because a document the fleet
cannot use is not rejected by the fleet: each host warns once and degrades to its own built-in
copy, silently, one at a time as containers restart. Override with a different URL, or with
`--feed-registry <path>`/`DZ_FEED_REGISTRY <path>` (a bind-mounted file, in Docker) — note the
bridge tries the URL first when it's non-empty, so pass an empty `--feed-registry-url ""` alongside
the file if you've also set a URL. A URL that can't be reached or fails validation falls back to
the built-in document silently by design; check `sudo docker logs <container> | grep 'feed
registry resolved'` (or the equivalent for a bare process) to see which document actually loaded.

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

⚠️ **With one ordering constraint** — and it is not specific to `sources`. A binary that predates
the block has no `sources` field, so it warns about `$.sources` and ignores it — and then validates
the rows against its own compiled-in table, where the new venue does not resolve. Under a URL origin
that rejection degrades the **whole** document to the built-in copy, so that host loses every other
feed-row change in the same republish, not just the new source.

### Ordering: any change an older deployed binary rejects

A new `sources` assignment is one instance of a wider class, and the class is what the rule is
about: **a document change that an already-deployed binary rejects at parse or validation degrades
that host to its built-in copy, whatever the field.** The others the schema allows today:

- a **`version` bump** — the loader's check is an equality test, not a floor, so a binary that
  understands `1` rejects `2` outright rather than reading what it recognizes;
- a new **`kind`** or **`arbitration`** value — both are closed enums, so an unknown variant fails
  the parse of the *whole document*, not of the row that carries it.

So until the fleet is upgraded, a document may only use values every deployed binary already
accepts; introducing one is a release *and* a republish, in that order.

Since the document is published from `main`, "in that order" means the release has to be out and the
fleet upgraded **before the document change merges** — merging is the republish, and there is no
later step at which to hold it back.

⚠️ **The publisher's validation gate does not cover this class**, and cannot: it runs the loader
from the commit being published, so a change that bumps `SUPPORTED_VERSION` and the document's
`version` together compiles a binary that supports the new value, passes the gate, and republishes a
document the whole *running* fleet rejects. The gate proves the document loads in the build it
shipped with; the ordering above is what makes it load in the builds already out there.
