# Solana shred forwarding

Alongside the market-data bridge, an optional **shred forwarder** (under
[`../src/shred/`](../src/shred/)) joins the DoubleZero `edge-solana-*` shred multicast feeds,
combines them, and fans each raw datagram out to one or more **local unicast** UDP destinations
(e.g. a Jito shredstream-proxy listener). By default it forwards **one copy of each shred**
(DoubleZero delivers the same shred on several overlapping multicast groups, so this collapses
the duplicate fan-out). `--shred-dedup-mode` (`DZ_SHRED_DEDUP_MODE`) is the single selector:
`dedup` (default), `sigverify` to additionally signature-verify that one copy (see below), or
`none` to forward every datagram. Each destination gets its own `connect`ed send socket, so an
async ICMP error from a down destination (e.g. nothing listening) stays isolated to that socket
and never drops a datagram bound for a healthy one. `--shred-forward` targets should be
local/fast sinks: sends are sequential per destination, so a slow or remote sink throttles the
whole forwarder (and sheds load); the send sockets pin no egress interface.

It **activates on discovery**: by default it shells out to `doublezero multicast group list
--json-compact` and selects the activated groups whose `code` starts with `--shred-code-prefix`
(default `edge-solana-`), binding each on `--shred-port` (default `7733`). If the CLI is missing,
errors, or finds no matching group, the forwarder stays off. Pass `--shred-source GROUP:PORT`
(repeatable) to override discovery entirely.

## Deduplication

`--shred-dedup-mode` (`DZ_SHRED_DEDUP_MODE`) picks one of three modes — it is the **only** method
selector:

- **Dedup-only** (`dedup`, the **default**): forward exactly **one copy** of each shred, keyed on
  `(slot, index, type)` in a bounded window — a duplicate of an already-forwarded copy is dropped.
  No leader lookup, no signature work, no RPC. This is the cheap suppressor for the
  multicast-overlap duplicates (forgery protection is moot on the trusted DoubleZero network).
- **Dedup + sigverify** (`sigverify`): same dedup, plus the first copy of each key is
  ed25519-verified against its slot leader before being forwarded. Requires `--shred-rpc-url`
  (`DZ_SHRED_RPC_URL`); the leader schedule is fetched from that RPC and cached, and the **next
  epoch is prefetched** so a slot stays resolvable across a rollover with no gap. An invalid copy
  is dropped but leaves the key open so a later valid copy can still win; a slot whose leader
  isn't known fails **closed** — the shred is dropped, never forwarded unverified.
  - **Sigverify never falls back to forwarding.** If the RPC is unreachable at startup the cache
    is empty and the forwarder drops **every** shred until the first schedule loads — an
    unreachable RPC means nothing flows, indefinitely. This is a deliberate reversal of the
    bare/dedup behaviour: if you want forward-when-unverified, use dedup-only. Once the current
    epoch is cached, prefetch keeps it resolvable, so beyond cold start `no_leader` drops only
    occur on a sustained RPC outage past the ~epoch prefetch lead or a garbled schedule. Every
    such drop increments the `no_leader` periodic tally, and the first one logs a `warn!` — the
    signal to watch for a blackout.
  - An RPC URL set in any other mode is **ignored** (logged at startup); sigverify is never
    auto-selected.
- **Bare** (`none`): every datagram is forwarded — duplicates and all (the original behaviour).

The `dedup` and `sigverify` modes share the bounded `(slot, index, type)` window
(`--shred-dedup-window-slots`): keys older than that many slots behind the tip are evicted, so
memory is bounded by `window × shreds-per-slot`.

> ⚠️ The shred/merkle byte offsets are transcribed from the agave shred layout and are **not** yet
> validated against a live `edge-solana-*` hexdump (the same caveat as the repo's unvalidated
> Midpoint/MBO codecs). This affects **both** dedup modes: a misparse mis-keys a shred and could
> over- or under-deduplicate (and, in sigverify mode, mis-verify). Confirm the offsets against a
> captured frame before relying on either in production; the forwarder logs a one-time warning
> when sigverify is on and a periodic tally so a systematic misparse (≈100% "invalid") is obvious.

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--shred-code-prefix` | `DZ_SHRED_CODE_PREFIX` | `edge-solana-` |
| `--shred-port` | `DZ_SHRED_PORT` | `7733` |
| `--shred-forward` (repeatable) | `DZ_SHRED_FORWARD` | `127.0.0.1:20000` |
| `--shred-source` (repeatable) | `DZ_SHRED_SOURCES` | — (override discovery) |
| `--shred-dedup-mode` | `DZ_SHRED_DEDUP_MODE` | `dedup` (one copy per shred; `sigverify` / `none` to change) |
| `--shred-rpc-url` | `DZ_SHRED_RPC_URL` | — (RPC endpoint; required by `sigverify` mode, ignored otherwise) |
| `--shred-dedup-window-slots` | `DZ_SHRED_DEDUP_WINDOW_SLOTS` | `512` (used in `dedup` or `sigverify` mode) |

Configure everything on the run command — there is no config file. The default `dedup` mode needs
no extra flags; switch modes (and pass any other shred config) inline at install/launch time:

```bash
# From source — default dedup-only, just point it at a destination:
./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081 \
  --shred-forward 127.0.0.1:20000

# Switch to sigverify (needs an RPC); or turn dedup off with --shred-dedup-mode none:
./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081 \
  --shred-dedup-mode sigverify --shred-rpc-url https://api.mainnet-beta.solana.com

# Via the installer one-liner the same config is passed as env vars before the pipe:
DZ_SHRED_DEDUP_MODE=sigverify DZ_SHRED_RPC_URL=https://api.mainnet-beta.solana.com \
  DZ_SHRED_FORWARD=127.0.0.1:20000 \
  curl -fsSL https://get.doublezero.xyz/connect | bash

# Or, running the image by hand, as -e env vars:
docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
  -e DZ_SHRED_DEDUP_MODE=sigverify -e DZ_SHRED_RPC_URL=https://api.mainnet-beta.solana.com \
  -e DZ_SHRED_FORWARD=127.0.0.1:20000 \
  doublezero-edge-connect
```

The forwarder reuses `--iface` and `--recv-buf`. Invalid `host:port` / `GROUP:PORT` values fail
fast at startup, and a non-loopback `--shred-forward` target is warned about (it would route raw,
unverified shreds out the default interface, off-box). Shreds are loss-tolerant, so under
forwarder backpressure the **newest** datagram is shed (with a periodic drop-count log) rather
than blocking ingest. Discovery binds every matched group; a group this host isn't actually
receiving on simply stays idle and periodically rejoins (harmless).

Source resolution is **one-shot at startup**: if the `doublezero` CLI isn't ready when the bridge
boots, or a group activates later, those groups aren't picked up until the process restarts
(periodic re-discovery is a follow-up). Once a group is resolved, its receiver survives interface
flap via the rejoin watchdog.
