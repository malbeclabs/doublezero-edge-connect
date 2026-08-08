# Input sources

The DZ Edge **multicast** feeds are always-on inputs. A second, optional input for some feeds are a
**public** WebSocket feed which acts as a **backstop**: the edge feed should win essentially always, 
so the public feed only matters when the edge feed gaps, stalls, or dies.

Both inputs converge on one shared arbiter that races them per `(venue, symbol)` `source_ts` tick,
so no second dedup stage is needed. In steady state an edge publisher opens each tick first (sub-ms
vs. the public feed's tens of ms over the internet), so the public copy loses the race and is
dropped as a no-op; when the edge gaps, the public copy is the first to cross the floor and fills
in. The backstop needs no health check, and the WebSocket output is identical regardless of which
input delivered a given update.

| Input source | Default | Enable / disable | Config flags (env) |
|--------------|---------|------------------|--------------------|
| **DZ Edge multicast** | **on** | always on | `--feed` selects feed rows by venue · `--publisher-port` narrows the publishers within them by base port (default: every publisher of every selected feed) · `--iface`/`--recv-buf` |
| **Hyperliquid public WS** (`ingest::ws_feeder`) | **off** | on when `--ws-input-coins` is non-empty | `--ws-input-coins` (`WS_INPUT_COINS`, e.g. `BTC,ETH`) · `--ws-input-url` (`WS_INPUT_URL`, default `wss://api.hyperliquid.xyz/ws`) |
| **Phoenix public WS** (`ingest::phoenix_feeder`) | **off** | on when `--phoenix-ws-input-markets` is non-empty | `--phoenix-ws-input-markets` (`PHOENIX_WS_INPUT_MARKETS`, bare tickers e.g. `SOL,BTC`) · `--phoenix-ws-input-url` (`PHOENIX_WS_INPUT_URL`, default `wss://perp-api.phoenix.trade/v1/ws`) |

One receiver task runs per `(venue, protocol, publisher)`, so an eleven-publisher venue runs eleven
receivers per protocol. Each is a full receiver — and for Market-by-Order a full independent book —
so `--publisher-port` (`DZ_PUBLISHER_PORTS`) is the release valve for capping ingest cost or
excluding a misbehaving publisher. A publisher is named by its **base port** — the market-data port
of its block — which is unique within a feed but not across feeds, so pair it with `--feed` to scope
the narrowing to one venue.

> **Size `--recv-buf` against the socket count, not one socket.** Every port of every publisher is
> its own socket requesting `--recv-buf` (default 8 MiB): the eleven-publisher Hyperliquid fleet
> binds 55 sockets (11 × 2 Top-of-Book + 11 × 3 Market-by-Order), 57 with Phoenix and 62 with
> Lashay's two single-publisher rows (2 Top-of-Book + 3 Market-by-Price), so the requested
> `SO_RCVBUF` total is ~456 MiB where a single-publisher deployment requested ~40 MiB.
> `net.core.rmem_max` clamps each socket individually and will not catch the aggregate — and the
> value every installer sets (`268435456`) is a per-socket ceiling well above the 8 MiB default, so
> it will not bound this either. Lower `DZ_RECV_BUF` or narrow `--publisher-port` if that exceeds the
> host's or container's memory budget. Market-by-Order additionally holds one independent L3 book set
> per publisher, so book memory also scales with the publisher count.

**Market-by-Price** (frame magic `0x4442`) is a further multicast protocol alongside Top-of-Book, Midpoint and Market-by-Order. Like Market-by-Order it binds three ports per publisher — mktdata (level deltas + trade prints), refdata (instrument definitions), and a snapshot port for recovery — and the bridge reconstructs the book internally, re-serving it as the **incremental `book`** product rather than full-state `depth`. It keeps one reconstructed book per `(publisher, channel, instrument)`: two arms mirror one feed but their per-instrument delta sequences are unrelated by construction, and a single group can be sharded across channels, so nothing below that triple identifies a book. The Lashay perps row (`lashay-2`) selects this kind; it stays **inert until the upstream group rename lands**, since `doublezero status` reports no matching code until then and the reconciler never activates it (no warning, no failed bind — just a receiver that never starts).

> **The Market-by-Price memory caps are per receiver task, not per process.** One task holds at most 4096 books, 256 `(publisher, channel)` reset/snapshot-routing keys, and 2^20 buffered deltas across every book it tracks. N publishers on separate port blocks are N tasks and so hold N times each bound; publishers sharing one port block share a single task's. On crossing the delta budget the processor drops the **largest** instrument's buffer and marks that instrument `Gap` — it recovers on its next snapshot and every other instrument keeps streaming. Sustained `dz_mbp_buffer_overflows_total` means the publisher's snapshot period is too long for this host, not that anything is broken; the other two caps are anti-forgery bounds on unauthenticated wire fields and should never bind in normal operation.

```bash
# From source — run the edge multicast feed with the public WS backstop for BTC and ETH:
./target/release/doublezero-edge-connect --feed Hyperliquid --ws-input-coins BTC,ETH

# Via the installer one-liner, as env vars before the pipe:
WS_INPUT_COINS=BTC,ETH curl -fsSL https://get.doublezero.xyz/connect | bash
```

Every public feeder is failure-isolated (its own task with reconnect + exponential backoff;
decode/socket errors are logged and never touch the multicast hot path) and relies on the edge
reference data for precision — it emits a public quote/trade only once that `(venue, symbol)`
instrument is known. The outbound `wss://` client is the one place TLS is used (rustls + bundled
webpki roots).

The **Phoenix** feeder is **trades only** — it does not backstop quotes, because the edge Phoenix
Quote is a spline-blended BBO while Phoenix's public orderbook channel is resting-only (a different
quantity). Phoenix names each market with the same bare ticker on the edge and public feeds (the edge
`instrument_id` equals the public `assetId`), so the configured symbol is used verbatim — to subscribe
the public feed and to tag the emitted trade — and trade dedup keys on the public `tradeSequenceNumber`.
Validated against a concurrent edge+public capture (2026-06-30): the public `tradeSequenceNumber`
equals the edge `trade_id` on every shared fill (257/257) and `side` maps `bid -> buy` / `ask -> sell`.

> **Caveat — trade dedup window vs. reconnect lag.** Cross-source trade dedup is a fixed-size
> windowed `trade_id` cache. A long public reconnect can deliver trades whose ids have aged out of
> the window during a high-volume burst, which would re-emit a duplicate trade. Sizing the window
> against the public feed's unbounded-lag failure mode is tracked separately (window-sizing issue);
> until then the window is a compile-time constant.

## Where a venue's `trade` tape comes from

A venue's feed rows can ride **separate multicast groups with separate subscription codes**, as
Lashay's do (`lashay-1` top of book, `lashay-2` market-by-price). A host may hold one and not the
other, and its WebSocket output must carry a tape either way — so both rows claim trades and the
reconciler decides which one serves them: top of book when both are up, market-by-price when it is
alone. The pick moves **without respawning** the receiver that keeps it, so the surviving publisher's
books and reference data survive the flip. `dz_tape_owner_changes_total` counts the moves.

Exactly one emitter per venue at any moment is what licenses forwarding a `trade_id == 0` print
unkeyed: a FIX-sourced print carries no venue trade id, so two simultaneous emitters would duplicate
every print with nothing to collapse them. Within the owning row, a `Sticky` venue's two arms are
gated the same way one level down — one arm serves, the peer's prints are dropped
(`dz_tape_arm_transfers_total`).
