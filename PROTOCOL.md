# Normalized Top-of-Book Feed Protocol - v1

An **engine-agnostic** WebSocket protocol for streaming normalized two-sided top-of-book
(best bid / best ask) market data. It carries the venue and symbol in every message, uses
plain JSON, and is independent of any trading framework.

`doublezero-edge-connect` is the **reference producer** (it ingests the DoubleZero Edge binary multicast feed
and re-serves it in this format) - but it is not part of the protocol. Any engine that can open a WebSocket and parse JSON can
consume it by writing a thin (~50-100 line) adapter to its own internal types. The producer's
*input* (multicast, binary, etc.) is an implementation detail; only the *output* below is the
contract.

## Transport

- **WebSocket**, one JSON object per **text** frame (no framing/batching). Plain `ws://`
  (no TLS - this service is intended for a trusted/local network; terminate TLS at a reverse
  proxy if you must expose it).
- The server **pushes** market data. A consumer may optionally **subscribe** to narrow the
  stream to specific venues/symbols; with no subscription it receives everything
  (see [Subscriptions](#subscriptions--filtering)).
- **Liveness**: the server sends periodic WebSocket Pings and closes a client that is silent
  past an idle timeout; clients may also send an app-level ping
  (see [Heartbeat & liveness](#heartbeat--liveness)).

## Connection lifecycle

On each new connection the producer:

1. **Replays the current instrument definitions** - one `instrument` message per symbol whose
   Source ID is known, so the consumer knows precision **before** the first quote/depth. For a
   publisher whose reference data carries no Source ID of its own, that means **known and priced
   at least once** - a symbol it defines but has not yet priced is not replayed. For a publisher
   whose reference data carries its own Source ID, every symbol it defines is replayed, priced or
   not (see [*A symbol appears only once its source is
   known*](#a-symbol-appears-only-once-its-source-is-known)).
2. **Replays the latest full-state book** per market, if any - the latest `depth` per symbol, and a
   `book` re-baseline (a `clear` plus the complete book) for every market that has one. See
   `book_scope` under [*Subscriptions & filtering*](#subscriptions--filtering) for the granularity of
   that re-baseline on an order-level market.
3. **Streams** `quote`/`trade`/`midpoint`/`depth`/`book` messages as they arrive, fanned out to all
   connected consumers.

```
connect -> instrument (xN) -> depth (xM, current books) -> quote -> trade -> depth -> ...
```

A consumer that connects mid-stream is therefore always able to build instruments first. New or
changed instrument definitions may also arrive at any later point in the stream.

## Message envelope

Every message is a JSON object tagged by a `type` field (`snake_case`):

| `type`       | Meaning                                  |
|--------------|------------------------------------------|
| `instrument` | An instrument/precision definition.      |
| `quote`      | A top-of-book update.                    |
| `trade`      | A trade print (last sale).               |
| `midpoint`   | A single derived mid price.              |
| `depth`      | A full order-book depth snapshot.        |
| `book`       | A batch of incremental order-book changes, price-aggregated or order-level. |
| `status`     | A venue-level feed-health transition.    |

Consumers **must ignore unknown `type` values and unknown fields** (forward compatibility).

### `instrument`

```json
{"type":"instrument","venue":"Hyperliquid","source":"Hyperliquid","source_id":1,"symbol":"SOL","channel":0,"instrument_id":1,"price_exponent":-2,"qty_exponent":-2}
```

| Field            | Type   | Meaning                                                              |
|------------------|--------|----------------------------------------------------------------------|
| `type`           | string | `"instrument"`.                                                      |
| `venue`          | string | **Deprecated.** Always identical to `source`. See [`source`, `source_id`, and the deprecated `venue`](#source-source_id-and-the-deprecated-venue). |
| `source`         | string | The source's registry name (e.g. `Hyperliquid`, `Phoenix`). **Preferred.** |
| `source_id`      | number | The wire Source ID, verbatim.                                        |
| `symbol`         | string | Instrument symbol as the venue names it (e.g. `SOL`, `SOL-PERP`).   |
| `channel`        | uint32 | The publisher's channel id: the instrument set this feed carries. Filterable. |
| `instrument_id`  | uint32 | Instrument id, unique within `channel`.                              |
| `price_exponent` | int8   | Price increment exponent: tick size = `10^price_exponent` (e.g. `-2` -> `0.01`). |
| `qty_exponent`   | int8   | Size increment exponent: step = `10^qty_exponent`.                  |

`channel` and `instrument_id` are the identity a consumer joins a [`book`](#book) to its definition on, rather than the colliding `symbol`.

`price_exponent` / `qty_exponent` give the venue's **precision**; `quote` prices/sizes are
already decimal values (below), so the exponents are used to set tick size / decimal places, not
to rescale integers.

### `quote`

```json
{"type":"quote","venue":"Hyperliquid","source":"Hyperliquid","source_id":1,"symbol":"SOL",
 "bid":184.20,"ask":184.21,"bid_size":12.5,"ask_size":8.0,"bid_n":3,"ask_n":2,
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

| Field             | Type    | Meaning                                                                 |
|-------------------|---------|-------------------------------------------------------------------------|
| `type`            | string  | `"quote"`.                                                              |
| `venue`           | string  | **Deprecated.** Always identical to `source`.                           |
| `source`          | string  | The source's registry name. **Preferred.**                              |
| `source_id`       | number  | The wire Source ID, verbatim.                                           |
| `symbol`          | string  | Symbol (matches an `instrument`'s `symbol`).                            |
| `bid`             | number  | Best bid price (decimal).                                               |
| `ask`             | number  | Best ask price (decimal).                                               |
| `bid_size`        | number  | Size at best bid (decimal).                                             |
| `ask_size`        | number  | Size at best ask (decimal).                                             |
| `bid_n`           | uint16  | Orders/sources at best bid (`0` if the venue does not report it). Part of the top-of-book identity: a change here is a distinct quote even at an unchanged price/size. |
| `ask_n`           | uint16  | Orders/sources at best ask (`0` if unavailable).                        |
| `source_ts_ns`    | uint64  | Venue/source timestamp, ns since Unix epoch. `0` if unknown.            |
| `recv_ts_ns`      | uint64  | Producer user-space receive time (after decode), ns since epoch.        |
| `kernel_rx_ts_ns` | uint64  | Kernel RX timestamp (`SO_TIMESTAMPNS`, `CLOCK_REALTIME`) captured in the driver softirq, before user space. `0` if unavailable. |
| `ws_send_ts_ns`   | uint64  | Wall clock sampled the instant this quote is serialized for the consumers. A single value shared by all consumers of this message (the producer serializes once and fans the identical frame out), not a per-connection send time. `0` if not stamped. |

All timestamps are **nanoseconds since the Unix epoch** (wall clock), and **`0` is the sentinel
for "not available."** Consumers must treat `0` as missing, not as 1970.

#### Why four timestamps

They decompose latency end-to-end and are usable by any engine, not just for backtests:

```
source_ts_ns --> kernel_rx_ts_ns --> recv_ts_ns --> ws_send_ts_ns --> (consumer recv)
  venue book        wire-adjacent       user-space        WS hand-off
                    arrival (defendable)  (post-decode)
```

- `kernel_rx_ts_ns - source_ts_ns` ~ network + venue->host transit (use kernel ts to avoid
  user-space scheduling jitter).
- `recv_ts_ns - kernel_rx_ts_ns` ~ decode + queueing inside the producer.
- `ws_send_ts_ns - recv_ts_ns` ~ fan-out hand-off.
- `consumer_recv - ws_send_ts_ns` ~ the WebSocket hop to your engine.

### `trade`

```json
{"type":"trade","venue":"Hyperliquid","source":"Hyperliquid","source_id":1,"symbol":"SOL",
 "price":184.20,"size":3.5,"aggressor_side":"buy","trade_id":987654,"cumulative_volume":12500.0,
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

A **trade print** (last sale) for a symbol. Prices/sizes are already decimal values (scaled by the
venue precision, same convention as `quote`).

| Field               | Type    | Meaning                                                              |
|---------------------|---------|----------------------------------------------------------------------|
| `type`              | string  | `"trade"`.                                                           |
| `venue`             | string  | **Deprecated.** Always identical to `source`.                        |
| `source`            | string  | The source's registry name. **Preferred.**                           |
| `source_id`         | number  | The wire Source ID, verbatim.                                        |
| `symbol`            | string  | Symbol (matches an `instrument`'s `symbol`).                         |
| `price`             | number  | Trade price (decimal).                                               |
| `size`              | number  | Trade size (decimal).                                                |
| `aggressor_side`    | string  | `"buy"`, `"sell"`, or `"unknown"` - the aggressor (taker) side.      |
| `trade_id`          | uint64  | Venue-assigned trade identifier.                                     |
| `cumulative_volume` | number  | Session cumulative traded volume (decimal); `0` if not provided.     |
| `source_ts_ns`      | uint64  | Venue/source timestamp, ns since epoch. `0` if unknown.              |
| `recv_ts_ns`        | uint64  | Producer user-space receive time (after decode), ns since epoch.     |
| `kernel_rx_ts_ns`   | uint64  | Kernel RX timestamp (`SO_TIMESTAMPNS`); `0` if unavailable.          |
| `ws_send_ts_ns`     | uint64  | Wall clock the instant this trade is serialized; shared by all consumers of this message (serialized once, not per-connection). `0` if unset.|

The same four timestamps as `quote` ride every trade (see *Why four timestamps*). Unlike a quote,
a trade is a **point-in-time event, not full state**: it is not replayed on connect, and a trade
dropped under backpressure is simply a missed print (it does not leave a stale book). A consumer
that only wants top-of-book may ignore `trade` per the forward-compatibility rule.

### `midpoint`

```json
{"type":"midpoint","venue":"MidpointVenue","source":"MidpointVenue","source_id":0,"symbol":"SOL","mid":184.205,
 "method":0,"quality_flags":0,
 "book_ts_ns":1781019263715344015,"compute_ts_ns":1781019263715350000,
 "recv_ts_ns":1781019263715501230,"kernel_rx_ts_ns":1781019263715300010,
 "ws_send_ts_ns":1781019263715600440}
```

A single **derived mid price** for a symbol, from the DZ Edge Midpoint sibling feed. Like a
`quote` it is **full state** per instrument (the latest mid), so it self-heals on the next message;
a consumer that connects mid-stream sees the matching `instrument` (for precision) first.

| Field            | Type   | Meaning                                                                |
|------------------|--------|------------------------------------------------------------------------|
| `type`           | string | `"midpoint"`.                                                          |
| `venue`          | string | **Deprecated.** Always identical to `source` (a Midpoint feed maps to its own venue). |
| `source`         | string | The source's registry name. **Preferred.**                             |
| `source_id`      | number | The wire Source ID, verbatim (`0` when the feed names no registry row). |
| `symbol`         | string | Symbol (matches an `instrument`'s `symbol`).                           |
| `mid`            | number | Mid price (decimal).                                                   |
| `method`         | uint8  | How the mid was computed (`0` = the instrument's default method).      |
| `quality_flags`  | uint8  | Bitfield: bit0 stale, bit1 one-sided, bit2 crossed/locked, bit3 synthetic. |
| `book_ts_ns`     | uint64 | Venue timestamp of the underlying book state; `0` if unknown.          |
| `compute_ts_ns`  | uint64 | When the publisher computed the mid; `0` if unknown.                   |
| `recv_ts_ns`     | uint64 | Producer user-space receive time (after decode), ns since epoch.       |
| `kernel_rx_ts_ns`| uint64 | Kernel RX timestamp (`SO_TIMESTAMPNS`); `0` if unavailable.            |
| `ws_send_ts_ns`  | uint64 | Wall clock the instant this midpoint is serialized; shared by all consumers of this message (serialized once, not per-connection). `0` if unset.|

The Midpoint feed carries **no sizes**, so its `instrument` reports `qty_exponent: 0` (ignore it
for mids). A consumer that only wants quotes/trades may ignore `midpoint` per forward-compat.

### `depth`

```json
{"type":"depth","venue":"MboVenue","source":"MboVenue","source_id":0,"symbol":"SOL",
 "bids":[[184.20,12.5],[184.19,4.0]],"asks":[[184.21,8.0],[184.22,6.5]],
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

A **full order-book depth snapshot** (top *N* levels per side), derived in the producer from the DZ
Edge Market-by-Order feed. `bids`/`asks` are arrays of `[price, size]` decimal pairs, **best
first** (bids high→low, asks low→high).

| Field            | Type     | Meaning                                                              |
|------------------|----------|----------------------------------------------------------------------|
| `type`           | string   | `"depth"`.                                                           |
| `venue`          | string   | **Deprecated.** Always identical to `source` (a Market-by-Order feed maps to its own venue). |
| `source`         | string   | The source's registry name. **Preferred.**                          |
| `source_id`      | number   | The wire Source ID, verbatim (`0` when the feed names no registry row). |
| `symbol`         | string   | Symbol (matches an `instrument`'s `symbol`).                         |
| `bids`           | number[][] | `[price, size]` pairs, highest price first.                        |
| `asks`           | number[][] | `[price, size]` pairs, lowest price first.                         |
| `source_ts_ns`   | uint64   | Timestamp of the latest applied book event; `0` if unknown.          |
| `recv_ts_ns`     | uint64   | When the producer built this snapshot, ns since epoch.               |
| `kernel_rx_ts_ns`| uint64   | Kernel RX timestamp (`SO_TIMESTAMPNS`); `0` if unavailable.          |
| `ws_send_ts_ns`  | uint64   | Wall clock the instant this snapshot is serialized; shared by all consumers of this message (serialized once, not per-connection). `0` if unset.|

**Each `depth` message is full state** (the complete top *N*, not a delta), so - like `quote` - it
**self-heals**: a consumer that drops one under backpressure recovers on the next snapshot, and a
client that connects mid-stream is replayed the latest `depth` per symbol on connect (after the
`instrument` definitions).

> **Market-by-Order produces `depth` *and* `book`.** The producer runs that feed's snapshot+delta
> recovery internally and derives both products from the same reconstructed L3 book: this full-state
> top-*N* `depth`, and the order-level [`book`](#book) carrying the venue's own `order_id`. Neither
> re-serves the upstream's raw add/cancel/execute events — a `book` change is the *resulting state* of
> one order, so a consumer needs no delta arithmetic, and a recovery still surfaces only as a
> re-baseline.

### `book`

```json
{"type":"book","venue":"BookVenue","source":"BookVenue","source_id":0,"symbol":"SOL","channel":2,"instrument_id":41,
 "changes":[{"action":"update","side":"bid","price":0.6200,"size":150},
            {"action":"delete","side":"ask","price":0.6300,"size":0}],
 "snapshot":false,"last":true,
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

A batch of **incremental** book changes for one instrument, derived in the producer from a DZ Edge order-book feed. Unlike `depth`, a `book` message is not full state: apply the changes in order to the book you already hold.

A change is either **price-aggregated** (`order_id: 0`, from the Market-by-Price feed) or **order-level** (a non-zero `order_id`, the venue's own, from the Market-by-Order feed). The two never mix within a market, and `order_id` is what says which you are reading.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"book"`. |
| `venue` | string | **Deprecated.** Always identical to `source`. |
| `source` | string | The source's registry name. **Preferred.** |
| `source_id` | number | The wire Source ID, verbatim. |
| `symbol` | string | **Display label.** Not guaranteed unique — see *Identity* below. |
| `channel` | uint32 | The publisher's channel id: the instrument set this feed carries. Filterable. |
| `instrument_id` | uint32 | Instrument id, unique within `channel`. |
| `changes` | object[] | Book changes, in order. `{ "action", "side", "price", "size", "order_id" }`. |
| `changes[].action` | string | `"clear"`, `"update"`, or `"delete"`. |
| `changes[].side` | string | `"bid"`, `"ask"`, or `"both"` (`"both"` only on a `clear`). |
| `changes[].price` | number | Price of the level or order (decimal). Ignored for a `clear`. |
| `changes[].size` | number | The **absolute** resulting size (decimal), not a delta — of the level for a price-aggregated change, of the order for an order-level one. `0` on a `delete`. |
| `changes[].order_id` | uint64 | The venue's order id for an order-level change, or `0` when the change is price-aggregated and carries no order identity. |
| `snapshot` | bool | Advisory: this batch is part of a rebuild. **Not** what re-baselines you. |
| `last` | bool | This is the final batch of a logical book event. |
| `source_ts_ns` | uint64 | Timestamp of the latest applied book event; `0` if unknown. |
| `recv_ts_ns` | uint64 | When the producer built this batch, ns since epoch. |
| `kernel_rx_ts_ns` | uint64 | Kernel RX timestamp (`SO_TIMESTAMPNS`); `0` if unavailable. |
| `ws_send_ts_ns` | uint64 | Wall clock the instant this batch is serialized; shared by all consumers of this message. `0` if unset. |

**Identity: key on `(venue, channel, instrument_id)`, not on `symbol`.** The upstream `symbol` is a fixed 16-byte field the publisher fills by keeping the ticker's rightmost 16 bytes — silently, with no hash and no length check — so on venues with long tickers distinct markets collide on it, and a consumer keying on `symbol` merges two books into one. `symbol` is for display, and for the convenience of venues where it happens to be unique. `instrument` messages carry `channel` and `instrument_id` too, so a consumer joins a book to its definition on the same identity, and learns the mapping from the connect-time replay of the definitions.

**Re-baselining is structural: `changes[0].action == "clear"`.** Do **not** key it off `snapshot`. A rebuild (on connect, after a recovery, or when the producer's authoritative source changes) arrives as a `clear` followed by the complete level set, with `snapshot: true` and `last: true` on the final batch. `snapshot` exists only so a consumer can tell a rebuild from ordinary activity; a consumer that ignores it stays correct.

**`last` is mandatory and must be honored.** A consumer that buffers a logical event until its final batch will wait forever if it is dropped — including on a re-baseline whose only change is the `clear`.

**Gap detection is the producer's job.** The producer runs the upstream feed's snapshot+delta recovery internally, per publisher, and re-serves only sequences it has verified as contiguous. There are no sequence numbers on the wire and a consumer needs no gap machinery of its own: a recovery surfaces as a re-baseline.

**One book per market, whichever upstream publisher wins.** Several independent publishers mirror each feed, and a consumer sees one coherent book from them, never two to merge. How that happens depends on whether the changes carry an `order_id`, but the consumer contract is the same either way and it is never told which publisher it is reading.

- **Price-aggregated.** The producer elects one authoritative publisher per market and republishes only its stream. A failover surfaces as a re-baseline: that market's next batch is a `clear` followed by the complete level set as the newly authoritative publisher holds it, or — when that publisher's own book is not yet complete — the `clear` alone, with the level set rebuilt by the batches that follow.
- **Order-level.** Every publisher stamps the venue's own `order_id`, so the producer instead publishes each venue event's first arrival and collapses the rest: a consumer gets each event once, from whichever publisher was fastest for *that* event. A publisher recovering by snapshot republishes its whole book only when no healthy peer is serving the market, so a recovery cannot wipe a book that another publisher is keeping current.

Either way a consumer that honors `clear` needs nothing else.

**An order-level consumer must ask for an order-level bootstrap.** See `book_scope` under *Subscriptions & filtering*: the default replay is price-aggregated, and a consumer that keys its book by `order_id` will otherwise be bootstrapped with levels carrying no ids and then receive changes for ids it never saw.

### `status`

```json
{"type":"status","venue":"Hyperliquid","source":"Hyperliquid","source_id":1,"state":"down","stale_ms":30000,"ts_ns":1781019263715344015}
```

A **venue-level feed-health** transition. The producer emits one when a venue's **quote**
(market-data) multicast goes silent past the idle watchdog (`state:"down"`), and again when quotes
recover (`state:"ok"`). It is emitted only on the **edge** (not repeatedly while silent), so a
consumer can gray out / restore that source. Unlike `quote`/`instrument` it carries **no `symbol`**
- it is about the whole venue feed - so the server matches it against a subscription **by venue
alone** (a `{"venue":"Hyperliquid","symbol":"SOL"}` subscriber still receives Hyperliquid status).

A venue is reported `down` only when **every** publisher mirroring its quote feed has gone silent
past the idle window; a single wedged publisher does not produce a `status` transition, because the
remaining publishers still deliver full-state updates for that venue. `status` stays what it has
always been — the health of the venue's *quote* stream — so a depth-only (Market-by-Order) publisher
going silent is not reported here, and a healthy one does not suppress a quote outage.

**A source whose receivers have revealed no Source ID may never produce a `status` message at
all.** The naming this message carries — like every other message's — depends on a Source ID
observed on the wire (see [*A symbol appears only once its source is
known*](#a-symbol-appears-only-once-its-source-is-known)); a source that never reveals its
identity this way has no name to emit `status` under, healthy or not. A consumer should not infer
"up" from the absence of a `status` message for a source it expects to hear from.

For a publisher whose reference data carries no Source ID of its own, revealing requires a decoded
price, so in practice this is the "no market data decoded" case its name suggests. For a publisher
whose reference data carries its own Source ID, reference data alone reveals it — `status` can
still be emitted for a source whose receivers have decoded reference data but never a single
quote or trade.

| Field       | Type   | Meaning                                                         |
|-------------|--------|-------------------------------------------------------------------|
| `type`      | string | `"status"`.                                                     |
| `venue`     | string | **Deprecated.** Always identical to `source`.                   |
| `source`    | string | The source's registry name whose quote feed changed health. **Preferred.** |
| `source_id` | number | The wire Source ID, verbatim.                                   |
| `state`     | string | `"down"` (quote multicast silent) or `"ok"` (quotes recovered). |
| `stale_ms`  | uint64 | Milliseconds the quote feed had been silent (`0` when `"ok"`).  |
| `ts_ns`     | uint64 | Wall clock (ns since epoch) the status was emitted.             |

Quote delivery is **not gated** on status - it is advisory health, and because every `quote` is
full state the feed self-heals on the next quote regardless. A consumer that ignores `status`
(per the forward-compatibility rule) simply forgoes the gray-out.

### `source`, `source_id`, and the deprecated `venue`

Every message carries three fields naming where the data came from:

| Field | Type | Meaning |
|---|---|---|
| `source` | string | The source's registry name. **Preferred.** |
| `source_id` | number | The wire Source ID, verbatim. |
| `venue` | string | **Deprecated.** Always identical to `source`. |

**This release changes what `venue` contains.** `source_id` is now the Source ID the publisher
stamped on the wire, passed through unmodified, and `source`/`venue` are both that ID's registry
name. Previously the bridge substituted its own configured label when it did not recognise an ID.
It no longer does: a publisher stamping an incorrect Source ID now produces messages named for the
source that ID identifies, because the Source ID is the contract and a wrong one is a publisher
defect to fix at the publisher.

If you filter or key on `venue`, re-check your values against a live feed rather than assuming they
are unchanged. `venue` and `source` always hold the same string; new consumers should read `source`,
or `source_id` for a stable numeric identity that needs no string matching.

An unregistered Source ID yields a stable synthesized name (`SOURCE_<id>`) rather than being dropped,
so data always flows and an unrecognised source is visible rather than silent.

### A symbol appears only once its source is known

A message is emitted for an instrument only after that instrument's Source ID has been observed —
but *when* that happens depends on the publisher's reference-data generation.

The original edge feed spec carried a Source ID only on price messages, never on reference data or
book snapshots. A publisher of that generation still works this way: an instrument that has
received reference data but no price yet produces **nothing** — no `instrument`, no `quote`, no
`book`, no `depth` — until a price names it. **Midpoint stays on this generation permanently**: its
reference-data message is a distinct, narrower definition with no Source ID field, so a `midpoint`
instrument is always deferred until priced, independent of any other feed's generation.

A newer publisher generation adds a Source ID to reference data itself. For a publisher of that
generation, an instrument is named the moment its definition is decoded — `instrument` reaches the
wire with no price required at all, even for a symbol that never trades.

Consequences for a consumer:

- The connect-time replay contains one `instrument` per symbol whose Source ID is known — every
  symbol a newer-generation publisher defines, but only the priced ones for a publisher (or
  Midpoint) still on the original generation.
- For a publisher still on the original generation, a `status` message may never appear for a
  source whose receivers have decoded no market data at all (see [`status`](#status)); a
  newer-generation publisher's reference data alone is enough to reveal it.
- Both generations can be live at once — a host may hold publishers of either kind for different
  venues, or for different rows of the same venue — so a consumer should not assume the deferral
  rule observed for one source holds for every source on the stream.

The deferral itself is deliberate, for whichever generation still needs it. The alternative is
announcing an instrument under a name the bridge guessed, which is what the previous behaviour
did.

## Subscriptions & filtering

A consumer may send control messages (JSON text frames) to filter the stream. **Subscriptions
are optional**: a client that never subscribes receives **all** venues/symbols (firehose). Once
it has >=1 active subscription, it receives only matching messages.

A subscription filter is `{ "source"?: string, "venue"?: string, "symbol"?: string, "channel"?: uint32, "type"?: string, "book_scope"?: "levels" | "orders" }` - an **omitted field matches any value** (so `{}` = everything, `{"symbol":"SOL"}` = SOL on every venue, `{"type":"book"}` = book updates only). `venue`/`source` are matched **case-insensitively** (`PHOENIX` selects `Phoenix`); `symbol`, `channel` and `type` are matched exactly.

`source` and `venue` are aliases and are matched case-insensitively. Supplying both ANDs them, so a
disagreeing pair matches nothing; supply one.

`venue`, `symbol` and `channel` are **scope** dimensions - which markets - and `type` is a **kind** dimension: which messages. The two behave differently on purpose.

A scope dimension never excludes a message that is not about one market. A message type that carries no channel (everything except `book` and `instrument`) is excluded by an explicit `channel` filter, so `{"channel":2}` selects channel 2's book updates and channel 2's instrument definitions - enough to scale those books - and nothing else. The one carve-out is a venue-level message (`status`), which carries neither symbol nor channel and is matched on `venue` and `type` alone, so a `{"venue":"Hyperliquid","symbol":"SOL"}` subscriber still receives Hyperliquid status.

A `type` filter is **absolute**: it delivers that message type and nothing else, including no `instrument` and no `status`. Filters are a union, so a consumer that wants books plus reference data and health subscribes to each - `{"type":"book"}`, `{"type":"instrument"}`, `{"type":"status"}` - or omits `type` and scopes by `venue`/`symbol`/`channel` instead. A client that sets `type` and never asks for `instrument` gets the connect-time replay of the definitions that exist then, and no later ones; that is the filter it asked for.

`symbol` cannot select a single `book` market where a venue's tickers collide under truncation (see *Identity* under [`book`](#book)); such a subscription delivers every colliding market's book. Filtering `book` down to one market is not expressible in v1.

**Client -> server:**

```json
{"method":"subscribe","subscription":{"venue":"Hyperliquid","symbol":"SOL"}}
{"method":"unsubscribe","subscription":{"venue":"Hyperliquid","symbol":"SOL"}}
{"method":"ping"}
```

**Server -> client** (control/ack frames are tagged by `channel`, distinct from data's `type`):

```json
{"channel":"subscription_response","method":"subscribe","subscription":{"venue":"Hyperliquid","symbol":"SOL"}}
{"channel":"pong"}
{"channel":"error","error":"max subscriptions reached"}
```

Unknown/malformed control messages get `{"channel":"error","error":"unrecognized message"}` and
are otherwise ignored.

Instrument definitions and current book state are replayed on connect (unfiltered, since a client has no subscriptions yet) and again on each `subscribe`, scoped to the filter just added — so a client that narrows after connecting is bootstrapped for its new scope instead of waiting for the next event. Replay is idempotent full state, so the overlap is harmless.

**`book_scope` selects the granularity of that `book` replay, not which messages arrive.** It defaults to `"levels"`: an order-level market is bootstrapped as price levels carrying `order_id: 0`, so an L2 consumer is never handed a venue's whole order population. `"orders"` bootstraps every resting order with its `order_id` instead, and is **required** for a consumer that keys its book by order id — with the default, such a consumer receives levels with no ids and then order-level changes referencing ids it never saw, and its book diverges silently. It is not a filter dimension, so it never excludes a message; it is part of the subscription's identity, so re-subscribing with the other scope is a distinct subscription and does bootstrap again.

## Heartbeat & liveness

- The server sends a **WebSocket Ping** every `WS_HEARTBEAT_SECS` (default 20s); a compliant
  client auto-replies Pong (no app action needed).
- A client that sends **no frame** (data Pong, app ping, or control message) for
  `WS_IDLE_TIMEOUT_SECS` (default 60s) is closed - this reaps dead/stalled consumers.
- App-level keepalive is also supported: `{"method":"ping"}` -> `{"channel":"pong"}`.

## Limits & backpressure

| Limit | Default | Behavior when exceeded |
|-------|---------|------------------------|
| Concurrent clients (`WS_MAX_CLIENTS`) | 64 | New connection is rejected (closed). |
| Subscriptions per client (`WS_MAX_SUBS`) | 256 | `subscribe` is refused with an `error`. |
| Inbound control msgs / client / min (`WS_MAX_INBOUND_PER_MIN`) | 600 | Client is disconnected. |
| Broadcast buffer (`WS_BROADCAST_CAPACITY`) | 4096 | A slow client **drops the oldest** messages (logged); it is never allowed to stall the feed. |

Because every `quote` is a full top-of-book snapshot, a consumer that drops messages under
backpressure **self-heals** on the next quote - no resync handshake is required.

## Consuming the feed (any engine)

```text
on connect:
  for each frame (JSON):
    msg = parse(frame)
    switch msg.type:
      "instrument":
        tick_size = 10 ** msg.price_exponent
        size_step = 10 ** msg.qty_exponent
        register/update instrument(msg.venue, msg.symbol, tick_size, size_step)
      "quote":
        inst = instrument(msg.venue, msg.symbol)   # may not exist yet -> buffer or skip
        emit_top_of_book(inst, msg.bid, msg.ask, msg.bid_size, msg.ask_size,
                         event_time = msg.source_ts_ns or msg.kernel_rx_ts_ns)
      "trade":
        inst = instrument(msg.venue, msg.symbol)
        emit_trade(inst, msg.price, msg.size, msg.aggressor_side,
                   event_time = msg.source_ts_ns or msg.kernel_rx_ts_ns)
      "depth":                                       # full snapshot each message (self-healing)
        inst = instrument(msg.venue, msg.symbol)
        replace_book(inst, msg.bids, msg.asks)       # overwrite, don't merge
      "book":                                        # incremental; apply in order
        book = book_for(msg.venue, msg.channel, msg.instrument_id)
        for c in msg.changes:                        # "clear" re-baselines, not msg.snapshot
          # c.order_id == 0 keys the change by price; non-zero keys it by the venue's order id
          # (and needs a `book_scope: "orders"` subscription so the bootstrap carries ids too).
          apply(book, c.action, c.side, c.price, c.size, c.order_id)
        if msg.last: publish(book)                   # honor `last` or you wedge
      _: ignore        # unknown type
    # ignore unknown fields throughout
  reply Pong to Ping; reconnect on close.
```

### Writing a consumer

- **Any engine** (Freqtrade, Hummingbot, a custom bot in Rust/Go/Python): implement the loop
  above against your engine's instrument/quote types. ~100 lines; all the framework coupling
  lives in the adapter, none on the wire.

## Conventions

- **Venue codes** and **symbols** are opaque strings agreed between producer and consumer; the
  protocol does not mandate a registry. Match them exactly (`symbol` on a `quote` equals the
  `symbol` on its `instrument`).
- A single feed endpoint may carry **multiple venues and symbols**; route by `venue`+`symbol`.
- One `doublezero-edge-connect` process ingests several upstream feeds at once and tags each
  message with that feed's venue, so one WebSocket endpoint is inherently multi-venue.

## Versioning & compatibility

- This document defines **v1**, which includes: the `instrument`/`quote`/`trade`/`midpoint`/`depth`/
  `book` data messages, the venue-level `status` feed-health message, optional **subscribe/unsubscribe**
  filtering, **app ping/pong + server heartbeat with idle timeout**, and **connection/subscription/
  rate limits with broadcast backpressure**.
- **`depth` is deprecated.** It is the full-state top-*N* product derived from the Market-by-Order feed; `book` supersedes it with the complete book, incrementally and — on that feed — at order level. Both are served today, from every feed that has one; `depth` is removed in v2. New consumers should implement `book`.
- **Additive in this revision, so still v1:** `order_id` on a `book` change, and `book_scope` on a subscription. Nothing is withdrawn, and a consumer that ignores both reads the same price-aggregated books it read before.
- **Breaking within v1: what `venue` contains changed**, and emission is now gated on a Source ID
  having been observed on the wire. Both are additive to the *shape* of the protocol (new fields,
  no removed ones) but change *values* an existing consumer may depend on — see [`source`,
  `source_id`, and the deprecated `venue`](#source-source_id-and-the-deprecated-venue) and [*A
  symbol appears only once its source is
  known*](#a-symbol-appears-only-once-its-source-is-known). The forward-compatibility rule below
  covers unknown fields/types, not this.
- There is no `v` field on the wire; the contract is this spec plus the
  **forward-compatibility rule**: consumers ignore unknown message types and unknown fields,
  so additive changes are non-breaking. A future revision may add an explicit `v` field.

### Not in v1 (candidate extensions)

- **TLS / `wss://`** - intentionally omitted; this service runs on a trusted/local network
  (use a reverse proxy if exposure is ever needed).
- **Sequence numbers + gap detection** per `(venue, symbol)`. Not needed for the top-of-book
  contract (every `quote` is full state and self-heals); would matter only for delta feeds.
- Additional message types: **funding rate**, **open interest** (the venue-level feed **`status`**
  message, **`trade`** prints, **`midpoint`** and order-book **`depth`** are now part of v1 - see
  above).
- **AuthN/AuthZ** and a **`/health` + metrics** endpoint (service/ops concerns, not the wire
  protocol).
