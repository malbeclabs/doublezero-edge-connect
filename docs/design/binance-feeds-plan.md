# Binance feeds — implementation plan

Design: [shared-source-names.md](shared-source-names.md) (the prerequisite, #168 and #169).
Publisher side: [`malbeclabs/binance`](https://github.com/malbeclabs/binance).

Ingest both Binance matching engines the `malbeclabs/binance` repository publishes, and serve them
over every surface this crate has. Both are Top-of-Book & Trades (magic `0x445A`), whose codec is
already byte-validated, so the bridge needs **no new decode path**: the work is registry data, its
identity consequences, verification against the wire, and docs.

## What the publisher emits

Read from the publisher's own deployment inventory
(`binance/infra/ansible/inventory/host_vars/aws-tyo-bn-usdsm-mainnet1/main.yml`), not from prose.

| | USD-margined perps | Spot |
|---|---|---|
| Source ID | `6` | `8` |
| Group `code` | `edge-binance-usdsm-tob` | `edge-binance-spot-tob` |
| Multicast group | `233.84.178.23` | `233.84.178.31` |
| Kind | `TopOfBook` | `TopOfBook` |
| Ports (`mktdata` / `refdata`) | `30001` / `30002` | `30001` / `30002` |
| `channel_id` | `0` | `1` |
| Publishers | one, `aws-tyo-bn-usdsm-mainnet1` | one, the same host |
| Live since | 2026-08-30 | 2026-09-25 |
| Messages | `Quote` only; `Trade` tracked in binance#69 | `Quote` only |
| Instruments | ~566 | ~1,373 (ingress partitioned; one feed on the wire) |

Both engines share the registry name `BINANCE`. That is the upstream Source ID registry's
allocation, not a choice made here, and it is exactly the case #169 exists for.

## Constraints

- Branch from `origin/main`: #169 is required, since without it `{6, BINANCE}` beside
  `{8, BINANCE}` is refused as `DuplicateSource`.
- **Port provenance.** A port in the registry is a claim about a machine outside this process. The
  values above come from the publishers' deployment inventory; Task 5's capture is the external check,
  and the feed-capture recorder inventory in the private infra repo is the authoritative fleet list. A
  row stays marked unverified in its `notes` until both agree.
- **Ordering of the hosted document.** A binary that predates #169 refuses the repeated name and, under
  a `Url` origin, degrades the **whole** document to its built-in copy. The hosted document already
  carries `HYPERLIQUID` under IDs 1 and 7, but confirm the fleet's image versions before Task 6 rather
  than inferring it from that.
- Lint contract: `cargo +nightly fmt --all -- --check --config imports_granularity=Crate` and
  `cargo +stable clippy --workspace --all-targets -- -Dclippy::all -Dwarnings`.
- Every test must fail when its subject is reverted. `CHANGELOG.md` entry with each code change.
- No AI attribution in commits, PRs or docs.

## Out of scope

- **Depth** (Market-by-Price / Market-by-Order). A publisher-side scope decision; nothing to ingest.
- **Index, mark, funding or settlement prices.** A standing legal constraint on the publisher
  (benchmark rights). The bridge must never synthesize one either.
- **Coin-margined futures and options.** No Source ID claimed and nothing published.

---

## Task 1 — Registry rows

**Files:** `src/ingest/registry.json`

- `sources`: add `{"id": 6, "name": "BINANCE"}` and `{"id": 8, "name": "BINANCE"}`.
- Two `explicit` rows, one publisher each:

  | `venue` | `category` | `code` | `group` | `mktdata` | `refdata` |
  |---|---|---|---|---|---|
  | `BINANCE` | `usdsm` | `edge-binance-usdsm-tob` | `233.84.178.23` | `30001` | `30002` |
  | `BINANCE` | `spot` | `edge-binance-spot-tob` | `233.84.178.31` | `30001` | `30002` |

- `emit_trades: true` on both. It is a capability claim that must agree with
  `reconcile::tape_rank_is_some(TopOfBook)`; `false` fails `EmitTradesDisagrees` and discards the whole
  document. The live USD-margined fragment still says `false` and binance#70 fixes it; that PR must
  land before the fragment is aggregated.
- `arbitration: "Sticky"` on both. One mode per venue is a cross-row invariant, and the published
  fragment already says `Sticky`. With a single publisher per row there is no race to arbitrate today;
  `Sticky` is what the tape leader needs once trades arrive.
- `category` strings match the publisher's fragments (`usdsm`; `spot` for the spot fragment once it
  exists). They separate the two universes for the tape owner, the authority scope and history.
- Operator-facing `notes`, in the voice of the existing rows: what the feed carries ("best bid and
  offer; no trades yet"), the wire-format link, and nothing about how the ports were verified.

Cross-row invariants that must still hold, and do: `(venue, category, kind)` unique;
`(group, port)` unique (ports repeat, groups differ); one arbitration mode for `BINANCE`.

## Task 2 — Compiled-in Source ID fallback

**Files:** `src/ingest/sources.rs`

- Add IDs 6 and 8 to `BUILT_IN`. `the_compiled_in_table_matches_the_built_in_document` fails until
  the two copies agree, which is the point.
- Extend `every_assigned_id_resolves`. `source_ids_of("BINANCE")` returns both IDs.
- This goes beyond the shared-names design's "names arrive with the hosted document", on purpose: a
  `Url` failure degrades to the built-in copy, and the rows added in Task 1 must still resolve there.

## Task 3 — Identity tests for two engines under one name

**Files:** `src/ingest/feeds.rs`, `src/ingest/registry.rs`, `src/ingest/arbiter.rs`, `src/products.rs`,
`src/sinks/api.rs`

The collision is real, not hypothetical: `BTCUSDT` is listed by both engines.

- `feeds`: each Binance row expands to one `FeedPublisher` at the expected group, ports and code
  (catches a later edit moving a value, never a value wrong when written; say so in the test's doc).
- `registry`: a document giving the two Binance rows different `arbitration` is refused; a document
  with only one of the two `sources` entries fails the row whose engine is unassigned.
- `arbiter`: a quote for `(6, BTCUSDT)` and one for `(8, BTCUSDT)` at the same `source_ts` are both
  admitted. Neither latches the other's floor.
- `products`: `BINANCE:BTCUSDT` renders with the `#channel.instrument` suffix for both engines, and
  each suffixed id resolves back to its own Source ID. A spot-only symbol stays bare.
- `api`: `/v1/status` reports each row's `status` and product count under its own Source ID.
- `health`: one engine's receiver going down leaves the other's `status` `online`.

## Task 4 — Consumer-facing separation of the two engines

**Files:** `src/sinks/ws.rs`, `PROTOCOL.md`

A `{"venue":"BINANCE","symbol":"BTCUSDT"}` subscription receives both the perp and the spot quote,
and PROTOCOL.md today tells a consumer to filter on `source_id` **client-side**. With the first venue
whose two engines list the same symbols, that stops being an edge case.

- Add `source_id` as a fifth `SubFilter` dimension, through the same single `SubFilter::matches` both
  paths call, applied to the replay path too. Additive, so no protocol version change.
- Tests: a `source_id` filter excludes the other engine's `quote`, `instrument` and `status`; a
  `venue`-only filter still matches both.
- PROTOCOL.md: document the dimension, and use Binance as the worked example in *Several Source IDs
  can share one name*.

If this is judged out of scope for the feed PR, it becomes its own PR, but ship it before the hosted
document advertises Binance.

## Task 5 — Verify against the wire

On a host subscribed to both codes (`doublezero connect multicast` for each):

- `tcpdump -i doublezero1 -X 'dst 233.84.178.23 or dst 233.84.178.31'`: datagrams on `30001` and
  `30002` for each group, magic `0x445A`, header Source ID `6` / `8`, `channel_id` `0` / `1`.
- Run `--feed BINANCE` with `RUST_LOG=debug`:
  - `dz_receiver_up{venue="BINANCE"} == 1` for both publishers, `dz_feed_up` 1;
  - every symbol's `instrument` precedes its first `quote`;
  - `source_ts_ns` is **nanoseconds**, which is worth checking on spot above all, since that engine's
    upstream stamps microseconds. Compare against `kernel_rx_ts_ns`: the difference should be
    milliseconds (Tokyo to the host), not a factor of 1,000;
  - `/v1/status` shows both rows; `doublezero-edge` renders both.
- Record the result (date, host, what was checked) in each row's `notes` only if an operator needs it,
  and otherwise in the PR's *Testing* section.

## Task 6 — Hosted document

- Confirm every fleet image includes #169.
- Add the two `sources` entries and the two rows to `feeds/doublezero-edge-feeds-latest.json`. It is
  still aggregated by hand; the publisher's `feeds/binance/usdsm-latest.json` fragment reaches no
  consumer on its own.
- After publish, `connect.sh`'s "feed registry resolved" echo on a test host shows the new row count,
  and `/v1/status`'s `registry` block matches.

## Task 7 — Trades, once the publisher emits them

binance#69 adds `Trade`. Nothing in this crate changes: `emit_trades` is already `true`, the tape
owner ranks the single `TopOfBook` row per `(BINANCE, category)`, and the `Sticky` tape leader
admits it. What to do when it ships:

- Capture a datagram and check the decoded `trade_id`, `side` and `source_ts_ns`.
- Confirm prints for `(6, BTCUSDT)` and `(8, BTCUSDT)` are never deduplicated against each other.
- Drop "no trades yet" from both rows' `notes`.

## Task 8 — Public WebSocket backstop (optional, separate PR)

A `PublicVenue` for Binance in the pattern of `ingest::ws_input`, off by default, to fill gaps in the
edge feed. It is only worth building once the edge rows are live and measured, and two questions
decide whether it is honest:

- **Spot needs an Ed25519 API key even for public SBE market data**, while the JSON `bookTicker`
  endpoint does not. The backstop must use the same quantity the edge publishes (`bookTicker` BBO),
  and it must not carry a credential.
- **`source_ts` must be the same canonical value on both paths**, or the quote floor cannot collapse
  duplicates. The edge copy's stamp is set by the publisher from the venue's event time. The backstop
  must scale the same field to the same unit, and only a live side-by-side capture can confirm that.

Two venues (`fstream` for perps, `stream` for spot), one `PublicVenue` per Source ID, both gated on
the instrument being known under **that** Source ID (`instrument_known` already takes one).

## Task 9 — Docs

- `CLAUDE.md`: the Binance rows in the registry section, the two-engine shared name, and the
  unverified status until Task 5 passes.
- `docs/input-sources.md`: the socket count grows by 4 (2 rows x 2 ports); add the codes to the
  activation text.
- `docs/self-hosting.md`: the `sources` example gains the two entries.
- `docs/README.md`: link this plan until it is done, then remove the link.
- `CHANGELOG.md`: one entry per PR.

## PR split

1. Tasks 1-3 and 9 (the rows, the fallback, the tests, the docs). Mergeable before Task 5, because
   an unsubscribed host activates nothing; the rows say unverified until Task 5 passes.
2. Task 4 (the `source_id` filter).
3. Task 5 results, then Task 6 (hosted document).
4. Task 7 when binance#69 ships; Task 8 if and when it is wanted.
