# Fixture provenance

Every `.bin` is a sequence of `[u32 LE length][datagram bytes]` records (the publisher's
`encode_packets` format), where each record is one complete datagram in the little-endian binary
format this bridge consumes. The TOB goldens below come from the DoubleZero Edge HL publisher
(`malbeclabs/hyperliquid`, `app/publisher/`) and are byte-validated against an independent spec
oracle in the source repo. The **live-capture** fixtures (the MBO trio, the multi-publisher TOB
files, and the shreds) are described in their own sections.

| File | Source | Port role |
|------|--------|-----------|
| tob_marketdata.bin | server/tests/fixtures/hl_block_mode/golden/tob_marketdata.bin | TOB mktdata |
| tob_refdata.bin | server/tests/fixtures/hl_block_mode/golden/tob_refdata.bin | TOB refdata |
| mbo_refdata.bin | TYO recorder capture, publisher 148.51.123.3 (see "MBO fixtures") | MBO refdata |
| mbo_snapshot.bin | TYO recorder capture, publisher 148.51.123.3 (see "MBO fixtures") | MBO snapshot (real two-sided) |
| mbo_mktdata.bin | TYO recorder capture, publisher 148.51.123.3 (see "MBO fixtures") | MBO mktdata |
| phoenix_tob_refdata.bin | Phoenix capture, publisher 148.51.122.75 / source_id=2 (see "Phoenix TOB fixtures") | TOB refdata |
| phoenix_tob_marketdata.bin | Phoenix capture, publisher 148.51.122.75 / source_id=2 (see "Phoenix TOB fixtures") | TOB mktdata |

The TOB goldens carry `source_id=3` (beta publisher-host value; becomes `source_id=1` in
production); the MBO trio carries `source_id=1` (publisher 148.51.123.3). Do not hard-code the
source id.

**`tob_refdata.bin` record order:** The upstream capture tool (`first_packets_by_msg_type`) emitted
records in first-seen order: ChannelReset → InstrumentDefinition → ManifestSummary. The live wire
(and edge-feed-spec) requires ManifestSummary before the InstrumentDefinitions it covers, so the
records were reordered to ChannelReset → ManifestSummary → InstrumentDefinition. Datagram bytes are
unmodified; only record order changed.

The **MBO trio** (`mbo_refdata.bin` / `mbo_snapshot.bin` / `mbo_mktdata.bin`) is a real two-sided
recorder capture — see "MBO fixtures (real two-sided capture)" below. It replaces the earlier
hand-crafted empty-anchor snapshot from PR #2; no manual reorder or hand-crafting applies.

Regenerate the **TOB** mktdata+refdata by re-running the publisher's `hl_block_mode` golden
generation (`server/tests/fixtures/hl_block_mode/generate_from_source.py`) and copying the
goldens here. The `tob_refdata.bin` reorder must be re-applied after regeneration.

## MBO fixtures (real two-sided capture)

`mbo_refdata.bin`, `mbo_snapshot.bin`, and `mbo_mktdata.bin` are extracted from the same TYO
recorder capture as the multi-publisher TOB fixtures below (the raw 635 MB pcap is NOT committed),
publisher **148.51.123.3** (wire `source_id=1`), instrument **BTC** (instrument_id 0,
price_exponent -8, qty_exponent -5). They are a **real, two-sided** book — not the hand-crafted
empty-anchor of PR #2 — so `mbo_single_publisher_depth_contract`'s crossed-book assertion
(`best_bid < best_ask`) is active and green.

- `mbo_snapshot.bin` is one COMPLETE snapshot group (`snapshot_id=1106238`, `anchor_seq=11243876`,
  44598 resting orders: 28345 bids + 16253 asks). `book.rs` installs it on `SnapshotEnd` and the
  book is `Synced` two-sided immediately. The publisher's other BTC snapshot groups in this capture
  lost packets (received order count < the begin's promised `total_orders`); the converter selects
  the first group that is complete.
- `mbo_mktdata.bin` is the 125 datagrams carrying BTC's contiguous post-anchor deltas (per-instrument
  seq 26761713..26762012), which apply live after the snapshot.
- `mbo_refdata.bin` is one manifest era + BTC's definition, enough to resolve precision. The live
  publisher emits `ManifestSummary` with `Valid=0` (same as the TOB publisher); the bridge's
  `MboProcessor` overrides it to valid (logged once), matching `TobProcessor`.

**Side-mapping inversion (found and fixed during the E2E work):** the HL publisher encodes
`SIDE_BID=0 / SIDE_ASK=1` (`server/src/protocol/mbo/constants.rs`); `codec_mbo.rs` once used the
inverted `1/2`. It now uses `0=Bid / 1=Ask`. The active two-sided crossed-book assertion means a
re-inversion would fail loudly rather than silently cross the book.

Codec validation: every datagram decodes through the bridge's own `codec_mbo` during extraction (0
framing errors), and the selected snapshot group's order count is checked against the begin's
promised total before it is written.

Regenerate (single command; capture the raw pcap as in the multi-publisher TOB section, then build
the worktree — `cargo build --example pcap2datagrams`):

```
cargo run --example pcap2datagrams -- tyo_tob.pcap \
  --protocol mbo --src 148.51.123.3 --symbol BTC --mbo-minimal \
  -o tests/fixtures/mbo_btc
# writes mbo_btc.{refdata,snapshot,mktdata}.bin; rename to mbo_{refdata,snapshot,mktdata}.bin
```

`--mbo-minimal` trims a deep-book MBO capture to a ~2 MB committed fixture: the first complete
snapshot group, the contiguous post-anchor deltas (capped by `--mbo-max-deltas`, default 300), and
a minimal refdata (one manifest + the symbol's definition). It requires exactly one `--symbol`.

## Multi-publisher TOB fixtures (live capture)

`tob_btc_pubA.*` and `tob_btc_pubB.*` are **two independent live publishers of the same
Hyperliquid TOB feed**, for the multi-publisher dedup work (issue #3). They are genuinely
independent — disjoint datagram-sequence spaces (≈70.8M vs ≈53.7M) and distinct wire `source_id`
(3 vs 1) — and time-aligned (each spans the same ~40s window, `source_ts` 1781705333..1781705373).

| File | Publisher | Source IP | mktdata port |
|------|-----------|-----------|--------------|
| tob_btc_pubA.{refdata,mktdata}.bin | A | 148.51.120.79 | 9201 |
| tob_btc_pubB.{refdata,mktdata}.bin | B | 148.51.123.3  | 9601 |

**What these fixtures are — and are not.** The two publishers do NOT republish the same venue
updates: each independently samples/coalesces the BBO, so within the shared window pub A emits 4109
BTC quotes and pub B emits 4669, and only ~370 (~9%) share an identical `source_ts`. When they DO
coincide the content matches (369/370 agree on the full bid/ask/size tuple), but coincidence is
under a tenth of each feed. So these exercise **real independent-publisher dedup** — merge two
samplings of one book — NOT a "mirror collapse to one feed"; the publishers are not mirrors.
Quotes dedup by a per-`(venue, symbol)` `source_ts` staleness floor keyed on raw BBO content: it
keeps every distinct top-of-book change at the newest `source_ts` — including multiple distinct BBOs
that share a `source_ts`, which are real intra-tick updates (this matches the `hl-bbo-feed-race`
board's `(symbol, source_ts, bbo_hash)` identity) — but drops a lagging publisher's strictly-older
BBO (stale: the market moved on) and any exact `(source_ts, content)` duplicate. Because the two
publishers interleave, the laggard's older-tick replays are dropped, so the deduped count falls
between the raw count and the much smaller per-tick count a strict high-watermark would keep (the
watermark over-drops: it discards real intra-tick BBO changes, not just stale replays). A dedup test
on these must assert no business duplicates AND that emitted `source_ts` is **non-decreasing** (not
strictly increasing) per `(venue, symbol)`.

Both are `BTC` (instrument_id 0), windowed to the first 40s of the capture. The window is ≥~35s on
purpose: the exact-`BTC` definition re-sends on a ~30s round-robin (786 instruments, ~3144
defs/120s), so a shorter window omits it and the precision gate never resolves BTC. The
`.refdata.bin` files carry all in-window definitions+manifest. The `.mktdata.bin` files carry
**datagrams containing BTC** — a TOB datagram batches several instruments, so a datagram carrying BTC plus
others is kept whole (pub A: 1 such datagram, 22 non-BTC messages retained); they are not strictly
BTC-only.

**Demux is by source IP, not UDP port** — publishers are on distinct ports today, but the feed
team intends to normalize that, so source IP is the robust publisher key.

**Codec validation against the live feed** (every datagram decoded through the bridge's own codec):
- TOB: **0 framing errors** across ~130k datagrams from both publishers.
- MBO (same capture, `--protocol mbo`; not committed as fixtures — mktdata is ~12 MB/publisher):
  **0 framing errors** over ~36k datagrams / ~1.2M messages each (pub A: order_add=273757,
  order_cancel=273909, order_execute=4162, snapshot_msgs=384468, defs=1572, manifests=40). First
  real-feed check of the MBO framing offsets (previously only self-consistent); per-field offsets
  still rely on behavioral checks like the side-mapping fix.

**Regenerating** (the raw 635 MB pcap is intentionally NOT committed):

```
# capture on the recorder (read-only sniff; multicast is multi-listener):
sudo timeout 120 tcpdump -i doublezero1 -nn -s 0 -w tyo_tob.pcap 'host 233.84.178.15 and udp'
# then, with the worktree built (cargo build --example pcap2datagrams):
cargo run --example pcap2datagrams -- tyo_tob.pcap --src 148.51.120.79 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_pubA
cargo run --example pcap2datagrams -- tyo_tob.pcap --src 148.51.123.3 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_pubB
```

The converter (`examples/pcap2datagrams.rs`) demuxes one publisher by source IP, keeps TOB datagrams
(magic `0x445A`), filters mktdata to the chosen symbol, and writes the `[u32 LE length][datagram]`
record format `tests/common/replay.rs` replays.

### `tob_btc_dual.combined.bin` — interleaved two-publisher golden

`tob_btc_pubA`/`tob_btc_pubB` are *separate* per-publisher captures; replaying them back-to-back
does **not** reproduce the real wire, where the two publishers' samples arrive **interleaved**. The
quote staleness floor drops a sample only when its `source_ts` is strictly older than the floor, so
its behavior depends on the real interleaving (a laggard's sample is stale only relative to whatever
the leader has already advanced past); the dedup test needs that ordering. `tob_btc_dual.combined.bin` is that: both publishers' refdata +
BTC-filtered mktdata in **capture order**, each record tagged `[u32 LE len][4B src_ip][1B role:
0=refdata,1=mktdata][datagram]` (note the extra `src_ip`/`role` prefix — this is NOT the plain
`split_datagrams` format; the dedup test has its own reader). 235 refdata + 9330 mktdata datagrams, 0
decode errors. Regenerate:

```
cargo run --example pcap2datagrams -- tyo_tob.pcap \
  --src 148.51.120.79 --combined-with 148.51.123.3 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_dual
```

### `tob_multi_dual.combined.bin` — multi-symbol two-publisher golden

`tob_btc_dual.combined.bin` is BTC-only. The dedup is keyed per `(venue, symbol)` with an
**independent staleness floor per symbol**, so a single-symbol fixture cannot prove that one symbol's
volume does not perturb another's dedup. `tob_multi_dual.combined.bin` is the multi-symbol counterpart:
the same two publishers, same 40s window and same record format, but carrying three symbols spanning
a volume spread — **BTC** (busy), **SOL** (medium) and **DOGE** (quiet). 235 refdata + 12940 mktdata
datagrams, 0 decode errors, ~1.4 MB.

Raw kept quote messages per `(symbol, publisher)` (the pre-dedup baseline):

| Symbol | 148.51.120.79 (A) | 148.51.123.3 (B) | tier |
|--------|-------------------|------------------|------|
| BTC    | 4370              | 4960             | busy |
| SOL    | 1501              | 1577             | medium |
| DOGE   | 251               | 281              | quiet |

(Counts are quote messages within the *kept* datagrams; a TOB datagram batches several instruments, so a
datagram carrying any selected symbol is kept whole and its other symbols' messages are counted too —
hence these tally only the selected ids.) DOGE at ~532 raw vs BTC's ~9330 is a ~17x volume gap, so a
test can assert DOGE dedups to exactly what it would on its own (no cross-symbol interference from
BTC's traffic). Regenerate:

```
cargo run --example pcap2datagrams -- tyo_tob.pcap \
  --src 148.51.120.79 --combined-with 148.51.123.3 \
  --symbol BTC --symbol SOL --symbol DOGE --to 40 \
  -o tests/fixtures/tob_multi_dual
```

`--symbol` is repeatable; omitting it entirely keeps all symbols (used to survey per-symbol volume
before picking the busy/quiet pair).

## Phoenix TOB fixtures (live edge+public capture)

`phoenix_tob_refdata.bin` / `phoenix_tob_marketdata.bin` are a clean **Phoenix-only** slice of a
concurrent edge-multicast + public-API capture taken 2026-06-30 (`scripts/phoenix_capture.py` on
branch `bdz/phoenix-capture-script`; raw artifacts archived under
`worktrees/edge-pcaps/phoenix-capture-20260630/`). They back the Phoenix public-trade backstop
(`ingest::phoenix_feeder`, #53), its decode golden (`tests/codec_phoenix_fixtures.rs`), and its E2E
(`tests/phoenix_arbitrage.rs`).

**Why a slice — the source-id filter.** The capture host wildcard-bound `("", 9201/9202)`, so it
received BOTH publishers on the Phoenix ports: Phoenix (`148.51.122.75`, `source_id=2`) plus the
Hyperliquid publisher (`148.51.120.79`, `source_id=1`) leaking in (10,580 of 10,839 captured trades
were the Hyperliquid leak). The bridge itself does NOT see this — its receiver binds the group
address, not INADDR_ANY (`receiver::bind_multicast`). Only the `148.51.122.75` datagrams (Phoenix,
`source_id=2`) were kept; every Hyperliquid datagram was dropped before writing the fixtures.

**Contents.**
- `phoenix_tob_refdata.bin` (5 datagrams): one complete manifest era — `ManifestSummary{valid,
  manifest_seq=11, instrument_count=51}` first, then all 51 Phoenix `InstrumentDefinition`s at seq
  11. The manifest leads on purpose: the subscriber drops a definition whose `manifest_seq` doesn't
  match the latest manifest, so a def-before-manifest fixture would define nothing.
- `phoenix_tob_marketdata.bin` (409 datagrams, ~36 KB): the first Phoenix mktdata datagrams, carrying
  8 real `source_id=2` trade prints (SOL, BTC, AMD, INTC, META, MSFT, CRWV, AMZN) plus quotes. The
  pinned trade ids (BTC 869424, SOL 1188189, AMD 20418) are the on-chain trade sequence numbers; the
  same capture's public side reported them verbatim as `tradeSequenceNumber` — dedup-key verification
  #1 (257/257 shared fills matched, 0 mismatches).

Phoenix names each market with the same bare ticker on the edge and public feeds, and the edge
`instrument_id` equals the public `assetId` (id 0 = SOL, 1 = BTC, 45 = AMD, …). These carry
`source_id=2`; do not hard-code the source id.

**Regenerate.** Re-run `scripts/phoenix_capture.py --iface doublezero1 --secs 180` on a host with
both edge multicast and internet reach, keep only the datagrams from the Phoenix publisher IP (the
one carrying `source_id=2`), assemble a manifest-first refdata era, take a small mktdata slice with
real trades, and length-prefix both files (`[u32 LE len][datagram]`).

### `mbo_btc_dual.combined.bin` — two-publisher Market-by-Order golden

The MBO counterpart of `tob_btc_dual.combined.bin`, for the multi-publisher **depth** dedup (issue #3,
MBO half). Same two live HL publishers (A `148.51.120.79`, B `148.51.123.3`) and same `tyo_tob.pcap`,
BTC only. Record format is the same `[u32 len][4B src_ip][1B role][datagram]`, with a third role for the
snapshot port: **0=refdata, 1=mktdata, 2=snapshot**. 130 refdata + 2 snapshot + 1267 mktdata datagrams, 0
decode errors, ~1.6 MB. Replaying each publisher's records through `MboProcessor` reconstructs its BTC
book and emits depth (pub A 636, pub B 633) over an overlapping `source_ts` range — the cross-publisher
region the dedup must collapse.

**The 2 snapshot datagrams are SYNTHESIZED, not captured.** They are the same honest empty-book anchor
`mbo_snapshot.bin` uses, but `pcap2datagrams --empty-anchor` computes one per publisher from that
publisher's first in-window delta: `SnapshotBegin total_orders=0` + `SnapshotEnd`, with
`last_instrument_seq`/`anchor_seq` set one below that delta so it is contiguous after the anchor and the
book syncs immediately. Against the empty book the pre-window orders' cancels/executes no-op (as in the
single fixture) and the window's real, interleaved deltas build a coherent subset — real interleaving
and real dedup, no fabricated book state.

Why not **real** snapshots: the snapshot port round-robins the full book ~once per ~30 s, the two
publishers are out of phase (for BTC, pub B dumps it in 19 ms at t≈3.6 s while pub A streams it over
~31 s; for DOGE, pub B at t≈2.5 s, pub A at t≈15.7 s), and a book syncs only on a snapshot that arrives
**after** its definition (the def is itself on a ~30 s round-robin). No small window satisfies "def,
then a complete real snapshot, then contiguous deltas" for *both* publishers at once; the empty anchor
sidesteps all of it. `pcap2datagrams` keeps the real-snapshot mode (default) plus a window-coherence
report (definition time + per-publisher snapshot groups) so an aligned capture could still use it.

**MBO `Valid=0` manifest workaround (found minting this).** The raw capture's MBO `ManifestSummary`
carries `Valid=0` (seq=5, count=786) — the same live-publisher quirk `TobProcessor` already overrides.
`MboProcessor` previously passed `m.valid` through, which clears all definitions, so precision never
resolves and the **MBO feed emits zero depth in production**. The e2e MBO test missed it because the
vendored golden has `Valid=1`. `MboProcessor` now overrides `Valid=0`→true exactly like TOB (logged
once, `REVISIT`); `mbo_manifest_valid_zero_is_overridden_so_depth_flows` pins it. Without the override
this fixture's books never sync.

Regenerate (BTC, 3 s delta window after the t≈7.5 s definition):

```
cargo run --example pcap2datagrams -- tyo_tob.pcap \
  --protocol mbo --src 148.51.120.79 --combined-with 148.51.123.3 \
  --symbol BTC --from 9 --to 12 --empty-anchor -o tests/fixtures/mbo_btc_dual
```

`--from`/`--to` bound the snapshot/delta window; refdata is always kept from t=0 so the slow-round-robin
BTC definition resolves. A combined two-publisher depth dedup test must reconstruct an **independent book
per `(publisher, instrument)`** (issue #3, MBO item 1) — feeding both publishers' BTC deltas to one
instrument-keyed book collides their per-instrument sequences.

## Solana shred fixtures (`shred_sample.bin`, `shred_leaders.json`)

Unlike the HL fixtures above, these are a **live capture** from the DoubleZero `edge-solana-*`
shred multicast feed on mainnet-beta (an edge-scoreboard host subscribed to `edge-solana-shreds`
233.84.178.1, `edge-solana-retrans-amer` 233.84.178.14, and `edge-solana-root` 233.84.178.16, all
port 7733). They validate `src/shred/parse.rs`/`verify.rs`/`dedup.rs` against **real Solana shreds**
— a stronger oracle than the self-consistency round-trips in `parse.rs`, which cannot catch a
constant both construction and verification share.

| File | What |
|------|------|
| shred_sample.bin | 117 real shred datagrams, `[u32 LE len][datagram]` records (same format as above), curated from a single mainnet slot (427286518, epoch 989) to cover all four chained-merkle variant bytes `0x66`/`0x76`/`0x96`/`0xb6` plus cross-group duplicates (63 unique `(slot,index,type)` keys). |
| shred_leaders.json | `{slot: base58 leader pubkey}` for the fixture's slot, from `getLeaderSchedule`+`getEpochInfo` at capture time (epoch 989, first_slot 427248000). Slot 427286518 leader = `GREEDkgav1ox1jYyd9Anv6exLqKV2vYnxMw5prGwmNKc`. |

`fixture_tests.rs` asserts every datagram parses and ed25519-verifies against its slot leader, and
that dedup forwards exactly one copy per key. These tests caught three transcription bugs in the
originally-unvalidated `parse.rs` offsets (all flagged "NOT validated against a live hexdump"):
1. the chained-merkle **data** variant byte is `0x90`, not the assumed `0xa0` — `0x96` was ~half the
   data shreds on the wire and silently fell through to "unparseable";
2. merkle **data** shreds are **1203** bytes on the wire, code **1228** — the parser used a single
   1228 constant and misplaced the proof for data shreds;
3. the merkle hash domain prefixes are `\x00SOLANA_MERKLE_SHREDS_LEAF` / `\x01SOLANA_MERKLE_SHREDS_NODE`,
   not bare `\x00`/`\x01` — so every merkle root (data **and** code) was wrong and nothing verified.

**Regenerating:** capture with `tcpdump -i doublezero1 -s 0 'udp and net 233.84.178.0/24'` on a host
subscribed to the `edge-solana-*` groups, then re-run the extraction (curate datagrams covering all
variant bytes + multi-group keys into the record format, and build `shred_leaders.json` by inverting
a current `getLeaderSchedule` for the captured slots). The leader schedule must be fetched while the
captured epoch is still within the RPC's retention.

## Market-by-price fixtures

Real captures of the Lashay publisher, datagram magic `0x4442`, taken 2026-08-07 from a host with the
DoubleZero tunnel up. **These are an interim capture** — a longer one with publisher fixes is
expected, and `tests/codec_mbp_fixtures.rs` asserts invariants rather than recorded counts so a
re-capture drops in without editing a number. Two sets, because they cover different things.

### `mbp_{refdata,mktdata,snapshot}.bin` — the sharded feed (primary)

The deployment shape the code will actually ingest: **three `Channel ID`s on one group**, so this is
the only fixture that exercises per-channel snapshot grouping. Delta feed is thin.

| | |
|---|---|
| Source | market-by-price group `233.84.178.20`, publisher `148.51.120.6` |
| Ports | `33010`/`33063`/`33120` mktdata, `43010`/`43063`/`43120` refdata, `53010`/`53063`/`53120` snapshot |
| Channels | 10, 63, 120 (encoded in the port number; each an independent state machine) |
| Capture | 2026-08-07 16:54:58 UTC, 39.6s, 12,535 market-by-price datagrams |
| Whole feed | 1,238 instruments; 3,268/3,268 complete snapshot groups; `depth_bound == 0` on all |
| Committed | filtered to `XNFLCOTY-27-BSCH` (channel 10) and `XNCAAFSEC-26-UGA` (channel 63) — 285 refdata, 24 snapshot, 143 mktdata datagrams |

### `mbp_perps_{refdata,mktdata,snapshot}.bin` — the dense feed

One channel, thousands of contiguous per-instrument deltas. This is what pins sequence handling; the
sharded set above is too quiet to. **From the older publisher**, which is being retired — see the
known deviations below before treating anything here as normative.

| | |
|---|---|
| Source | market-by-price group `233.84.178.4`, publisher `148.51.121.69` |
| Ports | `31000` mktdata, `41000` refdata, `51000` snapshot |
| Capture | 2026-08-07 16:55:54 UTC, first 8s of a 39s capture, 2,712 datagrams |
| Whole capture | 13 instruments; 101/101 complete snapshot groups; `depth_bound == 0` on all; `KXBTCPERP` ran 12,892 deltas over `1294579..1307470` with zero gaps and zero duplicates |
| Committed | filtered to `KXBTCPERP` — 9 refdata, 20 snapshot, 997 mktdata datagrams; snapshot rotation is every 5s so the window holds 2 complete groups |

### Known deviations in these captures

Recorded so a later capture can be checked against them, and so nothing here is mistaken for the
protocol's intent. All three are publisher-side, not decoder-side.

1. **The two redundant paths of the older feed stamp different `Channel ID`s** (`1` and `2`) while
   carrying an identical instrument set — same ids, same symbols. The spec defines `Channel ID` as
   sharding *the active instrument set* across instances, which these two are not doing. It matters
   beyond tidiness: a market key that includes the channel would put the two paths on separate keys,
   so they would never arbitrate against each other. Treated as a defect of the publisher being
   retired; the sharded feed above does channels correctly (zero instrument-id overlap between them).
2. **Symbols overflow the 16-byte symbol field on the sharded feed.** 2,312 of its definitions carry
   no NUL terminator, and `EAVE-27JAN01-YES` is the truncation of two *different* instrument ids
   (1165 and 1403, both channel 120). `InstrumentSnapshot` is now keyed on the `(venue, channel,
   instrument_id)` identity rather than this display label, so both markets survive; `DepthSnapshot`
   (Market-by-Order only, unaffected by this feed) is still keyed `(venue, symbol)`.
3. **No `BookClear`, `InstrumentReset`, `BatchBoundary` or `EndOfSession`** in either capture, so
   those four types remain offset-test-only — the status `codec_mbo`'s `InstrumentReset`/`Heartbeat`/
   `EndOfSession` have. Three are exceptional events and a quiet window explaining their absence is
   expected; `BatchBoundary` is not, so confirm whether the publisher emits it at all.

### Measured: the two perps paths use disjoint `trade_id` conventions

The paths split cleanly, and identically on both protocols. Measured 2026-08-07 with
`examples/pcap2datagrams.rs`, which reports `zero_id_trades=` alongside `trades=`; `--src` selects one
publisher, so one run per source IP gives the per-path answer.

| Path | Protocol | trades | `zero_id_trades` |
|---|---|---|---|
| `148.51.121.69` | top-of-book | 102 | 102 (always) |
| `148.51.120.6`  | top-of-book | 65  | 0 (never) |
| `148.51.121.69` | market-by-price | 102 | 102 (always) |
| `148.51.120.6`  | market-by-price | 65  | 0 (never) |

**This is why the tape gate is `trade_id`-independent rather than a sentinel latch.** One path's prints
bypass the dedup window (the `0` sentinel means "no venue trade id"); the peer's carry real ids and
route to `WindowedDedup`. The two copies of one fill therefore never meet in either mechanism, so a
sentinel-only gate would collapse nothing and every print would double.

Two limits on what this shows. The captures do not overlap in time, so this is disjoint id
conventions on a shared group, not a captured duplicate — a simultaneous two-path capture would
demonstrate it outright. And it does not establish whether the two paths stamp *different real* ids
for the same fill; that needs content-matched id sets across paths and is not measured here. The
id-independent gate covers that case regardless.

Also worth knowing: the two paths' captures do **not** overlap in time (the older feed's are ~16s
apart), so no two-path interleaved fixture can be cut from them — a future capture should run both
publishers simultaneously. `--combined-with` is not implemented for `--protocol mbp` either.

### Regenerating

```
sudo timeout 60 tcpdump -i doublezero1 -nn -s 0 -w mbp.pcap 'host <group> and udp'
cargo run --example pcap2datagrams -- mbp.pcap --protocol mbp --group <group> \
  --src <publisher-ip> --symbol <sym> -o tests/fixtures/mbp
```

Keep at least one multi-channel set and one dense-delta set; the fixture tests assert both shapes.
Record the source IP, capture date, datagram counts and observed `depth_bound` above.

## Schema v3 reference data — `tob_v3`, `mbp_perps_v3`, `sports_v3`

Cut 2026-08-11 from the mainnet capture warehouse
(`s3://malbeclabs-multicast-pcap-warehouse/mainnet-beta/aws-cmh-mn-recorder1-16.59.144.33/2026/08/11/18/`),
refdata plane only, ~12 KB each:

| fixture | group | port | datagrams | sources |
|---|---|---|---|---|
| `tob_v3.refdata.bin` | `233.84.178.3` | 41000 | 114 | `148.51.121.69`, `148.51.120.6` |
| `mbp_perps_v3.refdata.bin` | `233.84.178.4` | 42000 | 113 | `148.51.120.6`, `148.51.121.69` |
| `sports_v3.refdata.bin` | `233.84.178.20` | 44041 | 13 | `148.51.121.250`, `148.51.121.209` |

**What they establish, and why they were cut.** Every datagram carries `schema_version = 3`, and the
symbol field is the widened one: `sports_v3` holds 99 distinct symbols up to **33 characters**, far
past the 16-byte field that made the older fixtures truncate. So the symbol collision recorded above —
one cut-off symbol standing for two instrument ids — **cannot occur on this schema**, which is what
retires the argument that trade dedup keyed on `(venue, symbol)` can silently drop a second market's
fills. These fixtures are the evidence for that claim; without them it rests on an upstream assertion.

**Both paths are present in every set** — unlike the older captures, these are simultaneous, so an
interleaved two-path fixture can be cut from this warehouse when one is needed.

### Regenerating

```
aws s3api get-object --bucket malbeclabs-multicast-pcap-warehouse \
  --key <prefix>/capture_<group>_000001.pcap --range bytes=0-12000000 slice.pcap
tshark -r slice.pcap -Y 'udp.dstport==<refdata port>' -T fields -e data.data
```

Concatenate the payloads; the fixture is raw datagrams back to back, split by magic like every other set
here. A ranged fetch is deliberate — the warehouse objects are 50 MB each and a refdata cycle is
seconds.
