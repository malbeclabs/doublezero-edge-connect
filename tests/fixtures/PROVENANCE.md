# Fixture provenance

These are golden wire-frame captures from the DoubleZero Edge HL publisher
(`malbeclabs/hyperliquid`, `app/publisher/`). Each `.bin` is a sequence of
`[u32 LE length][frame bytes]` records (the publisher's `encode_packets` format),
where each frame is a complete UDP datagram in the little-endian binary format
this bridge consumes. They are byte-validated against an independent spec oracle
in the source repo.

| File | Source (hyperliquid repo) | Port role |
|------|---------------------------|-----------|
| tob_marketdata.bin | server/tests/fixtures/hl_block_mode/golden/tob_marketdata.bin | TOB mktdata |
| tob_refdata.bin | server/tests/fixtures/hl_block_mode/golden/tob_refdata.bin | TOB refdata |
| mbo_mktdata.bin | server/tests/fixtures/hl_block_mode/golden/mbo_mktdata.bin | MBO mktdata |
| mbo_refdata.bin | server/tests/fixtures/hl_block_mode/golden/mbo_refdata.bin | MBO refdata (reordered, see below) |
| mbo_snapshot.bin | hand-crafted empty-book anchor (see below) | MBO snapshot |

Frames carry `source_id=3` (beta publisher-host value; becomes `source_id=1` in
production). Do not hard-code the source id.

**`tob_refdata.bin` record order:** The upstream capture tool (`first_packets_by_msg_type`) emitted
records in first-seen order: ChannelReset → InstrumentDefinition → ManifestSummary. The live wire
(and edge-feed-spec) requires ManifestSummary before the InstrumentDefinitions it covers, so the
records were reordered to ChannelReset → ManifestSummary → InstrumentDefinition. Frame bytes are
unmodified; only record order changed.

**`mbo_refdata.bin` record order:** Same first-seen-order issue as TOB. The upstream tool
emitted InstrumentDefinition (0x02) before ManifestSummary (0x07); the wire requires the
manifest first. Records were reordered to ManifestSummary → InstrumentDefinition (no
ChannelReset present in this capture). Frame bytes are unmodified; only record order changed.
The single instrument is BTC (instrument_id=0, price_exponent=-1, qty_exponent=-8).

**`mbo_snapshot.bin` is hand-crafted, NOT captured.** The publisher's golden generator does
not emit the snapshot port, so there is no captured snapshot golden to vendor. It is a single
frame carrying `SnapshotBegin(instrument_id=0, anchor_seq=0, total_orders=0, snapshot_id=1,
last_instrument_seq=0)` + `SnapshotEnd(instrument_id=0, anchor_seq=0, snapshot_id=1)` — an
**empty book** anchored at mktdata_seq 0. No order state is fabricated: the snapshot asserts
zero resting orders, and `book.rs` flips to `Synced` on the SnapshotEnd. The mktdata capture's
140 per-instrument deltas are contiguous starting at seq 1 (so `last_instrument_seq=0` makes the
first delta contiguous) and their carrying frame sequences are 1..6 (all `> anchor_seq=0`, so all
apply). The replayed deltas build the live book from real OrderAdds; `depth` flows.

The mktdata capture is **mid-session**: ~29 of its OrderCancels and 2 OrderExecutes reference
resting orders that were added before the capture began (no matching OrderAdd in the file).
Against the empty-anchor book these are harmless no-ops (`book.rs` `Cancel`/`Execute` skip
unknown order ids), so the resulting depth is the real subset of orders added during the
capture window — coherent, not fabricated. A fully faithful pre-capture book would require a
real snapshot-port capture from the same run (the publisher's golden generator does not produce
one today). The empty-anchor snapshot is the honest, intractable-state-free substitute for an
E2E depth-contract test.

**Side-mapping inversion (found and fixed during this E2E work):** the HL publisher encodes
`SIDE_BID=0 / SIDE_ASK=1` (`server/src/protocol/mbo/constants.rs`). The bridge's `codec_mbo.rs`
previously used `SIDE_BID=1 / SIDE_ASK=2` (inverted). That bug was caught by these E2E tests and
fixed: `codec_mbo.rs` now uses `0=Bid / 1=Ask`, matching the publisher.

**Regenerating `mbo_snapshot.bin`:** the file is hand-crafted in the `[u32 LE length][frame bytes]`
record format. Each frame is a complete codec_mbo datagram (24-byte frame header + 4-byte message
header per message). The single frame carries two messages: `SnapshotBegin(instrument_id=0,
anchor_seq=0, total_orders=0, snapshot_id=1, last_instrument_seq=0)` followed immediately by
`SnapshotEnd(instrument_id=0, anchor_seq=0, snapshot_id=1)`. Together they assert an empty book
anchored at mktdata_seq 0. Re-encode with `codec_mbo::encode_*` (or by hand from the spec layout)
and prefix with a u32 LE length equal to the frame byte count.

Regenerate the TOB/MBO mktdata+refdata by re-running the publisher's `hl_block_mode` golden
generation (`server/tests/fixtures/hl_block_mode/generate_from_source.py`) and copying the
goldens here. The MBO refdata reorder and the hand-crafted snapshot must be re-applied after any
regeneration (the generator does not emit the snapshot port).

## Multi-publisher TOB fixtures (live capture)

`tob_btc_pubA.*` and `tob_btc_pubB.*` are **two independent live publishers of the same
Hyperliquid TOB feed**, for the multi-publisher dedup work (issue #3). They are genuinely
independent — disjoint frame-sequence spaces (≈70.8M vs ≈53.7M) and distinct wire `source_id`
(3 vs 1) — and time-aligned (each spans the same ~40s window, `source_ts` 1781705333..1781705373).

| File | Publisher | Source IP | Infra id | mktdata port |
|------|-----------|-----------|----------|--------------|
| tob_btc_pubA.{refdata,mktdata}.bin | A | 148.51.120.79 | tob_aws_tyo_hl_mainnet2 | 9201 |
| tob_btc_pubB.{refdata,mktdata}.bin | B | 148.51.123.3  | tob_gcp_tyo_hl_mainnet1 | 9601 |

**What these fixtures are — and are not.** The two publishers do NOT republish the same venue
updates: each independently samples/coalesces the BBO, so within the shared window pub A emits 4109
BTC quotes and pub B emits 4669, and only ~370 (~9%) share an identical `source_ts`. When they DO
coincide the content matches (369/370 agree on the full bid/ask/size tuple), but coincidence is
under a tenth of each stream. So these exercise **real independent-publisher dedup** — merge two
samplings of one book — NOT a "mirror collapse to one stream"; the publishers are not mirrors.
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
**frames containing BTC** — a TOB frame batches several instruments, so a frame carrying BTC plus
others is kept whole (pub A: 1 such frame, 22 non-BTC messages retained); they are not strictly
BTC-only.

**Demux is by source IP, not UDP port** — publishers are on distinct ports today, but the feed
team intends to normalize that, so source IP is the robust publisher key.

**Codec validation against the live feed** (every frame decoded through the bridge's own codec):
- TOB: **0 framing errors** across ~130k frames from both publishers.
- MBO (same capture, `--protocol mbo`; not committed as fixtures — mktdata is ~12 MB/publisher):
  **0 framing errors** over ~36k frames / ~1.2M messages each (pub A: order_add=273757,
  order_cancel=273909, order_execute=4162, snapshot_msgs=384468, defs=1572, manifests=40). First
  real-feed check of the MBO framing offsets (previously only self-consistent); per-field offsets
  still rely on behavioral checks like the side-mapping fix.

**Regenerating** (the raw 635 MB pcap is intentionally NOT committed):

```
# capture on the recorder (read-only sniff; multicast is multi-listener):
sudo timeout 120 tcpdump -i doublezero1 -nn -s 0 -w tyo_tob.pcap 'host 233.84.178.15 and udp'
# then, with the worktree built (cargo build --example pcap2frames):
cargo run --example pcap2frames -- tyo_tob.pcap --src 148.51.120.79 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_pubA
cargo run --example pcap2frames -- tyo_tob.pcap --src 148.51.123.3 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_pubB
```

The converter (`examples/pcap2frames.rs`) demuxes one publisher by source IP, keeps TOB frames
(magic `0x445A`), filters mktdata to the chosen symbol, and writes the `[u32 LE length][frame]`
record format `tests/common/replay.rs` replays.

### `tob_btc_dual.combined.bin` — interleaved two-publisher golden

`tob_btc_pubA`/`tob_btc_pubB` are *separate* per-publisher captures; replaying them back-to-back
does **not** reproduce the real wire, where the two publishers' samples arrive **interleaved**. The
quote staleness floor drops a sample only when its `source_ts` is strictly older than the floor, so
its behavior depends on the real interleaving (a laggard's sample is stale only relative to whatever
the leader has already advanced past); the dedup test needs that ordering. `tob_btc_dual.combined.bin` is that: both publishers' refdata +
BTC-filtered mktdata in **capture order**, each record tagged `[u32 LE len][4B src_ip][1B role:
0=refdata,1=mktdata][frame]` (note the extra `src_ip`/`role` prefix — this is NOT the plain
`split_frames` format; the dedup test has its own reader). 235 refdata + 9330 mktdata frames, 0
decode errors. Regenerate:

```
cargo run --example pcap2frames -- tyo_tob.pcap \
  --src 148.51.120.79 --combined-with 148.51.123.3 --symbol BTC --to 40 \
  -o tests/fixtures/tob_btc_dual
```

### `tob_multi_dual.combined.bin` — multi-symbol two-publisher golden

`tob_btc_dual.combined.bin` is BTC-only. The dedup is keyed per `(venue, symbol)` with an
**independent staleness floor per symbol**, so a single-symbol fixture cannot prove that one symbol's
volume does not perturb another's dedup. `tob_multi_dual.combined.bin` is the multi-symbol counterpart:
the same two publishers, same 40s window and same record format, but carrying three symbols spanning
a volume spread — **BTC** (busy), **SOL** (medium) and **DOGE** (quiet). 235 refdata + 12940 mktdata
frames, 0 decode errors, ~1.4 MB.

Raw kept quote messages per `(symbol, publisher)` (the pre-dedup baseline):

| Symbol | 148.51.120.79 (A) | 148.51.123.3 (B) | tier |
|--------|-------------------|------------------|------|
| BTC    | 4370              | 4960             | busy |
| SOL    | 1501              | 1577             | medium |
| DOGE   | 251               | 281              | quiet |

(Counts are quote messages within the *kept* frames; a TOB frame batches several instruments, so a
frame carrying any selected symbol is kept whole and its other symbols' messages are counted too —
hence these tally only the selected ids.) DOGE at ~532 raw vs BTC's ~9330 is a ~17x volume gap, so a
test can assert DOGE dedups to exactly what it would on its own (no cross-symbol interference from
BTC's traffic). Regenerate:

```
cargo run --example pcap2frames -- tyo_tob.pcap \
  --src 148.51.120.79 --combined-with 148.51.123.3 \
  --symbol BTC --symbol SOL --symbol DOGE --to 40 \
  -o tests/fixtures/tob_multi_dual
```

`--symbol` is repeatable; omitting it entirely keeps all symbols (used to survey per-symbol volume
before picking the busy/quiet pair).

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
