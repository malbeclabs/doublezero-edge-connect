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

`tob_btc_pubA.*` and `tob_btc_pubB.*` are **two independent publishers of the same live
Hyperliquid TOB feed**, for the multi-publisher dedup work (issue #3). Unlike the synthetic
goldens above, these are sliced from a real capture, so the two publishers carry identical venue
BBO content (same `source_ts`, bid/ask, sizes) on **independent, instance-scoped frame
sequences** — exactly the condition the dedup must collapse (a same-bytes replay would be
collapsed by the per-channel sequence tracker instead, and prove nothing).

| File | Publisher | Source IP | Infra id | mktdata port |
|------|-----------|-----------|----------|--------------|
| tob_btc_pubA.{refdata,mktdata}.bin | A | 148.51.120.79 | tob_aws_tyo_hl_mainnet2 | 9201 |
| tob_btc_pubB.{refdata,mktdata}.bin | B | 148.51.123.3  | tob_gcp_tyo_hl_mainnet1 | 9601 |

Both are `BTC` (instrument_id 0), windowed to the first 40s of the capture. The window is ≥~35s
on purpose: the exact-`BTC` instrument definition re-sends on a ~30s round-robin (786 instruments,
~3144 defs/120s), so a shorter window omits it and the bridge's precision gate never resolves BTC.
The `.refdata.bin` files carry all in-window definitions+manifest (so precision resolves); the
`.mktdata.bin` files carry only BTC quotes/trades.

**Demux is by source IP, not UDP port** — publishers are on distinct ports today, but the feed
team intends to normalize that, so source IP is the robust publisher key.

**Codec validation:** the converter decodes every frame through the bridge's own `ingest::codec`;
this capture produced **0 TOB decode errors** across ~130k frames from both publishers, validating
the TOB codec byte offsets against the live feed (beyond the byte-validated reference).

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
