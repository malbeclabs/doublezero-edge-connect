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

Frames carry `source_id=3` (beta publisher-host value; becomes `source_id=1` in
production). Do not hard-code the source id.

**`tob_refdata.bin` record order:** The upstream capture tool (`first_packets_by_msg_type`) emitted
records in first-seen order: ChannelReset → InstrumentDefinition → ManifestSummary. The live wire
(and edge-feed-spec) requires ManifestSummary before the InstrumentDefinitions it covers, so the
records were reordered to ChannelReset → ManifestSummary → InstrumentDefinition. Frame bytes are
unmodified; only record order changed.

Regenerate by re-running the publisher's `hl_block_mode` golden generation
(`server/tests/fixtures/hl_block_mode/generate_from_source.py`) and copying the
goldens here. MBO fixtures (added later) must come from the SAME generation run
so the snapshot anchor sequences align with the mktdata sequences.
