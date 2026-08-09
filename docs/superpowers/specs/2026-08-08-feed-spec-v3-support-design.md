# Feed spec v3.0.0 support in edge-connect — design

**Date:** 2026-08-08 (revised for `3.0.0`)
**Status:** Approved
**Scope:** `src/ingest/{codec_common,codec,codec_mbo,codec_mbp,codec_midpoint}.rs`, `src/metrics.rs`. Decode only —
no change to `PROTOCOL.md`, the WebSocket output, or any processor.

## Goal

Decode both `Schema Version` 1 and 3 frames on the Top-of-Book, Market-by-Order and Market-by-Price
feeds, so the bridge keeps working across the publishers' v3 rollout in either direction and in any
order. No publisher has moved yet, so this lands before the first flip rather than in response to one.

## 1. Ground truth

**Two wire generations reach production, and `2.0.0` is not one of them.** `2.0.0` widened
`InstrumentDefinition`'s `Symbol` from `char[16]` to `char[64]`. Before any publisher shipped it,
`3.0.0` ([edge-feed-spec#29](https://github.com/malbeclabs/edge-feed-spec/pull/29)) inserted `Source ID`
(`u16`) after `Instrument ID`, moving every later field two more bytes and growing the message from 128
to 130 bytes. Publishers go from `1.x` straight to `3.x`, so **v2 will never be emitted**.

**Only `InstrumentDefinition` ever moved.** Across both breaking releases no other message type changed.
The market-data path — `Quote`, `Trade`, the order- and price-keyed payloads — is byte-identical in v1
and v3.

**Midpoint stays at `1.0.0`.** It kept a slimmer 64-byte definition variant through both releases, so its
`Schema Version` remains `1`. This is `VERSIONING.md`'s scheme working as designed: the specs are
siblings, not one versioned family, and a decoder reads each feed's byte to know which layout it holds.

**Our decoders do not validate `Schema Version` today, except one.** `decode_frame_with` checks `Magic`
and nothing else; `codec.rs`, `codec_mbo.rs` and `codec_midpoint.rs` read the byte into `FrameHeader` and
never look at it again. Only `codec_mbp.rs` gates. Nothing downstream checks it either. Every spec is
explicit that a decoder MUST reject a version it does not implement rather than attempt a best-effort
parse; three of our four codecs are non-conformant on that point.

**That gap is what makes the rollout dangerous, and it fails in two opposite ways.** On Top-of-Book and
Market-by-Order a v3 frame would parse at v1 offsets: the 130-byte body passes the message walk, then
`price_exponent` is read at `body+37`, which under v3 lands *inside* the 64-byte `Symbol` field. Every
price for that instrument is then scaled by a garbage power of ten, and `manifest_seq` reads garbage too,
corrupting the reference-data state machine — silently, with no error and no warning. On Market-by-Price
the existing gate rejects the frame whole, so the feed goes dark behind a rate-limited decode warning.
Loud and safe, but still an outage.

**A dual-version reference implementation exists.** `edge-multicast-ref`'s
`go/topofbook-parser/tob/topofbook_wire.go` already dispatches on the version byte with per-version
symbol-width and body-length constants, named to match its Market-by-Order and Market-by-Price siblings.
It is both our byte-level oracle and a worked example of the same problem.

**Real v3 frames will exist on merge.** edge-feed-spec#29 regenerates the conformance tool's
`conformant_tob.pcap` and `conformant_mbo.pcap` against the new layout, so v3 can ship byte-validated
against real frames rather than offset-guessed — the trap `codec_midpoint.rs` is still in.

## 2. Decisions

### 2.1 One shared definition decoder, not three

`codec.rs`, `codec_mbo.rs` and `codec_mbp.rs` each define a byte-identical `InstrumentDefinition` (same
fields, each implementing `InstrumentDef`) and each decode it at identical offsets. That triplication is
why a one-message spec change is a three-place edit, and it is the same structural weakness that let two
of the three ship with no version gate while the third had one.

The struct, the version constants and a version-aware decoder move to `codec_common.rs`. Each of the
three re-exports the struct under its existing path:

```rust
pub use crate::ingest::codec_common::InstrumentDefinition;
```

so `codec::InstrumentDefinition`, `PerPublisher<codec_mbo::InstrumentDefinition>` and every downstream
generic keep resolving unchanged. The offset table then exists once, is validated once, and a fourth
generation is a one-place change.

**Rejected: per-codec dispatch.** Smaller per-file diff, and it preserves each codec's self-containment,
which is the current idiom. But it writes the v3 offset table three times and re-bets on three copies
staying in sync — a bet this codebase has already lost once.

**Rejected: a data-driven layout descriptor.** Offsets as a table selected by version extends cleanly to
further generations, but offsets-as-data are far harder to check by eye against the spec's own table than
named field reads. With v2 never reaching the wire there are still only two live layouts, so this stays
rejected on the same grounds as before.

### 2.2 Supported versions are a set, not a range

`decode_frame_with` gains a `supported_versions` parameter and rejects anything outside it. The sets are
`{1, 3}` for Top-of-Book, Market-by-Order and Market-by-Price, and `{1}` for midpoint.

This is the one decision the `2.0.0` draft of this document got wrong. It specified a contiguous
`1..=max` ceiling, which is how the reference implementation gates. With v2 skipped the supported
versions have a hole in them, and a ceiling of 3 would accept a version we deliberately do not implement.

**v2 is rejected, not decoded.** We know the layout well enough to implement it, but it would be a code
path no publisher ever exercises, no fixture ever validates, and every future change has to keep
correct — precisely the `codec_midpoint.rs` trap this codebase flags in three places. Every spec's rule is
that a decoder MUST reject a version it does not implement, so declining to implement a version that will
never be emitted is conformant, and it fails loudly if a publisher is ever misconfigured rather than
silently producing a plausible instrument.

Putting the gate in the walker rather than in each codec is the point: it is then impossible to forget,
which is exactly how two codecs came to lack one. `codec_mbp.rs`'s bespoke gate collapses into it, so a
version is validated in exactly one place.

The walker's per-message callback gains `schema_version`, since dispatch happens at the message decoder.
That signature change ripples to all four codecs' call sites and is the widest part of this work.

**The message decoder rejects an unsupported version itself**, rather than relying on the header gate
having already run. The reference states the reason plainly: nothing should depend on that call order,
and the decoder must be correct on its own.

### 2.3 Length is cross-checked as a minimum, not an equality

The version byte alone is not enough. Reading a v1 body under the v3 layout would consume adjacent fields
as symbol bytes and yield *a plausible instrument rather than an error* — a silent corruption no sequence
check would catch. So the decoder also cross-checks the declared body length against the version it was
told: 76 bytes for v1, 126 for v3 (the 80- and 130-byte messages less their 4-byte header).

**We require at least that length, where the reference requires exactly it.** This is a deliberate
divergence. Every spec promises forward compatibility inside a MAJOR line and classifies appending a
field within a message's declared `Message Length` as a MINOR change that must keep working, with
decoders required to ignore trailing bytes. An exact-equality check would reject a conformant `3.1.0`
frame whose definition grew, taking the whole feed dark until we shipped support for a change the spec
says needs none.

A minimum still catches the dangerous direction: a 76-byte v1 body claiming v3 fails `>= 126`, which is
precisely the plausible-instrument case. What it does not catch is a publisher declaring v1 while sending
a v3 body — non-conformant in a way the version byte is already lying about, and indistinguishable from a
publisher lying about anything else.

This makes `InstrumentDefinition` a deliberate exception to `codec_mbp.rs`'s exact-length discipline.
That discipline exists for a different hazard — `SnapshotBegin` is a prefix-superset of its
Market-by-Order counterpart, so a lenient read would invent a `depth_bound` whose `0` claims a *complete*
book — which has no analogue here.

### 2.4 `source_id` is carried as `Option<u16>`

v3's new field is decoded and kept on the struct. A v1 definition does not carry one, so the field is
`Option<u16>` rather than a `u16` with a sentinel: the absence is a real, permanent property of a v1
frame, not a missing value, and a consumer that must handle "this publisher predates per-instrument
source attribution" should be made to see that in the type.

Nothing consumes it yet. It is decoded now because the field is the entire reason for the release, and
because a consumer is being built against it in a parallel workstream — see §7.

### 2.5 Midpoint is excluded from the shared decoder

Midpoint's `InstrumentDefinition` is a different 64-byte variant with `manifest_seq` at `body+56`. It is
also a different *type*, not merely a different layout: its fourth field is `default_method: u8` where
the widened feeds carry `qty_exponent: i8`. So it neither shares the struct nor the decoder, and hoisting
it would be wrong at the type level rather than just inconvenient.

It keeps its own struct and decode, and gains only a supported set of `{1}` — which closes the same
conformance gap its siblings have without pretending its layout is shared.

## 3. Wire layout

`Source ID` is inserted after `Instrument ID`, so under v3 `Symbol` starts two bytes later than it would
have under v2, and every field after it shifts by 50 bytes relative to v1. Offsets are relative to the
message body, i.e. after the 4-byte message header.

| Field | v1 | v3 |
|---|---|---|
| `instrument_id` | `body+0` | `body+0` |
| `source_id` | *(absent)* | **`body+4`** |
| `symbol` | `body+4`, 16 bytes | `body+6`, **64 bytes** |
| `price_exponent` | `body+37` | `body+87` |
| `qty_exponent` | `body+38` | `body+88` |
| `manifest_seq` | `body+74` | `body+124` |
| body length | 76 | 126 |

Only these fields are extracted; the rest of the message is skipped by declared length as before.
`Message Length` is a `u8`, so the 130-byte v3 message is still representable.

## 4. Modules

- **`codec_common.rs`** — gains `InstrumentDefinition`, the version constants, the version-aware
  `instrument_definition(...)` decoder, and `supported_versions` on `decode_frame_with` plus
  `schema_version` on its callback.
- **`codec.rs` / `codec_mbo.rs` / `codec_mbp.rs`** — drop their struct and decode site in favour of the
  shared ones, and declare `{1, 3}`. `codec_mbp.rs` additionally drops its now-redundant bespoke gate.
- **`codec_midpoint.rs`** — declares `{1}`; decode otherwise untouched.
- **`metrics.rs`** — one counter, `dz_frames_by_schema_version{venue,kind,version}`.

## 5. Error handling and observability

An unsupported version fails the frame with a named error and joins the existing rate-limited
decode-warning path, so there is no new failure mode and no new panic surface — every read stays
bounds-checked and returns `Option`.

`dz_frames_by_schema_version{venue,kind,version}` is the one addition. Cardinality is 2 per feed, and it
is how an operator watches the rollout happen per publisher and knows when v1 support is safe to retire.
Without it, "has this feed moved to v3 yet" is unanswerable from the outside.

## 6. Validation

Three layers, in increasing strength:

1. **Offset-independent unit tests per version**, built from the spec's table rather than from the
   implementation's constants, so a transposed offset fails rather than agreeing with itself.
2. **Real-frame v3 decode** against the conformance tool's regenerated `conformant_tob.pcap` and
   `conformant_mbo.pcap`. This is what makes v3 byte-validated on arrival instead of draft-only.
3. **A cross-version equivalence test**: the same logical instrument, encoded as v1 and as v3, decodes to
   the same `InstrumentDefinition` apart from `source_id`, which is `None` for v1. That is the
   backward-compatibility claim stated directly, and it is the test that fails if a future edit fixes one
   path and not the other.

Every existing v1 fixture must keep passing untouched — the regression guard that v1 support is intact.
The frame-level gate needs its own cases: `0` rejected, **`2` rejected as unimplemented**, `4` rejected,
`1` and `3` accepted on the three widened feeds, and `3` rejected on midpoint.

## 7. Out of scope

- **Consuming `source_id`.** It is decoded and stored, not used. A parallel workstream is reworking
  source-id handling and removing `venue` from the WebSocket stack, and a per-*instrument* source id is a
  strictly better disambiguator than the per-frame one it would otherwise key on — `codec.rs`'s
  `source_name` deliberately leaves ID 3 unmapped precisely because it is shared by two venues and must
  fall back to the feed row's venue. Wiring that up belongs to that workstream, not this one.
- **Re-keying `InstrumentSnapshot`/`DepthSnapshot` from `(venue, symbol)` onto `instrument_id`.** v3
  materially reduces the pain — one live capture had 2,312 definitions overflow the 16-byte field, with
  two distinct instruments on one channel truncating to the same string and clobbering each other — but
  64 bytes narrows the collision window rather than closing it, and the real fix is a separate
  three-venue decision already tracked.
- **`PROTOCOL.md` and the WebSocket output.** `symbol` is a JSON string with no declared length, so
  longer values need no contract change and no version bump. Consumers see better symbols and nothing
  else.
- **Retiring v1.** Both supported versions stay indefinitely. The new metric is what would eventually
  justify dropping v1, and that is a later decision with its own evidence.
- **The other two v3 feeds.** `order-intent` and `perp-stats` also move to `3.0.0`, but the bridge
  decodes neither.

## 8. Open questions

1. **Do the regenerated conformance pcaps carry `InstrumentDefinition` messages at all?** They are
   generated against v3, so any definition in them is a v3 one — but a capture containing only quotes and
   trades would give layer 2 of §6 nothing to assert. Checked during implementation; if they are thin,
   the fallback is synthesized frames validated field-by-field against the reference decoder's constants,
   which is layer 1 with a stronger oracle rather than a new approach.
2. **Should the shared decoder return the fields it currently discards?** `Leg1`/`Leg2`, tick and lot
   size, asset class and expiry are all decoded by the reference and dropped by us. Widening the struct
   is trivial once the layout is version-aware and would serve a future consumer, but nothing needs them
   today, so this design does not add them.
3. **edge-feed-spec#29 is still open.** The layout above is taken from its head commit
   (`69cfbafa`). If review moves a field before merge, §3 is the only section that changes.
