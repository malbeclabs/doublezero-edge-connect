# Dedupe quotes on the Venue Update ID — design

**Date:** 2026-10-21
**Status:** draft
**Depends on:** malbeclabs/edge-feed-spec#97 (top-of-book `3.2.0`)

## Goal

Let two or more paths publish one top-of-book feed whose quotes carry no venue timestamp, and still
hand consumers each book state once, in venue order.

## The wire change

edge-feed-spec#97 turns the reserved 4 bytes at `Quote` offset 56 into **Venue Update ID**: the low
32 bits of a venue-assigned per-instrument book update counter, `0` = unavailable. The counter is
the same on every connection and every publisher, because the venue assigns it. `Quote` stays 60
bytes and `Schema Version` stays `3`. IDs compare with serial-number arithmetic:

```rust
fn newer(a: u32, b: u32) -> bool { (a.wrapping_sub(b) as i32) > 0 }
```

Line references below are to `main` at `3867231`.

## Problem

Quote dedupe has one coordinate today, `source_ts`. The arbiter keeps a latch-to-leader
`StalenessFloor` per `(SourceKey, symbol)` (`src/ingest/arbiter.rs:1390`, type at `:474`). A quote
with `source_ts == 0` skips the floor and is forwarded as is (`arbiter.rs:3253-3259`, counted by
`dz_quotes_no_source_ts_total`, `src/metrics.rs:685`).

So a feed whose quotes carry no timestamp gets **every path's copy**. Two hot publishers double the
quote rate and interleave the copies, and a slower path's older quote can land last and read as
current.

Arbitration mode does not help. The `Quote` branch (`arbiter.rs:3249-3312`) never calls `mode_for`;
`Sticky` gates only the trade tape (`arbiter.rs:3324`, `tape_path_admits` at `:3076`) and the `book`
product.

The counter also helps feeds whose timestamps are coarse. Many distinct quotes share one tick today,
and the floor has to latch a leader to order them (`arbiter.rs:13-25`). The counter has no ties.

## Decode

`decode_body` reads `Quote` at fixed offsets and stops at `ask_n`, message offset 54
(`src/ingest/codec.rs:122-133`). Add one read: `u32le(b, body + 52)`, message offset 56..60.

**Read it only when the message's declared length covers those bytes** (`b[o + 1] >= 60`).
*Changed from the earlier draft, which said the bounds-checked readers were enough.* They are not:
the walker hands `decode_body` the whole datagram, not the message (`src/ingest/codec_common.rs:246`),
so in a datagram packing several messages a quote that under-declares its length would read its ID
out of the next message's header. Below 60 the ID decodes as `0`.

**Existing traffic is unchanged.** The shared encoder writes zeros there
(`edge-multicast-ref` `rust/codec/dz-edge-tob/src/quote.rs`, `dst[56..60].fill(0)`). Every publisher
built on it decodes to ID `0` and stays on today's floor. Before `3.2.0` the spec said only "Padding
to 60 bytes", with no MUST-zero, so a publisher with its own encoder that writes nonzero padding would
enter the ID regime by accident. #97 adds the MUST; such publishers need checking before this ships.

Carry it as `codec::Quote::venue_update_id` and `NormalizedQuote::venue_update_id`.
`NormalizedQuote` is serialized to consumers (`src/model.rs:183-184`), so the field is
`#[serde(skip)]` until `PROTOCOL.md` decides to expose it. It is a producer-side dedupe key, like the
`category` kept off the wire on `NormalizedTrade` (`model.rs:259`). The public WebSocket input sets
it to `0` (`src/ingest/ws_input.rs:241`).

## The rule

Per key `(SourceKey, symbol)`, the same key as today's floor. A key is in the **ID regime** while it
holds ID state.

| Quote carries | Key state | Decision |
|---|---|---|
| ID `!= 0` | no ID state | Admit. Store `last = id`. The key enters the ID regime. |
| ID `!= 0` | ID regime | Admit iff `newer(id, last)` and within the jump cap. Else drop. The path does not matter. |
| ID `0` | ID regime | Drop, counted as `regime`, whatever its `source_ts`. |
| ID `0`, `source_ts > 0` | no ID state | Today's floor, unchanged. |
| ID `0`, `source_ts 0` | no ID state | Today's bypass, unchanged. |

*Changed from the earlier draft*, whose table sent an ID-less `source_ts 0` quote to the bypass in
every state. The feed this exists for stamps `source_ts 0` on every quote, so under that table a
rollout's old path would still double every quote, contradicting the draft's own rule that a key in
the ID regime drops ID-less copies.

No leader latching. The ID orders the quotes, so arrival order across paths does not matter. The first
copy of each ID wins and the output is serially increasing per key.

A nonzero `source_ts` on an ID-bearing quote still passes the existing future-skew check first
(`arbiter.rs:3273`, `MAX_FUTURE_SKEW_NS` at `:90`). It does not order anything.

When no ID quote is in flight the arbiter does no extra lookup: an ID-`0` quote checks whether any key
holds ID state at all, and with none takes today's code path unchanged.

### The zero collision

The counter's low 32 bits are `0` once every `2^32` updates. That quote reads as unavailable and, on
an ID-regime key, falls to the `regime` drop. One lost quote per `2^32` updates per instrument is
accepted: the next quote corrects it, since a quote is full state.

### Mixed fleets and rollout

During a rollout one publisher of a key fills the ID and another does not. The two cannot be ordered
against each other, so a key in the ID regime **drops ID-less copies**. The upgraded path carries the
key; the old path is a backstop on silence only.

**Regime timeout.** If no ID quote is admitted for a key for `QUOTE_ID_REGIME_TIMEOUT` (5 s, the same
order as `MAX_FUTURE_SKEW_NS`), the key drops its ID state and the next quote is judged as if it had
none. A silent upgraded path then cannot black out a key that a working old path still serves. The
same timeout reopens a quiet key fresh, which also covers a counter that moved more than `2^31` while
the key was idle.

The check runs when the next quote for the key arrives, against the host's monotonic clock
(`model::now_mono_ns`), sampled in the arbiter. Not `recv_ts_ns`: that is wall time
(`model::now_ns`), so an NTP step would stretch or cut the timeout, and a 60 s step back would hold an
ID-less backstop muted for 65 s. The future-skew check keeps wall time, because it compares against
`source_ts`. Nothing sweeps.

One cost of reopening fresh: a copy that arrives more than 5 s late, on a key that was quiet that
long, is admitted even if its ID is older. Paths that far apart are not mirrors in any useful sense,
and the next quote corrects it.

The public WebSocket input carries ID `0`. On a key in the ID regime it fills in only after the
timeout; on every other key its behavior is unchanged.

### Forged and jumping IDs

The multicast wire is unauthenticated and the publisher identity is a spoofable source IP address
(`src/ingest/authority.rs:60`). There is no wall clock to bound an ID against, as there is for
`source_ts`.

Serial comparison caps a forward jump at `2^31 - 1`, which is not protection: a forged ID `2^31 - 1`
ahead is admitted and every real quote after it compares as older.

1. **Jump cap.** Admit only if `id.wrapping_sub(last) <= cap`. A larger forward jump is dropped,
   counted as `jump`, and kept as a candidate `(id, Transport)`.
2. **Escape by corroboration, forward only.** A candidate is accepted, and the key reset to the
   corroborating ID, when a **different** `Transport` delivers an ID within the cap of it, in either
   direction. Two honest paths therefore heal a real forward jump at the next pair of real copies, and
   so a forged first ID that sits more than the cap *behind* the real counter.
3. **Escape by timeout.** Everything else waits for the regime timeout: one path alone delivering IDs
   past the cap, and every ID that compares as **older** than `last`.

Older IDs never become candidates. A forged value ahead of the real counter, inside the cap or not,
makes real quotes compare as older, so nothing is admitted and the key heals on the regime timeout, or
sooner when the real counter passes the forged value. Corroborating older copies instead would be
unsafe: with three paths, two delayed mirrors delivering the same older ID would rewind a healthy key. A forger
that keeps sending can hold a key; that is the same residual the tape gate and the `book` authority
already document for an unauthenticated wire.

The cap trades false rejections against nothing much: the timeout bounds the wedge, and the cap only
decides whether one bad datagram costs a 5 s gap. So it is sized to never refuse an honest advance.
What to measure, per feed and instrument: the largest advance of the counter between two consecutive
quotes one path delivers, within the regime timeout. Book change rate times the timeout bounds it from
above. The default is `1_000_000`, which allows 200,000 book changes per second on one instrument for
the whole timeout. It is a flag, `--arb-quote-id-max-jump`, beside `--arb-book-ts-jump-secs`, which
answers the same question for book stamps (`src/main.rs:377`).

### Restart, era and venue resets

A publisher restart or a new `Reset Count` does not touch the ID: it is the venue's counter, not the
publisher's. Like today's quote floor (`arbiter.rs:1384-1389`), the ID state is **exempt** from the
session-reset path. No `EndOfSession`, `ChannelReset` or Source ID purge clears it.

A venue that resets its counter produces IDs that compare as older, or as a jump past the cap. Nothing
is admitted. A jump past the cap heals when a second path corroborates it. A counter that restarts
lower compares as older on every path, so it heals only on the regime timeout: one 5 s gap per
restart, accepted for the reason older IDs never corroborate. The clear goes through one reset helper shaped like
`reset_depth_floor_for_symbol` and `record_floor_resets` (`arbiter.rs:2103-2126`), counted by reason.
A clear after refused IDs counts as `venue_reset`; a clear that hands the key back to an ID-less quote
counts as `regime_timeout`. A key that was only quiet re-enters without a count, so the metric stays
an alert signal rather than a measure of idle instruments.

## State and bounding

Each key adds a `u32` last ID, the admitted quote's `QuoteId` without its counts, the last-admit instant, a refused-since-admit flag, and an optional
candidate (`u32` and its `Transport`). The candidate needs no age: an admit clears it, and with
nothing admitted the whole key expires on the regime timeout first. No new keys: the key space is today's quote floor, a
plain `HashMap` per `(SourceKey, symbol)` with no key cap (`arbiter.rs:474-480`). If that cap is ever
added, the ID state goes with the entry.

## Metrics

Following `dz_quotes_no_source_ts_total` and `dz_depth_floor_resets_total`:

- `dz_quotes_update_id_admitted_total{venue}`: quotes admitted in the ID regime.
- `dz_quotes_update_id_dropped_total{venue, reason}`, `reason` one of `duplicate` (the same ID again,
  the steady state of a second path), `conflict` (the same ID with other prices or sizes; the counts are left out, since a path may report `0` for unavailable), `stale`
  (an older ID), `regime` (an ID-less copy on an ID key), `jump`. *`duplicate` is split from `stale`,
  which the earlier draft folded together*: a second hot path makes duplicates half the traffic, and a
  real reordering would vanish into that. `conflict` is also dropped: one ID is one book state, so two
  contents mean a path is not carrying a venue counter, for example an encoder from before `3.2.0`
  that writes nonzero padding. edge-feed-spec#97 says a subscriber SHOULD then stop using the field
  for that Source ID. That is not built: every known encoder writes `0`, and the metric shows the case
  if it occurs.
- `dz_quote_floor_resets_total{venue, reason}`, `reason` one of `regime_timeout`,
  `corroborated_jump`, `venue_reset`.

The existing totals stay totals: an ID-regime admit also counts in `dz_quotes_admitted_total` and
`dz_emit_total`, and an ID-regime drop in `dz_quotes_dropped_total`. `dz_quote_ticks_won_total` is
per `source_ts` tick and does not count ID-regime quotes.

`regime` above zero for long outside a rollout means a publisher of that key is not filling the ID.

## Tests

- Two `Transport::Edge` paths, same IDs, interleaved arrival: each ID once.
- A later path delivers an older ID after a newer one: dropped as `stale`.
- IDs across the `u32` wrap (`0xFFFF_FFFE`, `0xFFFF_FFFF`, `1`) admit in order.
- Mixed fleet: ID-less copies dropped, `source_ts 0` included; after the timeout the floor serves them.
- A forged ID `2^31 - 1` ahead from one path is dropped as `jump`, and the key keeps admitting real
  quotes.
- A jump corroborated by a different `Transport` resets the key; the same `Transport` does not.
- The same ID with different content drops as `conflict`.
- The timeout runs on the monotonic clock: a quote's wall-clock `recv_ts_ns` does not move it.
- A venue counter reset: refused until the timeout, then a `venue_reset` and the new IDs admitted.
- The zero collision on an ID key drops as `regime`.
- A quote with zero padding, from today's encoder, decodes ID `0`; a quote that declares fewer than 60
  bytes decodes ID `0` and does not read the next message.
- Existing traffic: the recorded two-publisher fixtures decode to ID `0` throughout, and the floor and
  the `source_ts 0` bypass behave as before.
- `venue_update_id` never reaches the WebSocket JSON.
