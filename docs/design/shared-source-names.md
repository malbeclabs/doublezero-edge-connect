# Shared source names — design

**Date:** 2026-09-24
**Status:** draft

## Goal

Let a `sources` block give several Source IDs one name, as the upstream allocation does when one
venue runs several matching engines. Consumers see the shared name (`NAME:SYMBOL`); the bridge keeps
each engine's state separate.

## Problem

The loader refuses a repeated name (`DuplicateSource`) because internal state is keyed by the name,
not the ID. Two IDs under one name would share dedup floors, tape leaders, book authority and
snapshots, and a purge for one would clear the other. Four sites also resolve a row's name to one ID
with `source_id_of`, which takes the first match.

## Design

Two PRs.

**1. Key internal state by Source ID.** No behavior change while names are unique.

- Wire-derived keys (`MarketKey`, `ScopeKey`, `ChannelKey`, `BookKey`, `InstrumentSnapshot`,
  `DepthSnapshot`, and the arbiter's inline `(venue, symbol)` / `(venue, category)` keys) take a
  `SourceKey { id, name }` in place of the name. Equality includes the ID; the name keeps messages
  with no assigned ID (`source_id == 0`) apart, as today.
- Name-equality purges (`reset_depth_floor_for_symbol`, `remove_instrument` on an ID change)
  match on ID.
- The name stays the label: WebSocket `venue`/`source_name`, metric labels, subscription filters.
- Row-keyed state (`ReceiverKey`, `FeedKey`, `Universe`, arbitration mode) stays keyed by the row's
  configured name.

**2. Allow shared names.**

- `DuplicateSource` refuses a repeated ID only.
- `source_id_of` becomes `source_ids_of`, returning every ID for a name. `registry::feed_from`
  checks that at least one exists.
- The revealed set records IDs, not names; `emit_status` reports each revealed ID.
- `reconcile` channel purges and `/v1/status` product counts cover every ID of the row's name,
  scoped by the row's category and channel. That reaches the same rows as today's
  `(name, category, channel)` scope, which already includes a same-name row of another kind in
  that category. Shared IDs widen it by no row.
- Name-only lookups filter on a fixed Source ID: the public backstops'
  `instrument_known` / `resolve_instrument`, and the order-book sink's venue filter.
- Product ids: `resolve` already matches by name. The ambiguity count moves from
  `(source_id, symbol)` to `(name, symbol)`, so a symbol listed under two IDs of one name renders
  with the `#channel.instrument` suffix and resolves back.
- An instrument that moves off an ID drops that ID's depth and book replay once no path still
  serves it there. The arbiter tracks the paths, since it sees every receiver.
- Docs: `self-hosting.md` drops "a name may appear only once".

## Out of scope

- Adding assignments to the built-in `sources` block. Names arrive with the hosted document.
- A per-row `source_ids` field. Rows keep naming a venue; the wire says which IDs it carries.

## Testing

- PR 1: the existing suite unchanged, plus a two-ID test per key family asserting the IDs do not
  share a floor, leader, book or snapshot.
- PR 2: a document with two IDs under one name loads; both engines' instruments render as
  `NAME:SYMBOL`, each resolves back to its own ID, a colliding symbol renders suffixed; status is
  emitted once per revealed ID.

## Overlap

- #110 re-keys the MBO processor's own book map (not `BookKey`); it overlaps in `processor.rs`, so
  whichever lands second rebases.
- #161 publishes `src/ingest/registry.json`; unaffected.
