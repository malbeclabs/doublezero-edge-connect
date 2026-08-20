# Order-level `book` test inventory

The Market-by-Order cross-publisher **resurrection guard** in `src/ingest/arbiter.rs` decides whether a removed order may be forgotten on **venue time**: per channel, the newest `source_ts_ns` accepted defines a frontier at `newest - retention`, an event older than the frontier is refused, a removed entry older than it is forgotten, an event older than its own order's last published change is refused before any size comparison, and any non-zero size for an order already published as gone is refused. This file says which of the tests around it pin the *contract* — the ones a change to this guard must leave alone — and which pin the *mechanism*, which is deleted with the code it describes.

The rule the guard is held to: **a change to it touches none of the names under [Behaviour](#behaviour).** That check needs a list, which is what this is.

## Criterion

A test is **behaviour** if its assertions are things a WebSocket consumer can observe: what reaches the broadcast, whether a message is published at all, and whether a book rebuilt the way PROTOCOL.md tells a consumer to rebuild it matches the venue's. A test that reads a private field of the arbiter, a guard counter or a metric to decide whether it passed is **mechanism**, whatever its name says — it cannot survive the removal of the thing it reads.

Everything about quotes, depth, trades, midpoints, market-by-price levels, codecs, the registry, the reconciler, shreds and the CLI is out of scope and not listed. So is the **single-path authority gate** (`src/ingest/authority.rs` and the `book_publishes_one_path_*` / path-transfer / path-eligibility tests in `arbiter.rs`) — a neighbouring gate that decides which path serves a *price-aggregated* market, not the resurrection guard. It is out of scope for the classification, not for a diff: it shares `MarketKey` and the accumulator/replay/`last_admitted` eviction pairing, so anything that re-keys per-market state lands on it. If your diff touches those, they need their own read.

## Behaviour

### `tests/order_level_consumer_book.rs`

The whole file qualifies: every test drives batches through the real `Arbiter`, reads the broadcast, and compares a naively-rebuilt book to the venue's.

The consumer-equality net:

- `a_naive_consumers_book_matches_the_venue_across_gaps_and_races`
- `a_drifted_publisher_cannot_walk_a_consumers_order_backwards`
- `a_discharged_rebaseline_does_not_let_the_raising_path_repeat_its_claim`
- `a_book_larger_than_the_guard_does_not_resurrect_a_removed_order`
- `a_rebaseline_larger_than_the_guard_does_not_resurrect_a_removed_order`
- `a_batch_that_both_removes_and_disagrees_does_not_strand_the_removal`
- `one_markets_feed_leaves_another_on_the_same_channel_alone`
- `a_single_paths_feed_reaches_the_consumer_exactly`
- `two_paths_in_lockstep_publish_each_event_once`
- `an_path_behind_in_arrival_only_does_not_drift_the_consumer`
- `paths_synced_from_different_snapshot_anchors_keep_the_consumer_exact`
- `an_path_that_departs_and_returns_keeps_the_consumer_exact`
- `a_permanently_slower_path_never_stops_the_market_being_served`
- `a_peers_rebaseline_does_not_displace_a_served_book`
- `the_consumer_book_matches_the_venue_far_past_the_old_lag_ceiling`

The two clocks, and the frontier's own scenarios:

- `a_published_batch_carries_the_stamps_it_was_given` — the harness's venue and arrival stamps reach the wire as they were set, without which the whole split is unfalsifiable.
- `a_venue_time_skew_alone_does_not_drift_the_consumer`
- `a_venue_time_skew_past_the_dedup_window_refuses_the_stale_copy`
- `an_path_that_never_held_the_removed_orders_does_not_darken_the_market`
- `a_returning_links_contiguous_backlog_reaches_no_consumer`
- `a_whole_channel_gap_past_the_forward_bound_resumes`
- `a_cold_start_market_stamped_zero_still_bootstraps`
- `a_survivor_behind_the_dead_leaders_frontier_still_serves_the_market`
- `a_session_whose_clock_restarts_lower_keeps_publishing`

⚠️ Three caveats on this list.

The **scenario sizes** of several of them are derived from private constants mirrored at the top of the file: `GUARD_CAP` (the arbiter's `MAX_SEEN_ORDER_EVENTS`), `MARKET_TOMBSTONES` (the per-market cap this design deleted, kept because a scenario showing that crossing it costs the market nothing has to name the figure it crosses) and `RETENTION_NS` (the default `--arb-book-retention-secs`). A change to what bounds the guard has to re-derive those numbers even where the assertion itself stands. Re-deriving them is not a contract change; weakening what a scenario asserts is.

`a_venue_time_skew_alone_does_not_drift_the_consumer` is **still unfalsifiable**, and its own doc comment says so: the trailing path's copies collapse as duplicates before any rule reads a stamp, so swapping the venue stamp for the arrival stamp leaves it green. `a_venue_time_skew_past_the_dedup_window_refuses_the_stale_copy` is the scenario that actually measures venue time — verified by mutation, since swapping the two stamps and deleting the stale-copy rule each kill it while leaving the older test green. Keep the older one for the contrast it draws; do not read it as coverage.

`the_consumer_book_matches_the_venue_far_past_the_old_lag_ceiling` asserts through `arrival_lagged_feed`, which compares after **every** arrival. It replaces a sweep that compared only at the end of the run, where a trailer replaying the venue's whole life in order has converged on its own — that form passed with the racing guard removed outright and was evidence of nothing. Assert per arrival; do not add another terminal comparison.

### `src/ingest/arbiter.rs`

Collapse, refusal and re-baseline suppression, all decided on the broadcast:

- `order_events_collapse_across_publishers_keeping_first_arrival`
- `successive_partial_fills_of_one_order_all_reach_the_wire`
- `a_partly_duplicate_batch_publishes_only_its_new_events`
- `a_late_copy_cannot_resurrect_a_deleted_order` (also asserts `dz_book_resurrections_dropped_total`)
- `a_repeated_removal_is_not_treated_as_a_resurrection`
- `a_copy_past_the_window_re_emits_rather_than_corrupting`
- `a_rebaseline_keeps_the_resurrection_guard`
- `a_rebaseline_seeds_an_order_it_had_tombstoned`
- `a_rebaseline_is_suppressed_while_a_peer_is_serving`
- `a_claim_from_a_publisher_that_never_serves_does_not_suppress`
- `a_departed_publisher_does_not_suppress_a_peers_rebaseline`
- `an_interleaved_race_is_not_a_disagreement`
- `a_single_publisher_streams_unimpeded`
- `simultaneous_recoveries_produce_exactly_one_rebaseline`
- `an_evicted_market_still_routes_as_order_level` — decides purely on the broadcast (one message, not two), and it is the only check that an evicted market re-derives its routing from batch content. Reverting it to the single-path authority emits `clear_only` plus the batch, which tells every consumer to drop a live book.

The forced re-baseline a size disagreement raises, and what the republished view may claim:

- `a_size_disagreement_forces_a_rebaseline_rather_than_a_guess` (also asserts `dz_mbo_path_disagreement_total` and the forced-re-baseline counter)
- `a_forced_rebaseline_republishes_the_wire_not_an_paths_own_book`
- `the_path_that_discharges_a_rebaseline_does_not_own_the_floor_it_seeds`
- `a_rebaseline_seeds_the_guard_with_its_own_orders` (the drop is the assertion; the counter confirms *why* it was dropped)

### Elsewhere

Downstream of the merge point, so a change to the guard should not reach them — but they are the ones that notice if it changes the `book` product's shape rather than its arbitration.

- `tests/dedup.rs` — `interleaved_book_paths_publish_one_coherent_feed`
- `src/ingest/book.rs` (L3 reconstruction, single publisher) — `deltas_report_the_order_they_touched`, `a_rejected_delta_reports_nothing`, `a_removed_order_is_never_resurrected`, `a_fully_executed_order_is_never_resurrected`, `a_session_reset_reopens_the_id_space`, `order_set_is_complete_and_deterministically_ordered`, `order_set_tolerates_extreme_prices`, `end_of_session_drops_book_sequences_and_event_clock`, `instrument_reset_drops_the_event_clock`, `snapshot_then_contiguous_deltas_update_top_of_book`, `execute_reduces_then_removes_order`, `duplicate_and_old_deltas_are_ignored`, `gap_triggers_recovery_then_snapshot_replays_buffered_deltas`, `instrument_reset_drops_book_until_resnapshot`, `periodic_snapshot_while_ready_is_ignored`, `stale_snapshot_ahead_rebootstraps`, `incomplete_snapshot_is_discarded`, `instrument_reset_keeps_post_anchor_buffered_deltas`, `pending_buffer_is_bounded_while_recovering`, `resting_orders_are_bounded_under_add_flood`
- `src/model.rs` (wire shape + `BookAccumulator` replay) — `book_change_order_id_is_additive_and_round_trips`, `book_serializes_to_the_documented_shape`, `a_rebaseline_leads_with_a_clear_action`, `a_lone_clear_is_a_complete_message`, `book_round_trips`, `only_book_reports_a_channel`, `book_reports_its_venue_and_symbol_for_filtering`, `the_accumulator_materializes_the_applied_state`, `a_batch_awaiting_its_last_is_not_materialized`, `non_finite_prices_and_sizes_are_dropped`, `a_zero_source_ts_does_not_blank_the_replayed_event_time`, `a_materialized_book_carries_the_accumulated_source_id`, `the_accumulator_folds_orders_into_levels_with_counts`, `removing_the_last_order_at_a_price_removes_the_level`, `a_one_sided_clear_spares_the_other_sides_orders`, `a_rebaseline_discards_the_previous_order_population`, `the_accumulated_order_population_is_bounded`, `a_terminated_batch_larger_than_the_pending_cap_still_baselines`, `an_unterminated_event_past_the_cap_is_abandoned`, `replay_scope_folds_to_levels_or_materializes_orders`
- `src/ingest/processor.rs` — `mbo_emits_the_order_level_book_alongside_depth`, `mbo_still_emits_depth_alongside_the_book`, `a_cancelled_order_is_published_as_a_delete`, `a_snapshot_install_emits_clear_then_every_resting_order`, `mbo_undefined_instrument_creates_no_book`, `mbo_books_map_is_bounded_under_instrument_flood`
- `src/sinks/ws.rs` (replay/bootstrap) — `connect_replays_the_accumulated_book_rebaseline`, `the_book_replay_scope_always_follows_the_market`, `subscribe_scopes_the_book_replay_by_channel`, `instrument_is_replayed_before_the_book`, `markets_accumulated_partway_are_not_replayed`, `a_lagging_client_is_rebaselined`, `prepare_populates_the_channel_for_book_and_instrument`
- `src/sinks/hyperliquid.rs` — `l4book_subscribe_sends_the_whole_book_with_order_ids`, `l4book_orders_carry_no_fabricated_timestamp`, `l4book_forwards_order_diffs_after_the_snapshot`, `l4book_renders_a_gone_order_as_remove`, `l4book_ignores_other_venues_and_coins`, `l4book_never_emits_a_zero_order_id`, `a_market_that_never_baselined_is_not_published`, `a_price_aggregated_market_is_not_published`, `an_emptied_order_book_keeps_publishing_on_both_channels`, `one_book_message_serves_every_subscription_of_the_market`, `a_large_book_truncates_for_l2_and_renders_whole_for_l4`, `a_rebaseline_becomes_an_l4book_snapshot`, `l2book_renders_the_shape_nautilus_parses`, `l2book_truncates_to_n_levels_best_first`, `golden_l4book_frames_match_the_committed_fixtures`, `golden_l2book_frame_matches_the_committed_fixture`
- `tests/hyperliquid_sink_shapes.rs` — `a_golden_l4book_snapshot_parses_as_the_publisher_emits_it`, `a_golden_l4book_updates_frame_parses_as_the_publisher_emits_it`, `a_golden_l2book_frame_parses_as_nautilus_parses_it`
- `src/sinks/api.rs` — `book_reports_coverage_and_respects_baselined`, `book_and_ticker_do_not_cross_universes_sharing_a_channel_and_instrument_id`

## Mechanism

All in `src/ingest/arbiter.rs`, all reading guard internals, a guard metric or a private field. None of them is evidence the product still works, and a change that removes what they read deletes them rather than rewriting them — a rewritten mechanism test asserts whatever the replacement turns out to be.

- The venue-time frontier — `the_removed_population_tracks_the_window_not_the_anchor_difference`, `a_repeated_removal_does_not_hold_its_entry_above_the_frontier`, `the_removed_population_tracks_the_window_not_the_lag`, `the_work_one_batch_does_is_bounded_in_every_state`, `a_bounded_far_future_removal_does_not_pin_the_forgetting_queue`, `a_crawling_clock_does_not_starve_the_reseat`. These are in-crate rather than in the consumer net because the frontier's whole point is what it *forgets*, which nothing on the wire shows: they read the removed population (`book_events[..].n_dead`, an entry's `last_ts`) and the per-batch work counter (`examined`). The last one is the only assertion about the scan that maintains the population rather than the population itself, and it matters because that scan runs under the one mutex every receiver on every feed takes to emit. Their scenario sizes mirror the implementation's per-batch work bound and the default retention window; a wrong mirror makes them weaker, never wrong.
- Guard accounting and eviction — `guarded_tombstones_are_counted_exactly` (the process-wide running total against a recount from the maps), `evicting_a_live_floor_does_not_force_a_rebaseline`, `a_book_larger_than_the_guard_stays_proportional_to_its_input` (its proportionality half is consumer-visible; its `book_markets[..].rebaseline` half is not)
- Counters and rate limits — `a_content_disagreement_is_counted`, `the_rebaseline_rate_limit_does_not_follow_the_dedup_window`

**Four of these are bounds, not mechanism. Re-point them; do not delete them.** The wire is unauthenticated, so `order_id`, `channel_id` and `instrument_id` are all attacker-supplied, and these are the only assertions that the state keyed on them cannot grow without limit:

- `the_racing_window_is_bounded_by_event_count` — the sole check that `seen`/`resting` stay within `MAX_SEEN_ORDER_EVENTS`, the live half's bound against an `order_id` flood. The removed half is no longer bounded by a count at all: the retention window sizes it and the next entry backstops it.
- `the_process_wide_ceiling_forgets_rather_than_growing` — `MAX_TOMBSTONES_TOTAL` costs entries rather than memory. It is charged to the market being admitted and so is advisory; it asserts the shed happened, not that the total came under.
- `the_channel_clocks_are_bounded` — the per-channel frontier map, keyed on the wire's `channel_id`. Losing a clock degrades that channel to "frontier unset", which its next batch re-seeds.
- `a_far_future_batch_on_an_unset_clock_does_not_freeze_the_orders_it_touches` and `a_host_clock_behind_the_venue_still_re_seats_the_frontier` — the two states in which the frontier has no venue reference of its own and must fall back to the host clock: a batch the anchor refused while the clock is unset, and a host clock running behind the venue by more than the skew.
- `a_stream_of_in_bound_stamps_cannot_ratchet_the_frontier_past_the_host_clock` (in `tests/order_level_consumer_book.rs`, consumer-visible) — the per-step forward bound caps one advance, not a stream of them; without the host-clock anchor a spoofed source strands the whole channel and empties the guard.
- `a_batch_past_the_frontier_mints_no_market_state` — the one that keeps the two above honest. `book_events` is evicted alongside `book_markets`, which is only tracked once something is published, so a refused batch must create nothing: a flood of forged `(channel, instrument_id)` keys carrying ancient stamps is one datagram per key, none of them ever published.
- `eviction_drops_the_racing_state_too` — the sole check that `book_sync` and `book_events` are evicted with `MAX_BOOK_MARKETS`. `book_markets_are_bounded` covers `book_markets`, `book_order`, the replay map and the authority map, and neither of those two. `book_sync` is also what the re-baseline suppression reads, which is behaviour.

Whatever bounds a later change puts on the state that replaces these needs an equivalent test landing in the same change.

- Neighbouring, and only listed so nobody mistakes them for the guard: `src/ingest/book.rs::the_removed_set_is_bounded` (the per-book removed-id set, defence in depth behind the merge point) and `src/ingest/processor.rs::a_gapped_book_reports_itself_unsynced` (the sync report the re-baseline suppression consumes — if that stops being consumed, this one goes with it).

## What the venue-time frontier dropped

The consensus mechanism produced one consumer-visible property that no longer exists, and its tests went with it: a market the paths could not settle was **disowned** — it went dark, was told so exactly once as a bare `Clear`, and nothing but a producer's own re-baseline ended the outage (`an_unanswerable_guard_does_not_republish_our_own_view`, `evicting_an_unpassed_tombstone_invalidates_the_market`, `a_market_disowned_by_the_ceiling_is_announced_without_waiting_for_a_batch`, `evicting_a_disowned_market_does_not_let_it_resume_serving_deltas`, `a_bare_clear_ends_a_disowned_markets_outage_even_after_eviction`). Deliberate: one publisher synced from an older snapshot anchor can never report removals for orders it never held, so under consensus the ordinary case walked a market to a blackout every consumer paid for. `dz_mbo_market_invalidations_total` is gone with it, and `an_path_that_never_held_the_removed_orders_does_not_darken_the_market` now asserts the opposite property.

Two more went for the same reason — refusal held open by a *count* of removals rather than by how far behind the event is (`an_unreported_removal_keeps_refusing_the_resurrection_it_is_held_for`, `a_rebaseline_larger_than_the_cap_keeps_the_resurrection_guard`) — and one because it pinned re-seating the high-water gauge on *retirement*, which no longer happens (`the_high_water_re_seats_when_the_market_holding_it_retires_rather_than_reading_zero`; the gauge and its re-seat survive against the frontier's forgetting).

Nothing else was dropped: the forced re-baseline and the guard's other consumer-visible properties are listed under [Behaviour](#behaviour) above.

## What the net cannot reach

Two waits are measured on a monotonic clock inside the arbiter, with no seam an integration test can drive.

`PEER_SERVING_NS` silence — a path going quiet for 30 s — is why `an_path_that_departs_and_returns_keeps_the_consumer_exact` uses `Arbiter::forget_publisher_books`, the signal a receiver's registration sends as it exits and the authoritative one, with the timer as its backstop. Same consumer-visible property; the timer itself is not covered.

The frontier's **re-seat wait** has the same limitation, and the scenarios that need it (`a_whole_channel_gap_past_the_forward_bound_resumes`, `a_survivor_behind_the_dead_leaders_frontier_still_serves_the_market`, and in-crate `a_crawling_clock_does_not_starve_the_reseat`) reach it by configuring a `BookGuardConfig` with a short `reseat_after_ns` and sleeping real time against it. ⚠️ The emit-altitude crawling-clock one can **false-pass on a slow host**: its pacing keeps the movement-keyed wait fresh only while a round costs less than that wait, and past that the movement trigger fires by itself and it passes with the mechanism deleted. It is the population check; `a_crawling_advance_keeps_only_the_movement_wait_fresh` drives `ChannelClock` directly and is what isolates the trigger. That is a real sleep in the test suite, and the only alternative is injecting a clock into the emit path; it buys coverage of the one hatch that keeps a survivor behind a dead leader's frontier from being dark for the life of the process.
