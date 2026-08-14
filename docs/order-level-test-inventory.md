# Order-level `book` test inventory

The Market-by-Order cross-publisher **resurrection guard** — tombstones, per-arm reporter masks, retirement quorums, sweeps, caps and the market-disowning path in `src/ingest/arbiter.rs` — is being replaced. This file says which of today's tests pin the *contract* (they must survive that replacement unchanged) and which pin the *mechanism* (a later phase deletes them with the code they describe).

The rule the replacement is held to: **the implementation diff touches none of the names under [Behaviour](#behaviour).** That check needs a list, which is what this is. The [Both](#both) section is the part that breaks the rule if it is not read first.

## Criterion

A test is **behaviour** if its assertions are things a WebSocket consumer can observe: what reaches the broadcast, whether a message is published at all, and whether a book rebuilt the way PROTOCOL.md tells a consumer to rebuild it matches the venue's. A test that reads a private field of the arbiter, a guard counter or a metric to decide whether it passed is **mechanism**, whatever its name says — it cannot survive the removal of the thing it reads.

Everything about quotes, depth, trades, midpoints, market-by-price levels, codecs, the registry, the reconciler, shreds and the CLI is out of scope and not listed. So is the **single-arm authority gate** (`src/ingest/authority.rs` and the `book_publishes_one_arm_*` / arm-transfer / arm-eligibility tests in `arbiter.rs`) — a neighbouring gate that decides which arm serves a *price-aggregated* market, not the resurrection guard. It is out of scope for the classification, not for the diff: it shares `MarketKey` and the accumulator/replay/`last_admitted` eviction pairing, so a replacement that re-keys per-market state lands on it. If your diff touches those, they need their own read.

## Behaviour

These must pass unchanged.

### `tests/order_level_consumer_book.rs`

The whole file qualifies: every test drives batches through the real `Arbiter`, reads the broadcast, and compares a naively-rebuilt book to the venue's.

Written for this net, before the replacement's shape was known:

- `a_single_arms_stream_reaches_the_consumer_exactly`
- `two_arms_in_lockstep_publish_each_event_once`
- `an_arm_behind_in_arrival_only_does_not_drift_the_consumer`
- `arms_synced_from_different_snapshot_anchors_keep_the_consumer_exact`
- `an_arm_that_departs_and_returns_keeps_the_consumer_exact`
- `a_permanently_slower_arm_never_stops_the_market_being_served`
- `a_peers_rebaseline_does_not_displace_a_served_book`
- `one_markets_stream_leaves_another_on_the_same_channel_alone`

Already there:

- `a_naive_consumers_book_matches_the_venue_across_gaps_and_races`
- `a_drifted_publisher_cannot_walk_a_consumers_order_backwards`
- `a_discharged_rebaseline_does_not_let_the_raising_arm_repeat_its_claim`
- `a_book_larger_than_the_guard_does_not_resurrect_a_removed_order`
- `a_rebaseline_larger_than_the_guard_does_not_resurrect_a_removed_order`
- `a_lagging_arm_past_the_old_per_market_cap_costs_the_market_nothing`
- `the_consumer_book_matches_the_venue_up_to_a_one_second_inter_arm_lag`
- `a_batch_that_both_removes_and_disagrees_does_not_strand_the_removal`

⚠️ Two caveats on this list.

The **scenario sizes** of five of them are derived from private constants mirrored at the top of the file (`GUARD_CAP`, `GUARDED_ORDERS`, `MARKET_TOMBSTONES`). A replacement that changes what bounds the guard has to re-derive those numbers even where the assertion itself stands, and `a_book_larger_than_the_guard_does_not_resurrect_a_removed_order`, `a_rebaseline_larger_than_the_guard_does_not_resurrect_a_removed_order` and `a_lagging_arm_past_the_old_per_market_cap_costs_the_market_nothing` are named for a cap that may no longer exist. Renaming those three is not a contract change; weakening what they assert is.

`a_lagging_arm_past_the_old_per_market_cap_costs_the_market_nothing` and `the_consumer_book_matches_the_venue_up_to_a_one_second_inter_arm_lag` compare the consumer's book to the venue's **only at the end of the run**, and their trailing arm replays the venue's whole life in order — so a stale copy it publishes is corrected by its own next copy, and both pass with the racing guard removed outright. They measure the *lag ceiling* honestly and detect over-suppression; they are not evidence that under-suppression is caught. The scenarios added for this net assert after every arrival instead (`arrival_lagged_stream`), which is the form to copy. Strengthening the two older ones is a change to an existing test and was deliberately left out of this phase.

### `src/ingest/arbiter.rs`

- `order_events_collapse_across_publishers_keeping_first_arrival`
- `successive_partial_fills_of_one_order_all_reach_the_wire`
- `a_partly_duplicate_batch_publishes_only_its_new_events`
- `a_late_copy_cannot_resurrect_a_deleted_order`
- `a_repeated_removal_is_not_treated_as_a_resurrection`
- `a_rebaseline_keeps_the_resurrection_guard`
- `a_rebaseline_seeds_an_order_it_had_tombstoned`
- `a_rebaseline_is_suppressed_while_a_peer_is_serving`
- `a_claim_from_a_publisher_that_never_serves_does_not_suppress`
- `a_departed_publisher_does_not_suppress_a_peers_rebaseline`
- `an_interleaved_race_is_not_a_disagreement`
- `a_single_publisher_streams_unimpeded`
- `simultaneous_recoveries_produce_exactly_one_rebaseline`
- `an_evicted_market_still_routes_as_order_level` — decides on the broadcast (one message, not two), and it is the only check that an evicted market re-derives its routing from batch content. Reverting it to the single-arm authority emits `clear_only` plus the batch, which tells every consumer to drop a live book. It also asserts a private `seen`/`resting` bound, so it cannot pass *unchanged* if those fields go — but the consumer-visible half has to survive in some form.

### Elsewhere

Downstream of the merge point, so a replacement should not reach them — but they are the ones that notice if it changes the `book` product's shape rather than its arbitration.

- `tests/dedup.rs` — `interleaved_book_arms_publish_one_coherent_stream`
- `src/ingest/book.rs` (L3 reconstruction, single publisher) — `deltas_report_the_order_they_touched`, `a_rejected_delta_reports_nothing`, `a_removed_order_is_never_resurrected`, `a_fully_executed_order_is_never_resurrected`, `a_session_reset_reopens_the_id_space`, `order_set_is_complete_and_deterministically_ordered`, `order_set_tolerates_extreme_prices`, `end_of_session_drops_book_sequences_and_event_clock`, `instrument_reset_drops_the_event_clock`, `snapshot_then_contiguous_deltas_update_top_of_book`, `execute_reduces_then_removes_order`, `duplicate_and_old_deltas_are_ignored`, `gap_triggers_recovery_then_snapshot_replays_buffered_deltas`, `instrument_reset_drops_book_until_resnapshot`, `periodic_snapshot_while_ready_is_ignored`, `stale_snapshot_ahead_rebootstraps`, `incomplete_snapshot_is_discarded`, `instrument_reset_keeps_post_anchor_buffered_deltas`, `pending_buffer_is_bounded_while_recovering`, `resting_orders_are_bounded_under_add_flood`
- `src/model.rs` (wire shape + `BookAccumulator` replay) — `book_change_order_id_is_additive_and_round_trips`, `book_serializes_to_the_documented_shape`, `a_rebaseline_leads_with_a_clear_action`, `a_lone_clear_is_a_complete_message`, `book_round_trips`, `only_book_reports_a_channel`, `book_reports_its_venue_and_symbol_for_filtering`, `the_accumulator_materializes_the_applied_state`, `a_batch_awaiting_its_last_is_not_materialized`, `non_finite_prices_and_sizes_are_dropped`, `a_zero_source_ts_does_not_blank_the_replayed_event_time`, `a_materialized_book_carries_the_accumulated_source_id`, `the_accumulator_folds_orders_into_levels_with_counts`, `removing_the_last_order_at_a_price_removes_the_level`, `a_one_sided_clear_spares_the_other_sides_orders`, `a_rebaseline_discards_the_previous_order_population`, `the_accumulated_order_population_is_bounded`, `a_terminated_batch_larger_than_the_pending_cap_still_baselines`, `an_unterminated_event_past_the_cap_is_abandoned`, `replay_scope_folds_to_levels_or_materializes_orders`
- `src/ingest/processor.rs` — `mbo_emits_the_order_level_book_alongside_depth`, `mbo_still_emits_depth_alongside_the_book`, `a_cancelled_order_is_published_as_a_delete`, `a_snapshot_install_emits_clear_then_every_resting_order`, `mbo_undefined_instrument_creates_no_book`, `mbo_books_map_is_bounded_under_instrument_flood`
- `src/sinks/ws.rs` (replay/bootstrap) — `connect_replays_the_accumulated_book_rebaseline`, `the_book_replay_scope_follows_the_market_unless_asked`, `book_scope_does_not_change_a_subscriptions_identity`, `subscribe_scopes_the_book_replay_by_channel`, `instrument_is_replayed_before_the_book`, `mid_stream_markets_are_not_replayed`, `a_lagging_client_is_rebaselined`, `prepare_populates_the_channel_for_book_and_instrument`
- `src/sinks/hyperliquid.rs` — `l4book_subscribe_sends_the_whole_book_with_order_ids`, `l4book_orders_carry_no_fabricated_timestamp`, `l4book_forwards_order_diffs_after_the_snapshot`, `l4book_renders_a_gone_order_as_remove`, `l4book_ignores_other_venues_and_coins`, `l4book_never_emits_a_zero_order_id`, `a_market_that_never_baselined_is_not_published`, `a_price_aggregated_market_is_not_published`, `an_emptied_order_book_keeps_publishing_on_both_channels`, `one_book_message_serves_every_subscription_of_the_market`, `a_large_book_truncates_for_l2_and_renders_whole_for_l4`, `a_rebaseline_becomes_an_l4book_snapshot`, `l2book_renders_the_shape_nautilus_parses`, `l2book_truncates_to_n_levels_best_first`, `golden_l4book_frames_match_the_committed_fixtures`, `golden_l2book_frame_matches_the_committed_fixture`
- `tests/hyperliquid_sink_shapes.rs` — `a_golden_l4book_snapshot_parses_as_the_publisher_emits_it`, `a_golden_l4book_updates_frame_parses_as_the_publisher_emits_it`, `a_golden_l2book_frame_parses_as_nautilus_parses_it`
- `src/sinks/api.rs` — `book_reports_coverage_and_respects_baselined`, `book_and_ticker_do_not_cross_universes_sharing_a_channel_and_instrument_id`

## Mechanism

All in `src/ingest/arbiter.rs`, all reading guard internals, a guard metric or a private field. A later phase deletes them with the code they describe; none of them is evidence the product still works.

- Retirement and the arm masks — `a_tombstone_every_arm_reported_is_retired_on_the_spot`, `an_unreported_removal_does_not_hold_the_tombstones_behind_it`, `arms_snapshotting_from_different_anchors_do_not_walk_a_market_to_a_blackout`, `a_market_the_arms_cannot_settle_does_not_rescan_on_every_batch`, `a_sweep_schedules_the_next_one_above_the_population_it_left`, `a_synced_arm_with_nothing_on_the_wire_yet_holds_the_retirement_quorum`, `an_arm_that_stopped_arriving_leaves_the_retirement_quorum`, `a_peer_that_is_not_serving_does_not_hold_tombstones_open`, `the_batch_that_creates_a_markets_race_state_still_knows_who_is_serving`
- Caps, budgets and their accounting — `one_market_may_hold_more_tombstones_than_the_old_per_market_cap`, `the_cap_sweeps_before_it_evicts_rather_than_disowning_a_settled_tombstone`, `the_high_water_re_seats_when_the_market_holding_it_retires_rather_than_reading_zero`, `the_process_wide_ceiling_disowns_the_market_holding_the_tombstones`, `an_overcounted_budget_costs_a_recount_rather_than_every_market`, `guarded_tombstones_are_counted_exactly`, `drift_is_still_caught_after_a_caps_worth_of_removals`, `evicting_a_live_floor_does_not_force_a_rebaseline`
- Disowning bookkeeping — `a_flapping_market_does_not_discharge_its_own_disowning`
- Counters and rate limits — `a_content_disagreement_is_counted`, `the_rebaseline_rate_limit_does_not_follow_the_dedup_window`
**Two of these are bounds, not mechanism. Re-point them; do not delete them.** The wire is unauthenticated, so `order_id`, `channel` and `instrument_id` are all attacker-supplied, and these are the only assertions that the two per-market maps they key cannot grow without limit:

- `the_racing_window_is_bounded_by_event_count` — the sole check that `seen`/`resting` stay within `MAX_SEEN_ORDER_EVENTS`, the per-market bound against an `order_id` flood.
- `eviction_drops_the_racing_state_too` — the sole check that `book_sync` and `book_events` are evicted with `MAX_BOOK_MARKETS`. `book_markets_are_bounded` covers `book_markets`, `book_order`, the replay map and the authority map, and none of those two. `book_sync` is also what the re-baseline suppression reads, which is behaviour.

Whatever bounds the replacement puts on the state that replaces these needs an equivalent test landing in the same change.

- Neighbouring, and only listed so nobody mistakes them for the guard: `src/ingest/book.rs::the_removed_set_is_bounded` (the per-book removed-id set, defence in depth behind the merge point) and `src/ingest/processor.rs::a_gapped_book_reports_itself_unsynced` (the sync report the arm masks consume — if the replacement stops consuming it, this one goes with it).

## Both

Consumer-visible properties that the guard's *current* mechanism produces. Each is something a client can see, so it reads like a contract; each exists only because of a mechanism that may not survive. **These are the tests that break the "must pass unchanged" rule**, and the replacement has to state, for each, whether it keeps the property or deliberately drops it.

Market disowning — the market goes dark, told exactly once as a bare `Clear`, and nothing but a producer's own re-baseline ends the outage:

- `tests/order_level_consumer_book.rs::an_unanswerable_guard_does_not_republish_our_own_view`
- `arbiter.rs::evicting_an_unpassed_tombstone_invalidates_the_market`
- `arbiter.rs::a_market_disowned_by_the_ceiling_is_announced_without_waiting_for_a_batch`
- `arbiter.rs::evicting_a_disowned_market_does_not_let_it_resume_serving_deltas`
- `arbiter.rs::a_bare_clear_ends_a_disowned_markets_outage_even_after_eviction`

The forced re-baseline a size disagreement raises, and what the republished view may claim:

- `arbiter.rs::a_size_disagreement_forces_a_rebaseline_rather_than_a_guess`
- `arbiter.rs::a_forced_rebaseline_republishes_the_wire_not_an_arms_own_book`
- `arbiter.rs::the_arm_that_discharges_a_rebaseline_does_not_own_the_floor_it_seeds`
- `arbiter.rs::a_book_larger_than_the_guard_stays_proportional_to_its_input` (also reads `book_markets[..].rebaseline`)

Tombstones surviving a cap, and the arrival-keyed window's choice to re-emit a late copy rather than refuse it:

- `arbiter.rs::a_rebaseline_larger_than_the_cap_keeps_the_resurrection_guard`
- `arbiter.rs::a_rebaseline_seeds_the_guard_with_its_own_orders`
- `arbiter.rs::an_unreported_removal_keeps_refusing_the_resurrection_it_is_held_for` (also asserts `n_dead`)
- `arbiter.rs::a_copy_past_the_window_re_emits_rather_than_corrupting`

## One thing the net cannot reach

`PEER_SERVING_NS` silence. An arm going quiet for 30 s is measured on a monotonic clock inside the arbiter, with no seam an integration test can drive, so `an_arm_that_departs_and_returns_keeps_the_consumer_exact` uses `Arbiter::forget_publisher_books` — the signal a receiver's registration sends as it exits, and the authoritative one, with the timer as its backstop. Same consumer-visible property; the timer itself is covered only by `arbiter.rs::an_arm_that_stopped_arriving_leaves_the_retirement_quorum`, which pokes the private field and is listed as mechanism above.
