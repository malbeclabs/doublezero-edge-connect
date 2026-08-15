# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- The Market-by-Order resurrection guard forgets a removed order on **venue time** rather than by
  agreement between the publishers. It used to hold a per-arm reporter mask per removed order and retire
  it only once every arm still reaching the market had independently reported that removal; an arm whose
  own snapshot anchor never held the order could never report it, so the population grew with the market's
  whole history until a cap gave way and the market was **disowned** — dark for every consumer until some
  producer re-baselined it, which for a healthy publisher is its next recovery rather than its next
  snapshot rotation. One publisher's anchor could black out a market every consumer was watching. Per
  channel the arbiter now tracks the newest venue stamp accepted and refuses anything older than
  `newest - --arb-book-retention-secs`, forgetting removed entries on the same frontier; an event older
  than its own order's last published change is refused before any size comparison, which also removes the
  false size disagreements that capped the tolerable inter-arm lag at about a second.
- ⚠️ **`dz_mbo_market_invalidations_total` is removed.** Nothing disowns a market any more, so any alert
  or dashboard on it must be retired. Replace it with `dz_mbo_events_past_frontier_total` (a link
  returning with a backlog), `dz_mbo_guard_ceiling_evictions_total` (the process-wide ceiling forgetting
  entries the retention window would still have held) and `dz_mbo_guarded_tombstones_max` for headroom.
  `dz_mbo_guarded_tombstones{,_max}` keep their names and their 1,048,576-entry ceiling but are now sized
  by the retention window and the venue's removal rate rather than by how far the publishers lag.

### Added
- `--arb-book-retention-secs` (30), `--arb-book-ts-jump-secs` (5) and `--arb-book-reseat-secs` (10) tune
  the guard above. The retention default is sized against a measured p99.99 inter-arm separation of 2.77 s
  and 3,958 removals/s per publisher per channel: ~119k entries, 11% of the process-wide ceiling.
- `dz_mbo_events_stale_total`, `dz_mbo_frontier_bounded_total` and `dz_mbo_frontier_reseats_total` report
  the guard's three venue-time refusals and its self-corrections. A sustained `dz_mbo_frontier_bounded_total`
  means the forward bound is catching ordinary jitter rather than a bad stamp.

### Security
- `--admin-bind`/`DZ_ADMIN_BIND` now defaults to `127.0.0.1:9098` instead of empty, so
  `doublezero-edge channels list`/`channels set` work out of the box. The exposure is accepted on
  the condition that the default binds loopback only — a wildcard bind still requires an explicit,
  documented override, and the non-loopback warning in `scripts/connect.sh` is unchanged.
### Fixed
- The resurrection guard's out-of-queue sweep is scheduled rather than triggered on every batch. The
  threshold was clamped to half the per-market tombstone cap, so above that population it sat *below*
  the population itself and the comparison was true forever: a full scan of the market's tombstone map
  per datagram, held under the one arbiter mutex every receiver on every feed takes to emit. Measured
  synthetically at 141 µs a batch with 36,768 tombstones held, against 6.9 µs on a market in step —
  and one datagram from a source the market has never seen is enough to start the population climbing.
  The clamp's other, undocumented job was keeping a sweep ahead of the eviction that disowns a market,
  which is now explicit at that eviction and holds for a batch large enough to cross the cap on its
  own. After: 3.7 µs at 36,768 and 5.5 µs at the cap.
- `dz_mbo_guarded_tombstones_max` no longer reads zero while a market holds the real maximum. It
  followed the market holding it back down, so when that market's arms caught up and it retired, the
  gauge reported the shrunken figure — full headroom — while a market that had gone quiet still held
  its whole population, which is the market the gauge exists to find. It now re-seats on the largest
  survivor, as it already did when the holding market was dropped.
- A forced re-baseline's rate limit no longer follows `--arb-book-dedup-window-ms`. The two shared a
  value, so widening the window for its dedup reach (250 ms → 1 s, above) quadrupled how much of a
  real disagreement's stream a market skips — batches withheld there are lost, not delayed. It is a
  fixed 250 ms now. On the real capture the wider window drops **one** batch against 24–217 at 250 ms,
  because the false disagreements stop happening at all; this is about the worst case per
  disagreement, not the observed total.
- The resurrection guard's retirement no longer stalls on a removal one arm never reports. It ran
  head-of-queue over the removal order, so an arm whose snapshot anchor post-dates a removal — it
  never held that order, so it never reports it — blocked every tombstone behind it for the life of
  the market, and the population reverted from the arms' lag spread to the market's whole history,
  exiting only at the per-market cap where the market is disowned. Retirement now also sweeps out of
  queue order, on a threshold that doubles after each sweep so it costs O(1) amortized per tombstone
  and nothing at all while the arms keep up. **What counts as evidence is unchanged** — every arm
  still reaching the market must have reported the removal — so a forged datagram buys exactly what
  it bought before: one arm's bit on the one order it names.
- A market disowned by the process-wide tombstone ceiling is announced when it happens rather than on
  its next batch. That market is not the one being admitted and need never send another — a market
  whose arms drifted apart and then both went quiet is exactly how one comes to hold the most
  tombstones — so its consumers kept a book that silently stopped updating while a client connecting a
  second later got none at all.
- A disowning survives `MAX_BOOK_MARKETS` eviction. The record lived on the per-market state eviction
  drops, and the order-level memo is re-derived from batch content, so an evicted market came back
  reading as ordinary and resumed serving deltas onto a book nothing vouches for.
- The Market-by-Order resurrection guard no longer corrupts a consumer's book when the two
  publishers drift far apart. It was bounded by a per-market count of 512 removed orders, which is a
  count standing in for a *time* tolerance, and past it — 150 ms of inter-arm lag on a busy market,
  against 186 ms measured between the real publishers — it evicted a tombstone a lagging arm could
  still race and then re-baselined the market from our own accumulated view, which is the view the
  guard had just failed to protect: the resurrected orders were republished as a complete book and
  re-seeded as live, and nothing removed them again. Now a tombstone is retired as soon as every arm
  still reaching the market has reported the removal, so the population sizes itself to the arms' lag;
  it is bounded process-wide instead of per market (the same aggregate memory, spent where the guard
  needs it), with the ceiling charged to the market holding the tombstones rather than to whichever
  market records the next removal; and a guard that genuinely cannot answer **disowns** the market —
  state and replay entry dropped, consumers told to drop the book, nothing published until a producer
  re-baselines it — rather than republishing our own view. Replaying the two real captured publishers,
  a consumer that ended 994 orders wrong at 300 ms of inter-arm lag, permanently, now holds the venue's
  book exactly, and the guard's eviction never fires at any lag tested. With the wider dedup window
  below, first divergence on that capture moves from **153 ms to 2 s** — exact at every step through
  1 s. The synthetic sweep in `tests/order_level_consumer_book.rs` now partially fills every order, so
  it can produce the size disagreement the real capture shows, and it runs at the flagship's measured
  ~890 changes/s rather than a stress rate, so its figure is comparable to the capture's. It holds to
  1 s and is 223 orders wrong at 1.2 s — the ceiling being `seen`'s 1024-event cap (1.15 s at that
  rate), not the dedup window. A separate replay, of orders cancelled without ever trading, pins that
  holding more than the old 512 tombstones costs the market nothing.

### Changed
- `--arb-book-dedup-window-ms` now defaults to 1000 (was 250). The window stopped being the guard's
  reach, so what it governs now is whether a lagging arm's copy is recognized as a duplicate at all:
  below the arms' real separation, a stale copy of an add for an order the leader has since partially
  filled reads as a size *disagreement* between two healthy publishers, forcing a re-baseline whose
  withheld batches are lost. On the real two-publisher capture this is worth 500 ms → 2 s of tolerated
  inter-arm lag, and it takes the disagreements those healthy arms manufactured to zero. Costs no
  memory — `seen` is capped at 1024 events per market independently of the window, which is also why a
  value much above a second is inert on a busy market: 1 s and 10 s measure identically. It did cost
  something else, now fixed below: the same value was the forced re-baseline's rate limit, whose
  withheld batches are lost rather than delayed, so a real disagreement skipped up to 1 s of a
  market's stream instead of 250 ms.

### Added
- `dz_mbo_market_invalidations_total{venue}` counts markets disowned by the guard above,
  `dz_mbo_guarded_tombstones` reports the tombstones held against the process-wide ceiling, and
  `dz_mbo_guarded_tombstones_max` reports the largest single market's population against the
  per-market cap — **the one to alert on**, since that cap fires at a sixteenth of the aggregate and a
  market walking to a blackout is flat headroom on the sum. `dz_mbo_forced_rebaselines_total` loses its `reason="guard_evicted"`
  label, which no longer has a behaviour behind it.
- `doublezero-edge diagnose` reports why the container is (or is not) serving data — tunnel,
  subscriptions, activations — ending in one verdict an agent reads as `.diagnostics.diagnosis.code`.
  It reads the admin surface, which is not subscription-gated, so it answers on exactly the host
  where every `/v1` command fails. Exits 0 whatever the verdict; 3 only if the admin surface itself
  does not answer. It only ever *reports*: retrying a down tunnel stays where it already is,
  `doublezero connect multicast` inside the container, so nothing here can spend the container's
  onchain identity. `GET /admin/diagnostics` is unauthenticated like the rest of the admin surface
  and discloses this host's device/metro names, subscribed group codes and their multicast IPs, the
  configured binds and the feed-registry URL — on the loopback default, to the same audience that
  could already run `doublezero status`. The container also reads `doublezero latency` and reports
  each device's reachability and min/avg/max round trip, nearest first — on a down host, whether
  anything is reachable; on a connected one, whether a nearer device has appeared since it chose
  the one it is on. Probed every 5 minutes rather than every poll and bounded at 20s, so a stalled
  probe cannot wedge the reconciler's tick; the block carries its own `latency_at_unix`, and `null`
  (never probed) stays distinct from `[]` (probed, nothing answered).
  `--admin-url` is now a global flag rather than one on the two `channels` subcommands.
- `GET /v1/products` accepts `limit`/`cursor` query parameters and reports a `cursor` when more
  products remain; `doublezero-edge products list --paginate` follows it until the catalog is
  exhausted, accumulating every page into one response. Default stays unlimited: omitting `limit`
  returns every product in one response exactly as before.
- `GET /v1/best_bid_ask` accepts a comma-separated `product_ids` query parameter, filtering the
  response before serialization; `doublezero-edge products best_bid_ask` accepts a bare product id
  or `product_ids==A,B`, matching the sibling subcommands' positional-id convention. An unknown id
  404s exactly like `/v1/products/{id}` does, rather than a new convention. No parameter keeps
  today's behaviour: every product.

### Changed
- `doublezero-edge products book --output table` now renders as a ladder — asks descending
  above, bids descending below — instead of every bid then every ask, so the touch sits one row
  apart at the seam instead of split across a screenful. `--output json`/`--jq` are unchanged.

### Fixed
- The Market-by-Order cross-publisher resurrection guard no longer loses to its own bound. Three
  ways it did: a forced re-baseline stamped the arm that discharged it as owning the floors it
  seeded, so that arm — the one whose stale, larger size raised the flag — could re-assert it
  unchallenged; the eviction that decides whether losing an entry costs anything was scoped by the
  dedup window, which is the wrong horizon (the guard exists for copies arriving after it) read off
  the wrong clock (a delete mutated the entry in place, so a tombstone carried its add's timestamp);
  and a recovery snapshot larger than the guard's cap seeded itself over the tombstones, which is
  the normal state of a 44,598-order market. Eviction is now scoped by what is evicted — a live
  floor re-seeds itself and goes silently, a tombstone re-baselines the market and is counted on the
  existing `dz_mbo_forced_rebaselines_total{reason="guard_evicted"}` — with the two populations
  bounded apart so neither starves the other, and a tombstone every serving arm has reported treated
  as spent so a busy market's dead orders cannot fill the guard and re-baseline it forever.
- A forced re-baseline no longer discards a batch that removed an order. Only a tombstone-creating
  removal can cross the guard's dead-order bound, so the batch a `guard_evicted` force discarded always
  carried the delete for the order the eviction was about — and the same is reachable from a
  `disagreement` on a batch that both cancels one order and claims a drifted size for another. Either
  way the order stayed live in the republished book and the seed marked it live in the guard too, so
  nothing removed it again. A batch that removed nothing is still dropped, which keeps a disagreement's
  torn logical event off the wire wherever the removal does not make dropping it worse.
- The serving-arm set the guard reads to decide whether a tombstone is spent is now refreshed on the
  batch that creates a market's race state, not from the one after it. A session reset drops that
  state while the arms keep serving, so the batch re-creating it treated every tombstone it made as
  spent and evicted one without re-baselining.
- The Hyperliquid-compatible sink holds the shared book map's mutex — the one the ingest emit path
  takes on every published batch — for a clone and nothing else; every rendering step runs after the
  guard drops. The clone is the cheapest snapshot available rather than a fallback:
  measured on the 44,598-order fixture it is ~0.45 ms against ~9.1 ms to fold under the guard.
- A Hyperliquid-sink market that empties no longer stops being treated as order-level, which silently
  stopped both book channels — permanently for `l4Book`, whose snapshot is only sent once that gate
  passes. Order-level is now a property of the market rather than of what it currently holds.
- `mantissa: 1` is accepted on an `l2Book` subscription (the venue allows 1, 2 and 5 at `nSigFigs` 5);
  anything else is still refused rather than coerced.
- Two Market-by-Order publishers disagreeing about one order's resting quantity no longer publish
  the larger of the two. A resting order only shrinks, so a larger claim means one of the books has
  drifted — and which one is unknowable at the merge point: the larger rewinds a consumer past a
  fill the venue already applied, and preferring the smaller lets a forged size mute a real order.
  The market stops being served from either arm's deltas and is republished whole instead. The same
  happens when the cross-publisher resurrection guard is asked to age out an order a peer's copy could
  still be racing, which would otherwise silently reopen the path that guard exists to close (an
  eviction past that horizon costs the guard nothing and is left alone, so a book far larger than the
  per-market cap still streams normally). Both are counted by the
  new `dz_mbo_forced_rebaselines_total{venue,reason}` (`disagreement` / `guard_evicted`); each costs
  a full republish of the market's book — rate-limited to one per market per
  `--arb-book-dedup-window-ms` — so a sustained rate is the signal that the per-publisher book model
  is too expensive to keep. The republish is the book the wire agreed on, never one publisher's own.
- A re-baseline no longer wipes the cross-publisher resurrection guard. Only the dedup window is
  stale after one — a venue still never reuses an order id, so a lagging publisher's first and only
  copy of an `Add` for an order a peer already killed must still be refused. The re-baseline's own
  orders now seed the guard, so a peer claiming more than the snapshot holds is still caught as
  drift. Session and instrument boundaries keep dropping it outright: a new id space is the one case
  where that is correct.
- A departed Market-by-Order publisher no longer suppresses a surviving arm's re-baseline for 30s.
  Its receiver's exit is the authoritative departure signal and now releases its book standing;
  `PEER_SERVING_NS` stays only as the backstop for a publisher that goes quiet without
  deregistering. A gap-and-recover cycle is sub-second, so the timer never bound on it — and a
  suppressed re-baseline is never retried, which wedged the market for the life of the process.
- The installers (`scripts/connect*.sh`) could finish with `Done. Connected.` over a tunnel that
  never came up (#132): a single `doublezero connect multicast` attempt after a fixed 30s sleep
  loses the race against a cold daemon's device probing, its failure is non-fatal, and the closing
  banner never consulted the outcome. `connect` is now retried (4 attempts, 15/30/45s apart) and
  every closing message is gated on the daemon's own reported session — a failed or still-
  provisioning tunnel is named as such, with the by-hand retry and status commands, and restated
  as the last line rather than buried above the CLI offer. Exit status is unchanged (0): the
  container and CLI are installed either way.
- `docker-entrypoint.sh` matched `session_status` against the literal `"up"`, which matches none of
  the daemon's live values (`BGP Session Up`, `PIM Adjacency Up`) — so the graceful `doublezero
  disconnect` on `docker stop` never ran and every restart left the onchain session to expire on
  its own. It now matches the values' `Up` suffix, as the installers' new probe does.
- `GET /v1/products`'s `feed_kind` fell back to `unknown` for every market on a venue whose rows
  span more than one category, even when its own category resolves unambiguously (e.g. Lashay's
  single-kind `sports` category, sharing a venue with the two-kind `perps` category). The registry
  fallback now filters by `(venue, category)` instead of venue alone.
- `doublezero-edge`'s admin-surface connection failure still said the surface is "off unless
  DZ_ADMIN_BIND is set", which stopped being true when that bind defaulted to `127.0.0.1:9098`.
- `doublezero-edge` panicked with a broken-pipe error whenever its stdout output was piped into a
  consumer that stops reading early (`| head`, `| less -q`, a short-circuiting `grep -q`) —
  completely ordinary usage for a JSON/table-printing CLI. Every stdout write now goes through a
  handle that exits cleanly (code 0) on `EPIPE` instead of panicking.
- `scripts/connect.sh` configured a package repository nothing publishes to, so the CLI offer
  failed and skipped on every run — it now points at `malbeclabs/doublezero`, the repository a
  host running the DoubleZero client already trusts. The offer's wording and the closing output
  were also broken up; the package `maintainer` address is set.
- A mirror publisher's `publisher_offset` (the entry directly below) only ever resolved on a
  `derived` row; an `explicit` row could not declare one at all, so a live feed mirrored by a
  second publisher sharing one port block and separated only by `channel_id` had every market
  enter the catalog twice, one of the pair never getting a book. `publisher_offset` is now a
  row-level registry field so either shape can declare it.
- `GET /v1/products` returned the catalog in `HashMap` iteration order, which shuffles between
  calls. It is now sorted server-side by `(channel, instrument_id)` ascending, so every client
  benefits rather than just a table renderer that happened to sort client-side.
- The channel-departure purge fired on any shrink of the desired feed set, including a plain
  subscription loss (a group unsubscribed, or a `doublezero status` blip that parses fine and
  momentarily stops listing a code) — destroying a channel's catalog/book/history on a one-tick
  blip that used to be harmless (an unsubscribe only ever stopped receiver tasks before this
  reconciler purged anything). The purge is now driven solely by an explicit channel-filter
  narrowing (`--channels` at startup, or `POST /admin/channels`); a subscription loss still stops
  the receivers, but leaves their data alone to resync onto once the subscription returns.
- A mirror publisher that raises every channel id by a fixed offset on the same ports (registry
  `derived.publisher_offset`) minted a second catalog/history/book entry under the raised id, half
  of which a departure purge could never reach (it purges by the registry's roster id alone),
  leaving them served forever. Ingest now canonicalises the wire channel to the base id for every
  consumer-facing identity — catalog, history, book, product id — while producer-side state (books,
  sequence tracking, reset counts) stays keyed on the raw wire channel, since the two arms are
  independently sequenced.
- `doublezero-edge`'s `client::get`/`classify` treated a `2xx` response with an undecodable body
  as success, printing the synthesized `invalid_response` envelope to stdout with exit code 0 —
  pointing `--url` at the wrong port (e.g. the WebSocket) made `products list` "succeed" with
  garbage. A decode failure is now a distinct `Outcome::Invalid`, refused regardless of status,
  same as an unreachable server.
- `channels list`/`channels set` indexed the `/v1/status` `channels` block directly, so a body
  missing that key (a server predating it) failed to deserialize instead of defaulting — losing
  the drop preview and, without `--force`, refusing `channels set` outright, on exactly the server
  skew `#[serde(default)]` exists to tolerate. Both now default a missing key to an empty object.
- `--publisher-port` combined with `--channels` could narrow an enabled feed to zero publishers
  with no warning (a channel-filter clause can be individually valid against the whole registry
  while naming a channel `--publisher-port` already excluded), silently taking the WS sink, query
  API and history feeder down. Startup now refuses that combination.
- The same combination via `POST /admin/channels` returned `200` and emptied the feed on the
  reconciler's next tick. It now returns `400` and leaves the prior channel filter in force.
- `POST /admin/channels` accepted a bodyless request with a query string, which a plain HTML
  `<form>` post can produce with no attacker involvement beyond an open web page on the admin-bound
  host — loopback does not stop a request that originates on the host itself. `POST` now also
  requires an `X-DZ-Admin-Request` header (any value); `doublezero-edge channels set` sends it
  automatically.
- `candles`/`retention` answered an evicted product exactly like one genuinely holding no trades
  (both an empty candle list and `oldest == newest == now, truncated == false`), so a busy market
  bumped from the history store by the `MAX_PRODUCTS` cap read as quiet. `retention` now carries a
  `held` field distinguishing "the store no longer tracks this product" from "no trades in this
  window," and `MAX_PRODUCTS` is raised 1,024 -> 8,192 (see `history.rs`'s docs for the memory
  arithmetic).
- `products::resolve`'s ambiguity error rendered two candidates identically when two universes
  under one Source ID happened to share both symbol and `(channel, instrument_id)` — the one case
  the disambiguating suffix cannot break — naming no market a caller could actually ask for.
  `resolve` is now category-aware internally and the error appends the category when (and only
  when) two candidates would otherwise render identically. The product id format is unchanged.

### Added
- `scripts/connect.sh` now checks UDP `44880` (the liveness port the container's own doublezerod
  binds) before starting the container: a host daemon already bound there previously produced a
  "successful" install followed by a container that died seconds later. It offers to stop (and
  disable) the host daemon — only when systemd shows it active, never a service unrelated to the
  conflict — states that this disconnects any tunnel the host daemon owns, and refuses to start the
  container on a decline or an unanswered non-interactive run. `DZ_STOP_HOST_DAEMON=1|0` answers
  non-interactively.
- `GET /v1/status` carries a new `registry` block: which feed-registry document this process
  resolved (a URL, a bind-mounted file path, or `"built-in"`), its `version`, and its row/receiver
  counts — the same figures the bridge already logs once at startup as "feed registry resolved,"
  now checkable across a fleet without log access. `doublezero-edge status` renders it as one
  orientation line. `scripts/connect.sh`'s printed feed-registry line no longer leaks raw ANSI
  escapes from the container's coloured log output.
- `doublezero-edge` is now packaged as a signed deb and rpm, built as a static musl binary and
  published to the same repositories as the DoubleZero client — so installing it needs no new key
  and no new trust decision. The package carries the binary and shell completions and nothing else:
  no dependencies, no maintainer scripts, no unit files, which is what makes it safe for
  `scripts/connect.sh` to offer from a prompt. A `completion` subcommand generates the bundled
  scripts at build time.
- `scripts/connect.sh` offers to install that package once the bridge is up. The prompt states what
  it will do — run the vendor's repository setup script as root, then install one package — before
  doing any of it. `DZ_INSTALL_CLI=1|0` answers non-interactively. Declining, a package-manager
  failure, or a run with no terminal all leave the bridge running and the installer exiting success:
  the bridge is the product and the CLI is a convenience.
- The image now defaults `DZ_FEED_REGISTRY_URL` to the hosted feed registry, overridable with `-e`
  or by bind-mounting a document. A host that cannot reach it falls back to the built-in copy
  silently by design, so `connect.sh` prints which source actually resolved.
- A rolling one-hour, in-memory market-data history (`src/history.rs`): 1-second OHLCV buckets plus
  a bounded ring of recent prints, per product, fed from the post-arbiter broadcast — so every print
  arriving here is already deduplicated on `trade_id` and gated by the tape leader, one copy per
  print. Pre-aggregating into fixed one-second buckets keeps the footprint independent of trade rate
  (a product costs the same whether it prints once a second or five hundred times). **The window
  lives in memory only and is gone the moment the process restarts** — there is no persistence of
  any kind, and no retention beyond the hour. Bounded two ways, since a per-product cap alone
  doesn't bound total memory: `MAX_PRODUCTS` (1,024, a pure cardinality guard on the tracked-product
  map) and an aggregate `MAX_BUCKETS_ACROSS_PRODUCTS` (2^20) bucket budget across every product —
  the bound that actually holds, at ~121 MiB worst case together with the print-ring bound — both
  evicting the least-recently-traded product first.
- A read-only `/v1` HTTP query API (`src/sinks/api.rs`), a sibling output sink to the WebSocket and
  Prometheus endpoints: the instrument catalog (`GET /v1/products`, `/v1/products/{id}`), OHLCV
  candles (`/v1/products/{id}/candles`), recent trades + best bid/ask (`/v1/products/{id}/ticker`),
  an order book (`/v1/products/{id}/book`), best bid/ask across products (`/v1/best_bid_ask`), and
  feed/history status (`/v1/status`). Binds `127.0.0.1:9099` by default (`--api-bind` /
  `DZ_API_BIND`; empty disables it outright); the subscription reconciler activates it under the
  **same condition as the WebSocket sink** (≥1 market-data feed subscribed) and binds it
  non-fatally, so a taken port disables the API without crash-looping the tunnel. **No
  authentication and no TLS**, matching every other service surface here — the loopback default is
  load-bearing, since the container runs host networking and a wildcard bind would be genuinely
  network-reachable; terminate at a reverse proxy if this must be exposed. The catalog is not
  necessarily every instrument the feed defines: a product appears once its source is known, which
  for a publisher whose reference data carries its own Source ID is at definition time, but for one
  whose reference data carries no Source ID of its own is only after its first price — so a
  defined-but-never-traded instrument on the latter kind of publisher is absent from `/v1/products`
  until it prints, and both publisher generations can be live at once. Every response that could be
  mistaken for complete carries an honest coverage/retention block rather than a guess (an
  unbaselined `book`, a depth slice truncated at the wire's own cap, a `candles` page cut short by
  `limit`).
- **`doublezero-edge`**, a new read-only host-side CLI (its own workspace member) that queries that
  API: `products list`, `products get`, `products ticker`, `products candles`, `products book`,
  `products best_bid_ask`, and `status` — seven commands, modelled on the Coinbase Advanced Trade
  CLI's `key==value`/`--jq`/`--template`/`--output table` surface. `--url` (`DOUBLEZERO_EDGE_URL`,
  default `http://127.0.0.1:9099`) points it at a remote container. There is no order-placement or
  mutation path anywhere in edge-connect for it to reach, so unlike the tool it emulates, no command
  here ever needs a confirmation prompt. It builds on macOS as well as Linux; the bridge itself does
  not, since it uses `SO_TIMESTAMPNS` via `nix` with no `cfg` gate — the reason this is a separate
  workspace member rather than a bin in the bridge crate.

### Fixed
- The query API's history feeder resolved a `trade`'s product by matching `(venue, symbol)` against
  the instrument catalog, dropping any trade whose symbol matched more than one market. On a
  price-aggregated venue whose redundant publisher arms carry an identical instrument set under
  distinct channel ids, *every* symbol matches twice, so every trade was silently dropped: zero
  candles and zero ticker history for that venue's whole product set, indistinguishable from a
  market that simply had not traded. `trade` now carries its own `channel`/`instrument_id` (see
  Added, below), so the feeder keys straight off the message instead of guessing from a
  possibly-ambiguous symbol; the lookup this replaces is removed entirely.
- A trade's venue-supplied `source_ts_ns` more than a few seconds ahead of its own receive time, or
  older than the history window, is no longer trusted into the query API's rolling store. The store's
  late-print rejection compares only against a product's own high-water mark, which it never resets:
  one implausible print latched it permanently, late-dropping every subsequent, correctly-timed print
  for that product before it ever reached the print ring — so both `/candles` and `/ticker` emptied
  out for it with no recovery. An implausible timestamp now falls back to the trade's own receive
  time, the same fallback already used for the `source_ts_ns == 0` sentinel.
- The feed registry validated a document's `venue` for resolvability only, which admits a legacy
  alias for one Source ID even though only its canonical name ever reaches the wire. A document
  naming the alias validated cleanly and then silently split every downstream lookup keyed on the
  venue string (the arbitration mode, the channel-filter purge, `--feed <venue>` selection). The
  venue must now round-trip through its canonical name.
- A `--feed-registry` file that could not be read degraded to the built-in document with only a
  warning, so an unmounted volume or a typo'd path started the container healthy on a stale
  topology instead of refusing. That read failure is now fatal, matching the parse errors beside it
  and the file source's documented contract.
- An `explicit` publisher list left empty in the document installed a row with zero publishers —
  no error, no receivers, and a healthy-looking `rows=1 receivers=0` log line. It is now rejected
  the same way an empty `derived` roster already was.
- The feed registry fetch had no bound on response size, so a hostile or compromised endpoint could
  OOM the process instead of ever reaching the built-in fallback, and then crash-loop re-fetching on
  restart. The fetch is now capped, checking both a declared `Content-Length` and the accumulated
  body as it streams in, so a chunked response with no declared length is covered too.
- A departed channel's catalog/book/history purge ran right after its receiver's `abort()` call,
  but `abort()` only cancels a task at its next `.await`; a receiver already past
  `recv_any().await` can still run the rest of its synchronous body and re-insert the very state
  the purge just removed — permanently, since a channel is never diffed as departing twice. The
  purge now waits for the receiver's `JoinHandle` to report finished before running, with a bounded
  fallback so one that never does (e.g. wedged in a blocking call) can't leak its state forever.

### Added
- `trade` carries `channel`/`instrument_id`, the same identity pair `instrument`/`book` already carry.
  Purely additive — existing fields are unchanged, and the fields are ignored harmlessly by any
  consumer that doesn't read them yet. `0` on a source with no channel concept of its own (the public
  WS backstops resolve the real value from the edge catalog where one exists).
- `dz_history_unattributable_trades_total{venue}` counts a trade the query API's history store
  dropped because its declared `(venue, channel, instrument_id)` names no known instrument — a
  definition race, belt-and-braces alongside the fix above. Should stay flat at zero.
- `dz_history_feed_lagged_total` counts the query API's history feeder falling behind the broadcast
  and dropping messages (`Lagged`) — a hole in the rolling window, not a crash.

### Changed
- An instrument whose reference data carries its own Source ID (the newer feed-spec generation) is
  now named the moment its definition is decoded, instead of waiting on a price. `instrument`
  reaches the wire immediately — even for a symbol that never trades — and the connect-time replay
  includes it. This applies to `TobProcessor`, `MboProcessor` and `MbpProcessor`; a publisher whose
  reference data carries no Source ID of its own (the original generation) is unaffected and keeps
  deferring exactly as before, as does Midpoint permanently (its own, narrower reference-data
  message has no Source ID field at all, on any generation).
- **Breaking:** the wire `Source ID` is now authoritative for naming a source. `source_id` carries it
  verbatim and `source`/`venue` are its registry name. The bridge no longer substitutes its own
  configured label for an unrecognised ID, so `venue` can hold a different string than before for a
  publisher that stamps an incorrect Source ID. Re-check any consumer that filters or keys on `venue`.
- **Breaking:** registry source names are now **uppercase** (`HYPERLIQUID`, `PHOENIX`, …), which is
  the form `venue` and `source` carry on the WebSocket and every `venue=` metric label value holds —
  so a consumer composing a `SOURCE:SYMBOL` product identifier never has to case-fold what the wire
  gave it. The `--feed` argument and the `venue`/`source` subscription filters already compared
  case-insensitively and are unaffected; **dashboards and alerts matching a `venue` label value
  exactly must be updated.** Source ID 3 additionally answers to a legacy name through
  `sources::source_id_of`, so operator- and ledger-facing strings predating the current registry
  name keep resolving to the same ID; only the registry name is ever emitted.
- Reference-data messages arriving on a Market-by-Price **market-data or snapshot** port are now
  dropped instead of applied, matching the three sibling processors' `handle_refdata` gate. Decode
  does not care which physical port a message type arrives on, so a single forged datagram spoofing
  a publisher's source IP with a `ManifestSummary` one sequence ahead cleared that publisher's
  instrument definitions — and since every emission path gates on a resolved definition, the venue's
  `book` and trade tape went dark until the next reference-data burst. `MbpProcessor` also drains
  `PerPublisher`'s eviction now, so an evicted publisher's books, revealed Source IDs, announced
  symbols and per-channel snapshot state go with it rather than outliving the reference data they
  depend on.
- A `BookClear` whose `Clear Side` is *both* is now refused for **every** scope byte except the
  recognized whole-side one, at the codec and in `PriceBook` alike. The guard tested `scope == 1`
  while the apply path derives its behaviour from the complement, so an unassigned `2..=255` was
  treated as price-bounded and `{ clear_side: 2, scope: 2 }` removed bids at/below and asks at/above
  a single bound — the whole book — republished to every consumer as `Delete`s.
- A duplicated `SnapshotBegin` datagram is now a no-op instead of restarting assembly. Mid-rotation
  the status is `BuildingSnapshot`, which no decline rule covered, so an identical re-begin zeroed
  the open group; `on_snapshot_end` then failed its level count and took the incomplete-group path,
  which clears the book — destroying the **live** book on the `Ready`-rebuild path and dropping the
  market to `AwaitingSnapshot` until the next rotation. A begin differing in any identifying field is
  still a new rotation and still replaces the group under assembly.
- Trade-tape ownership now ranks over the **registered** receiver set rather than the desired one.
  Registration follows a successful socket bind, so a row that can never bind returned `Err`, was
  reaped and respawned every tick without ever registering — and, ranked as if live, held rank 0
  indefinitely while `publish_tape_owners` cleared the streaming peer's flag each tick, so no `trade`
  reached the wire for the venue at all while `status` and `dz_feed_up` still read healthy. Liveness
  is now three-state: "not registered yet" ranks below a live row (an incumbent keeps the tape until
  the newcomer really registers) but above a registered-and-dead one, so a cold start where nothing
  has bound still falls back to feed-kind rank.
- **Breaking:** a message is emitted for an instrument only once its Source ID has been observed. A
  publisher whose reference data carries no Source ID of its own can only reveal it through a price
  message, so an instrument that has received a definition but no price produces nothing at all, and
  the connect-time replay covers only symbols priced at least once. (A newer publisher generation
  changes this — see above.)

### Added
- **A Hyperliquid-compatible output sink** (`--hl-ws-bind` / `HL_WS_BIND`, off by default): the same
  market data in Hyperliquid's own WebSocket schema — `l2Book` (full depth with the per-level order
  count), `l4Book` (order-level snapshot then diffs, carrying the venue's order ids) and `trades` —
  so an existing Hyperliquid client consumes edge-connect by changing one URL. A rendering, not a
  second protocol: PROTOCOL.md is unaffected. See
  [Output sinks](docs/output-sinks.md#hyperliquid-compatible-sink), including what a stock
  NautilusTrader client can and cannot receive.
- **Market-by-Order now serves the order-level `book` alongside its existing `depth`.** The bridge no
  longer throws the order identity away: every change carries the venue's own `order_id`, a snapshot
  install re-baselines as a `clear` plus the full order set, and `depth` keeps working unchanged for
  the consumers on it. PROTOCOL.md stays **v1** — both additions are fields a consumer may ignore.
- `order_id` on a `book` change: the venue's order id for an order-level change, `0` when the change
  is price-aggregated (Market-by-Price) and carries no order identity.
- `book_scope` on a subscription (`"levels"` | `"orders"`): the granularity of the `book` bootstrap.
  Omitted — the default, and the only possibility on the connect-time replay — it **follows the
  market**, so an order-level market is bootstrapped as orders and a price-aggregated one as levels. A
  bootstrap and a stream of different granularity cannot be reconciled: an order-level change carries
  one *order's* absolute size, and applying it as a level's size corrupts the book.
- Order-level `book` events are **raced across publishers on venue event identity** rather than served
  by one elected arm: each event is published once, from whichever publisher delivered it first. What
  carries correctness is a per-order guard at the merge point, not the dedup window — a change for an
  order the producer has already published as gone is refused, so an arbitrarily late copy costs a
  redundant emission and cannot resurrect a dead order. A publisher recovering by snapshot republishes
  its whole book only when no peer is both synced and actually serving the market, so a recovery cannot
  wipe a live book and a departed publisher cannot block one either.
  `--arb-book-dedup-window-ms` (default 250) tunes how long a delivered event is remembered.
- `dz_book_events_deduped_total{venue}`, `dz_book_resurrections_dropped_total{venue}`,
  `dz_mbo_arm_disagreement_total{venue}` and `dz_mbo_removed_evicted_total`. The disagreement counter
  is the one to alert on: it fires when a publisher claims more resting quantity for an order than a
  peer already reported, which is a book that has silently drifted. See `docs/metrics.md`.
- Every message now carries `source` and `source_id` alongside `venue`. The subscription filter
  accepts `source` as an alias for `venue`; supplying both ANDs them.
- `dz_source_id_changed_total{venue}` counts a publisher changing an instrument's Source ID
  mid-stream, which triggers a fresh `instrument` announcement under the new name.

### Deprecated
- The `venue` field and the `venue` subscription filter key. Both still work and hold the same value
  as `source`; read `source` instead. Removal will be announced separately.

### Fixed
- `dz_mbp_orphan_snapshot_levels_total` counted every level of a snapshot rotation the book
  **deliberately declined**, not just genuinely unroutable ones. A book that is `Ready` and already
  past a rotation's `Last Instrument Seq` refuses it by design, but refusing opened no route, so its
  levels fell through the same branch as a lost `SnapshotBegin`. Publishers rotate snapshots
  continuously, so once the books sync this is the steady state: measured against the live Lashay
  perps groups, it was ~415 levels/s — 100% of the feed's snapshot-level rate — which buried the
  anomaly the counter exists to surface. A declined rotation now holds the route with an `accepted:
  false` marker (so its levels stay attributable and out of a neighbouring instrument's book) and is
  counted by the new `dz_mbp_declined_rotation_levels_total`, leaving the orphan counter to mean what
  its name says. With the noise removed, the live feed shows a genuine residual of ~2.6% of snapshot
  levels arriving with no `SnapshotBegin` — independently reproduced by `marketbyprice-parser` on the
  same groups, with zero host-side UDP or socket errors, so it is upstream loss or reordering rather
  than a receive-side defect.
- Five Hyperliquid publishers that had been live on `tiredsolid` since mid-June were missing from
  the feed registry, so the bridge bound 6 of 11 port blocks and ingested roughly a third of the
  group's datagrams — including none of the three highest-volume Top-of-Book blocks or the
  highest-volume Market-by-Order one. The registry had been sourced from the publisher deployment
  inventory, which covers only a subset of the hosts on the group; the authoritative list is the
  feed-capture recorder inventory. Adds TOB 9011/9501/9701/9801/9901 and their Market-by-Order
  peers, and pins the exact base-port set in a test so a future omission fails the build. **This
  roughly doubles ingest cost: 23 receivers over 57 sockets, ~456 MiB of requested `SO_RCVBUF` at
  the default `--recv-buf` — see the sizing note in `docs/input-sources.md`.** (#93)
- The idle-rejoin interval now escalates (30s doubling to a 5-minute cap) when a rejoin produces no
  market data, instead of rebinding the whole port block and logging a warn+info pair every 30s
  forever. A permanently-silent block — a retired publisher, or a registry row whose endpoint never
  went live — settles at ~12 rejoins/hour. Detection is unchanged: the socket stays bound, so a
  returning publisher is picked up on its first datagram, and the first `status: down` still fires
  at 30s. The interval resets only on market data arriving, never on a successful bind. (#93)

### Changed
- Trade-tape ownership is now a **runtime** decision instead of the static `Feed.emit_trades` flag. A venue's feeds can ride separate multicast groups with separate subscription codes, so a host may hold one and not the other and still needs a tape on the wire; both rows now claim trades and the reconciler ranks the running receivers (top of book over market-by-price) and flips an `AtomicBool` each processor reads per print. Ownership therefore moves **without respawning** the receiver that keeps it — a respawn would drop a healthy publisher's books and reference data every time a peer feed's subscription changed. `emit_trades` survives as the static capability claim, pinned against the ranking by a new agreement test in place of `at_most_one_trade_emitting_row_per_venue`. Within the owning row, a **per-venue tape leader** gates `Sticky` venues one level down: those arms share no trade-id space (one may stamp the `trade_id == 0` sentinel while its peer stamps a real venue id, a pair neither the sentinel latch nor the dedup window collapses), so the gate is id-independent: first arm to print leads, an arm the authority tracks displaces one it does not, the book-elected arm takes over once per election, and a silent incumbent yields after 5s so a dead trade stream never mutes the tape. Row ownership is likewise ordered liveness before rank, so a subscribed-but-dead row cannot hold the tape while its peer decodes prints and drops them. Together they preserve the invariant the sentinel bypass rests on: **at most one tape emitter per venue at any moment.** `dz_tape_owner_changes_total`, `dz_tape_arm_transfers_total` and `dz_tape_arm_dropped_total` report the moves and the drops. Every venue live today is `Coordinated`, so the arm gate changes nothing currently running. (#106)

### Added
- The two Lashay perps feed rows: `lashay-1` top of book on `233.84.178.3:7576/7577` and `lashay-2` market-by-price on `233.84.178.4:31000/41000/51000`, both claiming the tape and both `ArbitrationMode::Sticky`, one publisher block each (the two arms share a block and are told apart by source IP). Both groups are live and activated, so a host subscribed to either code begins ingesting on upgrade. A `code` that does not match its live group fails silently — no warning, no failed bind, just a permanently-zero `dz_receiver_up` — so both rows are pinned against the deployment by a test. The group codes are transcribed verbatim from what the DoubleZero ledger registers today; they are scheduled to be re-registered under new names, and the rows must be updated in the same change that lands the ledger rename, never before it. (#106)
- The incremental `book` product is arbitrated by the single-arm authority gate instead of passing through the arbiter undeduped: two arms' per-instrument delta series are unrelated by construction, so publishing both on one stream corrupts a consumer's book while every sequence check the producer ran still passes. It is gated in **both** arbitration modes on purpose — a `source_ts` tick can hold several deltas, so the quote floor's per-tick latch would interleave arms inside one logical event — and there is no mode branch. A change of serving arm (margin, silence, or the per-market health override) makes that market's next broadcast a re-baseline, a `clear` plus the new arm's complete current level set, emitted lazily on that arm's next *completed* logical event rather than as a venue-wide burst of clears; that is why the gate accumulates every eligible arm's book and not just the serving one. A re-baseline the gate cannot honestly complete — an arm that joined mid-stream holds only the levels that have moved since — degrades to a bare `clear` rather than claiming completeness, and the WS replay skips those markets for the same reason. Also wires the cross-arm trade matcher, the only producer of the matched-lead samples the speed re-election consumes (`--arb-match-window-secs`, `dz_arm_unmatched_trades_total`); it races **edge arms the authority already tracks** and nothing else, so the public backstop cannot win authority over a product it never publishes. Open question the matcher inherits: its key is the normalized `(venue, symbol, price, size, aggressor)`, and a wire `symbol` is a truncated 16-byte field, so on a sharded feed two colliding-symbol instruments can mis-pair systematically rather than merely losing a sample — `NormalizedTrade` carries no `instrument_id` to key on instead. Replays each market's accumulated book to a connecting WS client, and re-baselines a client that fell behind (an incremental product does not self-heal on the next message the way `quote`/`depth` do). Nothing exercises it in a running process yet: `MbpProcessor` emits `book` but no `FEEDS` row selects that kind, so behaviour is unchanged. (#105)
- `MbpProcessor` and the `FeedKind::MarketByPrice` receiver arm (mktdata + refdata + snapshot ports), turning decoded market-by-price frames into `PriceBook` state and the incremental `book` product. One book per `(publisher, channel, instrument)` — two arms mirror one feed on unrelated per-instrument delta sequences and one group can be sharded across channels, so nothing coarser identifies a book. Snapshot levels route by the open group per channel rather than by `snapshot_id` (monotonic per instrument, so two instruments routinely share a value), `EndOfSession` and a `Reset Count` change are scoped to the emitting arm and channel, and a cross-instrument delta-buffer budget drops the largest instrument's buffer rather than the process when a cold start floods it. Seven `dz_mbp_*` counters cover resets, buffer and level overflows, orphaned snapshot levels, duplicate deltas, crossed books and publisher action-vs-quantity divergence — see `docs/metrics.md`, and `docs/input-sources.md` for the per-receiver-task memory caps. No `FEEDS` row selects the kind, so no running process behaves differently. (#104)
- Single-arm arbitration for venues whose two redundant publishers stamp no comparable clock (`ingest::authority`, `ingest::arm_race`). Exactly one arm is authoritative and its stream is published verbatim. **Speed and silence are judged per arm, venue-wide** — latency is a property of an arm, so every sample from a source IP counts toward it whatever market carried it — while **health is the one per-market rule**, overriding the elected arm for a single market whose book is gapped and reverting when it recovers. Which arm is faster comes from `arm_race`, a cross-arm trade matcher keyed on content with a FIFO per signature (so identical repeats pair in order) that measures the two copies' arrival gap on our own receive clock; the venue's own timestamps are deliberately unused, because a publisher substitutes its own clock when the venue supplies none and an arm with no venue timestamp would look fastest by construction. Transfers need a median margin, a win rate and a sample floor to all hold (`--arb-*`). Nothing emits or consumes it yet — no processor wires a caller — so no running process behaves differently. (#98)
- The incremental `book` message (PROTOCOL.md, still v1 — `book` is additive and `depth` is now marked deprecated-and-removed-in-v2): a batch of absolute price-level changes for one instrument, keyed on `(venue, channel, instrument_id)`. A re-baseline is structurally a batch led by a `clear` action rather than a separate type or a boolean, because the reference consumer's book dispatcher branches on the action alone and would silently ignore a snapshot flag; `last` is mandatory on the final batch, including a lone clear, or a buffering consumer wedges. Ships with `BookAccumulator`, the replay state a connecting client is bootstrapped from — an incremental product's last batch means nothing to a client holding no book, so the bridge accumulates and materializes a clear plus the full level set on demand. Nothing emits `book` yet: no processor and no feed row, so no running process behaves differently. (#99)
- WebSocket subscription filters gain a `channel` dimension (the publisher's channel id) and a message-`type` dimension, so a consumer can take `book` without `quote`, or one channel's books without the rest. A message that carries no channel is excluded by an explicit `channel` filter — except `instrument`, since a client that cannot see a definition cannot scale the book it subscribed to. Both match paths (symbol-bearing and venue-level `status`) now route through the one `SubFilter::matches`, so a future dimension cannot silently exempt half the stream. Replay is also scoped: state is replayed on connect as before, and again on each `subscribe` for the filter just added, instead of only ever replaying every market at connect time. (#99)
- Each `Feed` now declares an `ArbitrationMode` (`Coordinated`/`Sticky`), carried into the arbiter as a per-venue map. Behaviour-neutral: every existing venue is `Coordinated` — today's latch-to-leader staleness floor — and an unregistered venue defaults to it. The seam exists for venues whose redundant publishers stamp no comparable venue clock, which cannot be arbitrated by a per-tick floor. (#94)
- `ingest::codec_mbp` — pure decoder for the Market-by-Price feed (frame magic `0x4442`): the frame
  walk, the five message types inherited from the byte-validated Top-of-Book layout, the three
  price-keyed payloads this feed defines (`LevelUpdate`, `BookClear`, `SnapshotLevel`), and the four
  it shares byte-for-byte with Market-by-Order (`Snapshot{Begin,End}`, `BatchBoundary`,
  `InstrumentReset`). Nothing ingests it yet — no `FEEDS` row, no processor. Two rules make it
  stricter than the sibling codecs, and they depend on each other: a frame declaring an
  unimplemented schema version is rejected whole, and within v1 a body length must equal the type's
  declared size exactly. `SnapshotBegin` is a prefix-superset of Market-by-Order's, so a lenient
  decode would read `depth_bound` from whatever follows the body — and the version gate is what
  keeps the length rule from silently rejecting a v2 frame whose bodies legally grew. Offsets are
  validated field-for-field against the Go reference decoder and against two committed real captures
  of the live publisher — a sharded multi-channel set and a dense single-channel set. Four message
  types appear in neither capture and stay offset-test-only; `tests/fixtures/PROVENANCE.md` records
  that and the publisher deviations the captures contain. (#95)
- `pcap2frames --protocol mbp`, so a Market-by-Price capture converts to fixtures the moment a host
  with tunnel access can take one. `--combined-with` is not implemented for it. (#95)
- `PriceBook` (`src/ingest/pricebook.rs`): the price-keyed L2 book and its snapshot+delta recovery
  state machine for the market-by-price feed — a sibling of the order-keyed `book.rs`, since the
  wire already carries absolute per-level quantities and has nothing to aggregate. Deltas apply only
  in unbroken per-instrument sequence, a gap buffers until a snapshot re-anchors, and buffered
  deltas past the snapshot's `anchor_seq` replay afterwards. Both the buffer and the level map are
  capped, so an unauthenticated forged stream cannot grow them without limit. Internal only — no
  codec, feed row or wire change yet, so no observable behaviour differs. (#96)
- Multi-publisher feeds: a `Feed` now lists N `FeedPublisher` port blocks and the reconciler runs
  one receiver per `(venue, protocol, publisher)`. All eleven live Hyperliquid publishers are
  ingested (previously only the 9201 block), so the arbiter's cross-publisher race, lead-time
  histograms and win-rate counters finally have a field of more than one. Publishers that share a
  single port block still work unchanged. (#88, #93)
- `--publisher-port <port>` (`DZ_PUBLISHER_PORTS`) narrows the publisher set per feed by **base
  port** (the market-data port of a publisher's block) — each publisher is a full receiver, and for
  Market-by-Order a full independent book, so an eleven-publisher venue is ~11x the ingest cost of
  one. Base ports are unique within a feed but not across feeds; pair with `--feed` to scope to one
  venue. (#88)
- `dz_receiver_up{venue,kind,publisher}` — per-publisher receiver health, where the `publisher`
  label value is the base port. (#88)

### Changed
- The wire `instrument` message carries the `(channel, instrument_id)` identity pair, so a consumer joins a `book` to its definition on the identity rather than on the colliding `symbol`. It is therefore now **channel-filterable** like every other channel-bearing message: the carve-out that sent every definition to a `{"channel":N}` client existed only because the message had no channel, and replay passes each definition's own channel so a channel-scoped client's bootstrap keeps its definitions. `status` remains the one venue-level carve-out. (#104)
- `dz_datagrams_received_total`, `dz_datagram_bytes_total`, `dz_socket_errors_total` and
  `dz_idle_rejoin_total` gained `kind` and `publisher` labels. Aggregating queries are unaffected;
  exact-match selectors on the old label set now match one series per publisher. (#88)
- `dz_feed_up` / `dz_feed_stale_ms` and the wire `status` message are venue-level **aggregates**: a
  venue reads down only when every one of its quote-bearing publishers has gone silent. Previously
  any single receiver could declare its whole venue down — including a depth-only Market-by-Order
  receiver, which no longer participates in the venue's quote health at all. A quote receiver that
  *stops* keeps the venue honest rather than letting a depth-only peer satisfy the aggregate. (#88)
- Multicast decode-error warnings are rate-limited to one line per 30s per receiver, carrying the
  suppressed count. Several port blocks are inferred rather than confirmed on-wire, and one that
  turns out to carry another protocol's traffic would otherwise log per datagram. (#88)
- Duplicate instrument definitions from mirrored publishers are collapsed before broadcast
  (`dz_instruments_dropped_total`), so reference-data traffic no longer scales with publisher count.
  The collapse is a rate limit, not a latch: unchanged content is re-announced every 15s, so a
  client that lost an `instrument` to backpressure still recovers it. (#88)
- Ingesting N mirrors re-baselines two existing series without renaming them:
  `dz_quotes_dropped_total`/`dz_depth_dropped_total` rise to ≈`(N-1)/N` of all samples (the
  cross-publisher collapse, not loss), and `dz_quote_lead_ns{winner="edge",loser="edge"}` becomes
  the dominant series and measures inter-mirror skew. See `docs/metrics.md`. (#88)
- **HFT hot-path optimization** — the ingest→broadcast→WebSocket path now does far less per-message
  work, with no change to the wire JSON field names or values:
  - **Broadcast backbone carries `Arc<FeedMessage>`** (`src/ingest/arbiter.rs`, `src/main.rs`): a
    per-subscriber delivery is now a reference-count bump instead of a deep clone of the message's
    owned `String`/`Vec` fields.
  - **WebSocket output serializes each message once**, not once per client
    (`src/sinks/ws.rs`): a single serializer task renders the JSON and re-broadcasts a shared,
    ready-to-write frame (`Arc<PreparedFrame>`); each client task only filters and writes a cheap
    `Utf8Bytes` clone. With no clients connected the serializer skips the work entirely. As a
    consequence, **`ws_send_ts_ns` is now a single serialization instant shared by all consumers of a
    message** (documented in PROTOCOL.md) rather than a per-connection send time — the accepted
    trade-off that enables serializing once.
  - **Allocation-free steady-state quote/trade path** (`src/model.rs`, `src/ingest/processor.rs`,
    `src/ingest/codec*.rs`, `src/ingest/arbiter.rs`): the wire `venue`/`symbol` are now `Arc<str>`
    (venues interned via `model::venue_arc`, symbols carried as `Arc<str>` on instrument definitions),
    and `NormalizedTrade.aggressor_side` is now the `Side` enum instead of an owned `String`, so
    building and dedup-keying a quote/trade no longer allocates.
  - **Pre-resolved arbiter metrics** (`src/ingest/arbiter.rs`): the emit path increments cached
    per-venue Prometheus counter/histogram handles instead of doing a label-map lookup per message,
    and the quote future-skew guard reuses the quote's own arrival timestamp instead of sampling the
    wall clock again.
  - **Zero-allocation receive loop** (`src/ingest/receiver.rs`): `recv_any` races a feed's 1–3
    sockets with a biased `select!` over the fixed port set instead of allocating a
    `Vec<Box<dyn Future>>` per datagram.
  - **Trimmed dependencies** (`Cargo.toml`): narrowed `tokio` from `features = ["full"]` to the used
    set; `serde` gains the `rc` feature for `Arc<str>` (de)serialization.
  - **Review follow-ups**: `venue_arc` (`src/model.rs`) is now backed by an `RwLock` so the
    steady-state hot path takes only a shared read lock (the write lock fires once per venue at
    warmup, not per message); the serializer task's broadcast-lag now increments a distinct
    `dz_ws_serializer_lagged_total` metric (`src/metrics.rs`) instead of sharing the per-client
    `dz_ws_client_lagged_total`, so a global serializer stall is no longer hidden behind a
    single-slow-client signal; and the arbiter's `(winner, loser)` lead-histogram index formula is
    now pinned by a unit test.

### Fixed
- Reference-data state is now tracked per publisher (source IP) rather than once per receiver,
  matching how sequence state is already keyed. `reset_count` is scoped to `(source_ip, group,
  port)`, so under a shared port block one publisher's restart previously cleared every publisher's
  instrument definitions — blanking the whole feed until the next reference-data burst, since all
  emission gates on a known definition. (#97)

### Added
- **Standalone `shred-proxy` binary** (new workspace member `shred-proxy/`): a lightweight service
  that joins the DoubleZero `edge-solana-*` shred multicast feeds, deduplicates, and forwards a
  single copy of each shred to a local UDP port — meant to run next to a validator without the full
  bridge. It reuses the bridge library's shred forwarder directly (`doublezero_edge_connect::shred`:
  receiver, dedup/sigverify, parser, multicast plumbing); the only new code is active-group
  detection via the kernel routing table (`ip route get`, instead of the `doublezero` CLI), the
  reconciler, and the CLI. Installs via a one-liner —
  `curl -fsSL https://get.doublezero.xyz/shred-proxy | bash` — which downloads a prebuilt static
  binary published by the new `release.shred-proxy.yml` workflow (tag `shred-proxy-v*`) and installs
  it as a systemd service. The repo is now a Cargo workspace; the bridge's Docker build is scoped to
  `-p doublezero-edge-connect` so the image is unchanged.
  - Review hardening: PR CI (`rust.yml`) now builds/lints/tests the whole workspace (`--workspace`),
    not just the root crate, so the member is actually compiled and tested on every PR. The
    installer self-elevates with `sudo` (so a plain `curl … | bash` works for a non-root user) and
    a re-run now `restart`s the running service onto the upgraded binary; `DZ_*` config is documented
    to be set after the pipe (on the `bash` invocation), and the unit/env/sysctl files are fetched
    from the resolved release tag rather than a moving `main`. The reconciler is fail-open: a
    transient `ip route get` failure keeps the current activation instead of tearing forwarding down
    (was fail-empty), and one long-lived signal listener avoids dropping a SIGTERM between polls. The
    release workflow now runs the tests before publishing, asserts the artifact is statically linked,
    and validates the (dispatch) tag is namespaced. The shipped `RUST_LOG` example no longer silences
    the forwarder's `doublezero_edge_connect` info logs. Additional hardening: the installer now
    errors (rather than silently skipping) when `SHA256SUMS` is missing unless
    `SHRED_PROXY_SKIP_CHECKSUM=1`; the systemd unit disables the start-rate limiter
    (`StartLimitIntervalSec=0`, `RestartSec=5`) so an unattended service never latches `failed`; and
    the binary bails at startup when `--iface` is an IP in detection mode (which would never match a
    routing-table interface name). Detection distinguishes a candidate the kernel has no route to (a
    clean `ip route get` non-zero exit → treated as inactive) from a genuine probe failure
    (spawn/decode error → keep current), so a routeless unsubscribed candidate on a host with no
    default route can't stall activation. Release publishes pin a dispatch-created tag to the built
    commit (`--target`), and the shipped env example documents `sigverify`/`DZ_RPC_URL`. The
    static-linkage assertion now also accepts `file`'s `static-pie linked` wording (a musl release
    build links as a PIE by default), so a genuinely static binary is no longer rejected at publish.
- **Per-tick win counters** `dz_quote_ticks_won_total{venue, publisher}` /
  `dz_depth_ticks_won_total{venue, publisher}` — the published win-rate primitive. Every
  `source_ts` tick counts exactly once, for the publisher class whose copy arrived first: a
  mirror's copy or the leader's later in-tick contents never re-count it, and a tick the public
  feed never delivers still counts for the edge (the walkover). `edge / sum` is the DZ win rate.
  The `dz_*_lead_ns` contest histograms are deliberately NOT a win rate — they sample only
  in-tick head-to-heads (at most one contest per tick, consumed by whichever follower arrives
  first, usually a mirror's sub-ms copy) and never count a loser that arrives after the floor
  advanced, so ratios built on them systematically understate the edge (docs/metrics.md
  "Published win rate" has the intended query). Quote `source_ts == 0` sentinels bypass the
  floor and count nothing; depth's `0` empty-anchor tick is real and counts.
- **Event-driven image rebuilds on doublezero base publish**: new
  `.github/workflows/release.docker.edge-connect.dispatch.yml` listens for a
  `repository_dispatch` (`doublezero-base-published`) from the upstream `malbeclabs/doublezero`
  repo and rebuilds the affected variant (moving + `:sha-` tags) within a minute, instead of
  waiting for the daily digest poll. Previously edge-connect only reacted to a new base image via
  `release.docker.edge-connect.poll.yml` (cron `23 5 * * *`), so a released base could lag up to
  24h — and only if the upstream base moving tag had actually moved. The poll is kept unchanged as
  a safety net. (Requires the upstream repo to fire the dispatch after publishing the base; see
  its `release.docker.client.yml`.) The dispatch's `client_payload` is passed through `env:` vars
  rather than inline `${{ }}` interpolation in the validate step, so a crafted payload can't break
  out of the shell quoting (defense-in-depth; the dispatch is already authenticated). A
  `concurrency` group (`edge-connect-publish-<env>`, `cancel-in-progress: false`) serializes
  rebuilds of the same variant so two dispatches can't push the same moving tag concurrently and
  invert ordering; the `::warning::` on an unrecognized env is kept static with the raw payload
  value logged on a separate plain line (so an embedded newline can't spoof a workflow command).
- **Depth-floor session-reset escape hatch** (#66): the MBO processor now clears the arbiter's
  latched depth floor on `EndOfSession` (whole venue) and `InstrumentReset` (that symbol), so a
  venue that restarts its event clock below the latched high-water no longer wedges depth
  permanently. `EndOfSession` is treated as a feed-level boundary: **every** publisher's book is
  dropped to `Recovering` (sequences, buffered deltas and the event clock all discarded), so a
  mirror publisher's old-session tail — or a boundary-loss resync stamping pre-session time —
  can't re-latch the floor at the old high-water and undo the clear. `InstrumentReset` likewise
  drops the resetting book's event clock, scopes its clear by the symbol the depth was actually
  emitted under (immune to an id→symbol remap across manifest epochs), and falls back to a
  venue-wide floor clear when neither that nor a current definition resolves. Both resets also
  purge the matching WS-replay `depth` entries, so a client connecting across the boundary is
  never replayed the ended session's final book — including a delisted instrument's phantom book,
  which nothing else would ever remove. Cleared entries are counted in
  `dz_depth_floor_resets_total{venue, reason}`. Note for consumers: the first `depth`
  after a reset/resync may carry the `source_ts_ns = 0` sentinel (per PROTOCOL.md, fall back to
  `kernel_rx_ts_ns`).
- **Subscription-driven feed activation** — the bridge now activates only the feeds this host is
  actually subscribed to, and adds/removes them at runtime as subscriptions change:
  - A single detector (`src/ingest/subscriptions.rs`) reads the host's subscriptions from
    `doublezero status --json` (`multicast_groups`, the `S:<code>` entries — the authoritative
    per-host view, unlike the network-wide `multicast group list`), resolving shred-group IPs via
    `multicast group list` (`src/shred/discovery.rs::parse_group_code_ips`). Each market-data feed
    now carries its group `code` (`src/ingest/feeds.rs`: `tiredsolid` = Hyperliquid, `scottsdale` =
    Phoenix).
  - A periodic reconciler (`src/ingest/reconcile.rs`) polls every `--subscription-refresh-secs`
    (`DZ_SUBSCRIPTION_REFRESH_SECS`, default 30) and diffs desired-vs-running, spawning/aborting
    market-data receivers, the WebSocket sink, and the shred forwarder. The **WebSocket sink comes
    up only when ≥1 market-data feed is subscribed** (so a shreds-only host serves no WS and can't
    collide with an existing `:8081` service); shred sources come from the subscribed
    `edge-solana-*` groups.
  - **Default-on with fail-open**: with no `doublezero` CLI (running from source) gating falls open
    to the static always-on set; a transient CLI failure keeps the current activations rather than
    flapping. `--subscription-gating-disable` (`DZ_SUBSCRIPTION_GATING_DISABLE`) forces the static
    model. A single feed dying no longer exits the process — the reconciler respawns it.
- Cross-source de-duplication **win metrics**, surfacing how the edge feed beats the
  original/public sources in both quantity and latency at each de-dup contest:
  - Quotes/trades (`src/ingest/arbiter.rs`): the staleness floor and windowed dedup now report
    the first cross-source follower of a `source_ts` tick / `trade_id` as a contest, recording
    `dz_quote_lead_ns` and `dz_trade_lead_ns` histograms (labelled by `winner` **and** `loser`,
    each `edge`/`public`; `_count` is the head-to-head win count, the buckets are the lead margin)
    plus `dz_trades_admitted_total` (the trade-side mirror of `dz_quotes_admitted_total`). The
    `loser` label keeps an edge-vs-edge mirror race (`{winner="edge",loser="edge"}`) out of the
    headline edge-vs-public margin (`{winner="edge",loser="public"}`) in multi-mirror deployments.
  - Shreds (`src/shred/`): each datagram now carries its source multicast group and a monotonic
    arrival timestamp, and the dedup window records the winning group, so a duplicate from a
    *different* group emits `dz_shred_wins_total{winner}` and `dz_shred_lead_ns{winner}` (how far
    the group that delivered first led by). A same-group retransmit stays a plain drop.
  - Recording is always on (only the `/metrics` exposer stays gated by `--metrics-bind`); lead
    times are clamped non-negative.
- Phoenix public-API trade feeder (`ingest::phoenix_feeder`), an off-by-default backstop for the edge
  Phoenix multicast TRADE stream (#53). It subscribes Phoenix's public `trades` channel per market,
  emits `NormalizedTrade`s through the shared arbiter as `Publisher::PublicWs` (deduped on
  `trade_id` = the public `tradeSequenceNumber`), and is enabled with `--phoenix-ws-input-markets`
  (`PHOENIX_WS_INPUT_MARKETS`, bare tickers e.g. `SOL,BTC`) / `--phoenix-ws-input-url`. Trades only —
  no quote backstop (the edge BBO is spline-blended; Phoenix's public book is resting-only). Validated
  against a live edge+public capture (2026-06-30): Phoenix uses the same bare symbol on both feeds
  (edge `instrument_id == public assetId`) and `trade_id == tradeSequenceNumber` on shared fills. No
  `FEEDS` row depends on it.
- Behavioral regression tests for the `scripts/connect*.sh` installers (`tests/scripts/`, bats-core),
  run in CI by a new `shell-tests` workflow. The suite drives the **byte-identical shipped scripts**
  end-to-end through a stub-first `PATH` (fake `docker`/`sudo`/`ss`/`curl`/...) and asserts on what
  each installer tried to do — no source guard or refactor added to the files served over the CDN. It
  iterates all three installers (so per-script drift is caught) and pins the #70 fix: with the WS port
  free the installer must survive preflight and reach `docker run` (the pre-#70 code aborted here under
  `set -e`); with the port busy, preflight must actually detect the conflict (warn) and, non-interactively,
  continue to `docker run` anyway.
- **Multi-publisher dedup for Market-by-Order `depth`** (#28, the MBO half of #3 — TOB shipped
  earlier). `MboProcessor` now reconstructs an **independent L3 book per `(publisher, instrument)`**
  (keyed on the datagram source IP), since two publishers' instance-scoped per-instrument delta
  sequences collide and cannot be merged into one book; `SnapshotOrder` (which carries only a
  `snapshot_id`, no instrument id) routes only to the originating publisher's building book. The
  resulting redundant `depth` is collapsed at the shared `Arbiter` by a **latch-to-leader staleness
  floor** keyed on `(venue, symbol)` with a content-inclusive `DepthId` (top-N levels at canonical
  `10^-8` fixed-point) — the same primitive as the quote floor, **but with no `source_ts == 0`
  bypass**: the two identical synced-but-empty book anchors two publishers emit at `source_ts == 0`
  deliberately collapse to one. The WS-replay depth map is written by the arbiter on the admit
  decision (the leader's broadcast book), not pre-floor. New metrics
  `dz_depth_admitted_total{venue,publisher}` (who is winning the book race), `dz_depth_dropped_total`,
  `dz_depth_future_rejected_total`, plus — mirroring the cross-source win metrics (#60) — the
  head-to-head lead-time histogram `dz_depth_lead_ns{venue,winner,loser}` (how far the leading
  publisher's book beat the follower's at a contested `source_ts` tick). Fixture-backed two-publisher MBO depth test over
  `mbo_btc_dual.combined.bin` (falsifiable: bypassing the floor re-emits the duplicate empty anchor).
- Installer one-liner (`scripts/connect*.sh`) now runs a **pre-flight access-pass check before
  installing anything**. Right after reading the access secret — and before installing Docker,
  pulling the image, or touching the host network — it verifies onchain that the configured identity
  has an access pass bound to this host's public IP **or** to `0.0.0.0` (the any-IP wildcard), and
  aborts with a clear, non-technical message — directing the operator to contact DoubleZero to
  arrange access, and printing the identity + public IP to share with support — if not. The check
  is pure host-side (no Docker, no CLI): it derives the identity from the
  DZ_-token/keypair, computes the access-pass PDA, and reads it over the DoubleZero ledger's public
  JSON-RPC via an embedded `python3` helper. It **degrades to a warning** (and continues, letting
  `doublezero connect` be the fallback) when the host's public IP can't be determined, the ledger
  RPC is unreachable, or `python3` is absent. New installer env vars: `DZ_CLIENT_IP` (override the
  detected public IP) and `DZ_LEDGER_RPC_URL` (override the ledger RPC).
- Shred forwarder opt-out kill switch, `--shred-forward-disable` (`DZ_SHRED_DISABLE`), **default
  off** so existing behaviour is unchanged. The forwarder is otherwise activate-on-discovery — it
  runs whenever `doublezero multicast group list` reports an `edge-solana-*` group, which a mainnet
  access pass always makes discoverable, and there was previously no way to turn it off short of
  abusing `--shred-code-prefix` to match nothing. A deployment with no consumer on the forward
  target (`127.0.0.1:20000` by default) thus silently burned CPU forwarding the full shred firehose
  into a dead port. When set, the flag forces the forwarder off regardless of discovery and skips
  the discovery shell-out to the `doublezero` CLI entirely. The activation decision is now a single
  unit-tested contract, `shred::decide_activation(disabled, source_count) -> ShredActivation`
  (`Disabled`/`NoSources`/`Run`), that `main` matches on to drive both the spawn and its
  operator-facing log line. Dockerfile one-liner examples document the opt-out.
- **Prometheus metrics endpoint** (`--metrics-bind` / `METRICS_BIND`, **off by default**). When a
  bind address is given (e.g. `127.0.0.1:9090`) the bridge serves the Prometheus text format at
  `GET /metrics` (plus a `GET /` / `GET /healthz` liveness probe) over a hand-rolled minimal HTTP
  handler — no HTTP framework, no TLS (terminate at a reverse proxy if exposed). Metrics are
  recorded regardless of whether the endpoint is enabled. Coverage spans the whole pipeline:
  ingest reception (`dz_datagrams_received_total`, `dz_datagram_bytes_total`,
  `dz_socket_errors_total`, `dz_idle_rejoin_total`, `dz_feed_up`, `dz_feed_stale_ms`,
  `dz_seq_events_total`), the arbiter emit stage (`dz_emit_total`, `dz_quotes_dropped_total`,
  `dz_trades_dropped_total`, `dz_quotes_future_rejected_total`, `dz_quotes_no_source_ts_total`),
  `dz_quotes_admitted_total` (attributing each admitted quote to its winning `publisher`,
  `edge`/`public` — the direct signal of the public backstop filling an edge gap)), the WebSocket
  sink (`dz_ws_clients`, `dz_ws_connections_total`, `dz_ws_messages_sent_total`,
  `dz_ws_bytes_sent_total`, `dz_ws_client_lagged_total`, `dz_ws_inbound_total`,
  `dz_ws_rate_limited_total`, `dz_ws_idle_timeout_total`), the public WS input feeder
  (`dz_ws_feeder_up`, `dz_ws_feeder_reconnects_total`, `dz_ws_feeder_decode_errors_total`,
  `dz_ws_feeder_messages_total`), and the shred forwarder (`dz_shred_*` —
  datagrams and bytes received per group, processed/parsed/unparsed/forwarded/dropped, verify-ok,
  no-leader, dedup tracked slots, per-destination sends and bytes sent), plus the standard Linux
  process metrics. Both the ingest and client-output paths expose message **and** byte counters
  (UDP and WebSocket). The feed-health gauges (`dz_feed_up`/`dz_feed_stale_ms`) are initialized to
  their healthy state at startup, so a feed that never goes down still exposes a `dz_feed_up{venue}`
  series for `dz_feed_up == 0` alerting. The `/metrics` HTTP server is GET-only with per-connection
  read/write timeouts and a concurrency cap. Labels are bounded (`venue`/`group`/`dest`/`publisher`
  and small fixed enums; no per-symbol labels).
- Two-publisher **Market-by-Order** depth-dedup golden `tests/fixtures/mbo_btc_dual.combined.bin` plus
  the tooling to mint it. `examples/pcap2frames.rs` `--combined-with` now supports `--protocol mbo`
  (three port roles — refdata/snapshot/mktdata — vs TOB's two, with per-publisher `SnapshotOrder`
  routing); it keeps refdata across the whole scan while windowing snapshot+deltas to `[--from,--to]`
  (so the slow-round-robin instrument definition still resolves precision), reports a window-coherence
  summary, and adds `--empty-anchor`, which synthesizes a per-publisher empty-book snapshot anchor
  (real per-instrument snapshots ride a ~30 s, per-publisher-phased round-robin and can't be captured
  coherently in a small window — see `tests/fixtures/PROVENANCE.md`).
- Shred forwarder deduplication is now selected by a single mode flag, `--shred-dedup-mode`
  (`DZ_SHRED_DEDUP_MODE`), and **defaults to dedup-only** — the forwarder now forwards exactly one
  copy of each shred out of the box, collapsing the multicast-overlap duplicates DoubleZero delivers
  across its several `edge-solana-*` groups (previously the default forwarded every copy). The three
  modes are `dedup` (default; `(slot, index, type)` dedup, **no** signature verification or RPC),
  `sigverify` (dedup + ed25519 leader-signature check, requires `--shred-rpc-url`), and `none`
  (forward every datagram). The mode is the only method selector: an RPC URL set in a non-sigverify
  mode is ignored (logged), never auto-promoting to sigverify. Replaces the boolean `--shred-dedup`
  (`DZ_SHRED_DEDUP`) flag added earlier in this unreleased cycle. `dedup`/`sigverify` share the same
  bounded `DedupWindow` (`--shred-dedup-window-slots`). ⚠️ Dedup still depends on the unvalidated
  agave shred offsets, so a misparse could over- or under-deduplicate — confirm against a captured
  frame before relying on it. The `curl … | bash` installer scripts (`scripts/connect*.sh`) now
  relay the `DZ_SHRED_*` env vars into the container, so the shred forwarder can be tuned from the
  one-liner (e.g. `DZ_SHRED_DEDUP_MODE=sigverify DZ_SHRED_RPC_URL=… curl … | bash`).
- Explicit duplicate-packet de-duplication tests across all three dedup paths. Decoded-message unit
  tests in `arbiter.rs` (an identical quote from the same source, the same BBO mirrored by two
  multicast publishers, and an identical trade all collapse to one emission); raw-packet replay
  tests in `tests/dedup.rs` that deliver every mktdata datagram twice — byte-for-byte and from a
  second publisher IP — and assert the emitted quote/trade set is unchanged; and a shred-level
  `same_datagram_twice_forwards_once` proving the second copy is dropped without re-verifying.
- Real two-sided Market-by-Order E2E fixture (#5): `mbo_{refdata,snapshot,mktdata}.bin` are now a
  live TYO recorder capture (publisher 148.51.123.3, BTC) of a complete 44,598-order snapshot
  (28,345 bids + 16,253 asks) plus contiguous post-anchor deltas, replacing the hand-crafted
  empty-anchor anchor from PR #2. `mbo_single_publisher_depth_contract` now asserts an active,
  unconditional two-sided crossed-book check (`best_bid < best_ask`). The `pcap2frames` example
  gained `--mbo-minimal` (with `--mbo-max-deltas`) to extract this minimal fixture in one command:
  the first complete snapshot group + capped post-anchor deltas + a minimal refdata. See
  `tests/fixtures/PROVENANCE.md`.
- Shred forwarder sigverify + dedup (#25): when `--shred-rpc-url` (`DZ_SHRED_RPC_URL`) is
  set, the forwarder forwards exactly **one valid copy** of each shred. A bounded,
  prefer-valid dedup window keyed by `(slot, index, type)` (`--shred-dedup-window-slots`,
  default `512`) drops duplicates of an already-forwarded copy without a signature check;
  the first copy of a key is ed25519-verified against its slot leader (fetched per epoch
  via `getLeaderSchedule`/`getEpochInfo`) over the legacy payload or recomputed merkle
  root; an invalid copy is dropped but leaves the key open so a later valid copy can still
  win. A slot whose leader isn't known yet fails open (forwarded, not deduped). Without
  `--shred-rpc-url`, behaviour is unchanged (forward every datagram). New deps:
  `ed25519-dalek`, `sha2`, `bs58`, `reqwest` (rustls). ⚠️ The shred/merkle byte offsets are
  transcribed from the agave layout and are **not** validated against a live `edge-solana-*`
  hexdump (same status as the repo's unvalidated sibling codecs); the forwarder logs a
  one-time warning and a periodic verify tally so a misparse is visible.
- Shred forwarder (`src/shred/`): joins the DoubleZero `edge-solana-*` shred multicast
  feeds, combines them, and fans each datagram out to one or more local UDP destinations
  (no dedup / no signature verification yet). Sources are discovered via `doublezero
  multicast group list` (prefix-matched, default `edge-solana-`) or overridden with
  repeatable `--shred-source GROUP:PORT`. Activates on discovery; configured with
  `--shred-code-prefix`, `--shred-port` (default `7733`), `--shred-forward` (default
  `127.0.0.1:20000`), reusing `--iface`/`--recv-buf`. Reuses the `ingest::receiver` socket
  plumbing (`bind_multicast`, `wait_for_interface_ip`) — now `pub` (#24).
  Discovery deserializes `doublezero multicast group list --json-compact` (the machine-readable
  contract) instead of scraping the human table, and filters on `status == activated`. The
  forwarder uses one `connect`ed send socket per destination so a down destination's async ICMP
  error can't drop a datagram bound for a healthy one. A shred-side failure is logged and
  isolated — it never takes the market-data bridge down. Datagrams that fill the recv buffer
  (likely truncated, no `MSG_TRUNC`) are dropped rather than forwarded corrupt (#24).
- Hyperliquid **public** WebSocket input feeder (`src/ingest/ws_feeder.rs`), a second ingest source
  that backstops the DZ Edge multicast feed (#8). It connects to `wss://api.hyperliquid.xyz/ws` over
  TLS, subscribes `bbo` + `trades` per configured coin on one connection, decodes the HL JSON into the
  same `FeedMessage`s the multicast pipeline produces, and emits them through the shared arbiter as a
  distinct `Publisher::PublicWs`. Because it shares the per-`(venue, symbol)` latch-to-leader floor with
  the edge feed, the backstop falls out with **no health check**: the edge wins every tick in steady
  state (the public copy loses the race and is dropped as a no-op), and when the edge gaps the public
  copy is the first to cross the floor and fills in. The public block time (ms) is scaled to ns so both
  sources share the same canonical `source_ts`; trades dedup on `tid` (the edge feed's `trade_id`).
  **Off by default**, enabled with a non-empty `--ws-input-coins` (env `WS_INPUT_COINS`);
  `--ws-input-url` (env `WS_INPUT_URL`) overrides the endpoint. Failure-isolated (its own task with
  reconnect + exponential backoff; decode/socket errors are logged and swallowed), and each public
  quote/trade is gated on its `(venue, symbol)` instrument being known (precision before price). A mock
  HL WS input harness drives two new E2E cases (edge-leads-in-steady-state, edge-gap→public-fills-in).
  The feeder adds no new WebSocket output fields of its own; it populates the same `bid_n`/`ask_n`
  (from the public `bbo` level's `n`) the edge feed serves.
  - Reconnect backoff resets to the floor only after a session stays up past a minimum duration, so a
    connect-then-immediate-drop loop keeps escalating instead of hammering the public endpoint.
  - Shared mutexes (`InstrumentSnapshot`/`DepthSnapshot`/arbiter) lock via a poison-recovering helper
    (`model::lock`), so an unrelated panic in one ingest task can't cascade into the others.
- Cross-source quote identity is the canonical `bbo_hash` (`StableBBOHash`): bid/ask price + size at
  the `10^-8` scale plus `bid_n`/`ask_n`. Computing it at a fixed scale (not raw `f64` bits) collapses
  the edge's `raw * 10^exp` and the public feed's parsed float for the same economic price onto one
  identity, so a cross-source copy dedups. The arbiter also drops a quote whose `source_ts` is
  implausibly far in the future before it can advance the shared floor — one bad/hostile public
  timestamp would otherwise latch the floor ahead and drop every real edge quote as stale until restart.
- Real Hyperliquid Market-by-Order (MBO) feed ingestion: a confirmed `FEEDS` row
  (`233.84.178.15`, ports `10201`/`10202`/`10203`, depth-only) re-served as full-state
  `depth`. `--feed <venue>` now selects every protocol feed for that venue.
- Per-feed `emit_trades` flag so a venue carried by both Top-of-Book and Market-by-Order
  does not double-emit `trade` messages (Top-of-Book owns trades; MBO is depth-only).
- End-to-end test suite that drives the release binary over loopback multicast and asserts
  the WebSocket output contract, with deduplication-oracle assertions for future work.
- `examples/pcap2frames.rs` dev tool: converts a multicast pcap into the test harness's
  frame-log fixtures, demultiplexing one publisher by source IP and filtering by protocol
  (Top-of-Book/Market-by-Order) and symbol. Decoding each frame through the real codecs
  doubles as live-feed validation of the codec byte offsets.
- Live two-publisher Top-of-Book BTC fixtures (`tests/fixtures/tob_btc_pub{A,B}.*`) for the
  upcoming multi-publisher deduplication work; provenance and regeneration in
  `tests/fixtures/PROVENANCE.md`.
- `pcap2frames --combined-with <ip>`: emits one capture-ordered, source-IP-and-role-tagged stream
  of two publishers (`tob_btc_dual.combined.bin`), preserving the real interleaving the
  multi-publisher dedup must collapse.
- `pcap2frames --symbol` is now repeatable (and the combined report tallies kept quote messages
  per `(symbol, publisher)`), enabling a multi-symbol two-publisher fixture
  (`tob_multi_dual.combined.bin`: BTC busy / SOL medium / DOGE quiet) that exercises the dedup's
  per-`(venue, symbol)` independent windows.
- Multi-publisher Top-of-Book deduplication: when several independent publishers mirror one feed
  onto a multicast group, the bridge merges them into one clean stream. Datagrams are demultiplexed
  by source IP (`FrameCtx.publisher`); the frame-sequence tracker is per-publisher so a slower
  publisher's frames aren't dropped before dedup. Quotes dedup on a per-`(venue, instrument)`
  `source_ts` latch-to-leader floor keyed on the **canonical BBO identity** (the components of the
  spec's `bbo_hash`: bid/ask price + size + the `bid_n`/`ask_n` source counts): within one `source_ts`
  tick (the venue stamps coarsely, so a tick holds a whole sub-sequence of real top-of-book changes)
  it emits only the *leader* — the first publisher to open the tick — and drops other publishers'
  samples at that `source_ts`. This is because arrival order across publishers is corrupted by
  per-publisher network delay (the `hl-bbo-feed-race` board shows inter-feed skew over 100 ms), so
  interleaving two sources inside one tick can serve a stale sample as the freshest — on a falling
  price, a slower publisher's older, higher sample landing last would read as a phantom uptick. The
  leader is re-selected each new tick, so the lowest-delay publisher for a given moment naturally wins.
  A strictly-older BBO (stale laggard) and the leader's exact `(source_ts, content)` repeats are
  dropped too, so the emitted `source_ts` is non-decreasing (not strictly increasing) per instrument
  and within a tick the series is one publisher's coherent, in-order subsequence. `source_ts == 0`
  (the "not available" sentinel) bypasses the floor (always forwarded, never latched) so a feed that
  stops stamping time can't wedge non-leaders, and the per-tick content set is capacity-bounded so a
  stalled `source_ts` can't grow it without limit. The dedup key is allocation-free on the hot path
  (`(&'static venue, instrument_id)`). Trades, being point-in-time events, dedup on a windowed
  `(venue, instrument, trade_id)` identity so every distinct print is kept. (Market-by-Order depth
  dedup is tracked separately.)
- Top-of-Book `quote` messages now carry `bid_n`/`ask_n` (the edge-feed-spec "Bid/Ask Source Count":
  orders/sources at the best bid/ask, `0` if unavailable). They were decoded-and-discarded before;
  now decoded, re-served on the WebSocket (additive, forward-compatible — see PROTOCOL.md), and part
  of the canonical BBO identity, so a count-only change at an unchanged price/size is a distinct quote.

### Changed
- Installer (`scripts/connect*.sh`) now detects an **existing edge-connect instance** before
  installing: if a container named `$DZ_NAME` (default `doublezero-edge-connect`) already exists on
  the host, the installer warns (naming the instance's env/image, since all three installers share
  `$DZ_NAME` — so e.g. the testnet installer flags a live *mainnet* container) and prompts to
  **reinstall or cancel** instead of silently colliding with the live tunnel/ports. On reinstall it
  prints "Uninstalling existing instance..." and, for a running instance, tears it down **gracefully
  via `docker stop`** — the container entrypoint's SIGTERM trap runs a bounded `doublezero
  disconnect` (releasing the GRE tunnel/routes/on-chain session) before doublezerod is killed —
  rather than a raw, unbounded `docker exec … disconnect`; the stop is `timeout`-guarded so a wedged
  or restarting container can't hang the installer. After removal it verifies the `doublezero1`
  tunnel interface is actually gone from the host netns and warns loudly if it lingers (orphaned
  session). Interactively, declining aborts and leaves the instance untouched; non-interactively
  (`DZ_ASSUME_YES=1`, or no usable TTY) it reinstalls, preserving the previous silent-reinstall
  behaviour for automation. TTY detection probes an actual `/dev/tty` open rather than trusting
  `-r`, so a headless run with no controlling terminal is classified correctly. The env/image
  labelling is best-effort — a `docker inspect` that fails mid-teardown (container removed between
  detection and inspect, or a daemon blip) no longer aborts the installer under `set -o pipefail`.
  Applied identically to all three installers; covered by `tests/scripts/reinstall_existing.bats`.
- `dz_depth_dropped_total` now carries a `publisher` label (the dropped copy's source class),
  symmetric with `dz_depth_admitted_total`, so a lagging publisher losing the book race is
  directly visible (#66). This changes the label set of an existing series — exact-label matchers
  and recording rules on this metric need updating (`sum by (venue)` aggregations are unaffected).
- `QuoteId`/`DepthId` canonical fixed-point widened `i64` → `i128`: an `f64→i64` cast saturates at
  ~9.2e10 (at the `10^-8` scale), which could collapse two distinct huge quantities into one
  identity and wrongly dedup the second (#66).
- Installer (`scripts/connect*.sh`) usability fixes after review:
  - **WebSocket port preflight**: before starting the container the installer checks whether the WS
    port is already bound on the host and, interactively, offers to pick another port, disable the
    sink, or continue (non-interactively it warns and continues — the bridge then runs without the
    sink, tunnel unaffected). The interactive prompt now explains what the WS sink is (an *optional*
    local WebSocket a shred → jito-shredstream setup does not use), spells out each option's
    consequence, and **defaults to disable** rather than continue — the port is already known taken,
    so continuing was the one choice guaranteed to fail to bind.
  - **`WS_BIND=""` now works through the one-liner**: `WS_BIND` is forwarded whenever it is *set*,
    including set-but-empty, so the WS sink can be disabled straight from the pipe (previously only
    non-empty values were relayed, forcing a hand-written `docker run`).
  - **Firewall guidance for default-deny-incoming hosts**: the ufw/firewalld hints now note that
    allowing GRE + UDP 44880 admits only the *outer* encapsulated packets — the decapsulated inner
    multicast re-traverses `INPUT` on the tunnel interface (`doublezero1`) and must be allowed too
    (`sudo ufw allow in on doublezero1`). Mirrored in `README.md` / `scripts/README.md`.
- Public-feeder transport scaffolding extracted into a venue-generic `ingest::public_feeder`
  (a `PublicVenue` trait + one reconnecting run loop + shared decode helpers); Hyperliquid
  (`ingest::ws_feeder`) is the first implementor (#53). The four `dz_ws_feeder_*` metrics are now
  labelled by `venue` so a second venue's series don't collide.
- Container logs can no longer fill the host disk, and the default is quieter:
  - The installer's `docker run` (`scripts/connect.sh`) now pins the `json-file` log driver with
    `max-size=20m` + `max-file=3`, capping the long-lived container's on-disk log at ~60 MB
    (previously unbounded — the default driver rotated nothing). Documented for by-hand runs in
    `docs/self-hosting.md`.
  - The default log filter (when `RUST_LOG` is unset) is now `warn,doublezero_edge_connect=info`
    instead of a blanket `info`: the bridge's own startup/operational breadcrumbs stay at `info`
    while noisy dependency chatter is held to `warn`. Set `RUST_LOG=debug` for verbose output.
    Applied in both `src/main.rs` and the image `ENV`.
- `codec_mbo` field offsets validated and the blanket "draft" caveat lifted (#4, follow-up to #2),
  with the per-type oracle strength documented honestly rather than claimed uniform:
  - **Shared-with-TOB** layouts (frame/message headers, `InstrumentDefinition`, `Trade`,
    `ManifestSummary`, type tags) reuse the byte-validated TOB `codec.rs`; a new cross-codec test
    (`tob_shared_layouts_decode_identically`) decodes the same bytes through both codecs and asserts
    equal fields, so the sharing is self-enforcing.
  - **Real publisher capture** backs `Order{Add,Cancel,Execute}`, `BatchBoundary`, the full
    `Snapshot{Begin,Order,End}` group, and the shared `InstrumentDefinition`/`ManifestSummary` via a
    new real-frame decode test (`tests/codec_mbo_fixtures.rs`) over the two-sided TYO recorder
    fixtures (#36). The snapshot is BTC's complete 44,598-order book, so `SnapshotOrder` is
    well-covered, and the test asserts `total_orders == decoded order count` as a cross-field check.
  - **Offset-test-only** (no committed fixture; pinned by the offset-independent unit tests, confirm
    against a live frame before a live MBO feed): `InstrumentReset`, `Heartbeat`, `EndOfSession`.
  No offset discrepancies found — the side-mapping bug fixed in #2 was the only one. The "size 20 vs
  fields-to-24" `ManifestSummary` suspicion was a non-issue: the body is 20 bytes (on-wire 24),
  identical to TOB, and no size-20 constant exists in code.
- README refocused on the **operator**: it now leads with what the bridge does, the install
  one-liner (`curl -fsSL https://get.doublezero.xyz/connect | bash`, plus the testnet/devnet
  variants), and how to configure/override it via environment variables before the pipe. The
  detailed per-feature reference (self-hosting/from-source + Docker, output sinks, input sources,
  Solana shred forwarding) moved into a new `docs/` directory the README links out to. Removed the
  misleading `https://doublezero.xyz/install` command that contradicted the canonical
  `get.doublezero.xyz/connect` one-liner.
- Shred sigverify mode (`--shred-dedup-mode sigverify`) now **prefetches the next epoch's leader
  schedule** and **fails closed** on an unknown leader. The leader cache holds two epochs (current + next), fetched
  by explicit slot so the result is independent of rollover timing, eliminating the routine
  ~30s-per-epoch gap where new-epoch slots had no schedule. With prefetch in place, a slot whose
  leader is unknown is now **dropped** rather than forwarded unverified — sigverify forwards only
  what it can verify. Because the full current epoch is always cached, a transient RPC glitch never
  blacks out the feed; an unknown leader now means cold start, an RPC outage past the ~epoch
  prefetch lead, or a garbled schedule, and is surfaced as a `no_leader` counter in the periodic
  tally. (Forward-when-unverified is exactly dedup-only mode; sigverify no longer degrades into it.)
  Removes the now-unused `leader_known` fail-open path from `DedupWindow::decide`.
- Shred dedup-only mode (`--shred-dedup-mode dedup`) now keys its dedup window on `(slot, index, type,
  content-fingerprint)` instead of `(slot, index, type)`, so it collapses copies that match over the
  signed content. A shred sharing `(slot, index, type)` but carrying different signed content
  (equivocation, corruption, a forged first-arriver) now still forwards rather than being silently
  dropped onto the first copy — loss-averse, since without sigverify the forwarder can't tell which
  copy is valid. The fingerprint excludes the trailing 64-byte **retransmitter signature** of
  resigned merkle shreds (variants `0x70`/`0xb0`), which is rewritten per turbine path: cross-group
  copies of the same shred differ *only* there, so hashing the whole datagram would give each its own
  key and dedup none of them. Excluding that tail needs only the already-decoded `resigned` flag and
  the datagram length, not the unvalidated merkle offsets. The fingerprint is a deterministic hash
  computed only in dedup-only mode; sigverify mode is unchanged (keyed content-agnostically, since
  the signature picks the valid winner). Adds `examples/bench_dedup_vs_sigverify.rs`, which measures
  the fingerprint's marginal cost at ~135× cheaper than an ed25519 verify.
- The quote latch-to-leader floor and the windowed trade dedup moved out of `TobProcessor` into a
  shared pre-broadcast `Arbiter` (`src/ingest/arbiter.rs`) that owns the broadcast `Sender` and
  exposes one `emit(msg, publisher)` entry point (#8). Every ingest source — each multicast receiver
  and the new public WS feeder — funnels through one `Arc<Mutex<Arbiter>>`, so they all race on the
  same per-`(venue, symbol)` floor instead of each owning a private one. A `Publisher { Edge(IpAddr),
  PublicWs }` enum is the floor's leader identity. Behavior-preserving for the edge path (the
  two-publisher and single-publisher counts are unchanged); the refactor itself adds no output fields.
- Feed registry is keyed by `(venue, kind)` instead of `venue`, so one venue can carry
  multiple protocol feeds.
- Bumped dependencies from the open Dependabot PRs: `tokio-tungstenite`
  0.23 → 0.29, `socket2` 0.5 → 0.6, `nix` 0.29 → 0.31, and the GitHub Actions
  `actions/checkout` (v6.0.3), `docker/login-action` (v4.2.0),
  `docker/setup-buildx-action` (v4.1.0), `docker/build-push-action` (v7.2.0),
  and `aws-actions/configure-aws-credentials` (v6.2.0). The `tokio-tungstenite`
  0.29 upgrade switched `Message::Text`/`Ping`/`Pong` payloads to
  `Utf8Bytes`/`Bytes`, updated in `src/sinks/ws.rs`.
- Graceful container shutdown runs `doublezero disconnect` to free the access-pass session,
  but only on an operator `docker stop` (TERM/INT) and only when a tunnel is actually up —
  so a bridge crash under `--restart unless-stopped` no longer releases the session. The
  disconnect is wrapped in a `timeout` so a wedged daemon can't consume the whole stop budget.
- Exposed the ingest pipeline, wire model, and sinks as a library (`src/lib.rs`); the binary
  (`src/main.rs`) is now a thin wrapper, so dev tooling and tests can reuse the codecs.

### Fixed
- Trades stamped `trade_id == 0` (the "no venue trade id" sentinel, emitted by FIX-sourced publishers) now bypass the cross-source dedup window instead of being keyed on it. Previously the second and every later such print was discarded as a same-publisher duplicate and `0` never aged out of the window, collapsing the tape to a single print per `(venue, symbol)` for the process's lifetime. A bypassed sentinel has no window to collapse against, so the bypass holds only while one publisher owns a venue's tape: `dz_trades_no_id_total{venue}` counts the sentinel prints and `dz_trades_no_id_conflict_total{venue}` reports a second *concurrent* publisher emitting one (a double-printed tape), which no feed does today. Inheriting a tape that has gone quiet for 5s is a failover, not a conflict, so the counter does not latch on across a legitimate ownership change. (#94)
- Installer daemon head start bumped from 15s to 30s before `doublezero connect multicast`, so a
  cold daemon finishes device probing and no longer races the connect on slower hosts
  (`scripts/connect*.sh`).
- `select_feeds` now dedups repeated `--feed` names on `(venue, kind)`, so `--feed Hyperliquid
  --feed Hyperliquid` spawns the same receivers as `--feed Hyperliquid` (previously each match was
  spawned twice, contending for the same multicast group/port) (`src/main.rs`, #9).
- A taken WebSocket-sink port no longer takes the whole bridge down. A bind failure on `--ws-bind`
  (e.g. the default `0.0.0.0:8081` colliding with a pre-existing `127.0.0.1:8081` listener) was
  fatal: the process exited, the container's `--restart unless-stopped` restarted it, doublezerod
  and the DoubleZero tunnel came down with it, and — since `doublezero connect multicast` runs only
  once from the installer — the tunnel never re-established (status stuck `disconnected`, the real
  cause buried in the restart loop). The listener is now bound eagerly (`sinks::ws::bind`, split
  from `serve`) and a bind failure is logged and skipped: the bridge runs without the sink while
  the tunnel and shred forwarding keep going (`src/main.rs`, `src/sinks/ws.rs`).
- Installer pre-flight access-pass check (`scripts/connect*.sh`) hardened after review:
  - A confirmed miss (an identity with no pass for the host IP or `0.0.0.0`) now only hard-aborts
    when the public IP was **explicitly supplied** via `DZ_CLIENT_IP`; when the IP was only
    **auto-detected** (best-effort egress lookup, which can differ from the bound IP behind
    NAT/CGNAT/multi-homed hosts) it now **warns and continues** instead of aborting a
    legitimately-provisioned operator, leaving `doublezero connect` as the real check.
  - Reading the keypair file for the check no longer runs under `set -e`, so a root-owned `0600`
    key (readable by the root Docker mount but not by the invoking user) degrades to a warning
    instead of silently aborting the whole installer.
  - The detected/supplied public IP is now strictly validated as a dotted quad (round-tripped
    through `inet_ntoa(inet_aton(ip))`), rejecting lenient `inet_aton` forms (`1.2.3`, trailing
    junk) that could yield a confident-but-wrong verdict; a malformed IP is treated as unknown.
  - An unreadable/invalid keypair (not a 64-int JSON array) now produces a distinct "could not
    read or parse the keypair" warning instead of misattributing the failure to the ledger RPC.
  - The ledger RPC URL is asserted to be `http(s)://` before use, so a `DZ_LEDGER_RPC_URL` with a
    `file://` (or other) scheme can't be dereferenced.
- Installer no longer advertises a WebSocket URL the bridge won't serve (`scripts/connect*.sh`). The
  final status print gated only on `WS_BIND=""`, so a shreds-only (or not-yet-subscribed) host was
  still told `ws://<host>:8081` even though the subscription reconciler activates the WS sink only
  when ≥1 market-data feed is subscribed — nothing was listening there. The installer now observes
  the bridge's own decisions (its `activating WebSocket sink` / `activating shred forwarder` logs,
  emitted at the default `warn,doublezero_edge_connect=info` level, plus a direct WS-port probe),
  waiting up to one reconcile interval (`DZ_SUBSCRIPTION_REFRESH_SECS`, default 30s) and exiting as
  soon as either activates, then reports serving-quotes / forwarding-shreds / nothing-subscribed-yet
  accordingly rather than asserting an unbound socket.
- **MBO depth was silently broken on the live feed.** The live HL publisher emits MBO
  `ManifestSummary` with `Valid=0` (the same quirk `TobProcessor` already overrides); `MboProcessor`
  honored it, which clears all instrument definitions, so precision never resolved and the feed emitted
  zero `depth`. `MboProcessor` now overrides `Valid=0`→true like TOB (logged once, `REVISIT`).
  Regression test: `mbo_manifest_valid_zero_is_overridden_so_depth_flows`. The e2e MBO test missed this
  because its vendored golden carries `Valid=1`; the bug surfaced minting a real-capture MBO fixture.
- Docker release workflow could not push to GHCR: the reusable
  `release.docker.edge-connect.build` workflow declared a top-level `permissions:
  contents: read` block, which intersects with (and so can only narrow) the caller's
  grant — silently dropping the `packages: write` that the publish/rebuild jobs grant, so
  the push was denied. Removed the block entirely so the `workflow_call`-only workflow
  inherits each caller's permissions (publish/rebuild → write, smoke → read), which is the
  only form that both authorizes the push and keeps smoke (PR) builds push-gated.
- Corrected inverted Market-by-Order order-book side constants (`0 = Bid`, `1 = Ask` per
  the edge-feed-spec); bids and asks in `depth` were previously swapped.
- Market-by-Order manifest `Valid=0` workaround (#5): the live HL MBO publisher emits
  `ManifestSummary` with `Valid=0`, same as the Top-of-Book publisher. `MboProcessor` passed
  `m.valid` straight through, so the manifest was rejected, no instrument definition ever
  resolved, and `depth` was silently never emitted. It now overrides to valid (logged once),
  mirroring `TobProcessor`. Surfaced by the real two-sided MBO fixture below.
- `MboProcessor` no longer re-broadcasts a duplicate full-state `depth` when a book change
  leaves the published top-N unchanged (deep-book churn): it now emits only when the top-N
  actually changes, matching the documented contract and avoiding redundant WS traffic.
- Warn instead of silently clobbering when two feeds for the same `(venue, symbol)` publish
  instrument definitions with different price/quantity exponents.

### Security
- Hardened the codec frame walker against out-of-bounds reads: the per-message body decoders
  now read every field through bounds-checked little-endian readers, so a truncated or
  malformed datagram (a runt message that under-declares its length) decodes to
  `Message::Other` instead of panicking the receiver task — which previously propagated out
  of `run_feed` and exited the whole process (a single crafted datagram could take the bridge
  down for every venue and WS consumer). Applies to all three sibling codecs (TOB / Midpoint /
  Market-by-Order).
- Bounded the per-publisher frame-sequence map (`TobProcessor`) to `MAX_PUBLISHERS` (256) with
  least-recently-inserted eviction. The map is keyed on the datagram source IP, which is
  unauthenticated and spoofable, so without a cap a forged-source flood could grow it without
  limit (memory-exhaustion DoS); an evicted legitimate publisher simply re-anchors its sequence
  on its next frame.
- Gated and bounded the Market-by-Order book map (`MboProcessor`). The live Hyperliquid MBO
  `FEEDS` row processes order deltas/snapshots keyed by an unauthenticated, spoofable wire
  `instrument_id`, and previously minted an unbounded `BookState` per id with no definition gate
  (unlike the Top-of-Book/Midpoint quote paths) — a strictly larger memory-exhaustion surface
  than the sequence map above, and live (not gated behind an absent feed). A forged MBO stream
  could grow memory two ways: distinct `instrument_id`s, or a flood of never-cancelled `OrderAdd`s
  for one instrument. Now a book is created only once its instrument definition is known (an
  undefined instrument can never emit `depth`); the book map is capped at `MAX_BOOKS` (4096) with
  least-recently-inserted eviction; and each book bounds its resting-order population, in-flight
  snapshot, and `Recovering` delta buffer (`MAX_ORDERS_PER_BOOK`/`MAX_PENDING_DELTAS`), dropping to
  snapshot recovery rather than growing without limit. Real feeds stay far below every cap.

## [0.1.0]

### Added
- Initial release of `doublezero-edge-connect`: ingests DoubleZero Edge binary
  multicast feeds (Top-of-Book & Trades, Midpoint, Market-by-Order), runs the
  reference-data subscriber state machine, and re-serves normalized market data over a
  WebSocket in the engine-agnostic JSON protocol specified in `PROTOCOL.md` (v1).

[Unreleased]: https://github.com/malbeclabs/doublezero-edge-connect/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/malbeclabs/doublezero-edge-connect/releases/tag/v0.1.0
