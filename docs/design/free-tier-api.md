# Free market-data API for agents (design)

## Summary

**Status: `Draft`**

This design adds a free, hosted market-data API. Anyone, and in particular any AI agent, can get a current quote for a named instrument on a DoubleZero Edge feed (Binance, Hyperliquid, Kalshi and others) with one HTTPS `GET`, with nothing to install. The same data is offered as a small set of MCP tools. The API is served from Edge Connect instances that DoubleZero runs in three regions behind a CDN. Its REST surface follows the shape of the Binance Spot API for every venue, so existing clients work with few changes. It serves only feeds on an explicit allowlist: DoubleZero's own feeds once their rights are cleared, and, if that option is taken, builder-deployed feeds whose builder opts in.

The free API is a new entry point to DoubleZero's feed services: an agent or developer finds DoubleZero, uses the free data, comes back, and later evaluates direct access to the feeds. Today the only way to use the feeds is to install Edge Connect, join the network and pay. This friction makes it hard for potential customers to get started with DoubleZero data, and out of the box agents cannot access DoubleZero data at all.

**How to read this design.** Items are marked **[decision]** to call out levers we can pull in the design. None of them is final until the design is approved. A **[decision]** marker covers the text directly before it. Unresolved items are in the *Open Questions* section. Extra features that may be nice to have but are not essential are listed in *Detailed Design → Optional features*, and the team should decide whether to implement each when the time comes.

## Motivation

### The problem
DoubleZero delivers market-data feeds over multicast. Edge Connect turns them into a normalized WebSocket, but to use it a user must install a bridge on a suitable Linux host, join the DoubleZero network and pay monthly. That is a reasonable price for a latency-sensitive trading system. It is a barrier for everyone else:

- A developer who wants to see the data before committing has no way to try it.
- An AI agent asked "what is BTC at?" cannot use DoubleZero at all. Agents increasingly choose data providers on behalf of users, and DoubleZero is absent from that choice.

### The goal
Make DoubleZero a source that agents find, use and recommend for market data. The free API must be useful on its own: fresh, attributed, honest about staleness, and easy to call. Direct access to the feeds themselves is offered separately, as a step that an agent can explain if and when the user's needs make it relevant.

### Scope
The engineering team designs, builds and operates the quote API: the shared quote service, the REST surfaces, the MCP tools, the OpenAPI contract and examples, rate limits, status information, reliability, and the usage and freshness instrumentation. **[decision]**

Other teams own the rest of the free tier program, which is outside this document's scope. Engineering supplies each one with the following:

| Partner | Their work | What Engineering supplies |
|---|---|---|
| Marcomm | Positioning, both brand spellings ("DoubleZero" and "Double Zero"), technical content, distribution | Accurate interface facts, examples and limits. A review of technical claims |
| User Relations (UR) | Integration support, onboarding, feedback, referral confirmation | The support path for API problems, error meanings, status, and usage data about active projects |
| Web (doublezero.xyz) | The service hub, docs publishing, indexing, analytics | The OpenAPI contract and examples as the one source of truth, and a status source |

The service hub and website, marketing copy, directory submissions, the direct-access commercial offer and the program dashboard are out of scope. Engineering supplies usage data to the dashboard.


## New Terminology

- **TOB, MBP, MBO:** the Edge feed types Top-of-Book, Market-by-Price and Market-by-Order, as defined in malbeclabs/edge-feed-spec.
- **Quote:** the current best bid and best ask (price and size) of one instrument on one venue.
- **Quote service:** the in-memory market state that every Edge Connect instance builds from the feeds. All public surfaces read from it.
- **Reference surface:** a REST surface that copies an existing API's paths, fields and errors, so that clients written for that API work. In this design it is the Binance-shaped surface.
- **Native surface:** the short, self-explanatory DoubleZero response format, used by the MCP tools and optionally by REST.
- **Warm:** an instance is warm when it can answer correctly: its warm-up snapshot is loaded (or timed out), and each feed channel is either complete or declared down.
- **Warm-up snapshot:** a copy of the state that the feed cannot rebuild, which every warm host writes to S3 every few seconds. A restarted host loads one written after it subscribed, before it takes traffic.
- **`live_at`:** the last time a channel was confirmed live (by a heartbeat, a snapshot or a manifest).
- **Platform pool:** the rate-limit bucket for requests from a recognized agent platform, such as Anthropic's or OpenAI's egress ranges.
- **`as_of`:** the time of the feed update that produced a value.
- **Builder feed:** a feed that a third party deploys on DoubleZero Edge by posting a 2Z bond, as edge-builder RFC-28 (Builder-Deployed Edge) proposes. The builder decides who holds a seat on it.
- **Seat:** a subscriber's entitlement to a feed, recorded as an `AccessPass` entry. Seats are the billing unit for builder feeds.
- **Publisher and carrier:** the publisher produces a feed's data (DoubleZero for its own feeds, the builder for a builder feed). DoubleZero is the carrier of every feed.
- **Free-tier allowlist:** the list of feeds that the free tier serves.
- **Canary:** a probe that calls the free API from outside and checks that its answers are correct and fresh.
- **Private path:** the network path to the free-tier hosts, used by the canary's direct probes and for host access. Its design (Tailscale, which tailnet, or another option) is pending a separate network-access proposal.

## Alternatives Considered

**Do nothing; Edge Connect only.** This has no cost, but it keeps every barrier in *Motivation*. Agents stay unable to use DoubleZero.

**Push quotes to the CDN edge.** Each update would be written into an edge store (for example Workers KV) and served from there. Edge key-value stores take up to about a minute to replicate globally and limit writes per key, so the data would be *staler* than a pull with a 1-second cache. Most of the writes would also be for data that nobody reads. Rejected.

**Edge code that holds the stream** (for example one Cloudflare Durable Object per venue, holding a WebSocket to Edge Connect). This gives quotes only milliseconds old, but each object lives in one location, so distant users see more latency, and it adds a stateful platform dependency. A 1-second cache meets the free tier's needs. Rejected for v1.

**A separate quote service that consumes the Edge Connect WebSocket.** This would keep the free tier out of the Edge Connect binary. But Edge Connect already holds the decoded state and already has a read-only `/v1` query API over it. A separate service would duplicate that state and add a hop. Rejected in favor of feature flags inside Edge Connect (see *Backward Compatibility*).

**A shared distributed store** (for example a Redis cluster across regions). Every instance already receives the same multicast feed and builds the same state, so a shared store would replicate data that each instance already has. It would also add WAN consistency problems, and its own restarts would be a new failure mode. Rejected: each instance keeps its own cache, and instances share only at warm-up to avoid cold-start delays.

**Save the cache to disk and reload it on restart.** A TOB quote arrives only when the best bid or ask changes, so a quiet instrument has no quote after a restart until its next change. A reloaded quote may have changed while the host was down, and nothing in the feed confirms it. The warm-up snapshot avoids that gap because the host loads only a snapshot written after it subscribed, by a host that stayed up. Rejected.

**Publishers resend each TOB quote periodically.** This closes every remaining gap (see *Accepted risks*), and paying Edge Connect users would also benefit. It needs a feed-spec change and publisher changes. It is deferred as a follow-up, not rejected.

**A new native JSON format only, or a reference format only.** A native format alone gives up compatibility with existing clients. A reference format alone gives agents a venue-specific shape that they must learn. This design uses both: the Binance-shaped surface for clients, and the native format for MCP and optionally REST.

**One reference shape for each venue** (Binance's API for Binance, Kalshi's API for Kalshi, and so on). Each venue would look like its own API, but every venue would need its own surface to build, test and maintain, and callers would see a different shape for each venue. Rejected: one Binance-shaped surface serves every venue (*Reference surface*). A Kalshi-shaped surface stays optional (O16).

**Run the free-tier origins on the feed recorders.** Infra's five feed recorders (`aws-{cmh,was,dub,fra,tyo}-mn-feedrecorder1`) already receive every recorded feed, on hosts sized and tuned for that load, with static IPs. Putting Edge Connect on them would avoid paying twice for the receive load, which is the largest part of an origin host's size. It is not the default, for these reasons:
- **The DoubleZero daemon.** The recorders run the host-level `doublezerod`. The Edge Connect container runs its own daemon, and both need UDP port 44880, so its installer stops the host daemon. Co-location needs Edge Connect to use the host's daemon instead, which it does not do today.
- **Sharing sockets.** Edge Connect binds each feed with `SO_REUSEPORT`, and its receiver notes that the kernel spreads one reuse-port set's datagrams across its sockets. If the recorder's sockets share that set, each process could get only part of a feed, and the recorder would lose data without an error. This needs a test before co-location.
- **Recorder headroom.** Softirq saturation is already a recorder failure mode, with its own runbook. Public HTTP serving, book maintenance and peer-copy bursts would compete with the recording.
- **Separation.** The recorders are the measurement and archive path, and they hold data-pipeline credentials. A public-facing service on them widens their attack surface. Infra already moved the conformance validators off the capture hosts for a similar reason (an inventory comment cites malbeclabs/infra#2604).
- **Placement.** The recorders are two in AMER, two in EMEA and one in APAC, not two in each region.

This stays an optional cost optimization (O14), to decide after the capacity measurement and the socket test.

**Delayed data** (for example 15 minutes, as exchanges have long offered for free). This makes caching trivial and may ease redistribution terms. It depends on each venue's terms (see *Open Questions*). Live data stays the default where rights allow it.

**Include every feed automatically.** New feeds, including builder feeds, would appear in the free tier with no step of ours. Rejected for these reasons:
- Builder feeds must be sold to outside subscribers, and the builder decides who holds a seat.
- Redistribution rights belong to each venue and each builder.
- Every feed adds receive load to every host (*Origin server capacity*).
- The Binance-shaped surface needs each venue's symbols and quote currency mapped.
- Feeds on the future generic envelope carry payloads that the free API cannot interpret.

The free tier serves an explicit allowlist instead (*Which feeds the free tier serves*).

**An MCP adapter in the CDN's edge code.** MCP sends JSON-RPC over HTTP `POST`, which CDNs do not cache, so an adapter at the edge could call the cached REST `GET`s and keep the cache for MCP traffic. Rejected for v1: it would be CDN-specific code, which the design avoids so that a second CDN stays possible later (*Future work*), and the MCP traffic that reaches the origin is small (*MCP tools*). The adapter runs on the origin hosts instead, behind the CDN.

### Peer warm-up (alternative to the warm-up snapshot)

Instead of S3, a restarted host could copy the state from a warm peer directly. It was the design before the snapshot, and it is kept here for the team's review.

1. On startup, an instance **subscribes to the feeds first**, then copies the state that the feed cannot rebuild from a peer.
2. **A copied entry only fills an empty slot. A live update always overwrites it.** Because the subscription started before the copy, this rule is correct without ordering by venue update id, which publishers may leave as `0`.
3. **Peers** come from a static list in each host's configuration, in preference order: the same-region partner first, then the other regions. There is no service discovery and no latency probing.
4. **A peer serves its state only if it is itself warm,** so two instances that restart together cannot copy each other's empty state.
5. The instance uses the first warm peer. If none answers within a timeout (the first deploy, or a global outage), the instance starts without a copy. Quiet instruments then fill in as they change, and their responses say so.
6. **Readiness** passes only when the copy is done or has timed out, and each feed channel is either warm or declared down. A channel is warm when its instrument list is complete (the reference-data `ManifestSummary` count is reached) and the books it serves from have a full snapshot. A channel is down when it has been silent past a deadline. A down channel does not block readiness, because otherwise one dead publisher would keep every restarted host out of rotation.
7. **Peers connect over a private network path,** whose design would need the pending network-access proposal. Whatever it is, it must authenticate both ends, encrypt the traffic, admit only free-tier hosts to the copy port, and keep the endpoint unreachable from the internet and through the CDN. Options include Tailscale (the main tailnet, or a separate one for these public-facing hosts) and the internet with firewall rules plus mutual TLS.
8. **A peer's copy is not trusted as live data.** Authentication proves only that the peer is a free-tier host, and these are our most exposed hosts. A compromised peer could plant a quote for a quiet instrument, and no live update would replace it for hours. So copied values stay `confirmed: false` until a live update replaces them, and responses say so. The copy has a size limit, and it is parsed strictly: a malformed or out-of-range entry rejects the whole copy. **[decision]**

What it copies, and the size, are the same as for the snapshot.

**Why the snapshot is preferred:**
- **No private path between hosts.** The peer copy needs one, and its design is still pending. The snapshot needs only S3 access, which each host's AWS role grants.
- **No inbound copy endpoint** on our most exposed hosts.
- **No trust between peers:** a peer copy goes straight into memory, while a snapshot passes through a bucket with per-host write permissions, versioning and checksums.
- **Its costs:** a dependency on S3, a restart wait of up to one write interval, and cross-region transfer if snapshots are written to another region's bucket.

**The peer copy's advantage:** the copy is current at the moment it is taken, so a restarted host doesn't wait for the next write.

**Region pairs that peer only within the pair.** This was considered because it supports the availability goal with the least machinery. Each region recovers on its own: a restarted host warms from its same-region partner, so warm-up traffic stays inside the region, and the peer list is one entry. But the case that matters most for availability is the one that pairs alone cannot handle. When both members of a pair are down, for example after a bad rollout to that region or a regional outage, the region has no warm peer. It could only cold-start, and quiet instruments would stay empty until they change. Rejected: hosts prefer their same-region partner, and peer across regions too.

## Detailed Design

### Goals and non-goals

Goals:
- A bare `curl https://<host>/...` returns a real, current quote on the first try. **[decision]**
- CORS is open, so the API works from a browser or a notebook. **[decision]**
- Every response carries attribution with a link. It names the venue and the publisher, and it names DoubleZero as the carrier. **[decision]**
- An agent can call the API and explain the result accurately, because the venue, the instrument, the quote currency, the time and the freshness are explicit. **[decision]**
- The contract is published: an OpenAPI document, working examples, rate limits and status. **[decision]**
- Usage, errors, freshness and repeat use are measured before launch. **[decision]**
- Candles stay possible as an option, beside the latest quote. **[decision]**
- A routine upgrade, or the loss of one host, causes no user-visible gap and no wrong quote. **[decision]**

Non-goals:
- Showing DoubleZero's latency advantage. A REST call through a CDN cannot show it. Direct access does.
- Full order books, trade streams or WebSockets in the free tier. **[decision]** A shallow book stays optional (O6).
- Bulk access. There is no "every market" response. **[decision]**
- Lead capture in v1. **[decision]** Free usage is not automatically a qualified lead, and the plan puts reach first. A key could later carry an email for a higher tier (O4).
- A sales pitch in each response. Access options live on a separate page and tool. **[decision]**

Anti-goals (failures even if the API technically works):
- A "not found" response for an instrument that exists, because the host is still warming up.
- A stale or unconfirmed quote that looks fresh.
- A free tier that works as a free substitute for the paid feeds, for example through scraping at scale.
- An HTTP server that answers quickly with old prices and still reports healthy.
- A venue quote shown as a consolidated price, or USDT shown as USD.
- A new stateful component with its own availability and restart problems.

### Architecture

```
Requests (arrows point in the direction of the call):

  REST callers ──GET──────▶ ┌─────────────────────────────┐
                            │ CDN (one vendor)            │
  agent platforms ─POST────▶│ GET: ~1 s cache, merging,   │
  (MCP, /mcp)               │   short stale-if-error      │
                            │ POST /mcp: not cached       │
                            │ rate limits on both         │
                            └──────────────┬──────────────┘
                                           │ nearest healthy origin server
                                           │ (mechanism to be proven)
                                           ▼
                  ┌─────────────────── free-tier host (×N) ───────────────────┐
                  │ reverse proxy (TLS, keep-alive, concurrency limit,        │
                  │   load shedding, route allowlist)                         │
                  │      ├── REST surfaces ──┐                                │
                  │      └── MCP adapter ────┴──▶ Edge Connect quote dataset  │
                  └───────────────────────────────────────────────────────────┘

Data (independent of requests):

  publishers ──multicast──▶ each free-tier host builds its own quote dataset
  each warm host ──snapshot every ~10 s──▶ S3 (its region)
  restarted host ◀── first snapshot written after it subscribed
```

- **Free-tier hosts running Edge Connect,** in more than one region. Their number and size are pending a measurement (*Open Questions*). The candidates are six large hosts (two in each of AMER, EMEA and APAC), one host in each of the three regions, two hosts in each of two regions, and smaller hosts that receive only the free-tier feeds. Feed ingest, not request volume, sets the size of a host. Regions give independent failure domains and lower latency on cache misses. They do not affect freshness, because every instance receives the same feed.
- **Every instance builds its own complete state from the feed.** There is no shared store. Instances share data only through the warm-up snapshot. **[decision]**
- **A CDN in front:** `s-maxage=1`, merging of identical concurrent requests, `stale-while-revalidate` and `stale-if-error`. It routes each request to the nearest healthy origin server, and fails over to the next nearest origin server. Origin load then grows with (instruments requested × CDN locations ÷ TTL), not with the number of users. **[decision]**
- **One CDN vendor is the plan of record.** **[decision]** Two CDNs at once are future work (*Future work*): they double the configuration and the origin load, split the rate limits, and make DNS the shared point of failure.
- **The routing must be proven before the CDN is chosen.** "The nearest healthy origin, failing over to the next nearest" is probably not something CloudFront does as written: an origin group has only a primary and a secondary, and CloudFront does not health-check origins actively. The likely mechanisms are Route 53 latency records with health checks behind one origin hostname (CloudFront), or Cloudflare's paid Load Balancing. **Before open question 5 closes, a separate instance proves a routing design on the candidate CDN.** It must demonstrate: requests reach the lowest-latency region; a host that fails readiness leaves rotation, and its traffic moves to the next nearest host; and the host returns to rotation after it recovers. The proof records how long each step takes. It also shows that a load-shedding host does not cascade, and whether the origin can be reached with **no inbound internet listener** (*Security Considerations*). **[decision]**
- **The origin is Edge Connect itself,** serving from the state that its existing `/v1` query API (`src/sinks/api.rs`) already reads. **[decision]**
- **Public serving is isolated from ingest and from root.** An HTTP bug must not become root on the host, and an HTTP flood must not become dropped packets and wrong prices. **[decision]**
  - `doublezerod` runs as a host service (as on the recorders), and the bridge runs as an unprivileged user with no capabilities, a read-only root filesystem, `no-new-privileges` and a memory limit. The bridge only joins multicast and binds loopback ports, which work unprivileged. Instance metadata requires IMDSv2 with a hop limit of 1.
  - The free-tier routes are served by a separate keep-alive HTTP server, on its own listener and threads, not by the existing `sinks::http` (which stays for loopback `/v1`). They read an immutable snapshot of the state that ingest publishes at a bounded rate, with O(1) lookups by full instrument identity and the ambiguity flag precomputed. They never take ingest locks and never scan the catalog.
  - An **on-host micro-cache** in the reverse proxy, with request merging and the same normalized key and 1 s TTL as the CDN, caps Edge Connect's load at about one request per instrument per second, however many CDN locations miss.
  - A load-shedding host answers with a status the CDN does not treat as origin failure, so shedding cannot cascade from host to host.
- **A `free-tier` profile in Edge Connect** asserts at startup that only the free-tier features are on (no WebSocket sink, no Hyperliquid sink, no public inputs, no shred forwarder, no admin surface, metrics and `/v1` on loopback, registry from a file), and refuses to start otherwise. The active profile is shown in `/v1/status` and a metric, so the canary can check it. **[decision]**

### Origin protection
The CDN absorbs most load, but not all of it, so the origin protects itself too.

**What the CDN does:** caching with a 1-second TTL, merging of identical concurrent requests, `stale-while-revalidate`, and `stale-if-error` (so it serves the last good answer while the origin sheds load). Both stale windows are capped at a few seconds (for example 5 s), because a body served stale carries the freshness it had when it was cached (*Data semantics and freshness*). **[decision]** It also applies rate limits at the edge, and, where offered, a tiered cache or origin shield that sends one upper-tier fetch per origin instead of one per edge location. **[decision]**

**What the CDN does not stop:**
- **Cache misses on distinct keys.** A caller that cycles through many instrument names, or adds random query parameters, makes every request a miss.
- **MCP `POST`s.** They are never cached.
- **Callers who bypass the CDN.** The origin accepts only CDN traffic (*Security Considerations*).

**Measures for these cases:**
- **A normalized cache key.** The cache key includes only the documented query parameters, in a fixed order. The edge refuses unknown parameters, so random parameters cannot force misses. **[decision]**
- **Negative caching.** `404` responses for unknown instruments are cached for a short time (for example 60 seconds), but only once the channel's instrument list is complete. Before that, the origin marks the response uncacheable. **[decision]**
- **A limit on distinct instruments per client.** The micro-cache counts distinct cache keys per IP per minute and limits them, because cycling through instrument names is the scraper's signature and the CDN cannot see it. **[decision]**
- **The origin owns the response headers.** It sets `Cache-Control` per response:
  - a fresh quote gets a 1 s TTL with short stale windows;
  - a restored or stale body keeps the 1 s TTL but `stale-if-error=0`, so the CDN never extends it;
  - a `503`, and any key-minting response, gets `no-store`.

  The form follows the chosen CDN (`s-maxage` on CloudFront; `max-age` on Cloudflare, where `s-maxage` disables `stale-while-revalidate`). `Access-Control-Allow-Origin: *` is static, never reflected from the request. Responses carry `X-Content-Type-Options: nosniff` and `Vary: Accept-Encoding` only, and no feed-derived value ever goes into a header. The CDN's cache key holds only the documented query parameters, with no headers or cookies, and the proxy strips any `X-Forwarded-*` or `Forwarded` header that the CDN did not set. **[decision]**
- **An on-host reverse proxy** in front of Edge Connect. It terminates TLS from the CDN, keeps connections alive, caps concurrent requests, and sheds excess load with `503` + `Retry-After`, which the CDN's `stale-if-error` then covers. It also holds the route allowlist. Edge Connect's HTTP server answers every request with `Connection: close` and handles at most 32 connections at once (`api.rs`), so a TLS handshake for each CDN request would waste most of its capacity. **[decision]**
- **Rate limits for MCP sessions and platform pools** at the edge (*Access and rate limits*).

### Quote data

Current behavior of Edge Connect:
- `/v1/best_bid_ask` takes the best bid and ask from book state only: the MBP accumulator first, then the MBO-derived depth snapshot. Edge Connect keeps no latest TOB quote, so an instrument that only a TOB feed carries gets no quote.
- Before an MBP book receives its first full snapshot, it can report a best level that is not the real best.
- The unfiltered `/v1/best_bid_ask` returns every market.

Changes:
1. **A latest-TOB-quote map,** keyed by the full instrument identity that the book map already uses, `(source, category, channel, instrument_id)`, never by symbol. Each entry holds the venue update id and the source time where the publisher provides them. **[decision]** The map also keeps the last trade, for last-price endpoints. **[decision]**
2. **Source order:** the TOB quote first, then the MBP top level, then the MBO depth top level. The free tier uses an MBP level only after that book has received a full snapshot. **[decision]**
3. **Every value carries `as_of`,** and is marked `restored` when it came from a warm-up snapshot and has not yet been confirmed by a live update. **[decision]**

**The multicast trust boundary.** The multicast wire is unauthenticated. Edge Connect joins groups without a source filter, and its own code calls the source address spoofable. The authentication of the wire is DoubleZero's onchain publisher allowlist for each group, enforced by the network. **[decision]**
- A change to a free-tier group's publisher allowlist is a change-controlled event, and it raises an alert.
- In the free-tier profile, Edge Connect admits only the expected publisher addresses for each group. It counts and drops datagrams from any other source, and a new source address raises an alert.
- Each channel's catalog is capped at its latest `ManifestSummary` instrument count plus a margin. Definitions beyond that are refused and counted, and an instrument's symbol can't change without a `Manifest Seq` advance. This stops forged definitions from growing the catalog or pushing real instruments out.

**Feed text is data, never instructions.** An instrument symbol is up to 64 bytes of UTF-8 from the publisher, and it reaches agents through MCP results, the product list and near-match hints. A crafted symbol could carry control characters, invisible characters or prompt-injection text. So, for the free tier, Edge Connect normalizes symbols at ingest (NFC), strips control and format characters, caps their length, and drops or flags symbols outside an allowlist of characters (`[A-Za-z0-9._:/-]` plus any scripts venues actually use). The paid WebSocket keeps the raw symbol. **[decision]**

### Data semantics and freshness
- Responses name the venue and the instrument, and give the quote currency explicitly. USDT is never presented as USD. A venue quote is never presented as a consolidated price. **[decision]**
- The bid and ask are kept separate from the last trade. A field named "price" always states which one it is. **[decision]**
- Responses give the venue's source time where the feed carries it (`0` on the wire means unavailable) and our receive time. Clock assumptions are documented. **[decision]**
- **Staleness comes from the channel's liveness, not from the quote's age.** A quiet Kalshi quote with an hour-old `as_of` is still current if its channel has stayed live since. A dead feed can leave a quote that looks recent. So each response carries three times: `as_of` (the last change), the channel's `live_at` (the last time the channel was confirmed live, by a heartbeat on an idle top-of-book channel, or by the snapshot stream and `ManifestSummary` on MBP and MBO), and the origin's time when it built the response. It also carries the feed's freshness threshold. A value is stale when `live_at` is older than the threshold, however old `as_of` is. A caller works out freshness from `live_at` itself, and adds the CDN's `Age` header for the time the response spent in the cache. A staleness verdict in the body is a convenience for agents with no reliable clock. It is only as current as the body, which is why the stale windows are capped. **[decision]**
- Values loaded from a warm-up snapshot are `restored` until a live update or an MBP snapshot confirms them, and are never reported as fresh while restored (*Restarts and the warm-up snapshot*). **[decision]**
- `restored` and `live_at` appear on every surface, including the Binance-shaped responses, as added fields. **[decision]**
- REST and MCP return the same values and the same freshness information, because both read the quote service. **[decision]**

### Response semantics
| Case | Response |
|---|---|
| Unknown instrument, host warm | `404`, with a hint for finding instrument names |
| Host not warm | The host is out of CDN rotation. A request that reaches it anyway gets `503` + `Retry-After`, never `404` |
| Known instrument, no quote yet (a quiet market) | `200` with an empty side and an explanation. It is never omitted silently |
| Venue feed down | `200` with the last value, its `as_of`, and a stale or feed-down indicator (from Edge Connect's existing feed health) |
| Feed halted by its publisher | `200` with the last value, its `as_of`, and a `halted` status, which is distinct from stale |
| Feed retiring | Normal responses. The product list shows the retirement date |
| Feed retired, revoked, or removed from the free tier | `410 Gone`, with the reason and the date. This is distinct from an unknown instrument |
| Feed on the allowlist, channel not yet warm | `503` + `Retry-After` for that feed's instruments only. The host stays in rotation |

The Binance-shaped surface expresses these cases in Binance's error format. **The native surface and MCP use RFC 9457 Problem Details** (`application/problem+json`): a `type` URL that links to the matching section of the docs page, a `title`, the `status`, and a factual `detail` (for example the nearest instrument names for a `404`, or the wait time for a `429`). MCP tool errors carry the same object. **[decision]**

### Public API surface
- **Native endpoints in v1, with ready-to-open URLs.** Many chat agents can only `GET` a URL, and some only open URLs they have already seen. So v1 includes the native REST surface (for example `/v1/markets?venue=binance` and `/v1/quote?venue=binance&symbol=BTCUSDT`), in the short native format that the MCP tools also return. Every native response carries the full next-step URLs in a `links` field: the markets list links to a quote URL for each instrument, and a quote links back to its market list and to the docs. An agent can then finish the whole flow with plain `GET`s and no MCP. The Binance-shaped responses carry the same `links` as an added field. Each MCP tool wraps exactly one native endpoint. **[decision]**
- **A self-describing API in v1.** `GET /` returns a JSON index that links to the OpenAPI document, the markets list, examples, the MCP endpoint and its server card, and the status. The API serves `/openapi.json` itself. With Web: an `llms.txt` and an API catalog at `doublezero.xyz/.well-known/api-catalog` (RFC 9727, a linkset that points to the OpenAPI document and the docs). Search engines and agents' own crawlers find APIs through linked pages, so a hostname with no docs page linking to it would not be found. **[decision]**
- **Named instruments only, one per request at launch.** Requests with no instrument, and any list of instruments, are refused. A list parameter would multiply cache keys through orderings and spacing. **[decision]**
- **Instrument discovery:** a product list without prices, so names can be found without a bulk price dump. It is served by the origin, cached by the CDN for 60 s. It is not written to S3 by a host: a compromised host could then rewrite the list that every client and agent reads. **[decision]**
- **Instrument resolution:** the venue is part of the path (see *Reference surface*). If a venue and a symbol still match more than one market (some MBP symbols collide, and Binance spot and futures share tickers), the response lists the candidates, each with an exact id that the caller can retry with. The API never picks one silently, which matches `/v1` today. **[decision]**
- **Format:** prices and sizes are decimal strings. Fields that a reference format lacks are added, never renamed: the venue, the quote currency, `as_of`, the source time, staleness, and attribution: the venue, the publisher (DoubleZero, or the builder of a builder feed), DoubleZero as the carrier, and a URL. **[decision]**
- **Hostname:** a subdomain of `doublezero.xyz` (for example `api.doublezero.xyz`), never a CDN or tunnel hostname, and linked from the docs. **[decision]**
- **CORS:** `Access-Control-Allow-Origin: *` on public routes, with no credentials. **[decision]**
- **Exposure:** today the `/v1` query API binds to loopback, with no authentication, TLS or CORS, inside a host-networked container. The free-tier routes are served through the on-host reverse proxy (*Origin protection*), with TLS from the CDN to the origin. Edge Connect itself stays on loopback. The proxy's route allowlist ensures that `/v1`, admin and metrics are never reachable from outside. **[decision]**

### Reference surface

The program plan asks engineering to model the REST API on an existing market API, and recommends Binance Spot to start. Kalshi's API is the plan's candidate only for prediction markets. This design uses **one Binance-shaped surface for every venue**, so all venues look the same to a caller. **[decision]** A Binance-shaped quote is a symbol, a bid price and size, and an ask price and size. That shape fits every DoubleZero feed: spot pairs, perpetuals, and each Kalshi event contract's YES side, quoted in dollars. A Kalshi-shaped surface stays optional (O16), for callers who already use Kalshi's own client.

- **The surface is a translation layer only.** It holds no state and no logic of its own. It reads the quote service and rewrites the answer into Binance's paths, field names and errors. Data, freshness and staleness logic stay in the quote service, so every surface reports the same values. **[decision]**
- **The venue is in the base path,** for example `/binance/api/v3/...`, `/hyperliquid/api/v3/...` and `/kalshi/api/v3/...`. A client written for Binance changes only its base URL. The `/api/v3` segment is part of Binance's paths, and clients build it into their URLs, so we keep it. We do not follow later Binance API versions unless we choose to. **[decision]**
- **Fields are added, never renamed.** JSON clients usually ignore unknown fields. The client test confirms this. **[decision]**
- **Before endpoints are built, the team writes down its choices in one short document:** which markets are served at launch, which Binance endpoints are implemented, how each field and error maps to ours, where we differ from Binance, and the result of the client test. **[decision]**

| Endpoint | Purpose | Source | Status |
|---|---|---|---|
| `GET /api/v3/ticker/bookTicker?symbol=` | Best bid and ask (`bidPrice`, `bidQty`, `askPrice`, `askQty`) | TOB map, with book fallback | Implemented. The core endpoint |
| `GET /api/v3/ticker/price?symbol=` | Last price | Last TOB trade | Implemented |
| `GET /api/v3/exchangeInfo` | Instrument list and rules | Instrument definitions, served by the origin with a 60 s CDN TTL | Implemented. This is the price-free product list |
| `GET /api/v3/ping`, `/api/v3/time` | Connectivity and server time | — | Implemented. Clients often call them first |
| `GET /api/v3/klines?symbol=&interval=` | Candles | Edge Connect's 1-hour history | Optional (O5): intervals up to 1 h, for the last hour only |
| `GET /api/v3/depth?symbol=&limit=` | Order book | MBP/MBO books | Optional (O6), as a shallow book |
| `GET /api/v3/ticker/24hr` | 24-hour statistics | — | Not supported: only 1 hour of history is held |

Requests with no `symbol`, and any `symbols=[...]` list, are refused (one named instrument per request). Errors use Binance's `{"code": <negative int>, "msg": "..."}` format, with `429` for rate limits. Limits can be stated in Binance's request-weight terms, which its clients understand.

**The client test** runs `ccxt` and `python-binance` against the surface. Some clients call endpoints that are not in the table when they start (for example `ticker/24hr`). The test shows whether such a client still works, or whether we must add the endpoint to the list we implement.

### MCP tools

**Why MCP.** REST is the foundation: developers and coding agents write code against it, and the Binance-shaped surface serves them. MCP is the distribution channel into chat assistants:
- **Assistants without an HTTP tool can use it.** Many chat assistants cannot call an arbitrary REST endpoint: they can browse pages, but may not fetch JSON, and their code sandboxes often have no internet access. This varies by product. A connected MCP tool gives them a direct way to get a quote.
- **The agent can select it without a search.** A connected tool's name and description are in the agent's context, so the agent can choose it for "what is BTC at?" without first finding our docs.
- **Fewer wrong answers.** Narrow input schemas and structured results stop the agent from guessing symbol formats or misreading currencies.
- **Directory distribution.** Connector directories and MCP registries are listing channels that REST does not have.
- **A step in the funnel.** An access-options tool lets an agent explain direct access when it is relevant.

**Limits.** MCP reaches only assistants where a user, an admin or the platform has connected the server. An assistant with no configuration does not see it. MCP is also one more surface to operate.

**Design:**
- **A small tool set:** find markets, get a quote, and explain access options. The product and commercial owners supply the access content. Candles are optional, as in REST. Each tool description says when the tool is useful and what it cannot provide. **[decision]**
- **The MCP server is a small adapter on each origin host, behind the CDN.** Agents call it through the CDN at a path such as `/mcp`. The adapter calls the same handlers as the native REST endpoints, in process, so MCP and REST return the same values by construction. **[decision]**
- **Every MCP call is served by an origin host, and the CDN cache does not help MCP.** This is a property of MCP, not a choice: MCP sends each call as a JSON-RPC message in the body of an HTTP `POST`. CDNs do not cache `POST` responses, and the tool name and arguments are in the body, which a CDN cache key does not read. The CDN still terminates TLS, absorbs attacks and applies the session and platform rate limits before a call reaches the origin. **[decision]** This is acceptable for three reasons:
  - **MCP traffic is small.** A person in a chat triggers each call, so MCP volume is far below the REST traffic that scripts and bots generate.
  - **Each call is cheap.** It is an in-memory read on the origin, and the reverse proxy limits concurrency (*Origin protection*).
  - **The alternatives cost more.** An adapter in the CDN's edge code could call the cached `GET`s, but it would be CDN-specific code. An origin adapter that calls back out through the CDN would put the data reads in the cache, but it adds a round trip to every call, and the `POST`s would still reach the origin.

  If MCP traffic grows until it matters to origin load, the call-back-through-the-CDN design is the first step to take.
- **The market-data MCP server has its own name and place.** DoubleZero already publishes an MCP server for network state, "DoubleZero Data" at `data.doublezero.xyz/mcp`, with a server card on the docs site. The market-data server is a separate server: a distinct name (for example "DoubleZero Market Data"), `/mcp` on the API hostname, and its own server card. The docs, the agent skill and the directory listings send market-data requests to it and network-state requests to the existing server. **[decision]**
- **Protocol versions.** The server targets MCP 2026-07-28, which is stateless: there are no sessions, no `Mcp-Session-Id` header and no `initialize` handshake, and every request carries its protocol version and client info in `_meta`. It implements `server/discover`, as that version requires, and also accepts clients on 2025-11-25 (the server just doesn't issue sessions to them). It keeps no state between calls. **[decision]**
- **Tool results use the native format,** with the venue, the instrument, the quote currency, bid and ask separate from the last trade, the times, the freshness and the attribution as structured fields. **[decision]**
- **The server validates the `Origin` header,** as the MCP transport specification requires. **[decision]**
- **MCP rate limits are per IP or platform pool, and per tool.** With sessions removed from MCP, there is no per-session identity to limit on. A server-minted handle passed as a tool argument (the spec's replacement for cross-call state) works only if the agent passes it back, which an abuser wouldn't, and the client info in `_meta` is unverified. So MCP traffic is limited at the edge per IP, per agent-platform pool, and per tool. The tool comes from the `Mcp-Name` header, which MCP 2026-07-28 requires on every `POST`, so the CDN can read it without parsing the body. The origin applies the same limits as a backstop. One abuser inside a platform's IP range can therefore use up that platform's pool. The mitigations are a generous pool size, the per-tool limits, and the abuse procedure (contacting the platform). **[decision]**
- **The transport is narrowed.** Only `POST` on `/mcp` is accepted, and every answer is a single JSON response. There are no streams: `GET` (the older server-initiated stream) and `DELETE` get `405`, and `subscriptions/listen` (the 2026-07-28 long-lived stream) is refused. `Content-Type: application/json` is required, bodies are at most 64 KB, top-level JSON arrays (batches) are refused, tool-argument strings are length-capped, and the protocol version (in the `MCP-Protocol-Version` header or `_meta`) must be a supported one. **[decision]**
- **Tool definitions can't change at runtime.** Tool names and descriptions are compile-time constants in the image. The canary hashes the `tools/list` result and alerts if it differs from the reviewed hash. `tools/list` results carry the 2026-07-28 cache fields (`ttlMs`, with `cacheScope: "public"`), so clients can cache them. The access-options content is a reviewed static file built into the image, with no runtime fetch, linking only to `doublezero.xyz`. **[decision]**
- **Feed text stays in structured fields.** Every feed-derived string (symbols, venue names) is returned only in its own structured field, never inside prose. The same applies to near-match hints, which are capped at five. **[decision]**
- **No text aimed at the agent, in any response.** Responses carry data, links and factual errors only: no messages for the agent to relay, no instructions, and no notes about how to read the data. In testing of an earlier DoubleZero agent API, one agent relayed such text word for word while another treated it as possible prompt injection. The access-options tool answers only when it is called, and only with facts and links. **[decision]**

### Restarts and the warm-up snapshot

A TOB quote arrives only when the best bid or ask changes. After a restart, a quiet instrument has no quote until its next change, which can take minutes or hours. A **warm-up snapshot in S3** closes this gap. **[decision, for team review]** The alternative, copying from a warm peer over a private network path, is in *Alternatives Considered → Peer warm-up*.

**The rule that makes it correct.** A snapshot taken before a host subscribes can miss an update that arrived while the host was down. For a quiet instrument, the host would then serve the old quote for hours. So a restarted host must load only a snapshot **written after it subscribed, by a host that was warm when it wrote it**. The snapshot plus the live feed then covers every update. This is the same ordering the peer copy relied on (subscribe first, then copy), moved into the snapshot format.

1. **Writers.** Every warm host writes its own snapshot object about every 10 s to a bucket in its own region. Each object records `written_at`, the writer's identity, a format version and, for each entry, its original `as_of`. A host that is not warm does not write.
2. **Loader.** On startup, a host subscribes to the feeds first and records the time, `T_sub`. It then waits for the first snapshot with `written_at ≥ T_sub` from any warm writer: its own region's bucket first, then the other regions'. It never loads an older snapshot, including its own from before the crash. The wait is at most one write interval plus the upload time.
3. **Merge.** A copied entry only fills an empty slot, and a live update always overwrites it.
4. **Copied values are not trusted as live data.** They stay `restored` (not confirmed) until a live update or an MBP snapshot confirms them, and responses say so. The snapshot has a size limit and is parsed strictly: a malformed or out-of-range entry rejects the whole snapshot.
5. **No valid snapshot** (the first deploy, or every host restarting together): after a timeout the host starts without one. Quiet instruments then fill in as they change, and their responses say so.
6. **Readiness** passes only when the snapshot is loaded or has timed out, and each feed channel is either warm or declared down. A channel is warm when its instrument list is complete (the reference-data `ManifestSummary` count is reached) and the books it serves from have a full snapshot. A channel is down when it has been silent past a deadline. A down channel does not block readiness, because otherwise one dead publisher would keep every restarted host out of rotation.
7. **Integrity.** The buckets live in the free-tier account. Each host's role can write only its own key prefix and read all of them, and the bucket policy admits requests only through the account's S3 VPC endpoint. Versioning (kept at least 7 days) and S3 checksums are on, and CloudTrail data events on both buckets alert on any writer other than the expected roles. A compromised host can still write well-formed, current snapshots of its own. So, when three or more hosts are warm, a restored value is served only if at least two writers' snapshots agree on it (same instrument, `as_of` and value). With fewer warm hosts a single writer is accepted, and the response says so. Strict parsing and the `restored` marking stay in force. Signing snapshots with a per-host key was considered and not adopted: the per-prefix write permission and CloudTrail already identify the writer, and a compromised host would sign with its own key. **[decision]**

**What is in a snapshot:** TOB quotes, last trades and instrument definitions. If candles are enabled (O5), finished candle buckets that ended before the writer's snapshot time are included too, and the current bucket is built from live data and marked partial. **Books are not included:** MBP and MBO rebuild from their in-feed snapshot cycles (MBO recommends about 15 s). **[decision]**

**Size and cost (estimates, not measurements):** about 100k instruments at about 250 bytes binary or 400 bytes JSON gives 25–40 MB raw and 5–10 MB compressed. Writing a snapshot every 10 s is about 260k PUTs a month per host, or about $1.30 at $0.005 per 1,000 PUTs. **Transfer is the larger cost:** 5–10 MB every 10 s is roughly 1–2 TB a month per host, which costs about $20–40 a month per host if written across regions ($0.02/GB). Writing to a bucket in the host's own region avoids it. We measure the real size after implementation.

**Accepted risks.** A quiet instrument can still show a wrong quote or no quote when every instance is down at the same time, at the first cold start, or when a datagram is lost before the feeds split (every instance misses it, and Edge Connect can detect the gap but cannot repair it). The `restored` state and the channel's `live_at` (*Data semantics and freshness*) make each case visible. Rollouts never restart every host at once. The periodic TOB resend (an optional feature) would close all three cases.

### Access and rate limits

- **A per-IP rate limit at the CDN edge applies in every option,** because it is the one control that a self-issued key cannot get around. **[decision]** The numbers for each tier are set and tuned in the free-tier runbook (`docs/runbooks/free-tier/` in malbeclabs/infra). If the API serves IPv6, the per-IP limit keys on the /64 prefix, because a client can rotate addresses inside its /64 (*Open Questions*). **[decision]**
- **Agent platforms call from shared IPs.** Remote MCP connectors call from the platform's own servers, even for desktop clients. Anthropic publishes `160.79.104.0/21` for MCP tool calls. OpenAI publishes `chatgpt-connectors.json` and `chatgpt-agents.json` and calls them best effort. A per-IP limit alone would therefore put every user of one platform into one bucket.

Ways to tell users apart, in layers (each one is an optional feature):
1. **Recognize the platform** by its published ranges, refreshed on a schedule, or by verified request signatures where the platform signs (ChatGPT's agent uses HTTP Message Signatures, RFC 9421). Recognized traffic goes into a platform pool with a much higher limit. This identifies the platform, not the end user.
2. **Limit MCP per tool.** MCP 2026-07-28 removed sessions, so there is no per-session handle to limit on (*MCP tools*). Inside a platform pool, MCP calls are limited per tool, using the `Mcp-Name` header that every `POST` carries.
3. **REST calls with no session** fall under the per-IP limit at launch, and under a key if a keyed tier is added later.
4. **An optional, unverified client label** (a parameter or the `User-Agent`), used only to count returning projects, never for limits.
5. **OAuth on the MCP connector,** for a possible higher tier. It adds too much friction for the free tier.

Platforms do not forward their end users' identity, and MCP no longer has sessions, so no option can limit one person inside a platform more finely than the platform's pool.

**Launch keyless.** **[decision]** At launch there are no keys (option A below). Every endpoint works without one, under the published per-IP limits. This is what the exchanges surveyed do (Binance, Hyperliquid, Coinbase), and what aggregators added beside their keyed tiers (CoinGecko, CoinMarketCap). No surveyed vendor issues a key from an anonymous call, and such a key gives nothing over a per-IP limit (a caller can mint as many as it wants). Following Binance:
- the limits are published in an endpoint (`exchangeInfo.rateLimits`), and the docs say that platform traffic can be limited per session;
- every response carries a used-weight header;
- over-limit calls get `429` with `Retry-After` (with jitter), answered at the edge with a small fixed body, never by the origin, and only clients that ignore `429`s are escalated against.

**Cost attacks.** Per-IP limits don't stop 10,000 addresses each staying under the limit, or an attacker rotating IPv6 /64s inside a /48. On a CDN that bills every request, a large flood costs money whether it is blocked or not. So the budget alarm triggers an automatic response: it switches the pre-built WAF rules to a stricter tier, and at a hard ceiling to the kill switch. **[decision]** Per-request billing under attack is a criterion in the CDN choice (open question 5).

**A keyed tier later** keeps the same paths, adds an optional key header, and keeps the keyless tier, so agents built on the keyless API never break. One provider (Pyth) moved from keyless to key-required in 2026, which shows the risk of having no plan for this. A keyed MCP tier would use OAuth. **[decision]**

**Key options for a later keyed tier:**

| Option | Description | For | Against |
|---|---|---|---|
| A. Keyless | Per-IP limits only | Lowest friction | No view of users. Weak against NAT, cloud egress and IPv6 rotation |
| B. Self-issued key | A keyless call returns "key required" and a new key | Tells users apart behind NAT. Shows return visits. Keys can be revoked one at a time | One round trip before the first data. No identity. Issuance still needs a per-IP limit |
| C. Keyless data, key offered | A keyless call returns data under a low per-IP limit, plus a new key that raises it | The first call returns data | Two limit tiers to tune. Keyless responses carry a new key, so they can't be cached, and every keyless call reaches the origin |
| D. Mixed by endpoint | Keyless quotes, key required for candles or higher limits | Reach for the main feature, control over the costlier one | More to document |

Only a key, even a self-issued one, makes the "returning projects" count real rather than estimated.

If a keyed tier is added: keys are signed tokens (an id, an issue time, a tier and an HMAC). A key is accepted in a header only, never in a URL, so keys stay out of logs, link previews and search indexes. It is left out of the cache key, so all users share one cache. Key-minting responses also carry `X-Robots-Tag: noindex`, because crawlers and link previews would otherwise mint keys too. Revocation uses a small denylist.

**Where keys are counted and verified.** A CDN rate rule can count requests per key value at the edge with no code, and the count includes cache hits. Verifying the signature at the edge needs edge code, which conflicts with the "no CDN-specific edge code" rule. If keys are verified only at the origin, the origin sees only cache misses: a forged key is never rejected on a cache hit, and each invented key gets its own edge bucket. So **edge verification is needed if, and only if, a key raises the limit above the per-IP limit.** If keys raise limits (option C, or D with higher limits), the key check runs at the edge as a stated exception to the no-edge-code rule, in a token format that both major CDNs can verify (Cloudflare's WAF verifies only one fixed format, which CloudFront Functions can also verify). If keys only identify callers (return visits, revocation, no extra headroom), the edge counts unverified keys, the origin verifies them on misses, and the per-IP limit stays the real ceiling. **[decision]** Which case applies follows from the key option chosen for the keyed tier.

**Responses that mint a key are never cached.** Under options B and C, a keyless call returns a new key in the body. If that response were cacheable, the CDN would give the same key to every caller. So key-minting responses are `Cache-Control: no-store`. Under option C this also means keyless data is not cached: every keyless call reaches the origin, so its per-IP limit must be low. **[decision]**

### Instrumentation
- Before launch, the service counts requests, valid fresh responses, stale responses, errors and availability, each reported separately. It also counts distinct and returning projects, and anonymous counts are labeled as estimates. **[decision]**
- Health includes freshness, not only liveness. **[decision]**
- Operations (the owner, alerts, the incident path, the status source, support, deployment, the data feed to the program dashboard and the launch checklist) are in the free-tier runbook, not in this design.

### Which feeds the free tier serves
- **The free tier serves only feeds on an explicit allowlist.** **[decision]**
- **DoubleZero's own feeds** join the allowlist after their redistribution rights are cleared (*Risks*). DoubleZero publishes them, so DoubleZero opts them in. **[decision]**
- **Builder feeds** (edge-builder RFC-28) are optional (O15). If the free tier serves them, a builder feed joins only when its builder opts in. Under that RFC, the builder decides who holds a seat, a feed must be sold to outside subscribers, and a builder with know-your-customer obligations must control its subscriber list. A builder feed is eligible when it is active and passes conformance, its builder opts in and grants seats to the free-tier hosts, and DoubleZero approves the allowlist entry. **[decision]** Who pays for those seats, and how they count against bonds and onboarding caps, are open questions.
- **If builder feeds are served (O15), DoubleZero does not vouch for their data.** Responses name the builder as the publisher. Where they exist, the product list shows the feed's declared service level and DoubleZero's published measurements of it: conformance, service level, and divergence from the public feed that the builder named. Agents can then explain quality without DoubleZero judging the numbers, which matches RFC-28's "measuring rather than judging". **[decision]**
- **Feed lifecycle.** RFC-28 lets a builder halt and resume a feed, and retire it with thirty days' notice. During testnet, the core team can also revoke a publisher. The free tier follows the feed's status (*Response semantics*). A retired or revoked feed leaves the allowlist automatically. **[decision]**
- **Market data only.** Feeds on the future generic envelope carry payloads that the transport does not inspect, and the free API cannot interpret them either. **[decision]**
- **Surfaces.** The native surface and the MCP tools can serve any allowlisted feed whose reference data gives the venue, the instrument and the quote currency. The Binance-shaped surface serves a feed after its symbols and quote currency are mapped. **[decision]**

### Feed and subscription changes
**Adding a subscription for a feed that Edge Connect already knows** (the feed is in the registry, but the free-tier hosts do not subscribe to it yet) needs no restart. The hosts get a seat on the feed, subscribe with `doublezero multicast subscribe`, and Edge Connect starts the receiver within 30 seconds. Until the new channel is warm, its instruments return `503` + `Retry-After`. Other feeds are not affected. **[decision]**

**The feed registry is pinned.** By default every host fetches the registry at startup from `get.doublezero.xyz`, whose hosting still lives in an AWS account that is being closed. A tampered registry could map a feed's groups, ports or Source IDs wrongly, and the free tier would serve wrong prices under DoubleZero's name while every health check stays green. So the free-tier hosts load the registry from a file (Edge Connect's `DZ_FEED_REGISTRY`, which treats a bad or missing file as fatal), deployed by Ansible from a reviewed version with a recorded hash, and not from the URL. Moving the registry's hosting out of the old account is a separate launch prerequisite. **[decision]** A valid but altered file would look normal to every host, so Edge Connect computes the SHA-256 of the registry it loaded and shows it in its startup log, in `/v1/status` and as a metric. The canary and an alert compare it with the reviewed hash. The registry also rejects a group address that is not multicast. **[decision]**

**Adding a feed that Edge Connect does not know yet** needs a registry entry in the pinned file, then a rolling restart of the free-tier hosts, one at a time, because the registry has no hot reload. The warm-up snapshot keeps each restart invisible to callers. The steps for a subscription then follow. **[decision]** A publisher that changes an existing feed's groups, ports or channel set also changes the registry, so the same rolling restart applies. If the registry is later read from the ledger (Edge Connect's registry code leaves room for this), the registry edit goes away, but the restart stays unless hot reload is added.

The step-by-step procedures, including removing a feed, and the CLI and Edge Connect behavior they rely on, are in the free-tier runbook.

### Origin server capacity
**The feeds, not the HTTP traffic, set the size of an origin host.** The CDN bounds HTTP traffic (*Origin protection*), but every host must receive and decode every allowlisted feed, whatever the traffic. Edge Connect's docs give a host subscribed to every current feed 209 sockets and about 1.6 GiB of requested receive buffers, against about 40 MiB for one publisher. The usual failure is softirq saturation with silent receive drops, which infra has already met on its recorders. **[decision]**

**Starting estimate:** `c6i.2xlarge` (8 vCPUs, 16 GiB of memory) for each host. Infra's five feed recorders run this type, and they receive the same feeds today. Edge Connect also keeps books, the TOB map and optional history in memory, which a recorder does not, so the estimate is a floor to confirm, not a final size. **[decision]**

**Confirm it before launch:** run one Edge Connect host subscribed to the full launch set through peak periods, and size the fleet from the measured receive drops, softirq, CPU and memory, with headroom for new feeds. The procedure is in the runbook. **[decision]** If one host cannot carry every feed, the fleet could be split into host groups by venue, with the CDN routing on the venue in the path. That would end the rule that every host holds the full state, so it is a later decision. Sharing the receive load with the feed recorders is optional (O14).

### Canary
The canary checks the free API from the caller's side. **[decision]**
- **Where it runs:** from at least two locations outside our hosts, through the CDN. A second probe also calls each origin host directly over the private path (pending the network-access proposal), because the cache and the failover can hide one bad host.
- **What it checks,** for a few busy instruments on each allowlisted feed:
  1. **Freshness:** `as_of` is within the feed's threshold. When the feed is down upstream, the response carries the stale or feed-down indicator, and the API does not report fresh data.
  2. **Correctness:** the quote matches the venue's own public data within a tolerance. Examples are Binance's public `bookTicker`, or, for a builder feed, the public feed named in its declaration. Where DoubleZero already publishes this comparison for a feed (edge-builder RFC-28 *Data integrity*), the canary uses that result and does not repeat it.
  3. **Consistency:** the hosts agree with each other, and REST and MCP return the same values.
  4. **Contract:** responses validate against the OpenAPI document and the reference shapes, and a representative client (for example `ccxt`) passes a periodic smoke test.
  5. **Behavior:** an unknown instrument gets `404` (not `503`), an ambiguous one lists its candidates, rate-limit and CORS headers are present, and halted and retired feeds get their own responses.
  6. **MCP:** call `server/discover`, list the tools (and compare their hash with the reviewed one), and call the quote tool, on both supported protocol versions.
  7. **Integrity:** quiet instruments are sampled as well as busy ones, since a planted quote on a quiet instrument is the attack that matters. Every host's registry hash, profile and image version match the reviewed ones, and no served symbol contains a character outside the allowlist.
- **One feed list.** The canary reads the free-tier allowlist, the same list that drives the subscriptions.
- **Its traffic is labeled** and excluded from usage counts, and its own key or rule keeps it from being rate-limited. It still passes through the CDN.
- **How canary failures are alerted, and how its results reach the public status source,** are in the free-tier runbook.

### Optional features
These are not required for v1. Each one is decided (v1, later, or dropped) as the work goes on.

| ID | Feature | Status |
|---|---|---|
| O1 | **Native REST endpoints:** moved into v1 (*Public API surface*) | v1 |
| O2 | **A self-describing API:** moved into v1 (*Public API surface*) | v1 |
| O3 | **Errors that say how to fix the problem:** covered in v1 by Problem Details errors (*Response semantics*) | v1 |
| O4 | A keyed tier (options B–D), after a keyless launch | later |
| O5 | **Candles** (REST and MCP). Probably not in the first release. The last hour comes from Edge Connect's memory (lost on restart unless the snapshot carries it). Longer history comes from S3 (Parquet files built from the pcap warehouse or written by the hosts), served behind a long CDN TTL, never from the shared ClickHouse cluster, which lags by minutes to hours and loads no Kalshi books | later |
| O6 | Shallow order book (`/api/v3/depth`, Kalshi `/orderbook`, 5 to 10 levels) | undecided |
| O7 | Binance futures surface (`/fapi/v1`) for perpetuals, with mark price and funding | undecided |
| O8 | Recognition of agent platforms (published ranges and request signatures) | undecided |
| O9 | The MCP session id as a signed per-session key. Dropped: MCP 2026-07-28 removed sessions | dropped |
| O10 | An optional client label, for measurement only | undecided |
| O11 | OAuth on the MCP connector, for a higher tier | later |
| O12 | Periodic TOB quote resend by publishers (an edge-feed-spec change, with Edge Connect dropping resends that repeat the last quote) | later |
| O13 | Two CDNs at once: moved to *Future work* | future |
| O14 | **Free-tier origins on the feed recorders,** as a cost optimization. See *Alternatives Considered* for the checks it needs first | undecided |
| O15 | **Builder feeds in the free tier,** under the rules in *Which feeds the free tier serves* | undecided |
| O17 | **An MCP translator off the origin hosts.** A serverless function behind the CDN (for example Lambda) takes each MCP `POST` and calls the public cached `GET` URLs through the CDN, so MCP data reads hit the cache and origin hosts see only cache misses. The costs: an extra round trip per call (the translator to the nearest CDN location and back: a few milliseconds on a hit, plus the origin fetch on a miss), cold starts, a second billed CDN request and WAF evaluation per call, and an exemption from the per-IP limit for the translator's own address. Running the translator on the origin hosts instead would gain almost nothing, because the `POST` itself must reach the origin | undecided |
| O18 | **Bulk tickers per venue** as static S3 objects behind the CDN (Binance's `bookTicker` with no `symbol`). Cheap to serve, but it conflicts with the "no bulk access" non-goal, which limits scraping | undecided |
| O19 | **WebMCP tools on doublezero.xyz** for browser agents (for example Chrome with Gemini), later. The open CORS policy already allows it | later |
| O16 | **A Kalshi-shaped surface** (`/kalshi/trade-api/v2/markets/{ticker}` and related paths) for callers who already use Kalshi's client. It derives the NO side from the YES side (a NO bid = 1 − the YES ask), and it leaves out Kalshi fields we have no data for (volume, open interest, settlement) | undecided |

### Future work
**Two CDNs at once.** Both CDNs would front the same origins with the same configuration, and a DNS layer would steer traffic and drop a CDN whose health checks fail. It would survive a whole-CDN outage. It is not planned, because it costs:
- two configurations to keep identical (cache rules and keys, rate limits, TLS certificates, origin settings), generated from one source;
- no CDN-specific edge code on either side, or the logic written twice;
- failover only as fast as the DNS TTL;
- DNS as the shared point of failure: if the steering runs in Cloudflare DNS, a Cloudflare-wide outage takes the steering down too, unless the API hostname is delegated to a second DNS provider;
- roughly double the origin load, because each CDN has its own cache;
- rate limits counted per CDN, so a caller spread across both gets up to twice the limit;
- an origin allowlist and an authenticated pull for each CDN.

The design keeps it possible: logic stays on the origin, and keys are verifiable anywhere.

### Deferred to implementation phase
- Exact field names and URL paths, within this design and the reference-surface document.
- The snapshot format, its exact cadence (about 10 s), the timeouts and the readiness thresholds.
- The reverse proxy software and its limits.
- Whether the MCP adapter runs inside Edge Connect or as a separate process on the host.
- Measuring warm-up time and history size before candles are enabled.

## Impact

**Codebase.**
- malbeclabs/doublezero-edge-connect gets the TOB quote map, the public routes and formats, the response semantics, the warm-up snapshot writer and loader, readiness, and a free-tier bind on loopback, separate from `/v1`.
- malbeclabs/infra gets the free-tier hosts, the snapshot buckets with per-host write permissions, TLS from the CDN to the origin, the CDN distribution with routing and failover, rate-limit rules, edge key checks if keys are used, metrics, alerts and the status source.
- The MCP adapter, the canary, and platform recognition (a range refresh job and signature verification) are new components.
- Infra also gets static IPs for the hosts, the on-host reverse proxy, the free-tier access-pass file, subscription management in the Ansible role, and the pinned feed-registry file.
- Edge Connect also gets: the `free-tier` profile, unprivileged packaging, the separate free-tier server, the source-address allowlist, the catalog cap, symbol sanitization and the registry hash.
- Infra also gets: attestation checks before pinning, audit-log alerting, the egress firewall, and the free-tier role (built from the `dz-ec2` and `doublezero_client` patterns, not from today's `edge_connect` role).
- The OpenAPI document and examples go to Web for publishing.
- **Separate follow-ups:** cap WebSocket frame size in Edge Connect's WebSocket sink (it protects customer installs too); fix today's edge-terminal host (moving image tag, IMDSv1); and, for infra's owners, registrar lock and DNSSEC on `doublezero.xyz`.

The work exceeds one PR. A rough split: (1) the TOB quote map, (2) the routes, formats and semantics, (3) the warm-up snapshot and readiness, (4) hosts, (5) the CDN and rate limits, (6) the MCP adapter.

**Operations.** The operational impact (the hosts, the CDN, the status source, alerts, on-call and cost) is in the free-tier runbook.

**Performance.** Origin load depends on the number of instruments and CDN locations, not on users. The TOB map adds one small entry per instrument (about 100k entries). Customer Edge Connect installs are unaffected, because the new features are off by default.

**User experience and documentation.** A new public contract (OpenAPI, examples, limits, status) needs maintenance and versioning. Web, Marcomm and UR depend on it.

### Risks
- **The venues may not grant the right to serve their quotes for free.** Kalshi, Hyperliquid, Binance and the other venues behind the feeds may refuse free public redistribution, allow only delayed data, add conditions (a form of attribution, a rate cap, non-commercial use only), or withdraw permission later. Each outcome must be possible without a release: a venue can be enabled or disabled, and given a delay, by configuration. Removing a venue means taking it off the allowlist. **[decision]** The rights inventory is a launch gate. The plan's headline example, "get the latest BTC price", assumes Binance spot. If Binance's rights are delayed or refused, the launch example could be 2Z on Hyperliquid, subject to Hyperliquid's terms. It worked well in testing of an earlier DoubleZero agent API, and it keeps the demonstration about DoubleZero.
- **Few builders may opt in.** Builder feeds are sold, and a free channel competes with a builder's own sales unless the builder sees it as a way to find customers.
- **Feed growth can outrun the hosts** (*Origin server capacity*).
- **Builder-feed policy is not settled.** Edge-builder RFC-28 says the protocol does not ship a frontend for builder-deployed feeds, and it leaves bond scaling and onboarding caps open (*Open Questions*).

Ways to increase agent traffic to the free API (search, directories, packages, mentions) are outside this design. Marcomm, Web and UR own them.

## Security Considerations

- **Public exposure of an unauthenticated API.** `/v1`, admin and metrics bind to loopback today and must stay unreachable. The free tier is served through the on-host reverse proxy, which holds the route allowlist, with TLS from the CDN to the origin. **Preferred: no inbound internet listener at all.** With CloudFront VPC origins or a Cloudflare Tunnel, the hosts have no port open to the internet, hold no public certificate key, and need no DNS token for certificate renewal. Whether a VPC origin works for an instance that also holds the Elastic IP its access pass is pinned to is unverified, and the routing proof checks it. **[decision]** **Otherwise, the origin requires an authenticated pull from our own CDN configuration.** A CDN's published origin-facing ranges are shared by all its customers, so anyone could point their own distribution or zone at our origin and skip our rate limits and cache. On CloudFront: the origin-facing prefix list **and** a rotated secret origin header, or CloudFront's origin mTLS. On Cloudflare: Authenticated Origin Pulls with our own certificate, not the shared global one. The routing's health checkers (for example Route 53's) are admitted too, on the readiness path only. **[decision]**
- **The warm-up snapshot** lives in buckets in the free-tier account. Each host can write only its own key prefix. A compromised host could still write a bad snapshot, so a loader accepts only a snapshot written after it subscribed by a warm writer, parses it strictly with a size limit, and keeps every copied value `restored` until a live update or an MBP snapshot confirms it.
- **The feed registry is pinned** to a reviewed file with a recorded hash (*Feed and subscription changes*), because a tampered registry would publish wrong prices while health checks stay green.
- **Stale bodies:** `stale-while-revalidate` and `stale-if-error` are capped at a few seconds, so a body that says "fresh" is not served for long after it stops being true.
- **Key-minting responses are `no-store`,** so the CDN never hands one caller's key to another.
- **Secrets live in AWS Secrets Manager** in the free-tier hosts' account, not in SOPS files or GitHub secrets: the HMAC signing keys for API keys (if a keyed tier is added), the origin TLS keys and authenticated-pull certificates, the secret origin header value, and each host's DoubleZero user keypair. Rotation schedules are in the runbook. Keys are HMAC-signed, unguessable and revocable through a denylist, and they are accepted only in a header, never in a URL.
- **Abuse, scraping and redistribution:** named instruments only, per-IP limits, platform pools with per-session limits, and no bulk endpoint. These bound how much one caller can collect, and they do not eliminate scraping.
- **Cost exhaustion.** CDN request charges grow with abusive traffic, so rate limits at the edge and cost alarms are required.
- **MCP:** the server validates the `Origin` header (the transport specification requires this against DNS rebinding), holds no state between calls, and answers with single JSON responses, never streams.
- **CORS:** `*` with no credentials. The API uses no cookies.
- **Platform recognition:** IP ranges and verified signatures only. `User-Agent` and client labels are unverified and never grant higher limits.
- **Privacy:** CDN and origin logs contain IP addresses and keys. They are kept for a limited period (set in the free-tier runbook), and only aggregates leave them.
- **Data integrity:** stale data is always flagged, and fields with no source are left out rather than invented. Builder data is attributed to its builder, and DoubleZero does not vouch for it.
- **Revocation:** the free-tier hosts are subscribers too, and the core team's testnet power to revoke publishers and subscribers applies to them. A revoked feed leaves the free tier automatically.
- **Customer installs:** the new features are off by default, so the attack surface of customers' Edge Connect does not change.
- **Images are pinned by digest,** not by tag, so a re-pushed tag cannot change what the hosts run. The image build publishes provenance attestations, and infra verifies the attestation (the expected workflow and source commit) before a digest can be pinned. A scheduled check alerts if a host's running digest differs from the pin.
- **Certificates and DNS tokens never sit on a public host.** If a public certificate is needed, a central renewal job in the free-tier account issues it with a DNS token scoped to a delegated `_acme-challenge` zone, and pushes it to each host's secret. CAA records name only the CA in use, and Certificate Transparency monitoring alerts on any certificate for our domains that the job did not create.
- **Each host's DoubleZero keypair is a plain subscriber key:** no publisher allowlist entries, no admin or multicast-admin permission, and a balance sized for fees only. The fleet keypair that the recorders share is never used.
- **Change control.** Every free-tier CDN, WAF, DNS, bucket-policy and IAM object is in Terraform in the free-tier account, and changes need a second reviewer. Changes are applied with a dedicated role that requires MFA. CloudTrail and the CDN's and DNS provider's audit logs alert on any write that didn't come from that role in a recorded change window. This is the control that catches a stolen credential used quietly.
- **Host posture.** Hosts are rebuilt from a known image at least monthly and on critical CVEs, rather than patched in place. Outbound traffic is limited by a firewall allowlist (the security group, plus host nftables). SSH is reachable only over the private path, never from the internet, with key-only logins and logged sessions.
- **Logs.** Raw logs that hold IP addresses are kept 30 days. Audit logs are kept a year, and authentication failures and WAF samples 90 days with client addresses hashed. All of this goes through the privacy review.
- **Account placement:** the hosts are internet-facing, so they are not placed in an account by default (*Open Questions*).
- **Security review:** a new public endpoint goes through the organization's threat-model review of public APIs.

## Backward Compatibility

- **Customer Edge Connect installs are unchanged.** The new features are behind flags that are off by default. `/v1` and the WebSocket protocol do not change.
- **Customer releases do not restart the free-tier fleet.** The fleet pins its image by digest and upgrades on its own schedule (see the free-tier runbook).
- **Public API versioning:** the native surface is versioned (`/v1/...`) and changes additively within a version. The Binance-shaped surface keeps Binance's `/api/v3` paths and changes only additively. It does not track Binance's later API versions.
- **The optional TOB resend** would be an additive feed-spec change. Edge Connect drops resends that repeat the last quote it sent, so downstream consumers see no duplicates.

## Open Questions

1. **The reference-surface document** (*Reference surface*): the launch markets, the Binance endpoints implemented, the field and error mapping, the differences from Binance, and the client-test results. It is needed before endpoint work starts.
2. **Keys:** decided: launch keyless (*Access and rate limits*). Open: whether and when to add a keyed tier, and which option.
3. **Redistribution rights:** do Binance's, Hyperliquid's, Kalshi's and other venues' terms allow free public redistribution of live quotes? The answer may require delayed data for some venues, or exclude a venue (*Risks*). Engineering prepares the rights and coverage inventory, and a commercial or legal owner decides.
4. **Launch venues and markets.** "Get the latest BTC price" points to Binance spot `BTCUSDT`. The answer decides whether the TOB map is needed at launch.
5. **CDN vendor:** CloudFront (it fits the existing AWS estate and already serves `get.doublezero.xyz`) or Cloudflare (it runs our DNS, and the organization's review of public endpoints considers Cloudflare in front of them). The vendor must support request merging, short `stale-if-error`, per-IP limits, a configurable cache key, an authenticated origin pull and, if keys are used, per-key counting at the edge. This closes only after the routing proof (*Architecture*) passes on the candidate. Two security findings weigh toward Cloudflare: a Cloudflare Tunnel gives the origin no inbound listener directly, and Cloudflare's flat plans don't bill per request under attack, while CloudFront bills every request, blocked or not. The case for CloudFront is lower cost at launch volumes and its fit with the AWS estate.
6. **Candle retention** beyond Edge Connect's one-hour window would need storage. It is not in v1 unless decided.
7. **Hostname:** decided, a subdomain of `doublezero.xyz` (*Public API surface*). Agents can tell a real DoubleZero endpoint from a lookalike only by its domain and by what links to it. In testing, an agent stopped to check a server on a non-DoubleZero domain before trusting it. Open: the exact name, and the API versioning, both agreed with Web.
8. **Kalshi `series_ticker`:** does the Kalshi feed's reference data carry it? Kalshi-shaped candles (O16 with O5) need it.
9. **Agent-platform egress behavior:** confirm the published ranges, and check whether other target platforms publish ranges or sign requests.
10. **The free tier and builder feeds:** edge-builder RFC-28 says the protocol does not ship a frontend for builder-deployed feeds. Is the free API such a frontend, or a distribution path that a builder can opt into, next to "its own entitlement system" and "DoubleZero purchasing and guided access"?
11. **Free-tier seats on builder feeds:** seats are the billing unit, one per free-tier host. Who pays for those seats on an opted-in feed? Are those seats exempt from a bond that scales with subscriber count, and from per-feed onboarding caps (questions 3 and 5 of malbeclabs/edge-builder#117)?
12. **Feed metadata:** does each feed's reference data carry the quote currency and the market type, or do some feeds need a manual entry before the native surface can serve them?
13. **IPv6:** does the API serve IPv6 at all? Recommended: not at launch. If it does, per-IP limits key on the /64, plus a higher limit on the /48 if the CDN supports it.
14. **Account placement:** which AWS account holds the internet-facing hosts, the CDN and the routing? The organization's account and guardrail decisions (the AWS baseline and its SCP/RCP rollout) govern this, and this design has not yet been checked against them. Proposal, to confirm against them: a dedicated account for the free tier, separate from the accounts that hold the recorders, payments and data pipelines. A compromised public host then cannot reach their credentials, and the free tier's CDN, WAF, routing and Secrets Manager sit together under the same guardrails.
15. **Network access for the private path** (the canary's direct probes and SSH access): pending the separate network-access proposal, including whether these hosts join the main tailnet or a separate one. The security review recommends not the main tailnet, because a public host there would be a bridge into the recorders and QA hosts. That recommendation is input to the proposal, not a decision here.
16. **Kalshi quotes from MBP (research pending).** MBP feeds resend every instrument's book on a snapshot cycle, so a host subscribed to a venue's MBP groups rebuilds the top of book for quiet instruments within one cycle, with no warm-up. If the free tier takes Kalshi's best bid and ask from MBP rather than top-of-book, most of the roughly 100k quiet instruments would need no snapshot. This would change the source order in *Quote data* for Kalshi. To measure first: whether every Kalshi series has an MBP group, and each group's actual snapshot cycle period.
17. **Host count and size (measurement pending).** The candidates are six `c6i.2xlarge` hosts (two in each of three regions), one host in each of three regions, two hosts in each of two regions, and smaller hosts (for example `c6i.large`) that receive only the free-tier feeds. Six `c6i.2xlarge` cost about $1,489 a month in us-east-1, more elsewhere, and are most of the service's cost. To measure: the ingest CPU and network of the launch feeds on a smaller host. With one host per region, every restart or failure moves that region's traffic to another region, which costs latency on cache misses, not freshness.
18. **One process or two.** The free-tier routes run inside Edge Connect, isolated by an unprivileged user, a separate server and lock-free snapshots (*Architecture*). The alternative is a thin sibling process that reads a snapshot of only the quote map and catalog, which isolates serving from ingest further at the cost of a second process.
19. **Publisher allowlist enforcement:** do the DoubleZero devices enforce each group's publisher allowlist by source address, and can the free tier get the authoritative publisher addresses per group from the ledger, rather than from infra's inventory?
20. **Builder-feed text:** if builder feeds are served (O15), is there a conformance rule for text fields, and can DoubleZero refuse an allowlist entry because of symbol content alone?
