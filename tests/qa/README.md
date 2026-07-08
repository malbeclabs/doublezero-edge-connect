# Live install-and-connect QA (`tests/qa/`)

An **end-to-end** QA that runs the published `curl | bash` one-liner exactly as a
user would and asserts the edge-connect client **installs and connects** on
**testnet** and **mainnet**.

This is deliberately separate from the hermetic bats suite in
[`../scripts/`](../scripts/):

| | `tests/scripts/*.bats` | `tests/qa/connect_e2e.sh` (this) |
|---|---|---|
| Network / Docker | stubbed (offline) | **real** — pulls the image, runs the container, joins DoubleZero |
| What it proves | installer *logic* (argv it hands `docker`) | the client actually **installs + connects** |
| Where it runs | any CI runner (`bats tests/scripts/`) | a provisioned Linux/amd64 host (self-hosted) |
| Cost / determinism | fast, hermetic | slow, needs onchain access pass |

It lives under `tests/qa/` (not `tests/scripts/`) so the hermetic
`bats tests/scripts/` CI job never picks it up.

## What it asserts

For each environment (run once per env), in tiers:

**A1 — install:** the bridge container is running, logged `doublezerod ready`, and
did not exit early.

**A2 — connect:** `doublezero status --json` reports the tunnel session **up**
(`session_status == "BGP Session Up"`, legacy `"up"`) within a timeout. Schema and
values mirror the doublezero qaagent (`e2e/internal/rpc/agent.go`, `IsStatusUp`).

**Tier 1 — deterministic integrity** (always; no feed subscription needed):

- **T1.1** no `panic`/fatal in the bridge logs.
- **T1.2** the container is stable (`Running`, `RestartCount == 0`) after a settle
  window — not crash-looping.
- **T1.3** the `doublezero1` tunnel interface exists and is **UP** on the host.
- **T1.4** `status --json` fields are populated (`doublezero_ip`, `current_device`,
  `tunnel_dst`) — an "up" session with no device/IP is suspect.
- **T1.5** `doublezero latency --json` measures a device latency > 0 — proof the
  tunnel actually carries traffic (data-plane analog of the doublezero QA's
  ping-through-tunnel). Skip with `DZ_QA_SKIP_LATENCY=1`.
- **T1.6** the Prometheus metrics endpoint (relayed on via `METRICS_BIND`) serves
  `GET /healthz` (200) and `dz_` series at `/metrics` — proves the **bridge**
  process (not just the daemon) is alive.
- **T1.7** the running image matches the environment (`:testnet` / `:mainnet-beta`)
  — catches a mislabeled/stale pull (skipped if `DZ_IMAGE` is overridden).
- **T1.8** a **token**-derived key is not written to host disk (the installer's
  promise; skipped when `DZ_SECRET` is a file path, which is bind-mounted by design).

**Tier 2 — the market-data product path** (opt-in via `DZ_QA_EXPECT_FEED=1`; the
host must be **subscribed to a market-data group**, else these are non-deterministic
and skipped):

- **T2.1** the reconciler logged `activating WebSocket sink`.
- **T2.2** ⭐ inner multicast is actually flowing (`dz_datagrams_received_total > 0`,
  `dz_feed_up == 1`) — this catches the exact **silent** failure the installer can
  only *warn* about (a default-deny host firewall dropping the decapsulated inner
  multicast on `doublezero1`) and that A2 cannot see.
- **T2.3** normalized data is produced (`dz_quotes_admitted_total` /
  `dz_trades_admitted_total` > 0).
- **T2.4** the WS serves valid PROTOCOL.md frames end-to-end: a stdlib-only client
  (`ws_probe.py`) connects to `ws://127.0.0.1:<WS_QA_PORT>` and sees an `instrument`
  definition **and** a market-data message (`quote`/`trade`/`midpoint`/`depth`).

**Tier 3 — robustness:**

- **T3.1** error-counter ceilings (`dz_socket_errors_total` ≤ `MAX_SOCKET_ERRORS`,
  default 0) — a soft warning unless `DZ_QA_STRICT=1`.
- **T3.2** shred forwarding (opt-in via `DZ_QA_EXPECT_SHREDS=1`; needs an
  `edge-solana-*` subscription): `activating shred forwarder` +
  `dz_shred_forwarded_total > 0`.

**Tier 0 — harness self-integrity:**

- **Teardown verification** (every successful run): after disconnect + remove, the
  host is asserted clean — no `dz-qa-*` container and **no `doublezero1`** — so a
  leaked tunnel can't poison the next env/run.
- **Negative self-test** (opt-in via `DZ_QA_NEGATIVE=1`, run with a secret whose IP
  has **no access pass**): the tunnel must **not** come up (or the installer's
  pre-check must refuse). If it *does* connect, the harness fails loudly — this is
  what proves A2 can tell a broken connection from a good one (no false green).
  Skips Tiers 1–3.

  ```bash
  DZ_SECRET=DZ_<unprovisioned> DZ_QA_NEGATIVE=1 bash tests/qa/connect_e2e.sh --env testnet
  ```

Without `DZ_QA_EXPECT_FEED`, the run stays fully deterministic (A1/A2 + Tier 1 +
Tier 3.1) — WS serving is not asserted because it is subscription-gated.

## Run it manually

```bash
# testnet
DZ_SECRET=DZ_…  bash tests/qa/connect_e2e.sh --env testnet
# mainnet
DZ_SECRET=DZ_…  bash tests/qa/connect_e2e.sh --env mainnet-beta
```

Before installing, the harness **reclaims the host**: if the server carries a
leftover doublezero connection from an earlier run, it runs `doublezero disconnect`
(and removes any leftover `dz-qa-*` container) so the install starts from a clean
slate. On exit it always tears down — `doublezero disconnect`, then removes the
container **and the pulled image** (`DZ_QA_REMOVE_IMAGE=0` to keep it) — so the
server is left ready for the next QA run.

### Inputs (env vars)

| Var | Default | Purpose |
|-----|---------|---------|
| `DZ_SECRET` | *(required)* | `DZ_`-prefixed token or a keypair file path, whose identity has an access pass onchain for the host's public IP in the chosen env. |
| `--env` | *(required)* | `testnet` or `mainnet-beta`. |
| `DZ_INSTALL_BASE_URL` | `https://get.doublezero.xyz` | Origin of the one-liner. Override to smoke-test a preview origin. |
| `DZ_QA_CLIENT_IP` | *(unset)* | If set, passed as `DZ_CLIENT_IP` so the installer's access-pass pre-check is **strict** (a confirmed miss aborts instead of warn-and-continue). |
| `DZ_QA_EXPECT_FEED` | `0` | `1` enables Tier 2 (host must be subscribed to a market-data group). |
| `DZ_QA_EXPECT_SHREDS` | `0` | `1` enables T3.2 (host must be subscribed to an `edge-solana-*` group). |
| `DZ_QA_NEGATIVE` | `0` | `1` runs the Tier 0 negative self-test (unprovisioned secret must NOT connect); skips Tiers 1–3. |
| `NEG_TUNNEL_TIMEOUT` | `90` | How long the negative test waits to confirm the tunnel stays down (s). |
| `DZ_QA_STRICT` | `0` | `1` turns Tier 3 ceilings from warnings into failures. |
| `DZ_QA_SKIP_LATENCY` | `0` | `1` skips T1.5. |
| `DZ_QA_REMOVE_IMAGE` | `1` | `0` keeps the pulled image on teardown (faster iteration); default removes it to leave the server clean. |
| `WS_QA_PORT` / `METRICS_QA_PORT` | `18081` / `19090` | QA-only ports (avoid clobbering a real edge-connect on `:8081`/`:9090`). |
| `INSTALL_READY_TIMEOUT` / `TUNNEL_UP_TIMEOUT` / `LATENCY_TIMEOUT` / `FEED_TIMEOUT` | `90` / `120` / `45` / `90` | Per-phase timeouts (seconds). |
| `DZ_QA_LOCKFILE` | `/var/lock/dz-qa.lock` | Host-level mutex (see Isolation). |

## Host prerequisites

- **Linux / amd64** with Docker, `/dev/net/tun`, `flock`, `curl`, `jq`
  (assertions fall back to text matching without it), `python3` (Tier 2 WS probe),
  and GRE (IP proto 47) allowed.
- The host's **public IP** provisioned with an **access pass onchain** for
  testnet **and** mainnet (or the `0.0.0.0` wildcard):
  ```bash
  doublezero access-pass set --accesspass-type prepaid \
    --user-payer <IDENTITY> --client-ip <IP|0.0.0.0>
  ```
  Without it, `doublezero connect` won't bring the tunnel up and **A2 fails** —
  which is exactly the failure this QA is meant to catch.
- Cloud hosts: allow GRE at the provider level (and on AWS disable the ENI
  source/dest check), as the installer itself warns.

## Isolation vs the doublezero client QA (important)

The `doublezero` repo runs its own QA (`qa.devnet.yml`) that drives
`doublezero connect`/`disconnect` against the **host daemon** via a `qaagent`.
Because edge-connect runs `--network host`, the `doublezerod` **inside** our
container creates the same `doublezero1` on the host and uses the **same public
IP's** access pass. If the two QAs overlap on one server, **both corrupt each
other**.

The harness guards against this:

1. **Prefer a dedicated host** — one not in the doublezero QA host list and
   without its `qaagent`. This is the intended setup.
2. If a host must be shared, the harness:
   - re-runs itself under `flock -n /var/lock/dz-qa.lock` — the only cross-repo
     mutex on a shared box (GitHub Actions `concurrency` is intra-repo). If the
     lock is held, it **skips cleanly** (exit 0, loud SKIP) rather than clobber an
     in-flight run;
   - **skips** if a `doublezero-qaagent` is running — that signals the doublezero
     *client* QA infra is active, and we won't tear its connection out from under
     it (coordinate via the flock / a dedicated runner instead);
   - otherwise **reclaims** the host: a leftover `doublezero1` (from an earlier run,
     safe to disconnect since we hold the lock and there's no foreign qaagent) is
     `doublezero disconnect`-ed — via a leftover `dz-qa-*` container or a
     host-installed CLI — and, as a last resort, an orphaned interface is deleted.

   For full safety on a shared host, have the doublezero QA take the same
   `/var/lock/dz-qa.lock`, and schedule this workflow in a different window than
   the doublezero QA cron (`0 14 * * 1-5`).

## CI

`.github/workflows/qa.connect.yml` runs this on a self-hosted runner, on
`workflow_dispatch` (pick `both` / `testnet` / `mainnet-beta`) and a schedule. It
is **not** a PR check.
