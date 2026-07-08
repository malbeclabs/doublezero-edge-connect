#!/usr/bin/env bash
#
# Live e2e QA for the published one-liner installer.
# ---------------------------------------------------
# Unlike the hermetic bats suite under tests/scripts/ (which stubs docker/curl/ss
# and asserts on the argv the installer *tried*), this harness runs the REAL,
# CDN-published one-liner exactly as a user would, then asserts the client
# actually installed, connected, and — where the host is subscribed to a feed —
# is serving normalized market data.
#
# Assertion tiers (see tests/qa/README.md for the rationale):
#   A1 (install)   the bridge container is running, logged "doublezerod ready",
#                  and did not exit early.
#   A2 (connect)   `doublezero status --json` reports the tunnel session UP.
#   Tier 1         deterministic integrity of the installed client (no panics, not
#                  crash-looping, doublezero1 up, status fields sane, latency,
#                  metrics/healthz reachable, expected image, key not on host disk).
#   Tier 2         the product path — ONLY when DZ_QA_EXPECT_FEED=1 (the host must be
#                  subscribed to a market-data group): sink activated, inner
#                  multicast flowing, data admitted, WS serves valid PROTOCOL.md
#                  frames.
#   Tier 3         robustness: error-counter ceilings; optional shred forwarding.
#
# One environment per invocation (the workflow calls it once per env), so each is
# a separately-reportable step and the teardown between them is clean.
#
# Usage:
#   DZ_SECRET=DZ_… bash tests/qa/connect_e2e.sh --env testnet
#   DZ_SECRET=DZ_… DZ_QA_EXPECT_FEED=1 bash tests/qa/connect_e2e.sh --env mainnet-beta
#
# Requires: a Linux/amd64 host with Docker, /dev/net/tun, GRE (IP proto 47), flock,
# python3, and the host's public IP provisioned with an access pass onchain for the
# chosen environment. See tests/qa/README.md. Runs non-interactively.

set -euo pipefail

# ----------------------------------------------------------------------------
# config / inputs
# ----------------------------------------------------------------------------
DZ_QA_ENV="${DZ_QA_ENV:-}"
DZ_SECRET="${DZ_SECRET:-}"
# Where the one-liner is served from. Override to smoke-test a preview origin.
DZ_INSTALL_BASE_URL="${DZ_INSTALL_BASE_URL:-https://get.doublezero.xyz}"
# Optional: assert the access pass for this exact IP (makes a confirmed miss fatal
# in the installer's pre-check instead of a warn-and-continue).
DZ_QA_CLIENT_IP="${DZ_QA_CLIENT_IP:-}"
# Tier 2 (product path) is opt-in: only meaningful when this host is subscribed to a
# market-data group, which is not guaranteed on a fresh QA host.
DZ_QA_EXPECT_FEED="${DZ_QA_EXPECT_FEED:-0}"
# Tier 3 shred path is opt-in: only when subscribed to an edge-solana-* group.
DZ_QA_EXPECT_SHREDS="${DZ_QA_EXPECT_SHREDS:-0}"
# Tier 0 negative self-test: run with a secret whose IP has NO access pass and
# assert the tunnel does NOT come up (proves A2 can tell a broken connection from a
# good one — guards against a false-green harness).
DZ_QA_NEGATIVE="${DZ_QA_NEGATIVE:-0}"
# Teardown removes the pulled image too (default on) to leave the QA server clean
# and make the next run re-exercise the pull. Set 0 to keep it (faster iteration).
DZ_QA_REMOVE_IMAGE="${DZ_QA_REMOVE_IMAGE:-1}"

# Isolation: a dedicated container name + non-default ports so this QA never
# clobbers a real edge-connect on the host and never fights for :8081/:9090.
WS_QA_PORT="${WS_QA_PORT:-18081}"
METRICS_QA_PORT="${METRICS_QA_PORT:-19090}"
# Host-wide lock: the only cross-repo mutex on a shared box (GitHub Actions
# concurrency is intra-repo). The doublezero QA drives `doublezero connect` on the
# same host daemon; a `--network host` bridge here would collide on doublezero1.
LOCKFILE="${DZ_QA_LOCKFILE:-/var/lock/dz-qa.lock}"

# Timeouts (seconds).
INSTALL_READY_TIMEOUT="${INSTALL_READY_TIMEOUT:-90}"   # A1: container up + "doublezerod ready"
TUNNEL_UP_TIMEOUT="${TUNNEL_UP_TIMEOUT:-120}"          # A2: session_status up
NEG_TUNNEL_TIMEOUT="${NEG_TUNNEL_TIMEOUT:-90}"         # T0-neg: confirm it stays down (long enough a good link would be up)
TEARDOWN_VERIFY_TIMEOUT="${TEARDOWN_VERIFY_TIMEOUT:-15}" # T0: doublezero1 gone after disconnect
LATENCY_TIMEOUT="${LATENCY_TIMEOUT:-45}"               # Tier 1: device latency populated
FEED_TIMEOUT="${FEED_TIMEOUT:-90}"                     # Tier 2: sink active + data flowing
SETTLE_SECS="${SETTLE_SECS:-5}"                        # Tier 1: crash-loop settle window

# Tier 3 ceilings (soft warnings unless DZ_QA_STRICT=1).
DZ_QA_STRICT="${DZ_QA_STRICT:-0}"
MAX_SOCKET_ERRORS="${MAX_SOCKET_ERRORS:-0}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ----------------------------------------------------------------------------
# pretty output
# ----------------------------------------------------------------------------
if [ -t 1 ]; then BOLD=$'\033[1m'; RED=$'\033[31m'; YEL=$'\033[33m'; GRN=$'\033[32m'; RST=$'\033[0m'
else BOLD=; RED=; YEL=; GRN=; RST=; fi
info() { printf '%s==>%s %s\n' "$GRN" "$RST" "$*"; }
warn() { printf '%s!! %s%s\n' "$YEL" "$*" "$RST" >&2; }
die()  { printf '%sxx %s%s\n' "$RED" "$*" "$RST" >&2; exit 1; }
pass() { printf '%s  ok%s %s\n' "$GRN" "$RST" "$*"; }
# A benign skip (another QA in flight / a foreign tunnel present): exit 0 loudly so
# a scheduled run doesn't clobber someone else's run, but the job isn't marked red.
skip() { printf '%s-- SKIP: %s%s\n' "$YEL" "$*" "$RST" >&2; exit 0; }

usage() { echo "usage: $0 --env <testnet|mainnet-beta>" >&2; exit 2; }

# ----------------------------------------------------------------------------
# arg parse
# ----------------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --env) DZ_QA_ENV="${2:-}"; shift 2 || usage ;;
    --env=*) DZ_QA_ENV="${1#*=}"; shift ;;
    -h|--help) usage ;;
    *) warn "unknown arg: $1"; usage ;;
  esac
done

case "$DZ_QA_ENV" in
  testnet)      INSTALL_PATH="connect-testnet"; EXPECT_IMAGE_TAG="testnet" ;;
  mainnet-beta) INSTALL_PATH="connect";         EXPECT_IMAGE_TAG="mainnet-beta" ;;
  "") die "missing --env (testnet|mainnet-beta)" ;;
  *) die "unsupported --env '$DZ_QA_ENV' (want testnet|mainnet-beta; devnet is out of scope)" ;;
esac
[ -n "$DZ_SECRET" ] || die "DZ_SECRET is required (a DZ_-prefixed token or a keypair file path)."

DZ_NAME="dz-qa-${DZ_QA_ENV}"
INSTALL_URL="${DZ_INSTALL_BASE_URL}/${INSTALL_PATH}"
METRICS_URL="http://127.0.0.1:${METRICS_QA_PORT}"

# ----------------------------------------------------------------------------
# host lock (re-exec under flock so the whole run is mutually exclusive)
# ----------------------------------------------------------------------------
if [ -z "${DZ_QA_LOCKED:-}" ]; then
  command -v flock >/dev/null 2>&1 || die "flock not found (util-linux); required for host-level QA isolation."
  # Re-run the whole script under the host lock. -E 75 makes lock-contention exit
  # with a distinct code so we can tell "someone else holds it" (clean skip) from
  # "the run itself failed" (which passes through as flock returns the child's code).
  set +e
  env DZ_QA_LOCKED=1 flock -n -E 75 "$LOCKFILE" "$0" --env "$DZ_QA_ENV"
  rc=$?
  set -e
  if [ "$rc" -eq 75 ]; then
    skip "another DoubleZero QA holds $LOCKFILE — not clobbering an in-flight run."
  fi
  exit "$rc"
fi

# ----------------------------------------------------------------------------
# sudo (self-run may be unprivileged; docker/ip may need root)
# ----------------------------------------------------------------------------
if [ "$(id -u)" -eq 0 ]; then SUDO=""; else SUDO="sudo"; fi

# ----------------------------------------------------------------------------
# preconditions + exclusion preflight
# ----------------------------------------------------------------------------
[ "$(uname -s)" = Linux ] || die "Linux hosts only (got $(uname -s))."
command -v docker >/dev/null 2>&1 || die "docker not found on host."
command -v jq >/dev/null 2>&1 || warn "jq not found; assertions fall back to text matching."
command -v curl >/dev/null 2>&1 || die "curl not found on host."

# A running doublezero-qaagent means the doublezero *client* QA infra is active on
# this host — skip rather than tear its connection out from under it (we coordinate
# with that QA via the flock / a dedicated runner, not by force). -f (match full
# cmdline): the comm is truncated to 15 chars, so `pgrep -x` would never match.
if pgrep -f doublezero-qaagent >/dev/null 2>&1; then
  skip "doublezero-qaagent is running on this host — the doublezero QA may be mid-run."
fi

# Reclaim the host before installing: a QA server can carry a leftover doublezero
# connection from a previous run, which would collide with our --network host
# bridge. We hold the flock and there is no foreign qaagent, so anything here is a
# leftover — disconnect it and start from a clean slate.
reclaim_host() {
  local c leftovers gone=""
  # 1. Leftover edge-connect QA containers (ours): disconnect gracefully, then remove.
  leftovers="$($SUDO docker ps -aq --filter "name=^dz-qa-" 2>/dev/null || true)"
  if [ -n "$leftovers" ]; then
    warn "reclaim: found leftover dz-qa-* container(s); 'doublezero disconnect' + remove."
    for c in $leftovers; do
      $SUDO docker exec "$c" doublezero disconnect >/dev/null 2>&1 || true
      $SUDO docker rm -f "$c" >/dev/null 2>&1 || true
    done
  fi
  # 2. A host-installed doublezero client (not in a container): disconnect it too.
  if command -v doublezero >/dev/null 2>&1; then
    $SUDO doublezero disconnect >/dev/null 2>&1 || doublezero disconnect >/dev/null 2>&1 || true
  fi
  # 3. Confirm the tunnel is gone (give a graceful disconnect a moment to land).
  ip link show doublezero1 >/dev/null 2>&1 || { info "reclaim: host has no doublezero connection."; return 0; }
  warn "reclaim: doublezero1 still present; waiting for it to drop."
  for _ in $(seq 1 "$TEARDOWN_VERIFY_TIMEOUT"); do
    ip link show doublezero1 >/dev/null 2>&1 || { gone=1; break; }
    sleep 1
  done
  # 4. Last resort: delete an orphaned tunnel interface (no owning daemon/container).
  if [ -z "$gone" ]; then
    warn "reclaim: deleting orphaned doublezero1 interface."
    $SUDO ip link delete doublezero1 >/dev/null 2>&1 || true
    ip link show doublezero1 >/dev/null 2>&1 \
      && die "could not reclaim host: doublezero1 persists; refusing to install onto a connected host."
  fi
  info "reclaim: host disconnected from doublezero."
}
reclaim_host

# ----------------------------------------------------------------------------
# teardown (always: disconnect + remove so the host is clean for the next env/run)
# ----------------------------------------------------------------------------
SCRATCH=""
SUCCESS=0   # set to 1 only when all requested checks passed (gates teardown verify)
teardown() {
  info "Teardown: 'doublezero disconnect', then destroy the container (and image)."
  local img=""
  img="$($SUDO docker inspect -f '{{.Config.Image}}' "$DZ_NAME" 2>/dev/null || true)"
  # Disconnect from the service FIRST so the tunnel is cleanly torn down before the
  # container (which owns the daemon) goes away — leaves the host ready for the next
  # QA run rather than leaking a doublezero1 the daemon never dropped.
  $SUDO docker exec "$DZ_NAME" doublezero disconnect >/dev/null 2>&1 || true
  $SUDO docker rm -f "$DZ_NAME" >/dev/null 2>&1 || true
  # Remove the image too (default on) so the next run re-pulls and exercises the
  # real pull path, and the server is left clean. Override with DZ_QA_REMOVE_IMAGE=0.
  if [ "${DZ_QA_REMOVE_IMAGE:-1}" = 1 ] && [ -n "$img" ]; then
    $SUDO docker rmi "$img" >/dev/null 2>&1 || true
  fi
  [ -n "$SCRATCH" ] && rm -rf "$SCRATCH"
}
# T0: after a successful run, prove the host is actually left clean — a leaked
# tunnel or container would poison the next env/run. Only enforced on success (a
# failed run already reported its problem; teardown is still best-effort).
verify_clean() {
  if $SUDO docker ps -aq --filter "name=^${DZ_NAME}$" 2>/dev/null | grep -q .; then
    die "T0 FAILED: container ${DZ_NAME} still present after teardown."
  fi
  local gone=""
  for _ in $(seq 1 "$TEARDOWN_VERIFY_TIMEOUT"); do
    if ! ip link show doublezero1 >/dev/null 2>&1; then gone=1; break; fi
    sleep 1
  done
  [ -n "$gone" ] || die "T0 FAILED: doublezero1 still present after teardown (leaked tunnel — disconnect didn't take)."
  info "T0: host left clean (no container, no doublezero1)."
}
final_teardown() {
  teardown
  [ "$SUCCESS" = 1 ] && verify_clean
}
trap final_teardown EXIT

# Dump everything useful before teardown wipes it.
dump_diagnostics() {
  warn "==== diagnostics for ${DZ_NAME} (${DZ_QA_ENV}) ===="
  $SUDO docker logs "$DZ_NAME" --tail 200 2>&1 | sed 's/^/[logs] /' >&2 || true
  $SUDO docker exec "$DZ_NAME" doublezero status 2>&1 | sed 's/^/[status] /' >&2 || true
  curl -fsS "${METRICS_URL}/metrics" 2>/dev/null | grep '^dz_' | sed 's/^/[metrics] /' >&2 || true
  warn "==== end diagnostics ===="
}
fail() { dump_diagnostics; die "$1"; }

# ----------------------------------------------------------------------------
# small helpers
# ----------------------------------------------------------------------------
dlogs() { $SUDO docker logs "$DZ_NAME" 2>&1; }
scrape_metrics() { curl -fsS "${METRICS_URL}/metrics" 2>/dev/null || true; }
# metric_sum <metrics_text> <metric_name>: sum all samples of a counter/gauge
# (labels ignored). Prints an integer (0 if absent).
metric_sum() {
  printf '%s\n' "$1" | awk -v m="$2" '
    { split($1, a, "{"); if (a[1] == m) s += $NF }
    END { printf "%d", s + 0 }'
}
# gauge_any_eq <metrics_text> <metric_name> <value>: 0 if any sample equals value.
gauge_any_eq() {
  printf '%s\n' "$1" | awk -v m="$2" -v v="$3" '
    { split($1, a, "{"); if (a[1] == m && $NF == v) found = 1 }
    END { exit found ? 0 : 1 }'
}

# wait_a1: 0 if the container is running and logged "doublezerod ready" within
# INSTALL_READY_TIMEOUT; 1 on timeout or early exit. No side effects (callers decide
# how to treat failure — a hard fail in normal mode, inconclusive in negative mode).
wait_a1() {
  local _
  for _ in $(seq 1 "$INSTALL_READY_TIMEOUT"); do
    $SUDO docker ps -q --filter "name=^${DZ_NAME}$" | grep -q . || return 1  # exited early
    dlogs | grep -q "doublezerod ready" && return 0
    sleep 1
  done
  return 1
}

status_json() { $SUDO docker exec "$DZ_NAME" doublezero status --json 2>/dev/null; }
# status_is_up: 0 iff the tunnel session is up. Schema + values mirror the doublezero
# qaagent (e2e/internal/rpc/agent.go, IsStatusUp).
status_is_up() {
  local out; out="$(status_json)" || return 1
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$out" | jq -e '
      [ .[]? | .response.doublezero_status.session_status ]
      | any(. == "BGP Session Up" or . == "up")' >/dev/null 2>&1
  else
    $SUDO docker exec "$DZ_NAME" doublezero status 2>/dev/null \
      | grep -Eqi 'bgp session up|session[_ ]status[": ]+up|connected'
  fi
}
# wait_a2 <timeout>: 0 if the session comes up within <timeout>s, else 1.
wait_a2() {
  local _ t="$1"
  for _ in $(seq 1 "$t"); do
    status_is_up && return 0
    sleep 1
  done
  return 1
}

# ----------------------------------------------------------------------------
# 1. run the published one-liner, exactly as a user would
# ----------------------------------------------------------------------------
info "QA install: ${DZ_QA_ENV} via ${INSTALL_URL} (container ${DZ_NAME}, WS :${WS_QA_PORT}, metrics :${METRICS_QA_PORT})"

# Fetch the script on its own first so a CDN/network failure surfaces as a real
# error — `curl … | bash` only propagates bash's status, masking a dead origin.
SCRATCH="$(mktemp -d)"
SCRIPT="$SCRATCH/installer.sh"
curl -fsSL "$INSTALL_URL" -o "$SCRIPT" || die "could not fetch the one-liner from $INSTALL_URL."
[ -s "$SCRIPT" ] || die "fetched installer from $INSTALL_URL is empty."

# Non-interactive, isolated. WS_BIND + METRICS_BIND use QA-only ports (relayed
# straight through by the installer); DZ_CLIENT_IP only when provided (makes the
# access-pass pre-check strict). Run in a subshell so exports don't leak.
run_installer() {
  (
    export DZ_SECRET DZ_NAME
    export DZ_ENV="$DZ_QA_ENV" DZ_ASSUME_YES=1
    export WS_BIND="0.0.0.0:${WS_QA_PORT}" METRICS_BIND="127.0.0.1:${METRICS_QA_PORT}"
    [ -n "$DZ_QA_CLIENT_IP" ] && export DZ_CLIENT_IP="$DZ_QA_CLIENT_IP"
    bash "$SCRIPT"
  )
}
# ----------------------------------------------------------------------------
# Tier 0 — negative self-test (opt-in): an unprovisioned secret must NOT connect.
# Proves A2 can distinguish a broken connection from a good one (no false green).
# ----------------------------------------------------------------------------
if [ "$DZ_QA_NEGATIVE" = 1 ]; then
  info "Tier 0 negative self-test: unprovisioned secret → expecting NO tunnel."
  if ! run_installer; then
    info "T0-neg: installer refused to proceed (access-pass pre-check caught the miss) — correct."
    SUCCESS=1; exit 0
  fi
  wait_a1 || die "T0-neg inconclusive: container did not come ready (install problem, not an access-pass miss)."
  info "T0-neg: container ready; confirming the tunnel does NOT come up (<=${NEG_TUNNEL_TIMEOUT}s)."
  if wait_a2 "$NEG_TUNNEL_TIMEOUT"; then
    fail "T0-neg FAILED: tunnel came UP with a supposedly unprovisioned secret — A2 can't tell up from down (false-green risk), or the secret IS provisioned."
  fi
  pass "T0-neg: tunnel correctly stayed down — A2 detects a broken connection."
  SUCCESS=1; exit 0
fi

run_installer || die "the installer exited non-zero."

# ----------------------------------------------------------------------------
# A1: install succeeded — container running + "doublezerod ready", no early exit
# ----------------------------------------------------------------------------
info "A1: waiting for the bridge container to be ready (<=${INSTALL_READY_TIMEOUT}s)."
wait_a1 || fail "A1 FAILED: 'doublezerod ready' not seen within ${INSTALL_READY_TIMEOUT}s (or the container exited early)."
pass "A1: container running and daemon ready."

# ----------------------------------------------------------------------------
# A2: tunnel is up (session_status "BGP Session Up").
# ----------------------------------------------------------------------------
info "A2: waiting for the tunnel session to come up (<=${TUNNEL_UP_TIMEOUT}s)."
wait_a2 "$TUNNEL_UP_TIMEOUT" || fail "A2 FAILED: tunnel session not up within ${TUNNEL_UP_TIMEOUT}s (access pass for this IP? provider GRE firewall? host default-deny on doublezero1?)."
pass "A2: tunnel session is up."

# ============================================================================
# Tier 1 — deterministic integrity of the installed client
# ============================================================================
info "Tier 1: integrity checks."

# T1.1 no panics / fatals in the bridge logs (a Rust panic is a hard failure even
# if --restart bounces the container).
if dlogs | grep -Eq 'panicked at|thread .* panicked|\bFATAL\b'; then
  fail "T1.1 FAILED: panic/fatal found in bridge logs."
fi
pass "T1.1: no panic/fatal in logs."

# T1.2 not crash-looping: after a short settle, still running with 0 restarts.
sleep "$SETTLE_SECS"
read -r running restarts < <($SUDO docker inspect -f '{{.State.Running}} {{.RestartCount}}' "$DZ_NAME" 2>/dev/null || echo "false ?")
[ "$running" = "true" ] || fail "T1.2 FAILED: container not running after settle (${running})."
[ "$restarts" = "0" ] || fail "T1.2 FAILED: container restarted ${restarts}x (crash-looping)."
pass "T1.2: container stable (running, 0 restarts)."

# T1.3 the data-plane tunnel interface exists and is UP on the host.
if ! ip -o link show doublezero1 2>/dev/null | grep -qE 'state (UP|UNKNOWN)|<[^>]*\bUP\b'; then
  fail "T1.3 FAILED: doublezero1 interface is missing or not UP on the host."
fi
pass "T1.3: doublezero1 interface is UP."

# T1.4 status fields are sane (a session 'up' with no device/IP is suspect).
if command -v jq >/dev/null 2>&1; then
  if ! status_json | jq -e '
      any(.[]?; (.response.doublezero_ip // "") != ""
                and (.current_device // "") != ""
                and (.response.tunnel_dst // "") != "")' >/dev/null 2>&1; then
    fail "T1.4 FAILED: status is up but doublezero_ip/current_device/tunnel_dst are not all populated."
  fi
  pass "T1.4: status fields populated (device, doublezero_ip, tunnel_dst)."
else
  warn "T1.4 SKIPPED: jq not available."
fi

# T1.5 device latency populated — the tunnel actually carries probe traffic
# (the data-plane analog of the doublezero QA's ping-through-tunnel).
if [ "${DZ_QA_SKIP_LATENCY:-0}" = 1 ]; then
  warn "T1.5 SKIPPED: DZ_QA_SKIP_LATENCY=1."
else
  latency_ok=""
  for _ in $(seq 1 "$LATENCY_TIMEOUT"); do
    lat="$($SUDO docker exec "$DZ_NAME" doublezero latency --json 2>/dev/null || true)"
    if command -v jq >/dev/null 2>&1; then
      printf '%s' "$lat" | jq -e 'any(.[]?; (.avg_latency_ns // 0) > 0)' >/dev/null 2>&1 && { latency_ok=1; break; }
    else
      printf '%s' "$lat" | grep -Eq '[0-9]+' && { latency_ok=1; break; }
    fi
    sleep 1
  done
  [ -n "$latency_ok" ] || fail "T1.5 FAILED: no device latency measured within ${LATENCY_TIMEOUT}s (tunnel not carrying traffic?)."
  pass "T1.5: device latency measured (tunnel carries traffic)."
fi

# T1.6 metrics + liveness endpoint (proves the bridge process itself, not just the
# daemon, is alive and serving). METRICS_BIND was relayed to the container above.
health_ok=""
for _ in $(seq 1 20); do
  code="$(curl -fsS -o /dev/null -w '%{http_code}' "${METRICS_URL}/healthz" 2>/dev/null || true)"
  [ "$code" = "200" ] && { health_ok=1; break; }
  sleep 1
done
[ -n "$health_ok" ] || fail "T1.6 FAILED: metrics /healthz not 200 at ${METRICS_URL} (bridge not serving?)."
scrape_metrics | grep -q '^dz_' || fail "T1.6 FAILED: /metrics returned no dz_ series."
pass "T1.6: metrics + /healthz endpoint serving."

# T1.7 running image matches the expected environment (catches a mislabeled/stale
# pull). Only when DZ_IMAGE was not overridden.
if [ -z "${DZ_IMAGE:-}" ]; then
  img="$($SUDO docker inspect -f '{{.Config.Image}}' "$DZ_NAME" 2>/dev/null || true)"
  case "$img" in
    *doublezero-edge-connect*${EXPECT_IMAGE_TAG}*) pass "T1.7: image matches env (${img})." ;;
    *) fail "T1.7 FAILED: running image '${img}' does not look like the ${EXPECT_IMAGE_TAG} edge-connect image." ;;
  esac
else
  warn "T1.7 SKIPPED: DZ_IMAGE was overridden (${DZ_IMAGE})."
fi

# T1.8 a token-derived key is never written to host disk (the installer's promise).
case "$DZ_SECRET" in
  DZ_*)
    if $SUDO test -e /root/.config/doublezero/id.json 2>/dev/null; then
      warn "T1.8: /root/.config/doublezero/id.json exists on the host — token keys should live only in the container. Investigate."
    else
      pass "T1.8: token key not present on host disk."
    fi ;;
  *) info "T1.8 skipped: DZ_SECRET is a file path (bind-mounted by design), not a token." ;;
esac

# ============================================================================
# Tier 2 — the product path (opt-in; host must be subscribed to a feed)
# ============================================================================
if [ "$DZ_QA_EXPECT_FEED" = 1 ]; then
  info "Tier 2: market-data product path (DZ_QA_EXPECT_FEED=1)."

  # T2.1 the reconciler activated the WS sink (subscription took effect).
  sink_ok=""
  for _ in $(seq 1 "$FEED_TIMEOUT"); do
    dlogs | grep -q "activating WebSocket sink" && { sink_ok=1; break; }
    sleep 1
  done
  [ -n "$sink_ok" ] || fail "T2.1 FAILED: WS sink not activated within ${FEED_TIMEOUT}s (host subscribed to a market-data group?)."
  pass "T2.1: WS sink activated."

  # T2.2 ⭐ inner multicast is actually flowing — this is the exact silent failure
  # the installer can only warn about (a default-deny host firewall dropping the
  # decapsulated inner multicast on doublezero1) and A2 cannot detect.
  mcast_ok=""
  for _ in $(seq 1 "$FEED_TIMEOUT"); do
    m="$(scrape_metrics)"
    if [ "$(metric_sum "$m" dz_datagrams_received_total)" -gt 0 ] && gauge_any_eq "$m" dz_feed_up 1; then
      mcast_ok=1; break
    fi
    sleep 1
  done
  [ -n "$mcast_ok" ] || fail "T2.2 FAILED: no inner multicast received / no feed up (dz_datagrams_received_total==0). Check GRE + host firewall on doublezero1."
  pass "T2.2: inner multicast flowing (dz_datagrams_received_total>0, dz_feed_up=1)."

  # T2.3 normalized market data is being produced (quotes and/or trades admitted).
  data_ok=""
  for _ in $(seq 1 "$FEED_TIMEOUT"); do
    m="$(scrape_metrics)"
    q="$(metric_sum "$m" dz_quotes_admitted_total)"; t="$(metric_sum "$m" dz_trades_admitted_total)"
    if [ $((q + t)) -gt 0 ]; then data_ok=1; break; fi
    sleep 1
  done
  [ -n "$data_ok" ] || fail "T2.3 FAILED: no quotes/trades admitted within ${FEED_TIMEOUT}s."
  pass "T2.3: normalized market data admitted."

  # T2.4 the WS actually serves valid PROTOCOL.md frames end-to-end.
  command -v python3 >/dev/null 2>&1 || fail "T2.4 FAILED: python3 needed for the WS probe."
  if WS_QA_PORT="$WS_QA_PORT" WS_READ_TIMEOUT="$FEED_TIMEOUT" python3 "$HERE/ws_probe.py"; then
    pass "T2.4: WS served an instrument definition + a market-data message."
  else
    fail "T2.4 FAILED: WS did not serve both an instrument and a market-data frame."
  fi
else
  info "Tier 2 skipped (DZ_QA_EXPECT_FEED!=1): WS serving is subscription-gated and not asserted."
fi

# ============================================================================
# Tier 3 — robustness
# ============================================================================
info "Tier 3: robustness checks."

# T3.1 error-counter ceilings (soft unless DZ_QA_STRICT=1).
m="$(scrape_metrics)"
sock_err="$(metric_sum "$m" dz_socket_errors_total)"
rejoins="$(metric_sum "$m" dz_idle_rejoin_total)"
info "T3.1: dz_socket_errors_total=${sock_err} dz_idle_rejoin_total=${rejoins} (max socket errors=${MAX_SOCKET_ERRORS})."
if [ "$sock_err" -gt "$MAX_SOCKET_ERRORS" ]; then
  if [ "$DZ_QA_STRICT" = 1 ]; then
    fail "T3.1 FAILED: dz_socket_errors_total=${sock_err} > ${MAX_SOCKET_ERRORS} (strict)."
  else
    warn "T3.1: dz_socket_errors_total=${sock_err} exceeds ${MAX_SOCKET_ERRORS} (non-strict; not failing)."
  fi
else
  pass "T3.1: error counters within ceiling."
fi

# T3.2 shred forwarding (opt-in; host must be subscribed to an edge-solana-* group).
if [ "$DZ_QA_EXPECT_SHREDS" = 1 ]; then
  shred_ok=""
  for _ in $(seq 1 "$FEED_TIMEOUT"); do
    if dlogs | grep -q "activating shred forwarder" \
       && [ "$(metric_sum "$(scrape_metrics)" dz_shred_forwarded_total)" -gt 0 ]; then
      shred_ok=1; break
    fi
    sleep 1
  done
  [ -n "$shred_ok" ] || fail "T3.2 FAILED: shred forwarder not active / nothing forwarded within ${FEED_TIMEOUT}s."
  pass "T3.2: shred forwarder active and forwarding."
else
  info "T3.2 skipped (DZ_QA_EXPECT_SHREDS!=1)."
fi

# ----------------------------------------------------------------------------
# done — final_teardown (on EXIT) tears down and verifies the host is left clean.
# ----------------------------------------------------------------------------
SUCCESS=1
info "${BOLD}QA PASSED${RST} for ${DZ_QA_ENV}: the one-liner installed, connected, and passed integrity checks."
