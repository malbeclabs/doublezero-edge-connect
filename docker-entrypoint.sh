#!/usr/bin/env bash
#
# doublezero-edge-connect container entrypoint.
#
# The base image (ghcr.io/malbeclabs/doublezero) ships its own entrypoint at
# /usr/local/bin/docker-entrypoint.sh that starts the `doublezerod` daemon (the
# deb's systemd unit doesn't run inside a container), persists the CLI env, and
# then idles on the daemon. The bridge needs that daemon up to reach the DZ Edge
# multicast group, so we run the base entrypoint FIRST — in the background, so it
# brings up (and keeps alive) doublezerod — wait for its socket, then run the
# bridge in the background too. This shell stays PID 1 so its TERM/INT trap can
# disconnect from DoubleZero cleanly on `docker stop` before tearing things down.
set -euo pipefail

BASE_ENTRYPOINT=/usr/local/bin/docker-entrypoint.sh
DZ_SOCK="${DZ_SOCK:-/run/doublezerod/doublezerod.sock}"

# Run the base entrypoint with no args: it starts doublezerod, writes the CLI
# config, prints `doublezero status`, then `wait`s on the daemon (keeping it up).
"$BASE_ENTRYPOINT" &
BASE_PID=$!

BRIDGE_PID=""
# Set only on the signal path (see on_signal). `disconnect` keys off this so a
# bare child exit — a bridge crash/panic/OOM that pops `wait -n` below — never
# releases the access pass: that would defeat `--restart unless-stopped` (the
# one-shot `connect multicast` is run externally and is not re-run on restart).
SIGNALED=0

# True iff doublezerod reports a live tunnel. `doublezero status --json` emits an
# array whose entries carry `response.doublezero_status.session_status`
# (up/down/unknown); we match a session of `up`. Grepping the JSON field (a stable
# key) rather than the human table avoids depending on column layout — and on jq,
# which the image doesn't ship. `timeout` bounds a probe against a wedged daemon.
dz_connected() {
    timeout 5 doublezero status --json 2>/dev/null \
        | grep -qiE '"session_status"[[:space:]]*:[[:space:]]*"up"'
}

# Graceful shutdown: disconnect from DoubleZero *while the daemon is still up*,
# then tear the bridge and daemon down. Disconnect runs only when (a) we got here
# via an operator signal (SIGNALED), (b) the daemon socket still exists, and
# (c) a tunnel is actually up — so a crash-path teardown never frees the session.
# We must NOT `exec` into the bridge below, or this handler would be discarded and
# the bridge would run as PID 1 with no signal handler (docker stop -> SIGKILL,
# no disconnect).
shutdown() {
    trap '' TERM INT   # a second signal during cleanup is ignored
    if [ "$SIGNALED" = 1 ] && [ -S "$DZ_SOCK" ] && dz_connected; then
        echo "[bridge-entrypoint] disconnecting from DoubleZero" >&2
        timeout 30 doublezero disconnect \
            || echo "[bridge-entrypoint] disconnect failed or timed out (continuing teardown)" >&2
    fi
    [ -n "$BRIDGE_PID" ] && kill -TERM "$BRIDGE_PID" 2>/dev/null || true
    kill -TERM "$BASE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    exit 0
}

# `docker stop` (TERM/INT) is the only path that should disconnect; flag it so
# shutdown can tell an operator stop from a child exiting on its own (the
# `wait -n` fall-through below calls shutdown directly, leaving SIGNALED=0).
on_signal() {
    SIGNALED=1
    shutdown
}
trap on_signal TERM INT

# Wait (up to ~15s) for the daemon socket before starting the bridge.
for _ in $(seq 1 75); do
    [ -S "$DZ_SOCK" ] && break
    if ! kill -0 "$BASE_PID" 2>/dev/null; then
        echo "[bridge-entrypoint] base entrypoint exited before the daemon was ready" >&2
        wait "$BASE_PID"
        exit 1
    fi
    sleep 0.2
done
if [ ! -S "$DZ_SOCK" ]; then
    echo "[bridge-entrypoint] timed out waiting for $DZ_SOCK" >&2
    kill -TERM "$BASE_PID" 2>/dev/null || true
    exit 1
fi

echo "[bridge-entrypoint] doublezerod ready, starting bridge" >&2
# Background (not exec) so this shell stays PID 1 and the shutdown trap survives.
# `wait -n` returns when the bridge OR the daemon exits; `|| true` keeps set -e
# from bypassing the shutdown handler when the bridge exits non-zero.
doublezero-edge-connect "$@" &
BRIDGE_PID=$!
wait -n || true
shutdown
