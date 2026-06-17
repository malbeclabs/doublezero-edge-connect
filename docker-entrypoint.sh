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
# Marker the installer touches only after `doublezero connect multicast` exits 0
# (see scripts/connect*.sh). Its presence is how we know there's a tunnel worth
# tearing down on shutdown — disconnect runs only when it exists.
DZ_CONNECT_MARKER="${DZ_CONNECT_MARKER:-/run/doublezerod/connected}"

# Run the base entrypoint with no args: it starts doublezerod, writes the CLI
# config, prints `doublezero status`, then `wait`s on the daemon (keeping it up).
"$BASE_ENTRYPOINT" &
BASE_PID=$!

BRIDGE_PID=""

# Graceful shutdown: disconnect from DoubleZero (only if a successful connect was
# recorded) *while the daemon is still up*, then tear the bridge and daemon down.
# We must NOT `exec` into the bridge below, or this handler would be discarded and
# the bridge would run as PID 1 with no signal handler (docker stop -> SIGKILL,
# no disconnect). Reached both on `docker stop` (TERM/INT) and when the bridge or
# daemon exits on its own.
shutdown() {
    trap '' TERM INT   # a second signal during cleanup is ignored
    if [ -e "$DZ_CONNECT_MARKER" ]; then
        echo "[bridge-entrypoint] disconnecting from DoubleZero" >&2
        doublezero disconnect || echo "[bridge-entrypoint] disconnect failed (continuing teardown)" >&2
    fi
    [ -n "$BRIDGE_PID" ] && kill -TERM "$BRIDGE_PID" 2>/dev/null || true
    kill -TERM "$BASE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    exit 0
}
trap shutdown TERM INT

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
