#!/usr/bin/env bash
#
# doublezero-edge-connect container entrypoint.
#
# The base image (ghcr.io/malbeclabs/doublezero) ships its own entrypoint at
# /usr/local/bin/docker-entrypoint.sh that starts the `doublezerod` daemon (the
# deb's systemd unit doesn't run inside a container), persists the CLI env, and
# then idles on the daemon. The bridge needs that daemon up to reach the DZ Edge
# multicast group, so we run the base entrypoint FIRST — in the background, so it
# brings up (and keeps alive) doublezerod — wait for its socket, then exec the
# bridge as the foreground process.
set -euo pipefail

BASE_ENTRYPOINT=/usr/local/bin/docker-entrypoint.sh
DZ_SOCK="${DZ_SOCK:-/run/doublezerod/doublezerod.sock}"

# Run the base entrypoint with no args: it starts doublezerod, writes the CLI
# config, prints `doublezero status`, then `wait`s on the daemon (keeping it up).
"$BASE_ENTRYPOINT" &
BASE_PID=$!

# Forward termination to the base entrypoint (which traps it and stops the daemon
# cleanly) so `docker stop` tears the daemon down too, not just the bridge.
trap 'kill -TERM "$BASE_PID" 2>/dev/null || true' TERM INT

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
exec doublezero-edge-connect "$@"
