#!/usr/bin/env bats
#
# docker-entrypoint.sh, run end to end against stubs of the base image's entrypoint and the bridge.

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  mkdir -p "$STUB_BIN"
  export DZ_SOCK="$BATS_TEST_TMPDIR/doublezerod.sock"
  export BASE_ENTRYPOINT="$STUB_BIN/base-entrypoint"
  export BRIDGE_RAN="$BATS_TEST_TMPDIR/bridge-ran"

  # The daemon: like doublezerod, it cannot bind over an existing socket file.
  cat >"$BASE_ENTRYPOINT" <<'EOF'
#!/usr/bin/env bash
exec python3 -c 'import socket, sys, time
s = socket.socket(socket.AF_UNIX)
s.bind(sys.argv[1])
s.listen()
time.sleep(30)' "$DZ_SOCK"
EOF

  # The bridge: runs only once a daemon is listening behind the socket.
  cat >"$STUB_BIN/doublezero-edge-connect" <<'EOF'
#!/usr/bin/env bash
for _ in $(seq 1 50); do
  python3 -c 'import socket, sys; socket.socket(socket.AF_UNIX).connect(sys.argv[1])' "$DZ_SOCK" 2>/dev/null \
    && { touch "$BRIDGE_RAN"; exit 0; }
  sleep 0.1
done
exit 1
EOF
  chmod +x "$STUB_BIN"/*
}

@test "a socket left by the previous run: the daemon still starts and the bridge reaches it" {
  python3 -c 'import socket, sys; socket.socket(socket.AF_UNIX).bind(sys.argv[1])' "$DZ_SOCK"
  [ -S "$DZ_SOCK" ] || { echo "# could not plant the stale socket"; false; }
  local out="$BATS_TEST_TMPDIR/out" rc=0
  PATH="$STUB_BIN:$PATH" timeout 30 bash "$REPO_ROOT/docker-entrypoint.sh" >"$out" 2>&1 3>&- || rc=$?
  [ -e "$BRIDGE_RAN" ] || { echo "# the bridge never reached a live daemon (exit $rc):"; sed 's/^/#   /' "$out"; false; }
  grep -q "removing existing $DZ_SOCK" "$out" || { echo "# the removal was not logged:"; sed 's/^/#   /' "$out"; false; }
}
