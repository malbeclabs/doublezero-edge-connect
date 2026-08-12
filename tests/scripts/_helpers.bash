# Shared helpers for the connect*.sh black-box tests.
#
# These tests never modify the shipped installer. Each script is run end-to-end
# through a stub-first PATH: fake `docker`, `sudo`, `ss`, `curl`, `sleep`, ...
# shadow the real tools, so the byte-identical file users get via `curl | bash`
# is exercised, and we assert on what it *tried* to do (the argv it handed the
# `docker` stub, its exit status) rather than on any test-only seam.

# Repo layout: this file lives at <repo>/tests/scripts/_helpers.bash
HELPERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HELPERS_DIR/../.." && pwd)"
SCRIPTS_DIR="$REPO_ROOT/scripts"

# The three installers stay independent files (no shared lib), so every behavioral
# test iterates this list — a function that drifts and breaks in one is caught.
SCRIPTS=(connect connect-testnet connect-devnet)

# make_stubs <bindir>
# Populate <bindir> with the default stub set that lets the installer reach the
# `docker run` step without any privilege, network, or real container. Individual
# tests overwrite a single stub (e.g. `ss`) to script the case under test.
make_stubs() {
  local bin="$1"
  mkdir -p "$bin"

  # sudo: strip its own leading options and exec the rest unprivileged, so
  # `sudo -n true`, `sudo -v`, and `sudo docker ...` all behave.
  cat >"$bin/sudo" <<'EOF'
#!/usr/bin/env bash
while [ $# -gt 0 ]; do case "$1" in -*) shift;; *) break;; esac; done
[ $# -eq 0 ] && exit 0
exec "$@"
EOF

  # docker: record every invocation to $DOCKER_LOG; answer the few subcommands
  # the script reads back. `logs` emits the readiness line so the 30x wait loop
  # breaks on the first iteration.
  # `logs` always emits the readiness line the daemon-wait loop looks for; it also appends the
  # contents of $DZ_TEST_DOCKER_LOG_EXTRA (a file path), when set, so a test can make e.g. the
  # bridge's "feed registry resolved" startup line appear without a real container.
  cat >"$bin/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$DOCKER_LOG"
case "$1" in
  info) exit 0 ;;
  logs) echo "doublezerod ready"; [ -n "${DZ_TEST_DOCKER_LOG_EXTRA:-}" ] && cat "$DZ_TEST_DOCKER_LOG_EXTRA" 2>/dev/null; exit 0 ;;
  ps)   # DZ_TEST_CONTAINER_DIES=1 models a container that exits after the readiness
        # wait (a failed bind, a crash loop): present until the first connect, gone after.
        if [ "${DZ_TEST_CONTAINER_DIES:-0}" = 1 ] && grep -q 'doublezero connect multicast' "$DOCKER_LOG"; then exit 0; fi
        echo "stubcontainerid"; exit 0 ;;
  exec) exec "$(dirname "$0")/dz-stub-exec" "$@" ;;
  *)    exit 0 ;;
esac
EOF

  install_dz_exec_stub "$bin"

  # Fully hermetic, deterministic host — never touch the network or the real box.
  cat >"$bin/curl"   <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  # python3 stub: skip the onchain access-pass pre-check with an inconclusive
  # code (the script warns and continues). Keeps the run offline regardless of
  # whether real python3 is installed. (accesspass.bats uses the REAL python3.)
  cat >"$bin/python3" <<'EOF'
#!/usr/bin/env bash
exit 4
EOF
  cat >"$bin/uname" <<'EOF'
#!/usr/bin/env bash
case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo Linux ;; esac
EOF
  cat >"$bin/sleep"     <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat >"$bin/modprobe"  <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat >"$bin/sysctl" <<'EOF'
#!/usr/bin/env bash
[ "$1" = -n ] && { echo 268435456; exit 0; }
exit 0
EOF
  cat >"$bin/getenforce" <<'EOF'
#!/usr/bin/env bash
echo Disabled
EOF
  cat >"$bin/ufw" <<'EOF'
#!/usr/bin/env bash
echo "Status: inactive"
EOF
  cat >"$bin/firewall-cmd" <<'EOF'
#!/usr/bin/env bash
echo "offline"
exit 1
EOF

  # Default ss: report the WS port FREE (one unrelated listening socket).
  ss_reports_free "$bin"

  chmod +x "$bin"/*
}

# install_dz_exec_stub <bindir>
# The daemon model behind `docker exec ... doublezero <cmd>`, shared by every docker
# stub (reinstall_existing.bats has its own) so the installer's connect retry loop and
# its `dz_connected` session probe see one consistent daemon. Two knobs:
#   DZ_TEST_CONNECT_FAILS     how many leading `connect multicast` attempts exit 1 (default 0)
#   DZ_TEST_SESSION_UP_AFTER  attempts after which `status --json` reports a live session;
#                             `never` = never up (default: FAILS+1 — the first accepted
#                             attempt brings the tunnel up)
#   DZ_TEST_SESSION_UP_AFTER_PROBES  when set, overrides the above: the session comes up
#                             after this many `status --json` probes, which is how a tunnel
#                             that finishes provisioning mid-run is modelled
# Both counts come from $DOCKER_LOG, so the stub needs no state of its own.
install_dz_exec_stub() {
  cat >"$1/dz-stub-exec" <<'EOF'
#!/usr/bin/env bash
fails="${DZ_TEST_CONNECT_FAILS:-0}"
up_after="${DZ_TEST_SESSION_UP_AFTER:-$((fails + 1))}"
attempts() { grep -c 'doublezero connect multicast' "$DOCKER_LOG" 2>/dev/null || true; }
probes()   { grep -c 'doublezero status --json' "$DOCKER_LOG" 2>/dev/null || true; }
case "$*" in
  *"connect multicast"*)
    if [ "$(attempts)" -le "$fails" ]; then
      echo "Error: Timed out waiting for daemon to finish probing devices." >&2
      exit 1
    fi
    echo "Connected to DoubleZero" ;;
  *"status --json"*)
    if [ -n "${DZ_TEST_SESSION_UP_AFTER_PROBES:-}" ]; then
      up_after=never
      [ "$(probes)" -ge "$DZ_TEST_SESSION_UP_AFTER_PROBES" ] && up_after=0
    fi
    if [ "$up_after" != never ] && [ "$(attempts)" -ge "$up_after" ]; then
      echo '[{"response":{"doublezero_status":{"session_status":"BGP Session Up"}}}]'
    else
      echo '[{"response":{"doublezero_status":{"session_status":"disconnected"}}}]'
    fi ;;
esac
exit 0
EOF
  chmod +x "$1/dz-stub-exec"
}

# The three knob-setters the outcome tests script the daemon with.
connect_fails_n_times()   { export DZ_TEST_CONNECT_FAILS="$1"; }
connect_always_fails()    { export DZ_TEST_CONNECT_FAILS=99 DZ_TEST_SESSION_UP_AFTER=never; }
dz_reports_disconnected() { export DZ_TEST_SESSION_UP_AFTER=never; }

# ss_reports_free <bindir>: `ss -ltn` lists a socket that never matches a WS port.
ss_reports_free() {
  cat >"$1/ss" <<'EOF'
#!/usr/bin/env bash
echo "LISTEN 0 128 127.0.0.1:22 0.0.0.0:*"
EOF
  chmod +x "$1/ss"
}

# ss_reports_busy <bindir> <port>: `ss -ltn` lists <port> as already bound. The
# heredoc is quoted so the stub body stays literal (nothing expands here even if
# it grows); the port is supplied at stub runtime via the exported env var.
ss_reports_busy() {
  export DZ_TEST_BUSY_PORT="$2"
  cat >"$1/ss" <<'EOF'
#!/usr/bin/env bash
echo "LISTEN 0 128 0.0.0.0:${DZ_TEST_BUSY_PORT} 0.0.0.0:*"
EOF
  chmod +x "$1/ss"
}

# common_env: env every run needs to be non-interactive and offline.
#   - DZ_SECRET points at a keyfile (KEY_SRC=file) so we skip token decode here.
#   - DZ_CLIENT_IP short-circuits public-IP detection.
# Call after $STUB_BIN and $KEYFILE are set. Prepends the stub dir to PATH.
common_env() {
  export PATH="$STUB_BIN:$PATH"
  export DZ_SECRET="$KEYFILE"
  export DZ_ASSUME_YES=1
  export DZ_CLIENT_IP="203.0.113.7"
  # connect-devnet.sh pulls a private image and requires a ghcr token; the docker
  # stub makes `docker login` a no-op, so any non-empty value gets us past the gate.
  # Harmless for connect.sh / connect-testnet.sh, which ignore it.
  export DZ_GHCR_TOKEN="${DZ_GHCR_TOKEN:-stub-ghcr-token}"
  # DZ_ENV intentionally left unset — each installer picks its own default
  # (connect.sh -> mainnet-beta, connect-testnet.sh -> testnet, etc.).
}
