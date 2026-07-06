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
  cat >"$bin/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$DOCKER_LOG"
case "$1" in
  info) exit 0 ;;
  logs) echo "doublezerod ready"; exit 0 ;;
  ps)   echo "stubcontainerid"; exit 0 ;;
  *)    exit 0 ;;
esac
EOF

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

# ss_reports_free <bindir>: `ss -ltn` lists a socket that never matches a WS port.
ss_reports_free() {
  cat >"$1/ss" <<'EOF'
#!/usr/bin/env bash
echo "LISTEN 0 128 127.0.0.1:22 0.0.0.0:*"
EOF
  chmod +x "$1/ss"
}

# ss_reports_busy <bindir> <port>: `ss -ltn` lists <port> as already bound.
ss_reports_busy() {
  local port="$2"
  cat >"$1/ss" <<EOF
#!/usr/bin/env bash
echo "LISTEN 0 128 0.0.0.0:${port} 0.0.0.0:*"
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
