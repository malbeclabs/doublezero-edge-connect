#!/usr/bin/env bats
#
# Every bridge env var (`env = "..."` on `Args` in src/main.rs) must reach the container: the
# installers relay an allowlist, and a var missing from it is dropped silently (HL_WS_BIND was).

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  REGISTRY_FILE="$BATS_TEST_TMPDIR/registry.json"
  export DOCKER_LOG
  : >"$DOCKER_LOG"
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  printf '{}' >"$REGISTRY_FILE"
  make_stubs "$STUB_BIN"
}

bridge_env_vars() {
  grep -o 'env = "[A-Z0-9_]*"' "$REPO_ROOT/src/main.rs" | sed 's/env = "\(.*\)"/\1/' | sort -u
}

# A value the installer accepts for <var>; the rest get an opaque marker.
value_for() {
  case "$1" in
    DZ_FEED_REGISTRY) echo "$REGISTRY_FILE" ;;
    DZ_ADMIN_BIND) echo "127.0.0.1:9099" ;;
    DZ_SUBSCRIPTION_REFRESH_SECS) echo "1" ;;
    WS_BIND) echo "0.0.0.0:8081" ;;
    *) echo "v-$1" ;;
  esac
}

@test "every bridge env var is relayed to the container (all scripts)" {
  local vars; vars="$(bridge_env_vars)"
  [ "$(echo "$vars" | wc -l)" -gt 30 ] || { echo "# parsed too few env names from src/main.rs:"; echo "$vars"; false; }
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"
    local out="$BATS_TEST_TMPDIR/$s.out"
    (
      common_env; export DZ_INSTALL_CLI=0
      for v in $vars; do export "$v=$(value_for "$v")"; done
      bash "$SCRIPTS_DIR/$s.sh"
    ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    local run_line missing=()
    run_line="$(grep '^docker run ' "$DOCKER_LOG")"
    for v in $vars; do
      [[ " $run_line " == *" -e $v=$(value_for "$v") "* ]] || missing+=("$v")
    done
    [ "${#missing[@]}" -eq 0 ] || { echo "# $s does not relay: ${missing[*]}"; false; }
  done
}

@test "HL_WS_BIND set: the Done output prints the Hyperliquid sink URL (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=127.0.0.1:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0
      bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'Hyperliquid *: ws://127.0.0.1:8082' "$out" \
      || { echo "# $s never printed the Hyperliquid sink URL:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "HL_WS_BIND on a wildcard: the URL names this host, not 0.0.0.0" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=0.0.0.0:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Hyperliquid *: ws://.*:8082' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'ws://0.0.0.0:8082' "$out"
}

@test "HL sink failed to bind: warns with the bridge's reason instead of printing a URL" {
  local out="$BATS_TEST_TMPDIR/out" extra="$BATS_TEST_TMPDIR/extra.log"
  printf 'WARN doublezero_edge_connect: Hyperliquid-compatible sink disabled: Address already in use (os error 98) bind=127.0.0.1:8082\n' >"$extra"
  ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=127.0.0.1:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0 DZ_TEST_DOCKER_LOG_EXTRA="$extra"
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Address already in use' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Hyperliquid *: ws://' "$out"
}

@test "HL_WS_BIND unset: no Hyperliquid line" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Hyperliquid' "$out"
}
