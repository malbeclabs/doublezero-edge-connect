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
    WS_BIND) echo "127.0.0.1:8081" ;;
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

# hl_listening <bind>: make the stub bridge log a successful Hyperliquid bind.
hl_listening() {
  local extra="$BATS_TEST_TMPDIR/hl-listening.log"
  printf 'INFO doublezero_edge_connect::sinks::hyperliquid: Hyperliquid-compatible WebSocket sink listening bind=%s\n' "$1" >"$extra"
  export DZ_TEST_DOCKER_LOG_EXTRA="$extra"
}

@test "HL_WS_BIND set: the Done output prints the Hyperliquid sink URL (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=127.0.0.1:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0
      hl_listening 127.0.0.1:8082; bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'Hyperliquid *: ws://127.0.0.1:8082' "$out" \
      || { echo "# $s never printed the Hyperliquid sink URL:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "HL_WS_BIND on a wildcard: the URL names this host, not 0.0.0.0" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=0.0.0.0:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0
    hl_listening 0.0.0.0:8082; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
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

@test "HL bind outcome not in the log (e.g. RUST_LOG=error): warns unconfirmed instead of printing a URL" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 HL_WS_BIND=127.0.0.1:8082 DZ_SUBSCRIPTION_REFRESH_SECS=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Could not confirm the Hyperliquid-compatible sink' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Hyperliquid *: ws://' "$out"
}

@test "HL_WS_BIND unset: no Hyperliquid line" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Hyperliquid' "$out"
}

# binance_listening <bind>: make the stub bridge log a successful Binance bind.
binance_listening() {
  local extra="$BATS_TEST_TMPDIR/binance-listening.log"
  printf 'INFO doublezero_edge_connect::sinks::binance: Binance-compatible WebSocket sink listening bind=%s\n' "$1" >"$extra"
  export DZ_TEST_DOCKER_LOG_EXTRA="$extra"
}

@test "BINANCE_WS_BIND set: the Done output prints both Binance sink URLs (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 BINANCE_WS_BIND=127.0.0.1:8083 DZ_SUBSCRIPTION_REFRESH_SECS=0
      binance_listening 127.0.0.1:8083; bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'Binance USD-M *: ws://127.0.0.1:8083/usdsm' "$out" && grep -q 'Binance spot *: ws://127.0.0.1:8083/spot' "$out" \
      || { echo "# $s did not print both Binance sink URLs:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "BINANCE_WS_BIND on a wildcard: the URLs name this host, not 0.0.0.0" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 BINANCE_WS_BIND=0.0.0.0:8083 DZ_SUBSCRIPTION_REFRESH_SECS=0
    binance_listening 0.0.0.0:8083; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Binance spot *: ws://.*:8083/spot' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'ws://0.0.0.0:8083' "$out"
}

@test "Binance sink failed to bind: warns with the bridge's reason instead of printing URLs" {
  local out="$BATS_TEST_TMPDIR/out" extra="$BATS_TEST_TMPDIR/extra.log"
  printf 'WARN doublezero_edge_connect: Binance-compatible sink disabled: Address already in use (os error 98) bind=127.0.0.1:8083\n' >"$extra"
  ( common_env; export DZ_INSTALL_CLI=0 BINANCE_WS_BIND=127.0.0.1:8083 DZ_SUBSCRIPTION_REFRESH_SECS=0 DZ_TEST_DOCKER_LOG_EXTRA="$extra"
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Binance-compatible sink is NOT serving.*Address already in use' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Binance .*: ws://' "$out"
}

@test "BINANCE_WS_BIND unset: no Binance line" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Binance' "$out"
}

# run_on_host <script> <out> <addresses>: run an installer on a host whose `hostname -I` lists
# <addresses>, with the WebSocket sink and HL on 0.0.0.0 and Binance on [::].
run_on_host() {
  printf '#!/usr/bin/env bash\necho "%s"\n' "$3" >"$STUB_BIN/hostname"
  chmod +x "$STUB_BIN/hostname"
  local extra="$BATS_TEST_TMPDIR/listening.log"
  printf 'INFO doublezero_edge_connect::sinks::hyperliquid: Hyperliquid-compatible WebSocket sink listening bind=0.0.0.0:8082\n' >"$extra"
  printf 'INFO doublezero_edge_connect::sinks::binance: Binance-compatible WebSocket sink listening bind=[::]:8083\n' >>"$extra"
  ( common_env; export DZ_INSTALL_CLI=0 WS_BIND=0.0.0.0:8081 HL_WS_BIND=0.0.0.0:8082 BINANCE_WS_BIND='[::]:8083' DZ_SUBSCRIPTION_REFRESH_SECS=0 DZ_TEST_DOCKER_LOG_EXTRA="$extra"
    bash "$SCRIPTS_DIR/$1.sh" ) >"$2" 2>&1 || { echo "# $1 exited $?"; sed 's/^/#   /' "$2"; false; }
}

@test "WS_BIND unset: the WebSocket URL is loopback, not this host's address (all scripts)" {
  printf '#!/usr/bin/env bash\necho "203.0.113.9"\n' >"$STUB_BIN/hostname"
  chmod +x "$STUB_BIN/hostname"
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0; unset WS_BIND
      bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'WebSocket *: ws://127.0.0.1:8081 ' "$out" || { echo "# $s:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "WS_BIND unset: the summary says the WebSocket is loopback-only; set, it does not (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0; unset WS_BIND
      bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'loopback-only; rerun with WS_BIND=0.0.0.0:8081' "$out" || { echo "# $s, unset:"; sed 's/^/#   /' "$out"; false; }
    ( common_env; export DZ_INSTALL_CLI=0 DZ_SUBSCRIPTION_REFRESH_SECS=0 WS_BIND=127.0.0.1:8081
      bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    ! grep -q 'loopback-only' "$out" || { echo "# $s, set:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "IPv6 listed first: each URL names this host's address in its bind's family (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    run_on_host "$s" "$out" '2001:db8::1 203.0.113.9 2001:db8::2'
    grep -q 'WebSocket *: ws://203.0.113.9:8081 ' "$out" \
      && grep -q 'Hyperliquid *: ws://203.0.113.9:8082 ' "$out" \
      && grep -q 'Binance USD-M *: ws://\[2001:db8::1\]:8083/usdsm' "$out" \
      && grep -q 'Binance spot *: ws://\[2001:db8::1\]:8083/spot' "$out" \
      || { echo "# $s:"; sed 's/^/#   /' "$out"; false; }
  done
}

@test "no address in a wildcard bind's family: the URL names that family's loopback" {
  local out="$BATS_TEST_TMPDIR/out"
  run_on_host connect "$out" '2001:db8::1'
  grep -q 'WebSocket *: ws://127.0.0.1:8081 ' "$out" && grep -q 'Hyperliquid *: ws://127.0.0.1:8082 ' "$out" \
    || { sed 's/^/#   /' "$out"; false; }
  run_on_host connect "$out" '203.0.113.9'
  grep -q 'Binance spot *: ws://\[::1\]:8083/spot' "$out" || { sed 's/^/#   /' "$out"; false; }
}

# kalshi_listening <bind>: make the stub bridge log a successful Kalshi bind.
kalshi_listening() {
  local extra="$BATS_TEST_TMPDIR/kalshi-listening.log"
  printf 'INFO doublezero_edge_connect::sinks::kalshi: Kalshi-compatible WebSocket sink listening bind=%s\n' "$1" >"$extra"
  export DZ_TEST_DOCKER_LOG_EXTRA="$extra"
}

@test "KALSHI_WS_BIND set: the Done output prints the Kalshi sink URL at the trade API path (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    local out="$BATS_TEST_TMPDIR/$s.out"
    ( common_env; export DZ_INSTALL_CLI=0 KALSHI_WS_BIND=0.0.0.0:8084 DZ_SUBSCRIPTION_REFRESH_SECS=0
      kalshi_listening 0.0.0.0:8084; bash "$SCRIPTS_DIR/$s.sh" ) >"$out" 2>&1 || { echo "# $s exited $?"; sed 's/^/#   /' "$out"; false; }
    grep -q 'Kalshi *: ws://.*:8084/trade-api/ws/v2' "$out" \
      || { echo "# $s never printed the Kalshi sink URL:"; sed 's/^/#   /' "$out"; false; }
    ! grep -q 'ws://0.0.0.0:8084' "$out"
  done
}

@test "Kalshi sink failed to bind: warns with the bridge's reason instead of printing a URL" {
  local out="$BATS_TEST_TMPDIR/out" extra="$BATS_TEST_TMPDIR/extra.log"
  printf 'WARN doublezero_edge_connect: Kalshi-compatible sink disabled: Address already in use (os error 98) bind=127.0.0.1:8084\n' >"$extra"
  ( common_env; export DZ_INSTALL_CLI=0 KALSHI_WS_BIND=127.0.0.1:8084 DZ_SUBSCRIPTION_REFRESH_SECS=0 DZ_TEST_DOCKER_LOG_EXTRA="$extra"
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1 || { echo "# exited $?"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Kalshi-compatible sink is NOT serving.*Address already in use' "$out" || { sed 's/^/#   /' "$out"; false; }
  ! grep -q 'Kalshi *: ws://' "$out"
}
