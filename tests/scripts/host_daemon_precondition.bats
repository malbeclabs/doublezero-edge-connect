#!/usr/bin/env bats
#
# Coverage for the host-doublezerod / liveness-UDP-port precondition (section 4e) added to
# connect.sh: the container runs its OWN doublezerod under --network host, whose liveness
# manager binds UDP 44880. A host daemon already bound there makes the container's own
# doublezerod fail that bind, and the container exits within seconds -- a "successful" install
# followed by a dead container. connect.sh must catch this BEFORE starting the container.
#
# As with the other script tests, the REAL, unmodified installer is driven end-to-end through a
# stub-first PATH; we assert on what it actually invoked (docker argv, systemctl argv), not just
# exit status.

bats_require_minimum_version 1.5.0

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  SYSTEMCTL_LOG="$BATS_TEST_TMPDIR/systemctl.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  export DOCKER_LOG SYSTEMCTL_LOG
  : >"$DOCKER_LOG"
  : >"$SYSTEMCTL_LOG"
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  make_stubs "$STUB_BIN"
  install_systemctl_stub "$STUB_BIN"
}

# systemctl stub: logs every invocation to $SYSTEMCTL_LOG, and answers `is-active --quiet
# doublezerod` from $STUB_DZD_ACTIVE (default: active — most tests care about the busy-port
# path, not this one).
install_systemctl_stub() {
  cat >"$1/systemctl" <<'EOF'
#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$SYSTEMCTL_LOG"
case "$*" in
  "is-active --quiet doublezerod") [ "${STUB_DZD_ACTIVE:-1}" = 1 ] && exit 0 || exit 3 ;;
  "stop doublezerod")    exit "${STUB_STOP_RC:-0}" ;;
  "disable doublezerod") exit "${STUB_DISABLE_RC:-0}" ;;
esac
exit 0
EOF
  chmod +x "$1/systemctl"
}

@test "UDP 44880 free: no prompt, no systemctl call, install proceeds" {
  local err="$BATS_TEST_TMPDIR/err"
  ( common_env; bash "$SCRIPTS_DIR/connect.sh" ) 2>"$err"
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$err"; false; }
  if grep -qi 'UDP port 44880' "$err"; then
    echo "# warned about a UDP conflict on a free port:"; sed 's/^/#   /' "$err"; false
  fi
  if [ -s "$SYSTEMCTL_LOG" ]; then
    echo "# called systemctl even though the port was free:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false
  fi
  grep -q '^docker run ' "$DOCKER_LOG" || { echo "# never reached docker run"; false; }
}

@test "UDP 44880 busy, doublezerod active, DZ_STOP_HOST_DAEMON=1: stops+disables it, install proceeds" {
  ss_reports_busy "$STUB_BIN" 44880
  local err="$BATS_TEST_TMPDIR/err"
  ( common_env; export DZ_STOP_HOST_DAEMON=1 STUB_DZD_ACTIVE=1; bash "$SCRIPTS_DIR/connect.sh" ) 2>"$err"
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$err"; false; }
  grep -qi 'UDP port 44880 is already bound' "$err" || { echo "# never reported the conflict:"; sed 's/^/#   /' "$err"; false; }
  grep -qi 'disconnects any DoubleZero tunnel' "$err" || { echo "# never warned about the tunnel-disconnect cost:"; sed 's/^/#   /' "$err"; false; }
  grep -q '^systemctl stop doublezerod$' "$SYSTEMCTL_LOG" || { echo "# never stopped the host doublezerod:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false; }
  grep -q '^systemctl disable doublezerod$' "$SYSTEMCTL_LOG" || { echo "# never disabled the host doublezerod:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false; }
  grep -q '^docker run ' "$DOCKER_LOG" || { echo "# never reached docker run after clearing the conflict"; false; }
}

@test "UDP 44880 busy, doublezerod active, DZ_STOP_HOST_DAEMON=0: aborts, no stop, no container" {
  ss_reports_busy "$STUB_BIN" 44880
  local err="$BATS_TEST_TMPDIR/err"
  common_env
  export DZ_STOP_HOST_DAEMON=0 STUB_DZD_ACTIVE=1
  run --separate-stderr bash "$SCRIPTS_DIR/connect.sh"
  printf '%s' "$stderr" >"$err"
  [ "$status" -ne 0 ] || { echo "# exited 0 despite declining to stop the conflicting daemon"; sed 's/^/#   /' "$err"; false; }
  grep -qi 'Not stopping the host doublezerod' "$err" || { echo "# decline message missing / reason not obvious:"; sed 's/^/#   /' "$err"; false; }
  # Must hand the operator the exact manual commands.
  grep -q 'sudo systemctl stop doublezerod' "$err" || { echo "# no manual stop command given:"; sed 's/^/#   /' "$err"; false; }
  grep -q 'sudo systemctl disable doublezerod' "$err" || { echo "# no manual disable command given:"; sed 's/^/#   /' "$err"; false; }
  if grep -q '^systemctl stop doublezerod$' "$SYSTEMCTL_LOG"; then
    echo "# stopped the daemon despite the decline:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false
  fi
  if grep -q '^docker run ' "$DOCKER_LOG"; then
    echo "# started the container despite declining to clear the conflict:"; sed 's/^/#   /' "$DOCKER_LOG"; false
  fi
}

@test "UDP 44880 busy, doublezerod active, non-interactive (no TTY, no DZ_STOP_HOST_DAEMON): refuses rather than crash-loop" {
  # Chosen default: this precondition is NOT a nice-to-have like the WS-port preflight or the CLI
  # offer -- proceeding would start a container that dies seconds later, so headless runs with no
  # explicit answer refuse (same outcome as an interactive decline), rather than silently
  # continuing into a crash loop.
  command -v setsid >/dev/null 2>&1 || skip "setsid not available to drop the controlling TTY"
  ss_reports_busy "$STUB_BIN" 44880
  local err="$BATS_TEST_TMPDIR/err"
  common_env
  unset DZ_ASSUME_YES DZ_STOP_HOST_DAEMON
  export STUB_DZD_ACTIVE=1
  run --separate-stderr timeout 30 setsid -w bash "$SCRIPTS_DIR/connect.sh" </dev/null
  printf '%s' "$stderr" >"$err"
  [ "$status" -ne 0 ] || { echo "# exited 0 non-interactively with an unresolved port conflict"; sed 's/^/#   /' "$err"; false; }
  grep -qi 'Not stopping the host doublezerod' "$err" || { echo "# non-interactive refusal message missing:"; sed 's/^/#   /' "$err"; false; }
  if grep -q '^systemctl stop doublezerod$' "$SYSTEMCTL_LOG"; then
    echo "# stopped the daemon with no explicit non-interactive answer:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false
  fi
  if grep -q '^docker run ' "$DOCKER_LOG"; then
    echo "# started the container despite an unresolved conflict:"; sed 's/^/#   /' "$DOCKER_LOG"; false
  fi
}

@test "UDP 44880 busy, DZ_ASSUME_YES=1 (no explicit DZ_STOP_HOST_DAEMON): treated as yes, like other prompts" {
  # DZ_ASSUME_YES=1 already means "yes" to every other confirm() in this script (Docker install,
  # reinstall, the CLI offer); this prompt uses the same helper, so it inherits that convention
  # rather than needing a special case.
  ss_reports_busy "$STUB_BIN" 44880
  local err="$BATS_TEST_TMPDIR/err"
  ( common_env; export STUB_DZD_ACTIVE=1; bash "$SCRIPTS_DIR/connect.sh" ) 2>"$err"
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status under DZ_ASSUME_YES=1"; sed 's/^/#   /' "$err"; false; }
  grep -q '^systemctl stop doublezerod$' "$SYSTEMCTL_LOG" || { echo "# DZ_ASSUME_YES=1 didn't stop the conflicting daemon:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false; }
  grep -q '^docker run ' "$DOCKER_LOG" || { echo "# never reached docker run"; false; }
}

@test "UDP 44880 busy, but systemctl does not show doublezerod active: never tells the operator to stop it" {
  # The port may be held by something with nothing to do with doublezerod. A wrong instruction is
  # worse than none: the installer must still stop (a real conflict), but must not name doublezerod
  # as the remediation, and must not touch it.
  ss_reports_busy "$STUB_BIN" 44880
  local err="$BATS_TEST_TMPDIR/err"
  common_env
  export DZ_STOP_HOST_DAEMON=1 STUB_DZD_ACTIVE=0
  run --separate-stderr bash "$SCRIPTS_DIR/connect.sh"
  printf '%s' "$stderr" >"$err"
  [ "$status" -ne 0 ] || { echo "# exited 0 despite an unattributed port conflict"; sed 's/^/#   /' "$err"; false; }
  if grep -qi 'systemctl stop doublezerod' "$err"; then
    echo "# told the operator to stop doublezerod despite it not being the active service:"; sed 's/^/#   /' "$err"; false
  fi
  if grep -q '^systemctl stop doublezerod$' "$SYSTEMCTL_LOG"; then
    echo "# actually invoked systemctl stop doublezerod on an unrelated conflict:"; sed 's/^/#   /' "$SYSTEMCTL_LOG"; false
  fi
  grep -qi "doesn't show doublezerod active" "$err" || { echo "# didn't explain why doublezerod isn't the offered remediation:"; sed 's/^/#   /' "$err"; false; }
  if grep -q '^docker run ' "$DOCKER_LOG"; then
    echo "# started the container despite the unresolved conflict:"; sed 's/^/#   /' "$DOCKER_LOG"; false
  fi
}
