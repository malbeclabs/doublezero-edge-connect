#!/usr/bin/env bats
#
# WS-port preflight coverage for the connect*.sh installers, including the
# regression net for #70: under `set -e`, a bare `port_in_use "$p"` returning
# non-zero (the common case — the WS port is FREE) tripped errexit and aborted
# the whole installer before it ever started the container. The fix captures the
# status without letting the bare non-zero command trip errexit
# (`local rc=0; port_in_use || rc=$?`).
#
# Both tests drive the REAL, unmodified script end-to-end through a stub-first
# PATH — no change to the shipped file required.
#   - free port : asserts the installer survives preflight and reaches `docker
#                 run` (fails against pre-#70 code, passes against the fix).
#   - busy port : asserts preflight actually DETECTS the conflict (warns) and,
#                 non-interactively, continues to `docker run` anyway.

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  export DOCKER_LOG
  : >"$DOCKER_LOG"
  # A plausible keypair file (contents don't matter: KEY_SRC=file skips token
  # decode, and the access-pass check is stubbed out).
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  make_stubs "$STUB_BIN"
}

@test "free WS port: installer survives preflight and reaches docker run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"
    ss_reports_free "$STUB_BIN"
    (
      common_env
      bash "$SCRIPTS_DIR/$s.sh"
    )
    status=$?
    if [ "$status" -ne 0 ]; then
      echo "# $s.sh exited $status with a free WS port (the #70 abort)"; fails=1; continue
    fi
    if ! grep -q '^docker run ' "$DOCKER_LOG"; then
      echo "# $s.sh never reached 'docker run' — preflight aborted early"
      echo "# docker calls seen:"; sed 's/^/#   /' "$DOCKER_LOG"; fails=1; continue
    fi
  done
  [ "$fails" -eq 0 ]
}

@test "busy WS port, non-interactive: preflight detects it and still reaches docker run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"
    ss_reports_busy "$STUB_BIN" 8081
    local err="$BATS_TEST_TMPDIR/$s.err"
    (
      common_env
      bash "$SCRIPTS_DIR/$s.sh"
    ) 2>"$err"
    status=$?
    if [ "$status" -ne 0 ]; then
      echo "# $s.sh exited $status on a busy port"; fails=1; continue
    fi
    # Prove the busy branch actually fired. Without this, the test is a
    # near-duplicate of the free-port case: with DZ_ASSUME_YES=1 both paths reach
    # `docker run`, so it would still pass even if port_in_use wrongly reported
    # the busy port free. The warn ("already in use") comes only from the
    # conflict branch, so it pins that preflight detected the bound port.
    if ! grep -qi 'already in use' "$err"; then
      echo "# $s.sh never warned the WS port was busy — preflight didn't detect the conflict:"
      sed 's/^/#   /' "$err"; fails=1; continue
    fi
    if ! grep -q '^docker run ' "$DOCKER_LOG"; then
      echo "# $s.sh did not reach 'docker run' on a busy port"; fails=1; continue
    fi
  done
  [ "$fails" -eq 0 ]
}
