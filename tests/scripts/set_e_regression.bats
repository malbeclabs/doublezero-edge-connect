#!/usr/bin/env bats
#
# Regression net for #70: under `set -e`, a bare `port_in_use "$p"` returning
# non-zero (the common case — the WS port is FREE) tripped errexit and aborted
# the whole installer before it ever started the container. The fix captures the
# status without letting the bare non-zero command trip errexit
# (`local rc=0; port_in_use || rc=$?`).
#
# This drives the REAL, unmodified script end-to-end with the WS port reported
# free, and asserts it survives preflight and reaches `docker run`. It fails
# against the pre-#70 code and passes against the fix — no change to the shipped
# file required.

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
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"
    ss_reports_free "$STUB_BIN"
    (
      common_env
      bash "$SCRIPTS_DIR/$s.sh"
    )
    status=$?
    [ "$status" -eq 0 ] || {
      echo "# $s.sh exited $status with a free WS port (the #70 abort)"; return 1
    }
    grep -q '^docker run ' "$DOCKER_LOG" || {
      echo "# $s.sh never reached 'docker run' — preflight aborted early"
      echo "# docker calls seen:"; sed 's/^/#   /' "$DOCKER_LOG"; return 1
    }
  done
}

@test "busy WS port, non-interactive: installer still reaches docker run (all scripts)" {
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"
    ss_reports_busy "$STUB_BIN" 8081
    (
      common_env
      bash "$SCRIPTS_DIR/$s.sh"
    )
    status=$?
    [ "$status" -eq 0 ] || { echo "# $s.sh exited $status on a busy port"; return 1; }
    grep -q '^docker run ' "$DOCKER_LOG" || {
      echo "# $s.sh did not reach 'docker run' on a busy port"; return 1
    }
  done
}
