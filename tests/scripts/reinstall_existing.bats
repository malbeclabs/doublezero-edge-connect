#!/usr/bin/env bats
#
# Coverage for the "existing instance" guard (section 3b) added to the
# connect*.sh installers: when a container named $DZ_NAME already exists the
# installer must tear it down cleanly (graceful `docker stop` -> SIGTERM ->
# entrypoint disconnect) before starting a fresh one, and it must not touch a
# host that has no prior instance.
#
# The shared _helpers docker stub answers every `ps` with an id, so it can't tell
# a RUNNING instance from a STOPPED one. These tests install a smarter docker stub
# that distinguishes `ps -a -q` (exists?) from `ps -q` (running?) via STUB_EXISTS /
# STUB_RUNNING, and — crucially — flips to "container is up" once `docker run` has
# fired, so the installer's own post-run readiness loop still passes.
#
# As with the other script tests, the REAL, unmodified installer is driven
# end-to-end through a stub-first PATH; we assert on the argv it handed `docker`.

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  export DOCKER_LOG
  : >"$DOCKER_LOG"
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  make_stubs "$STUB_BIN"
  install_reinstall_docker_stub "$STUB_BIN"
}

# A docker stub that models an existing instance whose running-state is scripted,
# and that reports the *new* container as up after `docker run`.
install_reinstall_docker_stub() {
  cat >"$1/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$DOCKER_LOG"
case "$1" in
  info)    exit 0 ;;
  logs)    echo "doublezerod ready"; exit 0 ;;
  inspect) exit 0 ;;                       # no env/image labels needed here
  run)     : >"$DOCKER_LOG.ran"; exit 0 ;; # remember the fresh container now exists
  ps)
    # After `docker run`, every ps refers to the new container -> it's up (so the
    # installer's "Container exited early" guard passes).
    if [ -f "$DOCKER_LOG.ran" ]; then echo "newcontainerid"; exit 0; fi
    # Before run: `-a` is the "exists?" probe, otherwise the "running?" probe.
    case " $* " in
      *" -a "*) [ "${STUB_EXISTS:-1}" = 1 ]  && echo oldcontainerid ;;
      *)        [ "${STUB_RUNNING:-1}" = 1 ] && echo oldcontainerid ;;
    esac
    exit 0 ;;
  *) exit 0 ;;
esac
EOF
  chmod +x "$1/docker"
}

# line number of the first log entry matching a fixed string (empty if none)
first_line() { grep -nF "$1" "$DOCKER_LOG" | head -1 | cut -d: -f1; }

@test "running instance: stops it (graceful) before removing, then reaches docker run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"; rm -f "$DOCKER_LOG.ran"
    local err="$BATS_TEST_TMPDIR/$s.err"
    ( common_env; export STUB_EXISTS=1 STUB_RUNNING=1; bash "$SCRIPTS_DIR/$s.sh" ) 2>"$err"
    status=$?
    if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; sed 's/^/#   /' "$err"; fails=1; continue; fi
    if ! grep -qi 'already exists' "$err"; then echo "# $s.sh never warned an instance already exists"; fails=1; continue; fi
    local stop_l rm_l run_l
    stop_l="$(first_line 'docker stop ')"; rm_l="$(first_line 'docker rm -f ')"; run_l="$(first_line 'docker run ')"
    if [ -z "$stop_l" ]; then echo "# $s.sh did not 'docker stop' the running instance"; fails=1; continue; fi
    if [ -z "$rm_l" ] || [ -z "$run_l" ]; then echo "# $s.sh missing docker rm / docker run"; fails=1; continue; fi
    if [ "$stop_l" -ge "$rm_l" ]; then echo "# $s.sh removed before stopping (stop@$stop_l rm@$rm_l)"; fails=1; continue; fi
    if [ "$rm_l" -ge "$run_l" ]; then echo "# $s.sh ran before tearing the old one down"; fails=1; continue; fi
  done
  [ "$fails" -eq 0 ]
}

@test "stopped instance: removes without stopping, then reaches docker run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"; rm -f "$DOCKER_LOG.ran"
    local err="$BATS_TEST_TMPDIR/$s.err"
    ( common_env; export STUB_EXISTS=1 STUB_RUNNING=0; bash "$SCRIPTS_DIR/$s.sh" ) 2>"$err"
    status=$?
    if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; sed 's/^/#   /' "$err"; fails=1; continue; fi
    if ! grep -qi 'already exists' "$err"; then echo "# $s.sh never warned an instance already exists"; fails=1; continue; fi
    # A stopped container can't be `docker stop`ped for a graceful disconnect.
    if grep -q '^docker stop ' "$DOCKER_LOG"; then echo "# $s.sh tried to stop a non-running instance"; fails=1; continue; fi
    if [ -z "$(first_line 'docker rm -f ')" ]; then echo "# $s.sh did not remove the stopped instance"; fails=1; continue; fi
    if ! grep -q '^docker run ' "$DOCKER_LOG"; then echo "# $s.sh never reached docker run"; fails=1; continue; fi
  done
  [ "$fails" -eq 0 ]
}

@test "no existing instance: no warn, no stop, reaches docker run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"; rm -f "$DOCKER_LOG.ran"
    local err="$BATS_TEST_TMPDIR/$s.err"
    ( common_env; export STUB_EXISTS=0; bash "$SCRIPTS_DIR/$s.sh" ) 2>"$err"
    status=$?
    if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; sed 's/^/#   /' "$err"; fails=1; continue; fi
    if grep -qi 'already exists' "$err"; then echo "# $s.sh warned about an instance that does not exist"; fails=1; continue; fi
    if grep -q '^docker stop ' "$DOCKER_LOG"; then echo "# $s.sh stopped a non-existent instance"; fails=1; continue; fi
    if ! grep -q '^docker run ' "$DOCKER_LOG"; then echo "# $s.sh never reached docker run"; fails=1; continue; fi
  done
  [ "$fails" -eq 0 ]
}

@test "running instance, no TTY, DZ_ASSUME_YES unset: reinstalls (not abort) and says so (all scripts)" {
  # Pre-3b a headless re-run silently reinstalled; the guard must preserve that
  # (reinstall + a loud notice), NOT abort automation that never set DZ_ASSUME_YES.
  command -v setsid >/dev/null 2>&1 || skip "setsid not available to drop the controlling TTY"
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    : >"$DOCKER_LOG"; rm -f "$DOCKER_LOG.ran"
    local err="$BATS_TEST_TMPDIR/$s.err"
    (
      common_env; unset DZ_ASSUME_YES; export STUB_EXISTS=1 STUB_RUNNING=1
      # setsid detaches the controlling terminal so [ -r /dev/tty ] is false;
      # -w waits for the child (bare setsid forks and returns immediately);
      # timeout guards against a hang if that assumption ever breaks (a prompt).
      timeout 30 setsid -w bash "$SCRIPTS_DIR/$s.sh" </dev/null
    ) 2>"$err"
    status=$?
    if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status (headless re-run should reinstall, not abort)"; sed 's/^/#   /' "$err"; fails=1; continue; fi
    if ! grep -qi 'no terminal to prompt' "$err"; then echo "# $s.sh didn't announce the headless reinstall"; sed 's/^/#   /' "$err"; fails=1; continue; fi
    if ! grep -q '^docker run ' "$DOCKER_LOG"; then echo "# $s.sh never reached docker run"; fails=1; continue; fi
  done
  [ "$fails" -eq 0 ]
}
