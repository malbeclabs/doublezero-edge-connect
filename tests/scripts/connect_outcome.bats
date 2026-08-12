#!/usr/bin/env bats
#
# Coverage for the connect retry loop + outcome-gated closing message (#132): the
# installer used to make ONE `doublezero connect multicast` attempt after a fixed 30s
# sleep, treat its failure as non-fatal, and then print "Done. Connected." regardless
# — so a cold-daemon race left a new user with a running container, no tunnel, and a
# banner that said otherwise.
#
# As with the other script tests, the REAL, unmodified installers are driven end-to-end
# through a stub-first PATH; the daemon is modelled by the shared `dz-stub-exec` stub
# (see _helpers.bash) and we assert on the docker argv plus the printed text, not just
# exit status. `sleep` is stubbed to a no-op, so the retry *timing* is not exercised
# here — only the attempt count and the outcome each state reports.

bats_require_minimum_version 1.5.0

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  export DOCKER_LOG
  : >"$DOCKER_LOG"
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  make_stubs "$STUB_BIN"
}

connect_attempts() { grep -c 'doublezero connect multicast' "$DOCKER_LOG" || true; }

# run_installer <script>: sets $status, writes stdout to $OUT and stderr to $ERR.
# The banner goes to stdout and the warnings to stderr, and both matter here.
run_installer() {
  OUT="$BATS_TEST_TMPDIR/$1.out"
  ERR="$BATS_TEST_TMPDIR/$1.err"
  : >"$DOCKER_LOG"
  status=0
  ( common_env; bash "$SCRIPTS_DIR/$1.sh" ) >"$OUT" 2>"$ERR" || status=$?
}

dump() { echo "# --- stdout"; sed 's/^/#   /' "$OUT"; echo "# --- stderr"; sed 's/^/#   /' "$ERR"; }

@test "cold daemon that warms up: retries and reports a real connection (all scripts)" {
  # The issue's exact scenario: the first attempts lose the race against device
  # probing, a later one lands. This is the regression test the fix exists for.
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    ( connect_fails_n_times 2
      run_installer "$s"
      if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; dump; exit 1; fi
      n="$(connect_attempts)"
      if [ "$n" -lt 3 ]; then echo "# $s.sh made $n connect attempts, expected >=3"; dump; exit 1; fi
      if ! grep -qi 'connect attempt 1/4 failed' "$ERR"; then echo "# $s.sh never reported the failed first attempt"; dump; exit 1; fi
      if ! grep -q 'Done\.' "$OUT"; then echo "# $s.sh never reported success after connecting"; dump; exit 1; fi
      if grep -qi 'NOT CONNECTED' "$ERR"; then echo "# $s.sh claimed a failure after connecting"; dump; exit 1; fi
      exit 0
    ) || fails=1
  done
  [ "$fails" -eq 0 ]
}

@test "connect never succeeds: no success banner, retries, tells the operator what to run (all scripts)" {
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    ( connect_always_fails
      run_installer "$s"
      # Exit stays 0: the container and the CLI are installed either way and the
      # one-liner's contract is unchanged — the printed outcome is what differs.
      if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; dump; exit 1; fi
      n="$(connect_attempts)"
      if [ "$n" -ne 4 ]; then echo "# $s.sh made $n connect attempts, expected the full 4"; dump; exit 1; fi
      if grep -q 'Done\.' "$OUT"; then echo "# $s.sh printed a 'Done.' banner with no tunnel"; dump; exit 1; fi
      if grep -qi 'Connected\.' "$OUT"; then echo "# $s.sh claimed a connection with no tunnel"; dump; exit 1; fi
      if ! grep -qi 'NOT CONNECTED' "$ERR"; then echo "# $s.sh never said it is not connected"; dump; exit 1; fi
      if ! grep -q 'doublezero connect multicast' "$OUT"; then echo "# $s.sh never gave the by-hand retry command"; dump; exit 1; fi
      if ! grep -q 'doublezero status' "$OUT"; then echo "# $s.sh never gave the status check"; dump; exit 1; fi
      if ! grep -q '^docker run ' "$DOCKER_LOG"; then echo "# $s.sh never started the container"; dump; exit 1; fi
      exit 0
    ) || fails=1
  done
  [ "$fails" -eq 0 ]
}

@test "connect exits 0 but no session appears: reports provisioning, never 'Connected' (all scripts)" {
  # Upstream's `connect` prints "Tunnel provisioning in progress" and returns 0 when its
  # own provisioning poll times out, so an exit-code-only gate would call this connected.
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    ( dz_reports_disconnected
      run_installer "$s"
      if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; dump; exit 1; fi
      n="$(connect_attempts)"
      if [ "$n" -ne 1 ]; then echo "# $s.sh made $n connect attempts on an accepted connect, expected 1"; dump; exit 1; fi
      if grep -qi 'Connected\.' "$OUT"; then echo "# $s.sh claimed a connection the daemon never reported"; dump; exit 1; fi
      if ! grep -qi 'Not connected yet' "$ERR"; then echo "# $s.sh never reported the tunnel as still provisioning"; dump; exit 1; fi
      if grep -q 'NOT CONNECTED:' "$ERR"; then echo "# $s.sh reported an outright failure for an accepted connect"; dump; exit 1; fi
      exit 0
    ) || fails=1
  done
  [ "$fails" -eq 0 ]
}

@test "session comes up despite a failing attempt: stops retrying over a live tunnel (all scripts)" {
  # A client-side timeout can outlive a connect the daemon went on to complete. The
  # pre-attempt probe must see that and stop, not re-connect three more times.
  local fails=0
  for s in "${SCRIPTS[@]}"; do
    ( connect_always_fails
      export DZ_TEST_SESSION_UP_AFTER=1
      run_installer "$s"
      if [ "$status" -ne 0 ]; then echo "# $s.sh exited $status"; dump; exit 1; fi
      n="$(connect_attempts)"
      if [ "$n" -ne 1 ]; then echo "# $s.sh made $n connect attempts over a live tunnel, expected 1"; dump; exit 1; fi
      if ! grep -q 'doublezero status --json' "$DOCKER_LOG"; then echo "# $s.sh never asked the daemon for the session state"; dump; exit 1; fi
      if ! grep -q 'Done\.' "$OUT"; then echo "# $s.sh didn't recognise the live session"; dump; exit 1; fi
      exit 0
    ) || fails=1
  done
  [ "$fails" -eq 0 ]
}

@test "failed connect still finishes the script, and the last thing printed is the truth" {
  # The closing banner is the last thing a new user reads, and connect.sh's CLI offer
  # (section 9) owns the final screenful — so the outcome is restated after it. The
  # management hints and the offer must both still run: neither is load-bearing, but
  # silently dropping them would be a second regression.
  connect_always_fails
  export DZ_INSTALL_CLI=0
  run_installer connect
  [ "$status" -eq 0 ] || { echo "# exited $status"; dump; false; }
  grep -q 'Manage with:' "$OUT" || { echo "# management hints missing on the failure path"; dump; false; }
  grep -qi 'Skipping the doublezero-edge CLI' "$OUT" || { echo "# never reached the CLI offer"; dump; false; }
  grep -qi 'Reminder: this host is NOT connected' "$ERR" || { echo "# no closing reminder that the tunnel is down"; dump; false; }
}

@test "docker-entrypoint's dz_connected matches the daemon's real session values" {
  # It gates the graceful `doublezero disconnect` on `docker stop`; a pattern matching no
  # live value leaves every restart's onchain session to expire on its own instead.
  # Extracted rather than sourced: the entrypoint starts the daemon at load time.
  local fn="$BATS_TEST_TMPDIR/dz_connected.sh"
  sed -n '/^dz_connected()/,/^}/p' "$REPO_ROOT/docker-entrypoint.sh" >"$fn"
  grep -q 'session_status' "$fn" || { echo "# could not extract dz_connected from docker-entrypoint.sh"; false; }

  cat >"$STUB_BIN/doublezero" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$DZ_TEST_STATUS_JSON"
EOF
  chmod +x "$STUB_BIN/doublezero"

  probe() {  # probe <session_status value> -> 0 iff dz_connected reads it as live
    ( export PATH="$STUB_BIN:$PATH"
      export DZ_TEST_STATUS_JSON="[{\"response\":{\"doublezero_status\":{\"session_status\":\"$1\"}}}]"
      # shellcheck disable=SC1090
      . "$fn"
      dz_connected )
  }

  # Live values, from the daemon's own SessionStatus strings — the multicast one is what
  # this container actually reports (see the capture in src/ingest/subscriptions.rs).
  probe "BGP Session Up"   || { echo "# missed a live BGP session"; false; }
  probe "PIM Adjacency Up" || { echo "# missed a live multicast session"; false; }
  # Every failure value must read as down.
  for v in "BGP Session Down" "BGP Session Failed" "Pending BGP Session" \
           "Initializing BGP Session" "Network Unreachable" "disconnected"; do
    if probe "$v"; then echo "# read '$v' as a live tunnel"; false; fi
  done
}
