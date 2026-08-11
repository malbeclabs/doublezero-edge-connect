#!/usr/bin/env bats
#
# Coverage for the doublezero-edge CLI install offer (Task 5 of
# docs/design/cli-packaging-and-installer-plan.md), added as section 9 of connect.sh: once the
# bridge container is up, the installer offers to configure the org's Cloudsmith package
# repository and install the 'doublezero-edge' CLI package. The property that matters is that
# this step can never break an otherwise-good install -- a decline, or any package-manager
# failure, only warns; the bridge keeps running either way. Every test here asserts on what was
# actually invoked (not just the exit code), and on the container still running, so a revert of
# the underlying behavior is caught rather than passing on exit-status alone.
#
# Output is captured combined (stdout+stderr) into $out: info() (the "naming the repo/package"
# messages this step is about) writes to stdout, warn() to stderr, so a test that only redirects
# stderr would silently miss the very prompt/report text it needs to assert on.
#
# Only connect.sh gained this step (see the task); connect-testnet.sh / connect-devnet.sh are
# untouched, so this file drives connect.sh only, unlike the shared-across-SCRIPTS suites.

load _helpers

setup() {
  STUB_BIN="$BATS_TEST_TMPDIR/bin"
  DOCKER_LOG="$BATS_TEST_TMPDIR/docker.log"
  PM_LOG="$BATS_TEST_TMPDIR/pm.log"
  KEYFILE="$BATS_TEST_TMPDIR/id.json"
  export DOCKER_LOG PM_LOG
  : >"$DOCKER_LOG"
  : >"$PM_LOG"
  printf '[%s]' "$(seq -s, 64 | sed 's/[0-9]*/0/g')" >"$KEYFILE"
  make_stubs "$STUB_BIN"
  install_apt_stubs "$STUB_BIN"
}

# apt-get / apt-cache stubs, logged to $PM_LOG. Controlled by:
#   STUB_APT_CANDIDATE     "Candidate:" line body apt-cache should report (default "(none)",
#                          i.e. the repo is NOT configured yet)
#   STUB_APT_INSTALL_RC    exit status of `apt-get install` (default 0)
# Also overrides `curl` so the two Cloudsmith setup-script URLs are logged and answer
# STUB_CLOUDSMITH_CURL_RC (default 0); every other URL keeps the hermetic default helpers
# behavior (exit 1) so the rest of the installer (access-pass check, cloud detection, ...) is
# unaffected.
install_apt_stubs() {
  cat >"$1/apt-get" <<'EOF'
#!/usr/bin/env bash
printf 'apt-get %s\n' "$*" >>"$PM_LOG"
case "$1" in
  install) exit "${STUB_APT_INSTALL_RC:-0}" ;;
  *)       exit 0 ;;
esac
EOF
  cat >"$1/apt-cache" <<'EOF'
#!/usr/bin/env bash
printf 'apt-cache %s\n' "$*" >>"$PM_LOG"
echo "  Candidate: ${STUB_APT_CANDIDATE:-(none)}"
exit 0
EOF
  cat >"$1/curl" <<'EOF'
#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    *setup.deb.sh*|*setup.rpm.sh*)
      printf 'curl %s\n' "$*" >>"$PM_LOG"
      exit "${STUB_CLOUDSMITH_CURL_RC:-0}"
      ;;
  esac
done
exit 1
EOF
  chmod +x "$1/apt-get" "$1/apt-cache" "$1/curl"
}

# A fake already-installed doublezero-edge on PATH.
install_doublezero_edge_stub() {
  cat >"$1/doublezero-edge" <<'EOF'
#!/usr/bin/env bash
[ "$1" = --version ] && { echo "doublezero-edge 1.2.3"; exit 0; }
exit 0
EOF
  chmod +x "$1/doublezero-edge"
}

container_running() {
  grep -q '^docker run ' "$DOCKER_LOG"
}

@test "accept: configures the repo and installs the package" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  grep -q 'setup.deb.sh' "$PM_LOG" || { echo "# never configured the Cloudsmith repo:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -q '^apt-get install.*doublezero-edge' "$PM_LOG" || { echo "# never installed doublezero-edge:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -q 'Configuring the malbeclabs/doublezero-mainnet-beta Cloudsmith repository' "$out" \
    || { echo "# never named the repository before configuring it:"; sed 's/^/#   /' "$out"; false; }
}

@test "decline: does not install, exits success, container still runs" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  if grep -q 'apt-get install' "$PM_LOG"; then echo "# installed despite DZ_INSTALL_CLI=0:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  if grep -q 'setup.deb.sh' "$PM_LOG"; then echo "# configured the repo despite declining:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  grep -qi 'install it later' "$out" || { echo "# decline message didn't say how to install later:"; sed 's/^/#   /' "$out"; false; }
}

@test "already installed: no prompt, no repo setup, no install attempt" {
  local out="$BATS_TEST_TMPDIR/out"
  install_doublezero_edge_stub "$STUB_BIN"
  ( common_env; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  if [ -s "$PM_LOG" ]; then echo "# touched the package manager even though the CLI is already installed:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  grep -qi 'already installed' "$out" || { echo "# never reported the already-installed version:"; sed 's/^/#   /' "$out"; false; }
  grep -qi '1\.2\.3' "$out" || { echo "# didn't report the installed version:"; sed 's/^/#   /' "$out"; false; }
}

@test "repository already configured: skips repo setup, installs directly" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=1 STUB_APT_CANDIDATE=1.2.3; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  if grep -q 'setup.deb.sh' "$PM_LOG"; then echo "# reconfigured an already-configured repo:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  grep -q '^apt-get install.*doublezero-edge' "$PM_LOG" || { echo "# never installed doublezero-edge despite the repo already being configured:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -qi 'already configured' "$out" || { echo "# never said the repo was already configured:"; sed 's/^/#   /' "$out"; false; }
}

@test "package manager fails: warns, exits success, container still runs" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=1 STUB_APT_INSTALL_RC=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status when the package manager failed"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  grep -q '^apt-get install.*doublezero-edge' "$PM_LOG" || { echo "# install was never attempted:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -qi 'doublezero-edge failed' "$out" || { echo "# never warned that the install failed:"; sed 's/^/#   /' "$out"; false; }
  if grep -qi 'installed doublezero-edge cli' "$out"; then echo "# claimed success despite the failure:"; sed 's/^/#   /' "$out"; false; fi
}
