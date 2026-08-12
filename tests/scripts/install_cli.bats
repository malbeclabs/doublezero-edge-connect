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
  chmod +x "$1/apt-get" "$1/apt-cache"
  install_cloudsmith_curl_stub "$1"
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

# A fake `id` that reports uid 0 (root) for `id -u`, the way connect.sh actually runs
# in the common container deployment. That's the only case where $SUDO resolves empty
# and the "$SUDO -E bash" fix matters; CI's unprivileged runner never exercises it
# otherwise, so this is the seam the fix note (task-4-5-review.md) points at.
install_root_id_stub() {
  cat >"$1/id" <<'EOF'
#!/usr/bin/env bash
[ "$1" = -u ] && { echo 0; exit 0; }
exec /usr/bin/id "$@"
EOF
  chmod +x "$1/id"
}

# Build <bindir> from every real command reachable on the CURRENT $PATH, except
# apt-get/apt-cache/dnf/yum. Used to genuinely hide a package manager from
# `command -v`: a placeholder file that isn't executable doesn't work (PATH search
# skips it and keeps looking, so a real apt-get later on $PATH still wins), and this
# repo's containers have a real apt-get, so `install_apt_stubs`-style shadowing alone
# can't make dnf/yum "the only" package manager. Symlinks (not copies) so the real
# coreutils/sed/grep/etc. connect.sh depends on stay available underneath whichever
# fake package manager the caller layers on top with `make_stubs`/`install_*_stubs`.
install_no_real_pm_path() {
  local bin="$1" d f name
  mkdir -p "$bin"
  local IFS=:
  for d in $PATH; do
    [ -d "$d" ] || continue
    for f in "$d"/*; do
      [ -f "$f" ] && [ -x "$f" ] || continue
      name="${f##*/}"
      case "$name" in apt-get | apt-cache | dnf | yum) continue ;; esac
      [ -e "$bin/$name" ] || ln -s "$f" "$bin/$name" 2>/dev/null
    done
  done
}

# dnf stub: `dnf list --available doublezero-edge` reports "not configured" by default
# (STUB_DNF_AVAILABLE=1 to flip it); `dnf install` logs + answers STUB_DNF_INSTALL_RC.
# Shares the apt stubs' curl (setup.rpm.sh instead of setup.deb.sh, same log/RC knobs).
install_dnf_stubs() {
  cat >"$1/dnf" <<'EOF'
#!/usr/bin/env bash
printf 'dnf %s\n' "$*" >>"$PM_LOG"
case "$1" in
  list)    [ "${STUB_DNF_AVAILABLE:-0}" = 1 ] && exit 0; exit 1 ;;
  install) exit "${STUB_DNF_INSTALL_RC:-0}" ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$1/dnf"
  install_cloudsmith_curl_stub "$1"
}

# yum stub: same shape as dnf's, but `yum list available` (no leading `--`).
install_yum_stubs() {
  cat >"$1/yum" <<'EOF'
#!/usr/bin/env bash
printf 'yum %s\n' "$*" >>"$PM_LOG"
case "$1" in
  list)    [ "${STUB_YUM_AVAILABLE:-0}" = 1 ] && exit 0; exit 1 ;;
  install) exit "${STUB_YUM_INSTALL_RC:-0}" ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$1/yum"
  install_cloudsmith_curl_stub "$1"
}

# The curl stub install_apt_stubs also installs, factored out so the dnf/yum stubs
# share it without pulling in apt-get/apt-cache.
install_cloudsmith_curl_stub() {
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
  chmod +x "$1/curl"
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
  grep -q 'Configuring the malbeclabs/doublezero Cloudsmith repository' "$out" \
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

@test "no answer, no TTY (DZ_INSTALL_CLI unset): skips without crashing" {
  # Every test above sets DZ_INSTALL_CLI, so none of them reach the '*)' branch --
  # the one guarded by the confirm()-TTY-open probe (a readable /dev/tty inode can
  # still fail to OPEN with no controlling terminal; a bare confirm() call would then
  # read into 'ans' and abort under 'set -u'). This is the only test that leaves
  # DZ_INSTALL_CLI unset, so it's the only one that can catch a revert of that probe.
  # setsid drops the controlling terminal deterministically, matching the same
  # technique reinstall_existing.bats uses for its own no-TTY case.
  command -v setsid >/dev/null 2>&1 || skip "setsid not available to drop the controlling TTY"
  local out="$BATS_TEST_TMPDIR/out"
  (
    common_env; unset DZ_ASSUME_YES DZ_INSTALL_CLI
    timeout 30 setsid -w bash "$SCRIPTS_DIR/connect.sh" </dev/null
  ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  if grep -q 'apt-get install' "$PM_LOG"; then echo "# installed with no TTY and no answer:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  if grep -q 'setup.deb.sh' "$PM_LOG"; then echo "# configured the repo with no TTY and no answer:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  grep -qi 'Skipping the doublezero-edge CLI' "$out" || { echo "# never reported skipping the CLI offer:"; sed 's/^/#   /' "$out"; false; }
}

# The suite has no facility to simulate an actual interactive TTY *answer* (only
# `setsid` to remove one, used above) -- there's no `script`/pty helper anywhere in
# tests/scripts/_helpers.bash or the wider repo, so a "types y at the prompt" case
# isn't added here; see the task-5 report follow-up for why.

@test "root host (empty \$SUDO): the Cloudsmith setup script still runs" {
  # $SUDO is only empty when connect.sh runs as uid 0 (the common container
  # deployment) -- CI's unprivileged runner always has $SUDO=sudo, so this is the
  # only test that can catch a revert of the array-based setup_runner back to a bare
  # "$SUDO -E bash" (which, empty, executes "-E" as a command and fails the pipe).
  local out="$BATS_TEST_TMPDIR/out"
  install_root_id_stub "$STUB_BIN"
  ( common_env; export DZ_INSTALL_CLI=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  grep -q 'setup.deb.sh' "$PM_LOG" || { echo "# never attempted to configure the Cloudsmith repo:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -q '^apt-get install.*doublezero-edge' "$PM_LOG" \
    || { echo "# repo setup never led to an install on a root (empty \$SUDO) host:"; sed 's/^/#   /' "$PM_LOG"; false; }
  if grep -qi 'could not configure' "$out"; then echo "# repo setup failed on a root (empty \$SUDO) host:"; sed 's/^/#   /' "$out"; false; fi
}

@test "dnf host: configures the repo and installs via dnf" {
  # apt-get is real on this container, so install_apt_stubs-style shadowing can't
  # make dnf "the only" package manager -- hide the real apt-get/apt-cache from
  # $PATH entirely (install_no_real_pm_path) rather than just adding a competing stub.
  local out="$BATS_TEST_TMPDIR/out"
  local nopm="$BATS_TEST_TMPDIR/bin-dnf"
  make_stubs "$nopm"
  install_no_real_pm_path "$nopm"
  install_dnf_stubs "$nopm"
  ( common_env; export PATH="$nopm"; export DZ_INSTALL_CLI=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  grep -q 'setup.rpm.sh' "$PM_LOG" || { echo "# never configured the Cloudsmith repo via dnf:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -q '^dnf install.*doublezero-edge' "$PM_LOG" || { echo "# never installed doublezero-edge via dnf:"; sed 's/^/#   /' "$PM_LOG"; false; }
}

@test "yum host: configures the repo and installs via yum" {
  local out="$BATS_TEST_TMPDIR/out"
  local nopm="$BATS_TEST_TMPDIR/bin-yum"
  make_stubs "$nopm"
  install_no_real_pm_path "$nopm"
  install_yum_stubs "$nopm"
  ( common_env; export PATH="$nopm"; export DZ_INSTALL_CLI=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  grep -q 'setup.rpm.sh' "$PM_LOG" || { echo "# never configured the Cloudsmith repo via yum:"; sed 's/^/#   /' "$PM_LOG"; false; }
  grep -q '^yum install.*doublezero-edge' "$PM_LOG" || { echo "# never installed doublezero-edge via yum:"; sed 's/^/#   /' "$PM_LOG"; false; }
}

@test "no supported package manager: warns and skips, container still runs" {
  local out="$BATS_TEST_TMPDIR/out"
  local nopm="$BATS_TEST_TMPDIR/bin-nopm"
  make_stubs "$nopm"
  install_no_real_pm_path "$nopm"
  ( common_env; export PATH="$nopm"; export DZ_INSTALL_CLI=1; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  container_running || { echo "# container never reached docker run"; false; }
  if [ -s "$PM_LOG" ]; then echo "# touched a package manager despite none being available:"; sed 's/^/#   /' "$PM_LOG"; false; fi
  grep -qi 'no supported package manager' "$out" || { echo "# never warned about the missing package manager:"; sed 's/^/#   /' "$out"; false; }
}
