#!/usr/bin/env bats
#
# Coverage for Task 6 of docs/design/cli-packaging-and-installer-plan.md: the image now bakes in
# DZ_FEED_REGISTRY_URL (the hosted feeds document) as a Dockerfile ENV default, and connect.sh
# gained two matching pieces of behavior:
#
#   1. It surfaces the bridge's own "feed registry resolved" startup log line, since a host that
#      can't reach the URL falls back to the built-in document *silently by design* -- that log
#      line is the only signal, so the installer echoes it rather than leaving the operator to
#      dig for it.
#   2. Source::from_flags (ingest/registry.rs) picks the URL over a bind-mounted file whenever the
#      URL is non-empty. Now that the image always sets one, an operator who asks for the file
#      (DZ_FEED_REGISTRY) without also asking for a URL of their own would otherwise have the file
#      silently shadowed by the image's default -- so the installer clears the URL on the
#      container in that case, and leaves it alone whenever the operator set it explicitly.
#
# Only connect.sh is driven here (as install_cli.bats does for Task 5): connect-testnet.sh /
# connect-devnet.sh are untouched by either change.

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

run_args() {
  grep '^docker run ' "$DOCKER_LOG"
}

@test "surfaces the resolved feed registry source from the bridge log" {
  local out="$BATS_TEST_TMPDIR/out" extra="$BATS_TEST_TMPDIR/extra.log"
  printf 'feed registry resolved source="url https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json" version=1 rows=6 receivers=56\n' >"$extra"
  ( common_env; export DZ_TEST_DOCKER_LOG_EXTRA="$extra" DZ_INSTALL_CLI=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  grep -q 'Feed registry:.*source="url https://get.doublezero.xyz' "$out" \
    || { echo "# never surfaced the resolved registry source:"; sed 's/^/#   /' "$out"; false; }
}

@test "no resolved-registry log line yet: warns instead of failing" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_INSTALL_CLI=0; bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  grep -qi "feed registry" "$out" || { echo "# never mentioned the feed registry at all:"; sed 's/^/#   /' "$out"; false; }
}

@test "DZ_FEED_REGISTRY set, no explicit URL: the image's default URL is cleared" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_FEED_REGISTRY="$REGISTRY_FILE" DZ_INSTALL_CLI=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  run_args | grep -q -- "-e DZ_FEED_REGISTRY_URL=" \
    || { echo "# never cleared DZ_FEED_REGISTRY_URL on the container:"; sed 's/^/#   /' "$DOCKER_LOG"; false; }
  run_args | grep -q -- "-e DZ_FEED_REGISTRY=$REGISTRY_FILE" \
    || { echo "# never forwarded DZ_FEED_REGISTRY:"; sed 's/^/#   /' "$DOCKER_LOG"; false; }
}

@test "DZ_FEED_REGISTRY and an explicit DZ_FEED_REGISTRY_URL both set: the explicit URL is left alone" {
  local out="$BATS_TEST_TMPDIR/out"
  ( common_env; export DZ_FEED_REGISTRY="$REGISTRY_FILE" DZ_FEED_REGISTRY_URL="https://example.com/custom.json" DZ_INSTALL_CLI=0
    bash "$SCRIPTS_DIR/connect.sh" ) >"$out" 2>&1
  status=$?
  [ "$status" -eq 0 ] || { echo "# exited $status"; sed 's/^/#   /' "$out"; false; }
  run_args | grep -q -- "-e DZ_FEED_REGISTRY_URL=https://example.com/custom.json" \
    || { echo "# the operator's explicit URL was not forwarded as-is:"; sed 's/^/#   /' "$DOCKER_LOG"; false; }
  if run_args | grep -q -- "-e DZ_FEED_REGISTRY_URL= "; then
    echo "# an explicit URL was cleared back to empty:"; sed 's/^/#   /' "$DOCKER_LOG"; false
  fi
}
