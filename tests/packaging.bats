#!/usr/bin/env bats
#
# setup_file/teardown_file need bats-core >= 1.5.0.
bats_require_minimum_version 1.5.0

# Pins the CLI package's inertness (Task 4 of
# docs/design/cli-packaging-and-installer-plan.md): the built .deb/.rpm contain exactly the
# binary and three completion files and nothing else, declare no dependencies and ship no
# maintainer scripts, and the binary is statically linked. That is the property that lets
# scripts/connect.sh offer the package from a prompt without risk, so it is pinned here rather
# than assumed from release/.goreleaser.base.edge-cli.yaml.
#
# This test builds a REAL snapshot .deb/.rpm with goreleaser and asserts on `dpkg`/`rpm`/`ldd`
# output over those artifacts -- never on the YAML -- so a dependency or maintainer script that
# arrived through some other mechanism (a stray nfpm default, a future edit to a shared template,
# ...) still fails this test. A test that parsed the config and asserted "no `dependencies:` key"
# would only ever assert the config matches itself.
#
# goreleaser OSS cannot parse two goreleaser-pro-only top-level keys the shipped config carries
# (`monorepo:`, `nightly:`), so setup_file below builds from a COPY of the config with just those
# two keys deleted (via `yq`) and `dist:` redirected to a scratch directory. Every other key --
# builds/archives/nfpms/changelog/release/git, which is what this test actually cares about -- is
# exactly what ships. This keeps the test self-contained (no GORELEASER_KEY, so it runs on every
# PR including forks) at the cost of not exercising goreleaser-pro's monorepo tag resolution,
# which Task 3's release workflow is the only thing that does.
#
# Requires (Linux/amd64 only -- this package is Linux/amd64-only by design): cargo + the
# x86_64-unknown-linux-musl rustup target, musl-tools (musl-gcc), rpm, dpkg-deb, ldd, goreleaser,
# and yq. See .github/workflows/packaging-tests.yml for the CI job that installs them; a
# developer machine without this toolchain (e.g. macOS) skips every test here rather than failing.

REPO_ROOT="$(cd "$(dirname "${BATS_TEST_FILENAME}")/.." && pwd)"
GORELEASER_CONFIG="$REPO_ROOT/release/.goreleaser.base.edge-cli.yaml"

# The exact four paths the packages promise, and nothing else.
EXPECTED_PATHS="/usr/bin/doublezero-edge
/usr/share/bash-completion/completions/doublezero-edge
/usr/share/fish/vendor_completions.d/doublezero-edge.fish
/usr/share/zsh/site-functions/_doublezero-edge"

setup_file() {
  for tool in goreleaser yq dpkg-deb dpkg rpm ldd cargo rustup; do
    command -v "$tool" >/dev/null 2>&1 || skip "requires $tool on PATH -- see packaging-tests.yml"
  done
  rustup target list --installed 2>/dev/null | grep -qx 'x86_64-unknown-linux-musl' \
    || skip "requires the x86_64-unknown-linux-musl rustup target"

  PKG_WORK="${BATS_FILE_TMPDIR}/pkgwork"
  mkdir -p "$PKG_WORK"

  yq eval 'del(.monorepo) | del(.nightly) | .dist = "'"${PKG_WORK}"'/dist"' \
    "$GORELEASER_CONFIG" >"$PKG_WORK/goreleaser.yaml"

  (
    cd "$REPO_ROOT" || exit 1
    CC_x86_64_unknown_linux_musl=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
      goreleaser release --config "$PKG_WORK/goreleaser.yaml" --snapshot --clean --skip=publish
  ) >"$PKG_WORK/goreleaser.log" 2>&1
  echo "$?" >"$PKG_WORK/build.status"

  DEB_PATH="$(find "$PKG_WORK/dist" -maxdepth 1 -name '*.deb' 2>/dev/null | head -1)"
  RPM_PATH="$(find "$PKG_WORK/dist" -maxdepth 1 -name '*.rpm' 2>/dev/null | head -1)"
  printf '%s' "$DEB_PATH" >"$PKG_WORK/deb.path"
  printf '%s' "$RPM_PATH" >"$PKG_WORK/rpm.path"

  export PKG_WORK
}

teardown_file() {
  # The config's `before.hooks` write completions to build/completions/ relative to the repo
  # root (gitignored); clean up so nothing lingers in the working tree.
  rm -rf "${REPO_ROOT}/build"
}

setup() {
  PKG_WORK="${BATS_FILE_TMPDIR}/pkgwork"
  DEB_PATH="$(cat "$PKG_WORK/deb.path" 2>/dev/null)"
  RPM_PATH="$(cat "$PKG_WORK/rpm.path" 2>/dev/null)"
}

@test "goreleaser snapshot build succeeds and produces a deb and an rpm" {
  status="$(cat "$PKG_WORK/build.status" 2>/dev/null)"
  if [ "$status" != "0" ]; then
    cat "$PKG_WORK/goreleaser.log"
  fi
  [ "$status" = "0" ]
  [ -n "$DEB_PATH" ]
  [ -f "$DEB_PATH" ]
  [ -n "$RPM_PATH" ]
  [ -f "$RPM_PATH" ]
}

@test "deb contains exactly the binary and three completions, nothing else" {
  [ -n "$DEB_PATH" ]
  run dpkg -c "$DEB_PATH"
  [ "$status" -eq 0 ]
  # Regular-file entries only ($1 mode starts with '-'); directories are structural, not content.
  # dpkg -c paths are "./usr/bin/..." -- strip only the leading '.' so '/usr/bin/...' remains.
  actual="$(printf '%s\n' "$output" | awk '$1 ~ /^-/ { sub(/^\./, "", $6); print $6 }' | sort)"
  expected="$(printf '%s\n' "$EXPECTED_PATHS" | sort)"
  [ "$actual" = "$expected" ]
}

@test "rpm contains exactly the binary and three completions, nothing else" {
  [ -n "$RPM_PATH" ]
  run rpm -qlp "$RPM_PATH"
  [ "$status" -eq 0 ]
  actual="$(printf '%s\n' "$output" | sort)"
  expected="$(printf '%s\n' "$EXPECTED_PATHS" | sort)"
  [ "$actual" = "$expected" ]
}

@test "deb declares no Depends field" {
  [ -n "$DEB_PATH" ]
  run dpkg-deb -I "$DEB_PATH"
  [ "$status" -eq 0 ]
  info="$output"
  run grep -E '^ *Depends:' <<<"$info"
  [ "$status" -ne 0 ]
}

@test "deb ships no maintainer scripts" {
  [ -n "$DEB_PATH" ]
  local ctrl="${PKG_WORK}/deb-control"
  rm -rf "$ctrl"
  run dpkg-deb -e "$DEB_PATH" "$ctrl"
  [ "$status" -eq 0 ]
  for script in preinst postinst prerm postrm config; do
    [ ! -e "$ctrl/$script" ]
  done
}

@test "rpm declares no requires" {
  [ -n "$RPM_PATH" ]
  run rpm -qp --requires "$RPM_PATH"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "rpm ships no scriptlets" {
  [ -n "$RPM_PATH" ]
  run rpm -qp --scripts "$RPM_PATH"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "binary is statically linked" {
  [ -n "$DEB_PATH" ]
  local extract="${PKG_WORK}/deb-extract"
  rm -rf "$extract"
  run dpkg-deb -x "$DEB_PATH" "$extract"
  [ "$status" -eq 0 ]
  run ldd "$extract/usr/bin/doublezero-edge"
  # A dynamically-linked binary makes ldd print resolved libraries and exit 0; a static one
  # (including musl static-PIE, which still carries a PT_INTERP string) makes it print exactly
  # this and exit nonzero -- never partial output, so grepping for a marker string is unambiguous.
  echo "$output" # surfaced on failure
  printf '%s\n' "$output" | grep -qiE 'not a dynamic executable|statically linked'
}
