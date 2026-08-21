# CLI packaging and installer — implementation plan

Design: [cli-packaging-and-installer.md](cli-packaging-and-installer.md)

Six tasks. Each ends with something independently verifiable.

## Global constraints

- Build and test in the Linux container; the crate does not build on macOS. The `doublezero-edge`
  crate does build natively.
- Lint contract is `cargo +nightly fmt --all -- --check --config imports_granularity=Crate` and
  `cargo +stable clippy --workspace --all-targets -- -Dclippy::all -Dwarnings`. Plain `cargo fmt`
  is weaker than CI in both respects.
- Commit and PR text carry no venue names or venue-specific tickers.
- Commits are signed. No AI attribution.
- Every test must fail when its subject is reverted, and its fixture must be able to express the
  condition it is named for.

---

## Task 1 — `completion` subcommand

**Files:** `doublezero-edge/src/main.rs`

Add `doublezero-edge completion <bash|zsh|fish>` via `clap_complete`, writing to stdout. Packaging
generates the three files from it, so it must run without a config file, a server, or network.

- Test: each shell emits non-empty output naming the binary, and an unknown shell exits non-zero.
- Verify: `cargo run -p doublezero-edge -- completion bash | head`.

## Task 2 — goreleaser config

**Files:** `release/.goreleaser.base.edge-cli.yaml`, `release/.goreleaser.testnet.edge-cli.yaml`,
`release/.goreleaser.mainnet-beta.edge-cli.yaml`

Model on `.goreleaser.base.client.yaml` in the `doublezero` repo.

- `builder: rust`, `--package=doublezero-edge`, target `x86_64-unknown-linux-musl`, `musl-gcc` env.
- `before.hooks` generate the three completion files from Task 1.
- `nfpms:` deb + rpm, `package_name: doublezero-edge`, `bindir: /usr/bin`, completions to the same
  paths the client uses.
- No `dependencies`, no `scripts`, no unit files.
- Overlays add the `cloudsmiths:` block per environment.

Verify locally with `goreleaser build --snapshot --clean` and, if the pro binary is unavailable, by
schema-checking the YAML and reviewing against the client config field by field.

## Task 3 — release workflow

**Files:** `.github/workflows/release.edge-cli.yml`

Tag-triggered (`doublezero-edge/v*`), mirroring `release.client.yml`: musl target, `rpm` and
`musl-tools` installed, goreleaser-pro, secrets `GORELEASER_KEY` and `CLOUDSMITH_TOKEN`.

Verify: `actionlint` passes, and a dry run on a throwaway tag publishes to the testnet repository
only.

## Task 4 — package contents are inert

**Files:** `tests/packaging.bats` (or alongside the existing bats suite)

Assert from built artifacts, not from config:

- `dpkg -c` / `rpm -qlp` list the binary at `/usr/bin/doublezero-edge` and the three completion
  files, and **nothing else**.
- Neither package declares dependencies or maintainer scripts. This is the property that makes the
  package safe to install from a prompt, so it is pinned rather than assumed.
- `ldd` reports the binary as not dynamically linked.

## Task 5 — installer prompt

**Files:** `scripts/connect.sh`

After the container is healthy:

- Already installed → report version, no prompt.
- Otherwise prompt, naming the repository and the package before configuring anything.
- apt and dnf/yum paths; skip repo setup when already configured.
- `DZ_INSTALL_CLI=1|0`; `DZ_ASSUME_YES=1` implies yes.
- Declining, or any package-manager failure, warns and continues — the bridge is the product.

Tests with the package manager stubbed, as the existing suite does: accept, decline,
already-installed, repo-already-present, manager-fails. Each asserts the container is still running.

## Task 6 — image default and docs

**Files:** `Dockerfile`, `README.md`, `docs/self-hosting.md`, `CLAUDE.md`

- `ENV DZ_FEED_REGISTRY_URL=https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json`.
- `connect.sh` surfaces which registry document actually resolved, since falling back to the built-in
  copy is silent by design.
- Docs: what the one-liner installs, how to decline, CLI-only install for hosts that query a remote
  bridge over `--url`, upgrade and removal for both halves, and how to override the registry with a
  URL or a file.

Verify: `docker inspect` shows the ENV, and a container started without overrides logs the URL as
the resolved document.
