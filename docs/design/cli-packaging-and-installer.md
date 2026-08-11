# CLI packaging and installer — design

**Date:** 2026-08-11
**Status:** approved in conversation, not yet planned

## Goal

Ship `doublezero-edge` as a signed deb/rpm, and teach `scripts/connect.sh` to offer it during
install, so one command gets an operator both the bridge (container) and the CLI. Bake the hosted
feed-registry URL into the image. Document all of it.

## Scope

**In:** goreleaser/nfpm packaging for the CLI, a tag-driven release workflow publishing to Cloudsmith,
a prompt in `connect.sh` to install the package, `DZ_FEED_REGISTRY_URL` as an image default, and the
user-facing docs for all three.

**Out, explicitly:**
- Any change to how the bridge itself is deployed. It stays a container. The package carries the CLI
  and nothing else — no systemd unit, no dependencies, no maintainer scripts, nothing privileged.
- The unified multi-repo feed registry. The hosted document is hand-curated for now; the plumbing to
  combine feeds from several repos is a separate piece of work.
- macOS and non-amd64. The installer is already Linux/amd64-only because the image is.

## Packaging

Mirror the pattern already in use for the DoubleZero client (`release/.goreleaser.base.client.yaml`
in the `doublezero` repo), because it is proven here and because reusing it means reusing its signing
and distribution rather than inventing a second path.

- `release/.goreleaser.base.edge-cli.yaml` in this repo.
- `builder: rust`, `--package=doublezero-edge`, target `x86_64-unknown-linux-musl`, with the same
  `CC_x86_64_unknown_linux_musl=musl-gcc` / linker env the client build uses. **Static by design:** a
  glibc-linked binary built on Debian fails on older hosts, and we do not control the host distro.
- `nfpms:` emitting **deb and rpm**, `package_name: doublezero-edge`, `bindir: /usr/bin`.
- **No `contents:` beyond the binary and completions, no `dependencies:`, no `scripts:`.** The CLI-only
  scope is what makes this package safe to install unattended; keep it that way. If a future change
  wants a maintainer script, that is a design decision to revisit, not a detail.
- Shell completions generated in `before.hooks` and bundled to the same three paths the client uses
  (bash, zsh, fish). Requires adding a `completion` subcommand to the CLI — a few lines of clap, and
  what makes the two CLIs feel like one product.
- Release workflow modelled on `release.client.yml`: tag-triggered, installs `rpm` and `musl-tools`,
  runs goreleaser-pro.

### Signing

**Cloudsmith signs.** The org already publishes there (`cloudsmiths:` block, org `malbeclabs`), which
means repository indices and packages are signed with a key that already exists and is already
trusted by hosts that installed the DoubleZero client. There is no new key to create, custody to
arrange, or rotation story to invent. The release job needs `CLOUDSMITH_TOKEN` and `GORELEASER_KEY`,
the same secrets the client release uses.

### Which repository

Publish into the **existing** `doublezero-testnet` / `doublezero-mainnet-beta` repos, not a new one.

A host that installed the DoubleZero client already trusts that source, so `apt install
doublezero-edge` works with no second key, no second source file and no second trust decision. That
is the single largest UX gain available here and it costs nothing.

The CLI has no ledger coupling — it is an HTTP client over `/v1` and `/admin` — so unlike the client
it needs **no env-specific build variants**. One artifact, published to both repos.

## Installer flow

`connect.sh` keeps its current behaviour and gains one step after the container is confirmed healthy.

1. If `doublezero-edge` is already present, report the version and continue. No prompt.
2. Otherwise prompt — and **describe the action before taking it**: that it will configure a signed
   package repository (named) and install one package (named). An installer that adds a repo silently
   is the thing operators are right to be angry about.
3. Detect apt vs dnf/yum. If the DoubleZero repository is already configured — likely, since these
   hosts run the client — skip straight to install.
4. `DZ_INSTALL_CLI=1|0` answers non-interactively; existing `DZ_ASSUME_YES=1` implies yes.
5. **Declining is not an error.** The container still runs; the closing message says how to install
   later. Same for a package-manager failure: warn, do not `die` — the bridge is the product, the CLI
   is a convenience, and a broken mirror must not fail an otherwise good install.

## Feed registry URL in the image

`ENV DZ_FEED_REGISTRY_URL=https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json`

Flat path, no venue segment: the feeds document is a superset across all DoubleZero Edge sources.
(Per-source **channel** documents nest under a source segment; they are not part of this work.)

Chosen as an image `ENV` rather than a compiled-in clap default so the binary stays neutral — running
from source reaches no network — while the container is opinionated, inspectable via `docker inspect`
and overridable with `-e`.

Two consequences to document rather than discover:
- The container now makes an outbound HTTPS call at startup. Locked-down hosts should override with a
  bind-mounted file (`DZ_FEED_REGISTRY`), which the installer already supports and mounts.
- A host that cannot reach the URL falls back to the built-in copy **silently by design**. The log
  line naming the resolved source is the only signal, so `connect.sh` should surface it after startup.

## Version skew

The CLI now versions independently of the image. The compatibility contract, worth stating in docs:
`/v1` and `/admin` are additive-only and both sides ignore unknown fields, so an older CLI drops
fields it does not know and a newer CLI falls back when a field is absent. This is already how
`label` and `symbol_prefixes` behave.

## Documentation

- **README:** a one-liner install section covering what gets installed (container + CLI), what the
  prompt does, and how to decline; the `DZ_INSTALL_CLI` / `DZ_ASSUME_YES` knobs; and how to install
  the CLI alone on a host that only queries a remote bridge over `--url`.
- **Upgrades and removal:** `apt upgrade` / `dnf upgrade` for the CLI, image pull for the bridge, and
  what uninstalling each does and does not remove.
- **Feed registry:** where the document is served, that it is the default, how to override with a URL
  or a file, and how to tell from the logs which source actually loaded.
- **CLAUDE.md:** the packaging and release path, so the next change to it does not reinvent this.

## Testing

- Package contents asserted from the built artifact (`dpkg -c` / `rpm -qlp`): the binary at
  `/usr/bin/doublezero-edge`, the three completion files, and **nothing else** — the absence of
  maintainer scripts and dependencies is the property worth pinning, since it is what makes the
  package safe.
- The static build verified with `ldd` reporting a non-dynamic executable, so a glibc assumption
  cannot creep back in.
- Installer paths exercised with the package manager stubbed, as `connect.sh`'s existing tests do:
  accept, decline, already-installed, repo-already-configured, and package-manager-failure. Each must
  leave the container running.
- The baked URL asserted from the built image (`docker inspect`), and a startup log check that the
  resolved source is the URL and not the built-in fallback.
