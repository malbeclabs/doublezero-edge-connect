# Contributing to doublezero-edge-connect

Thanks for your interest in contributing! This document covers how to build, test, and
submit changes.

## Getting started

You need a recent stable Rust toolchain (plus nightly for `rustfmt` — see below).

```bash
cargo build --release
cargo test                 # codec round-trip + refdata subscriber state machine
cargo test quote_round_trip # run a single test by name
```

The architecture and internals are documented in [CLAUDE.md](CLAUDE.md); the WebSocket
output contract is in [PROTOCOL.md](PROTOCOL.md).

## Before you open a PR

CI runs the following and your PR must pass all of them. Run them locally first:

```bash
# Build
cargo build --release --verbose

# Formatting — nightly is required for the imports_granularity option
cargo +nightly fmt --all -- --check --config imports_granularity=Crate

# Lint — warnings are errors
cargo +stable clippy --all-targets -- -Dclippy::all -Dwarnings

# Tests
cargo test --all-features --verbose
```

If you have [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny) installed, you
can also run the supply-chain check that CI enforces:

```bash
cargo deny check
```

## Pull request guidelines

- **Branch from `main`** and open your PR against `main`.
- **Update [CHANGELOG.md](CHANGELOG.md)** under the `[Unreleased]` section. If a change
  genuinely doesn't warrant an entry (e.g. a typo fix), apply the `skip-changelog` label
  — CI enforces this.
- **Fill out the PR template**, including testing evidence.
- **Keep the protocol stable.** Any change to the WebSocket JSON (field names, message
  types, control frames) must preserve the forward-compatibility rule (consumers ignore
  unknown types/fields) and be reflected in [PROTOCOL.md](PROTOCOL.md).
- **Don't change validated codec offsets** in `src/ingest/codec.rs` without
  re-validating against the reference decoder (see the notes in CLAUDE.md).

## Reporting bugs and requesting features

Open an issue on the tracker. For security issues, **do not open a
public issue** — see [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the same license that covers this project.
