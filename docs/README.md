# Documentation

Reference docs for operating and extending `doublezero-edge-connect`. Start at the
[top-level README](../README.md) for what it does, the install one-liner, and configuration.

## Operating

- **[Self-hosting](self-hosting.md)** — build from source or run the Docker image by hand,
  outside the installer one-liner (CLI flags, image tags).
- **[Output sinks](output-sinks.md)** — the WebSocket output and how sinks are enabled.
- **[Input sources](input-sources.md)** — the always-on DZ Edge multicast feeds and the optional
  Hyperliquid public WebSocket backstop.
- **[Solana shred forwarding](shred-forwarding.md)** — the optional `edge-solana-*` shred
  forwarder, its dedup/sigverify modes, and configuration.

## Contracts & internals

- **[PROTOCOL.md](../PROTOCOL.md)** — the WebSocket JSON contract (v1) that consumers code against.
- **[CLAUDE.md](../CLAUDE.md)** — architecture and module-level internals.
- **[scripts/README.md](../scripts/README.md)** — the installer scripts and their full env-var
  reference.
- **[edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec)** — the upstream binary feed
  format.
