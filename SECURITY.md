# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Report them privately via GitHub Security Advisories:
[**Report a vulnerability**](https://github.com/malbeclabs/doublezero-edge-connect/security/advisories/new).

If you cannot use that channel, email **security@malbeclabs.com**.

Please include enough detail to reproduce: affected version/image tag, configuration,
and a proof of concept if available. We'll acknowledge your report, keep you updated on
remediation, and credit you when a fix ships (unless you prefer otherwise).

## Supported versions

This project is pre-1.0. Security fixes are applied to the latest release and `main`.
Pin to an immutable image digest (`:sha-<commit>`) or release tag (`:<env>-X.Y.Z`) for
reproducible deployments.

## Security model

`doublezero-edge-connect` is designed to run on a **trusted/local network** and serves
its WebSocket **without TLS** — the same stance as the DoubleZero overlay it rides on.
If you expose it beyond a trusted network, terminate TLS and apply access control at a
reverse proxy in front of it. See [README.md](README.md) and [PROTOCOL.md](PROTOCOL.md)
for the operational details.
