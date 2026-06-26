# syntax=docker/dockerfile:1
#
# doublezero-edge-connect — DoubleZero Edge multicast Top-of-Book -> normalized WebSocket bridge.
#
# Reusable image: other projects either pull it from a registry or reference it as a
# compose service, then consume the WebSocket on $WS_BIND. The multicast/binary input is
# an implementation detail; the only contract is the WS JSON protocol (see PROTOCOL.md).
#
#   docker build -t doublezero-edge-connect .
#   docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
#     doublezero-edge-connect                            # ingest ALL known feeds (default)
#   docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
#     doublezero-edge-connect --feed Hyperliquid         # narrow to specific venue(s)
#   docker run --rm --network host --cap-add NET_ADMIN --device /dev/net/tun \
#     -e DZ_SHRED_DISABLE=true doublezero-edge-connect   # opt out of the auto shred forwarder
#                                                        # (or append --shred-forward-disable)
#
# Host networking (`--network host`) is required: the bridge joins the DZ Edge multicast
# group on the host's `doublezero1` interface, which a bridged container network can't see.
# NET_ADMIN + /dev/net/tun are required by the doublezerod daemon (started by the entrypoint),
# which sets up the DoubleZero tunnel/routing — see docker-entrypoint.sh.

# Runtime base: the matching doublezero environment image (testnet / mainnet-beta /
# devnet). Declared in global scope so the runtime FROM below can use it; CI pins it
# to an immutable digest (...@sha256:...) per environment. The default mainnet-beta is
# what the public :latest resolves to today, so a plain `docker build .` is unchanged.
ARG DZ_BASE_IMAGE=ghcr.io/malbeclabs/doublezero:mainnet-beta

ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Cache the cargo registry and the target dir across builds (BuildKit cache mounts) for fast
# rebuilds. The target dir lives in the cache mount (not in the image layer), so the binary
# must be copied out within this same RUN before the mount goes away.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp /src/target/release/doublezero-edge-connect /usr/local/bin/doublezero-edge-connect

FROM ${DZ_BASE_IMAGE}

# Re-declare build args consumed below (Dockerfile ARG scope resets at each FROM).
# DZ_BASE_DIGEST is the staleness hook: rebuild-on-base.yml reads it back off the
# published image and compares it to the current base digest.
ARG DZ_BASE_IMAGE
ARG DZ_BASE_DIGEST=
ARG BUILD_VERSION=dev
ARG BUILD_COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/malbeclabs/doublezero-edge-connect" \
      org.opencontainers.image.title="doublezero-edge-connect" \
      org.opencontainers.image.description="DoubleZero Edge multicast Top-of-Book -> normalized WebSocket bridge" \
      org.opencontainers.image.base.name="${DZ_BASE_IMAGE}" \
      org.opencontainers.image.base.digest="${DZ_BASE_DIGEST}" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${BUILD_COMMIT}"
# iproute2 provides `ip`, which the receiver uses to resolve an interface NAME
# (e.g. doublezero1) to its IPv4 for join_multicast_v4. Without it, `--iface <name>`
# silently falls back to 0.0.0.0 (resolve_interface_ip in src/receiver.rs); passing an IPv4
# literal to --iface would work without it, but the default is a name, so we install it.
# No TLS at the bridge itself — terminate at a reverse proxy if you must expose it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Runs as root (the image's default user): the base entrypoint starts the
# `doublezerod` daemon, which manages the DoubleZero tunnel/routing and needs
# root + NET_ADMIN. The bridge itself needs no privileges (joining a multicast
# group and binding the WS port work unprivileged, and SO_RCVBUF up to the host's
# net.core.rmem_max works for any user — raise the ceiling on the HOST:
# `sudo sysctl -w net.core.rmem_max=268435456`), but it shares the entrypoint with
# the daemon, so we don't drop privileges here.

COPY --from=build /usr/local/bin/doublezero-edge-connect /usr/local/bin/doublezero-edge-connect

# Our entrypoint runs the base image's entrypoint first (it brings up doublezerod),
# then execs the bridge. See docker-entrypoint.sh for the handoff.
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/bridge-entrypoint.sh

# Every CLI flag also reads from an env var (DZ_FEEDS, DZ_IFACE, DZ_RECV_BUF, WS_BIND, WS_*,
# DZ_RECORD — see `Args` in src/main.rs), so downstream projects override behaviour with
# `-e VAR=...` / compose `environment:` without rebuilding (e.g. `-e DZ_FEEDS=Hyperliquid`).
# We only set RUST_LOG here; the binary's own clap defaults remain the source of truth for
# the rest (avoids this file drifting from the code).
ENV RUST_LOG=info

# Documentational: host networking ignores published ports, but this records the WS port (8081).
EXPOSE 8081

ENTRYPOINT ["bridge-entrypoint.sh"]
