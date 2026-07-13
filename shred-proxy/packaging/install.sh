#!/usr/bin/env bash
#
# Install shred-proxy as a systemd service on Ubuntu/Debian by downloading a prebuilt release
# binary from GitHub Releases. This is the script served at https://get.doublezero.xyz/shred-proxy,
# so the one-liner is:
#
#   curl -fsSL https://get.doublezero.xyz/shred-proxy | bash
#
# Idempotent: safe to re-run to upgrade the binary/unit. It never overwrites your existing
# /etc/default/shred-proxy config. Unlike a from-source build, it downloads a pinned, checksummed
# binary. By default it also enables + starts the service (override with SHRED_PROXY_NO_START=1).
#
# Configuration (environment variables set before the pipe):
#   SHRED_PROXY_VERSION   release tag to install (default: latest)
#   SHRED_PROXY_REPO      GitHub owner/repo to fetch from (default: malbeclabs/doublezero-edge-connect)
#   SHRED_PROXY_NO_START  set to 1 to install without enabling/starting the service
#   DZ_*                  any DZ_* var is written into /etc/default/shred-proxy on a fresh install
set -euo pipefail

BIN_NAME=shred-proxy
PREFIX=/usr/local/bin
UNIT_DST="/etc/systemd/system/${BIN_NAME}.service"
ENV_DST="/etc/default/${BIN_NAME}"
SYSCTL_DST="/etc/sysctl.d/60-${BIN_NAME}.conf"

VERSION="${SHRED_PROXY_VERSION:-latest}"
REPO="${SHRED_PROXY_REPO:-malbeclabs/doublezero-edge-connect}"

# The release publishes one static linux/amd64 binary. The release tag is namespaced
# (`shred-proxy-vX.Y.Z`) so it does not collide with the bridge's Docker release tags.
ASSET="${BIN_NAME}-x86_64-unknown-linux-musl"

# --- Preconditions ------------------------------------------------------------------------------

if [[ ${EUID} -ne 0 ]]; then
  echo "error: must run as root — re-run with: sudo bash, or pipe to 'sudo bash'" >&2
  exit 1
fi

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "error: this installer supports Linux/x86_64 only (got $(uname -s)/$(uname -m))." >&2
  echo "       Build from source instead: cargo build --release -p shred-proxy" >&2
  exit 1
fi

for tool in curl install systemctl sysctl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' not found in PATH" >&2; exit 1; }
done

# --- Resolve the download URL -------------------------------------------------------------------

if [[ "${VERSION}" == "latest" ]]; then
  BASE="https://github.com/${REPO}/releases/latest/download"
else
  BASE="https://github.com/${REPO}/releases/download/${VERSION}"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

echo "==> downloading ${ASSET} (${VERSION}) from ${REPO}"
curl -fSL --proto '=https' --tlsv1.2 -o "${TMP}/${BIN_NAME}" "${BASE}/${ASSET}"

# Verify the checksum against the published SHA256SUMS if it is available (it always is for our
# releases; tolerate its absence so a manual/mirror install still works).
if curl -fsSL --proto '=https' --tlsv1.2 -o "${TMP}/SHA256SUMS" "${BASE}/SHA256SUMS"; then
  echo "==> verifying checksum"
  expected="$(awk -v a="${ASSET}" '$2 == a || $2 == "*"a {print $1}' "${TMP}/SHA256SUMS" | head -n1)"
  if [[ -z "${expected}" ]]; then
    echo "error: ${ASSET} not listed in SHA256SUMS" >&2
    exit 1
  fi
  actual="$(sha256sum "${TMP}/${BIN_NAME}" | awk '{print $1}')"
  if [[ "${expected}" != "${actual}" ]]; then
    echo "error: checksum mismatch for ${ASSET}" >&2
    echo "       expected ${expected}" >&2
    echo "       actual   ${actual}" >&2
    exit 1
  fi
else
  echo "   (warning: SHA256SUMS not found; skipping checksum verification)"
fi

chmod 0755 "${TMP}/${BIN_NAME}"

# --- Install ------------------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> installing binary            -> ${PREFIX}/${BIN_NAME}"
install -D -m 0755 "${TMP}/${BIN_NAME}" "${PREFIX}/${BIN_NAME}"

# When run via the curl|bash one-liner there is no checked-out repo, so the unit/sysctl/env files
# aren't on disk next to this script. Fetch them from the release's source tree in that case.
fetch_asset() {
  # $1 = repo-relative path, $2 = local destination
  if [[ -f "${SCRIPT_DIR}/$(basename "$1")" ]]; then
    cp "${SCRIPT_DIR}/$(basename "$1")" "$2"
  else
    local ref="${VERSION}"
    [[ "${ref}" == "latest" ]] && ref="main"
    curl -fsSL --proto '=https' --tlsv1.2 \
      -o "$2" "https://raw.githubusercontent.com/${REPO}/${ref}/$1"
  fi
}

echo "==> installing systemd unit      -> ${UNIT_DST}"
fetch_asset "shred-proxy/packaging/${BIN_NAME}.service" "${TMP}/${BIN_NAME}.service"
install -D -m 0644 "${TMP}/${BIN_NAME}.service" "${UNIT_DST}"

if [[ -e "${ENV_DST}" ]]; then
  echo "==> keeping existing config      -> ${ENV_DST} (left untouched)"
else
  echo "==> installing default config    -> ${ENV_DST}"
  fetch_asset "shred-proxy/packaging/${BIN_NAME}.env.example" "${TMP}/${BIN_NAME}.env"
  install -D -m 0644 "${TMP}/${BIN_NAME}.env" "${ENV_DST}"
  # Persist any DZ_* overrides passed to the one-liner so the service picks them up.
  while IFS='=' read -r name value; do
    [[ "${name}" == DZ_* ]] || continue
    echo "${name}=${value}" >> "${ENV_DST}"
    echo "   (recorded ${name} from environment)"
  done < <(env)
fi

echo "==> installing kernel tuning     -> ${SYSCTL_DST}"
fetch_asset "shred-proxy/packaging/60-${BIN_NAME}.conf" "${TMP}/60-${BIN_NAME}.conf"
install -D -m 0644 "${TMP}/60-${BIN_NAME}.conf" "${SYSCTL_DST}"
sysctl --quiet --load="${SYSCTL_DST}" || echo "   (warning: could not apply sysctl now; it will apply on next boot)"

echo "==> reloading systemd"
systemctl daemon-reload

if [[ "${SHRED_PROXY_NO_START:-0}" == "1" ]]; then
  cat <<EOF

shred-proxy installed (not started, SHRED_PROXY_NO_START=1). Next steps:

  1. Review the config:   sudo nano ${ENV_DST}
  2. Enable at boot + start now:
                          sudo systemctl enable --now ${BIN_NAME}
  3. Follow the logs:     journalctl -u ${BIN_NAME} -f
EOF
else
  echo "==> enabling and starting service"
  systemctl enable --now "${BIN_NAME}"
  cat <<EOF

shred-proxy installed and started. Useful commands:

  Follow the logs:  journalctl -u ${BIN_NAME} -f
  Check status:     systemctl status ${BIN_NAME}
  Edit config:      sudo nano ${ENV_DST} && sudo systemctl restart ${BIN_NAME}
  Uninstall:        sudo ${SCRIPT_DIR}/uninstall.sh   (or fetch it from the repo)
EOF
fi
