#!/usr/bin/env bash
#
# Install shred-proxy as a systemd service on Ubuntu/Debian by downloading a prebuilt release
# binary from GitHub Releases. This is the script served at https://get.doublezero.xyz/shred-proxy,
# so the one-liner is:
#
#   curl -fsSL https://get.doublezero.xyz/shred-proxy | bash
#
# The privileged steps self-elevate with sudo (matching scripts/connect.sh), so a plain `| bash`
# works for a non-root user with sudo; run it as root and sudo is skipped. Idempotent: safe to
# re-run to upgrade the binary/unit (a re-run restarts the running service onto the new binary). It
# never overwrites your existing /etc/default/shred-proxy config. Unlike a from-source build, it
# downloads a pinned, checksummed binary. By default it also enables + starts the service (override
# with SHRED_PROXY_NO_START=1).
#
# Configuration variables must be set ON THE `bash` INVOCATION (after the pipe), not before `curl` —
# a `VAR=… curl … | bash` prefix scopes VAR to curl, so the piped script never sees it. Correct form:
#   curl … | DZ_FORWARD=… SHRED_PROXY_VERSION=… bash
#
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

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "error: this installer supports Linux/x86_64 only (got $(uname -s)/$(uname -m))." >&2
  echo "       Build from source instead: cargo build --release -p shred-proxy" >&2
  exit 1
fi

# Root / sudo: run as the invoking user (so `curl | bash` needs no sudo prefix) and self-elevate
# only the privileged steps via ${SUDO}. Matches scripts/connect.sh's model.
SUDO=""
if [[ ${EUID} -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || {
    echo "error: need root to install (binary, /etc, systemd) but sudo is not installed." >&2
    echo "       Re-run as root: curl … | sudo bash" >&2
    exit 1
  }
  SUDO="sudo"
fi

for tool in curl install systemctl sysctl ip; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' not found in PATH" >&2; exit 1; }
done

# Prime sudo once up front so the privileged steps below don't re-prompt mid-run (and only prompt if
# a password is actually required — `sudo -n true` succeeds silently under NOPASSWD or a cached
# timestamp).
if [[ -n "${SUDO}" ]] && ! ${SUDO} -n true 2>/dev/null; then
  echo "==> some steps need root; you may be prompted for your password once"
  ${SUDO} -v || { echo "error: could not obtain sudo. Re-run as root." >&2; exit 1; }
fi

# --- Resolve the download URL -------------------------------------------------------------------

if [[ "${VERSION}" == "latest" ]]; then
  BASE="https://github.com/${REPO}/releases/latest/download"
else
  BASE="https://github.com/${REPO}/releases/download/${VERSION}"
fi

# Resolve the concrete release tag so the packaging files (unit/env/sysctl fetched by the curl|bash
# path below) come from the same immutable tag as the binary, rather than a moving `main`. GitHub
# redirects the releases/latest URL to the tagged release; read the tag out of the redirect target.
RESOLVED_TAG="${VERSION}"
if [[ "${VERSION}" == "latest" ]]; then
  latest_url="$(curl -fsSL --proto '=https' --tlsv1.2 -o /dev/null -w '%{url_effective}' \
                "https://github.com/${REPO}/releases/latest" 2>/dev/null || true)"
  case "${latest_url}" in
    */tag/*) RESOLVED_TAG="${latest_url##*/tag/}" ;;
    *)
      echo "   (warning: could not resolve the latest release tag; packaging files will track 'main')"
      RESOLVED_TAG="main"
      ;;
  esac
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

# When run via `curl | bash` there is no script file on disk, so BASH_SOURCE[0] is unset — and
# under `set -u` referencing it would abort. Fall back so SCRIPT_DIR is simply empty in that case;
# fetch_asset then pulls the packaging files from the repo instead of looking next to the script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-/nonexistent}")" 2>/dev/null && pwd || true)"

echo "==> installing binary            -> ${PREFIX}/${BIN_NAME}"
${SUDO} install -D -m 0755 "${TMP}/${BIN_NAME}" "${PREFIX}/${BIN_NAME}"

# When run via the curl|bash one-liner there is no checked-out repo, so the unit/sysctl/env files
# aren't on disk next to this script. Fetch them from the release's source tree in that case.
fetch_asset() {
  # $1 = repo-relative path, $2 = local destination
  if [[ -f "${SCRIPT_DIR}/$(basename "$1")" ]]; then
    cp "${SCRIPT_DIR}/$(basename "$1")" "$2"
  else
    # Pinned to the resolved release tag (falls back to `main` only if resolution failed above).
    curl -fsSL --proto '=https' --tlsv1.2 \
      -o "$2" "https://raw.githubusercontent.com/${REPO}/${RESOLVED_TAG}/$1"
  fi
}

echo "==> installing systemd unit      -> ${UNIT_DST}"
fetch_asset "shred-proxy/packaging/${BIN_NAME}.service" "${TMP}/${BIN_NAME}.service"
${SUDO} install -D -m 0644 "${TMP}/${BIN_NAME}.service" "${UNIT_DST}"

if [[ -e "${ENV_DST}" ]]; then
  echo "==> keeping existing config      -> ${ENV_DST} (left untouched)"
else
  echo "==> installing default config    -> ${ENV_DST}"
  fetch_asset "shred-proxy/packaging/${BIN_NAME}.env.example" "${TMP}/${BIN_NAME}.env"
  # Persist any DZ_* overrides passed to the one-liner into the staged file first (as the invoking
  # user, in TMP), then install it in one privileged step — so the append works without root on the
  # final path and the config lands atomically.
  while IFS='=' read -r name value; do
    [[ "${name}" == DZ_* ]] || continue
    printf '%s=%s\n' "${name}" "${value}" >> "${TMP}/${BIN_NAME}.env"
    echo "   (recorded ${name} from environment)"
  done < <(env)
  ${SUDO} install -D -m 0644 "${TMP}/${BIN_NAME}.env" "${ENV_DST}"
fi

echo "==> installing kernel tuning     -> ${SYSCTL_DST}"
fetch_asset "shred-proxy/packaging/60-${BIN_NAME}.conf" "${TMP}/60-${BIN_NAME}.conf"
${SUDO} install -D -m 0644 "${TMP}/60-${BIN_NAME}.conf" "${SYSCTL_DST}"
${SUDO} sysctl --quiet --load="${SYSCTL_DST}" || echo "   (warning: could not apply sysctl now; it will apply on next boot)"

echo "==> reloading systemd"
${SUDO} systemctl daemon-reload

if [[ "${SHRED_PROXY_NO_START:-0}" == "1" ]]; then
  cat <<EOF

shred-proxy installed (not started, SHRED_PROXY_NO_START=1). Next steps:

  1. Review the config:   sudo nano ${ENV_DST}
  2. Enable at boot + start now:
                          sudo systemctl enable --now ${BIN_NAME}
  3. Follow the logs:     journalctl -u ${BIN_NAME} -f
EOF
else
  echo "==> enabling and (re)starting service"
  # `enable --now` only *starts* the unit — on a re-run/upgrade where the service is already active
  # that is a no-op, so the freshly installed binary would not take effect until a manual restart.
  # Enable for boot, then `restart` (which starts it if stopped, restarts it if running) so an
  # upgrade actually swaps to the new binary.
  ${SUDO} systemctl enable "${BIN_NAME}"
  ${SUDO} systemctl restart "${BIN_NAME}"
  cat <<EOF

shred-proxy installed and started. Useful commands:

  Follow the logs:  journalctl -u ${BIN_NAME} -f
  Check status:     systemctl status ${BIN_NAME}
  Edit config:      sudo nano ${ENV_DST} && sudo systemctl restart ${BIN_NAME}
  Uninstall:        sudo ${SCRIPT_DIR}/uninstall.sh   (or fetch it from the repo)
EOF
fi
