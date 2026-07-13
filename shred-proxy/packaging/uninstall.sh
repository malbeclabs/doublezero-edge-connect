#!/usr/bin/env bash
#
# Remove the shred-proxy systemd service installed by install.sh.
#
#   sudo ./packaging/uninstall.sh
#
# Stops and disables the service, then removes the binary, unit and sysctl drop-in. Your config at
# /etc/default/shred-proxy is left in place (remove it by hand if you really want it gone).
set -euo pipefail

BIN_NAME=shred-proxy
PREFIX=/usr/local/bin
UNIT_DST="/etc/systemd/system/${BIN_NAME}.service"
ENV_DST="/etc/default/${BIN_NAME}"
SYSCTL_DST="/etc/sysctl.d/60-${BIN_NAME}.conf"

if [[ ${EUID} -ne 0 ]]; then
  echo "error: must run as root — re-run with: sudo $0" >&2
  exit 1
fi

echo "==> stopping and disabling service"
systemctl disable --now "${BIN_NAME}" 2>/dev/null || true

echo "==> removing ${UNIT_DST}"
rm -f "${UNIT_DST}"

echo "==> removing ${PREFIX}/${BIN_NAME}"
rm -f "${PREFIX}/${BIN_NAME}"

echo "==> removing ${SYSCTL_DST}"
rm -f "${SYSCTL_DST}"

echo "==> reloading systemd"
systemctl daemon-reload
systemctl reset-failed "${BIN_NAME}" 2>/dev/null || true

if [[ -e "${ENV_DST}" ]]; then
  echo "==> left your config in place: ${ENV_DST} (remove manually if desired)"
fi

echo "shred-proxy uninstalled."
