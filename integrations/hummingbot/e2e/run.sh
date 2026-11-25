#!/usr/bin/env bash
# Runs the E2E against one Hummingbot image: e2e/run.sh hummingbot/hummingbot:version-2.18.0
set -euo pipefail

image="$1"
here="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
stub="127.0.0.1:18081"
cp -r "$here/e2e/conf" "$work/conf"
mkdir -p "$work/data" "$work/logs" "$work/conf/connectors" "$work/conf/controllers" "$work/conf/strategies"

finish() {
  status=$?
  if [ "$status" -ne 0 ]; then
    cat "$work/check.txt" 2>/dev/null || true
    docker logs dz-e2e-stub 2>&1 | tail -20 || true
    tail -n 80 "$work"/logs/*.log 2>/dev/null || docker logs dz-e2e-bot 2>&1 | tail -80 || true
  fi
  docker rm -f dz-e2e-stub dz-e2e-bot >/dev/null 2>&1 || true
}
trap finish EXIT

docker run -d --name dz-e2e-stub --network host -v "$here/e2e:/e2e:ro" "$image" \
  python /e2e/stub.py --port "${stub##*:}"
docker run -d --name dz-e2e-bot --network host -e HBOT_PASSWORD=ci -e DZ_E2E_STUB="$stub" \
  -v "$work/conf:/home/hummingbot/conf" -v "$work/data:/home/hummingbot/data" -v "$work/logs:/home/hummingbot/logs" \
  -v "$here/dz_hummingbot:/home/hummingbot/scripts/dz_hummingbot:ro" \
  -v "$here/e2e/dz_e2e.py:/home/hummingbot/scripts/dz_e2e.py:ro" \
  "$image" hbot start dz_e2e.yml --foreground

deadline=$((SECONDS + 240))
until [ -f "$work/data/dz_e2e_evidence.json" ] \
  && curl -fsS "http://$stub/report" > "$work/report.json" \
  && python3 "$here/e2e/check.py" "$work/data/dz_e2e_evidence.json" "$work/report.json" > "$work/check.txt"; do
  if [ "$SECONDS" -gt "$deadline" ]; then
    echo "E2E failed against $image"
    exit 1
  fi
  sleep 5
done
echo "E2E passed against $image"
