#!/usr/bin/env bash
# Hot-upgrade demo: tight client loop while vane-a hands the listener to
# vane-b. Fails the script if a single request fails or the run aborts.
#
# Usage: ./deploy/demo-hot-upgrade.sh
set -euo pipefail
cd "$(dirname "$0")/.."

IMG="${VANE_IMAGE:-ghcr.io/wyattau/vane:0.2.0}"
export VANE_IMAGE="$IMG"

cleanup() { docker compose -f docker-compose.hot-upgrade.yml down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker compose -f docker-compose.hot-upgrade.yml up -d --pull never

# Wait for the data plane.
for _ in $(seq 1 30); do
    curl -sf --max-time 2 http://127.0.0.1:8080/ >/dev/null 2>&1 && break
    sleep 1
done
curl -sf --max-time 5 http://127.0.0.1:8080/ >/dev/null

# Background prober: 40 rps, stop when told.
STOP=/tmp/opencode/vane-demo-stop
rm -f "$STOP"
fails=0
(
    while [ ! -f "$STOP" ]; do
        curl -sf --max-time 2 http://127.0.0.1:8080/ >/dev/null 2>&1 || fails=$((fails+1))
        sleep 0.025
    done
    echo "$fails" > /tmp/opencode/vane-demo-fails
) &
prober=$!

# Let the prober warm up, then cycle the generation.
sleep 3
docker compose -f docker-compose.hot-upgrade.yml up -d --no-deps vane-b >/dev/null 2>&1 || true
sleep 2
docker compose -f docker-compose.hot-upgrade.yml stop vane-a >/dev/null 2>&1 || true
sleep 3
touch "$STOP"
wait "$prober"

failed=$(cat /tmp/opencode/vane-demo-fails)
echo "handover complete: failed_requests=$failed"
test "$failed" -eq 0
