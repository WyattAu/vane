#!/usr/bin/env bash
# vane load benchmark — honest throughput/latency numbers via ApacheBench.
#
# Usage: scripts/bench.sh [duration_seconds]
# Requires: cargo (release build), ab (apache2-utils), python3.
#
# Topology: tiny Python upstream (single-threaded on purpose: measures the
# PROXY, not the backend) -> vane (release, 1 worker) -> ab clients.
set -euo pipefail

DURATION="${1:-10}"
PORT_UP=19999
PORT_PROXY=18080

echo "== building (release) =="
cargo build --release -p vane

echo "== upstream on :$PORT_UP =="
python3 - <<PY &
import socket, threading, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", $PORT_UP))
s.listen(1024)
def serve():
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()
def handle(c):
    try:
        c.recv(4096)
        body = b"hello-vane"
        c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n" + body)
    finally:
        c.close()
serve()
time.sleep($DURATION + 30)
PY
UP_PID=$!
sleep 0.5

cat > /tmp/vane-bench.toml <<TOML
[[listeners]]
address = "127.0.0.1:$PORT_PROXY"
workers = 1

[clusters.bench]
backends = ["127.0.0.1:$PORT_UP"]

[[routes]]
pattern = "/*rest"
cluster = "bench"

[admin]
enabled = false
TOML

echo "== vane (release, 1 worker) on :$PORT_PROXY =="
./target/release/vane run -c /tmp/vane-bench.toml &
VANE_PID=$!
sleep 1

echo "== ab: $DURATION s, 64 concurrent, keep-alive =="
# Fixed request count (ab -t counts deadline-cut responses as failures).
TOTAL=$((30000))
timeout "$DURATION" ab -n "$TOTAL" -c 64 -k "http://127.0.0.1:$PORT_PROXY/" 2>&1 |
  grep -E "Requests per second|Time per request|Failed requests|Complete requests|Non-2xx|HTML transferred" || true

kill "$VANE_PID" "$UP_PID" 2>/dev/null || true
wait 2>/dev/null || true
echo "== done =="
