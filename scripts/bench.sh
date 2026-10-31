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
# Random free ports per run: fixed ports invite stale proxies (SO_REUSEPORT
# silently load-balances into them).
PORT_UP=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
PORT_PROXY=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")

echo "== building (release) =="
cargo build --release -p vane-proxy

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
    # Keep-alive loop: same connection serves many requests, so ab measures
    # the PROXY (connection setup + routing + relay), not thread spawning.
    try:
        while True:
            d = c.recv(4096)
            if not d:
                break
            c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane")
    finally:
        c.close()
serve()
time.sleep($DURATION + 30)
PY
UP_PID=$!
sleep 0.5

WORKERS="${WORKERS:-1}"
cat > /tmp/vane-bench.toml <<TOML
[[listeners]]
address = "127.0.0.1:$PORT_PROXY"
workers = $WORKERS

[clusters.bench]
backends = ["127.0.0.1:$PORT_UP"]

[[routes]]
pattern = "/*rest"
cluster = "bench"

[admin]
enabled = false
TOML

echo "== vane (release, $WORKERS worker(s)) on :$PORT_PROXY =="
./target/release/vane run -c /tmp/vane-bench.toml &
VANE_PID=$!
sleep 1

echo "== loadgen: $DURATION s, 64 concurrent, keep-alive =="
# Duration-based (ab's fixed-count mode counts deadline-cut responses as
# failures). Fields: proto conns duration total rps ok conn_err read_err
# non200 0 p50_us 0 p99_us — rps and the percentiles are what gets quoted.
LG="$PWD/tools/loadgen/target/release"
if [ ! -x "$LG/loadgen" ]; then (cd tools/loadgen && cargo build --release) || exit 2; fi
"$LG/loadgen" "127.0.0.1:$PORT_PROXY" 64 "$DURATION" / bench || true

kill "$VANE_PID" "$UP_PID" 2>/dev/null || true
wait 2>/dev/null || true
echo "== done =="
