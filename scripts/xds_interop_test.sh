#!/usr/bin/env bash
# xDS interop: go-control-plane ADS server -> vane xds-client -> vane
# admin plane -> live route assertion. Requires: cargo build -p
# vane-proxy, the built ads fixture (interop/ads/ads-server), curl.
set -uo pipefail
cd "$(dirname "$0")/.."
export TMPDIR="${TMPDIR:-/tmp}"

BIN=./target/debug/vane
[ -x "$BIN" ] || cargo build -p vane-proxy

python3 - <<PY &
import socket, threading, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 18081)); s.listen(64)
def handle(c):
    try:
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\nconnection: keep-alive\r\n\r\nenvoy-ads")
    finally: c.close()
def serve():
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()
serve(); time.sleep(120)
PY
UP_PID=$!

./interop/ads/ads-server -port 18000 -node vane-envoy-e2e -upstream 127.0.0.1:18081 &
ADS_PID=$!
sleep 0.5

TOML=$(mktemp --suffix=.toml)
cat > "$TOML" <<TOML
[[listeners]]
address = "127.0.0.1:18080"

[clusters.placeholder]
backends = ["127.0.0.1:1"]

[[routes]]
pattern = "/*rest"
cluster = "placeholder"

[admin]
enabled = true
address = "127.0.0.1:9901"

[runtime]
force_mio = true
workers = 1
TOML
"$BIN" run -c "$TOML" &
VANE_PID=$!
sleep 1

"$BIN" xds-client --management 127.0.0.1:18000 --node-id vane-envoy-e2e --admin http://127.0.0.1:9901 &
XDS_PID=$!

RESULT=FAIL
for i in $(seq 1 30); do
  body=$(curl -s -m 3 -H "Host: shop.example.com" http://127.0.0.1:18080/api/x || true)
  if [ "$body" = "envoy-ads" ]; then RESULT=PASS; break; fi
  sleep 1
done
echo "xDS interop: $RESULT (after ${i}s)"

kill $XDS_PID $VANE_PID $ADS_PID $UP_PID 2>/dev/null
wait 2>/dev/null
[ "$RESULT" = "PASS" ]
