#!/usr/bin/env bash
# Debug bench: pool lifecycle trace under load.
set -u
PORT_UP=19999
PORT_PROXY=18080
LOG=/tmp/opencode/vb-dbg.log
AB=/tmp/opencode/ab-dbg.log

cleanup() {
  [ -n "${V:-}" ] && kill -9 "$V" 2>/dev/null
  [ -n "${UP:-}" ] && kill -9 "$UP" 2>/dev/null
}
trap cleanup EXIT

for port in "$PORT_UP" "$PORT_PROXY"; do
  fuser -k "${port}/tcp" 2>/dev/null
done
sleep 0.5

python3 - <<PY &
import socket, threading, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", $PORT_UP)); s.listen(1024)
def handle(c):
    try:
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane")
    finally:
        c.close()
def serve():
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()
serve()
time.sleep(30)
PY
UP=$!

: > "$LOG"
./target/debug/vane run -c /tmp/vane-bench.toml >> "$LOG" 2>&1 &
V=$!

sleep 2
ab -n 300 -c 8 -k "http://127.0.0.1:$PORT_PROXY/" > "$AB" 2>&1
sleep 0.5

echo "== ab =="
grep -E "Failed requests|Non-2xx|Complete requests" "$AB"
echo "== pool trace (first 20) =="
grep -E "\[pool\]" "$LOG" | head -20
echo "== errors =="
grep -E "upstream error|Bad file" "$LOG" | head -6
exit 0
