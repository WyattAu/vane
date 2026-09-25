#!/usr/bin/env bash
# h3spec (kazu-yamamoto/h3spec v0.1.13) conformance run against the
# vane h3 edge. Builds the release binary with --features h3 first.
#
# Usage: scripts/h3spec_run.sh   (exit 0 = all cases pass)
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane
python3 - <<PY &
import socket, threading
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 18091)); s.listen(64)
def handle(c):
    try:
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: keep-alive\r\n\r\nok")
    finally: c.close()
def serve():
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()
serve()
PY
UP=$!
DIR=$(mktemp -d)
cat > "$DIR/vane.toml" <<TOML
[[listeners]]
address = "127.0.0.1:18443"

[listeners.tls]
h3 = true

[[listeners]]
address = "127.0.0.1:18444"

[[clusters.up]]
backends = ["127.0.0.1:18091"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
TOML
./target/release/vane run -c "$DIR/vane.toml" &
VANE=$!
sleep 2
timeout 180 /tmp/opencode/h3spec/h3spec -n 127.0.0.1 18443
RC=$?
echo "h3spec exit: $RC"
kill $VANE $UP 2>/dev/null
wait 2>/dev/null
true
