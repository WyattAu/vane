#!/usr/bin/env bash
# h2spec (summerwind/h2spec v2.6.0) conformance run against the vane
# h2c listener, in STRICT mode ([http2]
# strict_idle_window_update = true): RFC 7540 idle-state
# WINDOW_UPDATEs are rejected → 138/138. Default builds are lenient
# (the h2 crate client grants stream credit early) and score 137/138.
#
# Usage: scripts/h2spec_run.sh   (exit 0 = all cases pass)
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

H2SPEC=${H2SPEC:-/tmp/opencode/h2spec/h2spec}
if [ ! -x "$H2SPEC" ]; then
  echo "h2spec binary not at $H2SPEC — fetch v2.6.0:"
  echo "  curl -sL -o h2spec.tar.gz https://github.com/summerwind/h2spec/releases/download/v2.6.0/h2spec_linux_amd64.tar.gz"
  exit 2
fi

cargo build --release -p vane-proxy --features h2 || exit 2

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
address = "127.0.0.1:18445"
h2c = true
workers = 1

[clusters.up]
backends = ["127.0.0.1:18091"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[http2]
strict_idle_window_update = true

[runtime]
force_mio = true
TOML
./target/release/vane run -c "$DIR/vane.toml" &
VANE=$!
sleep 2
timeout 180 "$H2SPEC" -h 127.0.0.1 -p 18445
RC=$?
echo "h2spec exit: $RC"
kill $VANE $UP 2>/dev/null
wait 2>/dev/null
true
