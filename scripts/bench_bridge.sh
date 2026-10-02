#!/usr/bin/env bash
# h3 bridge benchmark — what mesh-over-QUIC costs vs the plain-h1 path
# (release mode).
#
# Topology: tiny threaded Python upstream <- far vane (h3 edge on QUIC
# + plain upstream dial) <- near vane (two routes: an h1 cluster as
# the baseline, and an http3 cluster that crosses the in-process
# bridge). The loadgen drives both near routes; the only difference is
# the cluster the route selects — everything else is identical, so the
# delta is the bridge + QUIC cost.
#
# Usage: scripts/bench_bridge.sh [seconds_per_leg]
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-8}"
LG=/tmp/opencode/loadgen/target/release
if [ ! -x "$LG/loadgen" ] || [ ! -x "$LG/certgen" ]; then
  echo "== building bench tools (loadgen + certgen) =="
  mkdir -p /tmp/opencode/loadgen/src/bin
  [ -f /tmp/opencode/loadgen/Cargo.toml ] || { echo "loadgen crate missing at /tmp/opencode/loadgen"; exit 2; }
  (cd /tmp/opencode/loadgen && cargo build --release) || exit 2
fi

echo "== building vane (release) =="
cargo build --release -p vane-proxy --features h3 || exit 2

FREE=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
PORT_NEAR=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
PORT_FAR=18498

run() { # name host
  echo "== $1 =="
  "$LG/loadgen" "127.0.0.1:$PORT_NEAR" 8 "$DURATION" /bench "$2"
}

# ---- shared upstream ----
python3 - <<PY &
import socket, threading
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", $FREE)); s.listen(1024)
def handle(c):
    try:
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane")
    finally: c.close()
while True:
    c, _ = s.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY
UP=$!
sleep 0.3

DIR=$(mktemp -d)
"$LG/certgen" "$DIR"

# ---- far instance: vane h3 edge (QUIC $PORT_FAR) -> upstream ----
cat > "$DIR/far.toml" <<TOML
[[listeners]]
address = "127.0.0.1:$PORT_FAR"

[listeners.tls]
cert = "$DIR/cert.pem"
key = "$DIR/key.pem"
h3 = true

[clusters.up]
backends = ["127.0.0.1:$FREE"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
TOML
./target/release/vane run -c "$DIR/far.toml" &
FAR=$!

# ---- near instance: h1 cluster (baseline) + http3 cluster (measured) ----
cat > "$DIR/near.toml" <<TOML
[[listeners]]
address = "127.0.0.1:$PORT_NEAR"

[clusters.h1]
backends = ["127.0.0.1:$FREE"]

[clusters.quic]
backends = ["127.0.0.1:$PORT_FAR"]
http3 = true

[clusters.quic.h3_tls]
ca = "$DIR/cert.pem"
server_name = "localhost"

[[routes]]
host = "h1"
pattern = "/*rest"
cluster = "h1"

[[routes]]
host = "quic"
pattern = "/*rest"
cluster = "quic"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
TOML
./target/release/vane run -c "$DIR/near.toml" &
NEAR=$!
sleep 2

echo "== h1 cluster (baseline): 8 conns, ${DURATION}s =="
run "h1" "h1"
echo "== http3 cluster (bridge -> QUIC -> edge -> h1): 8 conns, ${DURATION}s =="
run "quic" "quic"

kill $NEAR $FAR $UP 2>/dev/null
wait 2>/dev/null
true
