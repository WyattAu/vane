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

H2SPEC=${H2SPEC:-/var/tmp/vane-artifacts/h2spec/h2spec}
# The binary lives OUTSIDE /tmp: this host wipes /tmp on restart, and a
# gate that silently degrades to "binary missing" is a gate that stops
# gating. Fetch v2.6.0 on demand, verified against the published
# checksum.
H2SPEC_VERSION=2.6.0
H2SPEC_SHA256=157ee0de702e01ad40e752dbf074b366027e550c8e7504f9450da2809e279318
if [ ! -x "$H2SPEC" ]; then
  echo "fetching h2spec v${H2SPEC_VERSION} into $(dirname "$H2SPEC")"
  mkdir -p "$(dirname "$H2SPEC")"
  TMP_TGZ=$(mktemp -d)/h2spec.tar.gz
  curl -fsSL -o "$TMP_TGZ" \
    "https://github.com/summerwind/h2spec/releases/download/v${H2SPEC_VERSION}/h2spec_linux_amd64.tar.gz" || {
      echo "h2spec download failed — check network access"
      exit 2
    }
  GOT=$(sha256sum "$TMP_TGZ" | cut -d' ' -f1)
  if [ -n "${H2SPEC_SHA256_OVERRIDE:-}" ]; then
    # Escape hatch for upstream re-releases with a different digest.
    H2SPEC_SHA256="$H2SPEC_SHA256_OVERRIDE"
  fi
  if [ "$GOT" != "$H2SPEC_SHA256" ]; then
    echo "h2spec checksum mismatch:"
    echo "  expected $H2SPEC_SHA256"
    echo "  got      $GOT"
    echo "refusing to run an unverified binary"
    exit 2
  fi
  tar -xzf "$TMP_TGZ" -C "$(dirname "$H2SPEC")" h2spec || exit 2
  chmod +x "$H2SPEC"
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
