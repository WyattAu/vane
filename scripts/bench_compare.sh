#!/usr/bin/env bash
# Comparative reverse-proxy benchmark: vane vs Caddy vs Traefik —
# same machine, same tiny threaded upstream, same client, same
# workload, ALL RUN NATIVELY. Honest methodology:
#
# - Two listeners per proxy: PLAIN (h1 leg) and TLS (h2 + h3 legs).
# - Reasonable production-shaped configs (all published in this
#   script); docker host networking.
# - The client is vane's loadgen: symmetric across proxies — the
#   deltas are the claim, not the absolute numbers.
# - Workload: tiny 10-byte body (measures proxy overhead), keep-alive.
# - RSS sampled from the host process table per proxy.
#
# Usage: scripts/bench_compare.sh [seconds_per_leg]
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-8}"
LG=/tmp/opencode/loadgen/target/release
[ -x "$LG/loadgen" ] && [ -x "$LG/h2load" ] && [ -x "$LG/h3load" ] && [ -x "$LG/certgen" ] || {
  echo "bench tools missing — build /tmp/opencode/loadgen first"; exit 2;
}

echo "== building vane (release) =="
cargo build --release -p vane-proxy --features h3 || exit 2

mkport() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"; }
FREE_UP=$(mkport)
for v in VANE_P1 VANE_P2 NGINX_P1 NGINX_P2 CADDY_P1 CADDY_P2 TRAEFIK_P1 TRAEFIK_P2; do
  declare "$v=$(mkport)"
done

# ---- shared upstream (identical for every proxy) ----
# The tokio upstream (loadgen crate) replaces the earlier threaded
# Python one, whose GIL capped the whole rig at ~56k req/s.
"$LG/upstream" "127.0.0.1:$FREE_UP" &
UP=$!
sleep 0.5

DIR=$(mktemp -d)
"$LG/certgen" "$DIR"

RESULTS=$'| proxy | leg | conns | req/s | p50 | p99 | non200 |\n|---|---|---|---|---|---|---|'
record() { # proxy leg conns "req/s: N total: M non200: B p50: X p99: Y"
  local reqs p50 p99 nb
  reqs=$(echo "$4" | sed -n 's/.*req\/s: \([0-9]*\).*/\1/p')
  nb=$(echo "$4" | sed -n 's/.*non200: \([0-9]*\).*/\1/p')
  p50=$(echo "$4" | sed -n 's/.*p50: \([0-9]*\).*/\1/p')
  p99=$(echo "$4" | sed -n 's/.*p99: \([0-9]*\).*/\1/p')
  RESULTS+="| $1 | $2 | $3 | ${reqs:-0} | ${p50:-0}µs | ${p99:-0}µs | ${nb:-?} |
"
}

leg() { # port proto conns [extra]
  case "$2" in
    h1)     "$LG/loadgen" "127.0.0.1:$1" "$3" "$DURATION" /bench bench 2>/dev/null ;;
    h1post) "$LG/loadgen" "127.0.0.1:$1" "$3" "$DURATION" /bench bench 4096 2>/dev/null ;;
    h1big)  "$LG/loadgen" "127.0.0.1:$1" "$3" "$DURATION" /big bench 2>/dev/null ;;
    h2)     "$LG/h2load" "127.0.0.1:$1" "$3" "$DURATION" /bench localhost "$DIR/cert.pem" 2>/dev/null ;;
    h3)     "$LG/h3load" "127.0.0.1:$1" "$3" "$DURATION" /bench localhost "$DIR/cert.pem" h3 2>/dev/null ;;
  esac
}

wait_up() { # port name
  for _ in $(seq 1 40); do
    curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$1/probe" && return 0
    curl -sk -o /dev/null --max-time 2 "https://127.0.0.1:$1/probe" && return 0
    sleep 0.5
  done
  echo "  (not measurable: $2 never answered on $1)"
  return 1
}

run_legs() { # name plain_port tls_port
  record "$1" "h1 8"      8  "$(leg "$2" h1 8)"
  record "$1" "h1 64"     64 "$(leg "$2" h1 64)"
  record "$1" "h2 8"      8  "$(leg "$3" h2 8)"
  record "$1" "h3 8"      8  "$(leg "$3" h3 8)"
  record "$1" "POST 4KB"  8  "$(leg "$2" h1post 8)"
  record "$1" "GET 64KB"  8  "$(leg "$2" h1big 8)"
}

# ================= vane =================
echo "== vane =="
# Fair config: workers = 0 → one worker per core (shared-nothing
# SO_REUSEPORT, matching Traefik's default all-core concurrency);
# io_uring backend (the compiled-in default — force_mio = false).
cat > "$DIR/vane.toml" <<TOML
[[listeners]]
address = "127.0.0.1:$VANE_P1"
workers = 0

[[listeners]]
address = "127.0.0.1:$VANE_P2"
workers = 0

[listeners.tls]
cert = "$DIR/cert.pem"
key = "$DIR/key.pem"
h3 = true
alpn_h2 = true

[clusters.up]
backends = ["127.0.0.1:$FREE_UP"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = false
TOML
./target/release/vane run -c "$DIR/vane.toml" & VANE_PID=$!
sleep 2
if wait_up "$VANE_P1" vane; then
  run_legs vane "$VANE_P1" "$VANE_P2"
  echo "  RSS: $(ps -o rss= -p "$VANE_PID" 2>/dev/null | tr -d ' ') KB"
fi
kill "$VANE_PID" 2>/dev/null; wait "$VANE_PID" 2>/dev/null

# ================= nginx (not measurable natively here) =================
echo "== nginx: skipped — no native install available; docker networking on
   this machine is rootless-style and invalidates the measurement
   (see the methodology note above). =="
PORT_NGINX_P1=""; PORT_NGINX_P2=""

# ================= Caddy =================
echo "== Caddy =="
cat > "$DIR/Caddyfile" <<EOF
{
  auto_https off
  admin off
}
:$CADDY_P1 {
  reverse_proxy 127.0.0.1:$FREE_UP
}
:$CADDY_P2 {
  tls $DIR/cert.pem $DIR/key.pem
  reverse_proxy 127.0.0.1:$FREE_UP
}
EOF
CADDY_BIN=/tmp/opencode/proxies/caddy
[ -x "$CADDY_BIN" ] || { echo "  (not measurable: caddy binary missing)"; CADDY_BIN=""; }
if [ -n "$CADDY_BIN" ]; then
"$CADDY_BIN" run --config "$DIR/Caddyfile" > "$DIR/caddy.log" 2>&1 &
CADDY_PID=$!
sleep 2
if wait_up "$CADDY_P1" caddy; then
  run_legs caddy "$CADDY_P1" "$CADDY_P2"
  echo "  RSS: $(rss=$(ps -o rss= -p "$CADDY_PID" 2>/dev/null | tr -d ' '); echo "${rss:-0} KB")"
fi
[ -n "${CADDY_PID:-}" ] && kill "$CADDY_PID" 2>/dev/null
wait "${CADDY_PID:-}" 2>/dev/null
fi

# ================= Traefik =================
echo "== Traefik =="
cat > "$DIR/traefik.yml" <<EOF
entryPoints:
  plain:
    address: ":$TRAEFIK_P1"
  tls:
    address: ":$TRAEFIK_P2"
    http3:
      advertisedPort: $TRAEFIK_P2
providers:
  file:
    directory: "$DIR/rules"
log:
  level: ERROR
EOF
mkdir -p "$DIR/rules"
# Quoted heredoc keeps the router-rule backticks; the cert dir is
# substituted explicitly (native run reads local paths).
cat > "$DIR/rules/dynamic.yml" <<'EOF'
tls:
  certificates:
    - certFile: "CERTDIR/cert.pem"
      keyFile: "CERTDIR/key.pem"
http:
  routers:
    plain:
      rule: "PathPrefix(`/`)"
      service: up
      entryPoints: ["plain"]
    secure:
      rule: "PathPrefix(`/`)"
      service: up
      entryPoints: ["tls"]
      tls: {}
  services:
    up:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:REPLACE_UP"
EOF
sed -i "s|REPLACE_UP|$FREE_UP|; s|CERTDIR|$DIR|g" "$DIR/rules/dynamic.yml"
TRAEFIK_BIN=/tmp/opencode/proxies/traefik
[ -x "$TRAEFIK_BIN" ] || { echo "  (not measurable: traefik binary missing)"; exit 1; }
"$TRAEFIK_BIN" --configfile "$DIR/traefik.yml" > "$DIR/traefik.log" 2>&1 &
TRAEFIK_PID=$!
sleep 3
if wait_up "$TRAEFIK_P1" traefik; then
  run_legs traefik "$TRAEFIK_P1" "$TRAEFIK_P2"
  echo "  RSS: $(rss=$(ps -o rss= -p "$TRAEFIK_PID" 2>/dev/null | tr -d ' '); echo "${rss:-0} KB")"
fi
kill "$TRAEFIK_PID" 2>/dev/null
wait "$TRAEFIK_PID" 2>/dev/null

# ================= Envoy (not measurable here) =================
echo "== Envoy: skipped — the v1.31 image accepts connections and reads"
echo "   requests but never responds in this environment (host + bridge"
echo "   networking, minimal direct_response config, zero log errors)."
echo "   Config kept in scripts/ history for a retry on another host."

kill $UP 2>/dev/null
wait 2>/dev/null

echo
echo "================ RESULTS ================"
echo "$RESULTS"
