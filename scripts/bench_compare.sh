#!/usr/bin/env bash
# Comparative reverse-proxy benchmark: vane vs Caddy vs Traefik —
# INTERLEAVED median-of-3 methodology.
#
# All proxies run simultaneously on distinct ports; the load generator
# cycles proxy x leg three times (one "window" each), so every proxy
# samples the same machine conditions. The published number per leg is
# the MEDIAN of the three windows — single-window spikes (external
# build storms on this host) are filtered by construction. Min/max
# spread per leg is in the raw file.
#
# Every proxy runs NATIVELY (this machine's docker is rootless-style:
# containerized proxies measure the network plumbing, not the proxy).
# Two listeners per proxy: PLAIN (h1 legs) and TLS (h2 + h3 legs).
# Configs are in this script. The client is vane's loadgen; deltas
# between proxies are the claim. Every leg reports its non-200 count.
#
# Usage: scripts/bench_compare.sh [seconds_per_leg]
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-8}"
WINDOWS=3
# Load generator lives in-repo now (tools/loadgen): it was previously an
# untracked scratch crate under /tmp/opencode, which a restart wiped.
LG="$PWD/tools/loadgen/target/release"
for t in loadgen h2load h3load certgen upstream; do
  if [ ! -x "$LG/$t" ]; then
    echo "== building tools/loadgen =="
    (cd tools/loadgen && cargo build --release) || exit 2
    break
  fi
done

echo "== building vane (release) =="
cargo build --release -p vane-proxy --features h3 || exit 2

mkport() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"; }
FREE_UP=$(mkport)
FREE_UP64=$(mkport)
VANE_P1=$(mkport); VANE_P2=$(mkport)
CADDY_P1=$(mkport); CADDY_P2=$(mkport)
TRAEFIK_P1=$(mkport); TRAEFIK_P2=$(mkport)

# ---- shared upstreams (identical for every proxy) ----
# `$FREE_UP` serves 10-byte bodies (the tiny-response legs); `$FREE_UP64`
# serves 64 KiB bodies (the breadth legs). Two processes, so a 64 KiB
# transfer can never contend with the tiny-response legs on upstream CPU.
"$LG/upstream" "127.0.0.1:$FREE_UP" &
UP=$!
"$LG/upstream" "127.0.0.1:$FREE_UP64" 65536 &
UP64=$!
sleep 0.5

DIR=$(mktemp -d)
"$LG/certgen" "$DIR"

# ---- vane ----
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

[clusters.up64]
backends = ["127.0.0.1:$FREE_UP64"]

[[routes]]
pattern = "/bench64"
cluster = "up64"

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = false
TOML
./target/release/vane run -c "$DIR/vane.toml" > "$DIR/vane.log" 2>&1 &
VANE_PID=$!

# ---- caddy ----
cat > "$DIR/Caddyfile" <<EOF
{
  auto_https off
  admin off
}
:$CADDY_P1 {
  handle_path /bench64* {
    reverse_proxy 127.0.0.1:$FREE_UP64
  }
  reverse_proxy 127.0.0.1:$FREE_UP
}
:$CADDY_P2 {
  tls $DIR/cert.pem $DIR/key.pem
  handle_path /bench64* {
    reverse_proxy 127.0.0.1:$FREE_UP64
  }
  reverse_proxy 127.0.0.1:$FREE_UP
}
EOF
/tmp/opencode/proxies/caddy run --config "$DIR/Caddyfile" > "$DIR/caddy.log" 2>&1 &
CADDY_PID=$!

# ---- traefik ----
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
cat > "$DIR/rules/dynamic.yml" <<'EOF'
tls:
  certificates:
    - certFile: "CERTDIR/cert.pem"
      keyFile: "CERTDIR/key.pem"
http:
  routers:
    plain64:
      rule: "Path(`/bench64`)"
      service: up64
      entryPoints: ["plain"]
    plain:
      rule: "PathPrefix(`/`)"
      service: up
      entryPoints: ["plain"]
    secure64:
      rule: "Path(`/bench64`)"
      service: up64
      entryPoints: ["tls"]
      tls: {}
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
    up64:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:REPLACE_UP64"
EOF
sed -i "s|REPLACE_UP64|$FREE_UP64|; s|REPLACE_UP|$FREE_UP|; s|CERTDIR|$DIR|g" "$DIR/rules/dynamic.yml"
/tmp/opencode/proxies/traefik --configfile "$DIR/traefik.yml" > "$DIR/traefik.log" 2>&1 &
TRAEFIK_PID=$!
sleep 3

# ---- load gate: external build storms invalidate measurements ----
wait_quiet() {
  for _ in $(seq 1 120); do
    local l
    l=$(awk '{print int($1)}' /proc/loadavg)
    [ "${l:-99}" -lt 4 ] && return 0
    sleep 5
  done
  echo "  (WARN: load never dropped below 4 — numbers may be depressed)"
  return 0
}

leg() { # port proto conns
  case "$2" in
    h1) "$LG/loadgen" "127.0.0.1:$1" "$3" "$DURATION" /bench bench 2>/dev/null ;;
    h2) "$LG/h2load" "127.0.0.1:$1" "$3" "$DURATION" /bench localhost "$DIR/cert.pem" 2>/dev/null ;;
    h3) "$LG/h3load" "127.0.0.1:$1" "$3" "$DURATION" /bench localhost "$DIR/cert.pem" h3 2>/dev/null ;;
  esac
}

RAW="/tmp/opencode/compare_raw.txt"
: > "$RAW"

run_legs() { # proxy plain_port tls_port window
  local proxy=$1 p1=$2 p2=$3 w=$4 out
  wait_quiet
  out=$(leg "$p1" h1 8);  echo "$proxy h1-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p1" h1 64); echo "$proxy h1-64 $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h2 8);  echo "$proxy h2-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h3 8);  echo "$proxy h3-8  $w $out" >> "$RAW"
  # Breadth legs: same wire path as h1-8, but payload-dominated. A proxy
  # that wins on tiny responses can lose here (header overhead per byte,
  # buffer sizing); without these legs the table overstates real edges.
  # POST 4KB also exercises the upload direction, which GET legs never
  # touch.
  wait_quiet
  out=$("$LG/loadgen" "127.0.0.1:$p1" 8 "$DURATION" /bench bench --method POST --body 4096 2>/dev/null | sed 's/^h1 /h1-post4k /')
  echo "$proxy h1-post4k $w $out" >> "$RAW"
  wait_quiet
  out=$("$LG/loadgen" "127.0.0.1:$p1" 8 "$DURATION" /bench64 bench64 2>/dev/null | sed 's/^h1 /h1-get64k /')
  echo "$proxy h1-get64k $w $out" >> "$RAW"
}

for w in $(seq 1 "$WINDOWS"); do
  echo "== window $w/$WINDOWS =="
  run_legs vane    "$VANE_P1"    "$VANE_P2"    "$w"
  run_legs caddy   "$CADDY_P1"   "$CADDY_P2"   "$w"
  run_legs traefik "$TRAEFIK_P1" "$TRAEFIK_P2" "$w"
done

echo "  RSS: vane $(ps -o rss= -p "$VANE_PID" 2>/dev/null | tr -d ' ') KB, caddy $(ps -o rss= -p "$CADDY_PID" 2>/dev/null | tr -d ' ') KB, traefik $(ps -o rss= -p "$TRAEFIK_PID" 2>/dev/null | tr -d ' ') KB"

kill "$VANE_PID" "$CADDY_PID" "$TRAEFIK_PID" "$UP" "$UP64" 2>/dev/null
wait 2>/dev/null

echo
echo "================ MEDIAN OF $WINDOWS WINDOWS ================"
echo "| proxy | leg | median req/s | median p50 | median p99 | non200(max) |"
echo "|---|---|---|---|---|---|"
# Raw row = "proxy leg window" + the loadgen's 13 fields, so loadgen
# field N sits at awk column N+3: rps=8, non200=12, p50=14, p99=16.
# (The columns used to line up at 5/9/11/13 when the generator printed
# 10 fields; the richer line shifted them and the first compare run
# computed medians over connection counts — caught before publishing.)
awk '
{
  k = $1 " " $2
  req[k] = req[k] " " $8
  if ($12 + 0 > mx[k] + 0) mx[k] = $12 + 0
  p50[k] = p50[k] " " $14
  p99[k] = p99[k] " " $16
}
function med(str,   n, a, i, j, t) {
  n = split(str, a, " ")
  for (i = 1; i <= n; i++)
    for (j = i + 1; j <= n; j++)
      if (a[j] < a[i]) { t = a[i]; a[i] = a[j]; a[j] = t }
  return a[int((n + 1) / 2)]
}
END {
  for (k in req)
    print "| " k " | " med(req[k]) " | " med(p50[k]) " | " med(p99[k]) " | " mx[k] " |"
}' "$RAW"
echo
echo "(per-window detail: $RAW)"
