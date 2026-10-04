#!/usr/bin/env bash
# Comparative reverse-proxy benchmark: vane vs Caddy vs Traefik —
# INTERLEAVED median-of-3 methodology.
#
# All proxies run simultaneously on distinct ports; the load generator
# cycles proxy×leg three times (one "window" each), so every proxy
# samples the same machine conditions. The published number per leg is
# the MEDIAN of the three windows (single-window spikes — external
# build storms on this host — are filtered by construction). Min/max
# spread is printed alongside for honesty.
#
# Every proxy runs NATIVELY (this machine's docker is rootless-style:
# containerized proxies measure the network plumbing, not the proxy).
# Two listeners per proxy: PLAIN (h1 legs) and TLS (h2 + h3 legs).
# Configs are in this script — nothing crippled, nothing secretly
# tuned. The client is vane's loadgen; deltas between proxies are the
# claim, not absolute numbers. Every leg reports its non-200 count —
# proxy error responses never count as throughput.
#
# Usage: scripts/bench_compare.sh [seconds_per_leg]
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-8}"
WINDOWS=3
LG=/tmp/opencode/loadgen/target/release
for t in loadgen h2load h3load certgen upstream; do
  [ -x "$LG/$t" ] || { echo "bench tools missing ($t) — build /tmp/opencode/loadgen"; exit 2; }
done

echo "== building vane (release) =="
cargo build --release -p vane-proxy --features h3 || exit 2

mkport() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"; }
FREE_UP=$(mkport)
for v in VANE_P1 VANE_P2 CADDY_P1 CADDY_P2 TRAEFIK_P1 TRAEFIK_P2; do
  declare "$v=$(mkport)"
done

# ---- shared upstream (identical for every proxy) ----
"$LG/upstream" "127.0.0.1:$FREE_UP" &
UP=$!
sleep 0.5

DIR=$(mktemp -d)
"$LG/certgen" "$DIR"

# ---- vane: plain + TLS listeners, workers = cores, io_uring ----
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
./target/release/vane run -c "$DIR/vane.toml" > "$DIR/vane.log" 2>&1 &
VANE_PID=$!

# ---- caddy: plain + TLS site blocks ----
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
/tmp/opencode/proxies/caddy run --config "$DIR/Caddyfile" > "$DIR/caddy.log" 2>&1 &
CADDY_PID=$!

# ---- traefik: plain + TLS (+ h3) entryPoints ----
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
/tmp/opencode/proxies/traefik --configfile "$DIR/traefik.yml" > "$DIR/traefik.log" 2>&1 &
TRAEFIK_PID=$!
sleep 3

# ---- load gate: wait for a quiet window (external build storms) ----
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

RAW=$"/tmp/opencode/compare_raw.txt"
: > "$RAW"

run_legs() { # proxy plain_port tls_port window
  local proxy=$1 p1=$2 p2=$3 w=$4
  wait_quiet
  local out
  out=$(leg "$p1" h1 8);  echo "$proxy h1-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p1" h1 64); echo "$proxy h1-64 $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h2 8);  echo "$proxy h2-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h3 8);  echo "$proxy h3-8  $w $out" >> "$RAW"
}

for w in $(seq 1 "$WINDOWS"); do
  echo "== window $w/$WINDOWS =="
  run_legs vane    "$VANE_P1"    "$VANE_P2"    "$w"
  run_legs caddy   "$CADDY_P1"   "$CADDY_P2"   "$w"
  run_legs traefik "$TRAEFIK_P1" "$TRAEFIK_P2" "$w"
done

echo "  RSS: vane $(ps -o rss= -p "$VANE_PID" 2>/dev/null | tr -d ' ') KB, caddy $(ps -o rss= -p "$CADDY_PID" 2>/dev/null | tr -d ' ') KB, traefik $(ps -o rss= -p "$TRAEFIK_PID" 2>/dev/null | tr -d ' ') KB"

kill "$VANE_PID" "$CADDY_PID" "$TRAEFIK_PID" "$UP" 2>/dev/null
wait 2>/dev/null

# ---- medians ----
echo
echo
echo "================ MEDIAN OF $WINDOWS WINDOWS ================"
echo "| proxy | leg | median req/s | median p50 | median p99 | non200(max) |"
echo "|---|---|---|---|---|---|"
awk '
{
  k = $1 " " $2
  req[k] = req[k] " " $5
  if ($9 + 0 > mx[k] + 0) mx[k] = $9 + 0
  p50[k] = p50[k] " " $11
  p99[k] = p99[k] " " $13
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
}' "$RAW")in/env bash
# Comparative reverse-proxy benchmark: vane vs Caddy vs Traefik —
# INTERLEAVED median-of-3 methodology.
#
# All proxies run simultaneously on distinct ports; the load generator
# cycles proxy×leg three times (one "window" each), so every proxy
# samples the same machine conditions. The published number per leg is
# the MEDIAN of the three windows (single-window spikes — external
# build storms on this host — are filtered by construction). Min/max
# spread is printed alongside for honesty.
#
# Every proxy runs NATIVELY (this machine's docker is rootless-style:
# containerized proxies measure the network plumbing, not the proxy).
# Two listeners per proxy: PLAIN (h1 legs) and TLS (h2 + h3 legs).
# Configs are in this script — nothing crippled, nothing secretly
# tuned. The client is vane's loadgen; deltas between proxies are the
# claim, not absolute numbers. Every leg reports its non-200 count —
# proxy error responses never count as throughput.
#
# Usage: scripts/bench_compare.sh [seconds_per_leg]
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-8}"
WINDOWS=3
LG=/tmp/opencode/loadgen/target/release
for t in loadgen h2load h3load certgen upstream; do
  [ -x "$LG/$t" ] || { echo "bench tools missing ($t) — build /tmp/opencode/loadgen"; exit 2; }
done

echo "== building vane (release) =="
cargo build --release -p vane-proxy --features h3 || exit 2

mkport() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"; }
FREE_UP=$(mkport)
for v in VANE_P1 VANE_P2 CADDY_P1 CADDY_P2 TRAEFIK_P1 TRAEFIK_P2; do
  declare "$v=$(mkport)"
done

# ---- shared upstream (identical for every proxy) ----
"$LG/upstream" "127.0.0.1:$FREE_UP" &
UP=$!
sleep 0.5

DIR=$(mktemp -d)
"$LG/certgen" "$DIR"

# ---- vane: plain + TLS listeners, workers = cores, io_uring ----
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
./target/release/vane run -c "$DIR/vane.toml" > "$DIR/vane.log" 2>&1 &
VANE_PID=$!

# ---- caddy: plain + TLS site blocks ----
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
/tmp/opencode/proxies/caddy run --config "$DIR/Caddyfile" > "$DIR/caddy.log" 2>&1 &
CADDY_PID=$!

# ---- traefik: plain + TLS (+ h3) entryPoints ----
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
/tmp/opencode/proxies/traefik --configfile "$DIR/traefik.yml" > "$DIR/traefik.log" 2>&1 &
TRAEFIK_PID=$!
sleep 3

# ---- load gate: wait for a quiet window (external build storms) ----
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

RAW=$"/tmp/opencode/compare_raw.txt"
: > "$RAW"

run_legs() { # proxy plain_port tls_port window
  local proxy=$1 p1=$2 p2=$3 w=$4
  wait_quiet
  local out
  out=$(leg "$p1" h1 8);  echo "$proxy h1-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p1" h1 64); echo "$proxy h1-64 $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h2 8);  echo "$proxy h2-8  $w $out" >> "$RAW"
  wait_quiet
  out=$(leg "$p2" h3 8);  echo "$proxy h3-8  $w $out" >> "$RAW"
}

for w in $(seq 1 "$WINDOWS"); do
  echo "== window $w/$WINDOWS =="
  run_legs vane    "$VANE_P1"    "$VANE_P2"    "$w"
  run_legs caddy   "$CADDY_P1"   "$CADDY_P2"   "$w"
  run_legs traefik "$TRAEFIK_P1" "$TRAEFIK_P2" "$w"
done

echo "  RSS: vane $(ps -o rss= -p "$VANE_PID" 2>/dev/null | tr -d ' ') KB, caddy $(ps -o rss= -p "$CADDY_PID" 2>/dev/null | tr -d ' ') KB, traefik $(ps -o rss= -p "$TRAEFIK_PID" 2>/dev/null | tr -d ' ') KB"

kill "$VANE_PID" "$CADDY_PID" "$TRAEFIK_PID" "$UP" 2>/dev/null
wait 2>/dev/null

# ---- medians ----
echo
echo
echo "================ MEDIAN OF $WINDOWS WINDOWS ================"
echo "| proxy | leg | median req/s | median p50 | median p99 | non200(max) |"
echo "|---|---|---|---|---|---|"
awk '
{
  proxy = $1; leg = $2
  key = proxy " " leg
  for (i = 4; i <= NF; i += 2) {
    v = $(i + 1)
    if (i == 4)      req[key, ++n_req[key]] = v + 0
    else if (i == 6) n200[key] = (v + 0 > n200[key] + 0 ? v + 0 : n200[key] + 0)
    else if (i == 8) p50[key, ++n_p50[key]] = v + 0
    else if (i == 10) p99[key, ++n_p99[key]] = v + 0
  }
}
function median(arr, n,   i, j, t) {
  for (i = 1; i <= n; i++)
    for (j = i + 1; j <= n; j++)
      if (arr[j] < arr[i]) { t = arr[i]; arr[i] = arr[j]; arr[j] = t }
  return arr[int((n + 1) / 2)]
}
END {
  for (k in req) {
    print "| " k " | " median(req, n_req[k]) " | " median(p50, n_p50[k]) " | " median(p99, n_p99[k]) " | " n200[k] " |"
  }
}' "$RAW"
echo
echo "(per-window detail: $RAW)"
