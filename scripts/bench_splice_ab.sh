#!/usr/bin/env bash
# A/B: vane get64k with the body-splice fast path ON vs OFF.
# SAME binary (response_splice feature compiled in); the config flag
# alone decides engagement, so both arms measure identical code.
# Interleaved median-of-3 windows per docs/perf-arc-2026-10.md
# discipline (bench_compare.sh methodology, vane-vs-vane instead of
# vane-vs-others). Verdict input for the response_splice default-on
# decision.
set -uo pipefail
cd /home/wyatt/dev/src/github.com/WyattAu/vane

DURATION="${1:-10}"
WINDOWS=3
ART=${VANE_ARTIFACTS:-/var/tmp/vane-artifacts}
RAW="$ART/splice_ab_raw.txt"
: > "$RAW"

LG="$PWD/tools/loadgen/target/release"
for t in loadgen upstream; do
  if [ ! -x "$LG/$t" ]; then
    echo "== building tools/loadgen =="
    (cd tools/loadgen && cargo build --release) || exit 2
  fi
done

echo "== building vane (release, response_splice feature) =="
cargo build --release -p vane-proxy --features h3,response_splice || exit 2

load() { uptime | sed 's/.*load average: //' | cut -d, -f1 | cut -d. -f1; }
wait_quiet() {
  for _ in $(seq 1 120); do
    [ "$(load)" -lt 8 ] && return 0
    sleep 5
  done
  echo "LOAD NEVER SETTLED" >&2
  return 1
}

mkport() { python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])"; }
FREE_UP64=$(mkport)
VANE_OFF=$(mkport)
VANE_ON=$(mkport)

"$LG/upstream" "127.0.0.1:$FREE_UP64" 65536 &
UP64=$!
sleep 0.5

# Two instances of the SAME binary; only the [runtime] flag differs.
declare -A PORTS=([off]="$VANE_OFF" [on]="$VANE_ON")
for variant in off on; do
  if [ "$variant" = on ]; then
    FLAG="response_splice = true"
  else
    FLAG="response_splice = false"
  fi
  cat > "$ART/vane_splice_$variant.toml" <<TOML
[[listeners]]
address = "127.0.0.1:${PORTS[$variant]}"
workers = 0

[clusters.up64]
backends = ["127.0.0.1:$FREE_UP64"]

[[routes]]
pattern = "/*rest"
cluster = "up64"

[admin]
enabled = false

[runtime]
force_mio = false
$FLAG
TOML
done

PORT_OFF=$VANE_OFF
PORT_ON=$VANE_ON
./target/release/vane run -c "$ART/vane_splice_off.toml" > "$ART/vane_splice_off.log" 2>&1 &
PID_OFF=$!
./target/release/vane run -c "$ART/vane_splice_on.toml" > "$ART/vane_splice_on.log" 2>&1 &
PID_ON=$!
sleep 1

leg() { # port tag
  "$LG/loadgen" "127.0.0.1:$1" 8 "$DURATION" /bench64 bench64 2>/dev/null | sed "s/^h1 /$2 /"
}

for w in $(seq 1 "$WINDOWS"); do
  echo "== window $w/$WINDOWS =="
  wait_quiet
  leg "$PORT_OFF" "splice-off" "$w" >> "$RAW"
  wait_quiet
  leg "$PORT_ON" "splice-on" "$w" >> "$RAW"
done

kill "$PID_OFF" "$PID_ON" "$UP64" 2>/dev/null
wait 2>/dev/null

echo
echo "========= MEDIAN OF $WINDOWS WINDOWS (get64k c=8) ========="
echo "| arm | median req/s | median p50 | median p99 | non200(max) |"
echo "|---|---|---|---|---|"
awk '
{
  k = $1
  req[k] = req[k] " " $5
  if ($10 + 0 > mx[k] + 0) mx[k] = $10 + 0
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
}' "$RAW"
echo
echo "(per-window detail: $RAW)"
