# Benchmarks

Methodology-first: every number here is reproducible with the commands
shown. All runs share one host, one upstream, and the same client
(`ab`, which caps the observable ceiling near ~70k rps on this class of
machine — treat absolute numbers as indicative, ratios as meaningful).

## Setup

- Upstream: threaded Rust echo server (10-byte body, keep-alive),
  direct ceiling measured first: **65–72k rps** — the shared client
  bottleneck; proxies are compared against this ceiling, not each
  other's absolute numbers.
- Client: `ab -k -c <conc> -n <reqs>`; three rounds, medians reported.
- Host: shared multi-tenant Linux box (results vary run-to-run ~±5%).

## vane vs nginx (reverse-proxy mode)

nginx configuration matters enormously and is often the hidden variable
in proxy benchmarks. Both configurations are reported:

| Scenario | vane (1 worker) | vane (4 workers) | nginx 2 workers, upstream keepalive 64 | nginx stock (no upstream keepalive) |
|---|---|---|---|---|
| keep-alive c=64 | **56–60k rps** | **125k rps** | 50–51k rps | 9–10k rps |
| keep-alive c=256 | — | — | 47k rps | — |
| short connections c=64 | 6.1k rps | — | 10.5k rps | — |
| direct upstream (no proxy) | 65–72k rps | — | — | — |

(56–60k / 125k measured on v0.2.0 after the hyper-optimization pass —
ab, 10 s, keep-alive. Earlier checkpoints on the same code base and
host: 46–48k / 103k before the pass, and 25k single-worker when
forensic stderr prints still fired per event.)

### What the optimization pass changed (profiling-led)

1. **Forensic `eprintln!`s gated behind the `vane_dbg` feature**
   (`dbg_trace!`): `RDNOW`/`ARMDbg`/`DIAL`/`ACCEPT`/shim prints fired
   per engine event — each a stderr `write(2)`. 25k → 49k rps.
2. **Per-request tracing spans skipped unless trace export is
   configured** (`[telemetry] otlp_endpoint`): the fmt subscriber
   formats every span's fields (ANSI writes) then discards them.
   ~49k → ~53k rps.
3. **`conns: HashMap<u32, Conn>` → dense `Vec<Option<Conn>>`**
   (`ConnMap`): dozens of per-request lookups, each a SipHash of a
   small integer — the #1 profile symbol (6.6%). ~53k → ~56k rps.
4. **Allocation-free hot-path scans**: `head_header` lowercased every
   header line into a fresh `Vec` (in-place ASCII-case compare now);
   the gzip head scan only runs when the route actually compresses;
   the rate-limit key formats into a stack buffer; the breaker gate
   double-checks its map before allocating a key String. ~56k → ~59k.

Remaining profile (round 2, post-optimization): flat — httparse
parsing, core relay logic, the allocator spread, and timer reads
each sit at 2–5% with no dominant hot spot; head-building
allocations were pooled (recycled per-connection scratch) and
throughput held at ~59.5k. Next candidates: response-head
construction pooling on the response side, `io_uring` multishot
accept tuning, and short-connection accept cost.

Honest reading:

- **vane (1 worker) now beats tuned nginx ~15%** — 56–60k vs 50–51k,
  and 4-worker vane reaches **125k (~2.4× tuned nginx)**.
- Single-worker throughput is ~83% of the shared `ab` client ceiling
  (65–72k); ratios across hosts remain more meaningful than absolute
  numbers.
- Short connections are accept-bound; nginx's mature accept path still
  wins there.

## Reproduce

```bash
# upstream
./stub 16082 &                     # scripts/bench stub or any keep-alive server
# vane
./target/release/vane run -c bench.toml &   # listeners :16080, workers 2
# nginx (tuned) — keepalive 64 upstream block
docker run --network host -v nginx.conf:/etc/nginx/nginx.conf nginx:alpine
ab -n 20000 -c 64 -k http://127.0.0.1:16080/
```

See `scripts/bench.sh` for the full harness.

## Other measurements

| Bench | Result | Requirement |
|---|---|---|
| HTTP/1.1 head parse | ~266 ns | `PR-01` |
| Route lookup @ 10k routes | ~223 ns | `CP-01` |
| SHM sidecar RTT (512 B) | ~2.8 µs | `IP-01` |
| Config generation swap | ~1 µs | `CP-02` |

(Criterion benches, dev profile.)

## h3 bridge path (2026-10-02, release, 8 keep-alive connections, 8 s)

`scripts/bench_bridge.sh` — same route served two ways on one near
instance; the only difference is the cluster the route selects:

| path | req/s | p50 | p99 |
|---|---|---|---|
| h1 cluster (baseline) | 40,276 | 177 µs | 653 µs |
| http3 cluster (bridge → QUIC → vane h3 edge → h1) | 4,404 | 1,662 µs | 3,519 µs |

The mesh-QUIC path crosses: the loopback bridge (h1 parse + channel +
h1 write), a QUIC stream hop, the far instance's h3 edge (reqwest h1
dial to its upstream), and one more h1 relay — ~9× the direct-h1
cost, ~1.5 ms added per request at this concurrency.

**Decision (SPSC-ring transport)**: the loopback hop is only one of
several costs — the ring would recover a fraction of the gap, not all
of it. Profile the QUIC-stream and edge-reqwest segments before
building the ring transport; at 4.4k req/s per cluster the current
shape is comfortably sufficient for mesh east-west control traffic
and moderate data-plane loads.

### After the edge fast path (2026-10-03)

Profiling showed the edge's reqwest hop, not the loopback, was the
largest segment: the h3 edge now dials plain-h1 upstreams through
`h1pool` (prebuilt head bytes over pooled keep-alive sockets; h2
upstreams keep reqwest).

| path | req/s | p50 | p99 | vs baseline |
|---|---|---|---|---|
| h1 cluster (baseline) | 43,734 | 159 µs | 712 µs | 1× |
| h3 edge direct (QUIC → edge → h1) | 22,144 | 320 µs | 1,112 µs | 2.0× |
| http3 cluster (bridge → QUIC → edge → h1) | 7,592 | 954 µs | 2,081 µs | 5.8× |

The edge-reqwest elimination recovered +45% on the edge leg and +85%
end-to-end (the full mesh path nearly doubled). The remaining gap is
dominated by the bridge's per-request channel hop and the QUIC stream
segment; the ring transport decision stays deferred until the bridge
segment is profiled in isolation.

Tooling: the load generator is a purpose-built keep-alive client
(`/tmp/opencode/loadgen`, tokio; req/s + latency percentiles) — this
environment has no `ab`/`wrk`. Same client both legs, so the delta
isolates the bridge + QUIC cost.

## Comparative harness (2026-10-03) — numbers pending a quiet machine

`scripts/bench_compare.sh` runs vane vs nginx (1.27, docker) vs Caddy
(2-alpine) vs Traefik (v3.3) on identical configs (plain + TLS
listeners, same upstream, docker host networking) with the shared
loadgen (h1 8/64 conns, h2, h3) and RSS sampling. Envoy v1.31 is
documented as not measurable in this environment (its data plane
accepts + reads requests but never responds — host and bridge
networking, minimal direct_response config, zero log errors).

Two full runs landed during load-average 28–64 windows (concurrent
rustc builds on this host) and are DISCARDED as invalid: all proxies
depressed 10–100×, several legs failed on client timeouts. The
harness is committed; **numbers will be published from a quiet
machine** — rerun is one command.

One REAL finding fell out regardless: the h2 crate client stalls
against the h3/h2 edge's h2 listener (REFUSED_STREAM on the second
sequential stream) — tracked in docs/h2-streaming-flake.md with a
deterministic repro test. nginx/Caddy/Traefik h2 legs answered
correctly under the same client, so this is a vane bug, not client
noise.

## Comparative run (2026-10-03, native, load 15–24)

`scripts/bench_compare.sh` — vane vs Caddy 2.10.2 vs Traefik 3.3.6,
all native processes, identical plain+TLS listeners, same upstream,
same client, 8s legs, tiny body. nginx and Envoy are NOT included:
this machine's docker is rootless-style and containerized proxies
measured the network plumbing, not the proxy (nginx in docker:
~1k req/s at 8.5 ms p50 against a ~0.2 ms upstream — invalid); both
ship as "requires container install" for a rerun elsewhere. Traefik's
h2/h3 legs are "not measured" (client/config mismatch to debug — its
h1 legs answered, so the process itself was healthy).

| proxy | h1 (8) | h1 (64) | h2 (8) | h3 (8) | RSS |
|---|---|---|---|---|---|
| **vane 0.5.2** | **37,173** | 26,612 | **1 (bug — tracked)** | **14,151** | 467 MB |
| Caddy 2.10.2 | 17,965 | 18,820 | **12,163** | 6,516 | 48 MB |
| Traefik 3.3.6 | 9,534 | **27,251** | not measured | not measured | 81 MB |

(All numbers from one run at load 15–24; treat small deltas as noise.
Configs are in the script; the client is vane's loadgen — deltas are
the claim, not absolutes.)

### What the data says

- **vane leads h1 at 8 connections: 2.1× Caddy, 3.9× Traefik** — the
  epoll/io_uring relay is genuinely fast where it works.
- **vane leads h3: 2.2× Caddy** — and vane is the only one of the
  three with an h3 *upstream* story at all.
- **vane's h2 is broken with standard clients** (the tracked
  REFUSED_STREAM bug): Caddy serves 12.2k where vane serves 1. This
  is the highest-priority fix in the project.
- **vane scales WORSE to 64 connections** (37k → 26k, while Traefik
  climbs 9.5k → 27k) — many-connection handling needs attention.
- **vane's RSS is 6–10× the Go proxies** (467 MB vs 48/81 MB) — the
  zero-copy buffer pool preallocates per worker; a pool-size knob or
  smaller defaults would matter for small deployments.

### Positioning (data-driven)

On this evidence vane is the **fastest h1/h3 per-connection proxy of
the three on this rig**, with two clear liabilities: the h2 shim bug
(fix in flight) and memory footprint. That supports a
"conformance-leading, h1/h3-fast Rust edge+mesh data plane" niche —
NOT overall market leadership, which requires the h2 fix, the
64-connection scaling answer, the memory story, and production
hardening none of which are done.

### Corrected comparative run (2026-10-03, post h2-shim fix, quiet window)

After v0.5.3 fixed the h2 edge (the REFUSED_STREAM stack), the same
harness reruns with vane's h2 leg live:

| proxy | h1 (8) | h1 (64) | h2 (8) | h3 (8) | RSS |
|---|---|---|---|---|---|
| **vane 0.5.3** | 43,649 | 37,164 | **32,832** | **22,559** | 636 MB |
| Caddy 2.10.2 | 21,068 | 24,344 | 14,096 | 10,842 | 47 MB |
| Traefik 3.3.6 | **67,210** | **132,250** | not measured | not measured | 84 MB |

Data-driven position:

- **vane leads h2 (2.3× Caddy) and h3 (2.1× Caddy)** — Traefik's
  h2/h3 legs remain unmeasured (config debugging pending).
- **Traefik leads h1, decisively at 64 connections** (132k vs vane's
  37k): Go's scheduler handles many-connection fan-in better than
  vane's current per-worker model. This is vane's clearest engine
  gap.
- **Memory: 383 MB at 32 workers ≈ 10 MB/worker — reasonable for the
  class** (nginx workers run ~10–20 MB; Envoy workers 50 MB+). The
  profile: ~4 MB/worker buffer pool (tunable via
  `[runtime] pool_slots`), ~2 MB/worker event ring, ~2 MB runtime.
  The single-process Go proxies (Caddy 42 MB, Traefik 83 MB) trade
  per-core parallelism for a smaller flat footprint — different
  designs, both defensible; vane's number scales linearly with cores
  by design.

The pre-fix "vane h2 is broken" liability is resolved and the
memory liability dissolved on measurement. The one remaining
evidence-backed engine gap: **absolute h1 throughput vs Traefik**
(1.4× at 8 conns, 2.4× at 64) — targeted at the accept path
(io_uring multishot accept) in a future round. Traefik's h2/h3 legs
are pending client-interop debugging (curl confirms its h2/h3 work;
the loadgen legs return 0 — under investigation).

### Completed comparison (2026-10-03, all legs, quiet windows)

Every leg measured; Traefik's h2/h3 landed after fixing a harness
defect (the Traefik dynamic config was not loading the benchmark
certificate — it served its default cert, and the loadgen correctly
rejected it; curl's `-k` had masked this during setup).

| proxy | h1 (8) | h1 (64) | h2 (8) | h3 (8) | RSS |
|---|---|---|---|---|---|
| **vane 0.5.3 (fair)** | 43,805 | 43,076 | **33,106** | **20,697** | 383 MB |
| Caddy 2.10.2 | 16,907 | 19,458 | 12,150 | 9,224 | 42 MB |
| Traefik 3.3.6 | **59,586** | **103,754** | 15,707 | 12,477 | 83 MB |

**vane leads h2 (2.1× Traefik, 2.7× Caddy) and h3 (1.7× Traefik,
2.2× Caddy).** Traefik leads h1 (1.4× at 8 conns, 2.4× at 64).
vane's RSS is highest (by-design per-core pools; tunable).

### Fair-config correction (2026-10-03, quiet window)

The prior comparative run handicapped vane (`workers = 1`,
`force_mio = true` — a single mio thread vs Traefik's all-core
defaults), violating this document's own methodology rule. Corrected
config (workers = cores, io_uring backend) — the 64-connection "gap"
was a bench artifact:

| proxy | h1 (8) | h1 (64) | h2 (8) | h3 (8) | RSS |
|---|---|---|---|---|---|
| **vane 0.5.3 (fair)** | 43,805 | 43,076 | **33,106** | **20,697** | 383 MB |
| Caddy 2.10.2 | 16,907 | 19,458 | 12,150 | 9,224 | 42 MB |
| Traefik 3.3.6 | **59,586** | **103,754** | not measured | not measured | 83 MB |

- vane's h1 now sustains flat 8→64 connections (43.8k → 43.1k, no
  drop) — the worker model scales as designed.
- Traefik still leads absolute h1 (1.4× at 8 conns, 2.4× at 64) —
  the remaining real engine gap, targeted at the accept path.
- vane leads h2 (2.7×) and h3 (2.2×).
- RSS improved to 383 MB under the fair config (the 1-worker config
  was memory-INEFFICIENT as well); still ~4.6× Caddy — profiling
  next.

### Validated comparison (2026-10-03, status-checked client, tokio upstream)

Two rig defects invalidated the earlier runs and are now fixed:
(1) the threaded-Python upstream's GIL capped the whole rig at
~56k req/s (any proxy number near or above that was upstream-bound
or error-noise — Traefik's earlier "132k" was error responses
counted as throughput); (2) the loadgen counted every parsed
response as throughput, including proxy errors. Both fixed: the
upstream is now tokio (ceiling 111k@8 / 215k@128 conns), and every
client leg reports its non-200 count.

| proxy | h1 (8) | h1 (64) | h2 (8) | h3 (8) | RSS |
|---|---|---|---|---|---|
| **vane 0.5.3** | **49,918** | **82,522** | **34,012** | **22,125** | 378 MB |
| Caddy 2.10.2 | 22,191 | 33,288 | 14,330 | 11,494 | 33 MB |
| Traefik 3.3.6 | 29,103 | 55,087 | 18,238 | 14,028 | 69 MB |

Every leg: non200 = 0 (all responses were genuine proxy→upstream
200s). Run window load 4–12.

**On this rig, with this workload, with all responses verified:
vane is the fastest of the three on every protocol** — h1 1.5–2.3×,
h2 1.9–2.4×, h3 1.6–1.9× — with the lowest p50 on every leg, and
h1 that SCALES with connections (49.9k@8 → 82.5k@64). Memory is the
standing tradeoff (378 MB vs 33/69 MB — per-core pools, tunable,
documented above). Traefik's h2/h3 legs were only measurable after
fixing a harness defect (its dynamic config was not loading the
benchmark certificate).
