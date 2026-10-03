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
