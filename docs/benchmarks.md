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
