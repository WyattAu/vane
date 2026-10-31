sections use the in-repo `tools/loadgen` (no artificial ceiling -
class of machine; treat absolute numbers as indicative, ratios as
  direct ceiling measured first: **65–72k rps**; the shared client
  windows, h1/h2/h3, latency percentiles; see the 13-field output
  external build storms (load ≥ 15) were discarded, not adjusted -
## vane vs Caddy vs Traefik: 2026-10-25 (v0.16.0, final worker-budget shape)
across all 54 legs, **zero load warnings**; the cleanest window this
75.9 MB (was 405 MB; the duplicated 16-worker pools pre-allocated
101k get64k; same shape, hotter window; run-to-run spread on this
64 MB; the fixed pre-allocation trade, stated rather than hidden.
## vane vs Caddy vs Traefik: 2026-10-07 (v0.8.0, historical)
`scripts/bench_compare.sh 10`; all three proxies run SIMULTANEOUSLY as
across windows is published below; that is the error bar.
| h1 GET 64KB | 15,326 (p50 478µs) | 16,864 | **19,528** | **0.78×; vane loses** |
historical table below was measured with `workers = 0` on two listeners; which then meant
  plain+TLS config; the historical 15.3k row above was this plus
plus per-session slabs; fixed up front by design; the others grow at
h1-8 at 10.7%; its third window dipped and the median comes from the
this (added in 0.8.0); measured vane-only on this same leg, same host,
## vane 0.13.0: pool telemetry under load (2026-10-17)
1,236,283, `vane_upstream_pool_misses_total` 94,394; a **92.9%
(median of 3); the pool changes are throughput-neutral within the
| keep-alive c=256 |; |; | 47k rps |; |
| short connections c=64 | 6.1k rps |; | 10.5k rps |; |
| direct upstream (no proxy) | 65–72k rps |; |; |; |
(56–60k / 125k measured on v0.2.0 after the hyper-optimization pass -
   per engine event; each a stderr `write(2)`. 25k → 49k rps.
   small integer; the #1 profile symbol (6.6%). ~53k → ~56k rps.
Remaining profile (round 2, post-optimization): flat; httparse
- **vane (1 worker) now beats tuned nginx ~15%**; 56–60k vs 50–51k,
# nginx (tuned): keepalive 64 upstream block
`scripts/bench_bridge.sh`; same route served two ways on one near
dial to its upstream), and one more h1 relay; ~9× the direct-h1
several costs; the ring would recover a fraction of the gap, not all
(`/tmp/opencode/loadgen`, tokio; req/s + latency percentiles); this
## Comparative harness (2026-10-03): superseded by the 2026-10-07 run above
accepts + reads requests but never responds; host and bridge
machine**; rerun is one command.
sequential stream); tracked in docs/h2-streaming-flake.md with a
## Comparative run (2026-10-03, native, load 15–24): superseded by the 2026-10-07 run above
`scripts/bench_compare.sh`; vane vs Caddy 2.10.2 vs Traefik 3.3.6,
~1k req/s at 8.5 ms p50 against a ~0.2 ms upstream; invalid); both
h2/h3 legs are "not measured" (client/config mismatch to debug; its
| **vane 0.5.2** | **37,173** | 26,612 | **1 (bug; tracked)** | **14,151** | 467 MB |
Configs are in the script; the client is vane's loadgen; deltas are
- **vane leads h1 at 8 connections: 2.1× Caddy, 3.9× Traefik**; the
- **vane leads h3: 2.2× Caddy**; and vane is the only one of the
  climbs 9.5k → 27k); many-connection handling needs attention.
- **vane's RSS is 6–10× the Go proxies** (467 MB vs 48/81 MB); the
"conformance-leading, h1/h3-fast Rust edge+mesh data plane" niche -
- **vane leads h2 (2.3× Caddy) and h3 (2.1× Caddy)**; Traefik's
- **Memory: 383 MB at 32 workers ≈ 10 MB/worker; reasonable for the
  per-core parallelism for a smaller flat footprint; different
(1.4× at 8 conns, 2.4× at 64); targeted at the accept path
the loadgen legs return 0; under investigation).
certificate; it served its default cert, and the loadgen correctly
`force_mio = true`; a single mio thread vs Traefik's all-core
config (workers = cores, io_uring backend); the 64-connection "gap"
  drop); the worker model scales as designed.
- Traefik still leads absolute h1 (1.4× at 8 conns, 2.4× at 64) -
  was memory-INEFFICIENT as well); still ~4.6× Caddy; profiling
- The h2 leg vs Traefik is the CLOSEST (18.0k vs 16.9k = 1.07×) -
- vane scales +93% from 8 to 64 connections (h1); the fair-config
GIL-capped Python upstream; superseded by this interleaved harness,
or error-noise; Traefik's earlier "132k" was error responses
vane is the fastest of the three on every protocol**; h1 1.5–2.3×,
h2 1.9–2.4×, h3 1.6–1.9×; with the lowest p50 on every leg, and
standing tradeoff (378 MB vs 33/69 MB; per-core pools, tunable,
**Landed**: rate limiting is opt-in; `None` short-circuits the
**Landed (2026-10-04)**: the dial-path head clones are gone; the
are reusable buffers (clear+push_str / take+restore; capacity