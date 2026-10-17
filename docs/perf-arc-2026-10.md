# Performance research arc — 2026-10

The follow-on to `docs/research-loop-2026-10.md` (which closed on the
security/protocol surface). This arc targets the part of the repo with
an honest, measured deficit: the relay hot path. The README's own
comparison table says a tuned nginx holds a ~1.4× throughput lead in
the keep-alive leg — that gap is the arc's first target, and every
iteration follows the same discipline: pull the sources, analyze, find
prior art, write the verdict, implement, prove it lean, measure on a
quiet window.

## Iteration 1 — upstream connection reuse

**Pulled:** the Cloudflare Pingora architecture post ("How we built
Pingora": multithreaded + work stealing so connection pools share
across threads; measured 87.1% → 99.92% reuse for one customer, 160×
fewer new origin connections, ~70% less CPU than the nginx-based
service, −80 ms p95 TTFB), Cloudflare's "Keepalives considered
harmful" (the counterweight: keep-alives PIN requests to workers, and
Linux's accept skew makes the busy worker busier — their fix was
SO_REUSEPORT, which vane already does per worker), nginx's
`keepalive N` per-worker upstream pool and its documented
`keepalive_timeout`, HAProxy's server pool semantics, and Envoy's
per-worker connection pools (the same design vane had).

**Analysis:** reuse is the biggest lever on proxy throughput, and the
repo's own benchmark table is the proof: stock nginx (no upstream
keep-alive) does 9–10k rps against the same upstream where the tuned
configuration does 50–51k. That 5× jump is *only* connection reuse.
vane HAS a pool (`pool_per_backend`, worker-local, per backend), but
three things made it weaker than the nginx configuration it is
benchmarked against:

1. **No reuse telemetry.** Nothing measured hits, misses or dials —
   an operator (and this repo's own bench harness) could not tell a
   healthy pool from a churning one. "Keep-alive pool (opt-in)" in
   the README measured one connection across five sequential requests
   on ONE downstream connection — the case that cannot miss — and
   never the cross-connection reuse that actually matters.
2. **No idle deadline.** A parked connection was reusable forever.
   Backends enforce their own idle timeouts (nginx
   `keepalive_timeout` 60 s, most LBs 60 s), so a connection parked
   past that point is a promise vane cannot keep: the first request
   that checks it out pays a failed attach plus a fresh dial — the
   exact latency spike pools exist to prevent. Worse, the dead fd
   stayed parked forever on quiet backends.
3. **Cap-based eviction without reaping.** A burst that filled the
   pool pushed out connections arbitrarily (Vec::pop of the freshest
   — but expired entries below were never reclaimed until a checkout
   walked them), so quiet periods left full pools of dead fds, and
   the next burst found nothing alive to reuse.

The cross-worker question (Pingora's shared pools) is deliberately
NOT adopted: a shared pool needs cross-thread handoff of live
connections on the hot path, trading the churn win for contention and
complexity. Envoy ships per-worker pools for the same reason. The
trigger for revisiting is now measurable — if
`vane_upstream_pool_hits_total / (hits + misses)` stays low while
worker count is high, the sharing experiment earns its complexity.
Cloudflare's own counterweight post ("Keepalives considered harmful")
warns the other direction too: connection pinning concentrates load;
SO_REUSEPORT (which vane's per-worker listeners already use) is their
mitigation.

**Verdict: KEEP (per-worker pool) + ADOPT (reuse telemetry, idle
deadline, reap-before-refuse).**

**Implemented (this iteration):**

- `vane_upstream_pool_hits_total`, `vane_upstream_pool_misses_total`,
  `vane_upstream_dials_total` (counters) and `vane_upstream_pool_idle`
  (gauge) — the reuse ratio is visible in production, per worker.
- `runtime.pool_idle_ms` (default 30 000): a parked connection is
  reusable for 30 s after it was parked; past that it is reaped, not
  handed out. Checkout walks the pool freshest-first and stops at the
  first live entry, reaping expired ones on the way.
- Park reaps expired entries BEFORE refusing room: a burst that
  filled the pool can no longer push out a connection the next burst
  would have reused; only genuinely live connections are evicted by
  the cap.
- `Drop for HttpProxy` closes every parked descriptor — parked fds
  are detached from the engine, so a dropped worker (embedder
  teardown, failed restart) used to leak them.

**Lean proof (all clock-free — `pool_idle_ms` is the only time
input, and a deadline of 0 means "already expired", so no test
sleeps on a race):**

- `proxy::pool_tests` (7 tests): live checkout reuses and empties the
  gauge; expired checkout closes instead of handing out; freshest is
  taken and the stale below it is reaped; park reaps expired entries
  before refusing room; a full pool closes the arrival; a dropped
  proxy closes its pooled descriptors; the metrics are published.
  Liveness of every handed-out/closed connection is asserted through
  the PEER end of a real socketpair (a 1-byte write succeeds iff the
  other end exists) — never through the descriptor number, which a
  parallel test can re-adopt after a close and make `fcntl(F_GETFD)`
  lie. That fd-number race produced one real flake before the
  rewrite; the peer-observable form is deterministic. The same suite
  caught a real bug during development (`get_mut` instead of `entry`
  silently disabled parking for every first-connection-per-backend).
- `tests/pool_idle.rs` (3 e2e tests): a real proxy against a real
  backend, read back through the admin `/metrics` an operator
  already has open — three sequential requests ride ONE upstream
  connection (dials 1, hits 2, misses 1); past the deadline the pool
  is reaped and the next request dials fresh (used 2, misses 2,
  hits 0); pooling disabled dials every request. The e2e suite
  synchronized on the idle gauge (the park completing is NOT the same
  instant as the response reaching the client), and the backend
  counts connections that CARRIED A REQUEST — the proxy's health
  checker TCP-probes every backend on a fixed 5 s interval, so raw
  accept counts are noisy by design.

**Measurement contract:** the throughput claim (does the tuned
default close the 1.4× gap?) is gated on a quiet window on this host
per docs/benchmarks.md — the ledger row is filled when that run
happens, not before. Runs taken under load are discarded, not
averaged.

**Open:** the health checker TCP-probes every backend every 5 s even
when no `health_path` is configured (documented as "empty disables
HTTP checks" — TCP liveness is the fallback). That surprised this
iteration's test harness and it is worth a `health.enabled` or a
probe-interval knob if a deployment ever needs a fully silent proxy;
parked until then.

Entries are appended per iteration; each one names its sources, the
verdict, the implementation and the proof, so the ledger always
reflects verified state.
