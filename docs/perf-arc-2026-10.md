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

**Measured (2026-10-17, quiet windows, load 4-7):** c=64 keep-alive,
2 workers, `tools/loadgen`, 10 s x 3 windows, medians: v0.13.0
86.7k rps (p50 129 us, 0 non-200 in 2.6M requests); the published
v0.12.0 binary 89.0k on the same workload. Interleaved re-runs put
v0.13.0 at 88.8k and 91.1k — the block-to-block difference is host
drift, NOT a pool regression; the pool work is throughput-neutral
here and the honest claim is "within run-to-run variance (~3%)".
The iteration's real payload is the telemetry: at c=64 the pool
logged 1,236,283 hits vs 94,394 misses/dials — a 92.9% reuse ratio,
13.6x more reuses than dials, visible for the first time. Little's
law explains why cap 4/worker suffices at this concurrency: 85k rps
x 129 us = ~11 requests in flight, so 8 parked slots plus in-flight
churn covers it. The cap-vs-concurrency question (raise the default?
scale by workers?) is parked with THIS metric as the trigger: touch
the default only when hits/(hits+misses) sags at higher concurrency.
The README's stale comparison table (33-35k vs tuned nginx, a v0.4-era
snapshot) is superseded by docs/benchmarks.md; nginx is not
installable on this host any more, so the historical tuned-nginx row
stands as measured 2026-10-07 and cross-day comparisons carry a
host-drift caveat.

**Open:** the health checker TCP-probes every backend every 5 s even
when no `health_path` is configured (documented as "empty disables
HTTP checks" — TCP liveness is the fallback). That surprised this
iteration's test harness and it is worth a `health.enabled` or a
probe-interval knob if a deployment ever needs a fully silent proxy;
parked until then.

## Iteration 2 — adaptive concurrency (overload shedding)

**Pulled:** Netflix's `concurrency-limits` Gradient2 (the full Java
source: exponential-average baseline — deliberately NOT a minimum,
"RPC methods can be very bursty"; gradient clamped to [0.5, 1.0];
queue allowance; smoothing; the app-limited rule; the fast-recovery
decay when baseline > 2x the latest sample), the Netflix Tech Blog
"Performance Under Load" (gradient = RTT_noload / RTT_actual,
newLimit = currentLimit x gradient + queueSize), Envoy's adaptive-
concurrency filter (the same family; per-worker, latency-window
based), and the USENIX SREcon 2023 adaptive-concurrency survey
(additive increase / multiplicative decrease as the family ancestor).

**Analysis:** vane had two blunt overload instruments — the circuit
breaker (failure-COUNT triggered: it only opens after backends start
FAILING) and GCRA rate limiting (client-keyed, workload-blind). What
it lacked was the middle instrument: shedding load when a cluster is
still succeeding but its latency says it is past capacity. Every
proxy in this family converged on latency-gradient concurrency
limits for exactly that gap. The algorithm is small (one EWMA, one
gradient, one clamp), which is why the verdict is ADOPT rather than
KEEP-and-park: it is ~150 lines, opt-in per cluster, worker-local
(shared-nothing like the pool — no atomics on the request path), and
the floor (`min_limit`) bounds the worst case.

**Verdict: ADOPT (opt-in per cluster), worker-local, Gradient2.**

**Implemented (this iteration):**

- `crates/vane/src/adaptive.rs`: the controller. Baseline = running
  average for 10 samples then exponential decay (600-sample memory);
  gradient clamped to [0.5, 1.0]; queue allowance; smoothing; floor
  and ceiling; the fast-recovery baseline decay; the app-limited rule.
- `runtime` wiring: `[clusters.<name>.adaptive]` (deny_unknown_
  fields, Netflix defaults), shed = 503 "overloaded" BEFORE dialing,
  per-cluster `vane_cluster_<name>_overload_shed_total` counter and
  `vane_cluster_<name>_concurrency_limit` gauge.
- Slot lifecycle: `Conn.inflight` is `take()`-based and released on
  EVERY terminal path (access emit runs before the access-log early
  return) plus the session reset — a leak would shed forever once the
  count stuck at the limit.
- Rejected requests never sample: shedding cannot poison the
  baseline (Netflix's own rule).

**The bug the lean proof caught (worth recording):** the first e2e
run never recovered — the limit stayed at the floor through 160
healthy requests. The probe trace showed `update_limit` ran ONCE in
the whole test: the app-limited rule sampled the in-flight count
AFTER the slot was released, and on a fast cluster completions
rarely overlap enough to clear `limit / 2`, so almost every update
was skipped. Netflix passes the PRE-release in-flight count for
exactly this reason; with that ordering the limit recovered 2 → 7
and shedding stopped. A post-release sample is a different
algorithm — the release instant is the one instant guaranteed to
understate concurrency.

**Lean proof:**

- 8 clock-free unit tests on the controller: steady traffic never
  sheds and growth converges (app-limited); a 4x collapse sheds to
  the floor; recovery grows back bounded; one outlier cannot halve
  the limit; an idle cluster stops growing; sustained overload does
  not raise the limit; enter/exit bookkeeping is exact; 2000
  pseudo-random latencies stay within [min, max].
- 1 e2e (`tests/adaptive_concurrency.rs`): a switchable-delay
  backend, four concurrent clients. Healthy → zero sheds. Collapsed
  (300 ms/responses) → sheds while the admitted requests still ride
  (shedding is never total). Recovered → the ramp grows the limit,
  the tail serves all four clients with the shed counter FROZEN.

**Operational notes:** the limit is per worker — read the gauge as
"per worker" and multiply by workers. Cluster config is
startup-bound (like the pool); xDS-driven adaptive changes are
parked behind the same route-table trigger as the other cluster
knobs. Retry-After on the 503 is parked with the response-"why"
headers item from the previous arc.

Entries are appended per iteration; each one names its sources, the
verdict, the implementation and the proof, so the ledger always
reflects verified state.


## Iteration 3 — the large-body relay (IN PROGRESS)

**The deficit:** the 2026-10-07 comparison's one losing leg: GET
64 KiB, vane 15.3k rps vs Traefik 19.5k (0.78x). A 64 KiB body
through a 4096-byte slot means 16 read+write round trips per
response, each a userland copy plus an engine completion — the
leg where per-byte overhead, not per-request overhead, dominates.

**Pulled:** the splice/sendfile literature (kernel-internals.org's
splice survey: MSG_ZEROCOPY breaks even around 10-100 KB, below that
the per-send notification cost dominates; the Go proxy case study —
io.CopyBuffer's default 32 KiB chunks cost "24x the fast path" in
syscall wall time, and a socket-to-socket splice collapsed 10,677
read/write pairs into splice calls at 70 us each; the 2013
"Rethinking Sendfile Support in Unix Kernel" paper; Netflix's CDN
finding that sendfile took memcpy from 15% to 2% of CPU), plus
in-repo prior art: `vane-kernel::splice::pump` already implements
the socket->pipe->socket splice loop with SPLICE_F_NONBLOCK EAGAIN
handling for the L4 mode.

**Candidate designs (verdict pending the profile):**

1. **Socket-to-socket splice fast path** for responses where BOTH
   legs are plain TCP, the response head declares identity
   Content-Length (no gzip, no chunked scanning), and no wasm body
   inspection is configured. The kernel moves bytes through a pipe;
   userland sees one completion per pipe-buffer (64 KiB) instead of
   sixteen slot round trips. Metrics survive: splice(2) returns the
   moved count. TLS legs, compression, and body inspection stay on
   the copied path by construction.
2. **Pipelined relay read-ahead**: submit the next slot read before
   the previous write completes — halves the round trips without
   splice, keeps body inspection, costs a second buffer per
   relaying connection (conditional read-ahead once the body proves
   larger than one slot).
3. **Bigger slots** (`buffer_size` already bounds to 65536): the
   documented trade is per-worker memory (pool_slots x buffer_size);
   the 2026-10-07 tuning note quantifies what the knob buys.

The decision input is a profile of the get64k leg, not taste: if the
time is in the copies, splice wins; if it is in completion-handling
round trips, read-ahead wins even for TLS legs. Base get64k windows
are queued on the next quiet window (the first attempt was fully
discarded — load 32, build storm, the discipline working as
designed).

**Engine findings from the code walk (the implementation map):**

- The io_uring engine ALREADY pipelines: `arm_upstream_read`
  throttles upstream reads only while `pending_down` exceeds
  2 x buffer_size, so response relay is not lockstep — a 64 KiB body
  is ~16 read+write pairs with the writes queued behind each other.
  The per-chunk cost is handler dispatch + write submission, not
  waiting.
- True-splice machinery exists for L4 mode: `engine.splice_pump`
  arms readiness on both fds and the worker issues splice(2) in the
  read dispatchers (`splice_forward_inflight` is the registered-
  buffer read-forward variant). What is MISSING for HTTP: a byte CAP
  (the L4 pump runs to EOF; an HTTP body must stop exactly at
  Content-Length, which splice(2)'s len parameter gives directly)
  and transaction-completion plumbing (the completion path must run
  access emit, the adaptive release, and the pool park when the cap
  is reached, and error the transaction if upstream EOF arrives
  short).
- Eligibility inventory for candidate 1: plain TCP both legs, identity
  Content-Length response, no gzip, no wasm body inspection, not h2
  (framing). TLS legs and everything else stay on the copied path by
  construction — which is also why candidate 2 (read-ahead) is not
  redundant even if candidate 1 wins: only candidate 2 helps TLS.

**Profiled (2026-10-17 21:32Z, load 7, perf record at 999 Hz during
the get64k leg):** the top TWO symbols are both libc at the same
offset (0x1b0049 — the memmove page) at **14.82% + 14.51% = 29.3%
of on-CPU time**: the 64 KiB body being copied through userland,
sixteen times per response. Everything else is far behind
(on_upstream_data_h1 4.8% across workers, h1_plain_request_data
3.4%, check_done 2.1%, malloc ~1.8%, vdso clock_gettime 2.5%,
httparse 1.4%, io_uring submit/poll ~2.3%). The copies ARE the
deficit — this is the profile evidence the verdict needed.
(Also visible for a later allocation tranche: on_upstream_connected
~1.9% — the upstream head build allocates.)

And a measurement-hygiene data point from the same hour: the
identical binary measured 12.4-15.3k rps at "load 7 with finishing
builds in the ps output" and 17.3-18.9k rps at true quiet (load 5-6)
— a ~25% swing that load average alone did not predict. Cross-window
comparisons on this host need equal-quiet windows, and the earlier
12.4k median was NOT the binary's number.

**Verdict: ADOPT candidate 1 — socket-to-socket splice for eligible
response bodies** (plain TCP both legs, identity Content-Length, no
gzip, no wasm body inspection, not h2). It targets the 29.3%
directly; read-ahead cannot (it still copies). Implementation is
queued with the full map above: byte-capped readiness splice in the
worker (`splice_remaining` in the slab; splice(2)'s len parameter
IS the cap), completion event to the handler on reaching zero
(access emit, adaptive release, pool park, keep-alive continuation),
short-EOF is a 502, behind an opt-in flag until measured — the same
dark-launch posture pool_per_backend had before load_pooled
validated it. Candidate 2 (read-ahead) stays OPEN as the TLS-leg
follow-up.**

**Base windows (2026-10-17 20:47Z, load 7):** get64k c=8, 2 workers,
10 s x 3: 12.4k / 12.5k / 15.3k rps (p50 ~440 us, non-200 = 0) —
today's median 12.5k against the 2026-10-07 record's 15.3k and
Traefik's 19.5k; the deficit reproduces and is the iteration's
target. A c=64 recheck started at load 7 but ran into rising load
(9 -> 12): 34-38k vs the quiet-window 86.7-91.1k — DISCARDED as
storm-contaminated, exactly what the >= 15 threshold plus trend
discrimination is for; the reuse ratio held at 85.5% even there
(449k hits / 76k dials). The perf profile of the leg is queued on
the next quiet window and decides splice vs read-ahead.
