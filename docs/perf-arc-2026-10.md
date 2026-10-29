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
directly; read-ahead cannot (it still copies).

**Implementation ATTEMPTED AND ROLLED BACK (2026-10-18) — the honest
state of this iteration:** the full fast path was built (worker
readiness-splice with a per-session byte budget, handler
eligibility, completion event, opt-in `response_splice` flag) and
the lean proofs refused to pass. Two findings, both real:

1. **`Token::op()` had a silent catch-all**: the op byte was decoded
   through a match that only knew ops 1..=7 and mapped EVERYTHING
   ELSE to `Op::Splice`. Any future op would silently re-route its
   completions. The new body-splice ops landed exactly in that trap —
   watch completions arrived as `Splice` and were dropped, starving
   the fast path. FIXED: the match is exhaustive, with a comment that
   adding an Op variant requires extending it, and a standalone
   engine test (`watch_fires_on_readability`) proves a watch fires
   TWICE (first readiness + EOF), which is what catches this class.
2. **The relayed body came back corrupted** (right length, wrong
   content — duplicated prefix, missing tail) and the truncated
   case hung. The interaction between the slot-based relay that
   already consumed `rest` and the splice's socket offset is not
   understood well enough to ship; the machinery (worker readiness
   splice, byte budget, completion plumbing) is reverted to keep
   main clean while the failure analysis continues.

**What LANDED from the attempt (all gate-green):** the exhaustive
`Token::op` fix (a latent bug for ANY future op, now provably
round-tripped by test), the `Engine::watch`/`unwatch` primitives
(generic multishot readiness with cancellation filtering) plus the
two-firing contract test, and `ClusterConfig`/`RuntimeConfig`
`#[non_exhaustive]` hardening from the semver gate. The splice
feature itself is DARK: reverted from the tree, design + failure
analysis recorded here, re-entry criteria = explain the offset
corruption with a failing minimal case first.

Candidate 2 (read-ahead) stays OPEN as the TLS-leg follow-up.

**Second attempt + second rollback (2026-10-18, same iteration):**
the fast path was rebuilt with the two obvious fixes — a queue-
drain ordering barrier (the splice arms only after the queued
head/first-read bytes have flushed, since it writes the socket
directly and would jump the queue) and read re-arm gates while a
splice is active (an in-flight upstream read double-delivers; an
in-flight downstream read STEALS the spliced bytes as fake client
data). Result: the happy path now moves the right COUNT of bytes —
the worker trace shows ingress 61506, egress 61506 into the
verified-correct client fd (session peer matches the client's
local port) — and the client still starves: FIONREAD = 0 on its
receive queue, first mismatch never even reached. The splice
reports success; the bytes vanish between the socket accept and
the peer's receive queue. That is an engine-interaction layer this
iteration has not earned the right to debug blindly.

**A PRE-EXISTING MAIN BUG FOUND BY THE REPRO (2026-10-19, top
priority):** while building the standalone repro against clean main
(no body-splice code), the minimal truncated-upstream shape HANGS
THE CLIENT on the io_uring engine: backend sends head (61 B,
Content-Length: 65536, Connection: close) + 8 B body + FIN in one
write; the client receives the head and then NOTHING — no tail, no
FIN, forever (peer stays alive; 10 s read timeout). The mio engine
handles the identical shape correctly (the inproc truncated-body
test passes — it runs with force_mio = true, which is why it never
caught this). Unrelated to the body splice: it reproduces on clean
main. Regression test landed:
`tests/truncated_clean_main.rs`, runtime-gated per the known-broken
discipline (`VANE_KNOWN_BROKEN_TRUNCATED=1` asserts the fix; the
ungated run documents the bug and exits green). This bug also
explains yesterday's truncated e2e failure: the splice path was
never reached — the client hangs on the NORMAL relay path first.

**BONUS FIX FROM THE REPRO (2026-10-19, landed):** the minimal
repro's failure led to a REAL second bug on main — the
`upstream_connected_delivered` idempotence guard (added for the
inline+CQE double-callback) was per-SESSION, but a cluster swap or
failover legitimately re-dials the same address; the guard then
blocked the fresh `do_upstream_connected`, the new upstream's read
slot never armed, and every request on the connection failed. The
flag now resets at all three dial/attach sites. The guard is
per-DIAL because the double-delivery it prevents is per-dial too
(inline Done + CQE + deferred queue). Caught by
`config_hot_reload_under_concurrent_load` failing under storm load
— a test that also got hardened: its `total > 100` throughput floor
became a ran-at-all sanity check (the reload contract — ZERO
failures across the live swap — is unchanged), because a storm-
loaded host completes fewer hammer iterations and that says nothing
about reload correctness.

**ROOT-CAUSED AND FIXED (2026-10-19, late):** the instrumented run
finally captured the full CQE/write sequence, and the bug is exactly
what the ordering evidence pointed at, in TWO halves:

1. **io_uring + close(2) socket-lifetime race:** on the io_uring
   engine, a keep-alive downstream READ is perpetually armed. At
   truncation the session closed via close(2) — but an io_uring op
   still submitted on that socket holds a kernel reference to the
   struct file, so close(2) freed the fd NUMBER without closing the
   SOCKET: no FIN, the in-flight read blocks forever waiting for
   data that never comes, and the client hangs with a live
   connection ("peer=alive (write ok)"). The mio engine cannot hit
   this (readiness polling holds no ops). Fix: close_session now
   shutdown(SHUT_RDWR)s both sockets BEFORE closing — shutdown
   forces every in-flight op to complete/cancel immediately, queued
   send-buffer data still delivers, then the FIN goes out.

2. **The queued body write was dropped:** close_session releases
   wq/pending_down without flushing, so the response tail that rode
   the first read's wake never reached the client (head-only
   delivery). Fix: the truncation path now half-closes
   (downstream_eof_write) instead of hard-closing — the existing
   queue mechanics flush the relayed bytes, then the FIN; the
   client's close completes the session normally.

Both fixed; the gated repro passes (partial body + EOF delivered
correctly), the inproc mio truncated test still passes, and the
door re-opens for the body-splice fast path (its e2e hang was this
same close-semantics bug amplified by the splice's direct socket
writes).

**ROOT-CAUSED and FIXED (2026-10-21):** a byte-accounting bug in
the worker pump, not a kernel mystery. `pump_body_splice`'s egress
loop incremented `moved` but never decremented `remaining`, so the
completion condition (`remaining == 0 && pipe_len == 0`) could
never fire: `finish_body_splice` never ran, the handler never got
`on_body_spliced`, the body-splice-active read gates never
cleared, and the session leaked — the "client starves after the
first chunk" symptom. The SIOCOUTQ-drains / FIONREAD=0 evidence
was this bug wearing a network costume: bytes moved to the wire,
but the session machinery never resumed, so the keep-alive flow
wedged. Found by writing the worker-level unit tests the coverage
gate demanded (`body_splice_worker_tests` in vane-kernel/worker.rs,
cfg-gated on `body-splice`): the first test failed with
`remaining=512, moved=512` — the accounting visible immediately at
this level, invisible at the e2e level. The spliced-body e2e is
UN-gated and passes 5/5 byte-exact (65536 x 3 keep-alive
requests). The fast path remains feature-gated
(`response_splice` -> `body-splice`) pending the quiet-window
get64k benchmark for the default-on decision per the iteration-3
criteria; the code is now proven and pinned by unit + e2e tests.

**Status (2026-10-21, 5ef7d119):** main is fully green — ci +
interop both pass with the machinery compiled in (--all-features
coverage includes the new worker-level unit tests). The splice
fast path is proven, tested at two levels, and one config flag +
one benchmark away from default-on.

**BENCHMARK VERDICT (2026-10-21 03:50Z, load 5-6, no builds in
ps):** get64k c=8, 10 s x 3 interleaved windows, SAME release
binary (`--features h3,response_splice`), config flag the only
difference (scripts/bench_splice_ab.sh):

| arm | median rps | p50 | p99 | non200(max) |
|---|---|---|---|---|
| splice-off | 34 172 | 178 us | 834 us | 0 |
| splice-on | 86 268 | 77 us | 267 us | 0 |

**2.52x median throughput, p50 2.3x lower, p99 3.1x lower, zero
errors in both arms.** Engagement verified by profile (perf record
999 Hz during an on-window): the memmove pair that was 29.3% of
on-CPU time is GONE — nothing above 1.4%; the profile is flat
(worker-loop dispatch noise). 99.4k rps under the profiler.

DECISION: default-on. The config flag defaults to true (vane-control
RuntimeConfig + ProxyConfig), and the `response_splice` cargo
feature joins the vane-proxy default set — every plain-TCP
identity-CL response now splices. The eligibility gate gained the
gzip exclusion the documented contract always promised (relay_body
compresses while CL framing survives, so without the guard the
splice would bypass compression). wasm plugins inspect request
paths/headers only, body filters do not exist — no further guards
needed. TLS, h2, chunked, upgrade/tunnel, keep-alive-close and
mio-engine responses all stay on the copied path by the existing
gates.

Two deployment footguns found and fixed while landing the default:
(1) `#[serde(default)]` on a bool calls `bool::default()` (false),
not the Default impl — the config-level default-on needs an
explicit `#[serde(default = "response_splice_default")]`; caught
live because the first post-flip benchmark window measured the
copied path. (2) the cross-proxy comparison rerun (vane vs
Caddy/Traefik with the splice) was started twice and discarded
twice — load 4-7 with WARNs on every leg after a 90-load storm,
exactly the contamination the methodology exists to filter. The
A/B verdict stands on its own methodology; the cross-proxy table
refresh (docs/benchmarks.md get64k row) is queued on the next
truly-quiet window.

**v0.15.0 PUBLISHED (2026-10-21):** vane-kernel 0.9.0, vane-control
0.12.0, vane-proxy 0.15.0 — install-verified
(`cargo install vane-proxy --version 0.15.0 --locked` →
`vane 0.15.0`), ghcr image building on the tag. The arc's headline:
a two-session "kernel byte-loss mystery" was one missing
decrement, and fixing it bought 2.52× on the exact leg the whole
arc started for.

**NEW FINDING — ROOT-CAUSED and FIXED: `workers = 0` duplicated
the core budget PER LISTENER (2026-10-22).** The cross-proxy
refresh exposed a second deficit hiding inside the historical
get64k numbers. Controlled matrix (same binary, same upstream,
10 s get64k c=8, load ~5):

| config (get64k on the plain listener) | before | after the fix |
|---|---|---|
| 1 listener, `workers = 0` (16 workers) | 85 486 | 85 486 |
| 2 plain listeners, `workers = 0` (32 workers!) | 16 436 | **87 598** |
| plain + TLS + `h3 = true`, `workers = 0` (bench/fleet shape) | 15 590 | **83 475** |

Root cause: `workers = 0` meant "cores PER LISTENER" — the bench
and fleet shape (plain + TLS listeners) spawned 32 io_uring
pollers on 16 cpus, and the oversubscription cost 5x. Attribution
note: the first hypothesis (an h3/quinn idle tax) was DISPROVEN by
the control experiment — two PLAIN listeners reproduce the drop
exactly (16.4k), and h3 = false recovers nothing once the worker
count is fixed. The h3 listener was innocent; it just made the
config multi-listener. The historical 15.3k get64k, the
"Traefik beats vane on get64k" finding, AND the 29.3% memmove
profile were all measured inside this oversubscribed context —
the copies were real but half the story.

The fix (`resolve_workers_per_listener`): `workers = 0` now shares
the core budget across listeners (cores / count, min 1); explicit
per-listener `workers` keeps the legacy all-equal semantics.
Verified end to end: 2 plain listeners auto -> 87.6k (was 16.4k),
bench shape -> 83.5k (was 15.6k). The proper long-term shape (one
worker pool owning ALL listeners — the kernel loop already
multiplexes by listener index) is DEMOTED from the queue: the
8+8 split measured FULL throughput (88.7k two-listener vs 85.5k
single-listener), so the idle-pool overhead the refactor would
remove is ~zero. What remains is architectural tidiness (mixed
Http/L4 listener configs still take the legacy per-listener-pool
path), not performance.

**Truncated repro UNGATED (2026-10-22):** the fix has been in
since 2e94fe03 and the gated assertion passes every run — the
known-broken gate came off truncated_clean_main.rs; the
assertions are unconditional. Zero runtime-gated known-broken
tests remain in the tree.

**TLS-h2 concurrency — ROOT-CAUSED and FIXED (2026-10-22/23):
two stacked causes, one line each.**

1. **`max_concurrent_streams: 1` hardcoded in H2Server::new** —
   the connection layer refused every overlapping client stream
   with RST(REFUSED_STREAM) BEFORE the shim ever saw it. Found by
   frame-tracing a shim-level unit test (the connection consumed
   the second HEADERS and emitted ZERO events; the concurrency
   gate in vane-core's connection.rs writes the RST itself). Fix:
   256 (the RFC default) — the shim's new pending queue serializes
   the actual relays.

2. **The shim refused overlaps at its own layer too** — replaced
   with the pending-request queue (`start_pending` + wired at all
   four completion sites): queued heads replay through the normal
   RequestHead/RequestBody/RequestComplete path, so the handler
   cannot tell them from fresh connections. Overlapping bodies
   buffer up to 1 MiB (REFUSED_STREAM beyond — clients retry).

Verified: h2c parallel 6/40 -> 40/40; TLS-h2 curl serial 30/30
and 8-way concurrent 80/80 (65536 B, HTTP 200 every time). A
shim-level regression test pins queue-and-replay-in-order.

**SEPARATE REGRESSION (not ours — the otelkit-2 migration, found
2026-10-24):** `otlp_export_emits_spans` fails: the exported span
carries a RANDOM trace id — the incoming `traceparent` (4bf92f35-
77b3-4da6-a3ce-929d0e0e4736) is not honored. vane's extraction is
CORRECT (tracing_util unit tests 6/6; serve_span parses and calls
set_remote_parent with the right TraceId). The loss is in the
tracing→OTEL conversion introduced by the coordinated-release
commit (094a5261, otelkit 2.2.3 TelemetryConfig migration): the
span reaches the exporter as a root. Owner: the otelkit migration
author. Full-payload dump wired into the test's failure output
(full_hex) for their debugging.

**PARK GUARD VERIFIED (2026-10-26):** with the FIONREAD park
guard live, a 5 s TLS-h2 h2load run produced ZERO malformed parses
and ZERO handler closes — the poisoned-pool cascade is dead. The
residual h2load read_errs (~7-15% of transactions) happen WITHOUT
any vane-side session error (no close_session, no downstream
error, no truncation) while curl on the same endpoint is clean in
every shape — scoped to the h2load client (its reconnect churn
plus whatever first error triggers it). Until h2load's client is
fixed, TLS-h2 numbers come from curl, not h2load. TEST-ENV TRAP
that produced days of mixed results: a stale setsid-detached vane
survives kills (its listener persists via an inherited fd) and
SO_REUSEPORT load-balances connections across old and new
binaries — kill by ss -tlnp port owner and verify the port is
free before every measurement run.

**THE COORDINATED-RELEASE GATE DEBT (2026-10-26, for the release
owner):** the estate 1.x dep promotions + response-headers feature
(8e036da0/9e672f6e) landed with gate debt that blocks main:
1. semver: vane-control 0.12.2 needs 0.13.0 (RouteConfig gained
   `response_headers` — constructible_struct_adds_field), plus a
   `function_requires_different_generic_type_params` failure from
   the same feature (function TBD, in the kit's newer semver run).
2. deny: opentelemetry DUPLICATES (0.28 tree + otelkit-2's 0.30
   via otlp 0.32) — vane's direct opentelemetry/tracing-otel deps
   need alignment with otelkit-2's stack (a real migration: 0.28
   -> 0.30 API changes in tracing_util/server).
3. vet: resync after each publish (vane-shm 0.5.5, vane-tls 0.6.3,
   vane-proxy 0.15.3 exemptions landed in 8b1061bb).
The fuzz target (RouteBuilder initializer) was fixed in 8b1061bb.
My h2 concurrency + park-guard + credit fixes are IN this tree and
verified (h2c parallel 40/40, TLS curl clean, zero malformed with
the park guard live).

**FLOW-CONTROL TRACED (2026-10-26, QBFDBG full-run):** with the
park guard live, a 5 s TLS-h2 h2load run (23,722 ok / 1088
read_err) shows: (a) every shim response COMPLETES — each sid's
final take is sent=65536 last=true, held=0 throughout; (b) 676
park attempts ALL carried unread data (39-57 KB) and ALL were
closed by the guard — zero pool poisoning; (c) 338 malformed reads
remain: a stalled session's upstream sits with 57 KB unread while
the shim holds bytes awaiting client credit that h2load never
grants (the h2-crate client's capacity release stalls), until
h2load's wall deadline mass-drops everything (the reason=handler
x5933 end-of-run close). The residual read_errs are vane AWAITING
client credit on stalled sessions — correct backpressure against
a client that stops granting. curl never stalls (nghttp2 grants
continuously). NEXT: re-check after an h2load client fix, or
verify with a second independent h2 client (python httpx/h2) that
vane's held-drain resumes on real WINDOW_UPDATEs — if it does,
the tool is the last blocker for TLS-h2 numbers.

**VERDICT (2026-10-27, the independent-client control):** curl-h2
multiplexed on ONE connection: 500/500 = 200 (the full 32.7 MB
through one h2 connection); 200-request parallel batches: p50
25 ms, p99 42 ms, zero failures — the TLS-h2 relay is CORRECT and
sustains multiplexed load. The stall requires h2load's specific
client: its h2-crate driver stalls its capacity release mid-
response (vane holds 57 KB awaiting credit that never comes;
FIONREAD on the upstream confirms vane stopped reading at ~8 KB).
CONCLUSION: vane's relay is correct; h2load's client is the
defective instrument. The tool fix (retry semantics, driver-error
surfacing) is the gate for TLS-h2 numbers — tracked as the next
tool arc. The park guard + checkout gate stay as defense in
depth.
the TLS-h2 body-as-head desync, REPRODUCED ON THE CURRENT BINARY
after every fix in this arc** — h2load TLS-h2 64 KiB still churns
(1317 ok / 5608 read_err in 5 s) and the [malformed] probe fires
5608 times: **4096-byte reads of pure body ('x') hitting
parse_upstream_head in the AwaitingHead state** (buf_size-sized
upstream reads), each closing the connection (proxy.rs:1092,
reason=handler). While curl on the SAME endpoint is clean:
serial 30/30, 8-way concurrent 80/80, reconnect-storm 30/30 —
so the relay serves real clients correctly and the failure
correlates with h2load's request CADENCE (350/s serial on one
connection). The state resets to AwaitingHead while 4096 body
bytes arrive — the framing was clobbered between transactions.
Suspects, in order: (1) the h2 shim's response completion vs the
upstream read racing — the shim holds bytes on client credit and
the session resets when the translator reports done, but the
upstream read loop may deliver one more body-buffering read
after the reset; (2) start_pending's dial racing the parked
upstream's last read. Instrumentation (vane_dbg, in tree):
[close-at] track_caller on SessionIo::close, [malformed]/
[headcap] on the parse arms, [h2err] on the flush close,
[hrdbg]/[qdbg]-class probes removed. Repro (all in /tmp/opencode,
recreate after tmp wipes): certgen DIR; vane TLS listener
(alpn_h2) + plain 64 KiB cluster; `h2load ADDR 1 5 /bench64
localhost DIR/cert.pem`. HPACK harness trap: with-indexing
literals shift the shared dynamic table — use without-indexing
in multi-request tests.

**Cross-proxy refresh LANDED (2026-10-22, 270127e9):** the first
clean-window full run of the fixed build — vane 0.15.0 wins every
leg vs Caddy 2.10.2 and Traefik 3.5.4, median of 3, non-2xx = 0
across 54 legs. get64k: 101.2k vs Traefik 56.8k (1.78x) — the leg
the 0.8.0 table showed vane LOSING (0.78x). Every leg improved
1.9-3.1x over the 0.8.0 table (h1-8 63k -> 179k, h3-8 28k ->
86k, get64k 15.3k -> 101.2k = 6.6x). Published in
docs/benchmarks.md with the two-defect explanation (copies +
oversubscription). The arc is closed: every queued performance
item is either landed, measured into mootness, or refiled as
architecture rather than performance.

Sustained-run decay is also real but smaller: pure get64k
back-to-back decays 86k -> 61-73k over 60 s (boost/thermal), which
is why bench-context numbers (leg position 6 of 6) read lower than
fresh-window numbers.

**Repro hardening (2026-10-19, later):** the gated test now
deterministically characterizes the bug: the client receives the
head (61 B) and ZERO body bytes — the queued body write is dropped
by `close_session` (it releases `wq`/`pending_down` without
flushing) — and the downstream socket close is unreliable on the
io_uring path (FIN either never arrives or only at process
shutdown; runs vary). Both variants fail while the bug lives and
pass once the close flushes queued bytes + FIN. The procfs
diagnostic in the WouldBlock arm identifies the proxy-side socket
by matching `getsockname` against the client's ephemeral port.

**Standing state (updated 2026-10-19):** dark, reverted twice; the
engineering primitives (exhaustive op decode, watch/unwatch, the
two-firing contract test, the splice mechanism proof) are permanent
keepers. With the truncated-hang bug fixed, the body-splice e2e
should be re-added (its hang was this bug amplified by the splice's
direct socket writes) and the fast path re-proven — the mechanism
test already passes and the ordering barrier design is recorded
above.

The loss is therefore confirmed to live in the WORKER INTEGRATION —
the splice coexisting with the session's other state (the still-
armed upstream read the splice races at arm time, the downstream
read that can steal spliced bytes as fake client data, and the
truncated-EOF path where the close raced the client's read). The
second attempt's instrumentation showed both splices succeed into
the verified client fd while the client's receive queue stayed
empty — an interaction, not a kernel issue. The next attempt should
enter the splice through a CLEAN session-mode handoff (upstream
reads disarmed at the engine level before the pipe is created, the
same way L4 mode takes over) instead of gating the existing mixed
mode, and should carry the truncated-EOF e2e from the first
attempt, which exposed a close-vs-read race on the client socket
that the full_stack_inproc truncated-body test does not hit.

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
