# Performance research arc: 2026-10
the keep-alive leg; that gap is the arc's first target, and every
## Iteration 1: upstream connection reuse
Linux's accept skew makes the busy worker busier; their fix was
1. **No reuse telemetry.** Nothing measured hits, misses or dials -
   on ONE downstream connection; the case that cannot miss; and
   that checks it out pays a failed attach plus a fresh dial; the
  ; but expired entries below were never reclaimed until a checkout
trigger for revisiting is now measurable; if
  (gauge); the reuse ratio is visible in production, per worker.
- `Drop for HttpProxy` closes every parked descriptor; parked fds
**Lean proof (all clock-free; `pool_idle_ms` is the only time
  other end exists); never through the descriptor number, which a
  already has open; three sequential requests ride ONE upstream
  counts connections that CARRIED A REQUEST; the proxy's health
v0.13.0 at 88.8k and 91.1k; the block-to-block difference is host
logged 1,236,283 hits vs 94,394 misses/dials; a 92.9% reuse ratio,
HTTP checks"; TCP liveness is the fallback). That surprised this
## Iteration 2: adaptive concurrency (overload shedding)
source: exponential-average baseline; deliberately NOT a minimum,
**Analysis:** vane had two blunt overload instruments; the circuit
(shared-nothing like the pool; no atomics on the request path), and
  return) plus the session reset; a leak would shed forever once the
run never recovered; the limit stayed at the floor through 160
algorithm; the release instant is the one instant guaranteed to
**Operational notes:** the limit is per worker; read the gauge as
## Iteration 3: the large-body relay (IN PROGRESS)
response, each a userland copy plus an engine completion; the
the per-send notification cost dominates; the Go proxy case study -
   the previous write completes; halves the round trips without
discarded; load 32, build storm, the discipline working as
  2 x buffer_size, so response relay is not lockstep; a 64 KiB body
  construction; which is also why candidate 2 (read-ahead) is not
offset (0x1b0049; the memmove page) at **14.82% + 14.51% = 29.3%
deficit; this is the profile evidence the verdict needed.
~1.9%; the upstream head build allocates.)
- a ~25% swing that load average alone did not predict. Cross-window
**Verdict: ADOPT candidate 1; socket-to-socket splice for eligible
**Implementation ATTEMPTED AND ROLLED BACK (2026-10-18); the honest
   completions. The new body-splice ops landed exactly in that trap -
   content; duplicated prefix, missing tail) and the truncated
the fast path was rebuilt with the two obvious fixes; a queue-
data). Result: the happy path now moves the right COUNT of bytes -
local port); and the client still starves: FIONREAD = 0 on its
write; the client receives the head and then NOTHING; no tail, no
test passes; it runs with force_mio = true, which is why it never
never reached; the client hangs on the NORMAL relay path first.
repro's failure led to a REAL second bug on main; the
- a test that also got hardened: its `total > 100` throughput floor
became a ran-at-all sanity check (the reload contract; ZERO
failures across the live swap; is unchanged), because a storm-
   truncation the session closed via close(2); but an io_uring op
   shutdown(SHUT_RDWR)s both sockets BEFORE closing; shutdown
   (downstream_eof_write) instead of hard-closing; the existing
cleared, and the session leaked; the "client starves after the
`remaining=512, moved=512`; the accounting visible immediately at
**Status (2026-10-21, 5ef7d119):** main is fully green; ci +
on-CPU time is GONE; nothing above 1.4%; the profile is flat
feature joins the vane-proxy default set; every plain-TCP
paths/headers only, body filters do not exist; no further guards
not the Default impl; the config-level default-on needs an
twice; load 4-7 with WARNs on every leg after a 90-load storm,
0.12.0, vane-proxy 0.15.0; install-verified
**NEW FINDING; ROOT-CAUSED and FIXED: `workers = 0` duplicated
Root cause: `workers = 0` meant "cores PER LISTENER"; the bench
the control experiment; two PLAIN listeners reproduce the drop
profile were all measured inside this oversubscribed context -
worker pool owning ALL listeners; the kernel loop already
since 2e94fe03 and the gated assertion passes every run; the
**TLS-h2 concurrency; ROOT-CAUSED and FIXED (2026-10-22/23):
1. **`max_concurrent_streams: 1` hardcoded in H2Server::new** -
   256 (the RFC default); the shim's new pending queue serializes
2. **The shim refused overlaps at its own layer too**; replaced
   buffer up to 1 MiB (REFUSED_STREAM beyond; clients retry).
**SEPARATE REGRESSION (not ours; the otelkit-2 migration, found
carries a RANDOM trace id; the incoming `traceparent` (4bf92f35-
and ZERO handler closes; the poisoned-pool cascade is dead. The
every shape; scoped to the h2load client (its reconnect churn
binaries; kill by ss -tlnp port owner and verify the port is
   `response_headers`; constructible_struct_adds_field), plus a
   via otlp 0.32); vane's direct opentelemetry/tracing-otel deps
read_err) shows: (a) every shim response COMPLETES; each sid's
closed by the guard; zero pool poisoning; (c) 338 malformed reads
client credit on stalled sessions; correct backpressure against
vane's held-drain resumes on real WINDOW_UPDATEs; if it does,
25 ms, p99 42 ms, zero failures; the TLS-h2 relay is CORRECT and
surfacing) is the gate for TLS-h2 numbers; tracked as the next
after every fix in this arc**; h2load TLS-h2 64 KiB still churns
serial 30/30, 8-way concurrent 80/80, reconnect-storm 30/30 -
bytes arrive; the framing was clobbered between transactions.
upstream read racing; the shim holds bytes on client credit and
literals shift the shared dynamic table; use without-indexing
clean-window full run of the fixed build; vane 0.15.0 wins every
across 54 legs. get64k: 101.2k vs Traefik 56.8k (1.78x); the leg
head (61 B) and ZERO body bytes; the queued body write is dropped
flushing); and the downstream socket close is unreliable on the
direct socket writes) and the fast path re-proven; the mechanism
The loss is therefore confirmed to live in the WORKER INTEGRATION -
empty; an interaction, not a kernel issue. The next attempt should
10 s x 3: 12.4k / 12.5k / 15.3k rps (p50 ~440 us, non-200 = 0) -
(9 -> 12): 34-38k vs the quiet-window 86.7-91.1k; DISCARDED as