# Handoff: proof-loop iteration 1 (TLS-h2 flow-control deadlock arc)

Stopped at the 180-minute wall-clock limit during iteration 1/4
(plan ✓ → research ✓ → prove/implement partial). This file is the
complete state for the next session.

## The arc in one paragraph

vane's TLS-h2 relay of 64 KiB bodies degrades (TLS-h2 h2load:
~370-4300 rps, read_errs up to 80% pre-fix) because of a
**flow-control deadlock**: vane reads ~8 KB of the response, the
h2 shim holds 57 KB awaiting client window credit, **vane's
upstream read stops and never re-arms when the client grants
credit**. The client waits for data; vane waits to send what it
believes the client cannot take. h2load's 5 s wall deadline drops
the connection; the half-read upstream fd reaches check_done's
park **with 39-57 KB unread**; pre-park-guard it poisoned the pool
(the malformed cascade); post-guard it is closed (correct) but the
stall itself remains.

## Phase states this iteration

- **PLAN; done** (`.loop/plan.md`, revised after research): the
  original framing-clobber theory was invalidated by the FIONREAD
  evidence; the goal is now the upstream-read resume on client
  credit.
- **RESEARCH; done** (`.loop/research.md`): the mechanism pinned
  with file:line citations and a full-run QBFDBG frame trace
  (12,028 frames, /tmp/opencode/vane_qbf.log + qbf_all.txt):
  every sid completes `sent=65536 last=true held=0`; the shim
  and the frame emission are CORRECT; the stall is in the
  upstream-read re-arm for h2 sessions. Key measured facts:
  676 park attempts ALL carried unread data (39-57 KB) before the
  guard; curl is clean in every shape (serial 30/30, 8-way
  concurrent 80/80, reconnect storms 30/30) while h2load churns -
  the trigger correlates with h2load's request cadence.
- **PROVE; partial**: the metric exists (h2load read_err count,
  baseline ~1088/5 s). The deterministic pinning test was NOT
  written; it needs the re-arm gate located first (see pick-up).
- **IMPLEMENT; partial**: the park-time FIONREAD guard was
  written, verified (zero malformed in clean runs), and COMMITTED
  (in 735afc0f's tree; `git show HEAD:crates/vane/src/proxy.rs |
  grep -c "parked with UNREAD"`; wait, see the caveat below).
  The checkout-side gate and the h2 re-arm fix are NOT landed.
- **TEST; partial**: workspace tests green at 19bf9331 (CI
  success); the all-features run had 1 transient failure
  (investigated: two duplicate gate runs collided on ports).
- **VALIDATE; done for the landed work**: CI + interop green on
  19bf9331; the tip's red CI (669d5238+) is the other actor's
  otelkit-2 migration regression (the trace-id loss; documented,
  theirs).

## Landed commits (all pushed, verified)

- `d88a08f7`; worker-budget fix (workers=0 shares the core
  budget; the 5× multi-listener tax gone; RSS halved)
- `159426a0`; request-body credit fix (the Data-arm rewrite had
  dropped WINDOW_UPDATEs; clients stalled at 64 KiB)
- `b19fb3b6`; h2 concurrent streams: the shim pending queue +
  max_concurrent_streams 256 (h2c parallel 6/40 → 40/40)
- `919e33d9`; loadgen --tls + h2load error capture
- `5ec69d66`; vane-proxy 0.16.0 + zstd advisory register
- `735afc0f`; splice-test drain hardening + the otlp full-payload
  dump (the trace-id regression diagnosis for the otelkit-2 owner)
- `19bf9331`; the ledger: the TLS-h2 stall traced to client
  credit (the final diagnosis this iteration)

## Deferred permission-blocked actions

1. **The checkout-side FIONREAD gate; LOST, must be re-written**
   (the working-tree copy was lost in the shared-tree flap; HEAD
   has no "checkout-side" marker). The design: in
   `take_pooled` (crates/vane/src/proxy.rs ~3753), for each
   candidate `conn.fd`: FIONREAD; if unread > 0 → push to
   `reaped` (close) and `continue` to the next candidate. The
   [checkout] vane_dbg log line is optional. Without this gate, a
   pooled fd that grew unread bytes between park and reuse is
   handed out stale.
2. **The malformed-probe fd attribution; IN THE WORKING TREE,
   UNCOMMITTED** (crates/vane/src/proxy.rs: the probe gains
   `ufd=Some(N)`, `unread`, `h2_active`, `resp_head_len` named
   args). Commit message:
   "debug(proxy): the malformed-parse probe gains the upstream fd
   (correlates stale-body reads with parked/checked-out
   connections)".
3. Standard: `git add -A && git commit && git push origin main`.

## What the next session picks up first (in order)

1. **Re-apply the checkout gate** (deferred #1); 15 lines, the
   design is fully specified above.
2. **Locate the h2 upstream-read re-arm gate**; the actual stall
   fix. The worker's arm gates (crates/vane-kernel/src/worker.rs
   ~1021/1044/1148/1163) gate on pending_down/splice/upstream_read
   _inflight; NONE reference the shim's held state. The design
   comment (proxy.rs, the h2 branch of on_upstream_eof): "the h1
   framing arms below would misread a CL body as truncated while
   held bytes await client credit"; the symmetric upstream-side
   resume is missing. Grep: `SendCredit` in proxy.rs (the event
   fired on client credit); the resume must re-arm the upstream
   read from there (or the shim must expose a "held drained"
   signal the worker acts on).
3. **The pinning test** at the located gate: an h2 session mid-
   response, client grants credit, assert the upstream read
   re-arms and the response completes (the QBFDBG probe shape
   works at this layer).
4. **Then**: the h2load client fix (retry REFUSED_STREAM, surface
   driver errors; the error capture is built in), the kit unpin
   (engineering-standards#6), the otelkit-2 trace-id regression
   (theirs), the benchmarks.md TLS-h2 leg.

## Measurement environment traps (read before benchmarking)

- Kill by `ss -tlnp` port owner, verify the port is FREE before
  starting: a stale setsid-detached vane survives kills and
  SO_REUSEPORT load-balances old/new binaries (produced days of
  mixed results).
- /tmp/opencode is wiped periodically: recreate certs/configs,
  verify before use.
- The perf-arc ledger (docs/perf-arc-2026-10.md) is the complete
  record: three root-caused defects, the corrected attributions,
  the measurement traps, and the gate-debt itemization.

## The uncommitted working tree (the human commits this)

`crates/vane/src/proxy.rs` (+38/-4):
1. The take_pooled checkout-side FIONREAD gate (deferred #1) -
   closes pooled connections that grew unread data between park
   and reuse, tries the next candidate.
2. The malformed-probe enrichment: ufd, h2_active, resp_head_len
   in the diagnostic line.

`.loop/`; this proof-loop's state (plan, research, handoff).

Suggested message: "fix(proxy): checkout-side FIONREAD gate + the
TLS-h2 desync instrumentation (the flow-control deadlock arc)".
After committing: push, then CI. The known CI state on the parent
commits: green through 735afc0f; the tip's red runs are the other
actor's otelkit-2 regression (the trace-id loss; documented in
docs/perf-arc-2026-10.md, their migration) plus the shared-runner
flake pattern (exit 101 install steps).
