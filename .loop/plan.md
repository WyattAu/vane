# Iteration plan (revised after research): fix the TLS-h2 upstream-read resume

## Goal (revised: research completed the diagnosis)

The desync is NOT a framing clobber between transactions: it is a
**deadlock between vane's upstream-read throttle and the client's
flow control** on the TLS-h2 path. Vane reads ~8 KB of a 64 KiB
response, the shim holds the rest awaiting client credit, vane
stops reading the upstream, the client grants WINDOW_UPDATE; and
**the upstream read never re-arms**. The client stalls, h2load's
wall deadline drops the connection, the poisoned upstream fd
cascades (pre-park-guard) and the churn degrades the leg (TLS-h2
~370-4300 rps vs curl clean at every shape).

The single unit for THIS iteration: **make the h2-session upstream
read resume when the shim's held queue drains on client credit** -
mirroring the h1 path's pending_down-based re-arm.

## Phase: research ✓ (complete: see .loop/research.md)

- The mechanism pinned: vane awaits client credit; the client
  waits for data; the upstream-read re-arm never fires for h2
  sessions whose shim holds bytes.
- Fix direction: the WINDOW_UPDATE/take_held resume must re-arm
  the upstream read. Cited file:line for every site.

## Phase: prove

- A regression test at the worker level: an h2 session mid-response
  whose client grants credit must re-arm the upstream read and
  complete the relay. Acceptance: fails on the pre-fix tree
  (the read stays disarmed), passes after. If a pure worker-level
  test cannot reach the shim-held gate, the test is the existing
  h2load-shaped full-stack repro with the read_err count as the
  metric (baseline: ~1088/5 s → target: ~0).

## Phase: implement

- Locate the h2 re-arm gate: either the SendCredit event handler,
  take_held's completion, or the worker's arm_upstream_read for h2
  sessions. Make the resume unconditional on client credit (mirror
  the h1 pending_down gate semantics for the shim-held state).
- Acceptance: `cargo check --workspace` clean, no new clippy
  warnings.

## Phase: test

- The h2load-shaped repro: read_errs ~0 across 3 × 5 s runs
  (baseline ~1088/run).
- `cargo test --workspace` and `--all-features`: 0 failed suites.
- clippy all-features `-D warnings`: clean. fmt: clean.

## Phase: validate

- Ledger updated (the arc's root cause + fix), pushed, CI green.
- Commit+push of the working tree (the checkout gate + the probes
  + this fix); the mutating actions deferred from the research
  phase are included here.

## Done means

The TLS-h2 64 KiB relay completes under h2load's cadence with
~zero read_errs, the fix pinned by a test, main green with CI.
