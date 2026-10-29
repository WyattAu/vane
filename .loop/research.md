# Iteration 1 — research phase findings + pending mutation

## Research findings (complete)

The TLS-h2 body-as-head desync: root cause localized, not yet
pinned to a single line.

### What the instrumented run shows (vane_dbg, this tree)

- Every shim response completes: each sid's final QBFDBG take is
  `sent=65536 last=true`, `held=0` — no stalled sids in the trace.
- 676 park attempts ALL carried unread data (39–57 KB) and the
  park guard closed every one — zero pool poisoning via the pool.
- 338 `[malformed]` reads remain: 4096-byte reads of pure body
  ('x') hitting `parse_upstream_head` in `AwaitingHead` state,
  each on `ufd=Some(49)` with `unread_on_upstream=39424–57436` —
  the upstream fd holds a PARTIALLY-CONSUMED response.

### The mechanism (consistent with all evidence)

The session reset (check_done keep-alive branch, proxy.rs:3535)
runs while the upstream is mid-response: the shim holds body
bytes awaiting client credit that h2load's h2-crate client does
not grant in time. The park guard (FIONREAD > 0 → close) now
prevents pool poisoning — which is why malformed dropped from
5608 to ~130-197/run — but the connection close itself races the
upstream's in-flight send: bytes keep landing in the fd's
receive queue after the close, the fd number is reaped and
re-adopted by a new connection, and a stale 4096-byte body read
surfaces as the next session's "head". curl never hits this
because nghttp2 grants credit continuously (no stall, no abort,
no mid-response close).

### Open question for the fix

Whether the correct fix is (a) at the park/abort site: close the
upstream and WAIT for the peer's FIN before releasing the fd
number (the fd-number reuse is the poison vector), or (b) at the
shim: don't reset until the upstream is quiescent (all reads
drained), or (c) both. The research favors (a): the desync
survives the pool guard, so the fd-number lifecycle is the
remaining vector.

## Pending mutation (deferred by the proof-loop)

`git commit` + `git push` of the working tree:
- crates/vane/src/proxy.rs — the malformed probe gains
  `ufd=Some(49)` fd attribution (the research instrument)
- The commit message: "debug(proxy): the malformed-parse probe
  gains the upstream fd (correlates stale-body reads with
  parked/checked-out connections)"

The human (or the validate phase) runs:
  git add crates/vane/src/proxy.rs
  git commit -m "debug(proxy): malformed-parse probe gains upstream fd"
  git push origin main

## Iteration goal (unchanged)

Fix the desync per .loop/plan.md: mechanism → pinning test → fix
→ zero `[malformed]` parses under h2load's cadence → ladder → CI.

## Tree-state warning (shared workspace)

The working tree lost two uncommitted pieces in the shared-tree
churn (the other agent's `git reset` + restarts):
- the checkout-side FIONREAD gate in take_pooled (proxy.rs)
- the CHECKOUT GATE ledger entry

Both are recoverable: commit 2d062f25 (on this machine's reflog)
contains the otel stack dedupe + vane-control/router bumps; the
checkout gate needs re-adding (it is small — see the iteration-1
diff in the session history). The implement phase of this
iteration must re-apply:
1. `git show 2d062f25 -- Cargo.toml crates/vane-control/Cargo.toml
   crates/vane-router/Cargo.toml Cargo.lock` — or re-run the bumps
2. re-add the take_pooled checkout gate (FIONREAD > 0 → close)
3. the .cargo/audit.toml zstd register (verify: committed in
   735afc0f's tree? `git show 735afc0f:.cargo/audit.toml`)

Verify after every write (the fleet tree flaps).

## RESEARCH COMPLETE — the mechanism pinned (2026-10-27)

The state-probe run (h2=true, resp_head_len=0, unread=57436,
len=4096, prefix='x'×80) pins the mechanism as a **deadlock
between vane's upstream-read throttle and the client's flow
control**:

1. Response 1 (64 KiB) streams: vane reads ~8 KB from the
   upstream, relays via the shim, and the shim HOLDS 57 KB
   awaiting client window credit (correct h2 backpressure).
2. Vane's upstream read STOPS here (the h2-session re-arm gate
   withholds the read while the shim holds — the exact location
   is the remaining unknown: worker.rs arm gates reference
   pending_down/splice, and the h2 held-based gate lives either
   in the SendCredit handling or the shim's take_held path).
3. h2load's h2-crate client consumes the 8 KB received and
   grants +64 KiB of WINDOW_UPDATE — but vane never re-arms the
   upstream read to consume them (the resume path from client
   credit to upstream re-arm is missing or gated by the same
   stall). DEADLOCK: the client waits for data; vane waits to
   send the held data it believes the client cannot take.
4. h2load's 5 s wall deadline drops the connection; vane sees
   downstream close → closes the session; the upstream fd (57 KB
   unread) reaches check_done's park with the guard closing it
   (post-fix) or poisoning the pool (pre-fix — the malformed
   cascade).

### Fix direction (implementation phase)

Find the h2-session upstream-read re-arm gate and make the resume
fire on client credit: the WINDOW_UPDATE handling (take_held /
SendCredit event) must re-arm the upstream read when the shim's
held queue drains. The h1 path's re-arm (worker.rs ~1044/1163,
pending_down-based) is the pattern to mirror for the shim-held
state.

### File:line citations

- crates/vane/src/proxy.rs:1092-1106 — the [malformed] probe
  (fd, unread, h2, resp_head_len, prefix) — the evidence site
- crates/vane/src/proxy.rs:1097-1111 — the Malformed arm
  (respond 502 + close, the churn close)
- crates/vane/src/h2_server.rs:640-648 — the QBFDBG take probe
- crates/vane/src/h2_server.rs:506-540 — queue_body (the
  credit-capped emit + held fallback)
- crates/vane/src/h2_server.rs:545-570 — take_held (the
  WINDOW_UPDATE resume)
- crates/vane-kernel/src/worker.rs:1044,1148,1163 — the h1
  upstream-read throttle gates (the h2 analog is MISSING)
- crates/vane/tests/h2_server_protocol_edges.rs:302 — the
  overlapping-stream test (the queue contract)
