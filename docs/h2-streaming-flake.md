# TLS h2 streaming flake — evidence dossier

Status: **FULLY RESOLVED**. 9b76d6d fixed the corruption (rustls
writer backpressure silently dropping plaintext); 2746ad2 un-
quarantined the last two tests — the gRPC relay regression was the
c8edae3 send_request(None) semantics change (fixed by the h2up
Some(0) bodyless repair), and chunked_relay's client never flushed
credit-driven pending_writes (a test bug; the relay was correct).

## ROOT CAUSE FOUND (2026-10-06): one-way upstream read throttle

`tls_h2upstream_large_body` and `h2c_native_engine_large_body` were
still failing **2 runs in 3 standalone** on an idle host (verified on a
clean tree at `74b616d3`, so not a regression of the CORS/split-head
work). The "large body streaming stall" family that this file tracks
across a dozen earlier attempts finally had its cause.

`WorkerState::arm_upstream_read` throttles itself while the client-bound
write queue is backed up:

```rust
// Read throttling: pause while the client-bound write queue is
// backed up (resumed from the downstream write-completion path).
if s.pending_down.len() > 2 * self.pool.buf_size() {
    return;
}
```

The completion path (`continue_downstream_write`) only resumed the
**client** read, and gated that on `pending_up` draining:

```rust
let drained = self.slab.get(slot)
    .is_some_and(|s| s.pending_up.len() <= 2 * self.pool.buf_size());
if drained { self.arm_downstream_read(slot, generation); }
```

Nothing ever re-armed the *upstream* read, so the throttle was a one-way
door: once `pending_down` crossed the threshold, upstream reads stopped
for the rest of the transaction. The client then waited for the rest of
a body vane would never fetch, until its read timeout fired. The stall
only ends when the client gives up, which is why it looked like
scheduling and why `ss -itm` showed empty kernel queues.

The threshold is `2 × DEFAULT_BUF_SIZE` = **8 KiB**, so the trigger is
not "a 1 MiB response": it is *any* moment where the client reads
slower than vane writes for 8 KiB worth of data — a mobile link, a GC
pause, a busy loop, a cold page cache. The earlier forensics ("the
shim intake trickles 13–17-byte reads", "the response send budget
trickles take=8193 → 4096 → 13") are the same thing seen from the
inside: vane had stopped reading, and the trickle was the few bytes
already queued.

Fixed by resuming upstream reads from the downstream write-completion
path, mirroring the client-read resume:

```rust
let downstream_ready = self.slab.get(slot)
    .is_some_and(|s| s.pending_down.len() <= 2 * self.pool.buf_size());
if downstream_ready { self.arm_upstream_read(slot, generation); }
```

`arm_upstream_read` keeps its own guards (`upstream` still attached, no
read in flight, no splice takeover), so re-arming is safe to do on every
write completion.

Regression coverage: `crates/vane/tests/downstream_backpressure.rs`
stalls the reader for 700 ms mid-response (deterministic) and asserts
the full 4 MiB body arrives with integrity, once and twice over a
keep-alive connection. Both cases fail on the pre-fix worker and pass
after. `tls_h2upstream_large_body` and
`h2c_native_engine_large_body` now pass 6/6 and 4/4 standalone.

Also removed in the same pass: unconditional `eprintln!` debug prints
in the h2 frame hot path (`CRDBG` on every DATA / HEADERS /
WINDOW_UPDATE / connection error), which bypassed the `vane_dbg` feature
gate and took a stderr lock per frame. They now go through
`dbg_trace!`.

## Residual known issue: WRITE_PENDING_CAP starvation truncation — FIXED

The final close path (worker close-on-overflow at 1 MiB under CPU
starvation) is fixed by handler-side backpressure: upstream-bound
bytes defer in `Conn::up_buf` and flush in 512 KiB chunks gated on
`on_upstream_flushed` (worker queue fully drained), so the worker's
close-on-overflow can no longer trip; a 16 MiB runaway guard still
kills stuck streams. `h2crate_client_h2c_large_body` passes 5/5
including under load. `large_body_streams_through_edge` remains
retired (REUSEPORT edge removed by design).

## Suite-sequence wedge — FIXED (2026-09-19, subprocess isolation)

The streaming family now runs the proxy as a **subprocess**
(`spawn_server_subprocess` in tests/h2_upstream.rs, killed on drop):
a leaked in-process server cannot interact with the next test
because nothing leaks. `--test-threads=1` and `=2` both green 2×
consecutively with the family un-quarantined (16 passed / 2
ignored — the remaining ignore is the retired REUSEPORT probe).

The following section records the pre-fix investigation; kept for
the mechanism notes (empty kernel queues, trickle signature) which
still explain WHY leaked servers starve successors.

## Suite-sequence wedge — pre-fix investigation (superseded)

`h2crate_client_h2c_large_body` passes standalone (0.11 s, repeated)
but wedges in-suite, and 7b3994c's backpressure fix did **not**
change that (its quarantine removal was premature; standalone
truncation was the part it fixed). Re-verified today:

* Reproduces on a **quiet** box (load 0.2, zero sibling agents) —
  this is not the sibling-coverage contention seen in September.
* Reproduces with a **two-test pair** (`h1_upstream_default_still_works`
  then the wedge target, `--test-threads=1`): fails after ~330 s;
  standalone passes in 0.11 s.
* Reproduces **identically at f7715c1** (pre-backpressure): 429 s
  failure, same pair. The wedge is orthogonal to the truncation.
* At the stall every TCP socket shows **empty kernel queues**
  (`ss -itm`: Recv-Q/Send-Q 0) — the stall is userspace scheduling,
  not TCP zero-window or loss.
* Proxy-side forensics: the h2 server's response send budget
  trickles (`QBBDBG take=8193 conn=36863 … take=4096 … take=13`),
  i.e. the client's flow-control window stops opening while the
  shim intake trickles 13–17-byte reads.

Prime suspect (unchanged from f7715c1's hypothesis, refined):
in-process server tests spawn `server::run` on their own thread with
`shutdown_after=None`, so **every prior test leaks a full server**
(runtime, mio workers, health-checker thread, listeners). The leaked
health checkers keep probing leaked shims (observed: ~10 piling
ESTAB probe connections). Test isolation fix candidates: (a) give
in-process server tests `shutdown_after` + a join guard, (b) move
the streaming family to subprocess servers, (c) make `run()`
cancellable via a shutdown handle and drop leaked runtimes.

Quarantine: LIFTED for `h2crate_client_h2c_large_body` and
`large_body_streams_native_engine` (and `engine_h2_client_to_h2_upstream`)
via the subprocess fix above. The standalone streaming family remains
a fast additional gate.

Legacy note (superseded context): running the full streaming family
sequentially on a box with concurrent sibling cargo/coverage runs
can also wedge tests through CPU/lock contention — standalone runs
are the reliable gate.

## Signature

~1-in-6 runs of `large_body_streams_native_engine` (TLS + h2-crate client,
1 MiB streamed POST through the echo) fail with the client reporting
`connection error detected: frame with invalid size` (FRAME_SIZE_ERROR) at a
partial byte count. Other runs stall (the ">window trickle") instead.

## What the h2 crate's error means

`h2` maps `LengthDelimitedCodecError` (a received frame header whose length
exceeds its 16 KiB `set_max_frame_size`) to `library_go_away(FRAME_SIZE_ERROR)`
(codec/framed_read.rs `map_err`). So the client genuinely received a
misaligned or oversized byte stream.

## Captured evidence (decrypting wire captures, committed tooling)

* `VANE_WIREPROXY=1` (decrypting TCP proxy): our server's emission was
  **byte-perfect** — 247 structurally valid frames, contiguous `i % 251`
  payload, no oversized frames. But proxy timing masks the bug (runs stall
  instead of corrupting).
* `VANE_WIRETEE=1` (passive tee of the client's decrypted read stream, direct
  mode): in failing runs the client's byte stream contained —
  1. a run where the payload stream was **shifted by exactly −9 bytes**
     (one h2 frame header missing) at the corruption point; and
  2. a run where a **17-byte PING frame (the "VANEPROB" stall probe) was
     spliced INSIDE a DATA frame's declared payload** (payload offset 16348
     of a 16383-byte frame), replacing pattern bytes.
* Backtrace probe at the last plaintext chokepoint (`raw_downstream`): the
  response (and the injected PING's sibling frames) flow through the normal
  `on_upstream_data_h1 → h2_write → h2_flush → raw_downstream` path.
* Frame-subset comparison (shim output vs client view): in healthy runs the
  client stream is an exact in-order subset of shim output; in the corrupted
  run the divergence is inside a single DATA frame's payload.

## RESOLUTION (9b76d6d)

`tls.writer().write_all(batch)` with batch > rustls' send buffer:
rustls' Writer applies backpressure (write() -> Ok(0)) once its send
buffer fills, so write_all failed with WriteZero **after a partial
consume**, and the ignored `let _ =` silently dropped the remaining
plaintext. Captured failing runs show exact-byte skips (69 B / 35 B)
with stall-probe PING records landing inside the skipped gap — the
client parsed a misaligned stream as FRAME_SIZE_ERROR. The all-inline
write-queue trace (zero parks/resumes) exonerated the engine write
queue.

Fix: feed plaintext in 16 KiB pieces, drain ciphertext between pieces,
treat any writer error as fatal. All three TLS write sites fixed.

## Original interpretation (superseded)

The corruption exists in the server's TLS plaintext (TLS cannot lose or
splice bytes silently), assembled between the shim's per-frame `Vec<u8>`
emission (atomic, verified) and the rustls writer. Prime suspect: the
worker write-queue / engine-write interaction — a stale or duplicate write
completion popping an unwritten queue entry would release a slot whose bytes
are then lost or overwritten by a later write (matching both observed
anomalies: short holes and foreign-frame splices).

An attempt to guard the queue with per-entry sequence numbers in the token
aux field stopped the corruption but introduced a slow-trickle regression
(aux interacts with the upstream epoch and possibly other semantics); the
experiment was reverted. Resume with that idea, but audit every token-aux
consumer first (deadline `reason_aux`, upstream epoch, splice direction,
accept listener index).

## Resume checklist

1. `VANE_WIRETEE=1 cargo test -p vane --features h2 --test h2_upstream
   large_body_streams_native_engine -- --include-ignored --nocapture`
2. On failure, diff `/tmp/opencode/wire/server_shim.bin` against
   `client_view.bin` (frame-subset walk — see session transcript for the
   script).
3. Instrument the worker write-queue (pop/submit log with seq numbers) and
   the mio engine's park/resume path (`try_write_now`, EPOLLOUT dispatch).

## Tracked (2026-10-03): h2 shim refuses the second sequential stream

Repro: `h2_edge_h2crate_client_sequential_streams` (full_stack_inproc,
#[ignore]d) — the h2 crate client, one TLS connection, sequential
request streams (h2load's exact pattern): request 1 (stream 1)
completes 200; request 2 (stream 3) is refused with REFUSED_STREAM.
The external symptom: h2load against the h2 edge stalls at ~2
requests (0 req/s in the comparative run).

The shim serializes transactions (`max_concurrent_streams = 1`) and
clears `active_stream` when the response completes — but stream 3's
HEADERS still see the slot held. A CRDBG trace of the stall showed
stream 3's HEADERS (10-byte, complete) processed twice. Suspects: the
slot release racing the next HEADERS frame, or a duplicate HEADERS
dispatch in the shim intake.

**FIXED (2026-10-03)** — three stacked bugs, found via the shim trace
(vane_dbg) with the deterministic repro:

1. **Kernel concurrency check counted closed-but-unpruned streams**
   (`streams.len() >= max_concurrent`): after stream 1 closed (still
   resident in the map), every later stream was REFUSED. Now counts
   open streams only, with closed-stream pruning at HEADERS time.
2. **Kernel/shim state divergence**: the shim emits raw frames outside
   the kernel's send API, so the kernel never saw the server's
   END_STREAM — streams stuck HalfClosedRemote (open) forever. The
   shim now syncs via `conn.mark_stream_closed` at every completion
   site.
3. **`resp_done` never reset between serialized transactions**: after
   response 1, every later response hit the `resp_done` short-circuit
   and was silently dropped (0 frames to the client).

The repro is un-ignored and green (50 sequential h2-crate streams);
h2spec stays 145/145 strict; the external h2load went 1 → 32.7k
req/s (8 conns).

## RESOLVED (2026-10-09): h2up POST body — two defects, one ours

The POST-through-h2up 504 decomposed into two independent pieces:

1. **Real relay bug (FIXED)**: the streaming-body path sent the
   request-body TAIL to the h2 upstream as raw h1 bytes — no h2up
   translation after the head-parse-time inline body. The h2 parser
   desynced and the upstream waited forever. Fixed by routing through
   `h2up.request_body`; the intake also flushes credit-driven frames
   now (WINDOW_UPDATE retries queued inside handle_read were never
   written upstream).

2. **Mock bug (the red herring)**: the tokio-h2 mock's accept loop
   blocked on `body.data()` inside the loop — tokio-h2 dispatches DATA
   frames to streams only while the CONNECTION is polled, so a body
   arriving in a later segment than the head never dispatched. Vane's
   wire bytes were byte-identical in pass and fail runs (verified with
   an UPFLUSH hex-dump instrumentation); the mock now spawns per-request
   handlers so the accept loop keeps polling. Lesson: before blaming
   the wire, prove the peer's event loop is being driven.

`post_body_round_trips_through_h2_framing` is now a full regression
test (ungated): 4.8 KiB POST, byte-exact echo through the engine's h2
framing.
