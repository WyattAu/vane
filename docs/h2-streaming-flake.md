# TLS h2 streaming flake — evidence dossier

Status: **FULLY RESOLVED**. 9b76d6d fixed the corruption (rustls
writer backpressure silently dropping plaintext); 2746ad2 un-
quarantined the last two tests — the gRPC relay regression was the
c8edae3 send_request(None) semantics change (fixed by the h2up
Some(0) bodyless repair), and chunked_relay's client never flushed
credit-driven pending_writes (a test bug; the relay was correct).

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
