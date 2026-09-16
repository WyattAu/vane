# TLS h2 streaming flake — evidence dossier

Status: **RESOLVED** (9b76d6d). The corruption family (invalid-size +
the >window stalls) was a silent plaintext drop in the TLS write path;
details below. Remaining quarantines: `grpc_trailers_relay_h2_to_h2`
(regression bisected to 4f2db09, mechanism unidentified) and
`chunked_relay_h2_to_h1` (relay lost-wakeup after ~one read buffer —
the ARMD trace shows downstream dispatches cease while data sits in
the socket buffer).

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
