# TLS h2 streaming flake — evidence dossier

Status: quarantined (`large_body_streams_native_engine` and siblings, c089c9c).
All tooling needed to resume the hunt is committed (16b67f4).

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

## Interpretation

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
