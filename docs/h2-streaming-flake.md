# TLS h2 streaming flake: evidence dossier
quarantined the last two tests; the gRPC relay regression was the
slower than vane writes for 8 KiB worth of data; a mobile link, a GC
## Residual known issue: WRITE_PENDING_CAP starvation truncation: FIXED
## Suite-sequence wedge: FIXED (2026-09-19, subprocess isolation)
ignored; the remaining ignore is the retired REUSEPORT probe).
## Suite-sequence wedge: pre-fix investigation (superseded)
* Reproduces on a **quiet** box (load 0.2, zero sibling agents) -
  (`ss -itm`: Recv-Q/Send-Q 0); the stall is userspace scheduling,
can also wedge tests through CPU/lock contention; standalone runs
  **byte-perfect**; 247 structurally valid frames, contiguous `i % 251`
  mode): in failing runs the client's byte stream contained -
with stall-probe PING records landing inside the skipped gap; the
worker write-queue / engine-write interaction; a stale or duplicate write
   `client_view.bin` (frame-subset walk; see session transcript for the
#[ignore]d): the h2 crate client, one TLS connection, sequential
clears `active_stream` when the response completes; but stream 3's
**FIXED (2026-10-03)**; three stacked bugs, found via the shim trace
   END_STREAM; streams stuck HalfClosedRemote (open) forever. The
## RESOLVED (2026-10-09): h2up POST body: two defects, one ours
   request-body TAIL to the h2 upstream as raw h1 bytes; no h2up
   blocked on `body.data()` inside the loop; tokio-h2 dispatches DATA