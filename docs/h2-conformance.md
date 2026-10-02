# HTTP/2 conformance (h2spec v2.6.0)

Run: `bash scripts/h2spec_run.sh` against a vane h2c listener in
**strict mode** (`[http2] strict_idle_window_update = true`).
Status 2026-10-02: **145/145 pass, 0 failed** (146 cases — http2 +
hpack — 1 skipped: TLS-only). Full-worklist history below.

Earlier baselines: 2026-09-26 **112/138** (25 failures, one class:
missing protocol validations in the native h2 shim); 2026-09-27
**137/138** (worklist complete except the lenient idle-stream
WINDOW_UPDATE divergence — now the `[http2]
strict_idle_window_update` knob: strict mode rejects per RFC 7540
§5.1, the default build stays lenient because the h2 crate client
grants stream credit before its HEADERS land); 2026-10-02 the run
widened to all specs (http2 + hpack) and exposed a hpack gap —
dynamic table size updates after field representations — now
enforced (COMPRESSION_ERROR).

(`--http2-prior-knowledge` client). Status 2026-09-26: **112/138
pass**, 25 failures + 1 skip — all one class: missing protocol
validations in the native h2 shim (`vane-core` h2 connection +
`vane::h2_server`). Each item is a validation plus the mandated
error-code emission (FRAME_SIZE_ERROR / PROTOCOL_ERROR /
FLOW_CONTROL_ERROR / REFUSED_STREAM):

1. `SETTINGS_MAX_FRAME_SIZE` enforcement: DATA and HEADERS frames
   above the limit → FRAME_SIZE_ERROR.
2. Idle-stream rules: frames (RST_STREAM, WINDOW_UPDATE, HEADERS,
   DATA, PRIORITY) on an idle stream → PROTOCOL_ERROR (STREAM_STATE
   semantics per RFC 9113 vs 7540 — h2spec 2.6 tests 7540).
3. Half-closed (remote): HEADERS after the request stream closed →
   STREAM_CLOSED.
4. Closed stream: HEADERS on a closed stream → STREAM_CLOSED.
5. PRIORITY: 0x0 stream identifier → PROTOCOL_ERROR; length ≠ 5 →
   FRAME_SIZE_ERROR.
6. SETTINGS_ENABLE_PUSH ≠ 0/1 → PROTOCOL_ERROR.
7. Window overflow: WINDOW_UPDATE pushing the window above 2^31-1
   (connection and stream) → FLOW_CONTROL_ERROR.
8. Trailer validations: pseudo-header fields in trailers, TE with a
   value other than "trailers", uppercase header names,
   second-HEADERS-without-END_STREAM → PROTOCOL_ERROR.
9. Content-Length ≠ DATA payload length (single or summed) →
   PROTOCOL_ERROR.

Also fixed 2026-09-26: the released binary built without the `h2`
feature silently hung on `h2c = true` listeners (the promotion code is
feature-gated) — `h2` is now a default feature and startup refuses
h2c listeners without it.

## Regression watch (2026-09-26): chunked_relay_h2_to_h1 — RESOLVED

The h2 receive-path validations briefly regressed `chunked_relay_h2_to_h1`
(0 of 65536 body bytes relayed). Root cause: the window-overflow
accumulator double-counted the initial 65,535 window — WINDOW_UPDATE
increments extend the window beyond the initial value, so the base for
the sum must be 0, not 65,535. With the 0-based accumulator the relay
passes 3/3 standalone and the suite is stable at 16/16 ×3.

State: chunked_relay and the streaming family are un-quarantined and
green (16 passed / 2 ignored per suite run). The single remaining
h2spec divergence is the lenient idle-stream WINDOW_UPDATE (see above).
