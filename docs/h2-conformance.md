# HTTP/2 conformance (h2spec v2.6.0)

Run: `bash scripts/h2spec_target.sh` against a vane h2c listener
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
