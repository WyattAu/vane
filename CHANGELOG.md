## [0.15.0]: 2026-10-21
default-on; get64k at 2.52× median with the profile flat where the
  seam for the fast path; request the splice, get the completion
  kills-the-session, re-entry/parking rejections; compiled only
  leaked; the "client starves after the first chunk" symptom that
  without closing the socket; no FIN, client hangs forever.
  default ON via an explicit serde default fn; `#[serde(default)]`
## [0.14.0]: 2026-10-18
what a performance experiment did to the public API; exactly what
  to `Op::Splice`; a latent trap where any new operation's
  docs/perf-arc-2026-10.md, iteration 3; the lean proofs refused
  `#[non_exhaustive]`; downstream `match`es over `Op` need a
## [0.13.0]: 2026-10-17
  reaped; a connection the backend has already retired (nginx
  503 `overloaded` when in-flight requests reach the current limit -
  eat ~289 of 290 updates on a fast cluster; a limit that never
- **A dropped worker no longer leaks pooled descriptors**; parked
  constructible; it is the programmatic embedder API; and gains a
  for every backend's first connection; pooling would have done
## [0.12.0]: 2026-10-17
  actually carries attacker-visible plaintext. Off by default; the
  by `""`; two such clusters collapsed onto one key, and the
  `traceparent`, but the access log emitted a freshly generated id -
above), the zero-copy path (KEEP; registered fixed buffers + splice
## [0.11.0]: 2026-10-16
  whole; a segmentation-dependent verdict, i.e. a request-smuggling
  connection CLOSES; request side, inline-body side, and the
  response; the Apache CVE-2024-27316 lesson); a CONTINUATION larger
  minute ceiling, and workflow_dispatch; and its first completed run
## [0.10.1]: 2026-10-14
  `obtain_certificate()` unconditionally; and `renew_at_fraction`,
## [0.10.0]: 2026-10-13
  connection to the address it carries; X-Forwarded-For, access
    header (health checks) claims no address; the real socket peer is
  assignment; struct expressions are refused cross-crate by design.
## [0.9.0]: 2026-10-11
workspace train; it is the crate that changed).
was not coverage-chasing; the dark lines were hiding the defects
  writes the case never occurs; CI's slower runners coalesce segments,
  and closed without answering; the most common "backend is gone"
  signature; counted as nothing; resets, dial failures and timeouts
  to stderr; leftover instrumentation from the previous release's
  window; the 5-failure-streak rule is off; with the gate shared, a
  flows, down windows yield gateway errors, recovery happens); called
  §6.5.2; the setting is a bool); previously every value was silently
  accessors only); an earlier variant addition was correctly rejected
  `deny_unknown_fields`; a typo like `challenge = "http-01"` (the
  protected headers the way a real CA would. No docker, ~0.03 s; the
  EDS endpoint overrides, SDS secrets, and the NACK branch; the loop
  the same way; all spec/bench artifacts now live under
## [0.8.0]: 2026-10-07
  in release a wrap to a *valid but wrong* range; the decoder would
- **DER SAN walk slice overrun** (`vane-tls`). `spiffe_id`; which
  parses the peer-presented leaf certificate during the TLS handshake -
  (`vane-control`). `namespaces = ["*"]` (or an empty list); the
  documented default in vane's Helm chart; was forwarded as a literal
- `h3_client` was compiled even without the `h3` feature; the
  slot round trips per response): +30% median throughput at 16384 -
  now has the same stalled-reader proof the h1 relay has; 8 MiB
- **`tools/loadgen`**; the benchmark load generator now lives in-repo
## [0.7.0]: 2026-10-06
  untouched; the browser is the enforcement point, so the common
- `vane_proto::chunked::ChunkedScanner`; incremental chunked-transfer
- `vane_router::CorsPolicy`; a resolved CORS policy (new public type).
  backed up, and nothing ever resumed them; the throttle was a one-way
  head on the same connection; a desync, and a smuggling vector when
  with the connection close; a broken stream. The same path also never
- **An h2 upstream response never terminated the downstream stream** -
## [0.6.1]: 2026-10-05 (vane-proxy)
- Release hygiene: this release corrects the version ordering; a
## [0.6.0]: 2026-10-04 (vane-control, vane-router, vane-proxy)
  path as xDS; no restart. An invalid config keeps the previous
- `[[routes]] retry`; `max_attempts` and `retry_5xx`. A 5xx retry
- `mirror = "shadow-cluster"`; a fire-and-forget copy of each request
## [0.5.7]: 2026-10-05, superseded
## [0.5.6]: 2026-10-04 (vane-proxy)
## [0.5.5]: 2026-10-04 (vane-proxy)
## [0.5.4]: 2026-10-03 (vane-proto, vane-proxy)
## [0.5.3]: 2026-10-03
## [0.5.2]: 2026-10-03 (vane-proxy)
## [0.5.1]: 2026-10-03 (vane-control, vane-proxy)
## [0.5.0]: 2026-10-02
## [0.4.1]: 2026-10-02
## [0.4.0]: 2026-10-02
## [0.3.1]: 2026-09-29 (vane-proxy)
## [0.3.0]: 2026-09-28
- **HTTP/3 promoted out of experimental; the `h3` feature is now
  survives; previously it tore the whole connection down. h2spec
  pre-bound listener's TLS slot live; certificate rotation via
  upstream; documented in `docs/h3-design.md`).
## [0.2.3]: 2026-09-26 (vane-proxy only)
  content-length mismatch); worklist in `docs/h2-conformance.md`.
## [0.2.2]: 2026-09-25 (vane-proxy only)
## [0.2.1]: 2026-09-21
### Fixed: xDS interop (found by the go-control-plane interop test)
  worked against self-encoded fixtures with invented field numbers -
  with `400`; `Content-Length` + `Transfer-Encoding` together,
  identities; the proxy presents a SVID, requires the backend to
  re-materializes the PEM cache on every agent push; rotation with
  END_STREAM on HEADERS; gRPC trailer relays complete again.
## [0.1.0]: 2026-09-09