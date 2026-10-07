# Changelog

All notable changes to vane are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
semver.

## [Unreleased]

### Fixed

- **HPACK string-length arithmetic overflow** (`vane-kernel` h2). A
  12-byte crafted header block drove `decode_int` to `u64::MAX - 1`, so
  `len_pos + len` overflowed: a panic in any overflow-checked build, and
  in release a wrap to a *valid but wrong* range — the decoder would
  copy bytes the declared length never described. Found by the 600 s
  nightly `hpack` fuzz run; fixed with checked arithmetic, and the
  minimized input is kept as three unit tests plus a tracked regression
  seed.
- **DER SAN walk slice overrun** (`vane-tls`). `spiffe_id` — which
  parses the peer-presented leaf certificate during the TLS handshake —
  indexed directly at the one step of the DER walk where every sibling
  used `get()`, so a GeneralName with an over-long length panicked.
  Found by a same-class sweep after the HPACK fix. `der_header` now
  rejects any TLV whose declared length exceeds the buffer it was read
  from, which bounds every caller at the source.
- New `cert_san` fuzz target covers the DER walk: it takes fully
  peer-controlled bytes and no target watched it (5.6 M runs clean
  after the fix).

### Added

- **`tools/loadgen`** — the benchmark load generator now lives in-repo
  (own workspace, `publish = false`) instead of an untracked scratch
  crate under `/tmp/opencode`: h1/h2/h3 clients with a shared 13-field
  output contract, `certgen`, and a configurable upstream. Other
  projects can clone it standalone. `scripts/bench.sh` moved off `ab`,
  and `scripts/bench_compare.sh` gained POST 4 KB / GET 64 KB breadth
  legs routed through every compared proxy.

## [0.7.0] — 2026-10-06

Crate versions: `vane-proxy` 0.7.0, `vane-proto` 0.5.5,
`vane-router` 0.6.1, `vane-control` 0.6.1.

### Added

- **Per-route CORS at the edge** (`[[routes]] cors`). Preflights
  (`OPTIONS` + `Access-Control-Request-Method`) are answered at the
  edge with 204 and the upstream is never dialed; a preflight is exempt
  from the route's method allowlist, since a browser will not send the
  real request if the preflight fails. Actual requests are proxied
  normally with the response headers injected into the upstream's head,
  and `Vary` is *merged* with the upstream's own rather than
  duplicated. `allow_origins = ["*"]` emits the wildcard, except with
  `allow_credentials`, where the fetch spec requires a named origin. A
  request with no `Origin`, or one outside the policy, is relayed
  untouched — the browser is the enforcement point, so the common
  non-CORS path allocates nothing. Per-route, and hot-reload applies
  with the rest of the routes.
- `vane_proto::chunked::ChunkedScanner` — incremental chunked-transfer
  decoder (new public module).
- `vane_router::CorsPolicy` — a resolved CORS policy (new public type).

### Fixed

- **A client that read slowly could deadlock the relay permanently.**
  Upstream reads are throttled while the client-bound write queue is
  backed up, and nothing ever resumed them — the throttle was a one-way
  door. Once the queue passed 8 KiB the response simply stopped arriving:
  the client waited for the rest of a body vane would never fetch, until
  its read timeout fired. Any pause long enough to cover 8 KiB of
  traffic triggers it (a mobile link, a GC pause, a busy loop). This is
  the root cause of the "large body streaming stall" family tracked
  across a dozen earlier attempts in `docs/h2-streaming-flake.md`.
- **Chunked framing is now decided by the grammar, not a content scan.**
  Both directions looked for `0\r\n\r\n` inside each read. That
  sequence is legal *inside* chunk data (any base64 blob, a compressed
  payload, text with a NUL) and can straddle two reads. On the response
  side that truncated bodies or hung the relay; on the request side it
  additionally left the remaining bytes to be parsed as the next request
  head on the same connection — a desync, and a smuggling vector when
  the backend frames the same bytes differently.
- **An upstream response head split across TCP segments returned 502.**
  TCP carries no message boundaries, so a valid head prefix is now
  accumulated instead of being treated as a malformed response.
- **Edge-generated responses replayed on the next keep-alive
  transaction.** Intake appends the request head to the connection's
  parse buffer and only the upstream path drains it, so every response
  generated before that point left the head in place: rate-limit and
  breaker short-circuits, plugin rejects, no-healthy-upstream,
  circuit-open and CORS preflights all made the *next* request on that
  connection re-parse the stale head and receive the previous response.
  Edge responses now consume the head; a request that declares a body is
  closed after the reply, since vane never read that body.
- **A gzipped response never terminated an h2 stream.** The head rewrite
  strips `Content-Length` (the compressed length is not known up front),
  so the shim had no way to derive END_STREAM and the response ended
  with the connection close — a broken stream. The same path also never
  accounted for body bytes that arrived with the head, so the gzip
  trailer was dropped and the deflate stream left incomplete.
- **An h2 upstream response never terminated the downstream stream** —
  the same missing-END_STREAM class, because an h2 response is framed by
  h2 rules and normally carries no `Content-Length` for the shim to count
  against.
- **A gzip feed that produced no output was framed as a chunk.**
  `chunk(&[])` is literally the terminal `0\r\n\r\n`, so any response
  whose input deflate held back was truncated mid-body.
- Unconditional `eprintln!` debug prints removed from hot paths: one per
  h2 DATA/HEADERS/WINDOW_UPDATE frame, two per accepted h2c connection,
  two per mirrored request. They now compile out without the `vane_dbg`
  feature instead of taking a stderr lock per frame.

### Tests

- **Read-boundary sweeps.** Every response split point across
  Content-Length, chunked, gzip and an h2 downstream; the same on the
  request side, twice per keep-alive connection; and across the h2
  upstream leg with the h2 backend driven by vane's own server shim.
  Loopback almost always coalesces a small response into one read, so
  every boundary defect above passed the suite indistinguishably from a
  correct relay. Reverting each fix fails a distinct sweep.

## [0.6.1] — 2026-10-05 (vane-proxy)

- Allocation tranche 2: reusable path/host buffers on the relay, so the
  per-request head scratch is allocated once per connection instead of
  once per request (RSS 225 MB → 205 MB on the plain benchmark shape).
- Release hygiene: this release corrects the version ordering — a
  `v0.5.7` tag had been cut after `v0.6.0`, so the tranche above would
  otherwise have shipped under a patch number that postdated a minor.

## [0.6.0] — 2026-10-04 (vane-control, vane-router, vane-proxy)

The adoption release.

- **Config hot-reload.** `vane run` watches the config file (notify,
  with a 30 s poll and an mtime debounce). A valid changed config
  re-applies routes, clusters and health through the same reconciler
  path as xDS — no restart. An invalid config keeps the previous
  generation and is logged. Listener changes remain startup-bound and
  are logged as restart-required.
- `[[routes]] retry` — `max_attempts` and `retry_5xx`. A 5xx retry
  replays the stored request head through the existing failover
  machinery; idempotent bodyless requests only (GET/HEAD/PUT/DELETE),
  capped by `max_attempts`.
- `mirror = "shadow-cluster"` — a fire-and-forget copy of each request
  to a named cluster, on a detached thread. Never delays or fails the
  real response; a down shadow is invisible to callers.

## [0.5.7] — 2026-10-05, superseded

The `v0.5.7` tag was cut on 2026-10-05, after `v0.6.0`, and re-issued
as `v0.6.1`,
which carries the same allocation-tranche work. Kept as a tag because
it was pushed; crates.io only ever saw 0.6.1.

## [0.5.6] — 2026-10-04 (vane-proxy)

- The circuit breaker `Arc` is cached per connection, keyed by cluster,
  instead of being resolved per request (~2.5% relay CPU).

## [0.5.5] — 2026-10-04 (vane-proxy)

- Zero-copy relay dial: the request-head buffer is taken and restored
  rather than cloned per request (+2-4% h1, RSS -4% / -41% depending
  on shape).
- Rate limiting is opt-in. The per-request GCRA is not run unless
  `[rate_limit]` is configured (~2% relay CPU).

## [0.5.4] — 2026-10-03 (vane-proto, vane-proxy)

- Security, from fuzzing: a length-varint overflow in the protobuf
  decoder and an unvalidated status range in the h1 pool response
  parser, both fixed with regression tests. Fuzz coverage extended to
  ten targets, including `h2_conn_server`, `xds_decode` and
  `h1pool_response`.
- `SECURITY.md` published with a reporting policy.

## [0.5.3] — 2026-10-03

- Three stacked h2 bugs refused or dropped every request after the
  first: the kernel counted closed streams without pruning them, the
  shim and kernel diverged on stream state, and `resp_done` was never
  reset. The h2 edge went from 1 to 32.7k req/s; h2spec 145/145 in
  strict mode is preserved.

## [0.5.2] — 2026-10-03 (vane-proxy)

- Pooled h1 upstream client for the h3 edge, shared by the mesh path
  (+85% on the mesh benchmark).

## [0.5.1] — 2026-10-03 (vane-control, vane-proxy)

- Full LDS: listener decode plus the multi-RDS attachment merge.

## [0.5.0] — 2026-10-02

- Mesh operations: bridge metrics, identity chaining.
- SDS: control-plane-delivered TLS secrets.

## [0.4.1] — 2026-10-02

- SPIFFE identity propagation for mesh callers, and a multi-listener h3
  fix.

## [0.4.0] — 2026-10-02

Mesh over QUIC, across all 11 crates: SVID rotation for the h3 bridge,
`mesh.http3`, and SPIFFE enforcement on the h3 edge. h2spec 145/145 in
strict mode.

## [0.3.1] — 2026-09-29 (vane-proxy)

- HTTP/3 upstream bridge: h3 to backends, with a shared endpoint,
  connection reuse and health ownership.

## [0.3.0] — 2026-09-28

### Changed

- **HTTP/3 promoted out of experimental — the `h3` feature is now
  default.** The published binary serves h3 (QUIC) listeners and the
  Alt-Svc advertisement out of the box, matching the h2 promotion in
  0.2.3. Compile it out with `default-features = false` or
  `features = ["h2"]`.

### Fixed

- **h2 stream-level WINDOW_UPDATE overflow is now a stream error**:
  the offending stream is reset with an in-band
  `RST_STREAM(FLOW_CONTROL_ERROR)` (RFC 7540 §6.9) and the connection
  survives — previously it tore the whole connection down. h2spec
  137/138 (the one documented divergence: lenient idle-stream
  WINDOW_UPDATE, required by the h2 crate client's early credit
  grants).
- **chunked_relay_h2_to_h1 un-quarantined**: the window-overflow
  accumulator had double-counted the initial 65,535 window and
  FLOW_CONTROL_ERROR'd the client's legitimate early grant. With the
  0-based accumulator the relay passes and the streaming suite is
  green (16/16 ×3).
- Idle-stream WINDOW_UPDATEs are applied (not rejected) via the
  retired-stream accumulator and merged into the stream window when
  the stream opens.

### Added

- **Snapshot-driven listener TLS rotation (LDS parity, TLS
  dimension).** `XdsSnapshot.listeners` carries
  `{ address, cert, key, client_ca }`; the admin plane swaps the
  pre-bound listener's TLS slot live — certificate rotation via
  control plane, same hot-swap the file watcher performs. An address
  that is not a pre-bound TLS listener is rejected with `400` (xDS
  never binds ports).
- h3spec harness (`scripts/h3spec_run.sh`) + calibration against the
  h3 crate's own reference server (identical 48 error-case failures
  upstream — documented in `docs/h3-design.md`).

## [0.2.3] — 2026-09-26 (vane-proxy only)

### Fixed

- **HTTP/2 is now a default feature.** The 0.2.2 installable binary was
  built without it, and `h2c = true` listeners silently hung: the h2
  promotion code is feature-gated, so connections were accepted and
  then never answered. h2c listeners now work out of the box, and
  startup fails fast with a clear message if the feature is compiled
  out while h2c is configured.
- h2spec v2.6.0 run against the h2c listener: 112/138 pass. The 25
  remaining failures are missing protocol validations (frame-size
  limits, idle/closed-stream rules, pseudo-header rules,
  content-length mismatch) — worklist in `docs/h2-conformance.md`.

## [0.2.2] — 2026-09-25 (vane-proxy only)

### Added

- **EDS endpoint merge** in `vane xds-client`: ClusterLoadAssignment
  resources are decoded (`envoy::decode_cla`) and override the inline
  CDS endpoint sets per cluster name; an EDS update re-publishes the
  snapshot so endpoint drift applies without config changes.
  (vane-proxy 0.2.1 carried only the RDS-after-CDS ordering fix.)
- Stray xds-client debug prints removed.

## [0.2.1] — 2026-09-21

### Fixed — xDS interop (found by the go-control-plane interop test)

- `DiscoveryRequest` wire fields were swapped: `version_info` is field
  1 and `node` is field 2 (envoy/service/discovery/v3/discovery.proto).
  Real management planes saw `node` as empty and never matched the
  snapshot; NACK `error_detail.message` also corrected (field 2 of
  google.rpc.Status, was 3).
- Envoy `Cluster` endpoint decoding: `load_assignment` is field 33 in
  the current v3 API (field 4 is `connect_timeout`),
  `ClusterLoadAssignment.endpoints` = 2,
  `LocalityLbEndpoints.lb_endpoints` = 2, `Address.socket_address` = 1,
  and `SocketAddress.port_value` = 3. The decoder previously only
  worked against self-encoded fixtures with invented field numbers —
  backends decoded as empty against go-control-plane.
- `vane xds-client` now subscribes RDS only after the CDS ACK: real
  management planes answer watches independently, and an RDS response
  arriving before CDS was mapped into a snapshot with an empty cluster
  set that the admin plane rejected.

### Added

- `interop/ads`: a go-control-plane-based ADS management-server
  fixture + local test driving `vane xds-client` against it end to
  end (route live in ~1 s).

### Renamed (crates.io)

- `vane-core` → **`vane-kernel`** and the installable package
  `vane` → **`vane-proxy`**: the crates.io names `vane`/`vane-core`
  are owned by an unrelated project. Library and binary names are
  unchanged (`cargo install vane-proxy` installs `vane`); published
  on crates.io as 0.2.0 alongside the rest of the workspace.

### Security

- Request-smuggling hardening: ambiguous request framing is rejected
  with `400` — `Content-Length` + `Transfer-Encoding` together,
  duplicate `Content-Length` (even self-consistent), multiple
  `Transfer-Encoding` headers, and transfer codings that do not end in
  `chunked` (vane-proto `ParseError::ConflictingFraming`).
- `Transfer-Encoding` is now treated as hop-by-hop on the h1 edge: the
  upstream head re-emits a normalized `Transfer-Encoding: chunked`
  instead of relaying the client's value verbatim.
- Inbound `X-Forwarded-For` / `X-Forwarded-Proto` / `X-Forwarded-Host`
  are stripped before the edge appends its own (spoofed values no
  longer reach upstreams).
- `X-Forwarded-Proto` reflects the terminating listener: `https` on
  TLS listeners (was hardcoded `http`).
- Admin plane: optional bearer-token auth via `[admin]
  auth_token_file` or the `VANE_ADMIN_TOKEN` env var; requests without
  a matching `Authorization: Bearer <token>` get `401` (constant-time
  compare). Default bind remains `127.0.0.1:9100`.
- `/readyz` now reports real readiness (`503` + JSON until routes are
  loaded and listeners are bound); `/healthz` liveness is unchanged.
- Added `SECURITY.md` and `THREAT-MODEL.md` (STRIDE per surface).

### Added

- **Mesh mTLS (docs/mesh-mtls-design.md)**: cluster `mesh` upstream
  identities — the proxy presents a SVID, requires the backend to
  chain to the mesh CA (ALPN `vane-mesh`), and enforces the backend's
  SPIFFE URI SAN prefix after the handshake; verification failure
  answers `502` while the response is unsent (TLS-or-nothing).
- **SPIFFE Workload API source**: cluster `mesh.svid_socket` fetches
  the X.509 SVID from the agent at startup and a watcher
  re-materializes the PEM cache on every agent push — rotation with
  no restarts. First fetch is fail-closed.
- **Per-route caller authorization**: routes gain
  `allowed_spiffe_prefixes`; with `listeners.tls.client_ca` set the
  listener requires client certificates and non-matching callers get
  `403` (missing certs fail the handshake with
  `certificate_required`).
- **xDS control plane (docs/xds-grpc-transport.md)**: hand-rolled
  gRPC/protobuf wire codecs (no prost/tonic), ADS session state
  machine with per-type ACK/NACK, a blocking ADS client over the
  engine's h2, Envoy `Cluster`/`RouteConfiguration` decoding into
  router snapshots, and the `vane xds-client` subcommand publishing
  snapshots to the admin plane. E2E-verified against a fake
  management plane: hand-encoded Envoy resources drive live
  host-scoped routing.
- **Kubernetes**: watch mode and a multi-namespace Gateway API
  operator (`vane gateway-operator`).
- **Compression**: downstream gzip on h2 edges (cluster
  `compression`).
- **Chunked upstreams**: `Transfer-Encoding: chunked` request bodies
  are re-framed and relayed to h2 upstreams.

### Fixed

- TLS streaming corruption: rustls' writer backpressure silently
  dropped plaintext on partial writes (16 KiB record-sized pieces +
  eager ciphertext drain + fatal errors at all TLS write sites).
- Upstream write backpressure: overflow under CPU starvation closed
  sessions mid-stream; upstream-bound bytes now defer and flush in
  512 KiB chunks gated on queue-drain (16 MiB runaway guard).
- h2 upstream: bodyless requests (no/zero `Content-Length`) set
  END_STREAM on HEADERS — gRPC trailer relays complete again.
- TLS listeners: handshake failures now flush rustls' queued alert
  before close (a cert-less mTLS client sees `certificate_required`
  instead of hanging), and early-exit replies (405) close the session
  instead of holding sockets until the idle deadline.

### Documentation

- `docs/h2-streaming-flake.md`: full evidence dossier for the
  streaming corruption + the remaining suite-sequence wedge
  (quiet-box forensics; fix candidates listed).
- `docs/xds-grpc-transport.md`, `docs/mesh-mtls-design.md`,
  `docs/h3-design.md`: designs with milestone status.
- `docs/config.md`: `mesh` (incl. `svid_socket`), `compression`,
  `outlier`, `h2c`, and route `allowed_spiffe_prefixes` reference.

## [0.1.0] — 2026-09-09

First tagged release of the full implementation.

### Engine (vane-core)

- Thread-per-core pinned workers (share-nothing), timer wheel, graceful
  drain with deadline
- `AsyncEngine` completion abstraction with two backends: **io_uring**
  (fixed registered buffers, optional SQPOLL, multishot accept, splice
  readiness via `PollAdd`) and **mio** (edge-triggered epoll/kqueue
  fallback with identical completion semantics)
- Upstream connection lifecycle: dial/connect-completion, detached-fd
  pooling primitives (`attach`/`detach` with epoch invalidation for
  stale completions), graceful half-close, deferred FIN on drain
- Generational session slab, fixed buffer pools (kernel-registered on
  io_uring), cacheline-padded lock-free SPSC command rings (loom
  model-checked), kernel `splice(2)` L4 passthrough
- Handler trait with `DeadlineReason`-aware timers; per-worker
  connection gauges

### Protocol (vane-proto)

- Zero-copy HTTP/1.1 request views (httparse, caller-owned storage),
  keep-alive semantics, per-second cached `Date` header, allocation-free
  response writer

### Routing (vane-router)

- Immutable path-copying radix trie (literal > param > catch-all with
  backtracking), EBR lock-free generation swap via crossbeam-epoch
- P2C / round-robin / least-connections balancers with shared health
  flags that survive config generations

### Filters (vane-filters)

- Monomorphized `Pipeline` composition (no vtable dispatch)
- GCRA rate limiting (throttle-kit), circuit-breaker gate (breaker),
  forwarded headers, request-id built-ins

### Control plane (vane-control)

- Layered TOML config with validation; `VANE_*` environment overrides
- Providers: file (inotify + poll), Docker (label-based), Kubernetes
  Gateway API HTTPRoute (feature `k8s`)
- ACME v2 client: ES256 JWS, HTTP-01, nonce queue + badNonce retry,
  kid via Location header, CSR finalize, renewal loop
- Flap-proof active health checks; reconciliation merges provider
  updates into router generations

### Sidecar (vane-shm, vane-client-sdk)

- POSIX SHM request/response transport over shm-rings SPMC pairs with
  slot arenas (~2.8 µs RTT measured), C ABI (`vane_sidecar.h`)
- Zero-loss hot upgrade: listener fds over SCM_RIGHTS + route state
  archive; standby choreography documented and e2e tested
- In-process sidecar bridge mode

### TLS (vane-tls)

- rustls termination with ALPN (h2/http1.1), ChaCha20-Poly1305 session
  ticket cache persisted across hot upgrades, certificate hot reload

### Edge (vane bin)

- run / validate / sidecar subcommands; TLS + ACME listeners; upstream
  keep-alive pooling; failover; enforced connect/first-byte/idle
  timeouts; access-log drain; per-cluster metrics; admin plane
  (`/metrics`, `/config`, `/health`); WebSocket-ready connection state
- Experimental: HTTP/2 edge acceptor (`h2`), wasm plugin sandbox
  (`wasm`), HTTP/3 scaffolding (`h3`)

### Known limitations

- Upstream HTTP/2 and HTTP/3 (frontend h2/h3 terminate to HTTP/1.1)
- H2 edge buffers request bodies (32 MiB cap)
- wasm plugin ABI is path-gate only (header access planned)
- `pool_per_backend` sustained-load hardening is fresh; regression
  suites pass on mio and io_uring
