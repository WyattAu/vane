# Changelog

All notable changes to vane are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
semver.

## [Unreleased]

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
