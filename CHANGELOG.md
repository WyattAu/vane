# Changelog

All notable changes to vane are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
semver.

## [Unreleased]

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
