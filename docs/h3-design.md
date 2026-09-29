# h3 + QUIC design (v0.7.0 target)

## Decision: quinn for QUIC, vane-tls for the handshake

h3 requires a QUIC transport with TLS 1.3 integration, 0-RTT, connection
migration, and datagram extension support. Building that inside vane-tls is
a multi-year project (congestion control, loss recovery, qlog) that buys no
architectural advantage: QUIC sits *below* our framing boundary the same way
TCP does.

**Adopt `quinn` as a vane-core transport peer to mio/io_uring:**

```
QuinnEndpoint (worker-owned)
  └── quinn::Connection (one per QUIC connection)
        └── h3::server::Connection (h3 crate)
              └── request streams → H2Server shim (REUSES the h2 shim:
                  h3 HEADERS/DATA semantics map 1:1 onto our engine events)
```

- The h3 crate (`h3`, `h3-quinn`) speaks the same `Event` model as our
  engine's h2: Headers/Data/Trailers. The existing `H2Server` shim is
  reused nearly verbatim; only the transport plumbing differs.
- **Flow control**: h3 flow control is per-stream + connection, same
  shape as h2 — `release_capacity`/`consume_send_budget` carry over.
- **Zero-copy**: quinn gives `Read`/chunks API; splice does not apply
  (userspace crypto), but our 16 KB slot model holds.
- **Feature gate**: `h3` feature off by default; listener config `h3 =
  true` enables QUIC on a UDP port alongside the TCP listener.
- **Alt-Svc**: TCP listeners advertise `alt-svc: h3=":443"` when an h3
  listener shares the route table (config flag).

## Risks

- quinn pulls tokio; the engine integration must own the endpoint poll
  loop (quinn's `poll`-style API, not its async facade) or run a dedicated
  runtime thread bridging CQEs into the worker — document the bridge.
- 0-RTT replay semantics: reject 0-RTT on non-idempotent methods.

## Milestones

1. ✅ QUIC transport bridge: quinn endpoint on the tokio control
   plane (`h3_edge::spawn`), UDP socket sharing the TLS listener's
   port number, h3 ALPN from the same rustls material.
2. ✅ h3 server on the shared edge context (route snapshot + filters +
   balancer + reqwest upstream clients, mirroring `h2_edge`);
   roundtrip e2e in `tests/h3_edge.rs` (quinn+h3 client → edge → h1
   upstream).
   **Conformance baseline (2026-09-25, h3spec v0.1.13)**: QUIC/TLS
   handshake green (0-RTT CRYPTO case passes); 48 of 49 cases fail —
   **calibrated against the h3 crate's own example server, which
   fails identically (48/49)**: the failures are h3spec's
   Haskell-QUIC client vs quinn interop quirks, not vane-specific.
   Our edge matches the reference implementation's behavior under
   the tool. Revisit if/when h3spec-quic/quinn interop improves;
   the harness is `scripts/h3spec_run.sh` (point it at any server).

   Root cause narrowed (wire-traced): the control-stream violations
   surface via `accept()`/resolve errors carrying an h3 `Code`, but
   `serve_request` returns silently on request-stream errors instead
   of resetting the stream (RequestStream::stop_sending) or closing
   the connection with the code; `h3` 0.0.8's `Code` exposes no
   numeric getter, so the close path needs a Code→u64 mapping table
   (or the `i-implement-a-third-party-backend-and-opt-into-breaking-
   changes` feature). ~1 focused session.
3. ✅ Alt-Svc advertisement: `tls.h3 = true` listeners inject
   `alt-svc: h3=":port"; ma=86400` into h1 response heads
   (`insert_header_once`, idempotent); route parity e2e asserts the
   advertised endpoint serves h3. h2spec-h3 parity remains open.
4. Client-side h3 (H3Upstream) for mesh east-west.

## Milestone 4: client-side H3Upstream — SHIPPED (2026-09-28, loopback-bridge form)

The upstream connector runs on the synchronous mio worker; quinn needs
an async runtime. Three options were considered:

| Option | Mechanism | Rejected/Chosen |
|---|---|---|
| A. quinn on the worker thread | `quinn::Endpoint` with a hand-rolled `quinn::Runtime` that parks the worker | Rejected: the worker's single-threaded completion loop cannot also drive QUIC timers/streams without an inversion of the engine's ownership model (the engine owns the fd set; quinn owns its own sockets) |
| B. dedicated h3 runtime thread | One tokio current-thread runtime per process hosts a shared quinn Endpoint; workers hand requests to it over an SPSC ring, responses stream back over a second ring | Chosen for a first cut: mirrors the in-process SHM sidecar bridge (`sidecar::spawn_bridge`) and keeps workers synchronous; per-request state lives on the h3 thread, workers keep their slot model |
| C. reqwest http3 (experimental feature) | reqwest's `http3` feature on the edge path | Rejected: pulls an unstable reqwest feature and hides the mesh connector (client certs/ALPN `vane-mesh`) behind an opaque stack |

### Shipped form (2026-09-28): the in-process loopback bridge

Option B's ring handoff, simplified to option **B′**: the bridge is an
in-process mini-proxy. The server spawns `h3_bridge::spawn(backends,
tls, timeout)` per `http3 = true` cluster; it binds an ephemeral
loopback TCP port and the cluster's backend list is REWRITTEN to that
address before the workers start — the synchronous mio worker dials a
normal h1 backend and the entire engine (pool, health, failover,
timeouts) works unchanged.

- h1 side: thread-per-connection accept loop; one httparse request
  per connection (CL-delimited or bodyless; chunked → 501).
- h3 side: ONE shared `quinn::Endpoint` on a dedicated runtime thread
  with a per-backend connection pool — one QUIC handshake per
  backend, requests multiplex as h3 streams (`h3_client::exchange`
  on pooled `SendRequest` handles). Health-aware round-robin with a
  single in-bridge retry on the next healthy backend.
- Health: the bridge owns backend health for the cluster — active
  probes (`cluster.health_path`) over the pooled connections every
  `probe_interval` (5 s), plus reactive marking on request/dial
  failure, plus probe-based recovery. Engine-side health probes,
  outlier ejection, and the breaker stay applied to the loopback
  bridge address only (vacuous by construction — skipped for http3
  clusters); the bridge is the owner of that logic in this shape.
- Failure semantics: when no backend answers, the h1 socket closes
  abruptly — the worker sees premature EOF and the existing failover
  rules apply (no synthetic 502 to mask a dead backend).
- TLS: `h3_tls.ca` (server trust), optional `client_cert`/`client_key`
  (mesh SVID for mTLS backends), `alpn` (default `h3`).
- The h3 client carries the authority in the URI (`:authority`); an
  explicit `host` header mismatching the URI host makes the h3 crate
  fail the header build (H3_INTERNAL_ERROR) — that bug class is
  designed out.

Acceptance: `tests/h3_bridge.rs` — h1→bridge→QUIC→vane-h3-edge→h1
roundtrip (200 + body), dead-backend abrupt close, the mTLS
variant (bridge presents a CA-signed SVID against a client-cert-
required h3 backend; anonymous dial rejected), health-aware routing
around a black-holed backend, and connection reuse (server-side
accept counter: 4 requests → exactly 1 QUIC handshake).
`tests/h3_client.rs` un-ignored with a plain-thread harness.

### Future optimization: SPSC rings (the original option B)

The loopback hop costs one memcpy per direction. If profiled as hot,
replace the TCP bridge with the original design: request/response
SPSC rings + eventfd wakeup registered in the worker's poller (the
SHM sidecar pattern). Semantics and config are unchanged — `h3_bridge`
swaps its transport under the same interface.
