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
   upstream). h2spec-h3 parity remains open.
3. ✅ Alt-Svc advertisement: `tls.h3 = true` listeners inject
   `alt-svc: h3=":port"; ma=86400` into h1 response heads
   (`insert_header_once`, idempotent); route parity e2e asserts the
   advertised endpoint serves h3. h2spec-h3 parity remains open.
4. Client-side h3 (H3Upstream) for mesh east-west.

## Milestone 4 design: client-side H3Upstream (pending implementation)

The upstream connector runs on the synchronous mio worker; quinn needs
an async runtime. Three options were considered:

| Option | Mechanism | Rejected/Chosen |
|---|---|---|
| A. quinn on the worker thread | `quinn::Endpoint` with a hand-rolled `quinn::Runtime` that parks the worker | Rejected: the worker's single-threaded completion loop cannot also drive QUIC timers/streams without an inversion of the engine's ownership model (the engine owns the fd set; quinn owns its own sockets) |
| B. dedicated h3 runtime thread | One tokio current-thread runtime per process hosts a shared quinn Endpoint; workers hand requests to it over an SPSC ring, responses stream back over a second ring | Chosen for a first cut: mirrors the in-process SHM sidecar bridge (`sidecar::spawn_bridge`) and keeps workers synchronous; per-request state lives on the h3 thread, workers keep their slot model |
| C. reqwest http3 (experimental feature) | reqwest's `http3` feature on the edge path | Rejected: pulls an unstable reqwest feature and hides the mesh connector (client certs/ALPN `vane-mesh`) behind an opaque stack |

### Plan (option B)

1. `h3_client` thread (tokio, current-thread): owns the quinn Endpoint
   per cluster (`mesh.upstream_h3 = true`), dials with the mesh SVID +
   ALPN `vane-mesh`, and multiplexes request streams.
2. Worker handoff: `upstream_send` on an h3 upstream writes the
   serialized head/body into a bounded SPSC ring keyed by session slot;
   the h3 thread drains it, opens a bidi stream, relays; response bytes
   come back over the response ring and re-enter the worker as
   synthetic `on_upstream_data` events (same shape as the h2up shim).
3. Backpressure: the request ring bound is the h3 upstream's
   `WRITE_PENDING_CAP` analog; response flow control maps h3
   `send_window` updates onto the existing credit path.
4. Failure semantics: h3 dial/response timeout → `upstream_failed`
   (502, failover rules identical to h1/h2 upstreams).
5. Tests: h3-edge-to-h3-edge roundtrip (both directions over QUIC),
   plus an mTLS variant asserting the SPIFFE identity of the h3 peer.

Estimated: ~600–800 LOC for the bridge + client, after the ring
handoff pattern is copied from the sidecar bridge.
