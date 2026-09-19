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
3. Alt-Svc advertisement + h3→h1/h2 route parity tests — parity e2e
   landed; Alt-Svc header injection pending.
4. Client-side h3 (H3Upstream) for mesh east-west.
