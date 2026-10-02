# HTTP/3 quickstart: serving and dialing QUIC

vane speaks HTTP/3 in two places: as an **edge** (clients dial vane
over QUIC, Alt-Svc advertised automatically) and as an **upstream**
(vane re-originates requests to your backends over QUIC through an
in-process bridge). Both are default features — no extra build flags.

## Serving h3 on a TLS listener

```toml
[[listeners]]
address = "0.0.0.0:8443"

[listeners.tls]
cert = "/etc/vane/cert.pem"
key = "/etc/vane/key.pem"
h3 = true          # serve HTTP/3 over QUIC on the same port (UDP)
```

QUIC rides the same port number as the TCP TLS listener. h2 and h1
continue to work on the TCP side; h3-capable clients discover the
endpoint via Alt-Svc (`alt-svc: h3=":8443"`), which vane injects
automatically on TLS responses when `h3 = true`.

### mTLS over QUIC (mesh callers)

`client_ca` applies to the QUIC listener the same way it applies to
TCP: the handshake requires a client certificate chaining to the CA.
Routes with `allowed_spiffe_prefixes` enforce the caller's SPIFFE ID
exactly as they do on h2 — wrong prefix → `403`, no certificate →
handshake failure. See docs/mesh-mtls-design.md.

## Dialing backends over h3 (`clusters.*.http3`)

```toml
[[listeners]]
address = "0.0.0.0:8080"

[clusters.up]
backends = ["10.0.0.7:443", "10.0.0.8:443"]
http3 = true

[clusters.up.h3_tls]
ca = "/etc/vane/upstream-ca.pem"     # trusts the backend's cert
server_name = "backend.internal"     # SNI for the QUIC handshake
# client_cert = "/run/secrets/svid.pem"   # optional: mTLS backends
# client_key  = "/run/secrets/svid-key.pem"
# alpn = "vane-mesh"                 # default: "h3"

[[routes]]
pattern = "/*rest"
cluster = "up"
```

How it works: vane spawns an in-process loopback bridge per http3
cluster. Your worker speaks plain HTTP/1.1 to it; the bridge
re-originates each request over QUIC/HTTP-3 to the real backends and
translates the response back. Behavior you get for free:

- **Connection reuse** — one QUIC handshake per backend; requests
  multiplex as h3 streams.
- **Health ownership** — the bridge actively probes
  `cluster.health_path` over the pooled connections (every 5 s) and
  routes around dead backends, with one in-bridge retry on the next
  healthy backend. Failure to reach any backend closes the client
  socket abruptly, so the engine's existing failover rules apply.
- **Material rotation** — the bridge fingerprints the `h3_tls` files
  every second and hot-swaps them (re-handshaking the pool). Point
  `client_cert`/`client_key` at SPIFFE SVID files refreshed by the
  Workload API and rotation just works.

### Mesh clusters over h3

Instead of duplicating material into `h3_tls`, let the mesh section
drive the bridge:

```toml
[clusters.up]
backends = ["10.0.0.7:443"]
http3 = true

[clusters.up.mesh]
cert = "/run/vane/svid.pem"
key = "/run/vane/svid-key.pem"
ca = "/etc/vane/mesh-ca.pem"
server_name = "peer.mesh"
spiffe_prefix = "spiffe://example.org/vane/"
http3 = true
```

With `mesh.http3 = true` the bridge presents the SVID as its client
certificate, trusts the mesh CA, and speaks ALPN `vane-mesh`. The
SVID watcher's rotations propagate to the bridge automatically.
(`mesh.http3` and `h3_tls` are mutually exclusive — mesh owns the
material.)

## Request limits and behavior notes

- The bridge's loopback h1 side accepts Content-Length-delimited or
  bodyless requests; `Transfer-Encoding: chunked` requests are
  declined with `501` (h3 has no chunked framing).
- Backend selection inside the bridge is health-aware round-robin;
  the engine's p2c policy and outlier ejection apply to the bridge
  address, not to individual h3 backends.
- Engine-side active health checks are skipped for http3 clusters
  (they would probe the bridge, not the backends) — the bridge's own
  probes replace them.
- Response bodies are fully buffered per request by the bridge
  (bounded by the engine's body limits).

## Conformance

- h2: `scripts/h2spec_run.sh` — 145/145 pass, 0 failed (146 cases, 1
  skipped: TLS-only). Runs vane in strict mode
  (`[http2] strict_idle_window_update = true`, which rejects
  idle-stream WINDOW_UPDATEs per RFC 7540 §5.1; the default build is
  lenient because the h2 crate client grants stream credit early).
- h3: `scripts/h3spec_run.sh` — see docs/h3-design.md for the
  calibration against the h3 crate's reference server.
