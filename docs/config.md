# vane configuration reference (`vane.toml`)

Validate any file with `vane validate -c <path>` — it runs the same
parse + semantic checks the binary does at startup. All sections are
optional except at least one `[[listeners]]` (the binary exits 1 with
"no listeners configured" otherwise).

## Minimal example

```toml
[[listeners]]
address = "0.0.0.0:8080"

[clusters.up]
backends = ["10.0.0.11:8000", "10.0.0.12:8000"]

[[routes]]
pattern = "/*rest"
cluster = "up"
```

## `[[listeners]]` — ingress sockets

| Field | Type | Default | Notes |
|---|---|---|---|
| `address` | string | — (required) | `ip:port`, e.g. `0.0.0.0:8080` |
| `mode` | `"http"` \| `"tcp"` | `"http"` | `tcp` = L4 splice passthrough (kernel zero-copy, no HTTP parsing) |
| `workers` | int | `0` (one per core) | threads pinned per listener |
| `tls` | table | absent | plaintext when absent |

`[listeners.tls]`:

| Field | Type | Notes |
|---|---|---|
| `cert` | path | PEM chain (leaf + intermediates) |
| `key` | path | PEM private key |
| `alpn_h2` | bool | serve h2 on this listener (feature `h2`) — a dedicated acceptor handles h2 while engine workers keep HTTP/1.1 |

## `[clusters.<name>]` — upstreams

| Field | Type | Default | Notes |
|---|---|---|---|
| `backends` | list of string | `[]` | `host:port` — **hostname or IP** (DNS names resolve via the system resolver at control-plane cadence; bare `port` implies `127.0.0.1`) |
| `unix_socket` | path | unset | single-backend UDS |
| `policy` | `p2c` \| `round-robin` \| `least-conn` | `p2c` | load balancing |
| `health_path` | string | unset | active `GET` probe path; empty disables HTTP checks |
| `http2` | bool | `false` | speak HTTP/2 to backends (prior knowledge) — honored by the h2 edge; the engine's h1 pool ignores it |

Clusters with no resolvable backends contribute no routes (no fatal error).

## `[[routes]]` — static routes

```toml
[[routes]]
pattern = "/api/*rest"
cluster = "api"
host = "api.example.com"       # optional host match
methods = ["GET", "POST"]      # optional method filter
strip_prefix = "/api"          # optional prefix strip
priority = 10                  # lower wins on conflicts
allowed_spiffe_prefixes = ["spiffe://example.org/vane/"]
```

`allowed_spiffe_prefixes` enables per-route caller authorization for
mTLS listeners (`listeners.tls.client_ca` set): the request is
answered **403** unless the client certificate's SPIFFE URI SAN
starts with one of the prefixes. Cert-less callers fail at the TLS
handshake (`certificate_required`).

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | any host | exact match, no port |
| `allowed_spiffe_prefixes` | string list | `[]` | caller SPIFFE URI SAN prefixes (mTLS listener only) |
| `pattern` | string | required | `/api/:id`, `/files/*rest` catch-alls |
| `methods` | list | all | uppercased; unmatched method → **405** |
| `cluster` | string | required | target cluster |
| `strip_prefix` | string | unset | removed before forwarding |
| `timeout_ms` | int | runtime default | per-request upstream timeout |
| `priority` | int | `0` | lower wins on pattern conflicts |

Unmatched request → **404**. Dynamic sources (file/docker/Kubernetes
providers, ACME) publish additional routes into the same lock-free table.

## `[admin]` — observability plane

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `true` | |
| `address` | string | `127.0.0.1:9100` | bind address (loopback default; expose deliberately) |
| `auth_token_file` | string | unset | file containing the admin bearer token; when set, every admin request needs `Authorization: Bearer <token>` (constant-time compare) or gets `401`. `VANE_ADMIN_TOKEN` env var overrides the file |

Endpoints: `/healthz` + `/readyz` (probes — `/readyz` answers `503` until
routes are loaded and listeners are bound), `/health` (backend states),
`/metrics` (Prometheus exposition), `/config` (route records). When a
token is configured it gates the entire plane, probes included.

## Providers

| Section | Fields | Notes |
|---|---|---|
| `[file_provider]` | `directory`, `poll_ms` (default 5000, 0 = watch only) | `*.toml` route files |
| `[docker]` | `enabled`, `socket` (default `/var/run/docker.sock`) | label-driven routes |
| `[kubernetes]` | `enabled`, `api_server` (unset = in-cluster discovery), `namespaces` (default `["default"]`, `"*"` = all) | Gateway API HTTPRoutes + Endpoints |

## `[acme]` — certificate management

| Field | Type | Notes |
|---|---|---|
| `directory_url` | URL | ACME v2 directory (Let's Encrypt, pebble in CI) |
| `emails` | list | account contacts |
| `storage_dir` | path | account key + `cert.pem`/`privkey.pem` |
| `insecure_tls` | bool | accept invalid endpoint certs (**pebble/CI only**) |
| `domains` | list of `{domain, challenge}` | `http-01` supported; `tls-alpn-01` rejected at load |

HTTP-01 tokens are served in-process from the shared token map; the
renewal loop polls expiry and hot-swaps TLS material without restart.

## `[sidecar]` — SHM transport

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `false` | co-located services talk to the running proxy over shared memory |
| `base` | string | `/dev/shm/vane-sidecar` | transport files live here |
| `slot_size` | int | `262144` | payload ceiling per message |
| `slots` | int | `16` | slots per direction |

## `[[plugins]]` — Wasm middleware (feature `wasm`)

| Field | Type | Notes |
|---|---|---|
| `path` | path | `.wasm` module, instantiated per worker; see the guest ABI in `vane-plugins` |

## `[runtime]` — engine tuning

| Field | Type | Default | Notes |
|---|---|---|---|
| `pool_slots` | int | `1024` | buffer pool slots per worker |
| `buffer_size` | int | `4096` | slot size in bytes; read/write lengths derive from it and backpressure thresholds scale with it (2×). Per-worker memory = `pool_slots × buffer_size`. Bounded `[512, 65536]` (startup error outside) |
| `ring_entries` | int | `4096` | io_uring queue depth |
| `sqpoll` | bool | `false` | zero-syscall submission (needs privileges) |
| `force_mio` | bool | `false` | disable io_uring even when available (e.g. seccomp'd containers) |
| `max_sessions` | int | `16384` | per worker |
| `backlog` | int | `4096` | accept backlog |
| `connect_timeout_ms` | int | `5000` | upstream dial |
| `first_byte_timeout_ms` | int | `30000` | upstream first response byte |
| `idle_timeout_ms` | int | `75000` | keep-alive idle |
| `pool_per_backend` | int | `4` | retained idle upstream connections per backend per worker |

## `[access_log]` — request logging

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `false` | one JSON line per completed request |
| `path` | path | stderr | append-only file when set |

Fields per line: `ts_ns`, `duration_us`, `worker`, `status`,
`bytes_out`, `client`, `upstream` (null for locally-answered requests),
`method`, `host`, `path`, `trace_id` (when W3C context propagated).
A full ring drops records (counted) rather than blocking workers.

## `[rate_limit]` — process-wide rate limiting

One GCRA bucket shared by **all** workers: the configured rate is the
true aggregate, not per-worker. Over-budget requests are rejected
`429` before routing.

| field | type | default | notes |
|---|---|---|---|
| `rps` | int | — | sustained requests/second (aggregate) |
| `burst` | int | `100` | instantaneous allowance |

Absent section = unlimited.

## `[jwt]` — bearer authentication

Requests to edge listeners must carry `Authorization: Bearer <jwt>`;
failures are `401`. Checked before routing.

| field | type | notes |
|---|---|---|
| `jwks_path` | string | JWKS file (RS256/ES256, `kid`-matched) |
| `secret_path` | string | HMAC secret file (HS256; trailing newline trimmed) |
| `issuer` | string | required `iss` claim (optional) |
| `audience` | string | required `aud` claim (optional) |

Either material file is required. The file is reloaded when its mtime
changes — rotate keys without restart.

## cluster `compression` — response gzip

```toml
[clusters.api]
backends = ["127.0.0.1:9001"]
compression = true
```

When the client sends `Accept-Encoding: gzip` and the upstream response
is a compressible type (text/*, JSON, XML, JS, SVG, wasm) without an
existing encoding, the body is gzipped and relayed chunked
(`Content-Encoding: gzip`, `Transfer-Encoding: chunked`). Otherwise
responses pass through verbatim. HTTP/2 (engine path) responses are not
compressed in 0.2.

## cluster `outlier` — ejection

```toml
[clusters.api.outlier]
consecutive_failures = 5   # dial errors or 5xx
ejection_ms = 30000
```

A backend reaching `consecutive_failures` is skipped by the balancer
for `ejection_ms` (it remains the fallback of last resort when nothing
else is live); any non-5xx completion resets its streak. Independent of
the active health checker.

## Listener `h2c` — cleartext HTTP/2

```toml
[[listeners]]
address = "0.0.0.0:8080"
h2c = true
```

Plain listeners with `h2c = true` speak prior-knowledge HTTP/2
(RFC 9113 §3.2: the client sends the 24-byte preface first). For
containers and service meshes that terminate TLS elsewhere. Requires
no TLS material; coexists with plain HTTP/1.1 on other listeners.

## Listener `tls.h3` — HTTP/3 over QUIC (experimental)

```toml
[[listeners]]
address = "0.0.0.0:8443"

[listeners.tls]
cert = "/etc/vane/certs/tls.pem"
key = "/etc/vane/certs/tls.key"
h3 = true
```

Requires building vane with the `h3` feature. When enabled, vane
binds a UDP socket on the same port number as the TCP TLS listener
and serves HTTP/3 (RFC 9114) over QUIC: the request shares the same
route table, filter chain, and upstream clients as the TCP path.
TCP responses advertise the endpoint via `alt-svc: h3=":port";
ma=86400` (RFC 7838). Status: experimental — settings/flow-control
parity with h2 is not yet exhaustive.

## `vane gateway-operator` — Gateway API dynamic config

Watches Kubernetes Gateway API resources (Gateways + HTTPRoutes,
`gateway.networking.k8s.io/v1`) in one namespace, compiles them into
xDS snapshots, and POSTs them to a vane admin plane
(`POST /xds/snapshot` — atomic router swap, errors leave the live
table untouched).

```bash
vane gateway-operator \
  --api-server https://kubernetes.default.svc \
  --namespace default \
  --token-path /var/run/secrets/kubernetes.io/serviceaccount/token \
  --admin http://vane-admin:7900 \
  --poll-secs 5
```

Backend refs resolve to `{name}.{namespace}.svc:{port}` (kube-dns).
Weighted refs become weighted backends. TLS listeners map to secret-
mount convention paths (`/etc/vane/certs/{secret}`). v0.2 is
poll-based (no watch API) and single-namespace; the Helm chart ships
a `gatewayOperator.enabled=true` deployment + read-only RBAC.

## `[http2]` — protocol tuning

```toml
[http2]
strict_idle_window_update = false   # default: lenient
```

`strict_idle_window_update = true` rejects WINDOW_UPDATE frames on
streams the peer never opened (RFC 7540 §5.1 idle state →
PROTOCOL_ERROR connection error), matching h2spec. The default is
lenient because the h2 crate client legitimately grants stream credit
before its HEADERS land — strict mode is for conformance runs
(`scripts/h2spec_run.sh`, 145/145).

## cluster `http3` — upstream over QUIC (bridge)

```toml
[clusters.up]
backends = ["10.0.0.7:443"]
http3 = true

[clusters.up.h3_tls]
ca = "/etc/vane/upstream-ca.pem"    # trusts the backend cert
server_name = "backend.internal"    # SNI for the handshake
# client_cert = "/run/secrets/svid.pem"   # optional mTLS SVID
# client_key  = "/run/secrets/svid-key.pem"
# alpn = "vane-mesh"                # default "h3"
```

The engine dials a loopback bridge that re-originates requests over
QUIC/HTTP-3: one QUIC handshake per backend (streams multiplex),
health-aware round-robin with active `health_path` probes every 5 s
plus failure-reactive marking, one in-bridge retry on the next
healthy backend, and hot reload of the `h3_tls` files every second
(SVID rotation without restart). Chunked requests are declined with
501; engine-side health probes/outlier ejection are skipped for
http3 clusters (the bridge owns backend health). Full story:
docs/quickstart-h3.md.

## xDS secrets (SDS)

`POST /xds/snapshot` snapshots may carry `secrets` — SDS-decoded TLS
material by name (PEM content, never file paths):

```json
{
  "version": "sds-1",
  "clusters": {},
  "routes": [],
  "listeners": [ { "address": "0.0.0.0:8443", "secret": "edge" } ],
  "secrets": { "edge": { "cert": "-----BEGIN...", "key": "-----BEGIN...", "ca": "-----BEGIN..." } }
}
```

A listener entry with `secret` serves the control-plane-delivered
certificate live (same hot-swap as file-based rotation); without
`secret`, `cert`/`key` are file paths as before. `vane xds-client`
subscribes SDS and carries secrets in its snapshots. Listener and
secret entries persist across xds-client republishes (empty
listeners = keep stored; secrets union).

## Identity propagation + chaining

Routes with `allowed_spiffe_prefixes` authorize the caller's verified
SPIFFE ID (mTLS ingress on h2 or h3; wrong prefix → 403, no cert →
handshake failure). The verified ID is forwarded upstream as
`X-Vane-Spiffe-Id`; inbound values of that header are stripped, so
only vane-verified identities flow. See docs/quickstart-h3.md.

## cluster `mesh` — mTLS upstreams with SPIFFE identities

```toml
[clusters.mesh-up]
backends = ["10.0.4.7:8080"]

[clusters.mesh-up.mesh]
cert = "/etc/vane/spiffe/cert.pem"     # client SVID chain
key = "/etc/vane/spiffe/key.pem"       # client SVID key
ca = "/etc/vane/spiffe/bundle.pem"     # mesh CA bundle
server_name = "mesh.local"             # SNI for the upstream
spiffe_prefix = "spiffe://example.org/vane/"
http3 = true                          # optional: mesh over QUIC
```

`http3 = true` originates this cluster over the QUIC bridge: the SVID
is the client certificate, `ca` the trust anchor, ALPN `vane-mesh`.
Mutually exclusive with `h3_tls`. Upstream connections dial with
mutual TLS (ALPN `vane-mesh`): the
proxy presents its SVID, requires the backend's certificate to chain
to `ca`, and enforces the backend's SPIFFE URI SAN against
`spiffe_prefix` after the handshake. Verification failure (or any TLS
error) answers **502** while the response is still unsent — mesh is
TLS-or-nothing, never a plaintext fallback. Identities are re-read
from disk per dial, so rotation is: replace the files (atomically),
next dial presents the new SVID.

### Workload API source (SPIRE)

```toml
[clusters.mesh-up.mesh]
svid_socket = "/run/spire/agent-sockets/workload_api.spiffe.io"
cert = "/var/lib/vane/svid/cert.pem"   # materialized by vane
key = "/var/lib/vane/svid/key.pem"
ca = "/var/lib/vane/svid/ca.pem"
server_name = "mesh.local"
spiffe_prefix = "spiffe://example.org/vane/"
```

With `svid_socket` set, the files become the local cache, not the
source: at startup vane fetches the X.509 SVID from the agent
(`FetchX509SVID` over the Unix socket) and writes `cert`/`key`/`ca`;
a watcher re-materializes on every SVID push from the agent, so
rotation happens with zero restarts. The first fetch is synchronous —
startup fails when the agent is unreachable (mesh is TLS-or-nothing).

## `[[routes]] retry` — per-route retries

```toml
[[routes]]
pattern = "/api/*rest"
cluster = "api"

[routes.retry]
max_attempts = 3        # total dial attempts including the first
retry_5xx = true        # also retry idempotent requests on 5xx
```

Retry policy for this route's cluster. `max_attempts` caps the total
dial attempts (default 3 = 2 retries). `retry_5xx = true` replays the
request through the failover machinery when an upstream answers 5xx —
idempotent bodyless requests only (GET/HEAD/PUT/DELETE), capped by
`max_attempts`. Default (absent): connect failures only, 3 attempts.

## `[[routes]] mirror` — request mirroring (shadow traffic)

```toml
[[routes]]
pattern = "/api/*rest"
cluster = "api"
mirror = "api-shadow"

[clusters.api-shadow]
backends = ["10.0.9.7:8080"]
```

A fire-and-forget copy of each request goes to the named cluster; its
responses are discarded. Mirroring is best-effort: it never delays or
fails the real response, and a down shadow is invisible to callers.
Use for canary validation and load previews.

## `[[routes]] cors` — cross-origin policy

```toml
[[routes]]
pattern = "/api/*rest"
cluster = "api"

[routes.cors]
allow_origins      = ["https://app.example.com", "https://admin.example.com"]
allow_methods      = ["GET", "POST", "DELETE"]   # default: the route's methods
allow_headers      = ["content-type", "authorization"]  # default: reflect
expose_headers     = ["X-Total-Count"]           # default: none
allow_credentials  = false
max_age_secs       = 86400
```

Enforces the Fetch/CORS spec per route. Two behaviors:

**Preflight** — an `OPTIONS` carrying `Access-Control-Request-Method` is
answered at the edge with `204` and the policy's headers. The upstream is
never dialed, so a preflight costs no backend capacity and does not show
up in upstream metrics. A preflight is exempt from the route's `methods`
allowlist: browsers will not send the real request if the preflight
fails, so an allowlist that omitted `OPTIONS` would break every
cross-origin call.

**Actual requests** — proxied normally, then the response headers are
injected into the upstream's response head. vane's `Vary` is *merged*
with the upstream's (`Vary: Accept-Encoding` becomes
`Vary: Accept-Encoding, Origin`) rather than duplicated.

A request with no `Origin`, or an origin outside `allow_origins`, is
relayed **untouched** — the browser is the enforcement point, so vane
neither adds headers nor synthesizes a rejection. A preflight naming a
method or header outside the policy is likewise relayed without
`Access-Control-Allow-Origin`.

`allow_origins = ["*"]` emits `Access-Control-Allow-Origin: *` — unless
`allow_credentials = true`, in which case the wildcard is invalid (the
fetch spec requires a named origin for credentialed requests) and vane
echoes the requesting origin instead.

vane does not strip `Access-Control-*` headers set by the backend: a
service with its own CORS policy keeps it, and vane's header goes after
it.

Edge-generated responses for a request that declares a body (rate limit,
plugin reject, open circuit) close the connection: vane never read the
body, so the stream cannot be resynchronized.

## Config hot-reload

`vane run -c vane.toml` watches the config file. A valid changed
config re-applies routes, clusters, and health through the same
reconciler path as xDS — no restart. Invalid configs keep the
previous generation (logged). Listener changes are startup-bound and
logged as restart-required.
