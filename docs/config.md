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

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | any host | exact match, no port |
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
| `ring_entries` | int | `4096` | io_uring queue depth |
| `sqpoll` | bool | `false` | zero-syscall submission (needs privileges) |
| `force_mio` | bool | `false` | disable io_uring even when available (e.g. seccomp'd containers) |
| `max_sessions` | int | `16384` | per worker |
| `backlog` | int | `4096` | accept backlog |
| `connect_timeout_ms` | int | `5000` | upstream dial |
| `first_byte_timeout_ms` | int | `30000` | upstream first response byte |
| `idle_timeout_ms` | int | `75000` | keep-alive idle |
| `pool_per_backend` | int | (built-in) | retained idle upstream connections per backend per worker |

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
