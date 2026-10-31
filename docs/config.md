Validate any file with `vane validate -c <path>`; it runs the same
the key named in the error; a typo'd key must never silently no-op.
## `[[listeners]]`: ingress sockets
| `address` | string |; (required) | `ip:port`, e.g. `0.0.0.0:8080` |
| `alpn_h2` | bool | serve h2 on this listener (feature `h2`); a dedicated acceptor handles h2 while engine workers keep HTTP/1.1 |
## `[clusters.<name>]`: upstreams
| `backends` | list of string | `[]` | `host:port`; **hostname or IP** (DNS names resolve via the system resolver at control-plane cadence; bare `port` implies `127.0.0.1`) |
| `http2` | bool | `false` | speak HTTP/2 to backends (prior knowledge); honored by the h2 edge; the engine's h1 pool ignores it |
## `[[routes]]`: static routes
## `[admin]`: observability plane
Endpoints: `/healthz` + `/readyz` (probes; `/readyz` answers `503` until
## `[acme]`: certificate management
within hours to leave real margin; at the 2/3 point the hourly loop
## `[sidecar]`: SHM transport
## `[[plugins]]`: Wasm middleware (feature `wasm`)
## `[runtime]`: engine tuning
| `pool_idle_ms` | int | `30000` | how long a pooled upstream connection stays reusable after it was parked; past this it is reaped instead of handed out (backends enforce their own idle timeouts; nginx `keepalive_timeout` 60 s, most LBs 60 s; so a stale parked connection would cost the next request a failed attach plus a fresh dial) |
### `clusters.<name>.adaptive`: adaptive concurrency limiting
| `min_limit` | int | `20` | floor; prevents death spirals |
| `max_limit` | int | `200` | ceiling; the gradient never grows past this |
## `[access_log]`: request logging
## `[rate_limit]`: process-wide rate limiting
| `rps` | int | - | sustained requests/second (aggregate) |
## `[jwt]`: bearer authentication
changes; rotate keys without restart.
## cluster `compression`: response gzip
## cluster `outlier`: ejection
## Listener `h2c`: cleartext HTTP/2
## Listener `proxy_protocol`: client IP behind an L4 load balancer
connections; each is detected on its own connection.
  valid header; a plain request, a garbage header; is closed with no
## Listener `tls.h3`: HTTP/3 over QUIC (experimental)
ma=86400` (RFC 7838). Status: experimental; settings/flow-control
## `vane gateway-operator`: Gateway API dynamic config
(`POST /xds/snapshot`; atomic router swap, errors leave the live
## `[http2]`: protocol tuning
before its HEADERS land; strict mode is for conformance runs
## cluster `http3`: upstream over QUIC (bridge)
`POST /xds/snapshot` snapshots may carry `secrets`; SDS-decoded TLS
## cluster `mesh`: mTLS upstreams with SPIFFE identities
error) answers **502** while the response is still unsent; mesh is
rotation happens with zero restarts. The first fetch is synchronous -
## `[[routes]] retry`: per-route retries
request through the failover machinery when an upstream answers 5xx -
## `[[routes]] mirror`: request mirroring (shadow traffic)
## `[[routes]] cors`: cross-origin policy
**Preflight**; an `OPTIONS` carrying `Access-Control-Request-Method` is
**Actual requests**; proxied normally, then the response headers are
relayed **untouched**; the browser is the enforcement point, so vane
`allow_origins = ["*"]` emits `Access-Control-Allow-Origin: *`; unless
reconciler path as xDS; no restart. Invalid configs keep the