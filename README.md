# vane

**A deterministic, zero-copy L4/L7 reverse proxy, edge gateway, and micro sidecar — in Rust.**

```text
            ┌──────────────────────────────────────────────────┐
            │  control plane (Tokio)                           │
            │  file/docker/K8s providers · ACME · health       │
            │  ── publishes route generations (EBR swap) ──▶   │
            └──────────────────────────────────────────────────┘
   ┌──────────────────────────┬──────────────────────────┐
   ▼                          ▼                          ▼
┌──────────────┐      ┌──────────────┐          ┌──────────────┐
│ worker 0     │      │ worker 1     │   ...    │ worker N     │
│ pinned core  │      │ pinned core  │          │ pinned core  │
│ io_uring /   │      │ io_uring /   │          │ io_uring /   │
│ mio fallback │      │ mio fallback │          │ mio fallback │
│ slab · fixed │      │ slab · fixed │          │ slab · fixed │
│ buffers      │      │ buffers      │          │ buffers      │
└──────────────┘      └──────────────┘          └──────────────┘
```

- **Thread-per-core, share-nothing** — pinned workers, zero locks on the data
  plane, cacheline-padded lock-free SPSC command paths.
- **io_uring with fixed registered buffers** (zero per-op kernel mapping),
  automatic **mio/epoll fallback** on older kernels.
- **Lock-free dynamic routing** — immutable radix trie, EBR generation swap
  across cores in well under 1 ms, ~220 ns lookups at 10k routes.
- **Monomorphized filter pipelines** — no vtable dispatch; rate limiting
  (GCRA) and circuit breaking reuse the [`throttle-kit`] and [`breaker`] kits.
- **POSIX SHM sidecar transport** — co-located services call the proxy over
  shared-memory rings: **~2.8 µs round-trip** (10× under the 30 µs target).
- **Zero-loss hot upgrade** — listeners hand over via `SCM_RIGHTS`; route
  state archives to disk; connections never reset.
- **Kernel splice passthrough** for L4 — request bytes never enter user space.

[`throttle-kit`]: https://crates.io/crates/throttle-kit
[`breaker`]: https://crates.io/crates/breaker

## Workspace

| Crate | Role |
|---|---|
| `vane-core` | io_uring/mio engines, pinned workers, session slab, SPSC, splice |
| `vane-proto` | zero-copy HTTP/1.1 (httparse), date cache, response writer |
| `vane-router` | EBR-published radix trie, P2C/RR/least-conn balancers |
| `vane-filters` | compile-time pipelines + GCRA/breaker/forwarded built-ins |
| `vane-control` | config, file/docker/K8s providers, ACME, health checks |
| `vane-shm` | sidecar ring transport, hot-upgrade handover, C ABI |
| `vane-tls` | rustls termination, ALPN, ChaCha20 session-ticket cache |
| `vane-plugins` | wasmtime sandbox (feature `wasm`) |
| `vane-client-sdk` | Rust SDK + `include/vane_sidecar.h` for C/C++ |
| `vane` | the binary: CLI, proxy handler, admin plane |

## Quick start

```bash
# 1. upstream
python3 -m http.server 9001 &

# 2. config
cat > vane.toml <<'EOF'
[[listeners]]
address = "0.0.0.0:8080"

[clusters.demo]
backends = ["127.0.0.1:9001"]

[[routes]]
pattern = "/*rest"
cluster = "demo"

[admin]
enabled = true
address = "127.0.0.1:9100"
EOF

# 3. run
cargo run -p vane -- run -c vane.toml

# 4. verify
curl -v localhost:8080/anything
curl localhost:9100/metrics
```

### Sidecar mode

```bash
# in the proxy process (or `vane sidecar`)
curl localhost:9100/config            # route table

# from a co-located service (Rust):
Sidecar::connect("/dev/shm/vane-sidecar")?.send(b"GET /health", 1s)?;
# from C/C++: see crates/vane-client-sdk/include/vane_sidecar.h
```

### TLS termination

```toml
[[listeners]]
address = "0.0.0.0:8443"
[listeners.tls]
cert = "/etc/vane/cert.pem"
key  = "/etc/vane/key.pem"
```

rustls with ALPN (`h2`, `http/1.1`) and a shared ChaCha20 session-ticket
cache; pair with the ACME manager for automated certificates.

### Production features

- **TLS termination** (rustls, ALPN, ticket cache) with per-listener
  cert/key or `VANE_TLS_CERT`/`VANE_TLS_KEY`
- **Failover**: a dead backend is skipped (up to 2 attempts) while the
  request is still unsent; breaker-recorded
- **Enforced timeouts**: connect / first-byte / idle — 504 on expiry
- **Opt-in rate limiting** (GCRA per client IP via `rate_limit_rps`)
- **Env overrides** (container-friendly layering over TOML):
  `VANE_LISTEN`, `VANE_ADMIN_ADDR`, `VANE_CLUSTER_<NAME>`
  (comma-separated backends), `VANE_WORKERS`
- **In-process SHM sidecar** (`[sidecar] enabled = true`) and **wasm
  plugins** (`[[plugins]] path = "..."`, feature `wasm`)

### Known issues / current limits

- **Sustained concurrent load** (`crates/vane/tests/load.rs`, always
  green): fixed in this cycle. The decisive defects were an io_uring
  `Connect` sockaddr use-after-free (bytes copied at submit, not at SQE
  build — surfaced as EAFNOSUPPORT storms) and an IPv4 byte-order slip
  (`1.0.0.127` SYN blackhole). Both carry regression coverage
  (`vane-core/tests/connect.rs`, `vane-core/tests/echo.rs` on both
  engines).
- **Upstream keep-alive pooling** on by default (4 idle conns per
  backend per worker); `pool_per_backend = 0` for dial-fresh.
- HTTP/2 is served by a **separate REUSEPORT acceptor** (see
  `h2_edge.rs`) whose bodies are buffered (32 MiB); no upstream h2 yet.
- Upstream HTTP/2 and HTTP/3 (frontend h2/h3 terminate to HTTP/1.1) —
  tracked on the roadmap.

### Hot upgrade

```bash
# new binary takes over listening sockets + routes with zero dropped packets:
./vane-new run -c vane.toml --handover-from /tmp/vane-handover.sock
# old binary: sends fds (SCM_RIGHTS) + route archive, then drains and exits
```

## Performance

Measured on the repository's criterion benches (dev profile — release builds
are faster):

| Bench | Result | Requirement |
|---|---|---|
| HTTP/1.1 head parse (5 headers) | ~266 ns incl. storage reset | `PR-01` |
| Route lookup @ 10k routes | ~223 ns | `CP-01` |
| SHM sidecar RTT (512 B) | ~2.8 µs | `IP-01` < 30 µs |
| Keep-alive pool (opt-in) | 1 upstream conn across 5 sequential requests | — |
| Config generation swap | one `Release` store + epoch retire | `CP-02` < 1 ms |

### Comparison against nginx (same box, same upstream)

`ab -k -c 64/-c 256` against an identical upstream (threaded Rust stub,
~76–78k rps direct), vane release build (2 workers) vs nginx:alpine in
Docker with host networking (1 worker, stock `proxy_pass` config):

| Scenario | vane | nginx (stock) |
|---|---|---|
| keep-alive, c=64 | 37.4–39.1k rps | 9.1–9.3k rps |
| keep-alive, c=256 | 37.8k rps | 8.5k rps |
| short connections, c=64 | 7.0k rps | 7.7k rps |

Caveats, honestly stated:

- nginx's stock `proxy_pass` speaks HTTP/1.0 upstream **without
  keep-alive** — it opens a fresh upstream connection per request. That
  is the default most deployments run, but `upstream` keep-alive pools
  narrow the gap.
- vane pools upstream connections by default (epoch-invalidated), which
  is where the keep-alive advantage comes from.
- Short-connection rate is accept-bound and effectively at parity.
- Measurements are single-run means on a shared multi-tenant host; treat
  ratios (≈4× keep-alive, ≈1× short) as indicative, not absolute.

## Engineering gates (Tier A)

`cargo clippy -D warnings` (pedantic) · `llvm-cov ≥ 90%` · **loom**
model-checking of the SPSC/event rings · **miri** on pure-logic modules ·
**cargo-fuzz** smoke runs on the parser/router/descriptor · criterion
baseline regressions. CI mirrors [`WyattAu/engineering-standards`].

[`WyattAu/engineering-standards`]: https://github.com/WyattAu/engineering-standards

## Feature flags

| Flag | Effect |
|---|---|
| `io-uring` (default) | io_uring engine; off ⇒ mio fallback |
| `h2` / `h3` | experimental HTTP/2 & HTTP/3 bridges |
| `wasm` | wasmtime plugin sandbox |
| `k8s` | Gateway API provider |
| `rkyv` | zero-copy route snapshots for handover |

## Deploy

- `Dockerfile` — distroless image
- `deploy/docker-compose.yml` — local two-service demo
- `deploy/helm/vane/` — chart with probes, SHM mount, scrape annotations
- `deploy/vane.example.toml` — full configuration reference

## License

Apache-2.0
