**A deterministic, zero-copy L4/L7 reverse proxy, edge gateway, and micro sidecar; in Rust.**
- **Thread-per-core, share-nothing**; pinned workers, zero locks on the data
- **Lock-free dynamic routing**; immutable radix trie, EBR generation swap
- **Monomorphized filter pipelines**; no vtable dispatch; rate limiting
- **POSIX SHM sidecar transport**; co-located services call the proxy over
- **Zero-loss hot upgrade**; listeners hand over via `SCM_RIGHTS`; route
- **Kernel splice passthrough** for L4; request bytes never enter user space.
- **Enforced timeouts**: connect / first-byte / idle; 504 on expiry
  build; surfaced as EAFNOSUPPORT storms) and an IPv4 byte-order slip
- Upstream HTTP/2 and HTTP/3 (frontend h2/h3 terminate to HTTP/1.1) -
Measured on the repository's criterion benches (dev profile; release builds
| Keep-alive pool (opt-in) | 1 upstream conn across 5 sequential requests |; |
connection reuse is the dominant lever; the same upstream serves
completed request; see [docs/config.md](docs/config.md).
| `post-quantum` | aws-lc-rs provider with X25519MLKEM768 leading the key-exchange list; [docs/post-quantum.md](docs/post-quantum.md) |
| adaptive concurrency | opt-in per cluster: latency-gradient load shedding (503 before queueing collapse); [docs/perf-arc-2026-10.md](docs/perf-arc-2026-10.md) |
- `Dockerfile`; distroless image
- `deploy/docker-compose.yml`; local two-service demo
- `deploy/helm/vane/`; chart with probes, SHM mount, scrape annotations
- `deploy/vane.example.toml`; full configuration reference