# Deploying vane

Three surfaces: local binary, Docker Compose, Kubernetes (Helm).
The full field reference is [config.md](config.md).

## 1. Binary

```bash
cargo build --release -p vane --features h2,file-provider,docker-provider
./target/release/vane validate -c vane.toml   # parse + semantic check
RUST_LOG=info ./target/release/vane run -c vane.toml
```

Signals: SIGTERM/SIGINT → graceful drain (or hot-upgrade handover when
`--handover-to` is set), SIGHUP → config reload is **not** supported;
ship a new config by swapping `[file_provider]` content or hot-upgrading
the binary.

Log level is `RUST_LOG` (default `info`); access logs are separate
(`[access_log]`, JSON lines to stderr or a file).

### Container quirks

Docker's default seccomp profile blocks `io_uring` syscalls. vane
detects this and falls back to the mio engine automatically (you'll see
`io_uring unavailable …, falling back to mio`). To run io_uring in a
container, use a permissive seccomp profile or `--privileged`.

The compose files also set two container knobs vane depends on:

- `shm_size: "256mb"` — `/dev/shm` defaults to 64 MB, too small for
  the default sidecar slot config; enlarge it whenever `[sidecar]` is
  enabled (see `[sidecar] slot_size × slots` in [config.md](config.md)).
- `cap_add: [IPC_LOCK]` — io_uring fixed buffers are page-locked
  (`IORING_REGISTER_BUFFERS`); without it, buffer registration fails
  and the engine falls back to heap buffers.

## 2. Docker Compose

A proxy + upstream demo (Compose files at the repo root):

```bash
docker compose -f docker-compose.yml up        # :8080 data, :9090 admin
curl http://127.0.0.1:8080/api/hi
curl http://127.0.0.1:9090/metrics
```

Zero-downtime upgrade demo (two generations share one listening socket
via SCM_RIGHTS fd passing):

```bash
docker compose -f docker-compose.hot-upgrade.yml up
```

Image tag selection: `image:` honors `${VANE_IMAGE}` for local runs
(`VANE_IMAGE=vane:0.2.0-dev docker compose up`); the default reference
is the published `ghcr.io/wyattau/vane:<version>` image.

## 3. Kubernetes (Helm)

```bash
# static config from values
helm install vane ./charts/vane
# Gateway API variant (watches HTTPRoutes + Endpoints; needs the
# gateway-api CRDs and RBAC — included in the chart)
helm install vane ./charts/vane -f charts/vane/values-k8s-provider.yaml
```

What the chart creates: Deployment (rolling, 0 unavailable, non-root),
dual-port Service (data + admin), ConfigMap-generated `vane.toml`,
`/healthz` + `/readyz` probes, Prometheus scrape annotations, and —
with `k8sProvider.enabled` — ServiceAccount + ClusterRole/ClusterRoleBinding
for `httproutes`, `endpoints`, and `services` list/watch.

Important values (`charts/vane/values.yaml`):

| Key | Purpose |
|---|---|
| `image.repository/tag` | image to deploy |
| `replicaCount` | pod count |
| `service.type/port/adminPort` | exposure |
| `config.clusters / config.routes` | static routing (DNS service names OK) |
| `k8sProvider.enabled/namespaces` | live Gateway API discovery |
| `prometheus.enabled` | scrape annotations |
| `resources`, `podSecurityContext` | standard knobs |

### Notes on running vane in-cluster

- **Providers**: the chart's static config and the K8s provider compose —
  static routes seed the table, provider updates publish generations.
- **TLS**: mount certs via a Secret into `/etc/vane/tls` and reference
  them from `[listeners.tls]` in `config` (or wire `[acme]`).
- **Hot upgrades**: rolling updates recycle pods (connections reset at
  pod churn). In-pod binary hot upgrade (SCM_RIGHTS) is for single-host
  deployments; in K8s, `maxUnavailable: 0` + retries is the pattern.
- **io_uring**: most distros' container runtimes block it (see above);
  the chart defaults to `force_mio = true` in the rendered config.
