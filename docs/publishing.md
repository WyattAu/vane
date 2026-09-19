# Publishing vane to crates.io

> Status (2026-09-19): PUBLISHED. `vane` and `vane-core` were taken
> on crates.io by an unrelated project, so the transport engine
> publishes as **`vane-kernel`** and the installable package as
> **`vane-proxy`** (the binary it installs is still named `vane`).
> Published at 0.2.0: vane-observe, vane-proto, vane-router,
> vane-filters, vane-shm, vane-control, vane-tls; 0.2.0 for
> vane-kernel, vane-plugins, vane-client-sdk, vane-proxy follows the
> same order below.

crates.io forbids git dependencies, so the leaves must land in
dependency order. The only external blocker was `slab-pool` (git dep
of the engine crate) — already published at 0.1.0.

## Publish order (dependency leaves first)

Each: `cargo publish --dry-run -p <crate>` then `cargo publish -p
<crate>` (crates.io rate-limits NEW crate creation — space publishes
~90 s apart and back off on 429):

1. `vane-observe` (no internal deps)
2. `vane-proto` (→ observe)
3. `vane-router` (→ proto)
4. `vane-filters`, `vane-shm` (→ observe/router)
5. `vane-control` (→ filters/shm), `vane-tls` (→ observe)
6. `vane-kernel` (→ slab-pool; formerly `vane-core`)
7. `vane-plugins`, `vane-client-sdk` (→ kernel/shm)
8. `vane-proxy` (the binary; installs `vane`)

## Verify

```bash
cargo install vane-proxy --version 0.2.0 --locked
vane --version
```

## Container image

After crates land: build + push `ghcr.io/wyattau/vane:0.2.0` via the
repo Dockerfile (needs registry push credentials):

```bash
docker build -t ghcr.io/wyattau/vane:0.2.0 .
docker push ghcr.io/wyattau/vane:0.2.0
```

The compose files and Helm chart already reference
`ghcr.io/wyattau/vane:0.2.0` by default.
