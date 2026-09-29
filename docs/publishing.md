# Publishing vane to crates.io

> Status (2026-09-19): PUBLISHED. `vane` and `vane-core` were taken
> on crates.io by an unrelated project, so the transport engine
> publishes as **`vane-kernel`** and the installable package as
> **`vane-proxy`** (the binary it installs is still named `vane`).
> Published: 0.2.0 train (all crates), 0.2.1–0.2.3 trains (vane-proxy
> + kernel), **0.3.0 (2026-09-28, all 11 crates — HTTP/3 GA**, h3
> default feature**)**, and **0.3.1 (2026-09-29, vane-control +
> vane-proxy — h3 upstream bridge, connection reuse, health)**,
> verified with `cargo install vane-proxy --version 0.3.1 --locked`.
> Order below unchanged.

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

Automated: pushing a `vX.Y.Z` tag triggers `.github/workflows/
release.yml`, which builds the Dockerfile and pushes
`ghcr.io/wyattau/vane:X.Y.Z` (+ `X.Y`) with the workflow-scoped
`GITHUB_TOKEN` — no PAT, no local credentials. No `latest` tag: charts
and compose pin exact versions. Backfill an already-cut tag by
deleting and re-pushing it (or run the workflow manually).

Local builds for testing: `docker build -t ghcr.io/wyattau/vane:X.Y.Z .`
