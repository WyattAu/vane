# Publishing vane to crates.io

> Status (2026-10-06): PUBLISHED. `vane` and `vane-core` were taken
> on crates.io by an unrelated project, so the transport engine
> publishes as **`vane-kernel`** and the installable package as
> **`vane-proxy`** (the binary it installs is still named `vane`).
> Published: 0.2.x trains, **0.3.0 (HTTP/3 GA)**, 0.3.1 (h3 upstream
> bridge), **0.4.0** (mesh over QUIC), the 0.5.x line (SDS, full LDS,
> fuzz-found parser fixes, the relay perf tranches), **0.6.0** (config
> hot-reload, retry/mirror), 0.6.1 (allocation tranche 2), and
> **0.7.0** (per-route CORS plus the relay-correctness tranche: the
> upstream read-throttle deadlock, chunked framing by grammar on both
> directions, split response heads, the keep-alive stale-head replay,
> and two missing-END_STREAM paths). Container images publish
> automatically on `vX.Y.Z` tags (release.yml). Order below unchanged.
>
> **v0.7.0 published 2026-10-06** (vane-proto 0.5.5, vane-router
> 0.6.1, vane-control 0.6.1, vane-proxy 0.7.0) and install-verified:
> `cargo install vane-proxy --version 0.7.0 --locked` → `vane 0.7.0`.
>
> `CHANGELOG.md` is the authoritative per-release record; the summary
> above is only a pointer.

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
cargo install vane-proxy --version 0.7.0 --locked
vane --version
```

## Version rules this repo has already been bitten by

* **Most crates inherit `version.workspace = true`** (0.5.3). A crate
  that needs a different version must set it explicitly in its own
  manifest — two publish failures came from forgetting.
* **Internal dependency requirements live in `[workspace.dependencies]`**
  and can lag the bumped crate versions. Bump them in the same commit as
  the manifest version, or the published crate resolves the *old*
  published version instead of your local one.
* **Never publish a patch that postdates a minor at a lower number.**
  `v0.5.7` was cut after `v0.6.0`; the work had to be re-issued as
  0.6.1. Check the tag dates, not just the numbers.
* **Publish order for a train:** leaves → kernel → control/router →
  proxy. `vane-router` and `vane-control` must land before `vane-proxy`
  whenever their public types changed.

## Container image

Automated: pushing a `vX.Y.Z` tag triggers `.github/workflows/
release.yml`, which builds the Dockerfile and pushes
`ghcr.io/wyattau/vane:X.Y.Z` (+ `X.Y`) with the workflow-scoped
`GITHUB_TOKEN` — no PAT, no local credentials. No `latest` tag: charts
and compose pin exact versions. Backfill an already-cut tag by
deleting and re-pushing it (or run the workflow manually).

Local builds for testing: `docker build -t ghcr.io/wyattau/vane:X.Y.Z .`
