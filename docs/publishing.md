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
> **v0.10.1 published 2026-10-14** (vane-control 0.9.1 →
> vane-proxy 0.10.1), install-verified: `cargo install vane-proxy
> --version 0.10.1 --locked` → `vane 0.10.1`. The ACME renewal fix:
> the loop used to re-issue certificates daily with no expiry check —
> breaking its own renewals against Let's Encrypt's 5/week duplicate
> limit on day 5 of uptime. It now reads the stored leaf's validity
> hourly and re-issues only past renew_at_fraction of the lifetime,
> which is also what makes Let's Encrypt's ~6-day short-lived
> certificates viable.
>
> **v0.10.0 published 2026-10-13** (vane-proto 0.6.0 → vane-control
> 0.9.0 → vane-proxy 0.10.0), install-verified: `cargo install
> vane-proxy --version 0.10.0 --locked` → `vane 0.10.0`, plus a
> dogfood run of the installed binary with `proxy_protocol = true`.
> Adds PROXY protocol v1/v2 listener support (client IP behind an L4
> load balancer; the header precedes the TLS handshake; trust is
> config-gated; malformed or missing headers close the connection).
> `ListenerConfig` is now `#[non_exhaustive]` so the next config field
> is not a breaking change. `quinn-proto` 0.11.19 for the QUIC
> advisories.
>
> **v0.9.0 published 2026-10-11** (vane-filters 0.6.0 — first version
> off the workspace train — → vane-control 0.8.0 → vane-kernel 0.7.0 →
> vane-proxy 0.9.0), install-verified: `cargo install vane-proxy
> --version 0.9.0 --locked` → `vane 0.9.0`. The relay-correctness
> release: tunnel bytes coalesced with the 101 head are no longer
> dropped, the h1 circuit breaker sees every upstream failure and is
> process-wide with an explicit anti-flap policy (breaker 2.0), RFC
> 9218 SETTINGS_NO_RFC7540_PRIORITIES is enforced and declared, config
> tables reject unknown fields. The coverage arc closed: 90.19%
> against the 90% Tier A bar, every gate green.
>
> **v0.8.0 published 2026-10-07** (vane-kernel 0.6.0 → vane-control
> 0.7.0 → vane-proxy 0.8.0): two remote-panic parser fixes, `[runtime]
> buffer_size`, tools/loadgen, tunnel backpressure proof, k8s
> namespaces fix, and the first valid three-proxy comparison
> (docs/benchmarks.md).
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
`ghcr.io/wyattau/vane:X.Y.Z` (+ `X.Y` + `latest`) with the
workflow-scoped `GITHUB_TOKEN` — no PAT, no local credentials.
Backfill an already-cut tag by deleting and re-pushing it (or run the
workflow manually).

`latest` is pushed on every release so it cannot drift. It did once: a
manually-pushed `latest` outlived several releases because the workflow
only ever pushed the semver tags, so anyone pulling `:latest` silently
got an old build. **Pin an exact tag when you need reproducibility** —
the charts and both compose files do.

Local builds for testing: `docker build -t ghcr.io/wyattau/vane:X.Y.Z .`
