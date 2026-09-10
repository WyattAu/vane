# Publishing vane to crates.io

crates.io forbids git dependencies, so the leaves must land in
dependency order. The only external blocker is `slab-pool` (git dep of
`vane-core`).

## Step 0 — you: publish `slab-pool` (one time)

From the slab-pool repo (needs *your* `crates.io` credentials):

```bash
cd /path/to/slab-pool
# 1. Confirm the Cargo.toml has: name, version (e.g. "0.1.0"),
#    description, license, repository.
cargo package --list          # sanity: no stray files
cargo publish --dry-run       # must pass
cargo publish                 # → slab-pool 0.1.0 on crates.io
```

Then tell me it's live and I'll continue with the rest.

## Step 1 — switch the workspace dep

In the workspace root `Cargo.toml`, replace:

```toml
slab-pool = { git = "https://github.com/WyattAu/slab-pool.git", branch = "main" }
```

with:

```toml
slab-pool = "0.1"
```

then `cargo update -p slab-pool` and re-run
`cargo publish --dry-run -p vane-core` (must be clean).

## Step 2 — publish order (dependency leaves first)

Each: `cargo publish -p <crate>` (after the previous is indexed;
`--dry-run` first in a fresh checkout):

1. `vane-observe` (no internal deps)
2. `vane-proto` (→ observe)
3. `vane-router` (→ proto)
4. `vane-filters`, `vane-shm` (→ observe/router)
5. `vane-control` (→ filters/shm), `vane-tls` (→ observe)
6. `vane-core` (→ everything above + slab-pool; **this is what Step 0 unblocks**)
7. `vane-plugins`, `vane-client-sdk` (→ core/shm)
8. `vane` (the binary)

All 11 crates are already tagged `0.2.0` in the workspace; publishing
does not require retagging unless versions diverge.

## Step 3 — verify

```bash
cargo install vane --version 0.2.0 --locked
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
