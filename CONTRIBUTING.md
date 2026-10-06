# Contributing to vane

## The gates (Tier A — latency/concurrency-critical)

Every PR runs the shared matrix plus vane-specific jobs:

| Gate | Command |
|---|---|
| build (locked, all-features, no-defaults) | `cargo build --workspace --all-features` |
| tests | `cargo test --workspace` |
| clippy (pedantic, `-D warnings`) | `cargo clippy --workspace --all-targets` |
| fmt | `cargo fmt --all --check` |
| vet (dependency supply chain) | `cargo vet --locked` |
| loom (lock-free primitives) | `cargo test -p vane-kernel --features loom --test loom_spsc` |
| miri (pure-logic modules) | `cargo +nightly miri test -p vane-kernel --lib slab::` |
| fuzz smoke | `cargo fuzz run parse_request -- -max_total_time=30` |
| criterion baselines | `cargo bench -p vane-proto -p vane-router -p vane-shm` |

### After a dependency change

`cargo vet` coverage is generated, not hand-maintained. When you add or
bump a dependency, `cargo vet --locked` names the packages that lost
coverage; regenerate the store with:

```sh
cargo vet init
python3 ../engineering-standards/scripts/resync-vet-exemptions.py --write
cargo vet fmt
```

`cargo vet fmt` owns `supply-chain/config.toml`'s formatting — do not
hand-edit it. First-party crates are declared `audit-as-crates-io`
because they publish under those names.

`deny.toml` and `.cargo/audit.toml` carry the reviewed exceptions, each
with a written justification. A new advisory or a rejected license means
fixing or justifying it, not deleting the gate.

## Data-plane rules (`vane-core`, `vane-proto`, `vane-filters`)

1. **No locks.** Atomics are limited to `Relaxed`/`Acquire`/`Release` — no
   `SeqCst`. If you need a lock, redesign.
2. **No allocation on the request path.** Sessions come from the slab, IO
   buffers from the fixed pool; new per-request state needs a pool story.
3. **No dynamic dispatch in pipelines.** Filters compose monomorphized;
   `Box<dyn Filter>` is a design bug in this layer.
4. **Every `unsafe` block carries a `// SAFETY:` comment** naming the
   invariant. `undocumented_unsafe_blocks` is deny-by-CI.
5. Loom-testable primitives get a loom double under the `loom` feature.

## Control-plane rules

Std-ecosystem crates are fine (Tokio, reqwest, axum). Reuse the WyattAu kit
ecosystem before reaching for anything new.

## Commits

Conventional Commits (`feat:`, `fix:`, `perf:`, `docs:`, `test:`, `chore:`).
Keep the repo's CI green; do not commit `target/` or `cargo-fuzz` artifacts.
