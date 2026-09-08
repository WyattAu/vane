# Contributing to vane

## The gates (Tier A — latency/concurrency-critical)

Every PR runs the shared matrix plus vane-specific jobs:

| Gate | Command |
|---|---|
| build (locked, all-features, no-defaults) | `cargo build --workspace --all-features` |
| tests | `cargo test --workspace` |
| clippy (pedantic, `-D warnings`) | `cargo clippy --workspace --all-targets` |
| fmt | `cargo fmt --all --check` |
| loom (lock-free primitives) | `cargo test -p vane-core --features loom --test loom_spsc` |
| miri (pure-logic modules) | `cargo +nightly miri test -p vane-core --lib slab::` |
| fuzz smoke | `cargo fuzz run parse_request -- -max_total_time=30` |
| criterion baselines | `cargo bench -p vane-proto -p vane-router -p vane-shm` |

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
