## The gates (Tier A: latency/concurrency-critical)
`cargo vet fmt` owns `supply-chain/config.toml`'s formatting; do not
1. **No locks.** Atomics are limited to `Relaxed`/`Acquire`/`Release`; no