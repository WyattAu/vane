# Tier A coverage closure — work plan

The coverage gate finally measures honestly (see the history below) and
reports **87.84% vs the 90% Tier A bar** — a gap of ~780 lines. This
document is the arc: where the misses are, and the test each needs.

Measured 2026-10-08, quality/coverage (unfiltered, the honest run).

## Where the misses are

| File | Missed | Cover | What's dark |
|---|---|---|---|
| vane/src/proxy.rs | 738 | 81.5% | relay edge paths (audit with line data next) |
| vane-kernel/src/h2/connection.rs | 324 | 83.4% | engine h2 error/flow paths |
| vane/src/h2_server.rs | 318 → 180* | 68% | protocol edges — see the targeted list below |
| vane/src/main.rs | ~220 | — | xds-client loop, sidecar bridge, watch-mode operator |
| vane/src/h3_edge.rs | 246 | 64.4% | h3 edge paths (h3 tests now run in coverage) |
| vane-control/src/acme.rs | 211 | 85.1% | renewal loop, HTTP-01 serving |
| vane/src/server.rs | 193 | 89.3% | startup/hot-reload edges |
| vane-control/src/providers/k8s.rs | ~150 | — | watch/spawn loop (list_routes now covered) |
| vane/src/xds_client.rs | 186 | 67% | ADS client loop (xds-interop covers it; separate workflow) |

\* after the targeted h2 run below; local llvm-cov on the h2 suites.

## h2_server.rs — the precise dark list (from lcov, 2026-10-08)

All unit-testable against the `H2Server` state machine directly
(`handle_read` → events; `response_bytes` → frames), the same way
`h2_upstream_boundary_sweep` drives it:

1. `maybe_probe_stall` (132–143): stall detection — never driven.
2. `conn_debug_windows`/`conn_error_code` (152–160): accessors — cover
   via a connection-error scenario.
3. RST_STREAM send path (206–210): protocol violation → reset.
4. `request_content_length` partial-head cases (230–260).
5. Multi-frame body emission (268–296): `body_remaining` partial takes —
   responses larger than `max_frame` split across DATA frames.
6. `Event::Trailers` (299–313): upstream trailers → h2 trailers frame.
7. `H2Event::SendCredit` (319): window credit returned to the client.

## Ground rules for this arc

- Every test asserts protocol bytes, not just "no panic".
- No skipping: `--skip` filters in the coverage invocation are gone (the
  last one silently measured two suites — cargo-llvm-cov eats libtest
  `--skip` flags and keeps the values as run-only filters).
- The bar stays at 90. If the gap closes slower than the code grows,
  the honest signal is a red gate, not a lower bar.

## History (why this arc exists)

- The coverage numbers were untrustworthy twice over: `shutdown_after`
  dead code (Sep 26) hung the quality jobs so they never reported; and
  rust-cache served stale instrumented binaries to the coverage job so
  even a completed run measured last week's code.
- The shared quality/coverage job (unfiltered) is the source of truth:
  87.84% is the honest baseline. The `ci.yml` coverage job now runs
  unfiltered too (its `--skip` args were being mangled into run-only
  filters — it measured two suites).
