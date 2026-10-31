# Tier A coverage closure: work plan
**90.19%** on commit `dafed7fb` and every CI job is green; the first
  (`relay_body` had no Tunnel arm; the catch-all relayed nothing).
  validated and declared once in our first SETTINGS frame; without a
The historical gap table below is kept for the record but is stale -
reports **87.84% vs the 90% Tier A bar**; a gap of ~780 lines. This
| vane/src/h2_server.rs | 318 → 180* | 68% | protocol edges; see the targeted list below |
| vane/src/main.rs | ~220 |; | xds-client loop, sidecar bridge, watch-mode operator |
| vane-control/src/providers/k8s.rs | ~150 |; | watch/spawn loop (list_routes now covered) |
## h2_server.rs: the precise dark list (from lcov, 2026-10-08)
1. `maybe_probe_stall` (132–143): stall detection; never driven.
2. `conn_debug_windows`/`conn_error_code` (152–160): accessors; cover
5. Multi-frame body emission (268–296): `body_remaining` partial takes -
  last one silently measured two suites; cargo-llvm-cov eats libtest
  filters; it measured two suites).