Status 2026-10-02: **145/145 pass, 0 failed** (146 cases; http2 +
hpack; 1 skipped: TLS-only). Full-worklist history below.
WINDOW_UPDATE divergence; now the `[http2]
widened to all specs (http2 + hpack) and exposed a hpack gap -
dynamic table size updates after field representations; now
pass**, 25 failures + 1 skip; all one class: missing protocol
   semantics per RFC 9113 vs 7540; h2spec 2.6 tests 7540).
feature-gated); `h2` is now a default feature and startup refuses
## Regression watch (2026-09-26): chunked_relay_h2_to_h1: RESOLVED
accumulator double-counted the initial 65,535 window; WINDOW_UPDATE