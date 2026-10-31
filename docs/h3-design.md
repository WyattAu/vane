  shape as h2; `release_capacity`/`consume_send_budget` carry over.
  runtime thread bridging CQEs into the worker; document the bridge.
   handshake green (0-RTT CRYPTO case passes); 48 of 49 cases fail -
## Milestone 4: client-side H3Upstream: SHIPPED (2026-09-28, loopback-bridge form)
address before the workers start; the synchronous mio worker dials a
  with a per-backend connection pool; one QUIC handshake per
- Health: the bridge owns backend health for the cluster; active
  bridge address only (vacuous by construction; skipped for http3
  abruptly; the worker sees premature EOF and the existing failover
  fail the header build (H3_INTERNAL_ERROR); that bug class is
Acceptance: `tests/h3_bridge.rs`; h1→bridge→QUIC→vane-h3-edge→h1
SHM sidecar pattern). Semantics and config are unchanged; `h3_bridge`