# Research loop: 2026-10 spec re-derivation (10 iterations)
- **KEEP**; vane's approach is at or past the state of the art; the
- **ADOPT**; a better mechanism exists elsewhere; the spec changes and
- **OPEN**; genuinely unsolved (by us or by anyone); recorded as a
## Iteration 1: HTTP/1.1 framing & smuggling defense
Desync Delusion" (Black Hat/DEF CON 2025; leading-OWS TE obfuscation
**Prior art:** Cloudflare's edge posture; reject non-HTTP/1.1,
invalid CL, multiple TE, non-exact `chunked`; is the strict
rejected CL+TE, duplicate CL, multiple TE, and non-chunked codings -
   connection instead of parsing the tail as a new request; request
## Iteration 2: HTTP/2 conformance & flood defense
connection error; explicitly NOT a buffered informative response; a
## Iteration 3: QUIC / HTTP/3 transport
validation tokens. ALL THREE live in the transport layer; quinn -
**Verdict: KEEP.** vane's h3 accept path never calls `into_0rtt()` -
## Iteration 4: Load balancing (P2C)
Balancing" (IEEE TPDS 2001; thesis 1996); d=2 gives an exponential
Peak EWMA balancer (2025; latency-aware P2C, the Finagle PeakEWMA
**Analysis:** the load SIGNAL is the whole game; the classic model
in-flight request counts; the Envoy least-request signal, correct.
LeastConn divided by weight); a `policy = "p2c"` cluster with
weight-0 clamps to 1, exact ties break toward the first pick -
RTT sampling wired through the relay hot path; a real feature with a
## Iteration 5: Outlier detection & circuit breaking
a fixed duration; but NO ejection cap. A shared-cause transient (a
most needed. Envoy's rule; cap ejections at a percent of the cluster,
always allowing at least one; is the cluster-survival guard.
**Lean proof:** `ejection_cap_keeps_the_cluster_serving`; with a 50%
**Closed (2026-10-17):** the arc shipped as two trains -
`vane-proxy` 0.11.0; published when the arc completes. `Listener
## Iteration 6: TLS: post-quantum key exchange & cert lifecycle
to a proxy NOW; the frontend TLS leg carries exactly the traffic an
2025; researched and ruled out).
Envoy (PQ via BoringSSL X25519MLKEM768, default since 2024); Envoy
default ring build is untouched; the switch changes the build matrix
## Iteration 7: Memory & zero-copy data path
**Pulled:** io_uring zero-copy send (LWN 877167/879724; 1.88-2.44x
flow steering); Seastar's asymmetric_io_uring backend (July 2026 -
`uring.rs`; the integration point where both features would land.
## Iteration 8: Control plane (xDS/ADS, hot reload)
to every type, loads everything eagerly, and its snapshot is bounded -
## Iteration 9: Observability (access logs, traces)
industry expectation; and vane's fixed structured record already
generated trace id; every log/trace join broke at vane's hop, and a
verbatim to the upstream; W3C wants a child span id per hop). Both
## Iteration 10: Correctness proof methods for parsers
(arXiv 2405.17737, Kallus/Anantharaman/Locasto/Smith; 100+ parser
INTERPRETATIONS (how a server parses a stream), not just outputs -
differential-invariant bug; the fuzz target's whole-vs-dripped
model); real value, real scope; parked, with the regression seeds