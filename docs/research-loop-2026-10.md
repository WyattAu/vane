# Research loop — 2026-10 spec re-derivation (10 iterations)

Driver: a re-examination of the whole spec from first principles. For
each part of the repo: pull current papers/specs, analyze the best
known solution, compare against how other projects solve the same
problem (or whether they could not), write the verdict here, and adjust
the lean proof (a test, a fuzz invariant, or a doc assertion) so the
proof artifacts match the spec.

Verdict vocabulary:

- **KEEP** — vane's approach is at or past the state of the art; the
  proof adjustment pins it.
- **ADOPT** — a better mechanism exists elsewhere; the spec changes and
  the code/test follows.
- **OPEN** — genuinely unsolved (by us or by anyone); recorded as a
  parked research item with a trigger.

| # | Part | Verdict | Lean proof adjusted |
|---|---|---|---|
| 1 | HTTP/1.1 framing & smuggling defense | KEEP + ADOPT | parser: 3 tests; e2e smuggling test |
| 2 | HTTP/2 conformance & flood defense | ADOPT | 3 kernel tests pinning the flood bounds |
| 3 | QUIC / HTTP/3 transport | KEEP | doc assertion: 0-RTT rejected by design |
| 4 | Load balancing (P2C) | KEEP + ADOPT | p2c_respects_weights unit test |
| 5 | Outlier detection & circuit breaking | KEEP + ADOPT | ejection_cap_keeps_the_cluster_serving unit test |
| 6 | TLS: post-quantum & cert lifecycle | ADOPT (feature-gated follow-up) | doc assertion in vane-tls |
| 7 | Memory & zero-copy data path | KEEP | doc assertion: parked SEND_ZC + ZC Rx triggers in uring.rs |
| 8 | Control plane (xDS/ADS, hot reload) | (fill) | (fill) |
| 9 | Observability (access logs, traces) | (fill) | (fill) |
| 10 | Correctness proof methods for parsers | (fill) | (fill) |


## Iteration 1 — HTTP/1.1 framing & smuggling defense

**Pulled (2025–2026 sources):** Pingora CVE-2026-2835 (HTTP/1.0
close-delimited bodies + multi-TE forwarded); Tinyproxy CVE-2026-54387
(conflicting CL + TE:chunked forwarded verbatim); Akamai CVE-2025-66373
(invalid chunked size forwarded, tail bytes smuggled); Kettle, "The
Desync Delusion" (Black Hat/DEF CON 2025 — leading-OWS TE obfuscation
as the desync-delusion class); OWASP WSTG 4.x modern vectors (h2→h1
translation, h2c upgrade).

**Analysis:** the framing-confusion class has three modern shapes: (a)
forwarding ambiguous framing verbatim and picking one interpretation,
(b) accepting an invalid chunked body as complete and parsing the tail
as a new message, (c) accepting obfuscated TE values. (a) is Pingora/
Tinyproxy; (b) is Akamai; (c) is the 2025 research frontier.

**Prior art:** Cloudflare's edge posture — reject non-HTTP/1.1,
invalid CL, multiple TE, non-exact `chunked` — is the strict
interpreter, and it is what kept their CDN unaffected while Pingora
(standalone) was vulnerable. hyper/llhttp converge on the same
strictness.

**Verdict: KEEP + ADOPT.** vane's `framing_is_ambiguous` guard already
rejected CL+TE, duplicate CL, multiple TE, and non-chunked codings —
strictly stronger than the vulnerable versions of Pingora/Tinyproxy.
ADOPTED three hardenings the research showed we lacked:

1. TE value OPENING with whitespace (`Transfer-Encoding:	chunked`) →
   ConflictingFraming. Comma-list separators stay legitimate
   (`gzip, chunked` still accepted).
2. HTTP/1.0 + Transfer-Encoding → ConflictingFraming (RFC 9112 §7.1;
   the CVE-2026-2835 class).
3. A malformed chunked body (invalid size line, over-long line) now
   sets a `malformed` flag on the scanner and the proxy CLOSES the
   connection instead of parsing the tail as a new request — request
   side, inline-body side, and the response relay side (which also
   stops reusing the upstream connection).

**Lean proof:** two parser tests (leading-OWS TE, HTTP/1.0+TE), the
e2e `malformed_chunked_request_closes_the_connection` (asserts exactly
one upstream transaction and that the smuggled request never becomes
one), scanner `is_malformed` covered by the existing malformed-size
test plus the new proxy wiring.


## Iteration 2 — HTTP/2 conformance & flood defense

**Pulled:** Envoy CVE-2024-30255 (CONTINUATION flood → CPU exhaustion,
~1 core per 300 Mbit/s), CVE-2024-27919 (the memory variant), Apache
CVE-2024-27316 (buffering for an informative 413 IS the memory bug),
Netty CVE-2026-33871 (zero-length CONTINUATIONs bypass size caps),
Mozilla CVE-2024-3302 (no count limit), plus the RFC 9113 §6.10
requirements.

**Analysis:** the CONTINUATION-flood literature converged on THREE
independent bounds, each covering a bypass of the others: total
assembled size (counters memory), per-frame size (RFC 9113 §4.1
already requires this per frame), and FRAME COUNT (counters
zero-length frames, which grow no buffer but each cost parser work).
The response to a bound must be an immediate connection error, never a
buffered informative error.

**Prior art:** Envoy's post-fix (max_consecutive_inbound_frames_with
_empty_payload = 1, header map limits enforced during assembly),
nghttp2's callbacks-with-early-RETURN design, Netty's post-CVE count
cap. All arrived at the same triple-bound after being burned.

**Verdict: ADOPT.** vane's h2 kernel had NONE of the three bounds:
CONTINUATION payloads accumulated without limit, the count was
uncapped (zero-length frames free), and per-frame size was unvalidated
on the CONTINUATION path (only on HEADERS). Now: fragments past
`max_header_list_size` (our advertised setting) are a PROTOCOL_ERROR
connection error — explicitly NOT a buffered informative response; a
CONTINUATION larger than `max_frame_size` is FRAME_SIZE_ERROR; and
more than 64 CONTINUATIONs per block is PROTOCOL_ERROR (legitimate
maximum is ~8: 128 KiB list / 16 KiB frames).

**Lean proof:** a kernel test pins all three bounds (zero-length flood
→ PROTOCOL_ERROR at exactly the count cap + 1; oversized frame →
FRAME_SIZE_ERROR; fragment growth past the advertised size →
PROTOCOL_ERROR), alongside the existing `continuation_assembles_block`
test proving legitimate multi-frame blocks still work.


## Iteration 3 — QUIC / HTTP/3 transport

**Pulled:** RFC 9000 §8/§9 (address validation, migration, anti-
amplification), RFC 9308 §3 (0-RTT applicability + replay), RFC 8740
(HTTP 0-RTT replay profile), draft-seemann-quic-nat-traversal (NAT
traversal via CONNECT-UDP + path validation), "How Resilient is QUIC
to Security and Privacy Attacks?" (arXiv 2401.06657, 2025).

**Analysis:** the QUIC server-side risk set is 0-RTT replay
(non-idempotent requests can be silently duplicated), spoofed
connection migration (CMRF), and amplification via replayed
validation tokens. ALL THREE live in the transport layer — quinn —
not in vane's application code. A proxy relaying POST bodies must
treat 0-RTT with suspicion: RFC 8740 only blesses idempotent methods.

**Prior art:** ngtcp2/quiche/haproxy-quic all expose 0-RTT acceptance
as an explicit server-side choice, defaulting to reject. quinn's
`Connecting::into_0rtt()` is the same gate; quinn-proto 0.11.19's
`ValidationTokenConfig` (14-day tokens, bloom-filter log) implements
the anti-amplification token validation.

**Verdict: KEEP.** vane's h3 accept path never calls `into_0rtt()` —
by construction quinn rejects 0-RTT early data, which eliminates the
replay class entirely (the correct posture for a proxy relaying
non-idempotent requests). Address validation, retry tokens, path
validation on NAT rebinding, and version negotiation are all
transport-layer (quinn 0.11 + RFC 9368), inherited and audited by the
quinn-proto bump in v0.10.1. alt-svc advertisement (RFC 7838/9308 §5)
is already implemented on h3 listeners.

**Lean proof:** doc assertion in `h3_edge::spawn` (the deliberate
0-RTT rejection, with the RFC 8740 rationale) and the quickstart note;
re-opening 0-RTT is an explicit, reviewed change (it would require the
HTTP-REPLAY analysis for every upstream semantic vane relays).


## Iteration 4 — Load balancing (P2C)

**Pulled:** Mitzenmacher, "The Power of Two Choices in Randomized Load
Balancing" (IEEE TPDS 2001; thesis 1996) — d=2 gives an exponential
max-load improvement over d=1, d=3 only a constant factor more; Envoy
least-request (P2C over active requests, "the host with the highest
active-request count never receives new requests and drains"); Envoy's
Peak EWMA balancer (2025 — latency-aware P2C, the Finagle PeakEWMA
lineage); Envoy weighted least-request (bias=1: weight/(active+1)).

**Analysis:** the load SIGNAL is the whole game — the classic model
uses queue length; Envoy uses active requests; the cutting edge adds
per-host latency EWMA. vane's P2C compares `ConnGauges`, which ARE
in-flight request counts — the Envoy least-request signal, correct.
The genuine gap: **backend weights were ignored by P2C** (only
LeastConn divided by weight) — a `policy = "p2c"` cluster with
weighted backends silently served everything with equal probability.

**Verdict: KEEP + ADOPT.** P2C stays (theory-backed, O(1)). ADOPTED
weight-awareness via Envoy's bias=1 formulation: effective load =
in-flight / weight, compared by cross-multiplication (no float,
weight-0 clamps to 1, exact ties break toward the first pick —
matching pre-change behavior for equal-weight clusters).

**Open (parked):** latency-aware P2C (Peak EWMA) would need per-host
RTT sampling wired through the relay hot path — a real feature with a
measurable cost; trigger: measured tail-latency imbalance across
homogeneous backends.

**Lean proof:** `p2c_respects_weights` unit test on the pure
comparison (equal gauges favor higher weight; cross-multiplied
tradeoffs; zero-weight clamp; tie-break).


## Iteration 5 — Outlier detection & circuit breaking

**Pulled:** Envoy outlier detection spec (v1.7 → v1.40 evolution):
consecutive-5xx (inline), success-rate (periodic sweep, mean −
stdev×factor, min-hosts + min-volume gates), consecutive-gateway,
failure-percentage; max_ejection_percent (10%, at-least-one);
split_external_local_origin_errors; base_ejection_time × ejection-count
backoff. Plus the breaker work already done in iteration 0 of this
train (breaker 2.0, process-wide gate, anti-flap policy).

**Analysis:** vane's outlier set had consecutive-failure ejection with
a fixed duration — but NO ejection cap. A shared-cause transient (a
network blip during a config push) could eject EVERY backend of a
cluster into a guaranteed total outage, exactly when the capacity is
most needed. Envoy's rule — cap ejections at a percent of the cluster,
always allowing at least one — is the cluster-survival guard.

**Prior art:** Envoy (consecutive + success-rate + percentage, capped,
at-least-one), Finagle/resilience4j breakers (windowed, half-open).

**Verdict: KEEP + ADOPT.** Consecutive-failure ejection stays (Envoy's
default-on type). ADOPTED `max_ejection_percent` (default 50% for
vane's typically small clusters where Envoy's 10% would usually mean
"eject nobody"; always at least one host): the cap-gated ejection
keeps the last healthy backend in rotation. Success-rate sweeps and
ejection-time backoff stay parked (trigger: measured flapping between
two detection types).

**Lean proof:** `ejection_cap_keeps_the_cluster_serving` — with a 50%
cap on a 2-backend cluster, the second backend past its threshold is
NOT ejected; a 100% cap still allows a full ejection.


## Interim release-train note (2026-10-16)

Iterations 1-5 produced config-struct and API changes (non_exhaustive
across the vane-control config family, OutlierConfig.max_ejection_
percent, weight-aware P2C in vane-router 0.7.0, the chunked smuggling
fix in vane-proto 0.7.0). Versions bumped for the train:
`vane-proto` 0.7.0, `vane-router` 0.7.0, `vane-control` 0.10.0,
`vane-proxy` 0.11.0 — published when the arc completes. `Listener
Config`'s non_exhaustive pattern (Default + field assignment) is now
the family-wide rule; `VaneConfig` stays exempt (parsed, never
constructed, downstream).


## Iteration 6 — TLS: post-quantum key exchange & cert lifecycle

**Pulled:** rustls defaults manual (X25519MLKEM768 via aws-lc-rs;
`prefer-post-quantum` feature makes it top-priority), rustls-post-
quantum 0.2.x (aws-lc-rs based; ML-KEM moved INTO rustls 0.23.22+),
draft-ietf-tls-mlkem-05 (ML-KEM-512/768/1024 NamedGroups, Nov 2025),
the X25519MLKEM768 I-D, and the deployment picture (Chrome +
Cloudflare default since 2024).

**Analysis:** harvest-now-decrypt-later makes PQ hybrid KEX relevant
to a proxy NOW — the frontend TLS leg carries exactly the traffic an
adversary would capture. Chrome/Cloudflare negotiate X25519MLKEM768 by
default; a rustls server on the ring provider (vane today) simply
never offers it. ACME lifecycle is already current: expiry-gated
hourly renewal (iteration 0 of this train), short-lived certificates
viable; OCSP stapling deliberately NOT built (LE retired OCSP Aug
2025 — researched and ruled out).

**Prior art:** Caddy (on-demand TLS, but no PQ), nginx (no PQ),
Envoy (PQ via BoringSSL X25519MLKEM768, default since 2024) — Envoy
is ahead here; rustls-based proxies need the provider switch.

**Verdict: ADOPT, feature-gated.** Three implementation routes exist:
(1) aws-lc-rs provider + `prefer-post-quantum` (official, but adds
cmake/NASM build requirements to every downstream build);
(2) `rustls-post-quantum` crate (aws-lc-rs based, official);
(3) `mlkem-tls` (ring-compatible kx group, third-party).
Route 1 is the defensible one (same vendor family as rustls, widely
deployed), implemented as a `post-quantum` cargo feature so the
default ring build is untouched — the switch changes the build matrix
(CI, docker, musl) and deserves its own gate cycle. Scheduled as its
own unit; NOT a drive-by.

**Lean proof (this iteration):** doc assertion in `vane-tls` at the
provider-install site recording the decision, the three routes, and
the trigger (feature work + build-matrix validation). The follow-up
feature gets its own tests when it lands.


## Iteration 7 — Memory & zero-copy data path

**Pulled:** io_uring zero-copy send (LWN 877167/879724 — 1.88-2.44x
over msg_zerocopy, best with fixed buffers, kernel 6.4+); io_uring
zero-copy RECEIVE (kernel docs, kernel 6.14+, ifq per NIC queue with
flow steering); Seastar's asymmetric_io_uring backend (July 2026 —
I/O offloaded to dedicated networking cores); Google's Snap / eRPC
lineage for the shared-nothing thread-per-core model.

**Analysis:** vane's relay path is already zero-copy where it pays:
registered fixed buffers for every read/write, kernel-only splice(2)
for L4 tunnels (bytes never enter user space), a data-buffer freelist
(no per-read allocation). The remaining kernel features are SEND_ZC
(send without the kernel copying out of our registered buffer) and
ZC Rx (receive without the kernel copying in). SEND_ZC is a clean
increment; ZC Rx requires per-NIC-queue flow steering that conflicts
with vane's SO_REUSEPORT thread-per-core accept model.

**Verdict: KEEP.** Registered fixed buffers + splice is the current
state of the art for this workload shape. Two parked items recorded
AT THE INTEGRATION POINT (uring.rs module doc): SEND_ZC behind a
measured large-body CPU regression trigger; ZC Rx behind NIC-support
+ measured receive-memcpy trigger. Seastar's dedicated-I/O-core
architecture is a different point in the design space (vane's
thread-per-core handlers do minimal syscall work already).

**Lean proof:** the parked triggers live as a doc assertion in
`uring.rs` — the integration point where both features would land.

Entries are appended per iteration; the table is filled at the end of
each so the ledger always reflects verified state.
