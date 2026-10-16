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
| 2 | HTTP/2 conformance & flood defense | (fill) | (fill) |
| 3 | QUIC / HTTP/3 transport | (fill) | (fill) |
| 4 | Load balancing (P2C) | (fill) | (fill) |
| 5 | Outlier detection & circuit breaking | (fill) | (fill) |
| 6 | TLS: post-quantum & cert lifecycle | (fill) | (fill) |
| 7 | Memory & zero-copy data path | (fill) | (fill) |
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

Entries are appended per iteration; the table is filled at the end of
each so the ledger always reflects verified state.
