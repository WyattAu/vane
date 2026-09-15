# Security Policy

vane is an L4/L7 edge proxy: it terminates untrusted client traffic and
forwards it to trusted backends. Its security posture is "fail closed,
keep the data plane panic-free, keep the control plane small."

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | yes       |
| < 0.2   | no        |

## Reporting a Vulnerability

- Use GitHub's private vulnerability reporting for this repository
  (**Security → Report a vulnerability** at
  `github.com/WyattAu/vane`), or open a coordinated-disclosure request
  via a maintainer if you cannot use it.
- Please include: affected version/commit, reproduction steps or PoC,
  observed vs expected behavior, and your assessment of impact.
- You will get an acknowledgment within **72 hours** and a remediation
  plan (or a reasoned decline) within **14 days**.
- Please do not open public GitHub issues for suspected vulnerabilities;
  coordinated disclosure is preferred. We credit reporters by default
  (opt out in your report).

In-scope: the data plane (HTTP/1.1 parsing, framing, request smuggling,
header handling), TLS termination, the admin plane, the xDS snapshot
API, and the plugin boundary. Out-of-scope: social engineering, attacks
requiring local shell on the proxy host, and vulnerabilities in the
Rust standard library or C library dependencies themselves.

## Hardening Defaults

- **Loopback admin plane.** `[admin] address` defaults to
  `127.0.0.1:9100`. Exposing it to other interfaces is a deliberate act.
- **Optional admin bearer auth.** Set `[admin] auth_token_file` (or the
  `VANE_ADMIN_TOKEN` env var) to require `Authorization: Bearer <token>`
  on every admin request; comparison is constant-time and failures
  return `401`. See `THREAT-MODEL.md` for the auth model.
- **Request-smuggling guard.** Ambiguous request framing is rejected
  with `400` before any upstream dial: `Content-Length` together with
  `Transfer-Encoding`, duplicate `Content-Length` headers (even
  self-consistent), multiple `Transfer-Encoding` headers, and transfer
  codings that do not end in `chunked`. `Transfer-Encoding` is treated
  as hop-by-hop: the upstream head is re-serialized with a normalized
  single `Transfer-Encoding: chunked` when (and only when) the request
  is chunked.
- **X-Forwarded-* sanitization.** Inbound `X-Forwarded-For`,
  `X-Forwarded-Proto`, and `X-Forwarded-Host` are stripped and replaced
  with edge-computed values (peer address; `https` on TLS-terminating
  listeners, `http` otherwise).
- **Readiness is real.** `/readyz` answers `503` until the route table
  is non-empty and listeners are bound; `/healthz` stays unconditional
  (`200`) so liveness never flaps on config state.

## Known Trade-offs

- **`panic = "abort"` + limited `expect` on the data plane.** The
  release profile aborts on panic (no unwinding across FFI/io_uring
  boundaries). A panic in the data plane therefore takes the process
  down and is restarted by the supervisor instead of being contained.
  We accept this trade-off deliberately: every panic path must carry a
  documented `INVARIANT:` (workspace lint denies `unwrap`, gates
  `expect`), and the threat model documents the resulting availability
  risk (see `THREAT-MODEL.md`, T-11).
- The admin plane ships an unauthenticated-by-default mode on loopback.
  Operators binding it to non-loopback addresses without setting a
  token own that risk (documented in `THREAT-MODEL.md`, OPEN-2).
