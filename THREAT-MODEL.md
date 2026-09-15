# Threat Model — vane

Status: **v1.0** · Method: STRIDE over the deployed surfaces: (1) the
data plane (HTTP/1.1 edge, L4 splice, HTTP/2 edge), (2) the admin
plane, (3) TLS termination, (4) the xDS control API, (5) the plugin
boundary.

Trust boundaries: (1) untrusted **clients** on the public internet,
(2) semi-trusted **upstreams** (backends vane routes to), (3) the
**operator** who writes configuration and can reach the admin plane,
(4) **plugin authors** whose Wasm/natively-linked code runs in-process,
(5) the Rust dependency stack. vane is an edge proxy: its job is to be
the point at which untrusted input becomes either a strict, normalized
forwarded request or a refusal.

Modeling conventions follow the house style: every threat lists the
mitigation that exists **in the code today** and the test that verifies
it; risks without mitigations are listed as OPEN, never papered over.

## Assets

| ID | Asset | Example |
|----|-------|---------|
| A1 | Routing integrity | A smuggled request bypassing route ACLs and reaching a backend vane would have refused |
| A2 | Client identity fidelity | Upstreams trusting a spoofed `X-Forwarded-For` for rate limiting / allowlists |
| A3 | Availability of the proxy | Malformed framing or TLS handshakes exhausting workers; panic=abort making a data-plane panic fatal |
| A4 | Admin-plane integrity | `POST /xds/snapshot` re-routing traffic to an attacker-chosen backend |
| A5 | TLS private keys | cert/key PEM files on disk, hot-reloaded in place |
| A6 | Backend isolation | The admin plane and data plane sharing a process |

## Surface 1 — Data plane

| # | Threat | Category | Mitigation | Verifying test |
|---|--------|----------|------------|----------------|
| T-1 | Request smuggling via ambiguous framing (`Content-Length` + `Transfer-Encoding`, duplicate/divergent `Content-Length`, multiple `Transfer-Encoding`, non-final-chunked TE) — frontends and backends disagree on framing precedence | Tampering / Elevation | `RequestView::parse_in` runs a framing-ambiguity guard before any routing or upstream dial; ambiguous heads return `ParseError::ConflictingFraming` → `400` + connection close | `cl_plus_te_is_rejected`, `duplicate_content_length_divergent_is_rejected`, `duplicate_content_length_identical_is_rejected`, `multiple_transfer_encoding_headers_are_rejected`, `transfer_encoding_not_ending_in_chunked_is_rejected`, `case_insensitive_framing_header_names_are_caught` (vane-proto); `cl_plus_te_request_is_rejected_end_to_end` (vane/tests/e2e.rs) |
| T-2 | `Transfer-Encoding` relayed verbatim (attacker-controlled coding list, e.g. obfuscated `xchunked`) desynchronizes the upstream body parser | Tampering | TE is treated as hop-by-hop: stripped from the forwarded head and re-emitted as a normalized `Transfer-Encoding: chunked` exactly when the validated framing is chunked | `cl_plus_te_request_is_rejected_end_to_end`; serialization covered by `build_upstream_head` strip list (proxy.rs) |
| T-3 | Client spoofs `X-Forwarded-For` / `X-Forwarded-Proto` / `X-Forwarded-Host` to impersonate another client or an HTTPS origin at the backend | Spoofing | Inbound `X-Forwarded-*` headers are stripped in the upstream head; the edge injects exactly one value per header: peer IP, and listener-derived scheme (`https` iff the connection terminated TLS) | `inbound_x_forwarded_headers_are_sanitized_end_to_end` (e2e.rs); `tls_listener_stamps_https_forwarded_proto` (full_stack_inproc.rs); `forwarded_proto_reflects_listener_scheme` (vane-filters) |
| T-4 | Header flood / oversized heads exhaust memory | DoS | Fixed 32 KiB head cap (`MAX_HEAD_BYTES`) → `431`; bounded pre-connect body buffer (`REQ_PENDING_CAP` 1 MiB) → `413`; fixed 64-slot header storage | existing parse suite (`partial_then_complete`, `garbage_is_error`); caps asserted in worker error paths |
| T-5 | Unroutable or hostile targets chosen via crafted `Host`/path | Elevation | Routing happens only through the compiled route table; no connector honors client-supplied absolute-form targets; unmatched → `404` | `no_route_is_404` (e2e.rs); `rejects_unknown_cluster` (vane-control) |

## Surface 2 — Admin plane

| # | Threat | Category | Mitigation | Verifying test |
|---|--------|----------|------------|----------------|
| T-6 | Unauthenticated `POST /xds/snapshot` reconfigures the router (traffic redirection, ACL removal) | Elevation / Tampering | Optional bearer auth: with `[admin] auth_token_file` (or `VANE_ADMIN_TOKEN`) set, every admin request requires `Authorization: Bearer <token>`; comparison is constant-time (length check + XOR fold, no early data exit) | `auth_rejects_missing_and_wrong_tokens`, `auth_accepts_correct_token`, `auth_gates_xds_snapshot`, `constant_time_eq_matches_std_eq` (admin_unit.rs / admin.rs) |
| T-7 | Admin plane exposed to the network by default | Information Disclosure / Elevation | Default bind is `127.0.0.1:9100`; exposure requires an explicit config change (verified default in `AdminConfig::default`) | `parses_and_validates` + default assertion (`assert_eq!(cfg.admin.address, "127.0.0.1:…")` in server env-override tests) |
| T-8 | Timing side channel leaking the admin token | Information Disclosure | `constant_time_eq` folds XOR over the full token; only the length comparison short-circuits (length is not treated as secret) | `constant_time_eq_matches_std_eq` |
| T-9 | Fake readiness hides a broken proxy (stale "ready" string) | Repudiation (operational) | `/readyz` computes readiness live: non-empty route table **and** listeners bound; otherwise `503` + JSON naming the missing halves. `/healthz` stays unconditional `200` (liveness ≠ readiness) | `readyz_ok`, `readyz_503_with_empty_routes`, `readyz_503_before_listeners_bound`, `healthz_stays_ok_when_not_ready` (admin_unit.rs) |

**Admin auth model.** Auth is opt-in and all-or-nothing: when a token
is configured it gates the *entire* admin router (probes included) —
the gate is a single `axum` middleware in front of all routes, so
sensitive endpoints cannot be reached by skipping an individually
guarded handler. Tokens are read once at startup (file contents
trimmed, or `VANE_ADMIN_TOKEN` env override) and held for the process
lifetime as `Arc<str>`, so a token file revocation requires a restart —
documented, deliberate (no per-request file I/O on any path). Operators
serving unauthenticated load-balancer probes should keep the token
unset and rely on the loopback bind, or front the admin plane with
something that terminates the token check.

## Surface 3 — TLS termination

| # | Threat | Category | Mitigation | Verifying test |
|---|--------|----------|------------|----------------|
| T-10 | Protocol downgrade / plaintext confusion on TLS listeners | Spoofing | Each listener is exclusively TLS or plaintext (`[listeners.tls]` presence decides); TLS records are handled by rustls per-connection (`ServerConnection`), handshake failures drop the connection before any HTTP parsing | `tls_termination_serves_https`, `tls_listener_stamps_https_forwarded_proto` (full_stack_inproc.rs) |
| T-11 | Key material exposure via hot reload races | Information Disclosure | Reload swaps an `Arc<rustls::ServerConfig>` in a shared slot — workers observe a consistent config snapshot; keys live only in memory + their configured files (mode is an operator concern, documented in deploy docs) | `tls_reload` crate tests (crates/vane/src/tls_reload.rs) |

## Surface 4 — xDS API

| # | Threat | Category | Mitigation | Verifying test |
|---|--------|----------|------------|----------------|
| T-12 | Malformed snapshot corrupts the live route table | Tampering / DoS | Snapshots are parsed and semantically validated; on any error the live table is untouched and `400` carries the reason | `xds_snapshot_applies_and_replaces`, `config_dry_run_reports` (admin_unit.rs) |
| T-13 | Snapshot replaces routes with an empty set (accidental or malicious) | DoS | Allowed by design (drain semantics); `apply_snapshot_state` is the only writer and `/readyz` flips to `503` when the table empties — detection is built in | `xds_snapshot_applies_and_replaces` + `readyz_503_with_empty_routes` |

## Surface 5 — Plugins

| # | Threat | Category | Mitigation | Verifying test |
|---|--------|----------|------------|----------------|
| T-14 | Guest (Wasm) code blocks or panics the worker | DoS | Guests run on the `wasmtime` boundary with typed `GuestVerdict` results; guest errors are logged and skipped, never propagated as panics | plugin paths in proxy.rs (`on_request_with_headers` error arm) |
| T-15 | Malicious plugin exfiltrates request data | Information Disclosure | **Open by design**: plugins are in-process and see request bytes they are handed. Trusting a plugin is equivalent to trusting a dependency — gate module provenance at deploy time | — (see OPEN-4) |

## The panic=abort trade-off (explicit, not accidental)

The release profile sets `panic = "abort"`. Rationale: the data plane
crosses io_uring/FFI boundaries and share-nothing workers where
unwinding would leave kernel-registered buffers, half-drained rings,
and pooled fds in states the recovery code cannot prove consistent;
abort-and-restart (systemd/k8s) is the honest recovery. Consequence: a
data-plane panic is an availability event, not a contained error.
Controls that make this acceptable:

- Workspace lints deny `unwrap` (`clippy::unwrap_used = deny`) and gate
  `expect` to documented startup invariants; `missing_docs` is warned
  (kept at zero).
- Every non-test `expect` must carry an `INVARIANT:`-style
  justification at the site.
- Parse paths (`httparse`, framing validation) return typed errors —
  attacker-controlled bytes never pick a panic arm. Fuzz targets under
  `fuzz/` guard the request/response head parsers.

Residual risk: an unknown panic path in worker code takes down the
process (restart loop under a supervisor). This is A3's accepted
residual and is monitored via process-restart alerts, not in-process.

## OPEN RISKS (missing mitigations — not fabricated)

- **OPEN-1 — upstream identity is network-trust.** Backends are
  addressed by IP:port from config; there is no upstream mTLS or
  service identity verification on the default path. A compromised
  backend network segment can impersonate a backend. Mitigation is
  deployment-level today.
- **OPEN-2 — admin auth is opt-in.** With no token configured the
  admin plane is unauthenticated (loopback bind is the only barrier).
  Anyone who can reach the bind address can reconfigure routing.
- **OPEN-3 — no admin rate limiting / audit log.** Authenticated or
  not, repeated xDS mutations are not throttled and (beyond access
  logs) not separately audit-trailed.
- **OPEN-4 — plugin capability model is binary.** A loaded plugin can
  observe everything it is given; there is no per-plugin capability
  declaration or network/egress sandbox beyond wasmtime's default.
- **OPEN-5 — RFC 7239 `Forwarded` header is not stripped.** The
  X-Forwarded-* family is sanitized, but a client-supplied standard
  `Forwarded` header is relayed verbatim (harmless to framing, but
  upstreams that honor it could be misled). Same class of fix applies.

## Out of Scope

- Backend application security (vane forwards; it does not sanitize
  payloads).
- Host/OS hardening (file permissions on key material, cgroup limits,
  supervisor configuration).
- DoS at line rate (SYN floods, bandwidth exhaustion) — kernel and
  upstream scrubbing territory.
- The Rust toolchain and third-party crate internals (`cargo deny`
  advisory auditing covers known CVEs; see `deny.toml`).

## Residual Risks

- Chunked request bodies are relayed verbatim after head validation;
  chunk-extension smuggling *within* an accepted chunked body relies on
  the upstream parser being conformant (the terminal `0\r\n\r\n` is
  observed for framing completion, extensions are not interpreted).
- `Content-Length` request bodies are trusted after validation; a
  client that sends fewer bytes than declared gets an idle timeout, and
  the connection is not reused — desynchronization requires the
  ambiguity T-1 rejects, which is the load-bearing mitigation.
- Admin tokens live as long as the process; rotating a token file
  requires restart (documented above, tracked as follow-up work with
  OPEN-2).
