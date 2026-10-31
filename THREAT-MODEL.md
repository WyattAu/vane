# Threat Model: vane
## Surface 1: Data plane
| T-1 | Request smuggling via ambiguous framing (`Content-Length` + `Transfer-Encoding`, duplicate/divergent `Content-Length`, multiple `Transfer-Encoding`, non-final-chunked TE); frontends and backends disagree on framing precedence | Tampering / Elevation | `RequestView::parse_in` runs a framing-ambiguity guard before any routing or upstream dial; ambiguous heads return `ParseError::ConflictingFraming` → `400` + connection close | `cl_plus_te_is_rejected`, `duplicate_content_length_divergent_is_rejected`, `duplicate_content_length_identical_is_rejected`, `multiple_transfer_encoding_headers_are_rejected`, `transfer_encoding_not_ending_in_chunked_is_rejected`, `case_insensitive_framing_header_names_are_caught` (vane-proto); `cl_plus_te_request_is_rejected_end_to_end` (vane/tests/e2e.rs) |
## Surface 2: Admin plane
is configured it gates the *entire* admin router (probes included) -
lifetime as `Arc<str>`, so a token file revocation requires a restart -
## Surface 3: TLS termination
| T-11 | Key material exposure via hot reload races | Information Disclosure | Reload swaps an `Arc<rustls::ServerConfig>` in a shared slot; workers observe a consistent config snapshot; keys live only in memory + their configured files (mode is an operator concern, documented in deploy docs) | `tls_reload` crate tests (crates/vane/src/tls_reload.rs) |
## Surface 4: xDS API
| T-13 | Snapshot replaces routes with an empty set (accidental or malicious) | DoS | Allowed by design (drain semantics); `apply_snapshot_state` is the only writer and `/readyz` flips to `503` when the table empties; detection is built in | `xds_snapshot_applies_and_replaces` + `readyz_503_with_empty_routes` |
## Surface 5: Plugins
| T-15 | Malicious plugin exfiltrates request data | Information Disclosure | **Open by design**: plugins are in-process and see request bytes they are handed. Trusting a plugin is equivalent to trusting a dependency; gate module provenance at deploy time |; (see OPEN-4) |
- Parse paths (`httparse`, framing validation) return typed errors -
## OPEN RISKS (missing mitigations: not fabricated)
- **OPEN-1; upstream identity is network-trust.** Backends are
- **OPEN-2; admin auth is opt-in.** With no token configured the
- **OPEN-3; no admin rate limiting / audit log.** Authenticated or
- **OPEN-4; plugin capability model is binary.** A loaded plugin can
- **OPEN-5; RFC 7239 `Forwarded` header is not stripped.** The
- DoS at line rate (SYN floods, bandwidth exhaustion); kernel and
  the connection is not reused; desynchronization requires the