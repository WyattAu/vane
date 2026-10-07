# Security Policy

## Reporting

Report vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repository's Security tab). Please
include a reproducing input where possible — the fuzzing corpora make
most parser issues trivially reproducible.

## Parser assurance (fuzzing)

Every byte parser that faces untrusted input is fuzzed with
libFuzzer (`fuzz/`), run in CI (`fuzz-smoke` job, 60 s per target)
and extendable locally:

    cargo fuzz run hpack -- -max_total_time=300

| Parser | Surface | Fuzz target |
|---|---|---|
| HPACK decoder | h2 header blocks | `hpack` |
| h2 frame validation | frame header/payload bounds | `frame` |
| **h2 connection state machine** | stream lifecycle, flow control, concurrency | `h2_conn_server` |
| h1 request head | edge request parsing | `parse_request` |
| upstream h1 response head | relay parsing | `parse_response` |
| h1pool response parse | h3-edge pooled upstream | `h1pool_response` |
| xDS protobuf decoders | control-plane resources | `xds_decode` |
| SHM descriptors | shared-memory transport | `shm_descriptors` |
| route lookup | host/path matching | `route_lookup` |
| access-log rendering | JSON escaping | `access_record` |
| **DER certificate SAN walk** | peer-presented TLS certificates | `cert_san` |

Regression seeds for every fuzz-found bug live in
`fuzz/corpus/regression/`. Found so far: a protobuf length-varint
overflow in the xDS decoders (fixed, 2026-10-03), an HPACK dynamic
table size ordering violation (fixed, 2026-09), out-of-range
status handling in the h1pool response parser (fixed, 2026-10-03), an
HPACK string-length arithmetic overflow reachable from a 12-byte
header block (fixed, 2026-10-07, `fuzz` nightly run), and an
over-long DER GeneralName length in `spiffe_id` — the parser that
reads peer-presented certificates (fixed, 2026-10-07, found by a
same-class sweep after the HPACK fix; the DER walk had no fuzz target,
so `cert_san` was added).

The sweep is the standing lesson: after any parser bug, audit every
other hand-rolled parser for the same class — unchecked arithmetic on
wire-derived lengths, direct slice indexing where siblings use `get()`.
The second bug is always the cheaper one to fix.

## Supported versions

The latest release line only. Patches land as patch releases.

## Dependency advisories (cargo audit, 2026-10-04)

| Advisory | Crate | Severity | Status |
|---|---|---|---|
| RUSTSEC-2026-0285 | rustls 0.23.44 | 5.3 | **fixed** (0.23.45) |
| RUSTSEC-2026-0315/0316/0325/0326/**0327** | wasmtime 48.0.1 | up to **9.3 (critical)** | **fixed** (48.0.5) — a native stack buffer overflow in the component model's async-lifted callback result count. The `wasm` feature is optional, so none of these were in a default build |
| RUSTSEC-2025-0134 | rustls-pemfile 2.2 | unmaintained | **removed** — no safe upgrade existed, but the code moved into `rustls-pki-types` behind `PemObject`; all six call sites in `vane-tls` and the integration tests migrated, and the dependency is gone from the graph |
| RUSTSEC-2023-0071 | rsa 0.9.10 | 5.9 | **dev-dependency only** — `vane-filters` pulls it in to generate an RS256 key so the JWT filter's RS256 verification has real coverage. Not in any shipped build graph. Recorded in `.cargo/audit.toml` and `deny.toml` with that justification; remove both entries if `rsa` ever becomes a normal dependency |

`cargo audit` runs in CI (`dependency audit`) and `cargo deny` in
`quality / deny`; both are configured from `.cargo/audit.toml` and
`deny.toml`, so an exception is recorded once with its justification and
applies locally and in CI.
