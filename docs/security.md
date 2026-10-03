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

Regression seeds for every fuzz-found bug live in
`fuzz/corpus/regression/`. Found so far: a protobuf length-varint
overflow in the xDS decoders (fixed, 2026-10-03), an HPACK dynamic
table size ordering violation (fixed, 2026-09), and out-of-range
status handling in the h1pool response parser (fixed, 2026-10-03).

## Supported versions

The latest release line only. Patches land as patch releases.
