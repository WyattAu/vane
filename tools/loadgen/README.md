# vane-loadgen

vane's benchmark load generator: HTTP/1.1, HTTP/2, and HTTP/3 clients, a
certificate generator, and a configurable upstream. Lives in its own
workspace (like `fuzz/`), so it is outside the published crate graph and
can be cloned standalone to benchmark other proxies.

## Binaries

| Binary | Purpose |
|---|---|
| `loadgen` | h1 keep-alive flood client (std threads, closed loop) |
| `h2load` | h2 over rustls (ring), one stream in flight |
| `h3load` | h3 over quinn, one request in flight per connection |
| `certgen` | self-signed `cert.pem`/`key.pem` (SANs `localhost`, `127.0.0.1`) |
| `upstream` | threaded h1 keep-alive upstream, configurable body size |

## Output contract

One line, 13 whitespace-separated fields. `scripts/bench_compare.sh`
parses positions `$5`/`$9`/`$11`/`$13` with awk, so they are load-bearing:

```
proto conns duration total rps ok conn_err read_err non200 0 p50_us 0 p99_us
```

`rps` counts *complete* responses; non-2xx responses are complete and are
therefore both counted and reported separately — a proxy that answers 503
fast must never look like a throughput win. Latencies are microseconds.

## Usage

```
loadgen ADDR CONNS DURATION PATH [HOST] [--method M] [--body N]
h2load  ADDR CONNS DURATION PATH SNI CERT [--method M] [--body N]
h3load  ADDR CONNS DURATION PATH SNI CERT [ignored-marker]
certgen DIR
upstream ADDR [BODY_BYTES]
```

The `--method`/`--body` flags are the breadth legs (POST 4 KB floods the
upload direction; a 64 KiB upstream body floods the download direction).

## Honest-measurement notes

- Closed-loop, one request in flight per connection, matching across
  protocols so h1/h2/h3 legs are comparable.
- Per-connection latency pools, merged once at the end: no lock on the
  hot path.
- Read/write timeouts so the generator always terminates — a server that
  stops mid-response must not hang the benchmark.
- h2 GETs pass `end_of_stream` on the request itself: the h2 crate queues
  an empty `send_data(Bytes::new(), true)` without emitting END_STREAM,
  which deadlocks a compliant server (found here first, kept as a
  comment at the call site).
- quinn endpoints must be built inside a tokio runtime; h3 request
  streams must be `finish()`ed explicitly.

## Build

```
cargo build --release
```

The bench scripts build it on demand; `scripts/bench.sh`,
`scripts/bench_compare.sh`, and `scripts/bench_bridge.sh` are the
harnesses that drive it.