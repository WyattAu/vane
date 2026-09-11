# Benchmarks

Methodology-first: every number here is reproducible with the commands
shown. All runs share one host, one upstream, and the same client
(`ab`, which caps the observable ceiling near ~70k rps on this class of
machine — treat absolute numbers as indicative, ratios as meaningful).

## Setup

- Upstream: threaded Rust echo server (10-byte body, keep-alive),
  direct ceiling measured first: **65–72k rps** — the shared client
  bottleneck; proxies are compared against this ceiling, not each
  other's absolute numbers.
- Client: `ab -k -c <conc> -n <reqs>`; three rounds, medians reported.
- Host: shared multi-tenant Linux box (results vary run-to-run ~±5%).

## vane vs nginx (reverse-proxy mode)

nginx configuration matters enormously and is often the hidden variable
in proxy benchmarks. Both configurations are reported:

| Scenario | vane (2 workers, pool) | nginx 2 workers, upstream keepalive 64 | nginx stock (no upstream keepalive) |
|---|---|---|---|
| keep-alive c=64 | 33–35k rps | 50–51k rps | 9–10k rps |
| keep-alive c=256 | 33.7k rps | 47k rps | — |
| short connections c=64 | 6.1k rps | 10.5k rps | — |
| direct upstream (no proxy) | 65–72k rps | — | — |

Honest reading:

- **nginx + upstream keepalive beats vane's keep-alive path by ~1.4×**
  (proxy-loop overhead: per-request parse + route + filter pipeline in
  the handler vs nginx's C loop). This is vane's main optimization
  target — tracked for v0.4 perf work.
- **vane beats stock nginx ~3.5×** — stock `proxy_pass` opens a fresh
  upstream connection per request, which is the default most
  deployments run.
- Short connections are accept-bound; nginx's mature accept path wins.

## Reproduce

```bash
# upstream
./stub 16082 &                     # scripts/bench stub or any keep-alive server
# vane
./target/release/vane run -c bench.toml &   # listeners :16080, workers 2
# nginx (tuned) — keepalive 64 upstream block
docker run --network host -v nginx.conf:/etc/nginx/nginx.conf nginx:alpine
ab -n 20000 -c 64 -k http://127.0.0.1:16080/
```

See `scripts/bench.sh` for the full harness.

## Other measurements

| Bench | Result | Requirement |
|---|---|---|
| HTTP/1.1 head parse | ~266 ns | `PR-01` |
| Route lookup @ 10k routes | ~223 ns | `CP-01` |
| SHM sidecar RTT (512 B) | ~2.8 µs | `IP-01` |
| Config generation swap | ~1 µs | `CP-02` |

(Criterion benches, dev profile.)
