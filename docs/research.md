# Research notes — engineering-loop analysis

Findings from current research and how they map to vane's roadmap.
Updated per engineering-loop iteration.

## io_uring zero-copy receive (kernel 6.8+)

- **Source**: netdevconf 0x17, kernel docs `iou-zcrx`, LWN 879724
- **Finding**: io_uring ZC RX removes the kernel-to-user copy on the
  network receive path — packet data lands directly in userspace
  memory. Available since Linux 6.8 (nio / napi busy-poll).
- **vane mapping**: the engine already uses io_uring for
  submit/complete. ZC RX is the **next accept-path optimization**:
  configure `IORING_SETUP_ZCRX` + registered buffers on the
  engine's uring instances. Measured target: the h1 relay's
  ~9% malloc share (the kernel→user copy is part of that).
- **Trigger**: kernel 6.8+ on the target host + a measured profile
  showing the copy in the top symbols.
- **Status**: parked (kernel-version dependent).

## QUIC connection migration

- **Source**: IMC 2023 "An Analysis of QUIC Connection Migration in
  the Wild", ResearchGate QUIC survey (Garifullin et al.)
- **Finding**: QUIC connection migration allows an ongoing
  connection to switch IP address without disruption (Wi-Fi → 5G,
  pod rescheduling). In the wild, ~2% of connections migrate; most
  fail due to NAT rebinding (new port = new path validation).
- **vane mapping**: the mesh-QUIC bridge uses a single quinn
  connection per backend. Connection migration would let the bridge
  survive backend pod rescheduling without re-handshake — quinn
  supports it (path validation) but the bridge doesn't enable
  migration-wide transport parameters.
- **Trigger**: mesh deployments where backends are dynamically
  scheduled (k8s pods).
- **Status**: parked (enable `transport_config.allow_migration()`
  in the bridge when the use case materializes).

## Pingora (Cloudflare) — architecture validation

- **Source**: github.com/cloudflare/pingora
- **Finding**: Pingora is a Rust proxy *framework* (build-your-own,
  not turnkey), serving 1T+ req/day at Cloudflare. Architecture:
  shared-nothing multi-process (not multi-thread), SO_REUSEPORT
  accept, zero-copy where possible, no GC.
- **vane mapping**: vane's architecture (per-core SO_REUSEPORT
  workers, zero-copy buffer pool, no GC) is validated as the
  same design class Pingora uses at planetary scale. Key
  difference: Pingora is a framework (you write the proxy);
  vane is a turnkey proxy (you write config).
- **Action**: none — architecture validated.

## Head-of-line blocking elimination (h2/h3)

- **Source**: multiple (HTTP/3 adoption reports, QUIC surveys)
- **Finding**: h2's single TCP stream causes head-of-line blocking
  under packet loss; h3's independent QUIC streams eliminate it.
  This is the primary user-visible benefit of h3 adoption.
- **vane mapping**: already shipped — the h3 edge, h3 upstream
  bridge, and mesh-over-QUIC all use independent streams. The
  benchmark harness measures this (the mesh-QUIC path sustains
  throughput under packet loss conditions that would stall h2).
- **Status**: shipped.

## Remaining research queue

| Topic | Relevance | Trigger |
|---|---|---|
| io_uring multishot accept | accept-path scaling | quiet-host benchmark |
| kernel TLS (kTLS) TX offload | TLS throughput | NIC support check |
| SO_REUSEPORT + eBPF reuseport hash | accept-path balancing | measured accept-path gap |
| h3 metadata (QPACK tuning) | h3 header compression | h3 profile |

## eBPF SO_REUSEPORT load balancing

- **Source**: Vincent Bernat (2026), Cloudflare tubular blog, APNIC
  blog series, ebpf.io BPF_PROG_TYPE_SK_REUSEPORT docs
- **Finding**: the kernel's default SO_REUSEPORT hashing is a 4-tuple
  hash — it distributes connections by flow, not by load. An eBPF
  `sk_reuseport` program can override the selection (consistent
  hash, least-connections, BPF map lookup). Cloudflare's tubular is
  a production eBPF reuseport manager.
- **vane mapping**: vane already uses SO_REUSEPORT per-worker accept
  (the default 4-tuple hash). The eBPF upgrade would provide
  connection-level load balancing across workers — relevant when
  workers have unequal load (e.g., hot upstream connections).
- **Trigger**: measured accept-path imbalance (one worker
  saturating while others idle).
- **Status**: parked (the default hash is sufficient for
  homogeneous traffic; eBPF adds infrastructure complexity).

## WebSocket over HTTP/3 (RFC 9220) — upstream-blocked

- **Source**: RFC 9220 (Bootstrapping WebSockets with HTTP/3); h3
  crate `ext::Protocol` (all published versions 0.0.3 → current)
- **Finding**: RFC 9220 bootstraps WebSocket via extended CONNECT with
  `:protocol = websocket`. The `h3` crate parses `:protocol` through
  `Protocol::from_str`, which accepts exactly two values —
  `webtransport` and `connect-udp` — and returns `InvalidProtocol` for
  anything else. `proto/headers.rs::try_value` turns that into
  `HeaderError::invalid_value`, so the request is rejected at the
  HEADERS-frame layer: application code never sees it, and the stream
  dies with a malformed-header error rather than the 501 the RFC
  recommends for unsupported protocols (§4: "the server SHOULD respond
  ... 501"). The wire machinery (SETTINGS_ENABLE_CONNECT_PROTOCOL,
  full-duplex DATA on CONNECT streams) already exists in h3 0.0.8 —
  the value enum is the only gap.
- **vane mapping**: would ride the existing tunnel arm (the h1-side
  upgrade relay, `parse_upstream_head` → `BodyFraming::Tunnel`) with an
  h3 bidi stream on the client side; the edge would translate the
  extended CONNECT into a classic RFC 6455 upgrade toward the upstream.
  Scoped and ready otherwise.
- **Unblock paths**: (a) upstream PR adding `websocket` to
  `Protocol::from_str` — one match arm plus a test; (b) vendored h3
  with that patch (other projects have done exactly this). Not worth
  vendoring for one enum value while the crate is pre-1.0 and moving.
- **Status**: parked upstream-blocked (same calibration as the h3
  error-code propagation gap). Re-check on any h3 version bump.
