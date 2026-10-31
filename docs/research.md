# Research notes: engineering-loop analysis
  network receive path; packet data lands directly in userspace
  survive backend pod rescheduling without re-handshake; quinn
## Pingora (Cloudflare): architecture validation
- **Action**: none; architecture validated.
- **vane mapping**: already shipped; the h3 edge, h3 upstream
  hash; it distributes connections by flow, not by load. An eBPF
  connection-level load balancing across workers; relevant when
## WebSocket over HTTP/3 (RFC 9220): upstream-blocked
  `Protocol::from_str`, which accepts exactly two values -
  `webtransport` and `connect-udp`; and returns `InvalidProtocol` for
  full-duplex DATA on CONNECT streams) already exists in h3 0.0.8 -
  `Protocol::from_str`; one match arm plus a test; (b) vendored h3