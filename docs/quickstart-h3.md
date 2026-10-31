in-process bridge). Both are default features; no extra build flags.
exactly as they do on h2; wrong prefix → `403`, no certificate →
- **Connection reuse**; one QUIC handshake per backend; requests
- **Health ownership**; the bridge actively probes
- **Material rotation**; the bridge fingerprints the `h3_tls` files
(`mesh.http3` and `h3_tls` are mutually exclusive; mesh owns the
  (they would probe the bridge, not the backends); the bridge's own
- h2: `scripts/h2spec_run.sh`; 145/145 pass, 0 failed (146 cases, 1
- h3: `scripts/h3spec_run.sh`; see docs/h3-design.md for the