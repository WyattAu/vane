# Mesh mTLS design (M4, v0.6.0)

## Goal

Vane-to-vane (and vane-to-workload) upstream connections authenticate with
SPIFFE identities over mutual TLS, with rotation and no connection churn.

## Identity model

- **SVID source (phase 1)**: file-based — the operator/sidecar mounts
  `cert.pem`/`key.pem`/`bundle.pem` from a SPIRE agent workload
  registration (`/etc/vane/spiffe`). No new protocol code.
- **SVID source (phase 2)**: SPIFFE Workload API over the Unix socket
  (`/run/spire/agent-sockets/workload_api.spiffe.io`): X.509SVID stream
  (watch), same rotation path as phase 1.
- Identity string: `spiffe://<trust-domain>/<ns>/<sa>` — encoded in the
  cert SAN URI. vane-tls gains `verify_spiffe_id(prefix)` for
  authorization (e.g. sidecars only accept `.../vane/`).

## Wire path

```
[cluster] upstream_tls = true
          spiffe_trust_domain = "example.org"
          spiffe_id_prefix   = "spiffe://example.org/vane/"

on_connected:
  vane_tls::Connector::with_client_cert(svid, key)
    → ALPN "vane-mesh" (h2c-style prior-knowledge h2 over mTLS)
    → after handshake: verify peer SAN URI against spiffe_id_prefix
      (fail → no failover to plaintext; mesh is TLS-or-nothing)
```

- Upstream pools: the existing dialer gains an mTLS mode; pooled
  connections revalidate the peer cert on reuse (rotation-aware:
  accept both old and new SVID during the grace window).
- Rotation: file mtime watcher (same machinery as the listener cert
  reload) swaps the connector's SVID; existing connections ride until
  natural close.

## Sidecar mode

- `vane sidecar` already proxies in/out; mesh adds: the inbound listener
  presents the workload SVID, the upstream connector requires the mesh
  ALPN. iptables/redirect config stays the deployment's job (documented
  recipes for kind + GKE).

## Milestones

1. ✅ vane-tls: client-cert connector + SPIFFE SAN verification (unit
   tested with in-test-generated CA/SVIDs, like the JWT RS256 tests).
2. ✅ Cluster `upstream_tls` + SPIFFE config surface; e2e: two vanes, mTLS,
   identity asserted in access logs. Verification failure (identity
   mismatch, TLS error) answers 502 while the response is unsent —
   TLS-or-nothing.
3. ✅ Workload API source (Unix socket watch) + rotation e2e
   (`workload_svid.rs`): `svid_socket` config; vane fetches the SVID
   at startup and a watcher re-materializes the PEM cache on every
   agent push; the per-dial file read picks rotations up with no
   restarts.
4. ✅ Authorization: per-route `allowed_spiffe_prefixes` — listeners
   with `tls.client_ca` require downstream client certs (missing cert
   → `certificate_required` alert, flushed before close); matched
   routes with prefixes answer 403 unless the caller's SPIFFE URI SAN
   starts with one of them (`spiffe_auth.rs`).
