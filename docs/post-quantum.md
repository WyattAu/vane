# Post-quantum key exchange (the `post-quantum` feature)

vane ships the **ring** crypto provider by default. The
`post-quantum` cargo feature switches the process rustls provider to
**aws-lc-rs** with rustls's `prefer-post-quantum`, which makes
**X25519MLKEM768** the highest-priority key-exchange group on every
TLS listener *and* on the HTTP/3 (QUIC) endpoint.

## Why a proxy needs this now

Harvest-now-decrypt-later traffic is being collected today and
decrypted later with a CRQC-capable computer (Natlana 2022, "harvest
now, decrypt later"). The record that matters is the one an observer
can capture and store: the **frontend TLS leg**. If that leg is
classical, the post-quantum migration happening behind you (Chrome
and Cloudflare have negotiated X25519MLKEM768 by default since 2024)
does not protect the ciphertexts your proxy sees. vane's edge is
exactly where that leg terminates.

X25519MLKEM768 is a *hybrid*: an X25519 share AND an ML-KEM-768
share. Against a CRQC attacker the ML-KEM half holds; against a
"break ML-KEM today" attacker the X25519 half holds. It is a
migration group, not a permanent one; see the parked trigger in
`crates/vane-tls/src/lib.rs` for when to revisit the ordering.

## Building and installing

```bash
# default: ring provider (no cmake needed)
cargo install vane-proxy --version 0.12.0 --locked

# post-quantum: aws-lc-rs provider
cargo install vane-proxy --version 0.12.0 --locked --features post-quantum
vane --version   # vane 0.12.0
```

**aws-lc-rs needs a C toolchain** (cmake; a compiler; NASM only for
the Windows x86-64 asm path). That is the reason the feature is
opt-in: the default build matrix, the musl container, and every
platform without cmake are untouched. Feature-gated builds are also
what CI's `--all-features` runs exercise, so the feature cannot rot
unnoticed.

No configuration change is needed; the provider install is
process-wide and happens before `main` (a `ctor` in `vane-tls`), so
every listener, every ACME renewal, every mesh handshake, and the
QUIC endpoint all pick it up. Explicit `install_crypto_provider()`
calls in the startup path are idempotent no-ops under the feature.

## Verifying it yourself

Reproduce the release verification with any TLS client that speaks
the group (`openssl` 3.5+ names it `X25519MLKEM768`):

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -keyout key.pem -out cert.pem -days 2 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost,IP:127.0.0.1

cat > vane.toml <<'TOML'
[[listeners]]
address = "127.0.0.1:18443"
[listeners.tls]
cert = "/path/cert.pem"
key  = "/path/key.pem"
[clusters.up]
backends = ["127.0.0.1:18081"]
[[routes]]
pattern = "/*rest"
cluster = "up"
TOML

vane run -c vane.toml &
echo | openssl s_client -connect 127.0.0.1:18443 -alpn http/1.1 \
  -groups X25519MLKEM768 2>/dev/null | grep -a "Negotiated TLS1.3 group"
# Negotiated TLS1.3 group: X25519MLKEM768
```

Measured against the published v0.12.0 binaries (installed from
crates.io, not a local build):

| server build | client offers | negotiated |
|---|---|---|
| ring (default) | `X25519MLKEM768` only | no shared group; handshake fails |
| ring (default) | `x25519` | classical |
| `post-quantum` | `X25519MLKEM768` | **X25519MLKEM768** |
| `post-quantum` | `X25519MLKEM768:x25519:secp256r1` | **X25519MLKEM768** (hybrid leads) |
| `post-quantum` | `x25519` only | classical (classical-only clients still work) |

The in-repo proof is `pq_provider_offers_x25519_mlkem768` in
`crates/vane-tls`: it asserts the installed provider's first kx group
is the hybrid **iff** the feature is on, so the contract cannot be
silently lost by a rustls upgrade that reorders the default list.

## Operational notes

* **Two vane instances can share one port.** Listeners use
  `SO_REUSEPORT` (`crates/vane-kernel/src/net.rs`), so the kernel
  load-balances between processes; useful for zero-copy dispatch,
  surprising when you are A/B testing two builds on the same port.
  Use different ports when comparing builds.
* **Clients that do not offer the hybrid still connect.** rustls's
  group list is the server's preference, not a requirement; the
  classical groups stay in the list below the hybrid.
* **Certificates are unchanged.** The feature switches key exchange
  and the aws-lc-rs signing/verification primitives. OCSP stapling
  is intentionally absent (Let's Encrypt retired OCSP in 2025);
  renewal is hourly and expiry-gated (see `crates/vane/src/acme.rs`).
* **FIPS 140-3** is not claimed: that requires the aws-lc-fips-sys
  backend and a validated module, which this build does not use.
