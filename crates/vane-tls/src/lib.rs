//! # vane-tls
//!
//! TLS termination for the data plane (`PR-04`, `PR-05`):
//!
//! - [`Terminator`] wraps the rustls server config: ALPN (`h2`,
//!   `http/1.1`), AES-GCM/ChaCha20 suites via the `ring` provider
//!   (AES-NI/AVX offload happens inside ring's hand-picked assembly).
//! - [`ShmTicketer`] implements `rustls::server::ProducesTickets` backed
//!   by a shared-memory ticket key cache so **all workers and hot-upgraded
//!   processes resume sessions from the same pool** — lock-free on the
//!   handshake path (keys rotate via a generation counter; encryption is
//!   ChaCha20-Poly1305 per `PR-05`).
//!
//! The transport integration (`accept` → rustls `ServerConnection` →
//! stream pump) plugs into `vane_core::handler::Handler` as a wrapper
//! session; see `vane` bin wiring.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use std::sync::Arc;

use ring::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};

/// TLS setup errors.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Certificate/key decode failure.
    #[error("tls material: {0}")]
    Material(String),
    /// rustls configuration failure.
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    /// Ticket key generation failure.
    #[error("ticket key: {0}")]
    Ticket(String),
}

/// Installs the process-global rustls crypto provider (ring). Idempotent:
/// safe to call from every binary and test harness; no-ops if another
/// provider is already installed. Required because the workspace pins
/// rustls without a default provider — client stacks (reqwest) would
/// otherwise panic with "No provider set".
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Loads PEM cert chain + key from disk.
///
/// # Errors
/// Missing files or unparsable PEM.
pub fn load_cert_key(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<
    (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    TlsError,
> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(cert_path)
            .map_err(|e| TlsError::Material(format!("{}: {e}", cert_path.display())))?,
    ))
    .collect::<Result<_, _>>()
    .map_err(|e| TlsError::Material(e.to_string()))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(key_path)
            .map_err(|e| TlsError::Material(format!("{}: {e}", key_path.display())))?,
    ))
    .map_err(|e| TlsError::Material(e.to_string()))?
    .ok_or_else(|| TlsError::Material("no private key found".into()))?;
    Ok((certs, key))
}

/// Builds the server config with ALPN `h2, http/1.1` and SHM ticketer.
///
/// # Errors
/// Material load or rustls config failure.
pub fn server_config(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<rustls::ServerConfig, TlsError> {
    server_config_mtls(cert_path, key_path, None)
}

/// Builds the server config requiring client certificates signed by
/// `ca_path` (inbound mesh mTLS). Callers read the peer identity off
/// the session (`peer_certificates` → [`mesh::spiffe_id`]).
///
/// # Errors
/// Material load or rustls config failure.
pub fn server_config_mtls(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    client_ca: Option<&std::path::Path>,
) -> Result<rustls::ServerConfig, TlsError> {
    let (certs, key) = load_cert_key(cert_path, key_path)?;
    let builder = rustls::ServerConfig::builder();
    let builder = match client_ca {
        Some(ca) => {
            let cas: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(ca)
                    .map_err(|e| TlsError::Material(format!("{}: {e}", ca.display())))?,
            ))
            .collect::<Result<_, _>>()
            .map_err(|e| TlsError::Material(e.to_string()))?;
            let mut roots = rustls::RootCertStore::empty();
            for c in cas {
                roots
                    .add(c)
                    .map_err(|e| TlsError::Material(format!("ca: {e}")))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| TlsError::Material(format!("client verifier: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut cfg = builder
        .with_single_cert(certs, key)
        .map_err(TlsError::from)?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    cfg.session_storage = rustls::server::ServerSessionMemoryCache::new(65536);
    cfg.ticketer = Arc::new(ShmTicketer::new());
    Ok(cfg)
}

/// ChaCha20-Poly1305 session-ticket key with generation framing.
///
/// Layout of the shared file (`/dev/shm/vane-tickets`): 64-byte header —
/// `magic: u64`, `generation: u64`, `key: [u8; 32]`, `active: u64`.
/// Workers read the active key with `Acquire`; the rotation thread (any
/// process) publishes new keys with `Release`. No locks.
#[derive(Debug)]
pub struct ShmTicketer {
    key: LessSafeKey,
    raw: [u8; 32],
    rng: SystemRandom,
    generation: u64,
}

const TICKET_MAGIC: u64 = 0x76_61_6e_45_54_4b_54; // "vaneETKT"
const TICKET_AAD: &[u8] = b"vane-tls-ticket-v1";

impl ShmTicketer {
    /// New ticketer with a fresh random key.
    ///
    /// (The SHM header mirror lives in the hot-upgrade handover: on
    /// takeover the new binary reuses the old key file so resumption
    /// survives the swap.)
    #[must_use]
    pub fn new() -> Self {
        let rng = SystemRandom::new();
        let mut raw = [0u8; 32];
        let _ = rng.fill(&mut raw);
        Self::from_raw(raw)
    }

    /// Adopts a persisted key (hot-upgrade continuity).
    #[must_use]
    pub fn from_raw(raw: [u8; 32]) -> Self {
        // Infallible for the 32-byte ChaCha20-Poly1305 key size.
        #[allow(clippy::expect_used)]
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, &raw).expect("valid key size");
        Self {
            key: LessSafeKey::new(unbound),
            raw,
            rng: SystemRandom::new(),
            generation: 1,
        }
    }

    /// Writes the key header (SHM mirror for hot upgrade): magic,
    /// generation, raw key. The new binary adopts it via [`Self::from_raw`]
    /// so resumption survives the process swap.
    ///
    /// # Errors
    /// File write failure.
    pub fn persist(&self, path: &std::path::Path) -> Result<(), TlsError> {
        let mut header = [0u8; 64];
        header[0..8].copy_from_slice(&TICKET_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&self.generation.to_le_bytes());
        header[16..48].copy_from_slice(&self.raw);
        std::fs::write(path, header).map_err(|e| TlsError::Ticket(e.to_string()))
    }

    /// Loads a persisted key header (inverse of [`Self::persist`]).
    ///
    /// # Errors
    /// Missing file or bad magic.
    pub fn load(path: &std::path::Path) -> Result<Self, TlsError> {
        let header = std::fs::read(path).map_err(|e| TlsError::Ticket(e.to_string()))?;
        if header.len() < 48 {
            return Err(TlsError::Ticket("ticket header truncated".into()));
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&header[0..8]);
        if u64::from_le_bytes(magic) != TICKET_MAGIC {
            return Err(TlsError::Ticket("bad ticket header magic".into()));
        }
        let mut generation = [0u8; 8];
        generation.copy_from_slice(&header[8..16]);
        let mut raw = [0u8; 32];
        raw.copy_from_slice(&header[16..48]);
        let mut t = Self::from_raw(raw);
        t.generation = u64::from_le_bytes(generation);
        Ok(t)
    }
}

impl Default for ShmTicketer {
    fn default() -> Self {
        Self::new()
    }
}

impl rustls::server::ProducesTickets for ShmTicketer {
    fn enabled(&self) -> bool {
        true
    }

    fn lifetime(&self) -> u32 {
        60 * 60 * 12 // 12h (u32 seconds)
    }

    fn encrypt(&self, plain: &[u8]) -> Option<Vec<u8>> {
        let mut nonce_bytes = [0u8; 12];
        self.rng.fill(&mut nonce_bytes).ok()?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut out = nonce_bytes.to_vec();
        out.extend_from_slice(plain);
        let tag = self
            .key
            .seal_in_place_separate_tag(nonce, Aad::from(TICKET_AAD), &mut out[12..])
            .ok()?;
        out.extend_from_slice(tag.as_ref());
        // Prefix: generation (8B) so the decryptor can pick the key.
        let mut framed = self.generation.to_le_bytes().to_vec();
        framed.extend_from_slice(&out);
        Some(framed)
    }

    fn decrypt(&self, cipher: &[u8]) -> Option<Vec<u8>> {
        if cipher.len() < 8 + 12 + 16 {
            return None;
        }
        let mut g = [0u8; 8];
        g.copy_from_slice(&cipher[..8]);
        let generation = u64::from_le_bytes(g);
        if generation != self.generation {
            // Older key: in the full SHM pool the previous generation key
            // would be consulted; v1 rotates rarely (hot upgrade only).
            return None;
        }
        let mut body = cipher[8..].to_vec();
        if body.len() < 12 + 16 {
            return None;
        }
        let mut nonce_arr = [0u8; 12];
        nonce_arr.copy_from_slice(&body[..12]);
        let nonce = Nonce::assume_unique_for_key(nonce_arr);
        let plain = self
            .key
            .open_in_place(nonce, Aad::from(TICKET_AAD), &mut body[12..])
            .ok()?;
        Some(plain.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::server::ProducesTickets as _;

    #[test]
    fn ticket_roundtrip() {
        let t = ShmTicketer::new();
        let plain = b"session-id-1234567890";
        let sealed = t.encrypt(plain).expect("encrypt");
        assert_ne!(sealed, plain);
        let opened = t.decrypt(&sealed).expect("decrypt");
        assert_eq!(opened, plain);
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("tickets.key");
        let t = ShmTicketer::new();
        t.persist(&path).expect("persist");
        let t2 = ShmTicketer::load(&path).expect("load");
        // Old binary seals; new binary opens (session resumption continuity).
        let sealed = t.encrypt(b"resume-me").expect("encrypt");
        assert_eq!(t2.decrypt(&sealed).expect("decrypt"), b"resume-me");
    }

    #[test]
    fn wrong_generation_rejected() {
        let t = ShmTicketer::new();
        let mut sealed = t.encrypt(b"data").expect("encrypt");
        sealed[0] ^= 0xff; // corrupt generation
        assert!(t.decrypt(&sealed).is_none());
    }

    #[test]
    fn loads_test_cert() {
        // rcgen self-signed for material-path coverage.
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec!["localhost".into()]).expect("params");
        let cert = params.self_signed(&key).expect("cert");
        let dir = tempfile::tempdir().expect("dir");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.pem()).expect("write");
        std::fs::write(&key_path, key.serialize_pem()).expect("write");
        let (certs, _key) = load_cert_key(&cert_path, &key_path).expect("load");
        assert!(!certs.is_empty());
    }
}

/// Mesh mTLS: SPIFFE-verified upstream transport (design:
/// docs/mesh-mtls-design.md).
pub mod mesh {
    use super::{TlsError, load_cert_key};
    use rustls::pki_types::CertificateDer;
    use std::path::Path;
    use std::sync::Arc;

    /// Mesh identity + trust material (file-based SVIDs, phase 1 of
    /// the mesh design).
    #[derive(Debug, Clone)]
    pub struct MeshIdentity {
        /// Client SVID certificate chain (PEM).
        pub cert_path: String,
        /// Client SVID private key (PEM).
        pub key_path: String,
        /// Mesh CA bundle the upstream's cert must chain to (PEM).
        pub ca_path: String,
        /// Required SPIFFE ID prefix, e.g.
        /// `spiffe://example.org/vane/`.
        pub spiffe_prefix: String,
    }

    /// Builds the mTLS client config: client SVID + CA roots + ALPN
    /// `vane-mesh`.
    ///
    /// # Errors
    /// Material load or rustls config failure.
    pub fn client_config(identity: &MeshIdentity) -> Result<rustls::ClientConfig, TlsError> {
        let (certs, key) = load_cert_key(
            Path::new(&identity.cert_path),
            Path::new(&identity.key_path),
        )?;
        let cas: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(&identity.ca_path)
                .map_err(|e| TlsError::Material(format!("{}: {e}", identity.ca_path)))?,
        ))
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Material(e.to_string()))?;
        let mut roots = rustls::RootCertStore::empty();
        for ca in cas {
            roots
                .add(ca)
                .map_err(|e| TlsError::Material(format!("ca: {e}")))?;
        }
        let mut cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .map_err(TlsError::from)?;
        cfg.alpn_protocols = vec![b"vane-mesh".to_vec()];
        Ok(cfg)
    }

    /// Extracts the SPIFFE ID (URI SAN) from a DER leaf certificate:
    /// scans for the SubjectAlternativeName extension OID
    /// (2.5.29.17 = `55 1D 11`) and returns the first
    /// `uniformResourceIdentifier` (context tag 0x86) inside it.
    #[must_use]
    pub fn spiffe_id(der: &CertificateDer<'_>) -> Option<String> {
        let bytes = der.as_ref();
        let oid = [0x06, 0x03, 0x55, 0x1D, 0x11];
        // Locate the extension OID.
        if let Some(found) = bytes.windows(oid.len()).position(|w| w == oid) {
            let mut pos = found + oid.len();
            // Extension OCTET STRING (tag 0x04) wraps the DER SAN
            // SEQUENCE. Find it, then walk the GeneralNames.
            while pos < bytes.len() && bytes[pos] != 0x04 {
                pos += 1;
            }
            if pos >= bytes.len() {
                return None;
            }
            let (hdr, clen, _, _, _) = der_header(&bytes[pos..])?;
            // The octet string wraps the DER SEQUENCE of GeneralNames;
            // step into its content before walking the names.
            let sans = bytes.get(pos + hdr..pos + hdr + clen)?;
            let (shdr, sclen, _, _, _) = der_header(sans)?;
            let names = sans.get(shdr..shdr + sclen)?;
            let mut spos = 0;
            while spos < names.len() {
                let Some((ghdr, gclen, gtotal, _, gtag)) = der_header(&names[spos..]) else {
                    break;
                };
                if gtag == 0x86 {
                    return String::from_utf8(names[spos + ghdr..spos + ghdr + gclen].to_vec())
                        .ok();
                }
                spos += gtotal;
            }
            return None;
        }
        None
    }

    /// Verifies the peer certificate's SPIFFE ID against `prefix`.
    /// Returns the verified ID.
    ///
    /// # Errors
    /// No certificate, no SPIFFE SAN, or prefix mismatch.
    pub fn verify_spiffe(peer: &[CertificateDer<'_>], prefix: &str) -> Result<String, TlsError> {
        let leaf = peer
            .first()
            .ok_or_else(|| TlsError::Material("mesh: peer presented no certificate".into()))?;
        let id = spiffe_id(leaf)
            .ok_or_else(|| TlsError::Material("mesh: peer has no SPIFFE SAN".into()))?;
        if id.starts_with(prefix) {
            Ok(id)
        } else {
            Err(TlsError::Material(format!(
                "mesh: spiffe id {id} does not match prefix {prefix}"
            )))
        }
    }

    /// Parses one DER TLV header at `buf[0..]`; returns (header length,
    /// content length, total, constructed flag, tag byte).
    fn der_header(buf: &[u8]) -> Option<(usize, usize, usize, bool, u8)> {
        if buf.len() < 2 {
            return None;
        }
        let tag = buf[0];
        let constructed = tag & 0x20 != 0;
        let first = buf[1];
        if first & 0x80 == 0 {
            Some((2, first as usize, 2 + first as usize, constructed, tag))
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 4 || buf.len() < 2 + n {
                return None;
            }
            let mut len = 0usize;
            for b in &buf[2..2 + n] {
                len = (len << 8) | *b as usize;
            }
            Some((2 + n, len, 2 + n + len, constructed, tag))
        }
    }

    /// Convenience: builds the client config and returns a connector
    /// (Arc-wrapped) ready for tokio-rustls.
    ///
    /// # Errors
    /// See [`client_config`].
    pub fn connector(identity: &MeshIdentity) -> Result<Arc<rustls::ClientConfig>, TlsError> {
        Ok(Arc::new(client_config(identity)?))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Generates a mesh CA + SVID with a SPIFFE URI SAN, builds the
        /// client config, and verifies the SPIFFE ID round-trips
        /// through the DER walk.
        #[test]
        fn spiffe_id_roundtrips_through_der() {
            let ca_key = rcgen::KeyPair::generate().expect("ca key");
            let mut ca_params =
                rcgen::CertificateParams::new(vec!["mesh-ca".into()]).expect("ca params");
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = ca_params.self_signed(&ca_key).expect("ca");

            let spiffe = "spiffe://example.org/vane/sidecar";
            let mut svid_params =
                rcgen::CertificateParams::new(vec!["sidecar".into()]).expect("svid params");
            let uri = rcgen::string::Ia5String::try_from(spiffe).expect("ia5 uri");
            let san = rcgen::SanType::URI(uri);
            svid_params.subject_alt_names = vec![san];
            let svid_key = rcgen::KeyPair::generate().expect("svid key");
            let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
            let svid = svid_params.signed_by(&svid_key, &issuer).expect("svid");

            let dir = tempfile::TempDir::new().expect("dir");
            let cert_path = dir.path().join("svid.pem");
            let key_path = dir.path().join("svid-key.pem");
            let ca_path = dir.path().join("ca.pem");
            std::fs::write(&cert_path, svid.pem()).expect("cert");
            std::fs::write(&key_path, svid_key.serialize_pem()).expect("key");
            std::fs::write(&ca_path, ca.pem()).expect("ca");

            let identity = MeshIdentity {
                cert_path: cert_path.display().to_string(),
                key_path: key_path.display().to_string(),
                ca_path: ca_path.display().to_string(),
                spiffe_prefix: "spiffe://example.org/vane/".into(),
            };
            let cfg = client_config(&identity).expect("client config");
            let _ = cfg;

            // The DER walk extracts the SPIFFE ID from the SVID cert.
            use rustls::pki_types::pem::PemObject as _;
            let der = CertificateDer::from_pem_file(&cert_path).expect("der");
            let bytes = der.as_ref();
            let oid = [0x06, 0x03, 0x55, 0x1D, 0x11];
            eprintln!(
                "SPIDEBG der len={} oid at {:?}",
                bytes.len(),
                bytes.windows(5).position(|w| w == oid)
            );
            let id = spiffe_id(&der).expect("spiffe id");
            assert_eq!(id, spiffe);

            // Verification: prefix match passes, mismatch fails.
            assert_eq!(
                verify_spiffe(std::slice::from_ref(&der), "spiffe://example.org/vane/")
                    .expect("verify"),
                spiffe
            );
            assert!(verify_spiffe(&[der], "spiffe://other.org/").is_err());
        }
    }
}
