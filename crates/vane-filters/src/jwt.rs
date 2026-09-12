//! JWT bearer authentication: verifies `Authorization: Bearer` tokens
//! against a locally-loaded JWKS (RS256/ES256) or HMAC secret file
//! (HS256), with issuer/audience checks. The JWKS file is reloaded
//! when its mtime changes — key rotation without restart.

use std::path::Path;
use std::sync::RwLock;
use std::time::SystemTime;

use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet};

/// Verification verdict details.
#[derive(Debug)]
pub enum AuthError {
    /// No/invalid Authorization header.
    Missing,
    /// Token failed cryptographic or claim validation.
    Invalid,
    /// Configuration/material unavailable (JWKS unreadable).
    Unavailable,
}

impl core::fmt::Display for AuthError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Missing => write!(f, "missing bearer token"),
            Self::Invalid => write!(f, "invalid token"),
            Self::Unavailable => write!(f, "auth material unavailable"),
        }
    }
}

/// Immutable verified-auth material, swapped wholesale on reload.
struct Loaded {
    jwks: Option<JwkSet>,
    secret: Option<Vec<u8>>,
    validation: jsonwebtoken::Validation,
}

/// Shared validator handle (one per worker, cheap clones).
#[derive(Clone)]
pub struct JwtValidator {
    inner: std::sync::Arc<RwLock<std::sync::Arc<Loaded>>>,
    jwks_path: Option<std::path::PathBuf>,
    secret_path: Option<std::path::PathBuf>,
    last_mtime: std::sync::Arc<std::sync::RwLock<Option<SystemTime>>>,
}

impl JwtValidator {
    /// Builds a validator from config material on disk.
    ///
    /// # Errors
    /// Neither material file readable, or the JWKS is malformed.
    pub fn new(
        jwks_path: Option<&Path>,
        secret_path: Option<&Path>,
        issuer: Option<&str>,
        audience: Option<&str>,
    ) -> Result<Self, String> {
        let (jwks, secret) = load_material(jwks_path, secret_path)?;
        let mut validation = jsonwebtoken::Validation::default();
        if jwks.is_some() {
            // Asymmetric JWKS material; HS256 is only for the HMAC
            // secret-file path.
            validation.algorithms = vec![jsonwebtoken::Algorithm::RS256];
        }
        if let Some(iss) = issuer {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = audience {
            validation.set_audience(&[aud]);
            validation.validate_aud = true;
        } else {
            validation.validate_aud = false;
        }
        validation.validate_exp = true;
        let last_mtime = current_mtime(jwks_path.or(secret_path));
        Ok(Self {
            inner: std::sync::Arc::new(RwLock::new(std::sync::Arc::new(Loaded {
                jwks,
                secret,
                validation,
            }))),
            jwks_path: jwks_path.map(std::path::PathBuf::from),
            secret_path: secret_path.map(std::path::PathBuf::from),
            last_mtime: std::sync::Arc::new(std::sync::RwLock::new(last_mtime)),
        })
    }

    /// Reloads material when the file mtime changed. Cheap stat per
    /// call; safe to invoke per request.
    pub fn maybe_reload(&self) {
        let path = match (self.jwks_path.as_deref(), self.secret_path.as_deref()) {
            (Some(p), _) | (None, Some(p)) => p,
            (None, None) => return,
        };
        let mtime = current_mtime(Some(path));
        let changed = {
            let last = self.last_mtime.read().expect("mtime lock");
            *last != mtime
        };
        if !changed {
            return;
        }
        // Rebuild; on failure keep the previous generation.
        if let Ok(()) = self.reload_locked() {
            *self.last_mtime.write().expect("mtime lock") = mtime;
        }
    }

    fn reload_locked(&self) -> Result<(), String> {
        let (jwks, secret) = load_material(self.jwks_path.as_deref(), self.secret_path.as_deref())?;
        let mut guard = self.inner.write().expect("validator lock");
        let validation = guard.validation.clone();
        *guard = std::sync::Arc::new(Loaded {
            jwks,
            secret,
            validation,
        });
        Ok(())
    }

    /// Verifies a bearer token; returns the decoded claims.
    ///
    /// # Errors
    /// [`AuthError`] for each rejection class.
    pub fn verify(&self, token: &str) -> Result<serde_json::Value, AuthError> {
        let guard = self.inner.read().expect("validator lock");
        // Split "Bearer <token>" if the caller passed the full header.
        let token = token
            .strip_prefix("Bearer ")
            .or_else(|| token.strip_prefix("bearer "))
            .unwrap_or(token);
        // Prefer a matching JWK (asymmetric).
        if let Some(jwks) = &guard.jwks {
            let header = jsonwebtoken::decode_header(token).map_err(|_| AuthError::Invalid)?;
            let kid = header.kid.as_deref();
            let mut decoded = None;
            for jwk in &jwks.keys {
                if let Some(k) = kid {
                    if jwk.common.key_id.as_deref() != Some(k) {
                        continue;
                    }
                }
                match &jwk.algorithm {
                    AlgorithmParameters::EllipticCurve(_) => {
                        let key = jsonwebtoken::DecodingKey::from_jwk(jwk)
                            .map_err(|_| AuthError::Invalid)?;
                        if let Ok(c) = jsonwebtoken::decode::<serde_json::Value>(
                            token,
                            &key,
                            &guard.validation,
                        ) {
                            decoded = Some(c.claims);
                            break;
                        }
                    }
                    AlgorithmParameters::RSA(_) => {
                        let key = jsonwebtoken::DecodingKey::from_jwk(jwk)
                            .map_err(|_| AuthError::Invalid)?;
                        if let Ok(c) = jsonwebtoken::decode::<serde_json::Value>(
                            token,
                            &key,
                            &guard.validation,
                        ) {
                            decoded = Some(c.claims);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            return decoded.ok_or(AuthError::Invalid);
        }
        // HMAC fallback.
        if let Some(secret) = &guard.secret {
            let key = jsonwebtoken::DecodingKey::from_secret(secret);
            let mut validation = guard.validation.clone();
            validation.algorithms = vec![jsonwebtoken::Algorithm::HS256];
            return jsonwebtoken::decode::<serde_json::Value>(token, &key, &validation)
                .map(|c| c.claims)
                .map_err(|_| AuthError::Invalid);
        }
        Err(AuthError::Unavailable)
    }
}

fn load_material(
    jwks_path: Option<&Path>,
    secret_path: Option<&Path>,
) -> Result<(Option<JwkSet>, Option<Vec<u8>>), String> {
    if let Some(p) = jwks_path {
        let bytes = std::fs::read(p).map_err(|e| format!("jwks {}: {e}", p.display()))?;
        let jwks: JwkSet =
            serde_json::from_slice(&bytes).map_err(|e| format!("jwks parse: {e}"))?;
        return Ok((Some(jwks), None));
    }
    if let Some(p) = secret_path {
        let bytes = std::fs::read(p).map_err(|e| format!("secret {}: {e}", p.display()))?;
        // Trim one trailing newline (conventional secret files).
        let secret = if bytes.last() == Some(&b'\n') {
            bytes[..bytes.len() - 1].to_vec()
        } else {
            bytes
        };
        return Ok((None, Some(secret)));
    }
    Err("no auth material configured".into())
}

fn current_mtime(path: Option<&Path>) -> Option<SystemTime> {
    let p = path?;
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &[u8] = b"unit-test-hmac-secret";

    fn hs256_validator(dir: &Path) -> JwtValidator {
        let secret_path = dir.join("secret");
        std::fs::write(&secret_path, TEST_SECRET).expect("write");
        JwtValidator::new(None, Some(&secret_path), None, None).expect("validator")
    }

    fn sign_hs256(claims: &serde_json::Value, secret: &[u8]) -> String {
        let key = jsonwebtoken::EncodingKey::from_secret(secret);
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            claims,
            &key,
        )
        .expect("encode")
    }

    #[test]
    fn accepts_valid_hs256_token() {
        let dir = tempfile::tempdir().expect("dir");
        let v = hs256_validator(dir.path());
        let claims = serde_json::json!({"sub": "u1", "exp": 4102444800u64});
        let token = sign_hs256(&claims, TEST_SECRET);
        let got = v.verify(&token).expect("verify");
        assert_eq!(got["sub"], "u1");
        // Full header form also works.
        let header = format!("Bearer {token}");
        let got2 = v.verify(&header).expect("verify");
        assert_eq!(got2["sub"], "u1");
    }

    #[test]
    fn rejects_bad_signature_and_garbage() {
        let dir = tempfile::tempdir().expect("dir");
        let v = hs256_validator(dir.path());
        let claims = serde_json::json!({"sub": "u1", "exp": 4102444800u64});
        let token = sign_hs256(&claims, b"wrong-secret");
        assert!(matches!(v.verify(&token), Err(AuthError::Invalid)));
        assert!(matches!(v.verify("not-a-jwt"), Err(AuthError::Invalid)));
    }

    #[test]
    fn rejects_expired_token() {
        let dir = tempfile::tempdir().expect("dir");
        let v = hs256_validator(dir.path());
        let claims = serde_json::json!({"sub": "u1", "exp": 1u64});
        let token = sign_hs256(&claims, TEST_SECRET);
        assert!(matches!(v.verify(&token), Err(AuthError::Invalid)));
    }

    #[test]
    fn issuer_and_audience_enforced() {
        let dir = tempfile::tempdir().expect("dir");
        let secret_path = dir.path().join("secret");
        std::fs::write(&secret_path, TEST_SECRET).expect("write");
        let v = JwtValidator::new(None, Some(&secret_path), Some("https://iss"), Some("api"))
            .expect("validator");
        let key = jsonwebtoken::EncodingKey::from_secret(TEST_SECRET);
        let mk = |iss: &str, aud: &str| {
            let claims = serde_json::json!({
                "sub": "u1", "iss": iss, "aud": aud, "exp": 4102444800u64
            });
            jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
                &claims,
                &key,
            )
            .expect("encode")
        };
        assert!(v.verify(&mk("https://iss", "api")).is_ok());
        assert!(v.verify(&mk("https://other", "api")).is_err());
        assert!(v.verify(&mk("https://iss", "other")).is_err());
    }

    /// RS256 round-trip: generate a key, build a JWKS from its
    /// components, verify a signed token through kid selection.
    #[test]
    fn accepts_rs256_via_jwks() {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rsa::rand_core::OsRng;
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("keygen");
        let priv_pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).expect("pem");
        let encoding =
            jsonwebtoken::EncodingKey::from_rsa_pem(priv_pem.as_bytes()).expect("encoding key");
        // Public components -> JWKS.
        use rsa::traits::PublicKeyParts as _;
        let pub_key = rsa::RsaPublicKey::from(&key);
        let n = base64url(&pub_key.n().to_bytes_be());
        let e = base64url(&pub_key.e().to_bytes_be());
        let jwks = format!(
            r#"{{"keys":[{{"kty":"RSA","kid":"k1","alg":"RS256","use":"sig","n":"{n}","e":"{e}"}}]}}"#
        );
        let dir = tempfile::tempdir().expect("dir");
        let jwks_path = dir.path().join("jwks.json");
        std::fs::write(&jwks_path, jwks).expect("write");
        let v = JwtValidator::new(Some(&jwks_path), None, None, None).expect("validator");

        let claims = serde_json::json!({"sub": "rsa-user", "exp": 4102444800u64});
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &encoding,
        )
        .expect("sign");
        let got = v.verify(&token).expect("verify");
        assert_eq!(got["sub"], "rsa-user");
    }

    #[test]
    fn jwks_reload_picks_up_rotation() {
        // Rotate the secret file; after maybe_reload the old token is
        // rejected and the new secret's tokens accepted.
        let dir = tempfile::tempdir().expect("dir");
        let secret_path = dir.path().join("secret");
        std::fs::write(&secret_path, b"secret-v1").expect("write");
        let v = JwtValidator::new(None, Some(&secret_path), None, None).expect("validator");
        let claims = serde_json::json!({"sub": "u1", "exp": 4102444800u64});
        let old_token = sign_hs256(&claims, b"secret-v1");
        assert!(v.verify(&old_token).is_ok());

        std::fs::write(&secret_path, b"secret-v2").expect("rotate");
        // Ensure the mtime differs (1s granularity on some filesystems).
        std::thread::sleep(std::time::Duration::from_millis(1100));
        v.maybe_reload();
        assert!(v.verify(&old_token).is_err(), "old secret must fail");
        let new_token = sign_hs256(&claims, b"secret-v2");
        assert!(v.verify(&new_token).is_ok());
    }

    fn base64url(bytes: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }
}
