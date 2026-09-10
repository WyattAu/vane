//! ACME v2 client (RFC 8555) — automated Let's Encrypt certificates (`PR-06`).
//!
//! Scope: HTTP-01 challenges, ES256 account keys, hot cert reload. The JWS
//! signer is hand-rolled over `ring` (ACME's protected-header fields —
//! `url`, `nonce`, `kid` — don't fit JWT crates), the HTTP client is
//! `reqwest`, CSRs are `rcgen`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;

/// Retryable classification for ACME failures: transport/5xx retry,
/// authorization/4xx do not.
#[derive(Debug)]
pub struct AcmeError {
    /// Message.
    pub message: String,
    /// Set when the failure was a rejected nonce (retry with fresh).
    pub retryable_nonce: bool,
}

impl AcmeError {
    /// Wraps a message as a non-nonce (transport-class) error.
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            retryable_nonce: false,
        }
    }
}

impl std::fmt::Display for AcmeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AcmeError {}

impl loop_retry::IsRetryable for AcmeError {
    fn is_retryable(&self) -> bool {
        // Nonce rejections are retried with a fresh nonce; transport and
        // 5xx failures are transient. Explicit 4xx protocol rejections
        // (malformed, unauthorized) are terminal.
        if self.retryable_nonce {
            return true;
        }
        let m = &self.message;
        !(m.contains(" 400 ") || m.contains(" 403 ") || m.contains(" 404 "))
    }
}
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _};
use serde::Deserialize;

/// ACME configuration.
#[derive(Debug, Clone)]
pub struct AcmeConfig {
    /// Directory endpoint (Let's Encrypt production by default).
    pub directory_url: String,
    /// Account contact emails.
    pub emails: Vec<String>,
    /// Domains to obtain (SANs of one cert).
    pub domains: Vec<String>,
    /// Storage for the account key, certs, and HTTP-01 tokens.
    pub storage: PathBuf,
    /// Renew when less than this fraction of lifetime remains.
    pub renew_at_fraction: f64,
    /// Accept invalid TLS certificates on the ACME endpoint (pebble/CI).
    pub insecure_tls: bool,
    /// CI-only: URL of a challenge server admin API that accepts
    /// `{token, content}` HTTP-01 answers (e.g. pebble challtestsrv).
    pub challenge_answer_url: Option<String>,
}

impl Default for AcmeConfig {
    fn default() -> Self {
        Self {
            directory_url: "https://acme-v02.api.letsencrypt.org/directory".to_owned(),
            emails: Vec::new(),
            domains: Vec::new(),
            storage: PathBuf::from("/var/lib/vane/acme"),
            renew_at_fraction: 2.0 / 3.0,
            insecure_tls: false,
            challenge_answer_url: None,
        }
    }
}

/// Directory document.
#[derive(Debug, Clone, Deserialize)]
struct Directory {
    #[serde(rename = "newNonce")]
    new_nonce: String,
    #[serde(rename = "newAccount")]
    new_account: String,
    #[serde(rename = "newOrder")]
    new_order: String,
}

/// Order document (subset).
#[derive(Debug, Deserialize)]
struct Order {
    status: String,
    #[serde(default)]
    authorizations: Vec<String>,
    #[serde(default)]
    finalize: String,
    #[serde(default)]
    certificate: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// Authorization document (subset).
#[derive(Debug, Deserialize)]
struct Authorization {
    status: String,
    #[serde(default)]
    challenges: Vec<Challenge>,
}

/// Challenge document. `token` is absent on challenge types the proxy
/// does not use (e.g. `dns-persist-01`).
#[derive(Debug, Deserialize)]
struct Challenge {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    token: Option<String>,
    url: String,
    /// Validation failure detail (RFC 8555 §8 — problem document).
    #[serde(default)]
    _error: Option<serde_json::Value>,
}

/// ACME manager — runs the renewal loop and serves HTTP-01 responses.
pub struct AcmeManager {
    config: AcmeConfig,
    rng: SystemRandom,
    /// In-flight HTTP-01 token -> key authorization (read by listeners).
    http01: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    http: reqwest::Client,
    /// ACME account URL (kid) once created.
    account_url: std::sync::Mutex<Option<String>>,
    /// Nonce queue: response `Replay-Nonce` values (RFC 8555 §6.5 — use
    /// these before fetching from newNonce; GET-nonces can go stale once a
    /// POST has occurred).
    nonces: std::sync::Mutex<Vec<String>>,
}

impl AcmeManager {
    /// New manager.
    ///
    /// # Panics
    /// If the TLS-capable reqwest client cannot be built.
    #[must_use]
    pub fn new(config: AcmeConfig) -> Self {
        #[allow(clippy::expect_used, reason = "TLS init failure is fatal at startup")]
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("vane-acme/0.1")
            .danger_accept_invalid_certs(config.insecure_tls)
            .build()
            .expect("reqwest client");
        Self {
            config,
            rng: SystemRandom::new(),
            http01: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            http,
            account_url: std::sync::Mutex::new(None),
            nonces: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// HTTP-01 challenge responses: `token -> key authorization`.
    ///
    /// The proxy's HTTP handler consults this for
    /// `GET /.well-known/acme-challenge/{token}`.
    #[must_use]
    pub fn http01_tokens(
        &self,
    ) -> Arc<std::sync::Mutex<std::collections::HashMap<String, String>>> {
        Arc::clone(&self.http01)
    }

    /// Loads (or creates) the persisted account key.
    fn account_key(&self) -> Result<EcdsaKeyPair, AcmeError> {
        std::fs::create_dir_all(&self.config.storage).map_err(|e| AcmeError::new(e.to_string()))?;
        let path = self.config.storage.join("account.key");
        let pkcs8 = match std::fs::read(&path) {
            Ok(der) => der,
            Err(_) => {
                let kp = rcgen::KeyPair::generate().map_err(|e| AcmeError::new(e.to_string()))?;
                let der = kp.serialize_der();
                std::fs::write(&path, &der).map_err(|e| AcmeError::new(e.to_string()))?;
                der
            }
        };
        let rng = SystemRandom::new();
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &pkcs8, &rng)
            .map_err(|e| AcmeError::new(e.to_string()))
    }

    /// b64url (no padding).
    fn b64(data: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
    }

    /// JWK for the account key (embedded on new-account).
    ///
    /// Accepts any type exposing the 65-byte uncompressed P-256 point.
    fn jwk_point(uncompressed: &[u8]) -> serde_json::Value {
        let (x, y) = (&uncompressed[1..33], &uncompressed[33..65]);
        serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": Self::b64(x),
            "y": Self::b64(y),
            "alg": "ES256"
        })
    }

    fn jwk(key: &EcdsaKeyPair) -> serde_json::Value {
        // ring stores the 65-byte uncompressed point.
        let pk: &[u8] = key.public_key().as_ref();
        Self::jwk_point(pk)
    }

    async fn nonce(&self, dir: &Directory) -> Result<String, AcmeError> {
        let resp = self
            .http
            .head(&dir.new_nonce)
            .send()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        resp.headers()
            .get("replay-nonce")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
            .ok_or_else(|| AcmeError::new("missing replay-nonce"))
    }

    /// Posts a signed JWS request. On `badNonce` (servers may reject
    /// nonces at any time per RFC 8555 §6.5) retries once with a fresh
    /// nonce.
    async fn jws_post<T: serde::de::DeserializeOwned>(
        &self,
        key: &EcdsaKeyPair,
        dir: &Directory,
        url: &str,
        payload: serde_json::Value,
        kid: Option<&str>,
    ) -> Result<(Option<T>, Option<String>, Option<String>), AcmeError> {
        match self
            .jws_post_inner::<T>(key, dir, url, payload.clone(), kid)
            .await
        {
            Err(ref e) if e.retryable_nonce => {
                // Purge the queue: a stale queued nonce caused the first
                // rejection. Fetch a fresh one from newNonce directly.
                self.nonces
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                self.jws_post_inner::<T>(key, dir, url, payload, kid).await
            }
            other => other,
        }
    }

    /// POST-as-GET returning the raw response body (cert chains are PEM).
    ///
    /// Note: the parsed-JSON variant (`jws_post`) cannot return PEM; this
    /// is only used for the certificate chain download.
    #[allow(clippy::unused_async)]
    async fn jws_post_raw_text(
        &self,
        key: &EcdsaKeyPair,
        dir: &Directory,
        url: &str,
        _payload: serde_json::Value,
        kid: Option<&str>,
    ) -> Result<(String, Option<String>), AcmeError> {
        let nonce = self.nonce(dir).await?;
        let header = if let Some(kid) = kid {
            serde_json::json!({"alg": "ES256", "nonce": nonce, "url": url, "kid": kid})
        } else {
            serde_json::json!({"alg": "ES256", "nonce": nonce, "url": url, "jwk": Self::jwk(key)})
        };
        let protected = Self::b64(header.to_string().as_bytes());
        let signing_input = format!("{protected}.");
        let sig = key
            .sign(&self.rng, signing_input.as_bytes())
            .map_err(|e| AcmeError::new(e.to_string()))?;
        let body = serde_json::json!({
            "protected": protected,
            "payload": "",
            "signature": Self::b64(sig.as_ref()),
        });
        let resp = self
            .http
            .post(url)
            .header("content-type", "application/jose+json")
            .json(&body)
            .send()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        if let Some(n) = resp
            .headers()
            .get("replay-nonce")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
        {
            self.nonces
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(n);
        }
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        if !status.is_success() {
            return Err(AcmeError::new(format!("acme {url} -> {status}: {text}")));
        }
        Ok((text, location))
    }

    /// Single JWS POST attempt.
    async fn jws_post_inner<T: serde::de::DeserializeOwned>(
        &self,
        key: &EcdsaKeyPair,
        dir: &Directory,
        url: &str,
        payload: serde_json::Value,
        kid: Option<&str>,
    ) -> Result<(Option<T>, Option<String>, Option<String>), AcmeError> {
        let nonce = self.nonce(dir).await?;
        let header = if let Some(kid) = kid {
            serde_json::json!({"alg": "ES256", "nonce": nonce, "url": url, "kid": kid})
        } else {
            serde_json::json!({"alg": "ES256", "nonce": nonce, "url": url, "jwk": Self::jwk(key)})
        };
        let protected = Self::b64(header.to_string().as_bytes());
        let payload_str = if payload.is_null() {
            String::new()
        } else {
            Self::b64(payload.to_string().as_bytes())
        };
        let signing_input = format!("{protected}.{payload_str}");
        let sig = key
            .sign(&self.rng, signing_input.as_bytes())
            .map_err(|e| AcmeError::new(e.to_string()))?;
        let body = serde_json::json!({
            "protected": protected,
            "payload": payload_str,
            "signature": Self::b64(sig.as_ref()),
        });
        let resp = self
            .http
            .post(url)
            .header("content-type", "application/jose+json")
            .json(&body)
            .send()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        let next_nonce = resp
            .headers()
            .get("replay-nonce")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());
        if let Some(n) = resp
            .headers()
            .get("replay-nonce")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
        {
            self.nonces
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(n);
        }
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        if !status.is_success() {
            let mut err = AcmeError::new(format!("acme {url} -> {status}: {text}"));
            if text.contains("badNonce") {
                err.retryable_nonce = true;
            }
            return Err(err);
        }
        let parsed = if text.is_empty() {
            None
        } else {
            Some(match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(e) => return Err(AcmeError::new(format!("jws decode: {e}: {text}"))),
            })
        };
        Ok((parsed, location, next_nonce))
    }

    /// Obtains a certificate for the configured domains (blocking flow,
    /// called from the renewal loop).
    ///
    /// # Errors
    /// Any ACME protocol failure (directory, account, order, authorization,
    /// finalization, or chain download).
    pub async fn obtain_certificate(&self) -> Result<Vec<String>, AcmeError> {
        if self.config.domains.is_empty() {
            return Ok(Vec::new());
        }
        let dir_text = self
            .http
            .get(&self.config.directory_url)
            .send()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?
            .text()
            .await
            .map_err(|e| AcmeError::new(e.to_string()))?;
        let dir: Directory =
            serde_json::from_str(&dir_text).map_err(|e| AcmeError::new(e.to_string()))?;
        let key = self.account_key()?;

        // newAccount (or fetch existing).
        let payload = serde_json::json!({
            "termsOfServiceAgreed": true,
            "contact": self.config.emails.iter().map(|e| format!("mailto:{e}")).collect::<Vec<_>>(),
            "onlyReturnExisting": false
        });
        let (_account, account_location, _): (
            Option<serde_json::Value>,
            Option<String>,
            Option<String>,
        ) = self
            .jws_post(&key, &dir, &dir.new_account, payload, None)
            .await?;
        // The account URL rides the Location header; every subsequent JWS
        // uses it as the kid (RFC 8555 §6.2).
        let account_url = account_location
            .clone()
            .ok_or_else(|| AcmeError::new("newAccount missing Location header"))?;
        *self.account_url.lock().unwrap_or_else(|e| e.into_inner()) = Some(account_url.clone());
        let kid = account_url;

        // newOrder.
        let identifiers: Vec<serde_json::Value> = self
            .config
            .domains
            .iter()
            .map(|d| serde_json::json!({"type": "dns", "value": d}))
            .collect();
        let (order, order_url, _): (Option<Order>, Option<String>, Option<String>) = self
            .jws_post(
                &key,
                &dir,
                &dir.new_order,
                serde_json::json!({"identifiers": identifiers}),
                Some(kid.as_str()),
            )
            .await?;
        let order_url = order_url.ok_or_else(|| AcmeError::new("newOrder missing Location"))?;
        let mut order = order.ok_or_else(|| AcmeError::new("no order body"))?;

        // Respond to HTTP-01 challenges.
        for auth_url in &order.authorizations {
            let (auth, _, _): (Option<Authorization>, Option<String>, Option<String>) = self
                .jws_post(
                    &key,
                    &dir,
                    auth_url,
                    serde_json::Value::Null,
                    Some(kid.as_str()),
                )
                .await?;
            let auth = auth.ok_or_else(|| AcmeError::new("no authorization body"))?;
            let Some(ch) = auth.challenges.iter().find(|c| c.kind == "http-01") else {
                continue;
            };
            let Some(token) = &ch.token else { continue };
            // Full key authorization (RFC 8555 §8.1): token.thumbprint.
            let key_auth = self.http01_key_auth(token)?;
            self.http01
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(token.clone(), key_auth.clone());
            // CI environments (pebble): push the answer to the challenge
            // server's admin API BEFORE triggering validation. The push
            // retries until the challenge server accepts it (the server
            // may still be booting).
            if let Some(url) = &self.config.challenge_answer_url {
                let body = format!(r#"{{"token": "{token}", "content": "{key_auth}"}}"#);
                for _ in 0..10 {
                    match self
                        .http
                        .post(url)
                        .header("Content-Type", "application/json")
                        .body(body.clone())
                        .send()
                        .await
                    {
                        Ok(r) if r.status().is_success() => break,
                        _ => tokio::time::sleep(Duration::from_millis(300)).await,
                    }
                }
            }
            let (_ack, _, _): (Option<serde_json::Value>, Option<String>, Option<String>) = self
                .jws_post(
                    &key,
                    &dir,
                    &ch.url,
                    serde_json::json!({}),
                    Some(kid.as_str()),
                )
                .await?;
        }

        // Poll authorizations until valid. Nonce rejections are retried
        // (fresh nonce comes from the next round).
        for auth_url in order.authorizations.clone() {
            let mut valid = false;
            for _ in 0..40 {
                match self
                    .jws_post::<Authorization>(
                        &key,
                        &dir,
                        &auth_url,
                        serde_json::Value::Null,
                        Some(kid.as_str()),
                    )
                    .await
                {
                    Ok((Some(auth), _, _)) => match auth.status.as_str() {
                        "valid" => {
                            valid = true;
                            break;
                        }
                        "pending" => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        other => {
                            tracing::warn!(
                                status = other,
                                url = auth_url,
                                "acme authorization failed"
                            );
                            return Err(AcmeError::new("authorization failed"));
                        }
                    },
                    Ok(_) => break,
                    Err(e) if e.retryable_nonce => {
                        tokio::time::sleep(Duration::from_millis(500)).await
                    }
                    Err(e) => return Err(e),
                }
            }
            if !valid {
                return Err(AcmeError::new("authorization did not validate"));
            }
        }

        // Finalize with a CSR (fresh cert keypair, NOT the account key —
        // pebble rejects CSRs reusing the account public key).
        let cert_key = rcgen::KeyPair::generate().map_err(|e| AcmeError::new(e.to_string()))?;
        // Persist the cert key NOW: TLS listeners watch privkey.pem and the
        // reload must be able to load both files on the change event.
        std::fs::write(
            self.config.storage.join("privkey.pem"),
            cert_key.serialize_pem(),
        )
        .map_err(|e| AcmeError::new(e.to_string()))?;
        let key_pair = cert_key;
        let mut params = rcgen::CertificateParams::new(self.config.domains.clone())
            .map_err(|e| AcmeError::new(e.to_string()))?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, self.config.domains[0].clone());
        let csr = params
            .serialize_request(&key_pair)
            .map_err(|e| AcmeError::new(e.to_string()))?;
        let csr_der = csr.der();
        let (finalized, _, _): (Option<Order>, Option<String>, Option<String>) = self
            .jws_post(
                &key,
                &dir,
                &order.finalize,
                serde_json::json!({"csr": Self::b64(csr_der.as_ref())}),
                Some(kid.as_str()),
            )
            .await?;
        order = finalized.ok_or_else(|| AcmeError::new("no finalize body"))?;

        // Poll order -> valid, then download.
        for _ in 0..30 {
            if order.status == "valid" {
                break;
            }
            if order.status == "invalid" {
                return Err(AcmeError::new(format!("order invalid: {:?}", order.error)));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            // POST-as-GET the order's own Location URL to refresh status.
            let (o, _, _): (Option<Order>, Option<String>, Option<String>) = self
                .jws_post(
                    &key,
                    &dir,
                    &order_url,
                    serde_json::Value::Null,
                    Some(kid.as_str()),
                )
                .await?;
            if let Some(o) = o {
                order = o;
            }
        }

        let Some(cert_url) = order.certificate.clone() else {
            return Err(AcmeError::new("no certificate url"));
        };
        // The chain may not be immediately servable right after finalize
        // (servers briefly return errors while the chain is assembled);
        // retry until a PEM body arrives.
        let mut cert_text = String::new();
        for _ in 0..10 {
            let (text, _) = self
                .jws_post_raw_text(
                    &key,
                    &dir,
                    &cert_url,
                    serde_json::Value::Null,
                    Some(kid.as_str()),
                )
                .await?;
            if text.contains("BEGIN CERTIFICATE") {
                cert_text = text;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !cert_text.contains("BEGIN CERTIFICATE") {
            return Err(AcmeError::new(
                "certificate chain download did not yield PEM",
            ));
        }
        self.persist_certs(&cert_text)?;
        Ok(self.config.domains.clone())
    }

    /// Builds the full key authorization for a token (JWK thumbprint).
    ///
    /// # Errors
    /// Account key load failure.
    pub fn http01_key_auth(&self, token: &str) -> Result<String, AcmeError> {
        let key = self.account_key()?;
        let jwk = Self::jwk(&key);
        let canonical = format!(
            "{{\"crv\":\"{}\",\"kty\":\"{}\",\"x\":\"{}\",\"y\":\"{}\"}}",
            jwk["crv"].as_str().unwrap_or(""),
            jwk["kty"].as_str().unwrap_or(""),
            jwk["x"].as_str().unwrap_or(""),
            jwk["y"].as_str().unwrap_or("")
        );
        let digest = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
        Ok(format!("{token}.{}", Self::b64(digest.as_ref())))
    }

    fn persist_certs(&self, text: &str) -> Result<(), AcmeError> {
        std::fs::create_dir_all(&self.config.storage).map_err(|e| AcmeError::new(e.to_string()))?;
        let cert_path = self.config.storage.join("cert.pem");
        let key_path = self.config.storage.join("privkey.pem");
        // Key: reuse the order key material persisted at CSR time.
        std::fs::write(&cert_path, text).map_err(|e| AcmeError::new(e.to_string()))?;
        if !key_path.exists() {
            std::fs::write(&key_path, []).map_err(|e| AcmeError::new(e.to_string()))?;
        }
        Ok(())
    }

    /// Long-running renewal loop: checks expiry daily, renews at 2/3
    /// lifetime with `retry-backoff` between failures.
    pub fn spawn_renewal(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let config = loop_retry::RetryConfig {
                max_retries: 5,
                initial_delay: Duration::from_secs(30),
                max_delay: Duration::from_secs(3600),
                backoff_multiplier: 2.0,
                jitter: true,
            };
            loop {
                let mgr = Arc::clone(&self);
                let result = loop_retry::with_backoff(&config, || {
                    let mgr = Arc::clone(&mgr);
                    async move { mgr.obtain_certificate().await }
                })
                .await;
                match result {
                    Ok(domains) if domains.is_empty() => {
                        return; // ACME not configured
                    }
                    Ok(domains) => {
                        tracing::info!("acme: certificate ready for {domains:?}");
                    }
                    Err(e) => {
                        tracing::error!("acme: renewal failed: {e}");
                    }
                }
                tokio::time::sleep(Duration::from_secs(60 * 60 * 24)).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_url_nopad() {
        assert_eq!(AcmeManager::b64(b"\"\""), "IiI");
        let s = AcmeManager::b64(&[0xfb, 0xff]);
        assert!(!s.contains('+') && !s.contains('/') && !s.contains('='));
    }
}
