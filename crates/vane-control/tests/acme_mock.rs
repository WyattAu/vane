//! ACME engine against a mocked ACME server (`wiremock`).
//!
//! Drives the full `obtain_certificate` flow — directory, newAccount,
//! newOrder, HTTP-01 challenge validation (token → key authorization →
//! token map), authorization polling, finalize (CSR), chain download,
//! and cert persistence — without network access. Exercises the
//! RFC 8555 §8 challenge-validation path end to end.

use std::path::PathBuf;
use std::sync::Arc;

use vane_control::acme::{AcmeConfig, AcmeManager};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PEM_CHAIN: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

fn base_config(storage: PathBuf, api: String) -> AcmeConfig {
    AcmeConfig {
        directory_url: format!("{api}/directory"),
        emails: vec!["ops@example.com".to_owned()],
        domains: vec!["edge.example.com".to_owned()],
        storage,
        insecure_tls: false,
        challenge_answer_url: None,
        ..AcmeConfig::default()
    }
}

/// Mounts a minimal but protocol-faithful ACME server: nonces on every
/// response, an immediately-valid authorization, and a finalize that
/// short-circuits to `valid` (no 2s polls).
async fn mount_acme(server: &MockServer) {
    let nonce = |resp: ResponseTemplate| resp.append_header("Replay-Nonce", "nonce-1");

    Mock::given(method("GET"))
        .and(path("/directory"))
        .respond_with(nonce(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({
                "newNonce": format!("{}/newNonce", server.uri()),
                "newAccount": format!("{}/newAccount", server.uri()),
                "newOrder": format!("{}/newOrder", server.uri()),
            }),
        )))
        .mount(server)
        .await;

    // HEAD newNonce (fresh nonce fetch).
    Mock::given(wiremock::matchers::method("HEAD"))
        .and(path("/newNonce"))
        .respond_with(nonce(ResponseTemplate::new(200)))
        .mount(server)
        .await;

    // newAccount: 201 + Location (the account kid).
    Mock::given(method("POST"))
        .and(path("/newAccount"))
        .respond_with(nonce(
            ResponseTemplate::new(201)
                .append_header("Location", format!("{}/acct/1", server.uri()))
                .set_body_json(serde_json::json!({})),
        ))
        .mount(server)
        .await;

    // newOrder.
    Mock::given(method("POST"))
        .and(path("/newOrder"))
        .respond_with(nonce(
            ResponseTemplate::new(201)
                .append_header("Location", format!("{}/order/1", server.uri()))
                .set_body_json(serde_json::json!({
                    "status": "pending",
                    "authorizations": [format!("{}/authz/1", server.uri())],
                    "finalize": format!("{}/finalize", server.uri()),
                })),
        ))
        .mount(server)
        .await;

    // Authorization: already valid (skips the 2s poll loop).
    Mock::given(method("POST"))
        .and(path("/authz/1"))
        .respond_with(nonce(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({
                "status": "valid",
                "challenges": [{
                    "type": "http-01",
                    "url": format!("{}/chal/1", server.uri()),
                    "token": "challenge-token-abc",
                }],
            }),
        )))
        .mount(server)
        .await;

    // Challenge ack.
    Mock::given(method("POST"))
        .and(path("/chal/1"))
        .respond_with(nonce(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
        ))
        .mount(server)
        .await;

    // Finalize: immediately valid with a certificate URL.
    Mock::given(method("POST"))
        .and(path("/finalize"))
        .respond_with(nonce(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({
                "status": "valid",
                "certificate": format!("{}/cert/1", server.uri()),
            }),
        )))
        .mount(server)
        .await;

    // Certificate chain (POST-as-GET, JWS body).
    Mock::given(method("POST"))
        .and(path("/cert/1"))
        .respond_with(nonce(ResponseTemplate::new(200).set_body_string(PEM_CHAIN)))
        .mount(server)
        .await;
}

#[tokio::test]
async fn obtain_certificate_end_to_end_and_challenge_validation() {
    let server = MockServer::start().await;
    mount_acme(&server).await;

    let dir = tempfile::tempdir().expect("tmp");
    let cfg = base_config(dir.path().to_path_buf(), server.uri());
    let mgr = Arc::new(AcmeManager::new(cfg));

    let domains = mgr.obtain_certificate().await.expect("cert obtained");
    assert_eq!(domains, vec!["edge.example.com".to_owned()]);

    // Challenge validation: the HTTP-01 token map carries the full key
    // authorization `token.thumbprint` (RFC 8555 §8.1) — the proxy's
    // `/.well-known/acme-challenge/{token}` handler serves this.
    let tokens = mgr.http01_tokens();
    let map = tokens.lock().unwrap_or_else(|e| e.into_inner());
    let key_auth = map
        .get("challenge-token-abc")
        .expect("http-01 token registered");
    let (tok, thumb) = key_auth
        .split_once('.')
        .expect("key auth is token.thumbprint");
    assert_eq!(tok, "challenge-token-abc");
    assert!(!thumb.is_empty(), "thumbprint present");

    // Key-auth derivation is deterministic for the persisted account key.
    let again = mgr
        .http01_key_auth("challenge-token-abc")
        .expect("key auth");
    assert_eq!(again, *key_auth);
    // A different token shares the same thumbprint but differs overall.
    let other = mgr.http01_key_auth("other-token").expect("key auth");
    assert_ne!(other, *key_auth);
    assert!(other.starts_with("other-token."));

    // Cert chain persisted (fullchain text at cert.pem).
    let cert = std::fs::read_to_string(dir.path().join("cert.pem")).expect("cert.pem");
    assert!(cert.contains("BEGIN CERTIFICATE"));
    // The order key was persisted for the TLS reload watcher.
    let key = std::fs::read_to_string(dir.path().join("privkey.pem")).expect("privkey.pem");
    assert!(key.contains("PRIVATE KEY"));
}

#[tokio::test]
async fn challenge_answer_url_receives_token_and_content() {
    let server = MockServer::start().await;
    mount_acme(&server).await;

    // The pebble-style challenge server admin API.
    wiremock::Mock::given(method("POST"))
        .and(path("/challtestsrv"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().expect("tmp");
    let mut cfg = base_config(dir.path().to_path_buf(), server.uri());
    cfg.challenge_answer_url = Some(format!("{}/challtestsrv", server.uri()));
    let mgr = Arc::new(AcmeManager::new(cfg));

    mgr.obtain_certificate().await.expect("cert obtained");

    // The answer push carried the token + key authorization.
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .any(|r| String::from_utf8_lossy(&r.body).contains("challenge-token-abc")),
        "challenge answer pushed: {requests:?}"
    );
}

#[tokio::test]
async fn empty_domains_short_circuits_without_network() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = base_config(dir.path().to_path_buf(), "http://127.0.0.1:1".to_owned());
    // No domains: the manager is a no-op (no listeners configured) and
    // never touches the network.
    let mgr = Arc::new(AcmeManager::new(AcmeConfig {
        domains: Vec::new(),
        ..cfg
    }));
    let domains = mgr.obtain_certificate().await.expect("no-op ok");
    assert!(domains.is_empty());
}
