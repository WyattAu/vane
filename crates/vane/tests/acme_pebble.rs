//! ACME e2e against a local pebble (`acme-pebble` feature).
//!
//! Requires: pebble on :14000 (HTTPS, random cert — `insecure_tls`) and
//! challtestsrv on :5002 (HTTP-01 answers) with its management API on
//! :8055. Start with:
//!
//! ```sh
//! docker run -d --name pebble -p 14000:14000 ghcr.io/letsencrypt/pebble:latest
//! docker run -d --name challtestsrv -p 5002:5002 -p 8055:8055 \
//!   ghcr.io/letsencrypt/pebble-challtestsrv:latest -defaultIPv4 "" -defaultIPv6 ""
//! ```
//!
//! The test drives the real ACME client end-to-end: directory →
//! newAccount (kid via Location) → newOrder → HTTP-01 (answers pushed to
//! challtestsrv from the client's key authorizations) → finalize →
//! certificate download.

use std::sync::Arc;
use std::time::Duration;
use vane_control::acme::{AcmeConfig, AcmeManager};

#[tokio::test]
async fn acme_issues_certificate_against_pebble() {
    let storage = tempfile::tempdir().expect("dir");
    let mgr = Arc::new(AcmeManager::new(AcmeConfig {
        directory_url: "https://127.0.0.1:14000/dir".into(),
        emails: vec!["ci@example.com".into()],
        domains: vec!["localhost".into()],
        storage: storage.path().to_path_buf(),
        renew_at_fraction: 2.0 / 3.0,
        insecure_tls: true,
        challenge_answer_url: Some("http://127.0.0.1:8055/add-http01".into()),
    }));

    // Drive issuance in the background; relay challenge answers to
    // challtestsrv as the client publishes tokens.
    let worker = Arc::clone(&mgr);
    let issuance = tokio::spawn(async move { worker.obtain_certificate().await });

    let challenge_relay = tokio::spawn(async move {
        let http = reqwest::Client::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut pushed: std::collections::HashSet<String> = Default::default();
        while std::time::Instant::now() < deadline {
            let Some(token) = mgr
                .http01_tokens()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .keys()
                .next()
                .cloned()
            else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            if pushed.insert(token.clone()) {
                let Ok(key_auth) = mgr.http01_key_auth(&token) else {
                    continue;
                };
                let body = format!(r#"{{"token": "{token}", "content": "{key_auth}"}}"#);
                let _ = http
                    .post("http://127.0.0.1:8055/add-http01")
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await;
            }
        }
    });

    let result = issuance.await.expect("issuance task");
    let _ = challenge_relay;
    assert!(result.is_ok(), "acme issuance failed: {result:?}");

    let cert = std::fs::read_to_string(storage.path().join("cert.pem")).expect("cert.pem written");
    assert!(cert.contains("BEGIN CERTIFICATE"));
}
