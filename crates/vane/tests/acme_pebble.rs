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

use std::os::unix::io::AsRawFd;
use std::sync::Arc;

use vane_control::acme::{AcmeConfig, AcmeManager};

/// Blocking cross-process test lock (flock on a temp file).
fn lock_serial() -> std::fs::File {
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("open lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(rc, 0, "flock");
    file
}

#[tokio::test]
async fn acme_issues_certificate_against_pebble() {
    // Serialize with the other proxy suites (shared pebble state + wall-
    // clock sensitive challenge window).
    let _lock = lock_serial();

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

    let result = issuance.await.expect("issuance task");
    assert!(result.is_ok(), "acme issuance failed: {result:?}");

    let cert = std::fs::read_to_string(storage.path().join("cert.pem")).expect("cert.pem written");
    assert!(cert.contains("BEGIN CERTIFICATE"));
}
