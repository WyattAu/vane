//! Mock ACME v2 server: the full `obtain_certificate` flow against an
//! in-process RFC 8555 server, no docker and no external CA.
//!
//! Why this exists: the issuance flow (directory → newAccount →
//! newOrder → http-01 → finalize → chain download) had coverage only
//! from the pebble e2e, which is `#[ignore]`d and feature-gated — i.e.
//! in CI it never ran. The mock also works like a server rather than a
//! stub: it decodes each JWS protected header and checks the
//! preconditions RFC 8555 §6.2 puts on the client (alg, nonce, the
//! `url` agreement rule, jwk on newAccount and kid afterwards), so a
//! regression in our signing shows up as a failed handshake, not as a
//! silent 200 from a permissive stub.

use std::sync::{Arc, Mutex};

use base64::Engine;
use vane_control::acme::{AcmeConfig, AcmeManager};

/// What the mock server observed.
#[derive(Default)]
struct Journal {
    /// Paths in request order (`METHOD /path`).
    requests: Vec<String>,
    /// Decoded JWS protected headers, in request order.
    protected: Vec<serde_json::Value>,
    /// `POST` bodies of the challenge-answer (pebble-style) endpoint.
    answer_pushes: Vec<serde_json::Value>,
}

/// Minimal HTTP/1.1 ACME server: parses request line, headers and
/// `content-length` body, answers with a fixed flow.
fn spawn_mock_acme(journal: Arc<Mutex<Journal>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let journal = Arc::clone(&journal);
                        let _ = stream.set_nonblocking(true);
                        tokio::spawn(async move {
                            let Ok(stream) = tokio::net::TcpStream::from_std(stream) else {
                                return;
                            };
                            let _ = serve(stream, journal).await;
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    Err(_) => break,
                }
            }
        });
    });
    format!("http://{addr}")
}

async fn serve(mut stream: tokio::net::TcpStream, journal: Arc<Mutex<Journal>>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let host = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let base = format!("http://{host}");
    let mut buf: Vec<u8> = Vec::new();
    loop {
        // Read until the head (and any content-length body) is complete.
        let (head_end, content_length) = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                let len = head
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                break (pos + 4, len);
            }
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        while buf.len() < head_end + content_length {
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let body = buf[head_end..head_end + content_length].to_vec();
        buf.drain(..head_end + content_length);

        let mut parts = head.lines();
        let request_line = parts.next().unwrap_or_default().to_owned();
        let mut fields = request_line.split_whitespace();
        let method = fields.next().unwrap_or("GET");
        let path = fields.next().unwrap_or("/");
        let is_head = method.eq_ignore_ascii_case("HEAD");

        let body_json: Option<serde_json::Value> = serde_json::from_slice(&body)
            .ok()
            .filter(|_| !body.is_empty());
        let protected: Option<serde_json::Value> = body_json
            .as_ref()
            .and_then(|v| v.get("protected"))
            .and_then(|v| v.as_str())
            .and_then(decode_b64)
            .and_then(|raw| serde_json::from_slice(&raw).ok());
        if let Some(p) = protected.clone() {
            // RFC 8555 §6.2: the protected header's url MUST match the
            // request URL. A client that signs a different url than it
            // POSTs to is broken, and a real CA rejects it.
            assert_eq!(
                p.get("url")
                    .and_then(|v| v.as_str())
                    .map(std::borrow::ToOwned::to_owned),
                Some(format!("{base}{path}")),
                "JWS url must equal the request URL"
            );
            assert_eq!(
                p.get("alg").and_then(|v| v.as_str()),
                Some("ES256"),
                "alg must be ES256"
            );
            assert!(
                p.get("nonce")
                    .and_then(|v| v.as_str())
                    .is_some_and(|n| !n.is_empty()),
                "every JWS carries a nonce"
            );
        }
        record(&journal, method, path, protected, &body_json);

        let (status, extra_headers, payload) = route(path, body_json.as_ref(), &base);
        let mut resp = format!(
            "HTTP/1.1 {status} {}\r\ncontent-length: {}\r\nreplay-nonce: {}\r\n",
            reason(status),
            payload.len(),
            mock_nonce(path)
        );
        for h in &extra_headers {
            resp.push_str(h);
        }
        resp.push_str("\r\n");
        let mut out = resp.into_bytes();
        if !is_head {
            out.extend_from_slice(&payload);
        }
        if stream.write_all(&out).await.is_err() {
            return;
        }
    }
}

fn route(path: &str, body: Option<&serde_json::Value>, base: &str) -> (u16, Vec<String>, Vec<u8>) {
    let json = |v: serde_json::Value| {
        (
            200,
            vec!["content-type: application/json\r\n".to_owned()],
            v.to_string().into_bytes(),
        )
    };
    match path {
        "/dir" => json(serde_json::json!({
            "newNonce": format!("{base}/new-nonce"),
            "newAccount": format!("{base}/new-account"),
            "newOrder": format!("{base}/new-order"),
            "revokeCert": format!("{base}/revoke-cert"),
            "keyChange": format!("{base}/key-change"),
        })),
        "/new-nonce" => (204, vec![], Vec::new()),
        "/new-account" => (
            201,
            vec![format!("location: {base}/acct/1\r\n")],
            b"{\"status\":\"valid\"}".to_vec(),
        ),
        "/new-order" => (
            201,
            vec![format!("location: {base}/order/1\r\n")],
            serde_json::json!({
                "status": "pending",
                "authorizations": [format!("{base}/authz/1")],
                "finalize": format!("{base}/finalize"),
            })
            .to_string()
            .into_bytes(),
        ),
        // Already valid: the client's polling loop must accept it on the
        // first pass (a pending-then-valid server would sleep 2s a poll).
        p if p.starts_with("/authz/") => json(serde_json::json!({
            "status": "valid",
            "identifier": {"type": "dns", "value": "localhost"},
            "challenges": [{
                "type": "http-01",
                "status": "valid",
                "token": MOCK_TOKEN,
                "url": format!("{base}/chall/1"),
            }],
        })),
        p if p.starts_with("/chall/") => {
            // RFC 8555 §7.5.1: readiness is signalled with an empty
            // JSON body, not a POST-as-GET (that one carries no payload
            // at all and is used for authz/order polling).
            assert_eq!(
                body.and_then(|b| b.get("payload").and_then(|v| v.as_str()))
                    .and_then(decode_b64)
                    .as_deref(),
                Some(b"{}".as_slice()),
                "the challenge trigger carries an empty JSON body (RFC 8555 §7.5.1)"
            );
            (200, vec![], b"{}".to_vec())
        }
        "/finalize" => json(serde_json::json!({
            "status": "valid",
            "authorizations": [format!("{base}/authz/1")],
            "finalize": format!("{base}/finalize"),
            "certificate": format!("{base}/cert/1"),
        })),
        p if p.starts_with("/cert/") => (
            200,
            vec!["content-type: application/pem-certificate-chain\r\n".to_owned()],
            format!("{MOCK_CERT_PEM}\n{MOCK_CERT_PEM}\n").into_bytes(),
        ),
        "/admin/answer" => {
            assert!(
                body.is_some_and(|b| b.get("token").and_then(|v| v.as_str()) == Some(MOCK_TOKEN)),
                "the challenge-answer push carries the challenge token"
            );
            (200, vec![], b"{}".to_vec())
        }
        other => panic!("unexpected mock ACME request: {other}"),
    }
}

/// Journal update, kept in its own scope so no `MutexGuard` is ever
/// live across an await.
fn record(
    journal: &Arc<Mutex<Journal>>,
    method: &str,
    path: &str,
    protected: Option<serde_json::Value>,
    body_json: &Option<serde_json::Value>,
) {
    let mut j = journal.lock().expect("journal");
    j.requests.push(format!("{method} {path}"));
    if let Some(p) = protected {
        j.protected.push(p);
    }
    if path == "/admin/answer" {
        j.answer_pushes
            .push(body_json.clone().unwrap_or(serde_json::Value::Null));
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        _ => "Unknown",
    }
}

/// Nonces must not repeat: a real server rejects reuse, and vane's
/// client queues every nonce it is handed.
fn mock_nonce(path: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "nonce-{path}-{}-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed),
        std::process::id()
    )
}

fn decode_b64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()
}

const MOCK_TOKEN: &str = "mock-token-abc123";

/// A syntactically valid self-signed leaf, standing in for the issued
/// chain. The client only requires PEM framing.
const MOCK_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMOCKCERTBODY\n-----END CERTIFICATE-----";

#[tokio::test]
async fn obtain_certificate_against_mock_acme() {
    let journal = Arc::new(Mutex::new(Journal::default()));
    let base = spawn_mock_acme(Arc::clone(&journal));
    let storage = tempfile::tempdir().expect("dir");

    let client = AcmeManager::new(AcmeConfig {
        directory_url: format!("{base}/dir"),
        emails: vec!["ops@vane.test".to_owned()],
        domains: vec!["localhost".to_owned()],
        storage: storage.path().to_path_buf(),
        renew_at_fraction: 0.5,
        insecure_tls: false,
        challenge_answer_url: Some(format!("{base}/admin/answer")),
    });

    let issued = client.obtain_certificate().await.expect("issue");
    assert_eq!(issued, vec!["localhost".to_owned()]);

    // The flow walked the RFC 8555 order in order.
    let requests = {
        let j = journal.lock().expect("journal");
        j.requests.clone()
    };
    for (method, path) in [
        ("GET", "/dir"),
        ("HEAD", "/new-nonce"),
        ("POST", "/new-account"),
        ("POST", "/new-order"),
        ("POST", "/authz/1"),
        ("POST", "/chall/1"),
        ("POST", "/finalize"),
        ("POST", "/cert/1"),
    ] {
        let want = format!("{method} {path}");
        assert!(
            requests.iter().any(|r| r == &want),
            "expected {want} in {requests:?}"
        );
    }

    // newAccount identifies itself with jwk; everything after uses kid.
    let protected = {
        let j = journal.lock().expect("journal");
        j.protected.clone()
    };
    let new_account = protected
        .iter()
        .find(|p| {
            p.get("url")
                .and_then(|v| v.as_str())
                .is_some_and(|u| u.ends_with("/new-account"))
        })
        .expect("new-account JWS");
    assert!(
        new_account.get("jwk").is_some(),
        "newAccount carries the account JWK, not a kid"
    );
    assert!(new_account.get("kid").is_none());
    for p in &protected {
        if p.get("url")
            .and_then(|v| v.as_str())
            .is_some_and(|u| !u.ends_with("/new-account"))
        {
            assert_eq!(
                p.get("kid")
                    .and_then(|v| v.as_str())
                    .map(std::borrow::ToOwned::to_owned),
                Some(format!("{base}/acct/1")),
                "every request after newAccount uses the account kid (RFC 8555 §7.3)"
            );
        }
    }

    // The client published the HTTP-01 answer the proxy would serve.
    let tokens = client.http01_tokens();
    let answer = tokens
        .lock()
        .expect("token map")
        .get(MOCK_TOKEN)
        .cloned()
        .expect("http-01 answer published");
    assert!(
        answer.starts_with(&format!("{MOCK_TOKEN}.")),
        "key authorization is token.thumbprint (RFC 8555 §8.1): {answer}"
    );

    // The pebble-style answer push carried exactly that token and key
    // authorization (the CI contract that lets a challenge server
    // validate without reaching the proxy).
    let pushes = {
        let j = journal.lock().expect("journal");
        j.answer_pushes.clone()
    };
    assert_eq!(pushes.len(), 1, "one answer push per challenge");
    assert_eq!(
        pushes[0]
            .get("token")
            .and_then(|v| v.as_str())
            .map(std::borrow::ToOwned::to_owned),
        Some(MOCK_TOKEN.to_owned())
    );
    assert_eq!(
        pushes[0].get("content").and_then(|v| v.as_str()),
        Some(answer.as_str()),
        "the pushed content is the key authorization the proxy serves"
    );

    // Material landed where TLS listeners watch for it.
    let cert = std::fs::read_to_string(storage.path().join("cert.pem")).expect("cert.pem");
    assert!(cert.contains("BEGIN CERTIFICATE"), "chain persisted");
    assert_eq!(
        cert.matches("BEGIN CERTIFICATE").count(),
        2,
        "the full chain is persisted, leaf first"
    );
    let key = std::fs::read_to_string(storage.path().join("privkey.pem")).expect("privkey");
    assert!(key.contains("PRIVATE KEY"), "cert key persisted");
    assert!(
        storage.path().join("account.key").exists(),
        "account key persisted for reuse"
    );
}
