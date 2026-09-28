//! In-process HTTP/3 upstream bridge (feature `h3`) — the milestone-4
//! integration (docs/h3-design.md, option B in its simplest form).
//!
//! The engine's synchronous workers dial a loopback TCP listener and
//! speak plain HTTP/1.1, exactly as they would with any other
//! backend; this bridge re-originates each request over QUIC/HTTP-3
//! to the cluster's real backends and translates the response back to
//! h1. The worker needs no knowledge of QUIC; the bridge thread owns
//! the async stack (one thread per accepted connection, `h3_client`
//! per request).
//!
//! Failure semantics: an h3 dial/response failure closes the socket
//! abruptly, so the worker sees premature EOF — the existing
//! failover/retry rules treat it like any dead backend.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

use vane_control::config::H3UpstreamTls;

/// Head/body read cap for the loopback h1 side (the engine only ever
/// sends bounded requests).
const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 32 * 1024 * 1024;

/// Spawns the bridge on a dedicated thread bound to an ephemeral
/// loopback port. Returns the address the engine should use as the
/// cluster's (single) backend.
///
/// # Errors
/// Listener bind or TLS material load failures.
pub fn spawn(
    backends: Vec<SocketAddr>,
    tls: Option<H3UpstreamTls>,
    first_byte_timeout: Duration,
) -> Result<SocketAddr, String> {
    if backends.is_empty() {
        return Err("no backends".into());
    }
    let cfg = build_client_config(tls.as_ref())?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| format!("bind: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("addr: {e}"))?;
    std::thread::Builder::new()
        .name("vane-h3-bridge".into())
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(sock) => {
                        let cfg = cfg.clone();
                        let backends = backends.clone();
                        let timeout = first_byte_timeout;
                        let _ = std::thread::Builder::new()
                            .name("vane-h3-bridge-conn".into())
                            .spawn(move || handle_conn(sock, &cfg, &backends, timeout));
                    }
                    Err(e) => {
                        tracing::debug!("h3 bridge accept: {e}");
                    }
                }
            }
        })
        .map_err(|e| format!("bridge thread: {e}"))?;
    Ok(addr)
}

/// Loads the TLS material and assembles the h3 client config.
fn build_client_config(
    tls: Option<&H3UpstreamTls>,
) -> Result<crate::h3_client::H3ClientConfig, String> {
    use rustls::pki_types::pem::PemObject as _;
    let mut cfg = crate::h3_client::H3ClientConfig::default();
    let Some(tls) = tls else {
        return Ok(cfg);
    };
    let cas: Vec<_> = rustls::pki_types::CertificateDer::pem_file_iter(&tls.ca)
        .map_err(|e| format!("ca {}: {e}", tls.ca))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("ca {}: {e}", tls.ca))?;
    cfg.server_certs = cas;
    cfg.server_name = tls.server_name.clone();
    if let Some(alpn) = &tls.alpn {
        cfg.alpn = vec![alpn.clone().into_bytes()];
    }
    match (&tls.client_cert, &tls.client_key) {
        (Some(cert), Some(key)) => {
            let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_file_iter(cert)
                .map_err(|e| format!("client cert: {e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("client cert: {e}"))?;
            let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
                .map_err(|e| format!("client key: {e}"))?;
            cfg.client_cert = Some(crate::h3_client::ClientCert { chain, key });
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err("h3_tls: client_cert and client_key must be set together".into());
        }
        (None, None) => {}
    }
    Ok(cfg)
}

/// Serves one loopback h1 connection: parse the request (single
/// request per connection — the engine's pool opens fresh upstream
/// connections for relay), re-originates it over h3, translates the
/// response back.
fn handle_conn(
    mut sock: std::net::TcpStream,
    cfg: &crate::h3_client::H3ClientConfig,
    backends: &[SocketAddr],
    timeout: Duration,
) {
    let _ = sock.set_read_timeout(Some(timeout.max(Duration::from_secs(1))));
    let _ = sock.set_nodelay(true);

    // Read the head (one `httparse::Request` parse yields method,
    // path, and headers).
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let (head_len, method, path, headers) = loop {
        if buf.len() > MAX_HEAD {
            return; // absurd head: drop
        }
        let mut storage = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut storage);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(n)) => {
                let (method, path) = match (req.method, req.path) {
                    (Some(m), Some(p)) => (m, p),
                    _ => return,
                };
                break (n, method, path, req.headers.to_vec());
            }
            Ok(httparse::Status::Partial) => {}
            Err(_) => return,
        }
        match sock.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return,
        }
    };

    // Framing: Content-Length only (the engine's h1 relay sends CL or
    // nothing for non-h2 routes; chunked is declined with 501).
    let mut content_length: Option<u64> = None;
    let mut chunked = false;
    let mut authority: Option<String> = None;
    for h in &headers {
        let name = h.name.to_ascii_lowercase();
        match name.as_str() {
            "content-length" => {
                content_length = std::str::from_utf8(h.value)
                    .ok()
                    .and_then(|v| v.trim().parse().ok());
            }
            "transfer-encoding" => chunked = true,
            "host" => authority = std::str::from_utf8(h.value).ok().map(str::to_owned),
            _ => {}
        }
    }
    if chunked {
        let _ = sock.write_all(
            b"HTTP/1.1 501 Not Implemented\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
        return;
    }
    let cl = content_length.unwrap_or(0);
    if cl > MAX_BODY as u64 {
        return;
    }

    // Read the body.
    let mut body = buf[head_len..].to_vec();
    while body.len() < cl as usize {
        match sock.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
            Err(_) => return,
        }
    }
    body.truncate(cl as usize);

    // Round-robin backend.
    let backend = {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let i = NEXT.fetch_add(1, Ordering::Relaxed) % backends.len();
        backends[i]
    };

    let authority = authority.unwrap_or_else(|| backend.to_string());
    let resp =
        match crate::h3_client::request(backend, cfg, method, path, &authority, &body, timeout) {
            Ok(r) => r,
            Err(e) => {
                // h3 failure: abrupt close → worker sees premature EOF
                // and applies its failover rules.
                tracing::debug!(%backend, method, path, "h3 bridge: upstream failed: {e}");
                return;
            }
        };

    // Translate to h1. The body is fully buffered, so Content-Length
    // is exact; hop-by-hop and framing headers are dropped.
    let mut out = Vec::with_capacity(resp.body.len() + 256);
    out.extend_from_slice(
        format!("HTTP/1.1 {} {}\r\n", resp.status, reason(resp.status)).as_bytes(),
    );
    for (n, v) in &resp.headers {
        let ln = String::from_utf8_lossy(n).to_ascii_lowercase();
        if matches!(
            ln.as_str(),
            "transfer-encoding" | "content-length" | "connection" | "keep-alive"
        ) {
            continue;
        }
        out.extend_from_slice(n);
        out.extend_from_slice(b": ");
        out.extend_from_slice(v);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("content-length: {}\r\n", resp.body.len()).as_bytes());
    out.extend_from_slice(b"connection: close\r\n\r\n");
    out.extend_from_slice(&resp.body);
    let _ = sock.write_all(&out);
    let _ = sock.shutdown(std::net::Shutdown::Write);
}

/// Reason phrase for the common status codes (empty reason is valid
/// h1; the table just keeps responses readable).
fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Response",
    }
}
