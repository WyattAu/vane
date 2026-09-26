//! Blocking HTTP/3 client (experimental, feature `h3`) — the client
//! side of the h3 story for mesh east-west (docs/h3-design.md
//! milestone 4). Dials a QUIC server with the given rustls material,
//! speaks HTTP/3 via the `h3` crate, and returns the full response.
//!
//! Blocking shape: mirrors the engine's synchronous upstream dial —
//! callers (the mesh connector, future `H3Upstream` engine
//! integration) call `request` and get the complete response.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;

/// Client TLS + ALPN material for the h3 dial.
#[derive(Clone)]
pub struct H3ClientConfig {
    /// Server certificate chain to trust.
    pub server_certs: Vec<CertificateDer<'static>>,
    /// SNI name for the TLS handshake.
    pub server_name: String,
    /// ALPN: h3 by default.
    pub alpn: Vec<Vec<u8>>,
}

impl Default for H3ClientConfig {
    fn default() -> Self {
        Self {
            server_certs: Vec::new(),
            server_name: "localhost".to_string(),
            alpn: vec![b"h3".to_vec()],
        }
    }
}

/// One HTTP/3 response: status + header pairs + full body.
#[derive(Debug, Clone)]
pub struct H3Response {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// Sends one HTTP/3 request and returns the full response. Blocks the
/// calling thread; runs the QUIC stack on an internal current-thread
/// runtime.
///
/// `body` non-empty adds a DATA frame + END_STREAM.
///
/// # Errors
/// Dial, handshake, send, or receive failures as strings.
pub fn request(
    addr: SocketAddr,
    cfg: &H3ClientConfig,
    method: &str,
    path: &str,
    authority: &str,
    request_body: &[u8],
    timeout: Duration,
) -> Result<H3Response, String> {
    // Run the QUIC stack on a dedicated OS thread: callers may sit on
    // a tokio worker (where block_on panics), and the engine's workers
    // are plain threads.
    let server_certs = cfg.server_certs.clone();
    let alpn = cfg.alpn.clone();
    let server_name = cfg.server_name.clone();
    let method = method.to_owned();
    let path = path.to_owned();
    let authority = authority.to_owned();
    let request_body = request_body.to_vec();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("h3 client runtime");
        rt.block_on(async move {
            let mut roots = rustls::RootCertStore::empty();
            for cert in &server_certs {
                roots.add(cert.clone()).map_err(|e| format!("root: {e}"))?;
            }
            let mut client_tls = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            client_tls.alpn_protocols = alpn.clone();

            let qcc = QuicClientConfig::try_from(client_tls)
                .map_err(|e| format!("quinn client config: {e}"))?;
            let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
            client_cfg.transport_config(Arc::new({
                let mut t = quinn::TransportConfig::default();
                if let Ok(idle) = quinn::IdleTimeout::try_from(timeout) {
                    t.max_idle_timeout(Some(idle));
                }
                t
            }));
            let bind: SocketAddr = "127.0.0.1:0".parse().expect("bind addr");
            let mut endpoint =
                quinn::Endpoint::client(bind).map_err(|e| format!("client endpoint: {e}"))?;
            endpoint.set_default_client_config(client_cfg);

            let quinn_conn = endpoint
                .connect(addr, &server_name)
                .map_err(|e| format!("connect: {e}"))?
                .await
                .map_err(|e| format!("quinn connect: {e}"))?;

            let (mut driver, mut send_request) =
                h3::client::new(h3_quinn::Connection::new(quinn_conn))
                    .await
                    .map_err(|e| format!("h3 handshake: {e}"))?;
            tokio::spawn(async move {
                let err = driver.wait_idle().await;
                eprintln!("h3 client driver done: {err:?}");
            });

            let req = http::Request::builder()
                .method(method.as_str())
                .uri(format!("https://{server_name}{path}"))
                .header("host", server_name.as_str())
                .body(())
                .map_err(|e| format!("request build: {e}"))?;
            let mut stream = send_request
                .send_request(req)
                .await
                .map_err(|e| format!("send request: {e}"))?;
            if !request_body.is_empty() {
                let data = Bytes::copy_from_slice(&request_body);
                if stream.send_data(data).await.is_err() {
                    return Err("send body failed".to_string());
                }
            }
            let _ = stream.finish().await;

            let response = stream
                .recv_response()
                .await
                .map_err(|e| format!("recv response: {e}"))?;
            let status = response.status().as_u16();
            let headers: Vec<(Vec<u8>, Vec<u8>)> = response
                .headers()
                .iter()
                .map(|(n, v)| (n.as_str().as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect();

            let mut body_out = Vec::new();
            loop {
                match stream.recv_data().await {
                    Ok(Some(mut chunk)) => {
                        use bytes::Buf as _;
                        body_out.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
                    }
                    Ok(None) => break,
                    Err(e) => return Err(format!("recv body: {e}")),
                }
            }
            Ok(H3Response {
                status,
                headers,
                body: body_out,
            })
        })
    });

    handle
        .join()
        .map_err(|p| format!("h3 client thread panicked: {p:?}"))?
}
