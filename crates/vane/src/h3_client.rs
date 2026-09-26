//! Blocking HTTP/3 client (experimental, feature `h3`) — the client
//! side of the h3 story for mesh east-west (docs/h3-design.md
//! milestone 4). Dials a QUIC server with the given rustls material,
//! speaks HTTP/3 via the `h3` crate, and returns the full response.
//!
//! The blocking shape mirrors the engine's synchronous upstream dial:
//! callers (the mesh connector, future `H3Upstream` engine integration)
//! call `request` and get the complete response.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;

/// Client TLS + ALPN material for the h3 dial.
#[derive(Clone)]
pub struct H3ClientConfig {
    /// Server certificate chain to trust (self-signed or mesh CA leaf).
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

/// Sends one HTTP/3 request (HEAD or GET semantics: no request body;
/// `body` non-empty adds a DATA frame + END_STREAM) and returns the
/// full response. Blocks the calling thread; runs the QUIC stack on an
/// internal current-thread runtime.
///
/// # Errors
/// Dial, handshake, send, or receive failures as strings.
pub fn request(
    addr: SocketAddr,
    cfg: &H3ClientConfig,
    method: &str,
    path: &str,
    authority: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<H3Response, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("h3 client runtime: {e}"))?;

    let cfg = cfg.clone();
    rt.block_on(async move {
        let mut roots = rustls::RootCertStore::empty();
        for cert in &cfg.server_certs {
            roots
                .add(cert.clone())
                .map_err(|e| format!("root add: {e}"))?;
        }
        let mut client_tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_tls.alpn_protocols = cfg.alpn.clone();

        let qcc = QuicClientConfig::try_from(client_tls)
            .map_err(|e| format!("quinn client config: {e}"))?;
        let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
        client_cfg.transport_config(Arc::new({
            let mut t = quinn::TransportConfig::default();
            t.max_idle_timeout(Some(timeout.try_into().ok().unwrap_or(
                quinn_proto::IdleTimeout::from_quic_jetlag(u32::MAX),
            ))));
            t
        }));
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap_or_default())
            .map_err(|e| format!("client endpoint: {e}"))?;
        endpoint.set_default_client_config(client_cfg);

        let quinn_conn = endpoint
            .connect(addr, &cfg.server_name)
            .map_err(|e| format!("connect: {e}"))?
            .await
            .map_err(|e| format!("quinn connect: {e}"))?;

        let (mut driver, mut send_request) =
            h3::client::new(h3_quinn::Connection::new(quinn_conn))
                .await
                .map_err(|e| format!("h3 handshake: {e}"))?;
        let driver = tokio::spawn(async move {
            if let Err(e) = driver.wait_idle().await {
                eprintln!("h3 client driver: {e:?}");
            }
        });

        let req = http::Request::builder()
            .method(method)
            .uri(format!("https://{authority}{path}"))
            .header("host", authority)
            .body(())
            .map_err(|e| format!("request build: {e}"))?;
        let mut stream = send_request
            .send_request(req)
            .await
            .map_err(|e| format!("send request: {e}"))?;
        if !body.is_empty() {
            stream
                .send_data(Bytes::copy_from_slice(body))
                .map_err(|e| format!("send body: {e}"))?;
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

        let mut out = Vec::new();
        loop {
            match stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    use bytes::Buf as _;
                    out.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
                }
                Ok(None) => break,
                Err(e) => return Err(format!("recv body: {e}")),
            }
        }
        let _ = driver.await;
        Ok(H3Response {
            status,
            headers: headers
                .into_iter()
                .map(|(n, v)| (n.into_bytes(), v.to_vec()))
                .collect(),
            body: out,
        })
    })
}
