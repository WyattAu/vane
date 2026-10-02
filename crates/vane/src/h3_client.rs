//! Blocking HTTP/3 client (experimental, feature `h3`) — the client
//! side of the h3 story for mesh east-west (docs/h3-design.md
//! milestone 4). Dials a QUIC server with the given rustls material,
//! speaks HTTP/3 via the `h3` crate, and returns the full response.
//!
//! Blocking shape: mirrors the engine's synchronous upstream dial —
//! callers (the mesh connector, future `H3Upstream` engine
//! integration) call `request` and get the complete response. The
//! QUIC stack runs on a dedicated OS thread so tokio-context callers
//! work too.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;

/// Client certificate for mTLS h3 dials (mesh SVID).
pub struct ClientCert {
    /// Certificate chain (leaf first).
    pub chain: Vec<CertificateDer<'static>>,
    /// Leaf private key (DER or PEM-derived).
    pub key: rustls::pki_types::PrivateKeyDer<'static>,
}

impl Clone for ClientCert {
    fn clone(&self) -> Self {
        Self {
            chain: self.chain.clone(),
            key: self.key.clone_key(),
        }
    }
}

impl ClientCert {
    /// Clones the key material (`PrivateKeyDer` is not `Clone`; the
    /// rustls ring/aws-lc keys are cheap handle clones via
    /// `clone_key`).
    #[must_use]
    pub fn clone_key(&self) -> rustls::pki_types::PrivateKeyDer<'static> {
        self.key.clone_key()
    }
}

/// Client TLS + ALPN material for the h3 dial.
pub struct H3ClientConfig {
    /// Server certificate chain to trust.
    pub server_certs: Vec<CertificateDer<'static>>,
    /// SNI name for the TLS handshake.
    pub server_name: String,
    /// ALPN: h3 by default.
    pub alpn: Vec<Vec<u8>>,
    /// Client certificate for mTLS backends (absent = no client auth).
    pub client_cert: Option<ClientCert>,
}

impl Clone for H3ClientConfig {
    fn clone(&self) -> Self {
        Self {
            server_certs: self.server_certs.clone(),
            server_name: self.server_name.clone(),
            alpn: self.alpn.clone(),
            client_cert: self.client_cert.clone(),
        }
    }
}

impl Default for H3ClientConfig {
    fn default() -> Self {
        Self {
            server_certs: Vec::new(),
            server_name: "localhost".to_string(),
            alpn: vec![b"h3".to_vec()],
            client_cert: None,
        }
    }
}

/// One HTTP/3 response: status + header pairs + full body.
#[derive(Debug, Clone)]
pub struct H3Response {
    /// Response status code.
    pub status: u16,
    /// Response header pairs (order preserved).
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    /// Response body bytes.
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
    let cfg = cfg.clone();
    let authority = authority.to_owned();
    let method = method.to_owned();
    let path = path.to_owned();
    let request_body = request_body.to_vec();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("h3 client runtime");
        rt.block_on(request_async(
            addr,
            &cfg,
            &method,
            &path,
            &authority,
            &request_body,
            timeout,
        ))
    });

    handle
        .join()
        .map_err(|p| format!("h3 client thread panicked: {p:?}"))?
}

/// Async core of [`request`]: dial, send, receive — awaitable inside
/// an existing runtime (the h3 bridge runs one per process thread).
///
/// # Errors
/// Dial, handshake, send, or receive failures as strings.
pub async fn request_async(
    addr: SocketAddr,
    cfg: &H3ClientConfig,
    method: &str,
    path: &str,
    authority: &str,
    request_body: &[u8],
    timeout: Duration,
) -> Result<H3Response, String> {
    let request_body = request_body.to_vec();
    let endpoint = client_endpoint(cfg, timeout)?;
    let quinn_conn = endpoint
        .connect(addr, &cfg.server_name)
        .map_err(|e| format!("connect: {e}"))?
        .await
        .map_err(|e| format!("quinn connect: {e}"))?;

    let (mut driver, mut send_request) = client_new(quinn_conn).await?;
    tokio::spawn(async move {
        let err = driver.wait_idle().await;
        tracing::trace!("h3 client driver idle: {err:?}");
    });

    exchange(
        &mut send_request,
        method,
        path,
        authority,
        &request_body,
        &[],
    )
    .await
}

/// Builds the QUIC client config (rustls material + transport
/// timeouts) — the reloadable half of [`client_endpoint`]. The bridge
/// rebuilds this when upstream material rotates and swaps it onto the
/// live endpoint.
///
/// # Errors
/// TLS material failures as strings.
pub fn client_config(
    cfg: &H3ClientConfig,
    timeout: Duration,
) -> Result<quinn::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in &cfg.server_certs {
        roots.add(cert.clone()).map_err(|e| format!("root: {e}"))?;
    }
    let builder = rustls::ClientConfig::builder();
    let mut client_tls = if let Some(cc) = &cfg.client_cert {
        builder
            .with_root_certificates(roots)
            .with_client_auth_cert(cc.chain.clone(), cc.key.clone_key())
            .map_err(|e| format!("client cert: {e}"))?
    } else {
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    client_tls.alpn_protocols = cfg.alpn.clone();

    let qcc =
        QuicClientConfig::try_from(client_tls).map_err(|e| format!("quinn client config: {e}"))?;
    let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
    client_cfg.transport_config(Arc::new({
        let mut t = quinn::TransportConfig::default();
        if let Ok(idle) = quinn::IdleTimeout::try_from(timeout) {
            t.max_idle_timeout(Some(idle));
        }
        t
    }));
    Ok(client_cfg)
}

/// Builds the shared client endpoint (rustls material + transport
/// timeouts). The bridge owns one endpoint per process and multiplexes
/// request streams over pooled connections.
///
/// # Errors
/// TLS material or endpoint bind failures as strings.
pub fn client_endpoint(cfg: &H3ClientConfig, timeout: Duration) -> Result<quinn::Endpoint, String> {
    let client_cfg = client_config(cfg, timeout)?;
    let bind: SocketAddr = "127.0.0.1:0".parse().expect("bind addr");
    let mut endpoint =
        quinn::Endpoint::client(bind).map_err(|e| format!("client endpoint: {e}"))?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

/// Runs one request/response exchange on an established h3 client
/// (`send_request` handle from [`client_new`]). The bridge multiplexes
/// these over pooled connections.
///
/// # Errors
/// Send or receive failures as strings.
pub async fn exchange(
    send_request: &mut H3SendRequest,
    method: &str,
    path: &str,
    authority: &str,
    request_body: &[u8],
    extra_headers: &[(&str, String)],
) -> Result<H3Response, String> {
    // The authority rides the URI (:authority pseudo-header).
    // An explicit `host` header mismatching the URI host makes
    // the h3 crate fail the header build (H3_INTERNAL_ERROR).
    let mut builder = http::Request::builder()
        .method(method)
        .uri(format!("https://{authority}{path}"));
    for (name, value) in extra_headers {
        builder = builder.header(*name, value.as_str());
    }
    let req = builder
        .body(())
        .map_err(|e| format!("request build: {e}"))?;
    let mut stream = send_request
        .send_request(req)
        .await
        .map_err(|e| format!("send request: {e}"))?;
    if !request_body.is_empty() {
        let data = Bytes::copy_from_slice(request_body);
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
}

/// The send-request handle type for a pooled h3 connection.
pub type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Completes the h3 handshake on an established QUIC connection:
/// returns the send-request handle; the returned driver future must
/// be polled (spawn it) for the connection to make progress.
///
/// # Errors
/// Handshake failure as a string.
pub async fn client_new(
    quinn_conn: quinn::Connection,
) -> Result<
    (
        h3::client::Connection<h3_quinn::Connection, Bytes>,
        H3SendRequest,
    ),
    String,
> {
    let (driver, send_request) = h3::client::new(h3_quinn::Connection::new(quinn_conn))
        .await
        .map_err(|e| format!("h3 handshake: {e}"))?;
    Ok((driver, send_request))
}
