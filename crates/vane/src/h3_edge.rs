//! HTTP/3 edge — experimental (`h3` feature).
//!
//! HTTP/3 over QUIC using quinn as the transport and the `h3` crate for
//! HTTP/3 framing. Runs on a UDP port alongside the TCP listeners.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ UDP :8443 (same port number as the TCP HTTPS listener)│
//! │         │                                             │
//! │   quinn::Endpoint (QUIC transport)                    │
//! │         │                                             │
//! │   h3::server::Connection (HTTP/3 framing)             │
//! │         │                                             │
//! │   Request → route lookup → upstream forward          │
//! │   (shared with the h1/h2 pipeline via the EBR        │
//! │    snapshot — no separate routing state)             │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! The edge mirrors `h2_edge`: the transport differs (QUIC instead of
//! TCP+TLS), the request handling shares the router snapshot, filter
//! chain, balancer, and upstream clients.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use vane_filters::pipeline::Filter as _;
use vane_router::Router;

/// HTTP/3 ALPN protocol identifier.
pub const H3_ALPN: &[u8] = b"h3";

/// The h3 server request-stream type over the h3-quinn adapter.
type H3RequestStream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// Returns the ALPN protocols that the h3 edge negotiates.
#[must_use]
pub fn alpn_protocols() -> Vec<Vec<u8>> {
    vec![H3_ALPN.to_vec()]
}

/// Returns the UDP address for the h3 edge (same port number as the
/// TCP HTTPS listener — QUIC and TCP coexist on the same port).
#[must_use]
pub fn quinn_addr(tcp_addr: SocketAddr) -> SocketAddr {
    tcp_addr
}

/// QUIC TLS configuration for the h3 edge.
///
/// Builds a `quinn::crypto::rustls::QuicServerConfig` from the given
/// rustls config with h3 ALPN.
///
/// # Errors
/// Returns an error if the rustls config cannot be adapted for QUIC.
pub fn quinn_server_config(
    tcp_cfg: &rustls::ServerConfig,
) -> Result<quinn::crypto::rustls::QuicServerConfig, String> {
    let mut cfg = tcp_cfg.clone();
    cfg.alpn_protocols = vec![H3_ALPN.to_vec()];
    quinn::crypto::rustls::QuicServerConfig::try_from(cfg)
        .map_err(|e| format!("quinn server config: {e}"))
}

/// Shared edge context (router + filters, mirroring `h2_edge`).
pub struct H3Edge {
    router: Arc<Router>,
    breaker: Arc<vane_filters::BreakerGate>,
    rate: Option<Arc<vane_filters::RateLimit>>,
    /// HTTP/1.1 upstream client (default).
    http: reqwest::Client,
    /// HTTP/2 prior-knowledge upstream client (clusters with
    /// `http2 = true`).
    http2: reqwest::Client,
    /// Structured access log (`None` = disabled).
    access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
}

impl H3Edge {
    /// New edge context.
    #[must_use]
    pub fn new(
        router: Arc<Router>,
        registry: Arc<vane_observe::metrics::Registry>,
        access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
    ) -> Self {
        let breaker = Arc::new(vane_filters::BreakerGate::new(Arc::clone(&registry)));
        Self {
            router,
            breaker,
            rate: None,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            http2: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .http2_prior_knowledge()
                .build()
                .expect("reqwest h2 client"),
            access,
        }
    }

    /// Accepts and serves connections until the endpoint closes.
    pub async fn serve(self: Arc<Self>, endpoint: quinn::Endpoint) {
        while let Some(incoming) = endpoint.accept().await {
            let edge = Arc::clone(&self);
            tokio::spawn(async move {
                let conn = match incoming.await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!("h3 quinn connect: {e}");
                        return;
                    }
                };
                if let Err(e) = edge.serve_connection(conn).await {
                    tracing::debug!("h3 connection: {e}");
                }
            });
        }
    }

    /// Serves one QUIC connection's request streams.
    async fn serve_connection(
        &self,
        conn: quinn::Connection,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut h3_conn = h3::server::Connection::new(h3_quinn::Connection::new(conn))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        loop {
            match h3_conn.accept().await {
                Ok(Some(resolver)) => {
                    let (request, stream) = resolver.resolve_request().await?;
                    self.serve_request(request, stream).await;
                }
                Ok(None) => break,
                Err(e) => return Err(Box::new(e)),
            }
        }
        Ok(())
    }

    async fn serve_request(&self, request: http::Request<()>, mut stream: H3RequestStream) {
        let started = std::time::Instant::now();
        let span = crate::tracing_util::serve_span(
            request.method().as_str(),
            request.uri().path(),
            request.headers().get("host").and_then(|h| h.to_str().ok()),
            request
                .headers()
                .get("traceparent")
                .and_then(|h| h.to_str().ok()),
        );

        let path = request
            .uri()
            .path_and_query()
            .map_or("/", |pq| pq.as_str())
            .to_owned();
        let host = request
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .map(|h| h.split(':').next().unwrap_or(h).to_owned())
            // h3: :authority lands in the request URI, not a host
            // header.
            .or_else(|| request.uri().host().map(|h| h.to_owned()));
        let method = request.method().clone();

        let (method_l, host_l, path_l) = (
            method.as_str().to_owned(),
            host.clone().unwrap_or_default(),
            path.clone(),
        );
        let emit = move |status: u16, bytes_out: u64| {
            if let Some(access) = &self.access {
                let rec = vane_observe::access::AccessRecord::now(
                    u16::MAX,
                    status,
                    u32::try_from(started.elapsed().as_micros()).unwrap_or(u32::MAX),
                    bytes_out,
                    ([0u8; 16], 0),
                    None,
                    method_l.as_bytes(),
                    host_l.as_bytes(),
                    path_l.as_bytes(),
                    &[],
                );
                access.emit(rec);
            }
        };
        macro_rules! reply {
            ($status:expr, $reason:expr) => {{
                let status: u16 = $status;
                let _ = $reason;
                let response = http::Response::builder()
                    .status(status)
                    .header("server", "vane")
                    .body(())
                    .expect("static response");
                if let Err(e) = stream.send_response(response).await {
                    tracing::debug!("h3 reply {status}: {e}");
                    return;
                }
                span.record("http.status_code", status);
                emit(status, 0);
                return;
            }};
        }

        // Route on the live snapshot. Phase-split so the !Send
        // TableGuard never lives across an await (the reply paths
        // below await on the h3 stream).
        enum RouteCheck {
            Route(Arc<vane_router::RouteEntry>),
            Method,
            NotFound,
        }
        let check = {
            let table = self.router.load();
            match table
                .table()
                .lookup(host.as_deref(), &path)
                .map(|matched| Arc::clone(&matched.terminal.value))
            {
                Some(route) => {
                    let allowed = route.methods.is_empty()
                        || route
                            .methods
                            .iter()
                            .any(|m| m.eq_ignore_ascii_case(method.as_str()));
                    if allowed {
                        RouteCheck::Route(route)
                    } else {
                        RouteCheck::Method
                    }
                }
                None => RouteCheck::NotFound,
            }
        };
        let route = match check {
            RouteCheck::NotFound => {
                reply!(404, "no route");
            }
            RouteCheck::Method => {
                reply!(405, "method not allowed");
            }
            RouteCheck::Route(r) => r,
        };
        let upstream_path = route
            .strip_prefix
            .as_ref()
            .and_then(|p| path.strip_prefix(p.as_str()))
            .map_or_else(
                || path.clone(),
                |rest| {
                    if rest.starts_with('/') {
                        rest.to_owned()
                    } else {
                        format!("/{rest}")
                    }
                },
            );

        // Filters: rate limit (async) + breaker (sync).
        let inject = {
            let mut path_mut = path.clone();
            let mut ctx = vane_filters::RequestCtx::new(
                method.as_str(),
                &mut path_mut,
                SocketAddr::from(([0, 0, 0, 0], 0)),
                host.as_deref(),
            );
            ctx.cluster = Some(&route.cluster);
            ctx.inject("X-Forwarded-Proto", "https");
            let mut rejected = false;
            if let Some(rate) = &self.rate {
                if matches!(
                    rate.check_async(ctx.client.ip().to_string().as_str()).await,
                    vane_filters::Outcome::Reject(..)
                ) {
                    rejected = true;
                }
            }
            if !rejected
                && matches!(
                    self.breaker.run(&mut ctx),
                    vane_filters::Outcome::Reject(..)
                )
            {
                rejected = true;
            }
            if rejected {
                reply!(503, "filtered");
            }
            ctx.inject_headers
        };

        let mut balancer = route.balancer(rand_seed());
        let Some(addr) = balancer.pick_addr() else {
            reply!(503, "no healthy upstream");
        };

        // Buffer the request body (bounded, mirroring h2_edge).
        use bytes::Buf as _;
        let mut body_bytes: Vec<u8> = Vec::new();
        loop {
            match stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    if body_bytes.len() + chunk.remaining() > 32 * 1024 * 1024 {
                        reply!(413, "body too large");
                    }
                    body_bytes.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let client = if route.upstream_h2 {
            &self.http2
        } else {
            &self.http
        };
        let url = format!("http://{addr}{upstream_path}");
        let mut upstream = client.request(method, &url);
        for (name, value) in &inject {
            upstream = upstream.header(name.as_str(), value.as_str());
        }
        let response = match upstream.body(body_bytes).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("h3 edge upstream send failed: {e}");
                self.breaker.record_failure(&route.cluster);
                reply!(502, "upstream unreachable");
            }
        };
        if response.status().is_server_error() {
            self.breaker.record_failure(&route.cluster);
        } else {
            self.breaker.record_success(&route.cluster);
        }

        let status = response.status();
        let head = add_headers(http::Response::builder().status(status), response.headers());
        if stream.send_response(head).await.is_err() {
            return; // client gone mid-response
        }
        let mut body = response;
        let mut bytes_out: u64 = 0;
        while let Some(chunk) = body.chunk().await.transpose() {
            match chunk {
                Ok(bytes) => {
                    bytes_out += bytes.len() as u64;
                    if stream.send_data(bytes).await.is_err() {
                        return; // client gone
                    }
                }
                Err(_) => break,
            }
        }
        let _ = stream.finish().await;
        span.record("http.status_code", status.as_u16());
        span.record("http.response_size", bytes_out);
        emit(status.as_u16(), bytes_out);
    }
}

fn rand_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0x9e37_79b9_7f4a_7c15, |d| d.as_nanos() as u64)
        ^ (std::process::id() as u64) << 32
}

fn add_headers(builder: http::response::Builder, headers: &http::HeaderMap) -> http::Response<()> {
    let mut b = builder.header("server", "vane");
    for (name, value) in headers {
        // HTTP/3 forbids connection-specific headers (RFC 9114 §4.2).
        let n = name.as_str();
        if matches!(
            n,
            "connection" | "keep-alive" | "transfer-encoding" | "upgrade" | "proxy-connection"
        ) {
            continue;
        }
        b = b.header(name, value);
    }
    b.body(()).expect("static response")
}

/// Spawns the h3 edge: builds the quinn endpoint on `udp` and serves
/// until the runtime shuts down.
pub fn spawn(
    udp: std::net::UdpSocket,
    tls: Arc<rustls::ServerConfig>,
    edge: Arc<H3Edge>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tls = tls.as_ref().clone();
        tls.alpn_protocols = alpn_protocols();
        let qcfg = match quinn_server_config(&tls) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("h3 edge: {e}");
                return;
            }
        };
        let server = quinn::ServerConfig::with_crypto(Arc::new(qcfg));
        let endpoint = match quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server),
            udp,
            Arc::new(quinn::TokioRuntime),
        ) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("h3 endpoint bind: {e}");
                return;
            }
        };
        tracing::info!("h3 edge: serving on {:?}", endpoint.local_addr().ok());
        edge.serve(endpoint).await;
    })
}
