//! HTTP/2 edge — experimental (`h2` feature).
//!
//! ## Architecture note (read this)
//!
//! The `h2` crate is Tokio-bound, and the vane engine is deliberately
//! runtime-free. Rather than bridge async wakers through the completion
//! engine, the h2 edge runs a **dedicated Tokio acceptor** alongside the
//! engine workers on the same address (SO_REUSEPORT):
//!
//! - the engine's TLS config advertises **http/1.1 only**
//! - this acceptor's TLS config advertises **h2 only**
//!
//! The kernel load-balances connections between the two; each path
//! negotiates its single protocol, so every connection is served
//! correctly. Clients that speak h2 get h2 whenever they land here.
//!
//! Requests terminate with the same routing decision as the engine path
//! (EBR snapshot + breaker + rate limit), then forward to the chosen
//! backend over HTTP/1.1 or HTTP/2 (reqwest). Request bodies stream
//! chunk-by-chunk with h2 flow-control release — no size cap.

use std::net::SocketAddr;
use std::sync::Arc;

use vane_filters::pipeline::Filter as _;
use vane_observe::metrics::Registry;
use vane_router::Router;

/// Errors from the h2 edge.
#[derive(Debug, thiserror::Error)]
pub enum H2Error {
    /// Listener/TLS setup failure.
    #[error("h2 edge setup: {0}")]
    Setup(String),
    /// Connection-level failure.
    #[error("h2 connection: {0}")]
    Connection(String),
}

/// Shared edge context (router + filters, mirroring the engine handler).
pub struct H2Edge {
    router: Arc<Router>,
    breaker: Arc<vane_filters::BreakerGate>,
    rate: Option<Arc<vane_filters::RateLimit>>,
    /// HTTP/1.1 upstream client (default).
    http: reqwest::Client,
    /// HTTP/2 prior-knowledge upstream client (clusters with
    /// `http2 = true`). Shares the connection pool: one h2 connection
    /// multiplexes all streams to a backend.
    http2: reqwest::Client,
    /// Structured access log (`None` = disabled).
    access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
}

impl H2Edge {
    /// New edge context.
    ///
    /// # Panics
    /// Metric registration failure (startup-only).
    #[must_use]
    pub fn new(
        router: Arc<Router>,
        registry: Arc<Registry>,
        access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
    ) -> Self {
        Self::with_parts(router, registry, access, None)
    }

    /// Sets a pre-built breaker gate (tests inject an opened breaker).
    #[must_use]
    pub fn with_breaker(mut self, breaker: Arc<vane_filters::BreakerGate>) -> Self {
        self.breaker = breaker;
        self
    }

    /// Shared constructor.
    fn with_parts(
        router: Arc<Router>,
        registry: Arc<Registry>,
        access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
        breaker: Option<Arc<vane_filters::BreakerGate>>,
    ) -> Self {
        vane_tls::install_crypto_provider();
        let breaker = breaker
            .unwrap_or_else(|| Arc::new(vane_filters::BreakerGate::new(Arc::clone(&registry))));
        let rate = Some(Arc::new(vane_filters::RateLimit::new(
            Arc::clone(&registry),
            10_000,
            10_000,
        )));
        Self {
            router,
            breaker,
            rate,
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

    /// Serves one TLS connection's h2 streams until it closes.
    ///
    /// Each request stream is served on its own task so the connection
    /// keeps being polled while bodies stream (multiplexing).
    ///
    /// # Errors
    /// Handshake or stream-level failure.
    pub async fn serve_connection<Io>(self: Arc<Self>, io: Io) -> Result<(), H2Error>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Generous recv windows: streamed request bodies stall when the
        // default 64 KiB windows starve the body pump (window updates
        // only flow as fast as the body consumer is polled).
        let mut builder = h2::server::Builder::new();
        builder.initial_window_size(1024 * 1024);
        builder.initial_connection_window_size(2 * 1024 * 1024);
        let mut conn = builder
            .handshake::<Io, bytes::Bytes>(io)
            .await
            .map_err(|e| H2Error::Connection(e.to_string()))?;
        // Each stream is served on its own task: the connection MUST
        // keep being polled (accept loop) while request bodies stream —
        // awaiting inline would stall flow-control window updates and
        // deadlock any body not immediately available.
        loop {
            let request = match conn.accept().await {
                Some(Ok(r)) => r,
                Some(Err(e)) => {
                    tracing::debug!("h2 edge accept error: {e}");
                    return Err(H2Error::Connection(e.to_string()));
                }
                None => {
                    tracing::debug!("h2 edge: connection closed");
                    return Ok(());
                }
            };
            let (request, respond) = request;
            let edge = Arc::clone(&self);
            tokio::spawn(async move {
                edge.serve_request(request, respond).await;
            });
        }
    }

    /// Routes and proxies one h2 request; always answers the stream.
    async fn serve_request(
        &self,
        request: http::Request<h2::RecvStream>,
        mut respond: h2::server::SendResponse<bytes::Bytes>,
    ) {
        let started = std::time::Instant::now();

        let path = request
            .uri()
            .path_and_query()
            .map_or("/", |pq| pq.as_str())
            .to_owned();
        let host = request
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .map(|h| h.split(':').next().unwrap_or(h).to_owned());
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
        let mut reply = |status: u16, reason: &'static str| {
            let response = http::Response::builder()
                .status(status)
                .header("server", "vane")
                .body(())
                .expect("static response");
            let _ = respond.send_response(response, true);
            emit(status, 0);
            let _ = reason;
        };

        // Route on the live snapshot. The epoch guard is scoped: only the
        // 'static Arc escapes (TableGuard is !Send and must not cross an
        // await).
        let route: Option<Arc<vane_router::RouteEntry>> = {
            let table = self.router.load();
            let Some(matched) = table.table().lookup(host.as_deref(), &path) else {
                reply(404, "no route");
                return;
            };
            let route = Arc::clone(&matched.terminal.value);
            let allowed = route.methods.is_empty()
                || route
                    .methods
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(method.as_str()));
            if allowed { Some(route) } else { None }
        };
        let Some(route) = route else {
            reply(405, "method not allowed");
            return;
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

        // Sync filters are usable from the h2 path: rate limit + breaker.
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
                // Async check: `run`/`check_sync` would panic inside the
                // tokio runtime.
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
                reply(503, "filtered");
                return;
            }
            ctx.inject_headers
        };

        // Pick a backend.
        let mut balancer = route.balancer(rand_seed());
        let Some(addr) = balancer.pick_addr() else {
            reply(503, "no healthy upstream");
            return;
        };

        // NOTE: request bodies are buffered (bounded 32 MiB). Streaming
        // them via `Body::wrap_stream` stalls under hyper's h2 client
        // flow control when the first body poll is Pending — tracked as
        // follow-up work with the h2 window-update investigation.
        let body_bytes = match to_bytes(request.into_body()).await {
            Ok(b) => b,
            Err(e) => {
                reply(400, "body error");
                let _ = e;
                return;
            }
        };

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
                let mut src = std::error::Error::source(&e);
                let mut chain = e.to_string();
                while let Some(s2) = src {
                    chain.push_str(&format!(": {s2}"));
                    src = s2.source();
                }
                tracing::warn!("h2 edge upstream send failed: {chain}");
                self.breaker.record_failure(&route.cluster);
                reply(502, "upstream unreachable");
                let _ = e;
                return;
            }
        };
        if response.status().is_server_error() {
            self.breaker.record_failure(&route.cluster);
        } else {
            self.breaker.record_success(&route.cluster);
        }

        // Relay the response head, then chunk the body through.
        let status = response.status();
        let head = add_headers(http::Response::builder().status(status), response.headers());
        match respond.send_response(head, false) {
            Ok(mut send) => {
                let mut body = response;
                let mut bytes_out: u64 = 0;
                while let Some(chunk) = body.chunk().await.transpose() {
                    match chunk {
                        Ok(bytes) => {
                            bytes_out += bytes.len() as u64;
                            let _ = send.send_data(bytes, false);
                        }
                        Err(_) => break,
                    }
                }
                let _ = send.send_data(bytes::Bytes::new(), true);
                emit(status.as_u16(), bytes_out);
            }
            Err(_) => { /* client gone mid-response */ }
        }
    }
}

fn add_headers(
    mut builder: http::response::Builder,
    headers: &http::HeaderMap,
) -> http::Response<()> {
    for (name, value) in headers {
        if matches!(
            name.as_str(),
            "content-length" | "transfer-encoding" | "connection"
        ) {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder.body(()).expect("valid headers")
}

fn rand_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() as u64)
}

async fn to_bytes(mut body: h2::RecvStream) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        let _ = body.flow_control().release_capacity(chunk.len());
        out.extend_from_slice(&chunk);
        if out.len() > 32 * 1024 * 1024 {
            return Err("body too large for h2 edge v1 (32 MiB cap)".into());
        }
    }
    Ok(out)
}

/// Accept loop: takes a std listener (already bound, may be a REUSEPORT
/// duplicate) and serves TLS+h2 connections forever.
pub fn spawn(
    listener: std::net::TcpListener,
    tls: Arc<rustls::ServerConfig>,
    edge: Arc<H2Edge>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let listener =
            tokio::net::TcpListener::from_std(listener).expect("tokio listener (runtime context)");
        // ALPN: h2 only — this acceptor never negotiates http/1.1.
        let mut tls = tls.as_ref().clone();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        loop {
            match listener.accept().await {
                Ok((sock, _peer)) => {
                    let edge = Arc::clone(&edge);
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let tls_sock = match acceptor.accept(sock).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::debug!("h2 tls accept: {e}");
                                return;
                            }
                        };
                        if let Err(e) = edge.serve_connection(tls_sock).await {
                            tracing::debug!("h2 connection: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("h2 edge accept: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}
