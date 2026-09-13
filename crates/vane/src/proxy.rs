//! The HTTP reverse-proxy handler: parses request heads, routes through
//! the EBR snapshot, applies the monomorphized filter pipeline, relays to
//! upstreams with keep-alive, and short-circuits errors.

use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use vane_core::handler::{Handler, SessionIo};
use vane_filters::pipeline::Filter as _;
use vane_filters::{BreakerGate, Outcome, Pipeline, RateLimit, RequestCtx};
use vane_observe::metrics::{MetricHandle, MetricKind, Registry};
use vane_observe::ring::EventRing;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Packs an IP into the v4-mapped 16-byte form used by access records.
fn ip16(ip: std::net::IpAddr) -> [u8; 16] {
    match ip {
        std::net::IpAddr::V4(v4) => {
            let o = v4.octets();
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, o[0], o[1], o[2], o[3],
            ]
        }
        std::net::IpAddr::V6(v6) => v6.octets(),
    }
}
use vane_observe::{LogEvent, LogLevel};
use vane_proto::request::{MAX_HEADERS, Parsed, RequestView};
use vane_proto::response::{Status, write_full};
use vane_router::{RouteEntry, Router};
use vane_shm::handover::RouteRecord;

/// Proxy handler configuration shared across workers.
pub struct ProxyConfig {
    /// Router (EBR snapshot source).
    pub router: Arc<Router>,
    /// Metrics.
    pub registry: Arc<Registry>,
    /// Worker event ring (access logs).
    pub events: Arc<EventRing<vane_observe::LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
    /// Enable the GCRA rate-limit filter (default per-second).
    pub rate_limit_rps: Option<u32>,
    /// Upstream connect timeout.
    pub connect_timeout_ms: u64,
    /// Idle session deadline.
    pub idle_timeout_ms: u64,
    /// Upstream first-byte timeout.
    pub first_byte_timeout_ms: u64,
    /// Idle upstream connections kept per backend (per worker).
    pub pool_per_backend: usize,
    /// TLS termination config (`None` = plaintext listener). Shared slot:
    /// hot reload swaps the inner Arc without touching workers.
    pub tls: Option<Arc<std::sync::RwLock<Arc<rustls::ServerConfig>>>>,
    /// Shared HTTP-01 token map (Some when ACME is configured).
    pub http01_tokens: Option<Arc<std::sync::Mutex<HashMap<String, String>>>>,
    /// Wasm plugin module paths (feature `wasm`; applied to every request
    /// in order, before routing).
    pub plugins: Vec<String>,
    /// Structured access log (`None` = disabled — no per-request work).
    pub access: Option<std::sync::Arc<vane_observe::access::AccessLog>>,
    /// JWT bearer authentication (`None` = disabled).
    pub jwt: Option<std::sync::Arc<vane_filters::jwt::JwtValidator>>,
    /// Process-wide GCRA bucket (`Some` replaces the per-worker
    /// limiter so the configured rate is the true aggregate).
    pub shared_rate_limit: Option<std::sync::Arc<vane_shm::ratelimit::SharedGcra>>,
    /// Serve h2c (prior-knowledge HTTP/2) on this plain listener.
    pub h2c: bool,
    /// L4 splice mode (`mode = "tcp"` listeners): kernel zero-copy
    /// passthrough to the first matching route's cluster — no HTTP
    /// parsing on the data path.
    pub l4_splice: bool,
}

/// Per-connection state.
#[derive(Default)]
struct Conn {
    head_buf: Vec<u8>,
    /// Matched route for the in-flight request.
    route: Option<Arc<RouteEntry>>,
    /// Rewritten request path.
    upstream_path: Option<String>,
    /// Pipeline verdict headers to inject.
    inject: Vec<(String, String)>,
    /// `Connection: close` for this transaction.
    close_after: bool,
    /// Response body framing decided from the upstream head.
    body: BodyFraming,
    /// WebSocket tunnel active (101 switched): raw bidirectional pump.
    tunnel: bool,
    remaining: u64,
    started: Option<Instant>,
    trace: Option<vane_observe::trace::TraceContext>,
    /// TLS termination state (TLS listeners only).
    tls: Option<rustls::ServerConnection>,
    /// Upstream established (connect finished / pooled attach).
    upstream_ready: bool,
    /// Where the current upstream dial went.
    upstream_addr: Option<SocketAddr>,
    /// Connect attempts for the current transaction (failover cap).
    attempts: u8,
    /// Access-log fields for the in-flight transaction.
    req_method: String,
    req_host: Option<String>,
    req_path: String,
    resp_status: u16,
    bytes_out: u64,
    /// Set once the transaction's access record was emitted.
    access_logged: bool,
    /// L4 splice mode session (no HTTP parsing).
    l4: bool,
    /// OTLP span for this transaction (recorded + dropped at completion).
    /// Stored (not entered) because entry guards cannot cross the await
    /// points in the worker loop; entered briefly at completion sites.
    span: Option<tracing::Span>,
    /// Request-body relay state.
    req_framing: ReqFraming,
    /// Body bytes held handler-side until the upstream connects
    /// (preserves ordering: inline bytes forward first, then these).
    req_pending: Vec<u8>,
    /// HTTP/2 server session (engine TLS path, ALPN negotiated h2).
    #[cfg(feature = "h2")]
    h2: Option<Box<crate::h2_server::H2Server>>,
    /// HTTP/2 upstream client (route `upstream_h2 = true`).
    #[cfg(feature = "h2")]
    h2up: Option<Box<crate::h2_client::H2Upstream>>,
    /// h2 frames queued for the peer (drained on flush).
    #[cfg(feature = "h2")]
    h2_out: Vec<u8>,
    /// Ciphertext received but not yet consumed by rustls (partial
    /// TLS records straddling read boundaries).
    tls_backlog: Vec<u8>,
    /// Response compression state (Some = gzip the relay).
    gzip: Option<GzipRelay>,
}

/// Streaming gzip relay state for one response transaction.
struct GzipRelay {
    stream: vane_proto::compression::GzipStream,
}

/// Request-side body framing (streaming relay — bodies are never
/// buffered whole; bytes go upstream as read events arrive).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum ReqFraming {
    /// Head not yet handled for this connection.
    #[default]
    AwaitingHead,
    /// Content-Length body: bytes remaining to relay.
    ContentLength(u64),
    /// Chunked request body: relay raw until the terminal 0-chunk.
    Chunked { seen_zero: bool },
    /// No body / body complete.
    Done,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum BodyFraming {
    #[default]
    AwaitingHead,
    /// Pass raw bytes until `remaining` bytes forwarded.
    ContentLength,
    /// Pass chunked bytes until terminal 0-chunk observed (len tracking).
    Chunked { last_was_lf: bool, seen_zero: bool },
    /// WebSocket/protocol tunnel: raw bidirectional pump.
    Tunnel,
    /// Response complete.
    Done,
}

/// The HTTP/1.1 proxy handler. One instance per worker.
pub struct HttpProxy {
    config: ProxyConfig,
    /// Pipeline: access-log -> rate-limit -> breaker -> forwarded headers.
    pipeline: Pipeline<
        vane_filters::pipeline::Chain<vane_filters::RateLimit, vane_filters::pipeline::Nil>,
    >,
    breaker: Arc<BreakerGate>,
    conns: HashMap<u32, Conn>,
    /// Per-worker date cache (one refresh/second, zero alloc otherwise).
    date: vane_proto::date::DateCache,
    /// Idle upstream connection pool (per backend, worker-local).
    pool: HashMap<SocketAddr, Vec<RawFd>>,
    /// Per-cluster request counters (lazily registered, one per cluster).
    cluster_metrics: HashMap<String, ClusterMetric>,
    /// Loaded plugin instances (one set per worker; instances are !Sync).
    #[cfg(feature = "wasm")]
    plugins: Vec<vane_plugins::PluginInstance>,
    metrics: ProxyMetrics,
    worker_id: usize,
}

struct ProxyMetrics {
    requests: MetricHandle,
    responses: MetricHandle,
    upstream_errors: MetricHandle,
    bytes_in: MetricHandle,
    bytes_out: MetricHandle,
    latency: MetricHandle,
}

/// Lazily-registered per-cluster counters. Registration takes a brief
/// control-plane lock on first sight of a cluster; every subsequent
/// request is lock-free.
#[derive(Clone)]
struct ClusterMetric {
    requests: MetricHandle,
    errors: MetricHandle,
}

impl ClusterMetric {
    fn register(registry: &Registry, cluster: &str) -> Self {
        // Prometheus label sanitation: [a-zA-Z0-9_].
        let safe: String = cluster
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        Self {
            requests: registry.register(
                &format!("vane_cluster_{safe}_requests_total"),
                MetricKind::Counter,
            ),
            errors: registry.register(
                &format!("vane_cluster_{safe}_upstream_errors_total"),
                MetricKind::Counter,
            ),
        }
    }
}

impl HttpProxy {
    /// Builds a handler for one worker.
    ///
    /// # Panics
    /// Metric registration failure (startup-only).
    #[must_use]
    pub fn new(config: ProxyConfig, worker_id: usize) -> Self {
        let registry = Arc::clone(&config.registry);
        let metrics = ProxyMetrics {
            requests: registry.register("vane_http_requests_total", MetricKind::Counter),
            responses: registry.register("vane_http_responses_total", MetricKind::Counter),
            upstream_errors: registry.register("vane_upstream_errors_total", MetricKind::Counter),
            bytes_in: registry.register("vane_bytes_in_total", MetricKind::Counter),
            bytes_out: registry.register("vane_bytes_out_total", MetricKind::Counter),
            latency: registry.register_histogram("vane_request_duration_us"),
        };
        let breaker = Arc::new(BreakerGate::new(Arc::clone(&registry)));
        // None = unlimited in practice (1M rps burst; GCRA stays O(1) and
        // rate limiting remains opt-in via config).
        // With a shared bucket the aggregate is enforced there, so the
        // per-worker stage runs wide open (it would multiply the rate).
        let (rps, burst) = if config.shared_rate_limit.is_some() {
            (1_000_000, 1_000_000)
        } else {
            config
                .rate_limit_rps
                .map_or((1_000_000, 1_000_000), |r| (r, r))
        };
        let pipeline = Pipeline::new().then(RateLimit::new(Arc::clone(&registry), rps, burst));
        // Wasm plugins (feature `wasm`): compiled per module, instantiated
        // once per worker.
        #[cfg(feature = "wasm")]
        let plugins: Vec<vane_plugins::PluginInstance> = config
            .plugins
            .iter()
            .filter_map(|path| {
                match vane_plugins::PluginModule::compile(std::path::Path::new(path)) {
                    Ok(m) => match m.instantiate() {
                        Ok(inst) => Some(inst),
                        Err(e) => {
                            eprintln!("[vane:warn] plugin {path}: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        eprintln!("[vane:warn] plugin {path}: {e}");
                        None
                    }
                }
            })
            .collect();
        #[cfg(not(feature = "wasm"))]
        if !config.plugins.is_empty() {
            eprintln!(
                "[vane:warn] {} plugin(s) configured but built without the `wasm` feature",
                config.plugins.len()
            );
        }
        Self {
            config,
            pipeline,
            breaker,
            conns: HashMap::new(),
            date: vane_proto::date::DateCache::new(),
            pool: HashMap::new(),
            cluster_metrics: HashMap::new(),
            #[cfg(feature = "wasm")]
            plugins,
            metrics,
            worker_id,
        }
    }

    /// Deadline helpers (shared arithmetic).
    fn deadline(&self, ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    /// Writes bytes downstream, encrypting through TLS when terminated.
    fn write_downstream(&mut self, io: &mut SessionIo<'_>, bytes: &[u8]) {
        let slot = io.slot_index();
        #[cfg(feature = "h2")]
        if self.conns.get(&slot).is_some_and(|c| c.h2.is_some()) {
            // h2 sessions translate response bytes into frames.
            self.h2_write(io, bytes);
            return;
        }
        let Some(conn) = self.conns.get_mut(&slot) else {
            return;
        };
        conn.bytes_out += bytes.len() as u64;
        let Some(tls) = &mut conn.tls else {
            io.respond(bytes);
            return;
        };
        let _ = tls.writer().write_all(bytes); // io::Write via import
        // Flush the TLS record layer out in one batch.
        let mut out = Vec::with_capacity(16 * 1024);
        loop {
            let mut buf = [0u8; 16 * 1024];
            let n = tls.write_tls(&mut buf.as_mut_slice()).unwrap_or(0);
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            if out.len() > 512 * 1024 {
                io.respond(&out);
                out.clear();
            }
        }
        if !out.is_empty() {
            io.respond(&out);
        }
    }

    /// Feeds ciphertext into the TLS state machine; returns decrypted
    /// application bytes (empty during handshake).
    fn tls_read(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        let Some(conn) = self.conns.get_mut(&slot) else {
            return;
        };
        let Some(tls) = &mut conn.tls else {
            self.plain_request_data(io, data);
            return;
        };
        // Feed through a persistent backlog: rustls' read_tls leaves
        // partial trailing records unconsumed, and the engine's next
        // read event must line up behind them.
        let mut backlog = std::mem::take(&mut conn.tls_backlog);
        backlog.extend_from_slice(data);
        let consumed = tls.read_tls(&mut io::Cursor::new(&backlog)).unwrap_or(0);
        backlog.drain(..consumed);
        let mut decrypted = Vec::new();
        // Process until no more plaintext emerges: the peer may batch its
        // Finished flight and application data in one TCP segment.
        let mut first = true;
        loop {
            if let Err(e) = tls.process_new_packets() {
                self.log(LogLevel::Warn, &format!("tls handshake error: {e}"));
                io.close();
                return;
            }
            let mut n_total = 0usize;
            let mut buf = [0u8; 16 * 1024];
            loop {
                let n = tls.reader().read(&mut buf[..]).unwrap_or(0);
                if n == 0 {
                    break;
                }
                decrypted.extend_from_slice(&buf[..n]);
                n_total += n;
            }
            // Terminate when the handshake is done and no data remains, or
            // when a second call yields nothing (nothing buffered).
            let handshaking = tls.is_handshaking();
            if !handshaking && n_total == 0 {
                break;
            }
            if n_total == 0 && !first {
                break;
            }
            first = false;
        }
        // Park the unconsumed ciphertext tail with the connection.
        conn.tls_backlog = backlog;
        // ALPN is only known once the handshake completes: promote the
        // session to HTTP/2 here (on_connected fires too early).
        #[cfg(feature = "h2")]
        let alpn_h2 = {
            let handshaking = conn.tls.as_ref().is_some_and(|t| t.is_handshaking());
            !handshaking
                && conn
                    .tls
                    .as_ref()
                    .is_some_and(|t| t.alpn_protocol() == Some(b"h2"))
        };
        // Emit any TLS flight (ServerHello..Finished) before app data.
        self.flush_tls(io);
        #[cfg(feature = "h2")]
        if alpn_h2 && self.conns.get(&slot).is_some_and(|c| c.h2.is_none()) {
            let mut h2s = Box::new(crate::h2_server::H2Server::new(slot));
            // The engine's initial SETTINGS were queued at
            // construction — flush them before app data.
            let out = h2s.pending_writes();
            self.conn(slot).h2 = Some(h2s);
            if !out.is_empty() {
                self.raw_downstream(io, &out);
            }
        }
        if !decrypted.is_empty() {
            self.plain_request_data(io, &decrypted);
        }
    }

    /// Flushes pending TLS records.
    fn flush_tls(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let Some(conn) = self.conns.get_mut(&slot) else {
            return;
        };
        let Some(tls) = &mut conn.tls else { return };
        let mut out = Vec::new();
        loop {
            let mut buf = [0u8; 16 * 1024];
            let n = tls.write_tls(&mut buf.as_mut_slice()).unwrap_or(0);
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        if !out.is_empty() {
            io.respond(&out);
        }
    }

    /// Plaintext request bytes (post-TLS or plain listener).
    /// Raw TLS-wrapped write to the client (shared h1/h2 writer).
    #[cfg(feature = "h2")]
    fn h2_intake(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        let Some(mut h2s) = self.conn(slot).h2.take() else {
            return;
        };
        let mut events = Vec::new();
        h2s.handle_read(data, &mut events);
        self.conn(slot).h2 = Some(h2s);
        for ev in events {
            self.h2_event(io, ev);
        }
        self.h2_flush(io);
    }

    /// Acts on a translated h2 driver event.
    #[cfg(feature = "h2")]
    fn h2_event(&mut self, io: &mut SessionIo<'_>, ev: crate::h2_server::H2Event) {
        match ev {
            crate::h2_server::H2Event::RequestHead { head, end_stream } => {
                // The shim's backlog guarantees the head is complete.
                self.h1_plain_request_data(io, &head);
                if end_stream {
                    let slot = io.slot_index();
                    if let Some(c) = self.conns.get_mut(&slot) {
                        c.req_framing = ReqFraming::Done;
                    }
                }
            }
            crate::h2_server::H2Event::RequestBody { data } => {
                self.h1_plain_request_data(io, &data);
            }
            crate::h2_server::H2Event::RequestComplete => {
                // Content-Length framing terminates the upstream relay
                // naturally (h2 has no chunked encoding).
            }
            crate::h2_server::H2Event::SendCredit => {
                let slot = io.slot_index();
                let (frames, done) = {
                    let Some(h2s) = self.conn(slot).h2.as_mut() else {
                        return;
                    };
                    h2s.take_held()
                };
                for f in frames {
                    self.conn(slot).h2_out.extend_from_slice(&f);
                }
                if done {
                    self.conn(slot).body = BodyFraming::Done;
                    self.h2_flush(io);
                    self.check_done(io);
                } else {
                    self.h2_flush(io);
                }
            }
        }
    }

    /// The HTTP/1.1 upstream response path (also fed by the h2 upstream
    /// client's synthetic h1 stream).
    fn on_upstream_data_h1(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        self.metrics
            .bytes_out
            .add(&self.config.registry, data.len() as u64);
        if self.conn(slot).body == BodyFraming::AwaitingHead {
            // First upstream byte: clear the FirstByte deadline.
            io.set_deadline(None, vane_core::handler::DeadlineReason::FirstByte);
        }
        #[cfg(feature = "h2")]
        if self.conns.get(&slot).is_some_and(|c| c.h2.is_some()) {
            // The h2 arm returns early below; keep deadline handling
            // identical to the h1 path.
            io.set_deadline(None, vane_core::handler::DeadlineReason::FirstByte);
        }
        if self.conn(slot).tunnel {
            // Tunnel: relay raw both ways, no deadlines (long-lived).
            io.set_deadline(None, vane_core::handler::DeadlineReason::Idle);
            let data = data.to_vec();
            self.write_downstream(io, &data);
            return;
        }
        #[cfg(feature = "h2")]
        let is_h2 = self.conns.get(&slot).is_some_and(|c| c.h2.is_some());
        #[cfg(not(feature = "h2"))]
        let is_h2 = false;
        let framing = self.conn(slot).body;
        match framing {
            BodyFraming::AwaitingHead => {
                let (head_len, _status, _close) = self.parse_upstream_head(slot, data);
                if head_len == 0 {
                    self.respond_full(io, Status::BadGateway, "bad upstream response\n");
                    io.close();
                    return;
                }
                self.metrics.responses.inc(&self.config.registry);
                #[cfg(feature = "h2")]
                if is_h2 {
                    // The shim re-parses the head from `data` and
                    // flow-controls the body portion.
                    self.h2_write(io, data);
                    return;
                }
                #[cfg(not(feature = "h2"))]
                let _ = is_h2;
                let head = data[..head_len].to_vec();
                // Gzip relay: compressible type, not already encoded,
                // and a body actually follows.
                let ct = vane_proto::compression::head_header(&head, b"content-type");
                let compressible = vane_proto::compression::is_compressible(ct)
                    && !vane_proto::compression::head_already_encoded(&head)
                    && !matches!(framing, BodyFraming::Done);
                let use_gzip =
                    self.conns.get(&slot).is_some_and(|c| c.gzip.is_some()) && compressible;
                if use_gzip {
                    // Rewrite the head: CL out, gzip + chunked in. The
                    // body relay then compresses through GzipRelay.
                    if let Some(rewritten) = vane_proto::compression::rewrite_head_for_gzip(&head) {
                        self.write_downstream(io, &rewritten);
                    } else {
                        self.write_downstream(io, &head);
                    }
                } else {
                    // Not compressing: release the encoder, relay raw.
                    if let Some(c) = self.conns.get_mut(&slot) {
                        c.gzip = None;
                    }
                    self.write_downstream(io, &head);
                }
                if self.conns.get(&slot).is_some_and(|c| c.tunnel) {
                    // 101 switch: the transaction is complete at upgrade;
                    // tunnel bytes belong to the stream, not this record.
                    let started = self.conns.get(&slot).and_then(|c| c.started);
                    self.access_emit(io, started);
                }
                let rest = &data[head_len..];
                if !rest.is_empty() {
                    self.relay_body(io, rest);
                }
                self.check_done(io);
            }
            BodyFraming::ContentLength | BodyFraming::Chunked { .. } => {
                #[cfg(feature = "h2")]
                if is_h2 {
                    self.h2_write(io, data);
                } else {
                    self.relay_body(io, data);
                    self.check_done(io);
                }
                #[cfg(not(feature = "h2"))]
                {
                    let _ = is_h2;
                    self.relay_body(io, data);
                    self.check_done(io);
                }
            }
            BodyFraming::Tunnel => {
                // Tunnel: relay raw both directions.
                let data = data.to_vec();
                self.write_downstream(io, &data);
            }
            BodyFraming::Done => { /* trailing bytes after done: ignore */ }
        }
    }

    /// h2 upstream intake: driver events become synthetic h1 bytes fed
    /// through the normal response path.
    #[cfg(feature = "h2")]
    fn h2up_intake(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        eprintln!("H2UPDBG intake {} bytes", data.len());
        let mut events = Vec::new();
        {
            let Some(h2up) = self.conn(slot).h2up.as_mut() else {
                return;
            };
            h2up.handle_read(data, &mut events);
        }
        if self.conn(slot).h2up.as_ref().is_some_and(|h| h.failed()) {
            self.log(LogLevel::Warn, "h2 upstream protocol error");
            self.respond_full(io, Status::BadGateway, "upstream unreachable\n");
            io.close();
            return;
        }
        // Synthetic h1 stream, in event order.
        let mut out = Vec::new();
        let mut complete = false;
        for ev in events {
            match ev {
                crate::h2_client::UpstreamEvent::ResponseHead(h) => out.extend_from_slice(&h),
                crate::h2_client::UpstreamEvent::ResponseBody(b) => out.extend_from_slice(&b),
                // Upstream response trailers (gRPC grpc-status): relay
                // to an h2 client via the server shim's trailer path;
                // dropped for h1 clients (h1 trailers need chunked
                // relay, documented v0.2 limitation).
                crate::h2_client::UpstreamEvent::ResponseTrailers(trailers) => {
                    let frames = self
                        .conn(slot)
                        .h2
                        .as_mut()
                        .map_or_else(Vec::new, |h2s| h2s.response_trailers(&trailers));
                    let out = &mut self.conn(slot).h2_out;
                    for f in frames {
                        out.extend_from_slice(&f);
                    }
                }
                crate::h2_client::UpstreamEvent::ResponseComplete => complete = true,
            }
        }
        if !out.is_empty() {
            self.on_upstream_data_h1(io, &out);
        }
        if complete && self.conn(slot).body != BodyFraming::Done {
            self.conn(slot).body = BodyFraming::Done;
            self.check_done(io);
        }
        // Request-side frames (credit retries) still pending?
        let pending = self
            .conn(slot)
            .h2up
            .as_mut()
            .map(|h| h.pending_writes())
            .unwrap_or_default();
        if !pending.is_empty() {
            io.write_upstream(&pending);
        }
    }

    /// Sends upstream response bytes (or error text) to an h2 client.
    #[cfg(feature = "h2")]
    fn h2_write(&mut self, io: &mut SessionIo<'_>, bytes: &[u8]) {
        let slot = io.slot_index();
        let (frames, done) = {
            let Some(h2s) = self.conn(slot).h2.as_mut() else {
                return;
            };
            h2s.response_bytes(bytes)
        };
        let out = &mut self.conn(slot).h2_out;
        for f in frames {
            out.extend_from_slice(&f);
        }
        // Flush the frames BEFORE completing the transaction: check_done
        // may queue a FIN (close_after) or park the session, and bytes
        // queued after the FIN would never reach the client.
        self.h2_flush(io);
        if done {
            self.conn(slot).body = BodyFraming::Done;
            self.check_done(io);
        }
    }

    /// Upstream EOF on an h2 transaction: end the stream (empty DATA
    /// with END_STREAM) unless already done.
    #[cfg(feature = "h2")]
    fn h2_upstream_eof(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let (frames, outcome) = {
            let Some(h2s) = self.conn(slot).h2.as_mut() else {
                return;
            };
            h2s.response_eof()
        };
        let out = &mut self.conn(slot).h2_out;
        for f in frames {
            out.extend_from_slice(&f);
        }
        match outcome {
            crate::h2_server::EofOutcome::Completed => {
                self.conn(slot).body = BodyFraming::Done;
                self.h2_flush(io);
                self.check_done(io);
            }
            crate::h2_server::EofOutcome::Truncated => {
                // Upstream died before its declared Content-Length:
                // nothing honest to send — drop the connection.
                self.log(LogLevel::Warn, "upstream truncated h2 response; closing");
                io.close();
            }
            crate::h2_server::EofOutcome::Continue => {
                // Held body bytes keep flowing on client credit.
                self.h2_flush(io);
            }
        }
    }

    /// Drains engine-queued frames and the response frame buffer to
    /// the client (TLS-wrapped). Closes on engine connection errors.
    #[cfg(feature = "h2")]
    fn h2_flush(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        thread_local! { static FLUSHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }
        FLUSHED.with(|c| {
            let t = c.get() + self.conn(slot).h2_out.len() as u64;
            c.set(t);
            if t / 100000 > (t - self.conn(slot).h2_out.len() as u64) / 100000 {
                eprintln!("WIREDBG ~{t} response bytes flushed");
            }
        });
        #[cfg(feature = "h2")]
        if !self.conn(slot).h2_out.is_empty() {
            let frames = std::mem::take(&mut self.conn(slot).h2_out);
            self.raw_downstream(io, &frames);
        }
        let failed = self
            .conns
            .get(&slot)
            .and_then(|c| c.h2.as_ref())
            .is_some_and(|h| h.failed());
        let engine_out = self
            .conn(slot)
            .h2
            .as_mut()
            .map_or_else(Vec::new, |h2s| h2s.pending_writes());
        let frames = std::mem::take(&mut self.conn(slot).h2_out);
        if !engine_out.is_empty() {
            self.raw_downstream(io, &engine_out);
        }
        if !frames.is_empty() {
            self.raw_downstream(io, &frames);
        }
        if failed {
            // Abort any in-flight transaction; never reuse the session.
            io.close();
        }
    }

    /// Raw TLS-wrapped write to the client (h2 control + data frames).
    #[cfg(feature = "h2")]
    fn raw_downstream(&mut self, io: &mut SessionIo<'_>, bytes: &[u8]) {
        let slot = io.slot_index();
        let Some(conn) = self.conns.get_mut(&slot) else {
            return;
        };
        conn.bytes_out += bytes.len() as u64;
        let Some(tls) = &mut conn.tls else {
            io.respond(bytes);
            return;
        };
        use std::io::Write as _;
        let _ = tls.writer().write_all(bytes);
        let mut out = Vec::with_capacity(16 * 1024);
        loop {
            let mut buf = [0u8; 16 * 1024];
            let n = tls.write_tls(&mut buf.as_mut_slice()).unwrap_or(0);
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            if out.len() > 512 * 1024 {
                io.respond(&out);
                out.clear();
            }
        }
        if !out.is_empty() {
            io.respond(&out);
        }
    }

    fn plain_request_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        // HTTP/2 sessions enter through the h2 translation shim.
        #[cfg(feature = "h2")]
        if self
            .conns
            .get(&io.slot_index())
            .is_some_and(|c| c.h2.is_some())
        {
            self.h2_intake(io, data);
            return;
        }
        self.h1_plain_request_data(io, data);
    }

    /// HTTP/1.1 intake (h2 events call this directly to avoid
    /// re-entering the h2 shim).
    fn h1_plain_request_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        self.metrics
            .bytes_in
            .add(&self.config.registry, data.len() as u64);
        let slot = io.slot_index();

        // Streaming request body: bytes relay upstream as they arrive —
        // never buffered whole. Until the upstream connects they queue
        // handler-side (req_pending) so ordering with the inline bytes
        // forwarded at connect is preserved.
        let (framing, upstream_ready) = self
            .conns
            .get(&slot)
            .map(|c| (c.req_framing, c.upstream_ready))
            .unwrap_or((ReqFraming::AwaitingHead, false));
        let body_active = match framing {
            ReqFraming::ContentLength(remaining) => remaining > 0,
            ReqFraming::Chunked { seen_zero } => !seen_zero,
            _ => false,
        };
        if body_active {
            {
                let conn = self.conn(slot);
                match &mut conn.req_framing {
                    ReqFraming::ContentLength(remaining) => {
                        let n = (data.len() as u64).min(*remaining) as usize;
                        *remaining -= n as u64;
                        if upstream_ready {
                            io.write_upstream(&data[..n]);
                        } else {
                            conn.req_pending.extend_from_slice(&data[..n]);
                            if conn.req_pending.len() > REQ_PENDING_CAP {
                                // Unreasonable pre-connect buffering.
                                self.respond_full(io, Status::PayloadTooLarge, "body too large\n");
                                io.close();
                            }
                            return;
                        }
                    }
                    ReqFraming::Chunked { seen_zero } => {
                        if !*seen_zero && data.windows(5).any(|w| w == b"0\r\n\r\n") {
                            *seen_zero = true;
                        }
                        if upstream_ready {
                            io.write_upstream(data);
                        } else {
                            conn.req_pending.extend_from_slice(data);
                            if conn.req_pending.len() > REQ_PENDING_CAP {
                                self.respond_full(io, Status::PayloadTooLarge, "body too large\n");
                                io.close();
                            }
                        }
                    }
                    _ => {}
                }
                return;
            }
        }

        {
            let conn = self.conn(slot);
            conn.head_buf.extend_from_slice(data);
        }
        let buf = self.conn(slot).head_buf.clone();
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(&buf, &mut storage) {
            Parsed::Partial => {
                // Refresh idle deadline while the client streams the head.
                io.set_deadline(
                    Some(self.deadline(self.config.idle_timeout_ms)),
                    vane_core::handler::DeadlineReason::Idle,
                );
            }
            Parsed::Error(e) => {
                let status = match e {
                    vane_proto::request::ParseError::TooLarge => Status::PayloadTooLarge,
                    vane_proto::request::ParseError::Malformed => Status::BadRequest,
                };
                self.respond_full(io, status, "bad request\n");
                io.close();
            }
            Parsed::Complete(view, head_len) => {
                self.metrics.requests.inc(&self.config.registry);
                self.handle_request(io, &view, head_len);
            }
        }
    }

    fn log(&self, level: LogLevel, msg: &str) {
        let ev = LogEvent::now(
            u16::try_from(self.worker_id).unwrap_or(u16::MAX),
            level,
            msg.as_bytes(),
        );
        let _dropped = self.config.events.try_push(ev); // never blocks
    }

    fn conn(&mut self, slot: u32) -> &mut Conn {
        self.conns.entry(slot).or_default()
    }

    /// Lazily registers and returns a cluster's metric handles. First sight
    /// of a cluster takes the registry's control-plane lock once; later
    /// requests are lock-free handle lookups.
    fn cluster_metric(&mut self, cluster: &str) -> ClusterMetric {
        if let Some(m) = self.cluster_metrics.get(cluster) {
            return m.clone();
        }
        let m = ClusterMetric::register(&self.config.registry, cluster);
        self.cluster_metrics.insert(cluster.to_owned(), m.clone());
        m
    }

    fn respond_full(&mut self, io: &mut SessionIo<'_>, status: Status, body: &str) {
        let slot = io.slot_index();
        self.conn(slot).resp_status = status.code();
        if let Some(span) = self.conns.get_mut(&slot).and_then(|c| c.span.take()) {
            span.record("http.status_code", status.code());
            span.record("http.response_size", body.len() as u64);
            drop(span);
        }
        let started = self.conns.get(&slot).and_then(|c| c.started);
        let mut buf = [0u8; 1024];
        if let Ok(n) = write_full(&mut buf, status, body.as_bytes(), &[], &self.date) {
            self.write_downstream(io, &buf[..n]);
            self.metrics.responses.inc(&self.config.registry);
        }
        self.access_emit(io, started);
        io.set_deadline(
            Some(self.deadline(self.config.idle_timeout_ms)),
            vane_core::handler::DeadlineReason::Idle,
        );
    }

    /// Emits the transaction's access record (once). No-op when the
    /// access log is disabled or the record was already written.
    fn access_emit(&mut self, io: &SessionIo<'_>, started: Option<Instant>) {
        let Some(access) = self.config.access.as_ref() else {
            return;
        };
        let slot = io.slot_index();
        let Some(conn) = self.conns.get(&slot) else {
            return;
        };
        if conn.access_logged {
            return;
        }
        let duration_us = started
            .map(|s| s.elapsed().as_micros())
            .unwrap_or(0)
            .min(u128::from(u32::MAX)) as u32;
        let client = io
            .peer()
            .map(|a| (ip16(a.ip()), a.port()))
            .unwrap_or(([0u8; 16], 0));
        let upstream = conn.upstream_addr.map(|a| (ip16(a.ip()), a.port()));
        let mut trace_hex = [0u8; 32];
        let mut trace_len = 0;
        if let Some(t) = &conn.trace {
            for (i, b) in t.trace_id.iter().enumerate() {
                trace_hex[i * 2] = HEX[usize::from(b >> 4)];
                trace_hex[i * 2 + 1] = HEX[usize::from(b & 0x0F)];
            }
            trace_len = t.trace_id.len() * 2;
        }
        let rec = vane_observe::access::AccessRecord::now(
            u16::try_from(self.worker_id).unwrap_or(u16::MAX),
            conn.resp_status,
            duration_us,
            conn.bytes_out,
            client,
            upstream,
            conn.req_method.as_bytes(),
            conn.req_host.as_deref().unwrap_or_default().as_bytes(),
            conn.req_path.as_bytes(),
            &trace_hex[..trace_len],
        );
        access.emit(rec);
        if let Some(c) = self.conns.get_mut(&slot) {
            c.access_logged = true;
        }
    }

    /// Serializes the upstream request head from the parsed view + pipeline
    /// mutations.
    #[allow(clippy::too_many_lines)]
    fn build_upstream_head(
        &mut self,
        view: &RequestView<'_>,
        route: &RouteEntry,
        path: &str,
        inject: &[(String, String)],
        close: bool,
        inline_body_len: usize,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(512);
        out.extend_from_slice(view.method.as_bytes());
        out.push(b' ');
        out.extend_from_slice(path.as_bytes());
        out.extend_from_slice(b" HTTP/1.1\r\n");
        // Host: keep original unless absent.
        let has_host = view.header("host").is_some();
        if !has_host {
            out.extend_from_slice(b"Host: ");
            out.extend_from_slice(route.host.as_deref().unwrap_or("vane").as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        for h in view.headers {
            let name = h.name;
            let lname = name.to_ascii_lowercase();
            if lname == "connection" || lname == "keep-alive" || lname == "proxy-connection" {
                continue; // hop-by-hop; we manage it
            }
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(h.value);
            out.extend_from_slice(b"\r\n");
        }
        for (name, value) in inject {
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        if view.content_length().is_none() && !view.is_chunked() && inline_body_len > 0 {
            // Head arrived without length but body bytes did (rare): add CL.
            out.extend_from_slice(format!("Content-Length: {inline_body_len}\r\n").as_bytes());
        }
        if close {
            out.extend_from_slice(b"Connection: close\r\n");
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    /// Parses the upstream response head and decides body framing.
    fn parse_upstream_head(&mut self, slot: u32, data: &[u8]) -> (usize, Status, bool) {
        let mut storage = [httparse::EMPTY_HEADER; vane_proto::response::MAX_RESPONSE_HEADERS];
        match vane_proto::response::parse_upstream_head(data, &mut storage) {
            Ok(Some(head)) => {
                let head_len = head.head_len;
                let code = head.code;
                let status = Status::from_code(code);
                self.conn(slot).resp_status = code;
                let content_length = head.content_length;
                let chunked = head.chunked;
                let upstream_close = head.close;
                let framing = if code == 101 {
                    // WebSocket / protocol switch: everything after the
                    // head is a raw bidirectional pump until either side
                    // closes.
                    self.conn(slot).tunnel = true;
                    BodyFraming::Tunnel
                } else if chunked {
                    BodyFraming::Chunked {
                        last_was_lf: false,
                        seen_zero: false,
                    }
                } else {
                    match content_length {
                        Some(0) | None => BodyFraming::Done,
                        Some(_) => BodyFraming::ContentLength,
                    }
                };
                let remaining = content_length.unwrap_or(0);
                let conn = self.conn(slot);
                conn.body = framing;
                conn.remaining = remaining;
                conn.close_after |= upstream_close;
                (head_len, status, upstream_close)
            }
            _ => (0, Status::BadGateway, true),
        }
    }
}

/// Handler-side cap for body bytes arriving before the upstream
/// connects. The window is one dial; anything beyond this is abusive.
const REQ_PENDING_CAP: usize = 1024 * 1024;

/// Sets the request-body framing from the parsed view.
fn conn_set_framing(conns: &mut HashMap<u32, Conn>, slot: u32, view: &RequestView<'_>) {
    if let Some(c) = conns.get_mut(&slot) {
        c.req_framing = if view.is_chunked() {
            ReqFraming::Chunked { seen_zero: false }
        } else if let Some(Some(n)) = view.content_length() {
            if n > 0 {
                ReqFraming::ContentLength(n)
            } else {
                ReqFraming::Done
            }
        } else {
            ReqFraming::Done
        };
    }
}

impl Handler for HttpProxy {
    fn on_connected(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        // Fresh per-session state: worker slots are reused across
        // connections and must not inherit the previous session's
        // buffered bytes, route, or timers (stale head_buf replays the
        // old request into the new session's parse).
        self.conns.insert(slot, Conn::default());
        if self.config.l4_splice {
            // L4: route by catch-all (no SNI parsing), dial, and splice
            // once the upstream is live. Early client bytes are written
            // upstream; the worker buffers them until the dial lands.
            let table = self.config.router.load();
            let matched = table.table().lookup(None, "/");
            let Some(m) = matched else {
                io.close();
                return;
            };
            let route = Arc::clone(&m.terminal.value);
            drop(table);
            let mut balancer = route.balancer(0);
            let Some(addr) = balancer.pick_addr() else {
                io.close();
                return;
            };
            {
                let conn = self.conn(slot);
                conn.l4 = true;
                conn.route = Some(route);
                conn.upstream_addr = Some(addr);
                conn.attempts = 0;
                conn.upstream_ready = false;
            }
            if !io.connect_upstream(addr) {
                self.metrics.upstream_errors.inc(&self.config.registry);
                io.close();
                return;
            }
            io.set_deadline(
                Some(self.deadline(self.config.connect_timeout_ms)),
                vane_core::handler::DeadlineReason::Connect,
            );
            return;
        }
        // h2c: plain listeners speak prior-knowledge HTTP/2 directly.
        #[cfg(feature = "h2")]
        if self.config.h2c && self.config.tls.is_none() {
            let mut h2s = Box::new(crate::h2_server::H2Server::new(slot));
            let out = h2s.pending_writes();
            self.conn(slot).h2 = Some(h2s);
            if !out.is_empty() {
                self.raw_downstream(io, &out);
            }
        }
        if let Some(tls_slot) = &self.config.tls {
            // Hot-reload point: read the current generation through the
            // shared slot (uncontended read lock, once per connection).
            let tls_cfg = tls_slot.read().expect("tls slot").clone();
            match rustls::ServerConnection::new(tls_cfg) {
                Ok(conn) => {
                    #[cfg(feature = "h2")]
                    {
                        let alpn_h2 = conn.alpn_protocol() == Some(b"h2");
                        let c = self.conn(slot);
                        c.tls = Some(conn);
                        if alpn_h2 {
                            c.h2 = Some(Box::new(crate::h2_server::H2Server::new(slot)));
                        }
                    }
                    #[cfg(not(feature = "h2"))]
                    {
                        self.conn(slot).tls = Some(conn);
                    }
                }
                Err(e) => {
                    self.log(LogLevel::Error, &format!("tls setup: {e}"));
                    io.close();
                    return;
                }
            }
        }
        io.set_deadline(
            Some(self.deadline(self.config.idle_timeout_ms)),
            vane_core::handler::DeadlineReason::Idle,
        );
    }

    fn on_downstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        // L4 splice: raw bytes toward the upstream (pre-splice only —
        // once spliced the kernel pumps without the handler).
        if self.conns.get(&slot).is_some_and(|c| c.l4) {
            io.write_upstream(data);
            return;
        }
        // WebSocket tunnel: raw bidirectional relay, no parsing.
        if self.conns.get(&slot).is_some_and(|c| c.tunnel) {
            self.metrics
                .bytes_in
                .add(&self.config.registry, data.len() as u64);
            io.write_upstream(data);
            return;
        }
        // Every downstream byte on a TLS listener is ciphertext: route
        // through the TLS pump (which handles handshake and app data).
        let tls = self.conns.get(&slot).and_then(|c| c.tls.as_ref()).is_some();
        if tls {
            self.tls_read(io, data);
        } else {
            self.plain_request_data(io, data);
        }
    }

    fn on_downstream_eof(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        if let Some(tls) = self.conns.get_mut(&slot).and_then(|c| c.tls.as_mut()) {
            tls.send_close_notify();
            self.flush_tls(io);
        }
        // Client half-close: forward to upstream (end of request body).
        io.upstream_eof_write();
    }

    fn on_upstream_connected(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        if self.conns.get(&slot).is_some_and(|c| c.l4) {
            // Kernel zero-copy pump takes over; the handler is done.
            if io.start_splice() {
                return;
            }
            io.close();
            return;
        }
        self.conn(slot).upstream_ready = true;
        let (route, upstream_path, inject) = {
            let conn = self.conn(slot);
            (
                conn.route.clone().expect("route"),
                conn.upstream_path.clone().expect("path"),
                std::mem::take(&mut conn.inject),
            )
        };
        // HTTP/2 upstream: speak h2 to the backend (prior knowledge),
        // translating the buffered h1 request head.
        #[cfg(feature = "h2")]
        if route.upstream_h2 {
            let buf = self.conn(slot).head_buf.clone();
            let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
            if let Parsed::Complete(view, head_len) = RequestView::parse_in(&buf, &mut storage) {
                if view.is_chunked() {
                    // h2 has no chunked encoding; v0.2 declines.
                    self.respond_full(
                        io,
                        Status::BadGateway,
                        "chunked to h2 upstream unsupported\n",
                    );
                    io.close();
                    return;
                }
                let mut h2up = Box::new(crate::h2_client::H2Upstream::new());
                let remaining = view.content_length().flatten().filter(|n| *n > 0);
                h2up.send_request(&buf, remaining);
                io.write_upstream(&h2up.pending_writes());
                io.mark_request_sent();
                let body = &buf[head_len..];
                {
                    let conn = self.conn(slot);
                    if let ReqFraming::ContentLength(remaining) = &mut conn.req_framing {
                        *remaining = remaining.saturating_sub(body.len() as u64);
                    }
                }
                if !body.is_empty() {
                    let frames = h2up.request_body(body);
                    if !frames.is_empty() {
                        io.write_upstream(&frames);
                    }
                }
                // Pre-connect queue behind the inline bytes.
                let pending = std::mem::take(&mut self.conn(slot).req_pending);
                if !pending.is_empty() {
                    let frames = h2up.request_body(&pending);
                    if !frames.is_empty() {
                        io.write_upstream(&frames);
                    }
                }
                let out = h2up.pending_writes();
                self.conn(slot).h2up = Some(h2up);
                if !out.is_empty() {
                    io.write_upstream(&out);
                }
                self.conn(slot).head_buf.drain(..head_len + body.len());
                io.set_deadline(
                    Some(self.deadline(self.config.first_byte_timeout_ms)),
                    vane_core::handler::DeadlineReason::FirstByte,
                );
            } else {
                self.respond_full(io, Status::BadGateway, "bad request\n");
                io.close();
            }
            return;
        }
        // Re-parse the buffered head for serialization (view borrows our
        // buffer; the handler call is synchronous so this is safe).
        let buf = self.conn(slot).head_buf.clone();
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        if let Parsed::Complete(view, head_len) = RequestView::parse_in(&buf, &mut storage) {
            let head = self.build_upstream_head(&view, &route, &upstream_path, &inject, false, 0);
            io.write_upstream(&head);
            io.mark_request_sent();
            // Forward any body bytes that arrived with the head, then
            // the pre-connect queue (ordering preserved).
            let body = &buf[head_len..];
            {
                let conn = self.conn(slot);
                if let ReqFraming::ContentLength(remaining) = &mut conn.req_framing {
                    *remaining = remaining.saturating_sub(body.len() as u64);
                }
                if let ReqFraming::Chunked { seen_zero } = &mut conn.req_framing {
                    if !*seen_zero && body.windows(5).any(|w| w == b"0\r\n\r\n") {
                        *seen_zero = true;
                    }
                }
            }
            if !body.is_empty() {
                io.write_upstream(body);
            }
            // Pre-connect queue behind the inline bytes.
            let pending = std::mem::take(&mut self.conn(slot).req_pending);
            if !pending.is_empty() {
                io.write_upstream(&pending);
            }
            // Trim the forwarded head+inline body: the buffer must not
            // re-parse old bytes on the next keep-alive transaction.
            self.conn(slot).head_buf.drain(..head_len + body.len());
            // First-byte deadline.
            io.set_deadline(
                Some(self.deadline(self.config.first_byte_timeout_ms)),
                vane_core::handler::DeadlineReason::FirstByte,
            );
        } else {
            self.respond_full(io, Status::BadGateway, "bad request\n");
            io.close();
        }
    }

    fn on_upstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        #[cfg(feature = "h2")]
        {
            let slot = io.slot_index();
            if self.conns.get(&slot).is_some_and(|c| c.h2up.is_some()) {
                self.h2up_intake(io, data);
                return;
            }
        }
        self.on_upstream_data_h1(io, data);
    }

    fn on_upstream_eof(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        #[cfg(feature = "h2")]
        if self.conns.get(&slot).is_some_and(|c| c.h2up.is_some()) {
            // h2 upstream: completion arrives via END_STREAM/CL; an EOF
            // before completion is a truncation.
            let complete = self
                .conn(slot)
                .h2up
                .as_ref()
                .is_some_and(|h| h.response_complete());
            if !complete {
                self.log(LogLevel::Warn, "h2 upstream closed mid-response; closing");
                io.close();
            }
            return;
        }
        #[cfg(feature = "h2")]
        let is_h2 = self.conns.get(&slot).is_some_and(|c| c.h2.is_some());
        #[cfg(not(feature = "h2"))]
        let is_h2 = false;
        // h2 sessions own their EOF semantics (the shim knows whether
        // body bytes are still mid-flight in its held queue): the h1
        // framing arms below would misread a CL body as truncated while
        // held bytes await client credit.
        #[cfg(feature = "h2")]
        if is_h2 {
            self.h2_upstream_eof(io);
            return;
        }
        let (framing, remaining, request_sent, ready) = {
            let conn = self.conns.get(&slot).expect("conn exists");
            (
                conn.body,
                conn.remaining,
                io.request_sent_upstream(),
                conn.upstream_ready,
            )
        };
        if ready && !request_sent {
            // Pooled connection died while idle (or before the head went
            // out): the request was never sent — safe to retry.
            self.log(LogLevel::Debug, "upstream closed pre-request; retrying");
            self.failover(io);
            return;
        }
        match framing {
            BodyFraming::AwaitingHead => {
                // Died before producing a response head, request already sent.
                self.upstream_failed(io);
            }
            BodyFraming::ContentLength if remaining > 0 => {
                self.log(LogLevel::Warn, "upstream truncated body; closing");
                io.close();
            }
            _ => {
                #[cfg(not(feature = "h2"))]
                let _ = is_h2;
                // Flush the gzip trailer + terminal chunk before the FIN.
                self.finish_gzip(io);
                io.downstream_eof_write();
                let done = {
                    let conn = self.conn(slot);
                    conn.body = BodyFraming::Done;
                    conn.close_after
                };
                if done {
                    io.close();
                }
            }
        }
    }

    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, err: io::Error) {
        self.metrics.upstream_errors.inc(&self.config.registry);
        let slot = io.slot_index();
        if let Some(cluster) = self
            .conns
            .get(&slot)
            .and_then(|c| c.route.as_ref())
            .map(|r| r.cluster.clone())
        {
            self.breaker.record_failure(&cluster);
            self.cluster_metric(&cluster)
                .errors
                .inc(&self.config.registry);
        }
        if let (Some(outlier), Some(addr)) = (
            self.conns
                .get(&slot)
                .and_then(|c| c.route.as_ref())
                .and_then(|r| r.outlier.clone()),
            self.conns.get(&slot).and_then(|c| c.upstream_addr),
        ) {
            outlier.record_failure(addr);
        }
        self.log(LogLevel::Warn, &format!("upstream error: {err}"));
        self.upstream_failed(io);
    }

    fn on_downstream_error(&mut self, io: &mut SessionIo<'_>, err: io::Error) {
        let _ = err;
        io.close();
    }

    fn on_shutdown_hint(&mut self, io: &mut SessionIo<'_>) {
        // Drain: finish the current transaction, then close. The idle
        // deadline still applies; keep-alive ends after this response.
        let slot = io.slot_index();
        self.conn(slot).close_after = true;
    }

    fn on_deadline(&mut self, io: &mut SessionIo<'_>, reason: vane_core::handler::DeadlineReason) {
        use vane_core::handler::DeadlineReason;
        match reason {
            DeadlineReason::Connect
                if !self
                    .conns
                    .get(&io.slot_index())
                    .is_some_and(|c| c.upstream_ready) =>
            {
                self.log(LogLevel::Warn, "upstream connect timeout");
                if let Some(route) = &self.conns.get(&io.slot_index()).expect("conn exists").route {
                    self.breaker.record_failure(&route.cluster);
                }
                self.metrics.upstream_errors.inc(&self.config.registry);
                self.upstream_failed(io);
            }
            DeadlineReason::FirstByte => {
                self.log(LogLevel::Warn, "upstream first-byte timeout");
                if let Some(route) = &self.conns.get(&io.slot_index()).expect("conn exists").route {
                    self.breaker.record_failure(&route.cluster);
                }
                self.respond_full(io, Status::GatewayTimeout, "upstream timeout\n");
                io.close();
            }
            _ => io.close(),
        }
    }
}

impl HttpProxy {
    #[allow(clippy::too_many_lines)]
    fn handle_request(&mut self, io: &mut SessionIo<'_>, view: &RequestView<'_>, _head_len: usize) {
        let started = Instant::now();
        let slot = io.slot_index();
        {
            let conn = self.conn(slot);
            conn.req_method.clear();
            conn.req_method.push_str(view.method);
            conn.req_host = view
                .header("host")
                .and_then(|h| std::str::from_utf8(h).ok())
                .map(|h| h.split(':').next().unwrap_or(h).to_owned());
            conn.req_path = view.path.to_owned();
            conn.resp_status = 0;
            conn.bytes_out = 0;
            conn.access_logged = false;
        }

        // Process-wide rate limiting (cross-worker aggregate).
        if let Some(gcra) = &self.config.shared_rate_limit {
            if !gcra.allow() {
                self.respond_full(io, Status::TooManyRequests, "rate limited\n");
                io.close();
                return;
            }
        }

        // JWT bearer authentication: before routing; health probes stay
        // open (the admin plane has its own listener).
        if let Some(jwt) = &self.config.jwt {
            let authz = view
                .header("authorization")
                .and_then(|h| std::str::from_utf8(h).ok());
            jwt.maybe_reload();
            match authz.map(|a| jwt.verify(a)) {
                Some(Ok(_claims)) => {}
                _ => {
                    self.respond_full(io, Status::Unauthorized, "unauthorized\n");
                    io.close();
                    return;
                }
            }
        }

        // ACME HTTP-01 challenges bypass routing entirely (RFC 8555 §8.3):
        // answered from the shared token map before any route lookup.
        if self.config.http01_tokens.is_some()
            && view.path.starts_with("/.well-known/acme-challenge/")
        {
            let token = &view.path["/.well-known/acme-challenge/".len()..];
            let answer = self
                .config
                .http01_tokens
                .as_ref()
                .expect("checked")
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(token)
                .cloned();
            match answer {
                Some(key_auth) => {
                    self.respond_full(io, Status::Ok, &key_auth);
                }
                None => {
                    self.respond_full(io, Status::NotFound, "unknown token\n");
                }
            }
            io.close();
            return;
        }

        let host = view
            .header("host")
            .and_then(|h| std::str::from_utf8(h).ok());
        let host_only = host.map(|h| h.split(':').next().unwrap_or(h));
        // Route lookup on the live snapshot.
        let table = self.config.router.load();
        let matched = table.table().lookup(host_only, view.path).map(|m| {
            let route = Arc::clone(&m.terminal.value);
            (route, m.wildcard)
        });

        let Some((route, _wild)) = matched else {
            self.respond_full(io, Status::NotFound, "no route\n");
            io.close();
            return;
        };

        // Method check.
        if !route.methods.is_empty()
            && !route
                .methods
                .iter()
                .any(|m| m.eq_ignore_ascii_case(view.method))
        {
            self.respond_full(io, Status::MethodNotAllowed, "method not allowed\n");
            return;
        }

        // Request span (after the error exits so 404/405 stay spanless
        // and cheap). Closed with status at response completion.
        if let Some(conn) = self.conns.get_mut(&slot) {
            let tp = view
                .header("traceparent")
                .and_then(|h| std::str::from_utf8(h).ok());
            conn.span = Some(crate::tracing_util::serve_span(
                view.method,
                view.path,
                view.header("host")
                    .and_then(|h| std::str::from_utf8(h).ok()),
                tp,
            ));
        }

        // Request-body framing (set BEFORE dialing so late client bytes
        // take the streaming relay path instead of re-entering here).
        conn_set_framing(&mut self.conns, slot, view);

        // Path rewrite.
        let upstream_path = route
            .strip_prefix
            .as_ref()
            .and_then(|p| {
                view.path.strip_prefix(p.as_str()).map(|rest| {
                    if rest.starts_with('/') {
                        rest.to_owned()
                    } else {
                        format!("/{rest}")
                    }
                })
            })
            .unwrap_or_else(|| view.path.to_owned());

        // Filter pipeline (scoped: ctx borrows `path_mut`).
        let mut path_mut = upstream_path.clone();
        let peer = io
            .peer()
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
        let (inject, short_circuit, trace) = {
            let mut ctx = RequestCtx::new(view.method, &mut path_mut, peer, host_only);
            ctx.cluster = Some(&route.cluster);
            ctx.inject("X-Forwarded-For", &peer.ip().to_string());
            let outcome = self.pipeline.run(&mut ctx);
            let rejected = outcome != Outcome::Continue
                || matches!(self.breaker.run(&mut ctx), Outcome::Reject(503, _));
            let sc = if rejected {
                ctx.short_circuit.or(Some((503, "upstream unavailable")))
            } else {
                None
            };
            (ctx.inject_headers, sc, ctx.trace)
        };
        if let Some((code, reason)) = short_circuit {
            self.respond_full(io, Status::from_code(code), reason);
            return;
        }

        // Wasm plugins (feature `wasm`): request-phase hooks run after
        // the native pipeline; the first Reject short-circuits with the
        // guest's status. Guest errors are logged and skipped.
        #[cfg(feature = "wasm")]
        if !self.plugins.is_empty() {
            let headers: Vec<(String, String)> = view
                .headers
                .iter()
                .filter_map(|h| {
                    let value = std::str::from_utf8(h.value).ok()?;
                    Some((h.name.to_owned(), value.to_owned()))
                })
                .collect();
            let mut rejected = None;
            for plugin in &mut self.plugins {
                match plugin.on_request_with_headers(&upstream_path, &headers) {
                    Ok(vane_plugins::GuestVerdict::Continue) => {}
                    Ok(vane_plugins::GuestVerdict::Reject(code)) => {
                        rejected = Some(code);
                        break;
                    }
                    Err(e) => {
                        eprintln!("[vane:warn] plugin on_request: {e}");
                    }
                }
            }
            if let Some(code) = rejected {
                self.respond_full(io, Status::from_code(code), "rejected by plugin\n");
                return;
            }
        }

        // Pick a backend (skipping outliers when detection is enabled).
        let mut balancer = route.balancer(u64::from(slot) ^ started.elapsed().as_nanos() as u64);
        let mut addr = balancer.pick_addr();
        if let (Some(outlier), Some(picked)) = (&route.outlier, addr) {
            if outlier.is_ejected(picked) {
                // Prefer a live backend; the ejected one is the fallback
                // of last resort (mirrors pick_addr_except semantics).
                addr = balancer.pick_addr_except(Some(picked));
                if addr.is_none() {
                    addr = Some(picked);
                }
            }
        }
        let Some(addr) = addr else {
            self.respond_full(io, Status::ServiceUnavailable, "no healthy upstream\n");
            return;
        };
        // Per-cluster request counter.
        let cluster_requests = self.cluster_metric(&route.cluster).requests;
        cluster_requests.inc(&self.config.registry);

        // Compression decision: route opt-in + client Accept-Encoding +
        // h1 downstream only (the h2 shim frame path is excluded).
        let accepts_gzip = {
            let ae = view.header("accept-encoding");
            vane_proto::compression::accepts_gzip(ae)
        };
        #[cfg(feature = "h2")]
        let is_h2_conn = self.conns.get(&slot).is_some_and(|c| c.h2.is_some());
        #[cfg(not(feature = "h2"))]
        let is_h2_conn = false;
        let want_gzip = route.compression && accepts_gzip && !is_h2_conn;

        // Stash transaction state.
        {
            let conn = self.conn(slot);
            conn.route = Some(Arc::clone(&route));
            conn.upstream_path = Some(path_mut);
            conn.inject = inject;
            conn.started = Some(started);
            conn.close_after = view.wants_close();
            conn.trace = Some(trace);
            conn.body = BodyFraming::AwaitingHead;
            conn.gzip = want_gzip.then(|| GzipRelay {
                stream: vane_proto::compression::GzipStream::new(),
            });
        }

        if self.breaker.cluster_breaker(&route.cluster).is_open() {
            self.respond_full(io, Status::ServiceUnavailable, "circuit open\n");
            return;
        }

        {
            let conn = self.conn(slot);
            conn.upstream_addr = Some(addr);
            conn.attempts = 0;
            conn.upstream_ready = false;
        }
        // Checkout a pooled keep-alive connection, else dial fresh.
        if self.config.pool_per_backend > 0 {
            let pooled = self.pool.get_mut(&addr).and_then(Vec::pop);
            if let Some(fd) = pooled {
                if io.attach_upstream(fd) {
                    // Pooled conn is already connected: drive the head now.
                    self.on_upstream_connected(io);
                    return;
                }
                // Stale pooled fd: discard, fall through to a fresh dial.
                self.close_fd(fd);
            }
        }
        if !io.connect_upstream(addr) {
            self.metrics.upstream_errors.inc(&self.config.registry);
            self.breaker.record_failure(&route.cluster);
            if let Some(outlier) = &route.outlier {
                outlier.record_failure(addr);
            }
            self.upstream_failed(io);
            return;
        }
        io.set_deadline(
            Some(self.deadline(self.config.connect_timeout_ms)),
            vane_core::handler::DeadlineReason::Connect,
        );
    }

    fn relay_body(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        // Compute the relay decision first (avoids overlapping borrows).
        let (take, framing_done) = {
            let conn = self.conn(slot);
            match conn.body {
                BodyFraming::ContentLength => {
                    let take = (data.len() as u64).min(conn.remaining) as usize;
                    conn.remaining -= take as u64;
                    let done = conn.remaining == 0;
                    (Some(take), done)
                }
                BodyFraming::Chunked { seen_zero, .. } => {
                    let done = !seen_zero && data.windows(5).any(|w| w == b"0\r\n\r\n");
                    if done {
                        if let BodyFraming::Chunked { seen_zero, .. } = &mut conn.body {
                            *seen_zero = true;
                        }
                    }
                    (Some(data.len()), done)
                }
                _ => (None, false),
            }
        };
        if let Some(n) = take {
            let bytes = &data[..n];
            // Gzip relay: compress the accepted bytes, frame as chunks.
            let compressing = self.conns.get(&slot).is_some_and(|c| c.gzip.is_some());
            let out = if compressing {
                let mut gz = self.conn(slot).gzip.take();
                if let Some(relay) = gz.as_mut() {
                    let compressed = relay.stream.feed(bytes).unwrap_or_else(|_| bytes.to_vec());
                    self.conn(slot).gzip = gz;
                    let mut out = Vec::with_capacity(compressed.len() + 18);
                    out.extend_from_slice(&vane_proto::compression::chunk(&compressed));
                    out
                } else {
                    bytes.to_vec()
                }
            } else {
                bytes.to_vec()
            };
            self.write_downstream(io, &out);
        }
        if framing_done {
            self.finish_gzip(io);
            self.conn(slot).body = BodyFraming::Done;
        }
    }

    /// Flushes the gzip trailer + terminal chunk when compressing.
    fn finish_gzip(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let Some(relay) = self.conn(slot).gzip.take() else {
            return;
        };
        let tail = relay.stream.finish().unwrap_or_default();
        let mut out = Vec::with_capacity(tail.len() + 18);
        out.extend_from_slice(&vane_proto::compression::chunk(&tail));
        out.extend_from_slice(&vane_proto::compression::final_chunk());
        self.write_downstream(io, &out);
    }

    fn check_done(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let (done, close_after, started) = {
            let conn = self.conns.get(&slot).expect("conn");
            (
                conn.body == BodyFraming::Done,
                conn.close_after,
                conn.started,
            )
        };
        if done {
            // Take (not clone) the span: ending it promptly lets the
            // batch exporter export it. Holding it in Conn until session
            // teardown would race the exporter shutdown at process exit.
            if let Some(span) = self.conns.get_mut(&slot).and_then(|c| c.span.take()) {
                let (status, bytes) = self
                    .conns
                    .get(&slot)
                    .map(|c| (c.resp_status, c.bytes_out))
                    .unwrap_or((0, 0));
                span.record("http.status_code", status);
                span.record("http.response_size", bytes);
                drop(span);
            }
            self.metrics.latency.observe_us(
                &self.config.registry,
                u64::try_from(started.map(|s| s.elapsed().as_micros()).unwrap_or(0))
                    .unwrap_or(u64::MAX),
            );
            self.access_emit(io, started);
            let conn = self.conns.get(&slot).expect("conn");
            if let Some(route) = &conn.route {
                self.breaker.record_success(&route.cluster);
                if let (Some(outlier), Some(addr)) = (&route.outlier, conn.upstream_addr) {
                    if conn.resp_status >= 500 {
                        outlier.record_failure(addr);
                    } else {
                        outlier.record_success(addr);
                    }
                }
            }
            if close_after {
                // FIN after the queued response bytes flush (deferred).
                io.downstream_eof_write();
            } else {
                // Keep-alive: park the upstream conn, reset the transaction.
                self.park_upstream(io);
                let conn = self.conn(slot);
                conn.head_buf.clear();
                conn.route = None;
                conn.upstream_path = None;
                conn.body = BodyFraming::AwaitingHead;
                conn.remaining = 0;
                conn.close_after = false;
                conn.attempts = 0;
                conn.upstream_ready = false;
                conn.req_method.clear();
                conn.req_host = None;
                conn.req_path.clear();
                conn.resp_status = 0;
                conn.bytes_out = 0;
                conn.access_logged = false;
                conn.req_framing = ReqFraming::AwaitingHead;
                conn.req_pending.clear();
                conn.gzip = None;
                #[cfg(feature = "h2")]
                {
                    // h2 upstream connections cannot sit in the h1 pool.
                    conn.h2up = None;
                }
                conn.span = None;
                io.set_deadline(
                    Some(self.deadline(self.config.idle_timeout_ms)),
                    vane_core::handler::DeadlineReason::Idle,
                );
            }
        }
    }
    /// Upstream unusable: fail over to another backend while the request
    /// is still unsent, otherwise answer 502/504.
    fn upstream_failed(&mut self, io: &mut SessionIo<'_>) {
        eprintln!("UFDBG upstream_failed sent={}", io.request_sent_upstream());
        if io.request_sent_upstream() {
            if self
                .conns
                .get(&io.slot_index())
                .is_some_and(|c| c.body == BodyFraming::AwaitingHead)
            {
                self.respond_full(io, Status::BadGateway, "upstream unreachable\n");
            }
            io.close();
            return;
        }
        self.failover(io);
    }

    /// Re-dials another backend for the buffered request.
    fn failover(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let attempts = self.conn(slot).attempts;
        if attempts >= 2 {
            self.respond_full(io, Status::BadGateway, "upstream unreachable\n");
            io.close();
            return;
        }
        let Some(route) = self.conn(slot).route.clone() else {
            self.respond_full(io, Status::BadGateway, "no route\n");
            io.close();
            return;
        };
        // Never re-dial the backend that just failed: mask it for this
        // pick so failover deterministically moves to a live backend.
        let failed = self.conns.get(&slot).and_then(|c| c.upstream_addr);
        let mut balancer = route.balancer(u64::from(slot) ^ u64::from(attempts + 1));

        let Some(addr) = balancer.pick_addr_except(failed) else {
            self.respond_full(io, Status::ServiceUnavailable, "no healthy upstream\n");
            io.close();
            return;
        };

        {
            let conn = self.conn(slot);
            conn.attempts += 1;
            conn.upstream_ready = false;
            conn.upstream_addr = Some(addr);
            conn.body = BodyFraming::AwaitingHead;
            conn.remaining = 0;
        }
        io.discard_upstream(); // the dead pooled fd must not linger
        if !io.connect_upstream(addr) {
            self.metrics.upstream_errors.inc(&self.config.registry);
            self.respond_full(io, Status::BadGateway, "connect failed\n");
            io.close();
            return;
        }
        io.set_deadline(
            Some(self.deadline(self.config.connect_timeout_ms)),
            vane_core::handler::DeadlineReason::Connect,
        );
    }

    /// Returns an idle keep-alive connection to the pool (or closes it).
    fn park_upstream(&mut self, io: &mut SessionIo<'_>) {
        if self.config.pool_per_backend == 0 {
            // Pooling disabled: close the upstream, keep the downstream
            // session alive for the next keep-alive request.
            if let Some(fd) = io.detach_upstream() {
                self.close_fd(fd);
            }
            return;
        }
        let slot = io.slot_index();
        let Some(addr) = self.conn(slot).upstream_addr else {
            return;
        };
        let Some(fd) = io.detach_upstream() else {
            return;
        };
        let cap = self.config.pool_per_backend;
        let idle = self.pool.entry(addr).or_default();
        if idle.len() < cap {
            idle.push(fd);
        } else {
            self.close_fd(fd);
        }
    }

    fn close_fd(&mut self, fd: RawFd) {
        // SAFETY: single ownership of a detached descriptor.
        unsafe { libc::close(fd) };
    }
}

/// Flattens the live route table into handover records.
#[must_use]
pub fn flatten_routes(router: &Router) -> Vec<RouteRecord> {
    let table = router.load();
    table
        .table()
        .snapshot_records()
        .into_iter()
        .map(|r| RouteRecord {
            host: r.host,
            pattern: r.pattern,
            cluster: r.cluster,
            backends: r.backends,
            strip_prefix: r.strip_prefix,
        })
        .collect()
}
