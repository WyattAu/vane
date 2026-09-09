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
    /// TLS termination config (`None` = plaintext listener).
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Wasm plugin module paths (feature `wasm`; applied to every request
    /// in order, before routing).
    pub plugins: Vec<String>,
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
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum BodyFraming {
    #[default]
    AwaitingHead,
    /// Pass raw bytes until `remaining` bytes forwarded.
    ContentLength,
    /// Pass chunked bytes until terminal 0-chunk observed (len tracking).
    Chunked { last_was_lf: bool, seen_zero: bool },
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
        let (rps, burst) = config
            .rate_limit_rps
            .map_or((1_000_000, 1_000_000), |r| (r, r));
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
        let Some(conn) = self.conns.get_mut(&slot) else {
            return;
        };
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
        let _ = tls.read_tls(&mut io::Cursor::new(data));
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
        // Emit any TLS flight (ServerHello..Finished) before app data.
        self.flush_tls(io);
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
    fn plain_request_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        self.metrics
            .bytes_in
            .add(&self.config.registry, data.len() as u64);
        let slot = io.slot_index();
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

    fn respond_full(&mut self, io: &mut SessionIo<'_>, status: Status, body: &str) {
        let _ = io.slot_index();
        let mut buf = [0u8; 1024];
        if let Ok(n) = write_full(&mut buf, status, body.as_bytes(), &[], &self.date) {
            self.write_downstream(io, &buf[..n]);
            self.metrics.responses.inc(&self.config.registry);
        }
        io.set_deadline(
            Some(self.deadline(self.config.idle_timeout_ms)),
            vane_core::handler::DeadlineReason::Idle,
        );
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
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut resp = httparse::Response::new(&mut storage);
        match resp.parse(data) {
            Ok(httparse::Status::Complete(head_len)) => {
                let code = resp.code.unwrap_or(500);
                let status = Status::from_code(code);
                let mut content_length: Option<u64> = None;
                let mut chunked = false;
                let mut upstream_close = false;
                for h in resp.headers {
                    let name = h.name.to_ascii_lowercase();
                    if name == "content-length" {
                        content_length = std::str::from_utf8(h.value)
                            .ok()
                            .and_then(|s| s.trim().parse().ok());
                    } else if name == "transfer-encoding"
                        && h.value
                            .to_ascii_lowercase()
                            .windows(7)
                            .any(|w| w == b"chunked")
                    {
                        chunked = true;
                    } else if name == "connection"
                        && h.value
                            .to_ascii_lowercase()
                            .windows(5)
                            .any(|w| w.eq_ignore_ascii_case(b"close"))
                    {
                        upstream_close = true;
                    }
                }
                let framing = if code == 204 || code == 304 {
                    BodyFraming::Done
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

impl Handler for HttpProxy {
    fn on_connected(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        if let Some(tls_cfg) = &self.config.tls {
            match rustls::ServerConnection::new(Arc::clone(tls_cfg)) {
                Ok(conn) => self.conn(slot).tls = Some(conn),
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
        // Every downstream byte on a TLS listener is ciphertext: route
        // through the TLS pump (which handles handshake and app data).
        let tls = self
            .conns
            .get(&io.slot_index())
            .and_then(|c| c.tls.as_ref())
            .is_some();
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
        self.conn(slot).upstream_ready = true;
        let (route, upstream_path, inject) = {
            let conn = self.conn(slot);
            (
                conn.route.clone().expect("route"),
                conn.upstream_path.clone().expect("path"),
                std::mem::take(&mut conn.inject),
            )
        };
        // Re-parse the buffered head for serialization (view borrows our
        // buffer; the handler call is synchronous so this is safe).
        let buf = self.conn(slot).head_buf.clone();
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        if let Parsed::Complete(view, head_len) = RequestView::parse_in(&buf, &mut storage) {
            let head = self.build_upstream_head(&view, &route, &upstream_path, &inject, false, 0);
            io.write_upstream(&head);
            io.mark_request_sent();
            // Forward any body bytes that arrived with the head.
            let body = &buf[head_len..];
            if !body.is_empty() {
                io.write_upstream(body);
            }
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
        self.metrics
            .bytes_out
            .add(&self.config.registry, data.len() as u64);
        let slot = io.slot_index();
        if self.conn(slot).body == BodyFraming::AwaitingHead {
            // First upstream byte: clear the FirstByte deadline.
            io.set_deadline(None, vane_core::handler::DeadlineReason::FirstByte);
        }
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
                // Relay the head verbatim (hop-by-hop cleanup minimal).
                let head = data[..head_len].to_vec();
                self.write_downstream(io, &head);
                let rest = &data[head_len..];
                if !rest.is_empty() {
                    self.relay_body(io, rest);
                }
                self.check_done(io);
            }
            BodyFraming::ContentLength | BodyFraming::Chunked { .. } => {
                self.relay_body(io, data);
                self.check_done(io);
            }
            BodyFraming::Done => { /* trailing bytes after done: ignore */ }
        }
    }

    fn on_upstream_eof(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
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
        if let Some(route) = &self.conns.get(&slot).expect("conn exists").route {
            self.breaker.record_failure(&route.cluster);
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

        // Pick a backend.
        let mut balancer = route.balancer(u64::from(slot) ^ started.elapsed().as_nanos() as u64);
        let Some(addr) = balancer.pick_addr() else {
            self.respond_full(io, Status::ServiceUnavailable, "no healthy upstream\n");
            return;
        };
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
            let bytes = data[..n].to_vec();
            self.write_downstream(io, &bytes);
        }
        if framing_done {
            self.conn(slot).body = BodyFraming::Done;
        }
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
            self.metrics.latency.observe_us(
                &self.config.registry,
                u64::try_from(started.map(|s| s.elapsed().as_micros()).unwrap_or(0))
                    .unwrap_or(u64::MAX),
            );
            if let Some(route) = &self.conns.get(&slot).expect("conn").route {
                self.breaker.record_success(&route.cluster);
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
        let mut balancer = route.balancer(u64::from(slot) ^ u64::from(attempts + 1));
        let Some(addr) = balancer.pick_addr() else {
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
