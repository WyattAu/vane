//! The HTTP reverse-proxy handler: parses request heads, routes through
//! the EBR snapshot, applies the monomorphized filter pipeline, relays to
//! upstreams with keep-alive, and short-circuits errors.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

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
        let pipeline = Pipeline::new().then(RateLimit::new(
            Arc::clone(&registry),
            config.rate_limit_rps.unwrap_or(10_000),
            config.rate_limit_rps.unwrap_or(10_000),
        ));
        Self {
            config,
            pipeline,
            breaker,
            conns: HashMap::new(),
            date: vane_proto::date::DateCache::new(),
            metrics,
            worker_id,
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
        let mut buf = [0u8; 1024];
        if let Ok(n) = write_full(&mut buf, status, body.as_bytes(), &[], &self.date) {
            io.respond(&buf[..n]);
            self.metrics.responses.inc(&self.config.registry);
        }
        io.set_deadline(Some(
            Instant::now() + std::time::Duration::from_millis(self.config.idle_timeout_ms),
        ));
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
        let deadline =
            Instant::now() + std::time::Duration::from_millis(self.config.idle_timeout_ms);
        io.set_deadline(Some(deadline));
    }

    fn on_downstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        self.metrics
            .bytes_in
            .add(&self.config.registry, data.len() as u64);
        let slot = io.slot_index();
        {
            let conn = self.conn(slot);
            conn.head_buf.extend_from_slice(data);
        }
        // Parse from a private copy (the view borrows; worker-local copy is
        // a few KB and keeps borrowck trivial).
        let buf = self.conn(slot).head_buf.clone();
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(&buf, &mut storage) {
            Parsed::Partial => (), // keep accumulating
            Parsed::Error(e) => {
                let status = match e {
                    vane_proto::request::ParseError::TooLarge => Status::PayloadTooLarge,
                    vane_proto::request::ParseError::Malformed => Status::BadRequest,
                };
                self.respond_full(io, status, "bad request\n");
                let close = true;
                let _ = close;
                io.close();
            }
            Parsed::Complete(view, head_len) => {
                self.metrics.requests.inc(&self.config.registry);
                self.handle_request(io, &view, head_len);
            }
        }
    }

    fn on_upstream_connected(&mut self, io: &mut SessionIo<'_>) {
        let slot = io.slot_index();
        let (route, upstream_path, inject) = {
            let conn = self.conns.get_mut(&slot).expect("conn exists");
            (
                conn.route.clone().expect("route"),
                conn.upstream_path.clone().expect("path"),
                std::mem::take(&mut conn.inject),
            )
        };
        // Re-parse the buffered head for serialization (view borrows our
        // buffer; the handler call is synchronous so this is safe).
        let buf = self.conns.get(&slot).expect("conn").head_buf.clone();
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        if let Parsed::Complete(view, head_len) = RequestView::parse_in(&buf, &mut storage) {
            let head = self.build_upstream_head(&view, &route, &upstream_path, &inject, false, 0);
            io.write_upstream(&head);
            // Forward any body bytes that arrived with the head.
            let body = &buf[head_len..];
            if !body.is_empty() {
                io.write_upstream(body);
            }
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
                io.respond(&data[..head_len]);
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
        let (framing, remaining) = {
            let conn = self.conns.get(&slot).expect("conn");
            (conn.body, conn.remaining)
        };
        match framing {
            BodyFraming::ContentLength if remaining > 0 => {
                // Upstream died mid-body: client gets a truncated response;
                // close to be safe.
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

    fn on_downstream_eof(&mut self, io: &mut SessionIo<'_>) {
        // Client half-close: forward to upstream (end of request body).
        io.upstream_eof_write();
    }

    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, err: io::Error) {
        self.metrics.upstream_errors.inc(&self.config.registry);
        let slot = io.slot_index();
        if let Some(route) = &self.conns.get(&slot).expect("conn").route {
            self.breaker.record_failure(&route.cluster);
        }
        self.log(LogLevel::Error, &format!("upstream error: {err}"));
        if self
            .conns
            .get(&slot)
            .is_some_and(|c| c.body == BodyFraming::AwaitingHead)
        {
            self.respond_full(io, Status::BadGateway, "upstream unreachable\n");
        }
        io.close();
    }

    fn on_shutdown_hint(&mut self, io: &mut SessionIo<'_>) {
        // Drain: finish current transaction, then close. The idle deadline
        // applies; keep-alive ends after this response.
        let slot = io.slot_index();
        self.conn(slot).close_after = true;
    }

    fn on_deadline(&mut self, io: &mut SessionIo<'_>) {
        io.close();
    }
}

impl HttpProxy {
    #[allow(clippy::too_many_lines)]
    fn handle_request(&mut self, io: &mut SessionIo<'_>, view: &RequestView<'_>, head_len: usize) {
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

        // Inline body bytes: keep them queued for the upstream write.
        let inline = &self.conn(slot).head_buf[head_len..];
        let inline = inline.to_vec();
        if !self.breaker.cluster_breaker(&route.cluster).is_open() {
            if !io.connect_upstream(addr) {
                self.metrics.upstream_errors.inc(&self.config.registry);
                self.breaker.record_failure(&route.cluster);
                self.respond_full(io, Status::BadGateway, "connect failed\n");
                io.close();
            }
        } else {
            self.respond_full(io, Status::ServiceUnavailable, "circuit open\n");
            return;
        }
        // Preserve inline body in head_buf for on_upstream_connected
        // (head_buf holds head + inline body; consumed there).
        let _ = inline;
    }

    fn relay_body(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let slot = io.slot_index();
        let conn = self.conn(slot);
        match conn.body {
            BodyFraming::ContentLength => {
                let take = (data.len() as u64).min(conn.remaining) as usize;
                conn.remaining -= take as u64;
                io.respond(&data[..take]);
                if conn.remaining == 0 {
                    conn.body = BodyFraming::Done;
                }
            }
            BodyFraming::Chunked {
                last_was_lf: _,
                seen_zero,
            } => {
                // Pass through verbatim; detect the terminal 0-chunk
                // ("0\r\n\r\n") for completion.
                io.respond(data);
                let conn = self.conn(slot);
                if let BodyFraming::Chunked {
                    last_was_lf,
                    seen_zero,
                } = &mut conn.body
                {
                    let _ = last_was_lf;
                    // Search for the zero-length chunk terminator.
                    if !*seen_zero && data.windows(5).any(|w| w == b"0\r\n\r\n") {
                        *seen_zero = true;
                    }
                    if *seen_zero {
                        conn.body = BodyFraming::Done;
                    }
                }
                let _ = seen_zero;
            }
            _ => {}
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
                // Keep-alive: reset transaction state for the next request.
                let conn = self.conn(slot);
                conn.head_buf.clear();
                conn.route = None;
                conn.upstream_path = None;
                conn.body = BodyFraming::AwaitingHead;
                conn.remaining = 0;
                conn.close_after = false;
            }
        }
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
