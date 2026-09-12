//! HTTP/2 upstream client: adapts the native sans-io engine
//! (`vane_core::h2`, client role) to the proxy's h1 upstream relay.
//!
//! One [`H2Upstream`] wraps one backend connection speaking prior-
//! knowledge h2. Requests enter as the client's original HTTP/1.1
//! head (translated to pseudo-headers + HPACK) plus body chunks;
//! the upstream's h2 response frames come back translated into a
//! synthetic HTTP/1.1 byte stream ([`UpstreamEvent::ResponseHead`] /
//! [`ResponseBody`]) that feeds the proxy's existing h1 relay
//! machinery unchanged — compression, access logs, and deadlines all
//! apply as with h1 backends.

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};

/// Driver events translated into the proxy's h1 upstream flow.
#[derive(Debug)]
pub enum UpstreamEvent {
    /// Synthetic HTTP/1.1 response head (status line + headers +
    /// blank line) — feed through the normal h1 response path.
    ResponseHead(Vec<u8>),
    /// Response body bytes.
    ResponseBody(Vec<u8>),
    /// The response is fully received (Content-Length satisfied or
    /// END_STREAM for lengthless bodies).
    ResponseComplete,
}

/// Client-side h2 state for one backend connection.
pub struct H2Upstream {
    conn: Connection,
    /// Bytes received but not yet consumed (partial frames).
    backlog: Vec<u8>,
    /// The request stream (client role: odd ids).
    stream: Option<u32>,
    /// Remaining request-body bytes to send (None = complete).
    req_remaining: Option<u64>,
    /// Request bytes over the send window, held for credit.
    req_held: Vec<u8>,
    /// END_STREAM still owed after the held tail drains.
    req_end_pending: bool,
    /// Response body bytes received (for CL completion checks).
    resp_sent: u64,
    resp_content_length: Option<u64>,
    /// Response complete.
    resp_done: bool,
}

impl H2Upstream {
    /// A new client session: the preface + initial SETTINGS are queued
    /// and must be flushed ([`Self::pending_writes`]) with the request.
    pub fn new() -> Self {
        Self {
            conn: Connection::new(Role::Client, ConnectionConfig::default()),
            backlog: Vec::new(),
            stream: None,
            req_remaining: None,
            req_held: Vec::new(),
            req_end_pending: false,
            resp_sent: 0,
            resp_content_length: None,
            resp_done: false,
        }
    }

    /// Connection is in an error state: the driver must drop the
    /// upstream (failover handles retry).
    pub fn failed(&self) -> bool {
        self.conn.connection_error().is_some()
    }

    /// Whether the response is fully received.
    pub fn response_complete(&self) -> bool {
        self.resp_done
    }

    /// The engine's connection error code, if any.
    pub fn conn_error_code(&self) -> Option<u32> {
        self.conn.connection_error().map(|e| e.code)
    }

    /// Frames queued for the backend.
    pub fn pending_writes(&mut self) -> Vec<u8> {
        self.conn.take_pending_writes()
    }

    /// Sends the request head (the client's original HTTP/1.1 bytes)
    /// as h2 HEADERS. `remaining` = request body size still to come
    /// (`Some(0)`/`None` = bodyless: END_STREAM on the head).
    pub fn send_request(&mut self, h1_head: &[u8], remaining: Option<u64>) {
        let headers = Self::translate_request_head(h1_head);
        let id = self.conn.alloc_stream_id();
        self.stream = Some(id);
        let end_stream = !matches!(remaining, Some(n) if n > 0);
        self.req_remaining = remaining.filter(|n| *n > 0);
        self.conn.send_headers(id, &headers, end_stream);
    }

    /// h1 request head → h2 header list (pseudo-headers first).
    fn translate_request_head(h1_head: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut method: &[u8] = b"GET";
        let mut path: &[u8] = b"/";
        let mut authority: Option<Vec<u8>> = None;
        let mut regular: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let split = h1_head
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap_or(h1_head.len());
        for (i, line) in h1_head[..split].split(|b| *b == b'\n').enumerate() {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if i == 0 {
                // METHOD SP PATH SP VERSION
                let mut parts = line.split(|b| *b == b' ');
                if let Some(m) = parts.next() {
                    method = m;
                }
                if let Some(p) = parts.next() {
                    path = p;
                }
                continue;
            }
            let Some(colon) = line.iter().position(|b| *b == b':') else {
                continue;
            };
            let name = line[..colon].to_ascii_lowercase();
            let value = trim(&line[colon + 1..]);
            match name.as_slice() {
                b"host" => authority = Some(value.to_vec()),
                b"connection" | b"keep-alive" | b"proxy-connection" | b"upgrade"
                | b"transfer-encoding" => {}
                _ => regular.push((name, value.to_vec())),
            }
        }
        let mut headers = Vec::with_capacity(regular.len() + 4);
        headers.push((b":method".to_vec(), method.to_vec()));
        headers.push((b":scheme".to_vec(), b"http".to_vec()));
        if let Some(a) = &authority {
            headers.push((b":authority".to_vec(), a.clone()));
        }
        headers.push((b":path".to_vec(), path.to_vec()));
        headers.extend(regular);
        headers
    }

    /// Relays request body bytes (flow-controlled). Returns the frames
    /// to write upstream; bytes over the current credit are held and
    /// retried automatically when WINDOW_UPDATEs arrive during
    /// [`Self::handle_read`]. END_STREAM goes out with the final byte
    /// once the declared Content-Length is satisfied.
    pub fn request_body(&mut self, data: &[u8]) -> Vec<u8> {
        let Some(rem) = self.req_remaining else {
            return Vec::new();
        };
        let take = (data.len() as u64).min(rem) as usize;
        let left = rem - take as u64;
        self.req_remaining = (left > 0).then_some(left);
        let is_last = left == 0;
        let Some(stream) = self.stream else {
            return Vec::new();
        };
        let chunk = &data[..take];
        let mut sent = self.conn.send_data(stream, chunk, is_last);
        if sent < chunk.len() {
            // Window exhausted mid-chunk: hold the unsent tail.
            self.req_held.extend_from_slice(&chunk[sent..]);
            if is_last {
                self.req_end_pending = true;
            }
        }
        if take < data.len() {
            // Beyond the declared Content-Length: drop (defensive).
            let _ = &data[take..];
        }
        let _ = &mut sent;
        self.pending_writes()
    }

    /// Request-side credit arrived (internal): drain held bytes.
    fn on_request_credit(&mut self) {
        if self.req_held.is_empty() {
            return;
        }
        let Some(stream) = self.stream else { return };
        let held = std::mem::take(&mut self.req_held);
        let end = self.req_end_pending;
        let mut offset = 0usize;
        while offset < held.len() {
            let sent = self.conn.send_data(stream, &held[offset..], end);
            if sent == 0 {
                break; // out of credit again
            }
            offset += sent;
        }
        if offset < held.len() {
            self.req_held = held[offset..].to_vec();
        } else {
            self.req_end_pending = false;
        }
    }

    /// Feeds backend bytes; emits translated events. The driver must
    /// flush [`Self::pending_writes`] after each call and honor
    /// [`Self::failed`].
    pub fn handle_read(&mut self, data: &[u8], events: &mut Vec<UpstreamEvent>) {
        self.backlog.extend_from_slice(data);
        loop {
            let mut engine_events = Vec::new();
            let consumed = self.conn.handle_read(&self.backlog, &mut engine_events);
            if consumed == 0 {
                break;
            }
            self.backlog.drain(..consumed);
            for ev in engine_events {
                self.on_engine_event(ev, events);
            }
            if self.failed() {
                break;
            }
        }
        if self.backlog.len() > 1024 * 1024 {
            let _ = self.conn.connection_error();
        }
    }

    fn on_engine_event(&mut self, ev: Event, events: &mut Vec<UpstreamEvent>) {
        match ev {
            Event::Headers {
                stream_id,
                end_stream,
                headers,
            } => {
                if self.stream != Some(stream_id) {
                    return;
                }
                let head = Self::translate_response_head(&headers);
                // Content-Length framing for the response.
                self.resp_content_length = headers
                    .iter()
                    .find(|h| h.name == b"content-length")
                    .and_then(|h| std::str::from_utf8(&h.value).ok())
                    .and_then(|v| v.parse().ok());
                events.push(UpstreamEvent::ResponseHead(head));
                if end_stream {
                    self.resp_done = true;
                    events.push(UpstreamEvent::ResponseComplete);
                }
            }
            Event::Data {
                stream_id,
                end_stream,
                data,
            } => {
                if self.stream != Some(stream_id) {
                    return;
                }
                self.conn.release_capacity(stream_id, data.len());
                self.resp_sent += data.len() as u64;
                events.push(UpstreamEvent::ResponseBody(data));
                let cl_complete = self
                    .resp_content_length
                    .is_some_and(|len| self.resp_sent >= len);
                if end_stream || cl_complete {
                    self.resp_done = true;
                    events.push(UpstreamEvent::ResponseComplete);
                }
            }
            Event::WindowUpdate { .. } => {
                self.on_request_credit();
            }
            Event::Reset { .. } | Event::GoAway { .. } | Event::SettingsAck => {}
        }
    }

    /// h2 response headers → synthetic HTTP/1.1 head.
    fn translate_response_head(headers: &[vane_core::h2::hpack::Header]) -> Vec<u8> {
        let status = headers
            .iter()
            .find(|h| h.name == b":status")
            .and_then(|h| std::str::from_utf8(&h.value).ok())
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(502);
        let reason = match status {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            301 => "Moved Permanently",
            302 => "Found",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            _ => "",
        };
        let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
        for h in headers {
            if h.name.starts_with(b":") {
                continue;
            }
            head.push_str(&String::from_utf8_lossy(&h.name));
            head.push_str(": ");
            head.push_str(&String::from_utf8_lossy(&h.value));
            head.push_str("\r\n");
        }
        head.push_str("\r\n");
        head.into_bytes()
    }
}

impl Default for H2Upstream {
    fn default() -> Self {
        Self::new()
    }
}

fn trim(b: &[u8]) -> &[u8] {
    let mut out = b;
    while let Some(f) = out.first() {
        if f.is_ascii_whitespace() {
            out = &out[1..];
        } else {
            break;
        }
    }
    while let Some(l) = out.last() {
        if l.is_ascii_whitespace() {
            out = &out[..out.len() - 1];
        } else {
            break;
        }
    }
    out
}
