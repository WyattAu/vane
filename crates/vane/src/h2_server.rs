//! HTTP/2 server termination on the engine data path: adapts the
//! native sans-io engine (`vane_core::h2`) to the HTTP/1.1
//! [`HttpProxy`](crate::proxy::HttpProxy) transaction flow.
//!
//! One [`H2Server`] wraps one engine session slot. v0.2 serializes
//! streams per connection (`max_concurrent_streams = 1`): a second
//! concurrent stream is REFUSED_STREAM (the standard retry signal);
//! full multiplexing is the v0.3 track.
//!
//! Inbound: decrypted h2 bytes accumulate in `backlog`;
//! `handle_read` drains complete frames, translating request heads to
//! HTTP/1.1 (`:method/:path/:authority` → request line/Host) and
//! relaying body DATA.
//!
//! Outbound: upstream HTTP/1.1 response bytes are translated to
//! HEADERS + DATA frames ([`ResponseTranslator`]) honoring the peer's
//! max frame size and flow-control windows; excess queues in `held`
//! until WINDOW_UPDATE credit arrives.
//!
//! Wire violations surface as engine connection errors: the driver
//! flushes the queued GOAWAY and closes (pool discards, no reuse).

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};

/// What the driver should do when the upstream EOFs mid-response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EofOutcome {
    /// Body still flowing (held bytes) — keep the transaction open.
    Continue,
    /// Response fully emitted (END_STREAM sent) — finish the
    /// transaction.
    Completed,
    /// Upstream died before the declared Content-Length — close.
    Truncated,
}

/// Engine events the driver must act on.
#[derive(Debug)]
pub enum H2Event {
    /// A complete request head (translated to HTTP/1.1).
    RequestHead {
        /// HTTP/1.1 request head bytes (ends with the blank line).
        head: Vec<u8>,
        /// Client half-closed with the head (no body follows).
        end_stream: bool,
    },
    /// Request body bytes.
    RequestBody {
        /// Body bytes.
        data: Vec<u8>,
    },
    /// The request stream is fully received.
    RequestComplete,
    /// Send credit arrived — retry held response bytes.
    SendCredit,
}

/// Per-connection h2 state (one engine session slot).
pub struct H2Server {
    /// The proxy transaction slot this session serves.
    pub slot: u32,
    /// The sans-io connection state machine.
    conn: Connection,
    /// Bytes received but not yet consumed (partial frames).
    backlog: Vec<u8>,
    /// Stream with an in-flight transaction.
    active_stream: Option<u32>,
    /// Remaining request-body bytes to accept (None = complete).
    body_remaining: Option<u64>,
    /// Outbound translation state.
    translator: Option<ResponseTranslator>,
    /// Response bytes held back by flow control.
    held: Vec<u8>,
}

/// Translates one upstream HTTP/1.1 response into h2 HEADERS + DATA.
struct ResponseTranslator {
    /// Content-Length from the upstream head (`None` = EOF-delimited).
    content_length: Option<u64>,
    /// Body bytes accepted so far.
    sent: u64,
    /// Head seen (state machine).
    head_done: bool,
    /// END_STREAM emitted.
    done: bool,
    /// Upstream bytes held while the head is incomplete (straddling
    /// reads); drained into translation once the head parses.
    head_buf: Vec<u8>,
}

impl H2Server {
    /// A new h2 session bound to `slot`.
    pub fn new(slot: u32) -> Self {
        let cfg = ConnectionConfig {
            // v0.2 serializes transactions per h2 connection; extra
            // concurrent streams are refused with REFUSED_STREAM.
            max_concurrent_streams: 1,
            ..ConnectionConfig::default()
        };
        Self {
            slot,
            conn: Connection::new(Role::Server, cfg),
            backlog: Vec::new(),
            active_stream: None,
            body_remaining: None,
            translator: None,
            held: Vec::new(),
        }
    }

    /// Connection is in an error state: flush GOAWAY and close.
    pub fn failed(&self) -> bool {
        self.conn.connection_error().is_some()
    }

    /// The engine's connection error code, if any.
    pub fn conn_error_code(&self) -> Option<u32> {
        self.conn.connection_error().map(|e| e.code)
    }

    /// Frames the engine has queued for the peer (SETTINGS/ACK/GOAWAY/
    /// RST/WINDOW_UPDATE and anything the driver emitted).
    pub fn pending_writes(&mut self) -> Vec<u8> {
        self.conn.take_pending_writes()
    }

    /// Feeds decrypted h2 bytes; emits driver events. Callers must
    /// flush [`Self::pending_writes`] and honor [`Self::failed`].
    pub fn handle_read(&mut self, data: &[u8], events: &mut Vec<H2Event>) {
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
        // Cap runaway backlogs (peer flooding partial frames).
        if self.backlog.len() > 1024 * 1024 {
            let _ = self.conn.connection_error();
        }
    }

    fn on_engine_event(&mut self, ev: Event, events: &mut Vec<H2Event>) {
        match ev {
            Event::Headers {
                stream_id,
                end_stream,
                headers,
            } => {
                if self.active_stream.is_some() {
                    // Serialized mode: refuse overlap.
                    self.conn.send_rst_stream(
                        stream_id,
                        vane_core::h2::connection::error_code::REFUSED_STREAM,
                    );
                    return;
                }
                // v0.2 relays bodies with a declared Content-Length
                // only: h1 upstreams would need chunked re-framing for
                // anything else. END_STREAM requests (GET etc.) carry
                // no body and are always accepted; a streamed body
                // without Content-Length is refused with
                // REFUSED_STREAM (retryable and honest).
                let accepted = end_stream || Self::request_content_length(&headers).is_some();
                if accepted {
                    if let Some(head) = Self::translate_head(&headers) {
                        self.active_stream = Some(stream_id);
                        self.body_remaining = match Self::request_content_length(&headers) {
                            Some(len) if !end_stream => Some(len),
                            _ => None,
                        };
                        events.push(H2Event::RequestHead { head, end_stream });
                        if end_stream {
                            events.push(H2Event::RequestComplete);
                        }
                    } else {
                        // Unparseable head (missing pseudo-fields).
                        self.conn.send_rst_stream(
                            stream_id,
                            vane_core::h2::connection::error_code::PROTOCOL_ERROR,
                        );
                    }
                } else {
                    self.conn.send_rst_stream(
                        stream_id,
                        vane_core::h2::connection::error_code::REFUSED_STREAM,
                    );
                }
            }
            Event::Data {
                stream_id,
                end_stream,
                data,
            } => {
                if self.active_stream != Some(stream_id) {
                    return; // unsolicited data on unknown stream
                }
                self.conn.release_capacity(stream_id, data.len());
                let take = match self.body_remaining {
                    Some(rem) => (data.len() as u64).min(rem) as usize,
                    None => 0,
                };
                if take > 0 {
                    if let Some(rem) = &mut self.body_remaining {
                        *rem -= take as u64;
                    }
                    events.push(H2Event::RequestBody {
                        data: data[..take].to_vec(),
                    });
                }
                if end_stream {
                    self.body_remaining = None;
                    events.push(H2Event::RequestComplete);
                }
            }
            Event::Trailers { stream_id, .. } => {
                // Request trailers (gRPC-style). h1 upstreams with
                // Content-Length framing cannot carry them — relay the
                // completion semantics only; the trailer fields are
                // dropped (v0.2 relays CL'd bodies, documented).
                if self.active_stream == Some(stream_id) {
                    self.body_remaining = None;
                    events.push(H2Event::RequestComplete);
                }
            }
            Event::WindowUpdate { .. } => {
                if !self.held.is_empty() {
                    events.push(H2Event::SendCredit);
                }
            }
            Event::Reset { .. } | Event::GoAway { .. } | Event::SettingsAck => {}
        }
    }

    /// The declared request body size (`None` when absent).
    fn request_content_length(headers: &[vane_core::h2::hpack::Header]) -> Option<u64> {
        headers
            .iter()
            .find(|h| h.name == b"content-length")
            .and_then(|h| std::str::from_utf8(&h.value).ok())
            .and_then(|v| v.parse().ok())
    }

    /// Translates HPACK-decoded request headers to an HTTP/1.1 head.
    fn translate_head(headers: &[vane_core::h2::hpack::Header]) -> Option<Vec<u8>> {
        let mut method: Option<&[u8]> = None;
        let mut path: Option<&[u8]> = None;
        let mut authority: Option<&[u8]> = None;
        let mut regular: Vec<(&[u8], &[u8])> = Vec::new();
        for h in headers {
            match h.name.as_slice() {
                b":method" => method = Some(&h.value),
                b":path" => path = Some(&h.value),
                b":authority" => authority = Some(&h.value),
                b":scheme" => {}
                // Connection-scoped headers are illegal in requests
                // (RFC 9113 §8.2.2) — reject.
                b"connection" | b"keep-alive" | b"proxy-connection" | b"upgrade" => return None,
                b"transfer-encoding" => return None,
                _ => regular.push((&h.name, &h.value)),
            }
        }
        let (method, path) = (method?, path?);
        // Header names with uppercase are invalid in h2; HPACK decoded
        // values pass through as received.
        let mut head = Vec::with_capacity(
            64 + regular
                .iter()
                .map(|(n, v)| n.len() + v.len() + 4)
                .sum::<usize>(),
        );
        head.extend_from_slice(method);
        head.push(b' ');
        head.extend_from_slice(path);
        head.extend_from_slice(b" HTTP/1.1\r\n");
        if let Some(a) = authority {
            head.extend_from_slice(b"host: ");
            head.extend_from_slice(a);
            head.extend_from_slice(b"\r\n");
        }
        for (n, v) in regular {
            head.extend_from_slice(n);
            head.extend_from_slice(b": ");
            head.extend_from_slice(v);
            head.extend_from_slice(b"\r\n");
        }
        head.extend_from_slice(b"\r\n");
        Some(head)
    }

    /// Feeds upstream response bytes; returns h2 frames to emit plus
    /// whether the response is fully emitted (END_STREAM sent).
    pub fn response_bytes(&mut self, data: &[u8]) -> (Vec<Vec<u8>>, bool) {
        let max_frame = self.conn.peer_max_frame_size();
        if self.translator.is_none() {
            self.translator = Some(ResponseTranslator::new());
        }
        let mut out = Vec::new();

        // Phase 1: head. Upstream heads may straddle reads; the bytes
        // are buffered in the translator until the head parses whole.
        let head_done = self.translator.as_ref().is_some_and(|t| t.head_done);
        if !head_done {
            let parsed: Option<usize> = {
                let t = self.translator.as_mut().expect("checked");
                t.head_buf.extend_from_slice(data);
                let scratch = t.head_buf.clone();
                let mut storage =
                    [httparse::EMPTY_HEADER; vane_proto::response::MAX_RESPONSE_HEADERS];
                let mut resp = httparse::Response::new(&mut storage);
                match resp.parse(&scratch) {
                    Ok(httparse::Status::Complete(head_len)) => Some(head_len),
                    _ => None,
                }
            };
            let Some(head_len) = parsed else {
                // Whole prefix stays buffered for the next read.
                return (out, false);
            };
            // Draining borrows sequentially; each step takes what it
            // needs before the next.
            let head_bytes: Vec<u8> = {
                let t = self.translator.as_mut().expect("checked");
                t.head_buf.drain(..head_len).collect()
            };
            {
                let t = self.translator.as_mut().expect("checked");
                t.translate_head_bytes(
                    self.active_stream.unwrap_or(0),
                    &head_bytes,
                    &mut out,
                    max_frame,
                );
            }
            // Body bytes that rode behind the head in the same read.
            let buffered = {
                let t = self.translator.as_mut().expect("checked");
                std::mem::take(&mut t.head_buf)
            };
            if !buffered.is_empty() {
                self.queue_body(&buffered, &mut out, max_frame);
            }
        } else {
            self.queue_body(data, &mut out, max_frame);
        }
        let done = self.translator.as_ref().is_some_and(|t| t.done);
        if done {
            self.translator = None;
            self.active_stream = None;
        }
        (out, done)
    }

    /// Upstream EOF with an EOF-delimited (or empty) body: end the
    /// stream. Returns frames plus whether the response completed now.
    pub fn response_eof(&mut self) -> (Vec<Vec<u8>>, EofOutcome) {
        let Some(t) = self.translator.as_mut() else {
            return (Vec::new(), EofOutcome::Continue);
        };
        // Content-Length bodies: the upstream closing is EXPECTED before
        // the client has drained everything — held bytes keep flowing on
        // credit, and completion fires from take_held instead. Ending
        // the stream here would cut off delivered body.
        if let (true, Some(cl)) = (t.head_done, t.content_length) {
            if self.held.is_empty() && t.sent < cl {
                // Upstream died mid-body with nothing left to send.
                return (Vec::new(), EofOutcome::Truncated);
            }
            return (Vec::new(), EofOutcome::Continue);
        }
        // EOF-delimited body: the upstream EOF IS the end.
        let stream_id = self.active_stream.unwrap_or(0);
        let mut out = Vec::new();
        if !t.done {
            // Empty DATA with END_STREAM.
            let mut frame = Vec::with_capacity(9);
            vane_core::h2::frame::write_header(
                &mut frame,
                0,
                vane_core::h2::frame::FrameKind::Data,
                vane_core::h2::frame::FrameFlags::from_u8(0x01),
                stream_id,
            );
            out.push(frame);
            t.done = true;
        }
        self.translator = None;
        self.active_stream = None;
        (out, EofOutcome::Completed)
    }

    /// Queues body bytes, honoring the send window; excess held.
    fn queue_body(&mut self, data: &[u8], out: &mut Vec<Vec<u8>>, max_frame: usize) {
        let stream_id = self.active_stream.unwrap_or(0);
        let t = self.translator.as_mut().expect("body after head");
        let mut offset = 0usize;
        loop {
            let want = data.len() - offset;
            if want == 0 {
                break;
            }
            let room = max_frame;
            // Debit the engine's windows for driver-emitted frames.
            let take = self.conn.consume_send_budget(stream_id, room.min(want));
            if take == 0 {
                break;
            }
            t.sent += take as u64;
            let last = t.content_length.is_some_and(|len| t.sent >= len);
            if last {
                t.done = true;
            }
            out.push(data_frame(stream_id, &data[offset..offset + take], last));
            offset += take;
            if last {
                break;
            }
        }
        if offset < data.len() {
            self.held.extend_from_slice(&data[offset..]);
        }
    }

    /// Retries held bytes after WINDOW_UPDATE credit. The bool reports
    /// whether the response completed now (END_STREAM emitted) — the
    /// driver must finish the transaction when true.
    pub fn take_held(&mut self) -> (Vec<Vec<u8>>, bool) {
        if self.held.is_empty() {
            return (Vec::new(), false);
        }
        let held = std::mem::take(&mut self.held);
        let max_frame = self.conn.peer_max_frame_size();
        let mut out = Vec::new();
        self.queue_body(&held, &mut out, max_frame);
        let done = self.translator.as_ref().is_some_and(|t| t.done);
        if done {
            self.translator = None;
            self.active_stream = None;
        }
        (out, done)
    }

    /// Relays upstream response trailers to the client as a HEADERS
    /// (END_STREAM) frame — the gRPC grpc-status path for h2→h2 relay.
    /// Pseudo-headers are dropped; the response's END_STREAM rides this
    /// frame. Returns the frames for the driver to flush.
    pub fn response_trailers(&mut self, trailers: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
        let stream_id = self.active_stream.unwrap_or(0);
        let max_frame = self.conn.peer_max_frame_size();
        let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(trailers.len());
        for (name, value) in trailers {
            if name.starts_with(b":") {
                continue;
            }
            headers.push((name.to_ascii_lowercase(), value.clone()));
        }
        let frames = emit_header_block_full_end(stream_id, &headers, max_frame);
        self.translator = None;
        self.active_stream = None;
        frames
    }

    /// Marks the active stream finished (transaction complete): future
    /// HEADERS may open the next stream.
    pub fn finish_stream(&mut self) {
        if let Some(id) = self.active_stream.take() {
            // If our side hasn't ended the stream (e.g. aborted), RST.
            if self.conn.stream_is_open(id) {
                self.conn
                    .send_rst_stream(id, vane_core::h2::connection::error_code::NO_ERROR);
            }
        }
        self.translator = None;
        self.held.clear();
        self.body_remaining = None;
    }
}

/// Serializes one DATA frame.
fn data_frame(stream_id: u32, data: &[u8], end_stream: bool) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9 + data.len());
    vane_core::h2::frame::write_header(
        &mut frame,
        data.len() as u32,
        vane_core::h2::frame::FrameKind::Data,
        vane_core::h2::frame::FrameFlags::from_u8(if end_stream { 0x01 } else { 0 }),
        stream_id,
    );
    frame.extend_from_slice(data);
    frame
}

impl ResponseTranslator {
    fn new() -> Self {
        Self {
            content_length: None,
            sent: 0,
            head_done: false,
            done: false,
            head_buf: Vec::new(),
        }
    }

    /// Translates an already-parsed upstream head into HEADERS frames.
    /// `head` must be a complete HTTP/1.1 response head (status line +
    /// headers + blank line).
    fn translate_head_bytes(
        &mut self,
        stream_id: u32,
        head: &[u8],
        out: &mut Vec<Vec<u8>>,
        max_frame: usize,
    ) {
        let mut storage = [httparse::EMPTY_HEADER; vane_proto::response::MAX_RESPONSE_HEADERS];
        let mut resp = httparse::Response::new(&mut storage);
        if resp.parse(head).is_err() {
            return; // unreachable: the caller parsed this head already
        }
        let code = resp.code.unwrap_or(200);
        let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(resp.headers.len() + 1);
        headers.push((b":status".to_vec(), code.to_string().into_bytes()));
        for h in resp.headers.iter() {
            let lname = h.name.to_ascii_lowercase();
            // Connection-scoped headers do not exist in h2 (RFC 9113
            // §8.2.2); Content-Length maps to the body delimiter.
            if matches!(
                lname.as_bytes(),
                b"connection" | b"keep-alive" | b"transfer-encoding" | b"upgrade"
            ) {
                continue;
            }
            if lname == "content-length" {
                self.content_length = std::str::from_utf8(h.value)
                    .ok()
                    .and_then(|v| v.parse().ok());
                continue;
            }
            headers.push((lname.into_bytes(), h.value.to_vec()));
        }
        self.head_done = true;
        out.extend(emit_header_block(stream_id, &headers, max_frame));
    }
}

/// Encodes headers and splits the block into HEADERS + CONTINUATION
/// frames sized to `max_frame` (END_HEADERS on the final fragment,
/// per RFC 9113 §4.3). The final HEADERS fragment carries END_STREAM.
fn emit_header_block_full_end(
    stream_id: u32,
    headers: &[(Vec<u8>, Vec<u8>)],
    max_frame: usize,
) -> Vec<Vec<u8>> {
    let mut encoder = vane_core::h2::hpack::HpackEncoder::new();
    let mut block = Vec::new();
    encoder.encode(headers, &mut block);
    let mut frames = Vec::new();
    let mut first = true;
    let mut off = 0usize;
    while first || off < block.len() {
        let take = (block.len() - off).min(max_frame);
        let last = off + take == block.len();
        let mut frame = Vec::with_capacity(9 + take);
        let kind = if first {
            vane_core::h2::frame::FrameKind::Headers
        } else {
            vane_core::h2::frame::FrameKind::Continuation
        };
        let flags = if last { 0x04 | 0x01 } else { 0 };
        vane_core::h2::frame::write_header(
            &mut frame,
            take as u32,
            kind,
            vane_core::h2::frame::FrameFlags::from_u8(flags),
            stream_id,
        );
        frame.extend_from_slice(&block[off..off + take]);
        frames.push(frame);
        off += take;
        first = false;
    }
    frames
}

/// Encodes headers and splits the block into HEADERS + CONTINUATION
/// frames sized to `max_frame` (END_HEADERS on the final fragment,
/// per RFC 9113 §4.3).
fn emit_header_block(
    stream_id: u32,
    headers: &[(Vec<u8>, Vec<u8>)],
    max_frame: usize,
) -> Vec<Vec<u8>> {
    let mut encoder = vane_core::h2::hpack::HpackEncoder::new();
    let mut block = Vec::new();
    encoder.encode(headers, &mut block);
    let mut frames = Vec::new();
    let mut first = true;
    let mut off = 0usize;
    while first || off < block.len() {
        let take = (block.len() - off).min(max_frame);
        let last = off + take == block.len();
        let mut frame = Vec::with_capacity(9 + take);
        let kind = if first {
            vane_core::h2::frame::FrameKind::Headers
        } else {
            vane_core::h2::frame::FrameKind::Continuation
        };
        let flags = if last { 0x04 } else { 0 };
        vane_core::h2::frame::write_header(
            &mut frame,
            take as u32,
            kind,
            vane_core::h2::frame::FrameFlags::from_u8(flags),
            stream_id,
        );
        frame.extend_from_slice(&block[off..off + take]);
        frames.push(frame);
        off += take;
        first = false;
    }
    frames
}
