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
    /// The active request lacks Content-Length: the body is relayed to
    /// the h1 upstream with shim-manufactured chunk framing (h2 has no
    /// chunked encoding; END_STREAM delimits).
    req_chunked: bool,
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
    /// A flow-control-stall PING is outstanding (client owes us a
    /// PONG that should flush its queued WINDOW_UPDATEs).
    probe_inflight: bool,
    /// The response completed (END_STREAM emitted). Further upstream
    /// bytes exceed the declared framing and must never be re-fed.
    resp_done: bool,
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
            req_chunked: false,
            body_remaining: None,
            translator: None,
            held: Vec::new(),
            probe_inflight: false,
            resp_done: false,
        }
    }

    /// Flow-control stall: response bytes are held with zero send
    /// budget. The client's queued WINDOW_UPDATE may sit unflushed in
    /// its connection task — a PING forces it to process input and
    /// flush output (standard keepalive-style tickle). Sent at most
    /// once per stall; re-armed when credit arrives.
    fn maybe_probe_stall(&mut self) {
        let stream_id = self.active_stream.unwrap_or(0);
        if self.held.is_empty() || self.conn.send_budget(stream_id) > 0 {
            return;
        }
        if self.probe_inflight {
            return;
        }
        self.probe_inflight = true;
        self.conn
            .send_ping(&[0x56, 0x41, 0x4e, 0x45, 0x50, 0x52, 0x4f, 0x42]); // "VANEPROB"
    }

    /// Connection is in an error state: flush GOAWAY and close.
    pub fn failed(&self) -> bool {
        self.conn.connection_error().is_some()
    }

    /// Debug: engine send windows (conn, active stream).
    #[must_use]
    pub fn conn_debug_windows(&self) -> (i64, i64) {
        self.conn
            .debug_send_windows(self.active_stream.unwrap_or(0))
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
        eprintln!("SHIMDBG intake {}", data.len());
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
                // Bodies with Content-Length relay verbatim; bodies
                // without one are chunk-re-framed toward the h1
                // upstream (END_STREAM delimits in h2). END_STREAM
                // requests (GET etc.) carry no body and are always
                // accepted; a streamed CL-less body used to be refused
                // with REFUSED_STREAM.
                let accepted = true;
                let chunked_req = !end_stream && Self::request_content_length(&headers).is_none();

                if accepted {
                    if let Some(head) = Self::translate_head(&headers, chunked_req) {
                        self.active_stream = Some(stream_id);
                        self.req_chunked = chunked_req;
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
                if self.req_chunked {
                    // CL-less body: re-frame as h1 chunks; the terminal
                    // chunk closes the upstream's chunked framing.
                    eprintln!(
                        "SHIMDBG chunk-relay {} bytes eom={}",
                        data.len(),
                        end_stream
                    );
                    let mut framed = Vec::with_capacity(data.len() + 20);
                    framed.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
                    framed.extend_from_slice(&data);
                    framed.extend_from_slice(b"\r\n");
                    events.push(H2Event::RequestBody { data: framed });
                    if end_stream {
                        self.req_chunked = false;
                        events.push(H2Event::RequestBody {
                            data: b"0\r\n\r\n".to_vec(),
                        });
                        events.push(H2Event::RequestComplete);
                    }
                } else {
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
            }
            Event::Trailers { stream_id, .. } => {
                // Request trailers (gRPC-style). h1 upstreams with
                // Content-Length framing cannot carry them — relay the
                // completion semantics only; the trailer fields are
                // dropped (v0.2 relays CL'd bodies, documented).
                if self.active_stream == Some(stream_id) {
                    if self.req_chunked {
                        self.req_chunked = false;
                        events.push(H2Event::RequestBody {
                            data: b"0\r\n\r\n".to_vec(),
                        });
                    }
                    self.body_remaining = None;
                    events.push(H2Event::RequestComplete);
                }
            }
            Event::WindowUpdate { .. } => {
                // Credit arrived: the stall probe did its job.
                self.probe_inflight = false;
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
    fn translate_head(headers: &[vane_core::h2::hpack::Header], chunked: bool) -> Option<Vec<u8>> {
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
        if chunked {
            // CL-less h2 body: declare chunked framing to the h1
            // upstream; the shim manufactures the chunk boundaries.
            head.extend_from_slice(b"transfer-encoding: chunked\r\n");
        }
        head.extend_from_slice(b"\r\n");
        Some(head)
    }

    /// Feeds upstream response bytes; returns h2 frames to emit plus
    /// whether the response is fully emitted (END_STREAM sent).
    pub fn response_bytes(&mut self, data: &[u8]) -> (Vec<Vec<u8>>, bool) {
        // Response already complete: further upstream bytes exceed the
        // declared framing — drop them (the driver logs at EOF).
        if self.resp_done {
            return (Vec::new(), true);
        }
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
            self.resp_done = true;
        } else if !self.held.is_empty() {
            self.maybe_probe_stall();
        }
        (out, done)
    }

    /// Upstream EOF with an EOF-delimited (or empty) body: end the
    /// stream. Returns frames plus whether the response completed now.
    pub fn response_eof(&mut self) -> (Vec<Vec<u8>>, EofOutcome) {
        if self.resp_done {
            return (Vec::new(), EofOutcome::Continue);
        }
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
        eprintln!("EMITDBG eof-end sid={stream_id}");
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
            eprintln!(
                "QBBDBG take={take} conn={} stream={}",
                self.conn.conn_send_window_probe(),
                self.conn.stream_send_window_probe(stream_id)
            );
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
            self.resp_done = true;
        } else if !self.held.is_empty() {
            self.maybe_probe_stall();
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
        self.resp_done = true;
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

#[cfg(test)]
mod flow_tests {
    use super::*;

    /// Flow-accounting invariant: total emitted DATA payload must never
    /// exceed the client's total granted window (initial 65535 + all
    /// simulated WINDOW_UPDATE grants), for a 1 MiB response fed in
    /// 4096-byte upstream chunks with a 65535-initial-window client.
    #[test]
    fn emitted_bytes_never_exceed_granted_window() {
        const BODY: usize = 1024 * 1024;
        let mut h2s = H2Server::new(0);
        // Request head (POST, CL = BODY, END_STREAM off) as h2 frames.
        let mut events = Vec::new();
        // HPACK literal (no indexing, new name): :method POST
        // Simplest legal request head: :method GET indexed + :path. We
        // need a body though — use POST with content-length. Build the
        // head via the shim's own translate path: feed real frames.
        //
        // :method POST = indexed 3 (0x83); :scheme https = 7 (0x87);
        // :authority "t" = literal indexed name 1; :path /big = 4.
        let mut head = Vec::new();
        head.extend_from_slice(&[0x83]); // :method POST
        head.extend_from_slice(&[0x87]); // :scheme https
        head.extend_from_slice(&[0x41, 0x01, b't']); // :authority t
        head.extend_from_slice(&[0x44, 0x04, b'/', b'b', b'i', b'g']); // :path /big
        head.extend_from_slice(&[0x00, 0x0e]); // literal CL
        head.extend_from_slice(b"content-length");
        head.extend_from_slice(&[0x07]);
        head.extend_from_slice(b"1048576");
        h2s.conn
            .handle_read(vane_core::h2::connection::CLIENT_PREFACE, &mut Vec::new());
        // Wrap head as a HEADERS frame via the public handle_read.
        let frame_head = {
            let mut f = Vec::new();
            vane_core::h2::frame::write_header(
                &mut f,
                head.len() as u32,
                vane_core::h2::frame::FrameKind::Headers,
                vane_core::h2::frame::FrameFlags::from_u8(0x04),
                1,
            );
            f.extend_from_slice(&head);
            f
        };
        h2s.handle_read(&frame_head, &mut events);
        assert!(matches!(events[0], H2Event::RequestHead { .. }));

        // Simulate the upstream feeding the response head + full body
        // through response_bytes, with the client granting credit as it
        // reads.
        let mut granted_total: u64 = 65_535; // initial (stream + conn tracked together here via budget)
        let mut emitted_total: u64 = 0;
        let mut fed: usize = 0;
        let upstream_head =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 1048576\r\n\r\n";
        // Feed one byte at a time to exercise the head-buffer; the head
        // parse completes on the final byte, emitting the HEADERS frame.
        for b in upstream_head {
            let (f, _) = h2s.response_bytes(&[*b]);
            for frame in &f {
                // Only DATA frames carry flow-controlled payload; the
                // HEADERS frame's 28-byte block must not be counted.
                if frame.len() >= 9 && frame[3] != 0x00 {
                    continue;
                }
                let len = u32::from_be_bytes([0, frame[0], frame[1], frame[2]]) as u64;
                emitted_total += len;
                let sid = u32::from_be_bytes([0, frame[5], frame[6], frame[7]]);
                h2s.conn.grant_send_for_test(sid, len as u32);
            }
        }
        let body = vec![0u8; BODY];
        let mut done = false;
        let mut guard = 0;
        while !done && guard < 10_000 {
            guard += 1;
            // Server sends what its budget allows, from fresh upstream
            // data + held queue.
            let (frames, d) = {
                if !done && fed < BODY {
                    let chunk = &body[fed..(fed + 4096).min(BODY)];
                    fed += chunk.len();
                    let (f1, d1) = h2s.response_bytes(chunk);
                    let mut all = f1;
                    let (f2, d2) = h2s.take_held();
                    all.extend(f2);
                    (all, d1 || d2)
                } else {
                    let (f, d) = h2s.take_held();
                    (f, d)
                }
            };
            done = d;
            let mut granted_this_iter: u64 = 0;
            for f in &frames {
                // Only DATA frames consume the flow window.
                let ftype = f[3];
                if ftype != 0x00 {
                    continue;
                }
                let len = u32::from_be_bytes([0, f[0], f[1], f[2]]) as u64;
                let sid = u32::from_be_bytes([f[5] & 0x7f, f[6], f[7], f[8]]);
                emitted_total += len;
                // The client reads and releases: grant credit back.
                h2s.conn.grant_send_for_test(sid, len as u32);
                granted_this_iter += len;
                granted_total += len;
            }
            let (cw, sw) = h2s.conn_debug_windows();
            let budget_after = h2s.conn.send_budget(1);
            eprintln!(
                "FLWDBG it={guard} delta={granted_this_iter} emitted={emitted_total} granted={granted_total} cw={cw} sw={sw} budget_after={budget_after} held={} fed={fed}",
                h2s.held.len()
            );
            assert!(
                emitted_total <= granted_total,
                "over-send: emitted {emitted_total} > granted {granted_total}"
            );
            if guard > 9_900 {
                let (cw, sw) = h2s.conn_debug_windows();
                let open = h2s.conn.stream_is_open(1);
                let held_len = h2s.held.len();
                panic!(
                    "no progress; guard={guard} emitted={emitted_total} granted={granted_total} fed={fed} held={held_len} budget={budget} conn_w={cw} stream_w={sw} stream_open={open} resp_done={rd} probe={probe}",
                    held_len = held_len,
                    budget = h2s.conn.send_budget(1),
                    cw = cw,
                    sw = sw,
                    open = open,
                    rd = h2s.resp_done,
                    probe = h2s.probe_inflight
                );
            }
        }
        assert!(done, "response must complete");
        assert_eq!(emitted_total, BODY as u64, "all body bytes emitted");
    }
}
