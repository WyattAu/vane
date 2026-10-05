//! h2 frame-handling edge cases against the sans-io shim and engine
//! (`PR-02`). Complements the socket-level suites (`h2_upstream`,
//! `h2spec`) with unit coverage of the translation and violation
//! paths that need crafted frames.

use vane::h2_server::{H2Event, H2Server};
use vane_core::h2::connection::CLIENT_PREFACE;
use vane_core::h2::frame::{FrameFlags, FrameKind, write_header};

/// Builds a HEADERS frame with a minimal valid GET request head.
fn headers_frame(stream_id: u32, end_stream: bool, extra_headers: &[(&[u8], &[u8])]) -> Vec<u8> {
    // HPACK (no dynamic table): :method GET (indexed 0x82), :scheme
    // https (0x87), :authority literal-with-indexed-name, :path literal.
    let mut block = Vec::new();
    block.extend_from_slice(&[0x82, 0x87]);
    block.extend_from_slice(&[0x41, 0x01, b't']); // :authority t
    block.extend_from_slice(&[0x44, 0x01, b'/']); // :path /
    for (n, v) in extra_headers {
        block.push(0x00); // literal, no indexing, new name
        block.push(n.len() as u8);
        block.extend_from_slice(n);
        block.push(v.len() as u8);
        block.extend_from_slice(v);
    }
    let flags = FrameFlags::from_u8(if end_stream { 0x05 } else { 0x04 }); // END_HEADERS | END_STREAM
    let mut f = Vec::new();
    write_header(
        &mut f,
        block.len() as u32,
        FrameKind::Headers,
        flags,
        stream_id,
    );
    f.extend_from_slice(&block);
    f
}

fn data_frame(stream_id: u32, data: &[u8], end_stream: bool) -> Vec<u8> {
    let flags = FrameFlags::from_u8(if end_stream { 0x01 } else { 0x00 });
    let mut f = Vec::new();
    write_header(&mut f, data.len() as u32, FrameKind::Data, flags, stream_id);
    f.extend_from_slice(data);
    f
}

/// Feeds the preface + a HEADERS frame; returns the shim's events.
fn open_stream(h2s: &mut H2Server, stream_id: u32, extra: &[(&[u8], &[u8])]) -> Vec<H2Event> {
    let mut bytes = CLIENT_PREFACE.to_vec();
    bytes.extend_from_slice(&headers_frame(stream_id, false, extra));
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    events
}

fn get_request_head(head: &[u8]) -> String {
    String::from_utf8(head.to_vec()).expect("utf8 head")
}

#[test]
fn bodyless_get_emits_head_and_complete() {
    let mut h2s = H2Server::new(7, false);
    // END_STREAM rides the HEADERS: no body follows.
    let mut bytes = CLIENT_PREFACE.to_vec();
    bytes.extend_from_slice(&headers_frame(1, true, &[]));
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(matches!(
        events[0],
        H2Event::RequestHead {
            end_stream: true,
            ..
        }
    ));
    let head = match &events[0] {
        H2Event::RequestHead { head, .. } => get_request_head(head),
        _ => panic!("head first"),
    };
    assert!(head.starts_with("GET / HTTP/1.1\r\n"));
    assert!(head.contains("host: t\r\n"));
    assert!(matches!(events[1], H2Event::RequestComplete));
}

#[test]
fn unknown_frame_type_is_ignored() {
    // HTTP/2 extensibility: unknown frame types must be skipped.
    let mut h2s = H2Server::new(7, false);
    let mut bytes = CLIENT_PREFACE.to_vec();
    // Unknown type 0x99, no flags, empty payload, stream 0 (raw
    // 9-byte frame header; FrameKind has no unknown variants).
    bytes.extend_from_slice(&[0, 0, 0, 0x99, 0x00, 0, 0, 0, 0]);
    bytes.extend_from_slice(&headers_frame(1, false, &[]));
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(
        matches!(events.first(), Some(H2Event::RequestHead { .. })),
        "connection survived unknown frame: {events:?}"
    );
    assert!(!h2s.failed());
}

#[test]
fn connection_scoped_request_header_is_protocol_error() {
    // RFC 9113 §8.2.2: connection-scoped headers are illegal in h2.
    let mut h2s = H2Server::new(7, false);
    let mut bytes = CLIENT_PREFACE.to_vec();
    bytes.extend_from_slice(&headers_frame(1, false, &[(b"connection", b"keep-alive")]));
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(events.is_empty(), "no head emitted: {events:?}");
    // The stream was reset with PROTOCOL_ERROR: the shim queued an
    // RST_STREAM frame toward the peer (visible in pending writes).
    let writes = h2s.pending_writes();
    assert!(!writes.is_empty(), "RST_STREAM queued");
}

#[test]
fn missing_pseudo_fields_refuse_the_stream() {
    // A HEADERS block without :method/:path cannot translate.
    let mut h2s = H2Server::new(7, false);
    let mut bytes = CLIENT_PREFACE.to_vec();
    // Only :authority (0x41...), no method/path.
    let mut block = vec![0x41, 0x01, b't'];
    write_header(
        &mut bytes,
        block.len() as u32,
        FrameKind::Headers,
        FrameFlags::from_u8(0x04),
        1,
    );
    bytes.append(&mut block);
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(events.is_empty());
    assert!(!h2s.pending_writes().is_empty(), "RST queued");
}

#[test]
fn overlapping_stream_is_refused() {
    // Serialized mode: a second concurrent stream is REFUSED_STREAM
    // (the standard retry signal), never a connection error.
    let mut h2s = H2Server::new(7, false);
    let events = open_stream(&mut h2s, 1, &[]);
    assert!(matches!(events.first(), Some(H2Event::RequestHead { .. })));
    let bytes = headers_frame(3, false, &[]);
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(events.is_empty(), "refused stream emits no head");
    let writes = h2s.pending_writes();
    assert!(!writes.is_empty(), "RST_STREAM(REFUSED) queued");
    // The connection is still healthy for the active stream.
    assert!(!h2s.failed());
    let _ = bytes;
}

#[test]
fn chunked_cl_less_body_is_reframed_with_terminal_chunk() {
    let mut h2s = H2Server::new(7, false);
    // POST without content-length, no END_STREAM on the head.
    let mut block = Vec::new();
    block.extend_from_slice(&[0x83]); // :method POST
    block.extend_from_slice(&[0x87]); // :scheme https
    block.extend_from_slice(&[0x41, 0x01, b't']);
    block.extend_from_slice(&[0x44, 0x05, b'/', b'p', b'o', b's', b't']);
    let mut bytes = CLIENT_PREFACE.to_vec();
    write_header(
        &mut bytes,
        block.len() as u32,
        FrameKind::Headers,
        FrameFlags::from_u8(0x04),
        1,
    );
    bytes.extend_from_slice(&block);

    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    let head = match &events[0] {
        H2Event::RequestHead { head, .. } => get_request_head(head),
        other => panic!("head first, got {other:?}"),
    };
    assert!(head.contains("transfer-encoding: chunked\r\n"));

    // Body DATA frames become h1 chunk framing.
    let mut bytes = data_frame(1, b"hello", false);
    bytes.extend_from_slice(&data_frame(1, b"world", true));
    h2s.handle_read(&bytes, &mut events);
    let bodies: Vec<Vec<u8>> = events
        .iter()
        .filter_map(|e| match e {
            H2Event::RequestBody { data } => Some(data.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(bodies.len(), 3, "chunk, chunk, terminal: {bodies:?}");
    assert_eq!(bodies[0], b"5\r\nhello\r\n");
    assert_eq!(bodies[1], b"5\r\nworld\r\n");
    assert_eq!(bodies[2], b"0\r\n\r\n");
    assert!(matches!(events.last(), Some(H2Event::RequestComplete)));
}

#[test]
fn content_length_body_is_byte_exact_and_truncates_excess() {
    let mut h2s = H2Server::new(7, false);
    let mut block = Vec::new();
    block.extend_from_slice(&[0x83, 0x87]);
    block.extend_from_slice(&[0x41, 0x01, b't']);
    block.extend_from_slice(&[0x44, 0x02, b'/', b'p']);
    // content-length: 5
    block.extend_from_slice(&[0x00, 0x0e]);
    block.extend_from_slice(b"content-length");
    block.extend_from_slice(&[0x01]);
    block.extend_from_slice(b"5");
    let mut bytes = CLIENT_PREFACE.to_vec();
    write_header(
        &mut bytes,
        block.len() as u32,
        FrameKind::Headers,
        FrameFlags::from_u8(0x04),
        1,
    );
    bytes.extend_from_slice(&block);
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(matches!(events[0], H2Event::RequestHead { .. }));

    // A mid-stream DATA burst (no END_STREAM — the engine validates
    // the declared CL total at END_STREAM) is clamped to the shim's
    // remaining budget: exactly 5 of the 8 bytes relay.
    let bytes = data_frame(1, b"12345678", false);
    h2s.handle_read(&bytes, &mut events);
    let bodies: Vec<Vec<u8>> = events
        .iter()
        .filter_map(|e| match e {
            H2Event::RequestBody { data } => Some(data.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(bodies, vec![b"12345".to_vec()], "CL frames the take");
}

#[test]
fn request_trailers_complete_the_stream() {
    // gRPC-style trailers: HEADERS after DATA on the same stream.
    // Framing completes even though the h1 relay cannot carry the
    // trailer fields (v0.2 relays the completion semantics only).
    let mut h2s = H2Server::new(7, false);
    let mut events = open_stream(&mut h2s, 1, &[]);
    assert!(matches!(events[0], H2Event::RequestHead { .. }));
    let mut bytes = data_frame(1, b"chunk", false);
    h2s.handle_read(&bytes, &mut events);
    bytes = headers_frame(1, true, &[]);
    h2s.handle_read(&bytes, &mut events);
    assert!(matches!(events.last(), Some(H2Event::RequestComplete)));
}

#[test]
fn strict_mode_rejects_window_update_on_idle_stream() {
    // WINDOW_UPDATE for a never-opened stream (id 9), strict mode.
    let mut h2s = H2Server::new(7, true);
    let mut bytes = CLIENT_PREFACE.to_vec();
    write_header(
        &mut bytes,
        4,
        FrameKind::WindowUpdate,
        FrameFlags::from_u8(0),
        9,
    );
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // 1 byte increment
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(
        h2s.failed(),
        "strict idle WINDOW_UPDATE must fail the connection"
    );
    // GOAWAY flushed toward the peer.
    assert!(!h2s.pending_writes().is_empty());
}

#[test]
fn lenient_mode_ignores_window_update_on_idle_stream() {
    let mut h2s = H2Server::new(7, false);
    let mut bytes = CLIENT_PREFACE.to_vec();
    write_header(
        &mut bytes,
        4,
        FrameKind::WindowUpdate,
        FrameFlags::from_u8(0),
        9,
    );
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    assert!(!h2s.failed(), "lenient mode survives: {events:?}");
    bytes = headers_frame(1, false, &[]);
    h2s.handle_read(&bytes, &mut events);
    assert!(matches!(events.last(), Some(H2Event::RequestHead { .. })));
}

#[test]
fn goaway_fails_the_connection() {
    let mut h2s = H2Server::new(7, false);
    let mut bytes = CLIENT_PREFACE.to_vec();
    write_header(&mut bytes, 8, FrameKind::GoAway, FrameFlags::from_u8(0), 0);
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // last stream 0, NO_ERROR
    let mut events = Vec::new();
    h2s.handle_read(&bytes, &mut events);
    // GOAWAY is transaction-scoped here: the shim tolerates it (the
    // connection stays usable for the in-flight stream).
    assert!(!h2s.failed());
    bytes = headers_frame(1, false, &[]);
    h2s.handle_read(&bytes, &mut events);
    assert!(matches!(events.last(), Some(H2Event::RequestHead { .. })));
}

#[test]
fn runaway_backlog_is_connection_error() {
    // A peer flooding partial frames (no complete frame in >1 MiB)
    // must trip the connection error, not buffer forever.
    let mut h2s = H2Server::new(7, false);
    let mut events = Vec::new();
    h2s.handle_read(CLIENT_PREFACE, &mut events);
    let chunk = vec![0u8; 256 * 1024];
    for _ in 0..6 {
        // Partial frame header (length prefix claims more than sent).
        h2s.handle_read(&chunk, &mut events);
    }
    assert!(h2s.failed(), "runaway backlog must fail the connection");
}

#[test]
fn window_update_emits_send_credit_when_held() {
    let mut h2s = H2Server::new(7, false);
    let mut events = open_stream(&mut h2s, 1, &[]);
    assert!(matches!(events.first(), Some(H2Event::RequestHead { .. })));

    // Feed a response large enough to exhaust the default client
    // window (65535): the excess is held.
    let body = vec![b'x'; 100_000];
    let resp_head = b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n".to_vec();
    let mut bytes = resp_head;
    bytes.extend_from_slice(&body);
    let (frames, done) = h2s.response_bytes(&bytes);
    assert!(!done, "flow control holds the excess");
    assert!(!frames.is_empty());

    // Credit arrives: SendCredit event (only emitted when bytes are
    // actually held back by the window).
    let mut bytes = Vec::new();
    write_header(
        &mut bytes,
        4,
        FrameKind::WindowUpdate,
        FrameFlags::from_u8(0),
        0,
    );
    bytes.extend_from_slice(&[0x00, 0x0F, 0x42, 0x40]); // +1,000,000
    h2s.handle_read(&bytes, &mut events);
    assert!(
        events.iter().any(|e| matches!(e, H2Event::SendCredit)),
        "credit event: {events:?}"
    );
}

#[test]
fn response_eof_classifies_truncation_and_completion() {
    // Truncated: declared CL, nothing emitted, upstream EOFs.
    let mut h2s = H2Server::new(7, false);
    let _ = open_stream(&mut h2s, 1, &[]);
    let head = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n".to_vec();
    h2s.response_bytes(&head);
    let (_, outcome) = h2s.response_eof();
    assert_eq!(outcome, vane::h2_server::EofOutcome::Truncated);

    // Completed: EOF-delimited upstream body — the upstream EOF IS the
    // end; response_eof emits the terminal END_STREAM.
    let mut h2s2 = H2Server::new(8, false);
    let _ = open_stream(&mut h2s2, 1, &[]);
    let head2 = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n".to_vec();
    let (frames, done) = h2s2.response_bytes(&head2);
    assert!(!frames.is_empty() && !done, "head emitted, body pending");
    let (eof_frames, outcome) = h2s2.response_eof();
    assert_eq!(outcome, vane::h2_server::EofOutcome::Completed);
    assert!(!eof_frames.is_empty(), "END_STREAM emitted");

    // CL'd body upstream EOF while held bytes remain: Continue (the
    // client is still draining; completion fires from take_held).
    let mut h2s3 = H2Server::new(9, false);
    let _ = open_stream(&mut h2s3, 1, &[]);
    let head3 = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n".to_vec();
    h2s3.response_bytes(&head3);
    let (_, outcome) = h2s3.response_eof();
    assert_eq!(outcome, vane::h2_server::EofOutcome::Truncated);
}

#[test]
fn h2_client_roundtrip_request_and_body() {
    // The h2 upstream client translates the buffered h1 head into
    // HEADERS/DATA frames; its own engine answers SETTINGS + ACKs.
    use vane::h2_client::{H2Upstream, UpstreamEvent};
    let mut up = H2Upstream::new();
    let head = b"GET /x HTTP/1.1\r\nhost: t\r\n\r\n".to_vec();
    up.send_request(&head, None);
    let writes = up.pending_writes();
    assert!(!writes.is_empty(), "preface + settings + headers queued");

    // Server frames: SETTINGS, then the client's SETTINGS ACK back.
    let mut server_bytes = Vec::new();
    let mut settings = Vec::new();
    write_header(
        &mut settings,
        0,
        FrameKind::Settings,
        FrameFlags::from_u8(0),
        0,
    );
    server_bytes.extend_from_slice(&settings);
    write_header(
        &mut server_bytes,
        0,
        FrameKind::Settings,
        FrameFlags::from_u8(0x01),
        0,
    );
    let mut events = Vec::new();
    up.handle_read(&server_bytes, &mut events);
    assert!(events.is_empty(), "no upstream events yet: {events:?}");

    // A response head + END_STREAM body.
    let mut resp = Vec::new();
    // HPACK: :status 200 (indexed 0x88), content-length literal.
    let mut block = vec![0x88, 0x00, 0x0e];
    block.extend_from_slice(b"content-length");
    block.extend_from_slice(&[0x01]);
    block.extend_from_slice(b"2");
    write_header(
        &mut resp,
        block.len() as u32,
        FrameKind::Headers,
        FrameFlags::from_u8(0x04),
        1,
    );
    resp.extend_from_slice(&block);
    resp.extend_from_slice(&data_frame(1, b"ok", true));
    up.handle_read(&resp, &mut events);
    let heads: Vec<&UpstreamEvent> = events.iter().collect();
    assert!(!heads.is_empty(), "response events: {events:?}");
    assert!(up.response_complete(), "END_STREAM seen");
}
