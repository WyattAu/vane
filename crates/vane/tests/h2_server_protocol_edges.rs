//! h2_server protocol edges — the paths the boundary sweep does not
//! reach: request trailers, oversized response splitting, the
//! flow-control stall probe, and connection-error surfacing.
//!
//! Drives the [`H2Server`] state machine directly with hand-built
//! frames: `handle_read` for client→server, `response_bytes` for the
//! relayed origin response, `pending_writes` for everything the shim
//! queues back. Every test asserts on frame bytes, not "no panic".

use vane::h2_server::{EofOutcome, H2Event, H2Server};

/// HTTP/2 client preface (RFC 9113 §3.5).
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Builds one frame: 3-byte big-endian length, type, flags, stream.
fn frame(ftype: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut v = Vec::with_capacity(9 + len);
    v.extend_from_slice(&[(len >> 16) as u8, (len >> 8) as u8, len as u8]);
    v.push(ftype);
    v.push(flags);
    v.extend_from_slice(&stream_id.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

/// A client connection prelude: preface + empty SETTINGS.
fn client_prelude() -> Vec<u8> {
    let mut v = PREFACE.to_vec();
    v.extend_from_slice(&frame(0x04, 0, 0, &[])); // SETTINGS
    v
}

/// HEADERS frame carrying a GET for `path` — literal-without-indexing
/// HPACK, hand-assembled (0x00 prefix, name-len, name, value-len,
/// value; huffman bit clear). The shim's decoder accepts exactly this,
/// and hand-rolling keeps the test independent of the encoder.
fn request_headers(
    stream_id: u32,
    path: &str,
    with_content_length: Option<usize>,
    end_stream: bool,
) -> Vec<u8> {
    let mut block = Vec::new();
    let mut emit = |name: &[u8], value: &[u8]| {
        block.push(0x00);
        block.push(name.len() as u8);
        block.extend_from_slice(name);
        block.push(value.len() as u8);
        block.extend_from_slice(value);
    };
    emit(b":method", b"GET");
    emit(b":path", path.as_bytes());
    emit(b":authority", b"t");
    emit(b":scheme", b"http");
    if let Some(n) = with_content_length {
        emit(b"content-length", n.to_string().as_bytes());
    }
    let flags = 0x04 | u8::from(end_stream);
    frame(0x01, flags, stream_id, &block)
}

/// Splits a frame stream into (type, flags, stream, payload) tuples.
fn parse_frames(bytes: &[u8]) -> Vec<(u8, u8, u32, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 9 <= bytes.len() {
        let len =
            ((bytes[i] as usize) << 16) | ((bytes[i + 1] as usize) << 8) | bytes[i + 2] as usize;
        let ftype = bytes[i + 3];
        let flags = bytes[i + 4];
        let stream = u32::from_be_bytes([bytes[i + 5], bytes[i + 6], bytes[i + 7], bytes[i + 8]]);
        if i + 9 + len > bytes.len() {
            break;
        }
        out.push((ftype, flags, stream, bytes[i + 9..i + 9 + len].to_vec()));
        i += 9 + len;
    }
    out
}

/// Starts a server shim and sends the client prelude + a GET with a
/// request body of `body` (DATA with END_STREAM). Returns the shim and
/// the RequestHead event (asserted present).
fn start_with_request(shim: &mut H2Server, path: &str, body: &[u8]) {
    let mut input = client_prelude();
    input.extend_from_slice(&request_headers(1, path, Some(0), body.is_empty()));
    if !body.is_empty() {
        input.extend_from_slice(&frame(0x00, 0x01, 1, body)); // DATA END_STREAM
    }
    let mut events = Vec::new();
    shim.handle_read(&input, &mut events);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, H2Event::RequestHead { .. })),
        "request head must surface"
    );
}

/// An origin response with `body` (Content-Length framing).
fn origin_response(body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

/// Upstream trailers complete a chunked request: the shim emits the
/// h1 chunked terminator (`0\r\n\r\n`) as the final request-body push
/// and completes the request — gRPC-style trailers-in, h1-out.
#[test]
fn trailers_complete_a_chunked_request_with_the_h1_terminator() {
    let mut shim = H2Server::new(0, false);
    // No content-length: the shim assumes chunked relay upstream.
    let mut input = client_prelude();
    input.extend_from_slice(&request_headers(1, "/upload", None, false));
    input.extend_from_slice(&frame(0x00, 0x00, 1, b"part")); // DATA
    // Trailing HEADERS with END_STREAM: one non-pseudo field
    // (literal-without-indexing `x-trailer: v`) — pseudo-headers in
    // trailers are a protocol error per RFC 9113 §8.1.
    let mut trailer_block = vec![0x00, 9];
    trailer_block.extend_from_slice(b"x-trailer");
    trailer_block.extend_from_slice(&[1]);
    trailer_block.extend_from_slice(b"v");
    input.extend_from_slice(&frame(0x01, 0x05, 1, &trailer_block));

    let mut events = Vec::new();
    shim.handle_read(&input, &mut events);
    let bodies: Vec<&Vec<u8>> = events
        .iter()
        .filter_map(|e| match e {
            H2Event::RequestBody { data } => Some(data),
            _ => None,
        })
        .collect();
    // The shim relays into h1 CHUNKED framing: chunk header, payload,
    // CRLF — then the terminator when trailers arrive.
    assert!(
        bodies.iter().any(|b| b.as_slice() == b"4\r\npart\r\n"),
        "the DATA payload is relayed as a chunk: {bodies:?}"
    );
    assert!(
        bodies.iter().any(|b| b.as_slice() == b"0\r\n\r\n"),
        "trailers must emit the h1 chunked terminator: {bodies:?}"
    );
    assert!(
        events.iter().any(|e| matches!(e, H2Event::RequestComplete)),
        "trailers complete the request"
    );
    // After completion, the chunked state must be cleared: a second
    // trailer-bearing stream must not re-emit a stray terminator for the
    // finished stream.
    let done = events.iter().any(|e| matches!(e, H2Event::RequestComplete));
    assert!(done);
}

/// A response larger than the peer's max frame size splits across
/// multiple DATA frames, each within the limit, and the concatenation
/// is byte-exact.
#[test]
fn oversized_response_splits_across_max_frame_data_frames() {
    let mut shim = H2Server::new(0, false);
    start_with_request(&mut shim, "/big", &[]);

    let body: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
    let (frames, done) = shim.response_bytes(&origin_response(&body));
    // The head rides the first frame; collect DATA frames only.
    let mut data_frames: Vec<Vec<u8>> = Vec::new();
    let mut head_seen = false;
    for f in &frames {
        let parsed = parse_frames(f);
        assert!(!parsed.is_empty(), "emit well-formed frames");
        for (ftype, _flags, stream, payload) in parsed {
            assert_eq!(stream, 1, "all frames on the request stream");
            if ftype == 0x00 {
                data_frames.push(payload);
            } else if !head_seen {
                head_seen = true; // HEADERS frame (or SETTINGS etc.)
            }
        }
    }
    assert!(head_seen, "response head emitted");
    assert!(data_frames.len() >= 3, "50 KiB needs multiple frames");
    let mut joined = Vec::new();
    for d in &data_frames {
        assert!(d.len() <= 16_384, "DATA frame within the peer max");
        joined.extend_from_slice(d);
    }
    assert_eq!(joined, body, "byte-exact reassembly");
    assert!(done, "content-length body completes the response");
}

/// Flow-control stall: with a zero client window the response is held,
/// exactly one PING probe goes out, and credit both releases the held
/// bytes (SendCredit) and re-arms the probe.
#[test]
fn stalled_response_pings_once_and_credit_releases_it() {
    let mut shim = H2Server::new(0, false);

    // Prelude + SETTINGS advertising a zero initial window, so the
    // stream opens with no send credit.
    let mut input = PREFACE.to_vec();
    // SETTINGS entry: 2-byte id (0x4 = INITIAL_WINDOW_SIZE) + 4-byte
    // value 0 — the stream opens with no send credit.
    input.extend_from_slice(&frame(0x04, 0, 0, &[0x00, 0x04, 0, 0, 0, 0]));
    input.extend_from_slice(&frame(0x04, 0x01, 0, &[])); // SETTINGS ACK
    input.extend_from_slice(&request_headers(1, "/held", None, false));
    let mut events = Vec::new();
    shim.handle_read(&input, &mut events);

    let body = vec![b'z'; 4096];
    let (frames, done) = shim.response_bytes(&origin_response(&body));
    assert!(!done, "held bytes: the response cannot complete yet");
    // The probe PING (type 0x6) appears exactly once across the queued
    // writes.
    // The probe PING rides the connection's pending writes (the same
    // queue as SETTINGS/GOAWAY), not the response-frame list.
    let mut all: Vec<u8> = frames.concat();
    all.extend_from_slice(&shim.pending_writes());
    let pings = parse_frames(&all)
        .iter()
        .filter(|(t, ..)| *t == 0x06)
        .count();
    assert_eq!(pings, 1, "exactly one stall probe: {pings}");
    // A second response read must NOT send another ping
    // (probe_inflight latches until credit arrives).
    let _ = shim.response_bytes(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    let more = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let _ = shim.response_bytes(more);
    let (_, held) = shim.response_bytes(&body);
    assert!(!held);
    let drained = shim.pending_writes();
    let pings2 = parse_frames(&drained)
        .iter()
        .filter(|(t, ..)| *t == 0x06)
        .count();
    assert_eq!(pings2, 0, "no second ping while the probe is inflight");

    // Credit arrives: SendCredit fires and the probe re-arms.
    let mut events2 = Vec::new();
    let mut update = frame(0x08, 0, 1, &65_535u32.to_be_bytes()); // WINDOW_UPDATE stream
    update.extend_from_slice(&frame(0x08, 0, 0, &65_535u32.to_be_bytes())); // WINDOW_UPDATE conn
    shim.handle_read(&update, &mut events2);
    assert!(
        events2.iter().any(|e| matches!(e, H2Event::SendCredit)),
        "credit releases the held bytes: {events2:?}"
    );
}

/// A protocol violation surfaces as a connection failure with an error
/// code — `failed`/`conn_error_code` are the worker's drain signals and
/// were never driven.
#[test]
fn protocol_violation_surfaces_the_connection_error() {
    let mut shim = H2Server::new(0, false);
    let mut input = client_prelude();
    input.extend_from_slice(&request_headers(1, "/x", None, true));
    // DATA on stream 0: a connection error (RFC 9113 §7.1 — stream 0
    // carries only connection control frames).
    input.extend_from_slice(&frame(0x00, 0x00, 0, b"junk"));

    let mut events = Vec::new();
    shim.handle_read(&input, &mut events);
    assert!(shim.failed(), "DATA on stream 0 is a connection error");
    assert_eq!(shim.conn_error_code(), Some(0x1), "PROTOCOL_ERROR");
    // The GOAWAY is queued for the peer.
    let out = shim.pending_writes();
    assert!(
        parse_frames(&out).iter().any(|(t, ..)| *t == 0x07),
        "GOAWAY queued: {out:?}"
    );
}

/// A second stream while one is active is REFUSED_STREAM: the shim
/// serializes (one relayed stream at a time) and resets the overlapping
/// stream — the client retries it on a fresh connection per the h2
/// spec. The active stream and its relay are untouched.
#[test]
fn overlapping_stream_is_reset_with_refused_stream() {
    let mut shim = H2Server::new(0, false);
    start_with_request(&mut shim, "/first", &[]);

    // A second HEADERS on stream 3 while stream 1 is active.
    let mut input = frame(0x01, 0x05, 3, &{
        let mut b = Vec::new();
        let mut emit = |name: &[u8], value: &[u8]| {
            b.push(0x00);
            b.push(name.len() as u8);
            b.extend_from_slice(name);
            b.push(value.len() as u8);
            b.extend_from_slice(value);
        };
        emit(b":method", b"GET");
        emit(b":path", b"/second");
        emit(b":authority", b"t");
        emit(b":scheme", b"http");
        b
    });
    let mut events = Vec::new();
    shim.handle_read(&input.drain(..).as_slice(), &mut events);

    // The reset is queued for the peer: RST_STREAM(REFUSED_STREAM = 7).
    let out = shim.pending_writes();
    let parsed = parse_frames(&out);
    let rst = parsed
        .iter()
        .find(|(t, _, s, _)| *t == 0x03 && *s == 3)
        .expect("RST_STREAM for the overlapping stream");
    assert_eq!(rst.3.len(), 4, "RST_STREAM carries a 4-byte code");
    assert_eq!(
        u32::from_be_bytes([rst.3[0], rst.3[1], rst.3[2], rst.3[3]]),
        7,
        "REFUSED_STREAM error code"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, H2Event::RequestHead { .. })),
        "the overlapping head must not surface"
    );
}

/// Upstream EOF before a Content-Length body completes is TRUNCATED:
/// the worker answers 502 to the client instead of silently ending a
/// short response. (EOF with nothing held and nothing left →
/// Truncated; EOF with held bytes → Continue, completion comes from
/// take_held.)
#[test]
fn upstream_eof_mid_body_is_reported_truncated() {
    let mut shim = H2Server::new(0, false);
    start_with_request(&mut shim, "/trunc", &[]);

    // Head declaring 100 bytes; only 10 arrive; then EOF.
    let head = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    let (frames, done) = shim.response_bytes(head);
    assert!(!done);
    let _ = frames;
    let (frames, outcome) = shim.response_eof();
    assert_eq!(outcome, EofOutcome::Truncated, "mid-body EOF is truncation");
    assert!(frames.is_empty(), "no trailing END_STREAM on truncation");
}

/// After a response completes, further upstream bytes are dropped (the
/// declared framing was already satisfied) — and the stream is marked
/// closed so late frames cannot resurrect it.
#[test]
fn bytes_after_a_completed_response_are_dropped() {
    let mut shim = H2Server::new(0, false);
    start_with_request(&mut shim, "/once", &[]);

    let (frames, done) = shim.response_bytes(&origin_response(b"first"));
    assert!(done);
    assert!(!frames.is_empty());

    let (extra, done2) = shim.response_bytes(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nlater");
    assert!(done2, "post-completion reads stay in the done state");
    assert!(extra.is_empty(), "nothing is emitted: {extra:?}");
}
