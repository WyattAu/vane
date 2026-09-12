//! HTTP/2 connection state machine (RFC 9113 §4, §5, §6) — sans-io.
//!
//! The [`Connection`] consumes inbound bytes and produces outbound
//! bytes + [`Event`]s; the engine driver owns the socket and forwards
//! between them. No I/O happens here, which keeps the state machines
//! fuzzable and runtime-agnostic (they run inside vane-core workers
//! without a tokio dependency).
//!
//! Covers: client connection preface, SETTINGS negotiation + ACK,
//! stream lifecycle (idle → open → half-closed → closed), stream-id
//! direction rules, HEADERS + CONTINUATION assembly with HPACK, DATA
//! relay with connection/stream flow-control accounting, WINDOW_UPDATE
//! release, PING/PONG, GOAWAY draining, RST_STREAM, unknown-frame
//! skipping, and uppercase field-name rejection (RFC 9113 §8.2.1).

use std::collections::HashMap;

use super::frame::{
    DEFAULT_MAX_FRAME_SIZE, FrameFlags, FrameHeader, FrameKind, Setting, parse_header,
    parse_rst_stream, parse_settings, parse_window_update, validate_payload, write_header,
    write_setting,
};
use super::hpack::{HpackDecoder, HpackEncoder, HpackError};

/// The HTTP/2 client connection preface (RFC 9113 §3.5).
pub const CLIENT_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// HTTP/2 error codes (RFC 9113 §7).
pub mod error_code {
    /// No error.
    pub const NO_ERROR: u32 = 0x0;
    /// Protocol detected error.
    pub const PROTOCOL_ERROR: u32 = 0x1;
    /// Implementation fault.
    pub const INTERNAL_ERROR: u32 = 0x2;
    /// Peer violated flow control.
    pub const FLOW_CONTROL_ERROR: u32 = 0x3;
    /// Frame on a closed stream.
    pub const STREAM_CLOSED: u32 = 0x5;
    /// Frame size invalid.
    pub const FRAME_SIZE_ERROR: u32 = 0x6;
    /// Stream refused (load shedding).
    pub const REFUSED_STREAM: u32 = 0x7;
    /// Stream cancelled by the receiver.
    pub const CANCEL: u32 = 0x8;
    /// HPACK state corrupt.
    pub const COMPRESSION_ERROR: u32 = 0x9;
}

/// Which side of the connection this state machine represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Receives odd stream ids, allocates even (servers).
    Server,
    /// Allocates odd stream ids, receives even (clients; server push
    /// is refused).
    Client,
}

/// Per-stream lifecycle state (RFC 9113 §5.1).
/// Per-stream lifecycle state (subset of RFC 9113 §5.1 states tracked
/// by this side of the connection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    /// HEADERS received (server) / sent (client), awaiting END_STREAM
    /// handling; relay in both directions.
    Open,
    /// We sent END_STREAM: no more request-side data; response may flow.
    HalfClosedLocal,
    /// Peer sent END_STREAM: request complete; we may still respond.
    HalfClosedRemote,
    /// RST_STREAM sent or received, or END_STREAM both ways.
    Closed,
}

/// An assembled request/response body chunk or complete set of headers.
#[derive(Debug)]
pub enum Event {
    /// Complete header block decoded for a stream.
    Headers {
        /// Stream the block belongs to.
        stream_id: u32,
        /// Peer half-closed with this block.
        end_stream: bool,
        /// Decoded header fields.
        headers: Vec<super::hpack::Header>,
    },
    /// Body bytes (flow-control consumed — call
    /// [`Connection::release_capacity`] to credit the peer).
    Data {
        /// Stream the data belongs to.
        stream_id: u32,
        /// Peer half-closed with this chunk.
        end_stream: bool,
        /// Body bytes.
        data: Vec<u8>,
    },
    /// Peer reset the stream.
    Reset {
        /// Reset stream.
        stream_id: u32,
        /// Error code from the peer.
        error_code: u32,
    },
    /// Peer is going away; streams above `last_stream_id` will not be
    /// processed by the peer.
    GoAway {
        /// Last stream id the peer will process.
        last_stream_id: u32,
        /// Error code from the peer.
        error_code: u32,
    },
    /// Peer ACKed our SETTINGS.
    SettingsAck,
    /// Peer granted send credit (a WINDOW_UPDATE frame, or a SETTINGS
    /// INITIAL_WINDOW_SIZE increase for `stream_id` 0). Drivers holding
    /// back response bytes on flow control should retry pending sends.
    /// `stream_id` 0 is connection-level credit (all streams may retry).
    WindowUpdate {
        /// Stream credited, or 0 for connection-level credit.
        stream_id: u32,
    },
}

/// Connection-level failure: the driver must send a GOAWAY with this
/// code and close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionError {
    /// HTTP/2 error code for the GOAWAY frame.
    pub code: u32,
}

/// Negotiated connection parameters.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Our SETTINGS_MAX_CONCURRENT_STREAMS (peer's opens are refused
    /// above this — we RST with REFUSED_STREAM).
    pub max_concurrent_streams: u32,
    /// Our SETTINGS_INITIAL_WINDOW_SIZE (per-stream recv credit).
    pub initial_window_size: u32,
    /// Our SETTINGS_MAX_FRAME_SIZE (largest frame we accept).
    pub max_frame_size: u32,
    /// Our SETTINGS_HEADER_TABLE_SIZE.
    pub header_table_size: u32,
    /// Largest header list we accept (approximate, bytes).
    pub max_header_list_size: u32,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 256,
            initial_window_size: 512 * 1024,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            header_table_size: 4096,
            max_header_list_size: 128 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Stream {
    state: StreamState,
    recv_window: i64,
    send_window: i64,
    /// Our send side ended with END_STREAM.
    sent_end: bool,
}

/// Assembles a HEADERS block across CONTINUATION frames.
#[derive(Debug, Default)]
struct HeaderBlockAssembler {
    fragments: Vec<u8>,
    stream_id: u32,
    end_stream: bool,
    active: bool,
}

/// HTTP/2 connection state machine (sans-io).
pub struct Connection {
    role: Role,
    cfg: ConnectionConfig,
    encoder: HpackEncoder,
    decoder: HpackDecoder,

    /// Bytes we owe the peer (frames written by this state machine).
    out: Vec<u8>,

    /// Client preface consumption progress (server role only).
    preface_pos: usize,
    preface_done: bool,

    /// Our SETTINGS awaiting peer ACK.
    settings_acked: bool,

    /// Peer's SETTINGS_MAX_FRAME_SIZE (caps frames we send).
    peer_max_frame: u32,
    /// Peer's SETTINGS_INITIAL_WINDOW_SIZE (per-stream send credit).
    peer_initial_window: i64,

    conn_recv_window: i64,
    conn_send_window: i64,

    next_stream_id: u32,
    last_peer_stream_id: u32,

    streams: HashMap<u32, Stream>,

    header_block: HeaderBlockAssembler,

    /// Set when a connection error occurred; out already carries the
    /// GOAWAY. The driver closes after flushing.
    connection_error: Option<ConnectionError>,
    goaway_sent: bool,
    /// Peer GOAWAY received.
    peer_goaway: Option<(u32, u32)>,
}

impl Connection {
    /// New connection. Emits the client preface + initial SETTINGS
    /// (client role) or the server's initial SETTINGS (server role)
    /// into the pending-write buffer.
    #[must_use]
    pub fn new(role: Role, cfg: ConnectionConfig) -> Self {
        let mut conn = Self {
            role,
            cfg: cfg.clone(),
            encoder: HpackEncoder::new(),
            decoder: HpackDecoder::new(cfg.header_table_size),
            out: Vec::new(),
            preface_pos: 0,
            preface_done: role == Role::Client,
            settings_acked: false,
            peer_max_frame: DEFAULT_MAX_FRAME_SIZE,
            peer_initial_window: 65_535,
            // Connection-level receive window: fixed 65535 initial
            // (RFC 9113 §6.9.1); SETTINGS only raises stream windows.
            conn_recv_window: 65_535,
            conn_send_window: 65_535,
            next_stream_id: match role {
                Role::Server => 2,
                Role::Client => 1,
            },
            last_peer_stream_id: 0,
            streams: HashMap::new(),
            header_block: HeaderBlockAssembler::default(),
            connection_error: None,
            goaway_sent: false,
            peer_goaway: None,
        };

        match role {
            Role::Client => {
                conn.out.extend_from_slice(CLIENT_PREFACE);
            }
            Role::Server => {}
        }
        conn.write_initial_settings();
        // Initial SETTINGS are in flight until the peer ACKs them.
        conn.settings_acked = true;
        conn
    }

    fn write_initial_settings(&mut self) {
        let hdr_pos = self.out.len();
        write_header(&mut self.out, 0, FrameKind::Settings, FrameFlags::EMPTY, 0);
        let mut payload = Vec::new();
        write_setting(&mut payload, 0x3, self.cfg.max_concurrent_streams);
        write_setting(&mut payload, 0x4, self.cfg.initial_window_size);
        write_setting(&mut payload, 0x5, self.cfg.max_frame_size);
        write_setting(&mut payload, 0x6, self.cfg.max_header_list_size);
        write_setting(&mut payload, 0x1, self.cfg.header_table_size);
        // Patch the length.
        let len = payload.len() as u32;
        self.out[hdr_pos] = (len >> 16) as u8;
        self.out[hdr_pos + 1] = (len >> 8) as u8;
        self.out[hdr_pos + 2] = len as u8;
        self.out.extend_from_slice(&payload);
    }

    /// Bytes the driver must flush to the peer.
    #[must_use]
    pub fn take_pending_writes(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.out)
    }

    /// Connection error, if any. The driver sends the GOAWAY (already
    /// queued in pending writes) and closes.
    #[must_use]
    pub fn connection_error(&self) -> Option<ConnectionError> {
        self.connection_error
    }

    /// The peer's SETTINGS_MAX_FRAME_SIZE (largest frame payload we
    /// may send).
    #[must_use]
    pub fn peer_max_frame_size(&self) -> usize {
        self.peer_max_frame as usize
    }

    /// Available send credit for `stream_id` (min of stream and
    /// connection windows, clamped at zero).
    #[must_use]
    pub fn send_budget(&self, stream_id: u32) -> usize {
        let stream = self
            .streams
            .get(&stream_id)
            .map_or(i64::MAX, |st| st.send_window);
        self.conn_send_window
            .min(stream)
            .max(0)
            .try_into()
            .unwrap_or(0)
    }

    /// Whether the stream is currently tracked (open or half-closed).
    #[must_use]
    pub fn stream_is_open(&self, stream_id: u32) -> bool {
        self.streams.contains_key(&stream_id)
    }

    /// Reserves `n` bytes of send credit for DATA the driver emits
    /// directly (frame bytes assembled outside the engine). The engine
    /// never sees those frames through [`Self::send_data`], so the
    /// stream and connection windows must be debited here; peer
    /// WINDOW_UPDATEs replenish via [`Self::handle_read`].
    /// Returns the amount actually reserved (≤ n).
    pub fn consume_send_budget(&mut self, stream_id: u32, n: usize) -> usize {
        let Some(st) = self.streams.get_mut(&stream_id) else {
            return 0;
        };
        let reserved = (n as i64)
            .min(self.conn_send_window.max(0))
            .min(st.send_window.max(0));
        self.conn_send_window -= reserved;
        st.send_window -= reserved;
        reserved as usize
    }

    /// Whether the peer sent GOAWAY.
    #[must_use]
    pub fn peer_goaway(&self) -> Option<(u32, u32)> {
        self.peer_goaway
    }

    fn conn_error(&mut self, code: u32) -> ConnectionError {
        let err = ConnectionError { code };
        if !self.goaway_sent {
            self.goaway_sent = true;
            let mut payload = Vec::new();
            payload.extend_from_slice(&self.last_peer_stream_id.to_be_bytes());
            payload.extend_from_slice(&code.to_be_bytes());
            write_header(
                &mut self.out,
                payload.len() as u32,
                FrameKind::GoAway,
                FrameFlags::EMPTY,
                0,
            );
            self.out.extend_from_slice(&payload);
        }
        self.connection_error = Some(err);
        err
    }

    /// Feeds inbound peer bytes into the state machine. Emits events and
    /// queues outbound frames (ACKs, WINDOW_UPDATEs, GOAWAY, RSTs).
    /// Returns the number of bytes consumed by fully-processed frames;
    /// a trailing partial frame is left unconsumed so drivers can
    /// buffer the remainder.
    pub fn handle_read(&mut self, data: &[u8], events: &mut Vec<Event>) -> usize {
        if self.connection_error.is_some() {
            return 0;
        }
        let total = data.len();
        let mut off = 0usize;

        // Server: consume the 24-byte client preface first.
        if !self.preface_done {
            let need = CLIENT_PREFACE.len() - self.preface_pos;
            let take = need.min(data.len());
            if data[..take] != CLIENT_PREFACE[self.preface_pos..self.preface_pos + take] {
                // Bad preface: RFC 9113 §3.5 — treat as connection error
                // with PROTOCOL_ERROR.
                let _ = self.conn_error(error_code::PROTOCOL_ERROR);
                return 0;
            }
            self.preface_pos += take;
            off += take;
            if self.preface_pos < CLIENT_PREFACE.len() {
                return off; // more preface bytes to come
            }
            self.preface_done = true;
        }

        while off < total {
            if self.header_block.active {
                // CONTINUATION must be the very next frame.
                if total - off < 9 {
                    break; // partial frame header
                }
                let hdr_data = &data[off..];
                let Ok(hdr) = parse_header(hdr_data) else {
                    break;
                };
                if hdr.kind != FrameKind::Continuation
                    || hdr.stream_id != self.header_block.stream_id
                {
                    let _ = self.conn_error(error_code::PROTOCOL_ERROR);
                    return off;
                }
                if total - off < 9 + hdr.length as usize {
                    break; // partial CONTINUATION payload
                }
                let frag = &data[off + 9..off + 9 + hdr.length as usize];
                self.header_block.fragments.extend_from_slice(frag);
                off += 9 + hdr.length as usize;
                if hdr.flags.end_headers() {
                    if let Err(code) = self.finish_headers(events) {
                        let _ = self.conn_error(code);
                        return off;
                    }
                }
                continue;
            }

            if total - off < 9 {
                break; // partial frame header
            }
            let hdr_data = &data[off..];
            let Ok(hdr) = parse_header(hdr_data) else {
                break;
            };
            if total - off < 9 + hdr.length as usize {
                break; // partial frame payload — wait for the rest
            }
            let payload = &data[off + 9..off + 9 + hdr.length as usize];
            off += 9 + hdr.length as usize;

            // Stream-id direction rules (RFC 9113 §5.1.1): clients
            // initiate streams with odd ids; responses (and any other
            // peer frames) reference those same odd streams. Even ids
            // exist only for server push, which we never accept.
            match hdr.kind {
                FrameKind::Data
                | FrameKind::Headers
                | FrameKind::Continuation
                | FrameKind::RstStream
                | FrameKind::PushPromise => {
                    if hdr.stream_id == 0 || hdr.stream_id & 1 == 0 {
                        let _ = self.conn_error(error_code::PROTOCOL_ERROR);
                        return off;
                    }
                }
                FrameKind::Settings | FrameKind::Ping | FrameKind::GoAway => {
                    if hdr.stream_id != 0 {
                        let _ = self.conn_error(error_code::PROTOCOL_ERROR);
                        return off;
                    }
                }
                FrameKind::WindowUpdate => {}
                FrameKind::Priority => {}
                FrameKind::Unknown(_) => {}
            }

            match self.handle_frame(&hdr, payload, events) {
                Ok(()) => {}
                Err(code) => {
                    let _ = self.conn_error(code);
                    return off;
                }
            }
            if self.connection_error.is_some() {
                return off;
            }
        }
        off
    }

    fn handle_frame(
        &mut self,
        hdr: &FrameHeader,
        payload: &[u8],
        events: &mut Vec<Event>,
    ) -> Result<(), u32> {
        match hdr.kind {
            FrameKind::Settings => self.handle_settings(hdr, payload, events),
            FrameKind::Ping => self.handle_ping(hdr, payload),
            FrameKind::WindowUpdate => self.handle_window_update(hdr, payload, events),
            FrameKind::GoAway => {
                if payload.len() < 8 {
                    return Err(error_code::FRAME_SIZE_ERROR);
                }
                let last = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let code = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                self.peer_goaway = Some((last, code));
                events.push(Event::GoAway {
                    last_stream_id: last,
                    error_code: code,
                });
                Ok(())
            }
            FrameKind::RstStream => {
                let code = parse_rst_stream(payload).map_err(|_| error_code::FRAME_SIZE_ERROR)?;
                let id = hdr.stream_id;
                if let Some(st) = self.streams.get_mut(&id) {
                    st.state = StreamState::Closed;
                }
                self.streams.remove(&id);
                events.push(Event::Reset {
                    stream_id: id,
                    error_code: code,
                });
                Ok(())
            }
            FrameKind::Headers => {
                self.handle_headers(hdr, payload, events)?;
                Ok(())
            }
            FrameKind::Continuation => {
                // Not expected here: handled by the header_block branch in
                // handle_read; receiving one standalone is a protocol error.
                Err(error_code::PROTOCOL_ERROR)
            }
            FrameKind::Data => self.handle_data(hdr, payload, events),
            FrameKind::Priority => Ok(()), // deprecated: ignore
            FrameKind::PushPromise => {
                // We do not accept server push (client role) and servers
                // never receive push. Refuse the stream politely.
                Err(error_code::REFUSED_STREAM)
            }
            FrameKind::Unknown(_) => Ok(()),
        }
    }

    fn handle_settings(
        &mut self,
        hdr: &FrameHeader,
        payload: &[u8],
        events: &mut Vec<Event>,
    ) -> Result<(), u32> {
        if hdr.flags.ack() {
            if !self.settings_acked {
                return Err(error_code::PROTOCOL_ERROR);
            }
            self.settings_acked = false;
            return Ok(());
        }
        let settings = parse_settings(payload).map_err(|_| error_code::FRAME_SIZE_ERROR)?;
        for setting in &settings {
            match *setting {
                Setting::MaxFrameSize(v) => {
                    if !(DEFAULT_MAX_FRAME_SIZE..=16_777_215).contains(&v) {
                        return Err(error_code::PROTOCOL_ERROR);
                    }
                    self.peer_max_frame = v;
                }
                Setting::InitialWindowSize(v) => {
                    if v > (1 << 30) - 1 {
                        return Err(error_code::FLOW_CONTROL_ERROR);
                    }
                    let delta = i64::from(v) - self.peer_initial_window;
                    self.peer_initial_window = i64::from(v);
                    if delta > 0 {
                        for st in self.streams.values_mut() {
                            st.send_window += delta;
                        }
                        // Credit grew for every open stream (connection
                        // window unchanged) — let drivers retry holds.
                        events.push(Event::WindowUpdate { stream_id: 0 });
                    } else {
                        for st in self.streams.values_mut() {
                            st.send_window += delta;
                        }
                    }
                }
                Setting::HeaderTableSize(_) => {
                    // Our encoder is stateless — nothing to apply.
                }
                _ => {}
            }
        }
        // ACK.
        write_header(
            &mut self.out,
            0,
            FrameKind::Settings,
            FrameFlags::from_u8(0x01),
            0,
        );
        Ok(())
    }

    fn handle_ping(&mut self, hdr: &FrameHeader, payload: &[u8]) -> Result<(), u32> {
        if payload.len() != 8 {
            return Err(error_code::FRAME_SIZE_ERROR);
        }
        if hdr.flags.ack() {
            return Ok(()); // our PONG acked
        }
        write_header(
            &mut self.out,
            8,
            FrameKind::Ping,
            FrameFlags::from_u8(0x01),
            0,
        );
        self.out.extend_from_slice(payload);
        Ok(())
    }

    fn handle_window_update(
        &mut self,
        hdr: &FrameHeader,
        payload: &[u8],
        events: &mut Vec<Event>,
    ) -> Result<(), u32> {
        let inc = parse_window_update(payload).map_err(|_| error_code::FRAME_SIZE_ERROR)?;
        if inc == 0 {
            return Err(error_code::PROTOCOL_ERROR);
        }
        let inc = i64::from(inc);
        if hdr.stream_id == 0 {
            self.conn_send_window += inc;
        } else if let Some(st) = self.streams.get_mut(&hdr.stream_id) {
            st.send_window += inc;
        }
        events.push(Event::WindowUpdate {
            stream_id: hdr.stream_id,
        });
        Ok(())
    }

    fn handle_headers(
        &mut self,
        hdr: &FrameHeader,
        payload: &[u8],
        events: &mut Vec<Event>,
    ) -> Result<(), u32> {
        use super::frame::validate_payload;
        let Ok(split) = validate_payload(hdr, payload, self.peer_max_frame) else {
            return Err(error_code::FRAME_SIZE_ERROR);
        };
        // Inbound HEADERS reference odd ids in both roles: odd are
        // client-initiated streams (responses ride them); even ids are
        // server push, which is refused.
        if hdr.stream_id & 1 == 0 {
            return Err(error_code::PROTOCOL_ERROR);
        }

        // Stream id monotonicity + reuse — only for NEW streams. HEADERS
        // on an already-open stream are trailers (RFC 9113 §8.1) and
        // keep the original id.
        let known = self.streams.contains_key(&hdr.stream_id);
        if !known && hdr.stream_id <= self.last_peer_stream_id {
            return Err(error_code::PROTOCOL_ERROR);
        }
        if !known
            && self.role == Role::Server
            && self.streams.len() >= self.cfg.max_concurrent_streams as usize
        {
            // Over the limit: refuse this stream (stream-level error).
            write_header(
                &mut self.out,
                4,
                FrameKind::RstStream,
                FrameFlags::EMPTY,
                hdr.stream_id,
            );
            self.out
                .extend_from_slice(&error_code::REFUSED_STREAM.to_be_bytes());
            return Ok(());
        }
        self.last_peer_stream_id = hdr.stream_id;

        // Priority hints parsed and ignored (RFC 9113 §5.3 deprecates).
        let mut frag = &payload[split.content_start..split.content_end];
        if hdr.flags.priority() {
            if frag.len() < 5 {
                return Err(error_code::FRAME_SIZE_ERROR);
            }
            frag = &frag[5..];
        }

        let end_stream = hdr.flags.end_stream();
        if hdr.flags.end_headers() {
            let headers = {
                let mut bd = HpackBlockDecoder {
                    fragments: frag.to_vec(),
                    decoder: &mut self.decoder,
                };
                bd.decode_all()
            };
            match headers {
                Ok(headers) => {
                    self.ensure_stream(hdr.stream_id, end_stream);
                    if end_stream {
                        if let Some(st) = self.streams.get_mut(&hdr.stream_id) {
                            st.state = StreamState::HalfClosedRemote;
                        }
                    }
                    events.push(Event::Headers {
                        stream_id: hdr.stream_id,
                        end_stream,
                        headers,
                    });
                    return Ok(());
                }
                Err(_) => return Err(error_code::COMPRESSION_ERROR),
            }
        }

        // Multi-frame block: start assembly.
        self.header_block = HeaderBlockAssembler {
            fragments: frag.to_vec(),
            stream_id: hdr.stream_id,
            end_stream,
            active: true,
        };
        Ok(())
    }

    fn finish_headers(&mut self, events: &mut Vec<Event>) -> Result<(), u32> {
        let (stream_id, end_stream) = (self.header_block.stream_id, self.header_block.end_stream);
        let mut decoder = HpackBlockDecoder {
            fragments: std::mem::take(&mut self.header_block.fragments),
            decoder: &mut self.decoder,
        };
        self.header_block.active = false;
        match decoder.decode_all() {
            Ok(headers) => {
                self.ensure_stream(stream_id, end_stream);
                if end_stream {
                    if let Some(st) = self.streams.get_mut(&stream_id) {
                        st.state = StreamState::HalfClosedRemote;
                    }
                }
                events.push(Event::Headers {
                    stream_id,
                    end_stream,
                    headers,
                });
                Ok(())
            }
            Err(_) => Err(error_code::COMPRESSION_ERROR),
        }
    }

    fn ensure_stream(&mut self, id: u32, end_stream: bool) {
        let entry = self.streams.entry(id).or_insert(Stream {
            state: StreamState::Open,
            recv_window: i64::from(self.cfg.initial_window_size),
            send_window: self.peer_initial_window,
            sent_end: false,
        });
        if end_stream {
            let retire = entry.sent_end;
            entry.state = StreamState::HalfClosedRemote;
            // Both halves done (we already sent END_STREAM): retire.
            if retire {
                self.streams.remove(&id);
            }
        }
    }

    fn handle_data(
        &mut self,
        hdr: &FrameHeader,
        payload: &[u8],
        events: &mut Vec<Event>,
    ) -> Result<(), u32> {
        let Ok(split) = validate_payload(hdr, payload, self.peer_max_frame) else {
            return Err(error_code::FRAME_SIZE_ERROR);
        };
        let id = hdr.stream_id;

        let Some(st) = self.streams.get_mut(&id) else {
            // Unknown/closed stream: connection error STREAM_CLOSED.
            return Err(error_code::STREAM_CLOSED);
        };
        if st.state == StreamState::Closed || st.state == StreamState::HalfClosedRemote {
            return Err(error_code::STREAM_CLOSED);
        }

        let content = &payload[split.content_start..split.content_end];
        // Flow control: connection + stream recv windows must cover it.
        self.conn_recv_window -= content.len() as i64;
        st.recv_window -= content.len() as i64;

        let end_stream = hdr.flags.end_stream();
        if end_stream {
            st.state = StreamState::HalfClosedRemote;
        }
        events.push(Event::Data {
            stream_id: id,
            end_stream,
            data: content.to_vec(),
        });
        Ok(())
    }

    /// Credits the peer for consumed body bytes (emits WINDOW_UPDATEs on
    /// connection + stream when the freed amount is significant).
    pub fn release_capacity(&mut self, stream_id: u32, n: usize) {
        let n = n as i64;
        // Credit is returned IMMEDIATELY on every release: batching at
        // a half-mark deadlocks transfers sized near a window multiple
        // (the peer's remaining credit hits zero while our threshold
        // waits for more consumption). The connection-level initial
        // window is fixed at 65535 (RFC 9113 §6.9.1); SETTINGS only
        // raises per-stream windows.
        self.conn_recv_window += n;
        if n > 0 {
            write_header(
                &mut self.out,
                4,
                FrameKind::WindowUpdate,
                FrameFlags::EMPTY,
                0,
            );
            self.out.extend_from_slice(&(n as u32).to_be_bytes());
        }
        if let Some(st) = self.streams.get_mut(&stream_id) {
            st.recv_window += n;
            if n > 0 {
                write_header(
                    &mut self.out,
                    4,
                    FrameKind::WindowUpdate,
                    FrameFlags::EMPTY,
                    stream_id,
                );
                self.out.extend_from_slice(&(n as u32).to_be_bytes());
            }
        }
    }

    /// Opens a new outbound stream (client role) or validates the id for
    /// a server-initiated response path.
    pub fn open_stream(&mut self, stream_id: u32) {
        self.streams.entry(stream_id).or_insert(Stream {
            state: StreamState::Open,
            recv_window: i64::from(self.cfg.initial_window_size),
            send_window: self.peer_initial_window,
            sent_end: false,
        });
    }

    /// Allocates the next outbound stream id.
    pub fn alloc_stream_id(&mut self) -> u32 {
        let id = self.next_stream_id;
        self.next_stream_id += 2;
        self.open_stream(id);
        id
    }

    /// Sends a response/request HEADERS block (HPACK-encoded, split into
    /// CONTINUATIONs by the peer's max frame size). `end_stream` closes
    /// the send side.
    pub fn send_headers(
        &mut self,
        stream_id: u32,
        headers: &[(Vec<u8>, Vec<u8>)],
        end_stream: bool,
    ) {
        let mut block = Vec::new();
        self.encoder.encode(headers, &mut block);

        // END_STREAM is 0x01, END_HEADERS is 0x04. END_HEADERS may only
        // be set on the FINAL fragment of the block (RFC 9113 §4.3):
        // on the first frame only when it fits whole.
        let fits = block.len() <= self.peer_max_frame as usize;
        let max_frag = self.peer_max_frame as usize;
        let first = &block[..block.len().min(max_frag)];
        let mut flags = if end_stream { 0x04 | 0x01 } else { 0x04 };
        if !fits {
            flags &= !0x04;
        }
        write_header(
            &mut self.out,
            first.len() as u32,
            FrameKind::Headers,
            FrameFlags::from_u8(flags & 0x04 | if end_stream && fits { 0x01 } else { 0 }),
            stream_id,
        );
        self.out.extend_from_slice(first);

        let mut rest = &block[first.len()..];
        while !rest.is_empty() {
            let n = rest.len().min(max_frag);
            let last = n == rest.len();
            write_header(
                &mut self.out,
                n as u32,
                FrameKind::Continuation,
                FrameFlags::from_u8(if last { 0x04 } else { 0 }),
                stream_id,
            );
            self.out.extend_from_slice(&rest[..n]);
            rest = &rest[n..];
        }
        if end_stream {
            let mut retire = false;
            if let Some(st) = self.streams.get_mut(&stream_id) {
                st.sent_end = true;
                st.state = match st.state {
                    StreamState::HalfClosedRemote => {
                        retire = true; // both halves done
                        StreamState::Closed
                    }
                    _ => StreamState::HalfClosedLocal,
                };
            }
            if retire {
                self.streams.remove(&stream_id);
            }
        }
    }

    /// Sends DATA on a stream, capped by connection + stream send
    /// windows and the peer's max frame size. Returns bytes sent
    /// (remainder awaits WINDOW_UPDATE credit).
    pub fn send_data(&mut self, stream_id: u32, data: &[u8], end_stream: bool) -> usize {
        let cap = self
            .streams
            .get(&stream_id)
            .map(|st| st.send_window.min(self.conn_send_window))
            .unwrap_or(0);
        let cap = (cap.max(0) as usize).min(self.peer_max_frame as usize);
        let n = data.len().min(cap);
        if n == 0 {
            return 0;
        }

        let last = n == data.len();
        let flags = if last && end_stream { 0x01 } else { 0x00 };
        write_header(
            &mut self.out,
            n as u32,
            FrameKind::Data,
            FrameFlags::from_u8(flags),
            stream_id,
        );
        self.out.extend_from_slice(&data[..n]);

        self.conn_send_window -= n as i64;
        let mut retire = false;
        if let Some(st) = self.streams.get_mut(&stream_id) {
            st.send_window -= n as i64;
            if last && end_stream {
                st.sent_end = true;
                st.state = match st.state {
                    StreamState::HalfClosedRemote => {
                        retire = true; // both halves done
                        StreamState::Closed
                    }
                    _ => StreamState::HalfClosedLocal,
                };
            }
        }
        if retire {
            self.streams.remove(&stream_id);
        }
        n
    }

    /// Sends RST_STREAM to close a stream with an error.
    pub fn send_rst_stream(&mut self, stream_id: u32, code: u32) {
        write_header(
            &mut self.out,
            4,
            FrameKind::RstStream,
            FrameFlags::EMPTY,
            stream_id,
        );
        self.out.extend_from_slice(&code.to_be_bytes());
        self.streams.remove(&stream_id);
    }

    /// Sends a PING with an opaque 8-byte payload.
    pub fn send_ping(&mut self, payload: &[u8; 8]) {
        write_header(&mut self.out, 8, FrameKind::Ping, FrameFlags::EMPTY, 0);
        self.out.extend_from_slice(payload);
    }

    /// Sends GOAWAY (draining).
    pub fn send_goaway(&mut self, error_code: u32) {
        if self.goaway_sent {
            return;
        }
        self.goaway_sent = true;
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.last_peer_stream_id.to_be_bytes());
        payload.extend_from_slice(&error_code.to_be_bytes());
        write_header(
            &mut self.out,
            payload.len() as u32,
            FrameKind::GoAway,
            FrameFlags::EMPTY,
            0,
        );
        self.out.extend_from_slice(&payload);
    }
}

/// HPACK decode helper over assembled fragments.
struct HpackBlockDecoder<'a> {
    fragments: Vec<u8>,
    decoder: &'a mut HpackDecoder,
}

impl HpackBlockDecoder<'_> {
    fn decode_all(&mut self) -> Result<Vec<super::hpack::Header>, HpackError> {
        self.decoder.decode(&self.fragments)
    }
}

#[cfg(test)]
mod conn_tests {
    use super::*;

    fn server() -> Connection {
        Connection::new(Role::Server, ConnectionConfig::default())
    }

    fn client_conn() -> Connection {
        Connection::new(Role::Client, ConnectionConfig::default())
    }

    fn frame_bytes(kind: FrameKind, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_header(
            &mut out,
            payload.len() as u32,
            kind,
            FrameFlags::from_u8(flags),
            stream_id,
        );
        out.extend_from_slice(payload);
        out
    }

    /// Server: client preface (magic + SETTINGS) → SETTINGS ack queued.
    #[test]
    fn server_acknowledges_client_settings() {
        let mut c = server();
        let mut events = Vec::new();
        // Preface magic + the client's SETTINGS frame, in one read
        // (frame header precedes its 6-byte payload).
        let mut preface_and_settings = CLIENT_PREFACE.to_vec();
        write_header(
            &mut preface_and_settings,
            6,
            FrameKind::Settings,
            FrameFlags::EMPTY,
            0,
        );
        write_setting(&mut preface_and_settings, 0x3, 128);
        c.handle_read(&preface_and_settings, &mut events);
        assert!(events.is_empty());
        let writes = c.take_pending_writes();
        // Initial server SETTINGS (5 x 6 + 9 hdr) + SETTINGS ACK (9).
        assert_eq!(writes.len(), 39 + 9, "expected SETTINGS + ACK: {writes:?}");
        // Second frame (offset 39) is the SETTINGS ACK.
        assert_eq!(writes[39 + 4] & 0x01, 0x01, "ACK flag");
    }

    #[test]
    fn bad_preface_is_connection_error() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(b"NOT THE PREFACE", &mut events);
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::PROTOCOL_ERROR)
        );
    }

    /// Client HEADERS (END_STREAM) on stream 1 → Headers event.
    #[test]
    fn server_headers_end_stream() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // 82 = :method GET indexed; 86 = :scheme https; END_STREAM.
        let block = [0x82, 0x86];
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 1, &block),
            &mut events,
        );
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Headers {
                stream_id,
                end_stream,
                headers,
            } => {
                assert_eq!(*stream_id, 1);
                assert!(*end_stream);
                assert!(!headers.is_empty());
            }
            other => panic!("expected headers event: {other:?}"),
        }
    }

    /// Even stream id from a client is a connection error.
    #[test]
    fn even_stream_id_from_client_is_error() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 2, &[0x82]),
            &mut events,
        );
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::PROTOCOL_ERROR)
        );
    }

    /// DATA on a stream we never saw HEADERS for → STREAM_CLOSED error.
    #[test]
    fn data_on_unknown_stream_is_error() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        c.handle_read(&frame_bytes(FrameKind::Data, 0x00, 5, b"x"), &mut events);
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::STREAM_CLOSED)
        );
    }

    /// DATA stream: HEADERS (END_STREAM off) → Data events → half-closed.
    #[test]
    fn data_relay_with_flow_accounting() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // HEADERS END_STREAM=off: :method GET (82).
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x04, 1, &[0x82]),
            &mut events,
        );
        assert_eq!(events.len(), 1, "headers event");

        // DATA chunk, END_STREAM set.
        c.handle_read(
            &frame_bytes(FrameKind::Data, 0x01, 1, b"payload"),
            &mut events,
        );
        assert_eq!(events.len(), 2);
        match &events[1] {
            Event::Data {
                stream_id,
                end_stream,
                data,
            } => {
                assert_eq!(*stream_id, 1);
                assert!(*end_stream);
                assert_eq!(data, b"payload");
            }
            other => panic!("expected data event: {other:?}"),
        }
    }

    /// release_capacity emits WINDOW_UPDATEs once past the half mark.
    #[test]
    fn release_capacity_emits_window_updates() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // HEADERS END_STREAM=off on stream 1 (stream recv window =
        // 512 KiB; connection window fixed at 65535).
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x04, 1, &[0x82]),
            &mut events,
        );

        // 5 rounds of 60 KiB: each release returns credit IMMEDIATELY
        // (both connection and stream) — no half-mark batching, which
        // deadlocks transfers sized near a window multiple.
        let chunk = vec![0u8; 61440];
        for _ in 0..5 {
            c.handle_read(&frame_bytes(FrameKind::Data, 0x00, 1, &chunk), &mut events);
            c.release_capacity(1, chunk.len());
        }
        let writes = c.take_pending_writes();
        assert_eq!(
            writes.len(),
            13 * 10,
            "5 conn + 5 stream WINDOW_UPDATEs (9 hdr + 4 payload each)"
        );
    }

    /// PING (no ACK) → PONG (ACK) with identical payload.
    #[test]
    fn ping_gets_pong() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        c.handle_read(
            &frame_bytes(FrameKind::Ping, 0x00, 0, &[7u8; 8]),
            &mut events,
        );
        let writes = c.take_pending_writes();
        assert_eq!(writes.len(), 9 + 8);
        // ACK flag set on the pong.
        assert_eq!(writes[4], 0x01);
        assert_eq!(&writes[9..17], &[7u8; 8]);
    }

    /// GOAWAY event surfaces last-stream-id + error code.
    #[test]
    fn goaway_event() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        let mut payload = Vec::new();
        payload.extend_from_slice(&7u32.to_be_bytes());
        payload.extend_from_slice(&2u32.to_be_bytes());
        c.handle_read(
            &frame_bytes(FrameKind::GoAway, 0x00, 0, &payload),
            &mut events,
        );
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::GoAway {
                last_stream_id,
                error_code,
            } => {
                assert_eq!(*last_stream_id, 7);
                assert_eq!(*error_code, 2);
            }
            other => panic!("expected goaway: {other:?}"),
        }
    }

    /// SETTINGS_INITIAL_WINDOW_SIZE adjusts open stream send windows.
    #[test]
    fn settings_initial_window_delta() {
        let mut c = client_conn();
        let mut events = Vec::new();
        c.handle_read(&frame_bytes(FrameKind::Settings, 0x00, 0, &[]), &mut events);

        // Open a stream (client allocates odd id 1).
        let id = c.alloc_stream_id();
        assert_eq!(id, 1);

        // Send SETTINGS raising initial window: old 65535 → new 1_048_576
        // (delta +983041).
        let mut payload = Vec::new();
        write_setting(&mut payload, 0x4, 1_048_576);
        c.handle_read(
            &frame_bytes(FrameKind::Settings, 0x00, 0, &payload),
            &mut events,
        );

        // Stream 1 send window: 65535 + 983041 = 1048576. Each send_data
        // call is frame-capped (peer MAX_FRAME_SIZE 16384); drain the
        // old window to prove the delta accounting kept it intact.
        let mut sent = 0usize;
        while sent < 65535 {
            let want = 16384.min(65535 - sent);
            let data = vec![0u8; want];
            let n = c.send_data(id, &data, false);
            assert_eq!(n, want, "frame at {sent}");
            sent += n;
        }
        // The stream window grew, but DATA also consumed the
        // connection-level window (65535, now exhausted): a stream-level
        // SETTINGS delta alone does not unblock sends.
        let n2 = c.send_data(id, &[0u8; 16384], false);
        assert_eq!(n2, 0, "connection window exhausted");

        // Replenish the connection window; the delta-adjusted stream
        // window (1048576) then allows sends to resume.
        let conn_payload = {
            let mut p = 100_000u32.to_be_bytes().to_vec();
            p[0] &= 0x7f;
            p
        };
        c.handle_read(
            &frame_bytes(FrameKind::WindowUpdate, 0x00, 0, &conn_payload),
            &mut events,
        );
        let n3 = c.send_data(id, &[0u8; 16384], false);
        assert_eq!(n3, 16384, "send resumes once both windows have credit");
    }

    /// Client role: server even-id HEADERS produce events.
    /// Client role: server response HEADERS arrive on the client's own
    /// ODD streams; EVEN ids are server push and are refused.
    #[test]
    fn client_receives_response_headers() {
        let mut c = client_conn();
        // Client preface is queued in pending writes at construction.
        let _ = c.take_pending_writes();

        // The client opened stream 1; the server responds on it.
        c.open_stream(1);
        let mut events = Vec::new();
        // :status 200 indexed = 0x88, END_STREAM.
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 1, &[0x88]),
            &mut events,
        );
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Headers {
                stream_id,
                end_stream,
                headers,
            } => {
                assert_eq!(*stream_id, 1);
                assert!(*end_stream);
                assert_eq!(headers[0].name, b":status");
                assert_eq!(headers[0].value, b"200");
            }
            other => panic!("expected headers: {other:?}"),
        }
    }

    /// Client role: even-stream HEADERS are server push (banned) —
    /// connection error.
    #[test]
    fn client_rejects_even_stream_headers() {
        let mut c = client_conn();
        let _ = c.take_pending_writes();
        let mut events = Vec::new();
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 2, &[0x88]),
            &mut events,
        );
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::PROTOCOL_ERROR)
        );
    }

    /// CONTINUATION assembles multi-frame header blocks.
    #[test]
    fn continuation_assembles_block() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // HEADERS without END_HEADERS, with END_STREAM: :method GET (82).
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x01, 1, &[0x82]),
            &mut events,
        );
        assert!(events.is_empty(), "block not yet complete");

        // CONTINUATION END_HEADERS=on: :scheme https (86).
        c.handle_read(
            &frame_bytes(FrameKind::Continuation, 0x04, 1, &[0x86]),
            &mut events,
        );
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::Headers {
                end_stream,
                headers,
                ..
            } => {
                assert!(*end_stream);
                assert!(headers.len() >= 2);
            }
            other => panic!("expected headers: {other:?}"),
        }
    }

    /// A non-CONTINUATION frame while assembling is a connection error.
    #[test]
    fn interrupted_header_block_is_error() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x01, 1, &[0x82]),
            &mut events,
        );
        c.handle_read(
            &frame_bytes(FrameKind::Ping, 0x00, 0, &[7u8; 8]),
            &mut events,
        );
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::PROTOCOL_ERROR)
        );
    }

    /// HPACK errors surface as COMPRESSION_ERROR connection failures.
    #[test]
    fn hpack_corruption_is_compression_error() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // Indexed header with index 0 — invalid per RFC 7541 §6.1.
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 1, &[0x80]),
            &mut events,
        );
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::COMPRESSION_ERROR)
        );
    }

    /// HEADERS split across CONTINUATIONs on SEND: a header block
    /// larger than the peer's max frame size is emitted as HEADERS
    /// (END_HEADERS off) followed by CONTINUATION (END_HEADERS on).
    #[test]
    fn send_headers_splits_into_continuations() {
        let mut c = client_conn();
        let _ = c.take_pending_writes(); // drain preface + initial SETTINGS
        let id = c.alloc_stream_id();
        // ~40 KiB of header value forces a split at 16384.
        let big = vec![b'a'; 40_960];
        let headers = vec![
            (b":method".to_vec(), b"GET".to_vec()),
            (b"x-big".to_vec(), big),
        ];
        c.send_headers(id, &headers, true);
        let writes = c.take_pending_writes();
        // Walk the emitted frames: first HEADERS, then CONTINUATIONs.
        let mut off = 0usize;
        let mut kinds = Vec::new();
        while off < writes.len() {
            let len =
                u32::from_be_bytes([0, writes[off], writes[off + 1], writes[off + 2]]) as usize;
            kinds.push((writes[off + 3], writes[off + 4]));
            off += 9 + len;
        }
        assert!(kinds.len() >= 2, "expected split frames: {kinds:?}");
        let (first_kind, first_flags) = kinds[0];
        assert_eq!(first_kind, 0x01, "HEADERS");
        assert_eq!(first_flags & 0x04, 0, "END_HEADERS clear on first frame");
        let (last_kind, last_flags) = *kinds.last().expect("nonempty");
        assert_eq!(last_kind, 0x09, "CONTINUATION");
        assert_eq!(last_flags & 0x04, 0x04, "END_HEADERS on last frame");
    }

    /// DATA on a stream closed by END_STREAM is a STREAM_CLOSED error.
    #[test]
    fn data_after_end_stream_is_stream_closed() {
        let mut c = server();
        let mut events = Vec::new();
        c.handle_read(CLIENT_PREFACE, &mut events);
        let _ = c.take_pending_writes();

        // HEADERS with END_STREAM closes stream 1.
        c.handle_read(
            &frame_bytes(FrameKind::Headers, 0x05, 1, &[0x82]),
            &mut events,
        );
        assert_eq!(events.len(), 1);
        // Late DATA on the closed stream.
        c.handle_read(&frame_bytes(FrameKind::Data, 0x00, 1, b"late"), &mut events);
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::STREAM_CLOSED)
        );
    }

    /// SETTINGS ACK arriving without a pending ACK is a PROTOCOL_ERROR.
    #[test]
    fn unexpected_settings_ack_is_protocol_error() {
        let mut c = server();
        let mut events = Vec::new();
        // Preface + client SETTINGS + ACK of ours: a legitimate
        // exchange (our initial SETTINGS were in flight).
        let mut preface = CLIENT_PREFACE.to_vec();
        preface.extend_from_slice(&frame_bytes(FrameKind::Settings, 0x00, 0, &[]));
        preface.extend_from_slice(&frame_bytes(FrameKind::Settings, 0x01, 0, &[]));
        c.handle_read(&preface, &mut events);
        assert!(c.connection_error().is_none());
        let _ = c.take_pending_writes();

        // A SECOND ACK with no SETTINGS in flight is a protocol error.
        c.handle_read(&frame_bytes(FrameKind::Settings, 0x01, 0, &[]), &mut events);
        assert_eq!(
            c.connection_error().map(|e| e.code),
            Some(error_code::PROTOCOL_ERROR)
        );
    }

    /// send_data respects stream + connection windows.
    #[test]
    fn send_data_respects_flow_control() {
        let mut c = client_conn();
        let id = c.alloc_stream_id();

        // Initial peer stream window 65535; connection window 65535.
        // Each send_data call is capped by the peer's MAX_FRAME_SIZE
        // (16384), so a full window needs 4 frames.
        // 65535 = 3 x 16384 + 16383.
        for expected in [16384usize, 16384, 16384, 16383] {
            let data = vec![0u8; expected];
            let n = c.send_data(id, &data, false);
            assert_eq!(n, expected);
        }
        // Window exhausted: next send is refused (0 bytes).
        assert_eq!(c.send_data(id, &[0u8; 1000], false), 0, "window exhausted");

        // Peer grants 200000 STREAM-level credit via WINDOW_UPDATE, but
        // DATA consumes both windows — sending stays blocked until the
        // CONNECTION-level window is replenished too.
        let mut events = Vec::new();
        let mut payload = 200_000u32.to_be_bytes().to_vec();
        payload[0] &= 0x7f; // reserved bit zero
        c.handle_read(
            &frame_bytes(FrameKind::WindowUpdate, 0x00, id, &payload),
            &mut events,
        );
        assert_eq!(c.send_data(id, &[0u8; 100], false), 0, "conn window empty");

        // Connection-level WINDOW_UPDATE (stream_id 0) unblocks sends.
        let conn_payload = {
            let mut p = (65_535u32 + 200_000).to_be_bytes().to_vec();
            p[0] &= 0x7f;
            p
        };
        c.handle_read(
            &frame_bytes(FrameKind::WindowUpdate, 0x00, 0, &conn_payload),
            &mut events,
        );
        let n = c.send_data(id, &[0u8; 16384], false);
        assert_eq!(n, 16384, "send resumes after credit");
    }
}
