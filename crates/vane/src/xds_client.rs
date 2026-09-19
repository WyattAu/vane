//! Blocking ADS client over h2: connects to an xDS management server
//! with prior-knowledge h2, opens one long-lived gRPC stream
//! (`AggregatedDiscoveryService/StreamAggregatedResources`), sends
//! DiscoveryRequests, and yields DiscoveryResponses. Wire codecs live
//! in vane-proto; session state in vane-control.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use vane_core::h2::connection::ConnectionConfig;
use vane_core::h2::connection::{Connection, Role};
use vane_proto::xds::{grpc_frame, grpc_unframe};

/// A connected ADS stream.
pub struct AdsClient {
    sock: TcpStream,
    conn: Connection,
    stream: Option<u32>,
    backlog: Vec<u8>,
    /// Server-pushed flow-control credit held for the next send.
    held: Vec<u8>,
    eof: bool,
}

impl AdsClient {
    /// Connects (TCP + h2 preface) to the management server.
    ///
    /// # Errors
    /// Connect / handshake write failures.
    pub fn connect(addr: SocketAddr, read_timeout: Duration) -> std::io::Result<Self> {
        let sock = std::net::TcpStream::connect(addr)?;
        sock.set_nodelay(true).ok();
        sock.set_read_timeout(Some(read_timeout)).ok();
        let mut conn = Connection::new(
            Role::Client,
            ConnectionConfig {
                ..ConnectionConfig::default()
            },
        );
        let preface = conn.take_pending_writes();
        let mut client = Self {
            sock,
            conn,
            stream: None,
            backlog: Vec::new(),
            held: Vec::new(),
            eof: false,
        };
        client.sock.write_all(&preface)?;
        Ok(client)
    }

    /// Opens the ADS gRPC stream (POST, content-type
    /// application/grpc, no END_STREAM — the stream stays open).
    ///
    /// # Errors
    /// Underlying socket write failure.
    pub fn open_stream(&mut self, path: &str) -> std::io::Result<()> {
        let id = self.conn.alloc_stream_id();
        self.stream = Some(id);
        let headers = vec![
            (b":method".to_vec(), b"POST".to_vec()),
            (b":scheme".to_vec(), b"http".to_vec()),
            (b":authority".to_vec(), b"ads.local".to_vec()),
            (b":path".to_vec(), path.as_bytes().to_vec()),
            (b"content-type".to_vec(), b"application/grpc".to_vec()),
            (b"te".to_vec(), b"trailers".to_vec()),
        ];
        self.conn.send_headers(id, &headers, false);
        self.flush()
    }

    /// Sends one gRPC-framed message on the ADS stream (flow-
    /// controlled: overflow holds until the next poll's WINDOW_
    /// UPDATEs).
    ///
    /// # Errors
    /// Underlying socket write failure.
    pub fn send_message(&mut self, message: &[u8]) -> std::io::Result<()> {
        let Some(stream) = self.stream else {
            return Ok(());
        };
        let mut payload = std::mem::take(&mut self.held);
        payload.extend_from_slice(&grpc_frame(message));
        let sent = self.conn.send_data(stream, &payload, false);
        if sent < payload.len() {
            self.held = payload[sent..].to_vec();
        }
        self.flush()
    }

    /// Flushes queued frames to the socket.
    fn flush(&mut self) -> std::io::Result<()> {
        let out = self.conn.take_pending_writes();
        if !out.is_empty() {
            self.sock.write_all(&out)?;
        }
        Ok(())
    }

    /// Reads socket bytes for up to `timeout`; returns fully received
    /// gRPC messages (unframed). Also drains held send bytes on
    /// WINDOW_UPDATEs.
    ///
    /// # Errors
    /// Socket read failure (timeouts surface as
    /// `ErrorKind::WouldBlock`).
    pub fn poll(&mut self, timeout: Duration) -> std::io::Result<Vec<Vec<u8>>> {
        self.sock.set_read_timeout(Some(timeout)).ok();
        let mut buf = [0u8; 16 * 1024];
        let n = match self.sock.read(&mut buf) {
            Ok(0) => {
                self.eof = true;
                return Ok(Vec::new());
            }
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        self.backlog.extend_from_slice(&buf[..n]);
        // The connection decodes h2 DATA payloads (the gRPC byte
        // stream) into events; collect them before unframing.
        let mut app_bytes = Vec::new();
        loop {
            let mut events = Vec::new();
            let consumed = self.conn.handle_read(&self.backlog, &mut events);
            if consumed == 0 {
                break;
            }
            self.backlog.drain(..consumed);
            for ev in events {
                if let vane_core::h2::connection::Event::Data { data, .. } = ev {
                    app_bytes.extend_from_slice(&data);
                }
            }
            // SETTINGS/WINDOW_UPDATE processing queues ACK frames;
            // the peer cannot proceed without them.
            let acks = self.conn.take_pending_writes();
            if !acks.is_empty() {
                self.sock.write_all(&acks)?;
            }
        }
        // Credit arrivals: flush held send bytes.
        if !self.held.is_empty() {
            let Some(stream) = self.stream else {
                return Ok(Vec::new());
            };
            let held = std::mem::take(&mut self.held);
            let mut offset = 0;
            while offset < held.len() {
                let sent = self.conn.send_data(stream, &held[offset..], false);
                if sent == 0 {
                    break;
                }
                offset += sent;
            }
            if offset < held.len() {
                self.held = held[offset..].to_vec();
            }
            self.flush()?;
        }
        // Unframe responses from the decoded application stream.
        let (frames, consumed) = grpc_unframe(&app_bytes);
        let _ = consumed;
        Ok(frames.into_iter().map(|(_, m)| m).collect())
    }

    /// The connection hit EOF or an h2 error.
    #[must_use]
    pub fn dead(&self) -> bool {
        self.eof || self.conn.connection_error().is_some()
    }
}

/// The driver's poll outcome.
pub enum PollOutcome {
    /// Messages decoded this round.
    Messages(Vec<Vec<u8>>),
    /// Nothing arrived within the timeout.
    Idle,
    /// The connection ended.
    Closed,
}

/// Reads with `timeout`, mapping to [`PollOutcome`].
///
/// # Errors
/// Socket failures other than timeouts.
pub fn poll_outcome(client: &mut AdsClient, timeout: Duration) -> std::io::Result<PollOutcome> {
    let msgs = client.poll(timeout)?;
    if client.dead() {
        return Ok(PollOutcome::Closed);
    }
    if msgs.is_empty() {
        Ok(PollOutcome::Idle)
    } else {
        Ok(PollOutcome::Messages(msgs))
    }
}
