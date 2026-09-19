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

/// The `vane xds-client` loop: runs the ADS client against an
/// Envoy-compatible management server (h2c prior knowledge), ACKs
/// per-type, maps decoded Clusters + RouteConfigurations into a
/// [`vane_control::xds::XdsSnapshot`], and POSTs it to the admin
/// plane on every accepted generation. Returns the process exit code
/// (0 is never reached — the loop runs until the management plane or
/// the transport fails).
///
/// # Errors / exit codes
/// `1` on unresolvable management address, transport failure, or the
/// management plane closing the stream.
pub fn xds_client_loop(management: &str, node_id: &str, admin: &str) -> i32 {
    use std::net::ToSocketAddrs as _;
    use std::time::Duration;
    use vane_control::envoy;
    use vane_control::xds_grpc::{AdsDecision, AdsSession, SUBSCRIBED_TYPES};

    let addr = match management
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
    {
        Some(a) => a,
        None => {
            eprintln!("xds-client: cannot resolve {management}");
            return 1;
        }
    };
    let snapshot_url = format!("{admin}/xds/snapshot");
    eprintln!("xds-client: mgmt={management} node={node_id} admin={admin}");

    let mut client = match AdsClient::connect(addr, Duration::from_secs(10)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("xds-client: connect: {e}");
            return 1;
        }
    };
    if let Err(e) = client.open_stream(vane_proto::xds::ADS_PATH) {
        eprintln!("xds-client: open stream: {e}");
        return 1;
    }
    let mut session = AdsSession::new(node_id, "vane");
    let mut clusters: Vec<envoy::EnvoyCluster> = Vec::new();
    let mut route_config: Option<envoy::EnvoyRouteConfig> = None;
    for t in SUBSCRIBED_TYPES {
        if let Err(e) = client.send_message(&session.initial_request(t)) {
            eprintln!("xds-client: subscribe {t}: {e}");
            return 1;
        }
    }
    let http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("http client");
    loop {
        let outcome = match poll_outcome(&mut client, Duration::from_secs(1)) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("xds-client: poll: {e}");
                return 1;
            }
        };
        let messages = match outcome {
            PollOutcome::Closed => {
                eprintln!("xds-client: management closed the stream");
                return 1;
            }
            PollOutcome::Idle => continue,
            PollOutcome::Messages(m) => m,
        };
        for msg in messages {
            let Some(response) = vane_proto::xds::decode_discovery_response(&msg) else {
                continue;
            };
            let type_url = response.type_url.clone();
            let mut resources = response.resources.clone();
            let mut decode_error: Option<String> = None;
            let validate = |res: &[Vec<u8>]| -> Result<(), String> {
                if type_url == vane_proto::xds::type_url::CLUSTER {
                    let mut decoded = Vec::new();
                    for any in res {
                        let (_, value) = vane_control::xds_grpc::any_value(any)
                            .ok_or_else(|| "cluster Any".to_string())?;
                        decoded.push(
                            envoy::decode_cluster(&value)
                                .ok_or_else(|| "cluster proto".to_string())?,
                        );
                    }
                    clusters = decoded;
                } else if type_url == vane_proto::xds::type_url::ROUTE {
                    for any in res {
                        let (_, value) = vane_control::xds_grpc::any_value(any)
                            .ok_or_else(|| "route Any".to_string())?;
                        route_config = Some(
                            envoy::decode_route_config(&value)
                                .ok_or_else(|| "route proto".to_string())?,
                        );
                    }
                }
                Ok(())
            };
            // `resources` is moved into on_response; keep a copy for the
            // publisher below via the closure's decode side effect.
            let _ = &mut resources;
            let Some(decision) = session.on_response(
                &type_url,
                &response.version_info,
                &response.nonce,
                resources,
                validate,
            ) else {
                continue;
            };
            if let AdsDecision::Nack { error } = &decision {
                decode_error = Some(error.clone());
            }
            if let Some(err) = &decode_error {
                eprintln!("xds-client: NACK {type_url}: {err}");
                let nack = session.nack_request(&type_url, err);
                if let Err(e) = client.send_message(&nack) {
                    eprintln!("xds-client: nack send: {e}");
                    return 1;
                }
                continue;
            }
            // ACK.
            let ack = session.ack_request(&type_url);
            if let Err(e) = client.send_message(&ack) {
                eprintln!("xds-client: ack send: {e}");
                return 1;
            }
            // CDS + RDS both accepted: publish the snapshot.
            if type_url == vane_proto::xds::type_url::ROUTE {
                if let Some(rc) = &route_config {
                    let snapshot = envoy::map_snapshot(&clusters, rc);
                    match serde_json::to_string(&snapshot) {
                        Ok(body) => match http
                            .post(&snapshot_url)
                            .header("content-type", "application/json")
                            .body(body)
                            .send()
                        {
                            Ok(r) if r.status().is_success() => {
                                eprintln!("xds-client: snapshot applied");
                            }
                            Ok(r) => {
                                eprintln!("xds-client: snapshot rejected: {}", r.status());
                            }
                            Err(e) => {
                                eprintln!("xds-client: admin unreachable: {e}");
                            }
                        },
                        Err(e) => eprintln!("xds-client: snapshot encode: {e}"),
                    }
                }
            }
        }
    }
}
