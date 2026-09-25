//! Envoy xDS mapping e2e: a fake management plane serving hand-encoded
//! Envoy Cluster + RouteConfiguration resources → our blocking ADS
//! driver → `envoy::decode_*` → `map_snapshot` → POST to the admin
//! plane → live route match (host-scoped, prefix-routed), with the
//! pre-snapshot static route displaced by the snapshot's replace-all
//! semantics.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener as StdListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use vane_control::envoy;
use vane_control::xds::XdsSnapshot;
use vane_control::xds_grpc::{AdsDecision, AdsSession, SUBSCRIBED_TYPES};
use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};
use vane_proto::{pb, xds};

const CLUSTER_VERSION: &str = "c-1";
const ROUTE_VERSION: &str = "r-1";
const CLUSTER_NONCE: &str = "n-c1";
const ROUTE_NONCE: &str = "n-r1";

// ---------- wire encoders (hand-rolled, mirrors envoy.rs's tests) ----------

/// Envoy Cluster: name (1) + load_assignment (4) with one
/// socket-address endpoint.
fn encode_envoy_cluster(name: &str, host: &str, port: u16) -> Vec<u8> {
    let mut cluster = Vec::new();
    pb::string_field(&mut cluster, 1, name);
    let mut sock = Vec::new();
    pb::string_field(&mut sock, 2, host);
    // SocketAddress.port_value = 3 per the real envoy proto.
    pb::varint_field(&mut sock, 3, u64::from(port));
    let mut address = Vec::new();
    // Address.socket_address = 1 (the address oneof).
    pb::message_field(&mut address, 1, &sock);
    let mut endpoint = Vec::new();
    pb::message_field(&mut endpoint, 1, &address);
    let mut lb = Vec::new();
    pb::message_field(&mut lb, 1, &endpoint);
    let mut locality = Vec::new();
    // LocalityLbEndpoints.lb_endpoints = 2.
    pb::message_field(&mut locality, 2, &lb);
    let mut cla = Vec::new();
    // ClusterLoadAssignment.endpoints = 2.
    pb::message_field(&mut cla, 2, &locality);
    pb::message_field(&mut cluster, 33, &cla); // load_assignment = 33
    cluster
}

/// Envoy RouteConfiguration: one virtual host (2) with `domain` (2)
/// and one prefix-route (3): match prefix (1) → cluster (2).
fn encode_envoy_route_config(domain: &str, prefix: &str, cluster: &str) -> Vec<u8> {
    let mut rc = Vec::new();
    let mut vh = Vec::new();
    pb::string_field(&mut vh, 1, "vh");
    pb::string_field(&mut vh, 2, domain);
    let mut route_match = Vec::new();
    pb::string_field(&mut route_match, 1, prefix);
    let mut route_action = Vec::new();
    pb::string_field(&mut route_action, 1, cluster);
    let mut route = Vec::new();
    pb::message_field(&mut route, 1, &route_match);
    pb::message_field(&mut route, 2, &route_action);
    pb::message_field(&mut vh, 3, &route);
    pb::message_field(&mut rc, 2, &vh);
    rc
}

/// `google.protobuf.Any`: type_url (1) + value (2).
fn encode_any(type_url: &str, value: &[u8]) -> Vec<u8> {
    let mut any = Vec::new();
    pb::string_field(&mut any, 1, type_url);
    pb::bytes_field(&mut any, 2, value);
    any
}

/// DiscoveryResponse: version (1), repeated resources (2), type_url
/// (4), nonce (5).
fn encode_response(version: &str, type_url: &str, nonce: &str, resources: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    pb::string_field(&mut buf, 1, version);
    for r in resources {
        pb::bytes_field(&mut buf, 2, r);
    }
    pb::string_field(&mut buf, 4, type_url);
    pb::string_field(&mut buf, 5, nonce);
    buf
}

// ---------- fake management plane ----------

/// ADS over one h2 stream: answers the CDS DiscoveryRequest with the
/// hand-encoded Envoy Cluster, then (only after CDS was served, so the
/// client knows the cluster before the route that references it) the
/// RDS request with the RouteConfiguration. LDS/EDS get no response.
/// Records the client's ACK nonces per type.
struct FakeAdsServer {
    conn: Connection,
    stream: Option<u32>,
    cds_served: bool,
    route_requested: bool,
    cluster_any: Vec<u8>,
    route_any: Vec<u8>,
    ack_nonces: std::collections::HashMap<String, String>,
}

impl FakeAdsServer {
    fn new(cluster_any: Vec<u8>, route_any: Vec<u8>) -> Self {
        Self {
            conn: Connection::new(Role::Server, ConnectionConfig::default()),
            stream: None,
            cds_served: false,
            route_requested: false,
            cluster_any,
            route_any,
            ack_nonces: std::collections::HashMap::new(),
        }
    }

    fn send_response(&mut self, type_url: &str, version: &str, nonce: &str) {
        let Some(stream) = self.stream else { return };
        let resources: &[Vec<u8>] = if type_url == xds::type_url::CLUSTER {
            std::slice::from_ref(&self.cluster_any)
        } else {
            std::slice::from_ref(&self.route_any)
        };
        let resp = encode_response(version, type_url, nonce, resources);
        self.conn.send_data(stream, &xds::grpc_frame(&resp), false);
    }

    fn serve_route_if_ready(&mut self) {
        if self.cds_served && self.route_requested {
            self.send_response(xds::type_url::ROUTE, ROUTE_VERSION, ROUTE_NONCE);
            self.route_requested = false;
        }
    }

    fn react(&mut self, ev: Event) {
        match ev {
            Event::Headers { stream_id, .. } => {
                self.stream = Some(stream_id);
                let headers = vec![
                    (b":status".to_vec(), b"200".to_vec()),
                    (b"content-type".to_vec(), b"application/grpc".to_vec()),
                ];
                self.conn.send_headers(stream_id, &headers, false);
            }
            Event::Data { data, .. } => {
                let (frames, _) = xds::grpc_unframe(&data);
                for (_, msg) in frames {
                    let (version, nonce, type_url) =
                        vane_control::xds_grpc::decode_request_fields(&msg);
                    if nonce.is_empty() {
                        // Initial subscription request. The client
                        // subscribes LDS → RDS → CDS → EDS, so RDS can
                        // arrive before CDS: serve RDS only once CDS
                        // has been served (route references the
                        // cluster), either right here or when CDS
                        // arrives later.
                        match type_url.as_str() {
                            t if t == xds::type_url::CLUSTER => {
                                self.send_response(t, CLUSTER_VERSION, CLUSTER_NONCE);
                                self.cds_served = true;
                                self.serve_route_if_ready();
                            }
                            t if t == xds::type_url::ROUTE => {
                                self.route_requested = true;
                                self.serve_route_if_ready();
                            }
                            _ => {}
                        }
                    } else if !version.is_empty() {
                        // ACK: echo recorded for the tail assertion.
                        self.ack_nonces.insert(type_url, nonce);
                    }
                }
            }
            _ => {}
        }
    }

    /// `true` once both canned responses have been ACKed (the
    /// subcommand-loop test closes the plane here).
    fn both_acked(&self) -> bool {
        self.ack_nonces.contains_key(xds::type_url::CLUSTER)
            && self.ack_nonces.contains_key(xds::type_url::ROUTE)
    }

    /// One read/flush cycle; `false` = connection over.
    fn drive_once(&mut self, sock: &mut TcpStream, backlog: &mut Vec<u8>, buf: &mut [u8]) -> bool {
        let pending = self.conn.take_pending_writes();
        if !pending.is_empty() && sock.write_all(&pending).is_err() {
            return false;
        }
        if self.conn.connection_error().is_some() {
            return false;
        }
        match sock.read(buf) {
            Ok(0) | Err(_) => false,
            Ok(n) => {
                backlog.extend_from_slice(&buf[..n]);
                loop {
                    let mut events = Vec::new();
                    let consumed = self.conn.handle_read(backlog, &mut events);
                    if consumed == 0 {
                        break;
                    }
                    backlog.drain(..consumed);
                    for ev in events {
                        self.react(ev);
                    }
                    let pending = self.conn.take_pending_writes();
                    if !pending.is_empty() && sock.write_all(&pending).is_err() {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn drive(&mut self, sock: &mut TcpStream, backlog: &mut Vec<u8>, buf: &mut [u8]) -> bool {
        loop {
            if !self.drive_once(sock, backlog, buf) {
                return false;
            }
        }
    }
}

// ---------- harness ----------

/// h1 upstream serving a canned body.
fn spawn_upstream(body: &'static str) -> SocketAddr {
    let listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            if s.write_all(resp.as_bytes()).is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// POSTs the snapshot to the admin plane over a raw h1 request.
fn post_snapshot(admin: SocketAddr, snapshot: &XdsSnapshot) -> bool {
    let body = serde_json::to_string(snapshot).expect("json");
    for _ in 0..20 {
        let Ok(mut s) = TcpStream::connect(admin) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let req = format!(
            "POST /xds/snapshot HTTP/1.1\r\nhost: admin\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        if s.write_all(req.as_bytes()).is_err() {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        let mut out = String::new();
        let _ = s.read_to_string(&mut out);
        if out.contains("200") {
            return true;
        }
        eprintln!("ADMSRV snapshot rejected: {out}");
        return false;
    }
    false
}

fn request(proxy: SocketAddr, host: &str, path: &str) -> String {
    let mut s = TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let req = format!("GET {path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd as _;
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    file
}

// ---------- the test ----------

#[test]
fn envoy_ads_snapshot_routes_live() {
    let _serial = lock_serial();

    let shop_up = spawn_upstream("shop-from-xds");
    let fallback_up = spawn_upstream("fallback-static");

    let proxy_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let proxy: SocketAddr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);
    let admin_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let admin: SocketAddr = admin_listener.local_addr().expect("addr");
    drop(admin_listener);

    let (dir, cfg) = {
        let d = tempfile::tempdir().expect("dir");
        let path = d.path().join("vane.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[[listeners]]
address = "127.0.0.1:{proxy}"

[clusters.fallback]
backends = ["{fallback_up}"]

[[routes]]
pattern = "/*rest"
cluster = "fallback"

[admin]
enabled = true
address = "127.0.0.1:{admin}"

[runtime]
force_mio = true
workers = 1
"#,
                proxy = proxy.port(),
                fallback_up = fallback_up,
                admin = admin.port(),
            ),
        )
        .expect("write");
        let p = path.to_str().expect("utf8").to_owned();
        (d, p)
    };
    let _cfg_guard = dir;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _ = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(cfg),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
            shutdown: None,
        }));
    });
    for _ in 0..60 {
        if TcpStream::connect(proxy).is_ok() && TcpStream::connect(admin).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Pre-snapshot: the static route serves every host.
    let resp = request(proxy, "shop.example.com", "/api/items");
    assert!(resp.contains("200 OK"), "static route: {resp}");
    assert!(resp.contains("fallback-static"), "static body: {resp}");

    // Management plane: hand-encoded Envoy resources pointing the
    // `shop` cluster at the live upstream.
    let cluster_proto = encode_envoy_cluster("shop", "127.0.0.1", shop_up.port());
    let cluster_any = encode_any(xds::type_url::CLUSTER, &cluster_proto);
    let route_proto = encode_envoy_route_config("shop.example.com", "/api", "shop");
    let route_any = encode_any(xds::type_url::ROUTE, &route_proto);

    let mgmt_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let mgmt: SocketAddr = mgmt_listener.local_addr().expect("addr");
    let server = std::thread::spawn(move || {
        let mut server = FakeAdsServer::new(cluster_any, route_any);
        let (mut sock, _) = mgmt_listener.accept().expect("accept");
        let mut backlog = Vec::new();
        let mut buf = [0u8; 16 * 1024];
        // Drive until the client goes away (both ACKs recorded).
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if !server.drive(&mut sock, &mut backlog, &mut buf) {
                break;
            }
        }
        server.ack_nonces
    });

    // The ADS client: subscribe all types, decode Envoy resources,
    // map, and publish to the admin plane on the accepted route.
    let client = std::thread::spawn(move || {
        use vane::xds_client::{AdsClient, PollOutcome, poll_outcome};
        let mut client = AdsClient::connect(mgmt, Duration::from_secs(10)).expect("connect");
        client.open_stream(xds::ADS_PATH).expect("open");
        let mut session = AdsSession::new("vane-envoy-e2e", "vane");
        for t in SUBSCRIBED_TYPES {
            client
                .send_message(&session.initial_request(t))
                .expect("subscribe");
        }
        let mut clusters: Vec<envoy::EnvoyCluster> = Vec::new();
        let mut route_config: Option<envoy::EnvoyRouteConfig> = None;
        let mut applied = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline && !applied {
            let outcome = match poll_outcome(&mut client, Duration::from_millis(200)) {
                Ok(o) => o,
                Err(e) => panic!("poll: {e}"),
            };
            let messages = match outcome {
                PollOutcome::Closed => panic!("management closed the stream early"),
                PollOutcome::Idle => continue,
                PollOutcome::Messages(m) => m,
            };
            for msg in messages {
                let Some(response) = xds::decode_discovery_response(&msg) else {
                    continue;
                };
                let type_url = response.type_url.clone();
                let resources = response.resources.clone();
                let validate = |res: &[Vec<u8>]| -> Result<(), String> {
                    if type_url == xds::type_url::CLUSTER {
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
                    } else if type_url == xds::type_url::ROUTE {
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
                let Some(decision) = session.on_response(
                    &type_url,
                    &response.version_info,
                    &response.nonce,
                    resources,
                    validate,
                ) else {
                    continue;
                };
                match decision {
                    AdsDecision::Ack { .. } => {
                        client
                            .send_message(&session.ack_request(&type_url))
                            .expect("ack send");
                    }
                    AdsDecision::Nack { error } => {
                        client
                            .send_message(&session.nack_request(&type_url, &error))
                            .expect("nack send");
                        panic!("management NACK {type_url}: {error}");
                    }
                }
                // Both resources accepted: map + publish the snapshot.
                if type_url == xds::type_url::ROUTE {
                    let Some(rc) = &route_config else {
                        continue;
                    };
                    let mut snapshot = envoy::map_snapshot(&clusters, rc);
                    snapshot.version = "envoy-v1".into();
                    assert!(
                        post_snapshot(admin, &snapshot),
                        "admin rejected the snapshot"
                    );
                    applied = true;
                }
            }
        }
        assert!(applied, "snapshot never applied within deadline");
    });
    client.join().expect("client thread");

    // Post-snapshot: host-scoped, prefix-routed via Envoy resources.
    let resp = request(proxy, "shop.example.com", "/api/items");
    assert!(resp.contains("200 OK"), "xds route: {resp}");
    assert!(resp.contains("shop-from-xds"), "xds body: {resp}");

    // Other host: the snapshot displaced the static catch-all.
    let resp = request(proxy, "other.example.com", "/api/items");
    assert!(resp.contains("404"), "host scoping: {resp}");

    // Right host, prefix mismatch.
    let resp = request(proxy, "shop.example.com", "/nope");
    assert!(resp.contains("404"), "prefix scoping: {resp}");

    // The admin plane recorded the snapshot version.
    let mut s = TcpStream::connect(admin).expect("admin connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(b"GET /xds/version HTTP/1.1\r\nhost: admin\r\nconnection: close\r\n\r\n")
        .expect("version write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    assert!(out.contains("envoy-v1"), "admin version: {out}");

    // The management plane saw our ACK nonces (per-type echo).
    let acks = server.join().expect("server thread");
    assert_eq!(
        acks.get(xds::type_url::CLUSTER).map(String::as_str),
        Some(CLUSTER_NONCE),
        "CDS ACK nonce"
    );
    assert_eq!(
        acks.get(xds::type_url::ROUTE).map(String::as_str),
        Some(ROUTE_NONCE),
        "RDS ACK nonce"
    );
}

/// Drives the REAL `vane xds-client` loop (`xds_client_loop`) against
/// the fake plane: subscribe → decode Envoy resources → ACK → map →
/// POST to the admin sink → management closes the stream → the loop
/// exits with the transport-failure code.
#[test]
fn xds_client_subcommand_loop_publishes_snapshot() {
    let shop_up = spawn_upstream("shop-from-xds");

    let cluster_proto = encode_envoy_cluster("shop", "127.0.0.1", shop_up.port());
    let cluster_any = encode_any(xds::type_url::CLUSTER, &cluster_proto);
    let route_proto = encode_envoy_route_config("shop.example.com", "/api", "shop");
    let route_any = encode_any(xds::type_url::ROUTE, &route_proto);

    let mgmt_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let mgmt: SocketAddr = mgmt_listener.local_addr().expect("addr");
    let server = std::thread::spawn(move || {
        let mut server = FakeAdsServer::new(cluster_any, route_any);
        let (mut sock, _) = mgmt_listener.accept().expect("accept");
        let mut backlog = Vec::new();
        let mut buf = [0u8; 16 * 1024];
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if server.both_acked() {
                // Both generations accepted: close the stream — the
                // subcommand loop treats this as transport failure and
                // exits (code 1).
                break;
            }
            if !server.drive_once(&mut sock, &mut backlog, &mut buf) {
                break;
            }
        }
        // Dropping `sock` closes the stream.
    });

    // Admin sink: accepts the snapshot POST, reads exactly
    // head + content-length body (deterministic — the client keeps
    // the connection alive), stores the request, answers 200.
    let sink_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let sink: SocketAddr = sink_listener.local_addr().expect("addr");
    let sink_body = Arc::new(std::sync::Mutex::new(String::new()));
    let body_for_assert = std::sync::Arc::clone(&sink_body);
    let body_for_test = std::sync::Arc::clone(&sink_body);
    std::thread::spawn(move || {
        for stream in sink_listener.incoming().flatten() {
            let mut s = stream;
            // Read to the head terminator.
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if s.read_exact(&mut byte).is_err() {
                    break;
                }
                head.push(byte[0]);
            }
            let mut req = String::from_utf8_lossy(&head).into_owned();
            // Body per content-length.
            let head_str = String::from_utf8_lossy(&head).to_ascii_lowercase();
            let len = head_str
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; len];
            if s.read_exact(&mut body).is_ok() {
                req.push_str(&String::from_utf8_lossy(&body));
            }
            let _ =
                s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            *body_for_assert.lock().expect("sink") = req;
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    });

    let code = vane::xds_client::xds_client_loop(
        &format!("127.0.0.1:{}", mgmt.port()),
        "vane-envoy-e2e",
        &format!("http://127.0.0.1:{}", sink.port()),
    );
    // The management plane closed the stream after both ACKs: the
    // subcommand reports the transport failure.
    assert_eq!(code, 1, "loop exit code after mgmt close");

    let body = body_for_test.lock().expect("sink").clone();
    assert!(
        body.contains("POST /xds/snapshot"),
        "sink saw the POST: {body}"
    );
    assert!(
        body.contains("\"shop\""),
        "snapshot carries the cluster: {body}"
    );
    assert!(
        body.contains("/api/*rest"),
        "snapshot carries the envoy prefix route: {body}"
    );

    server.join().expect("server thread");
}
