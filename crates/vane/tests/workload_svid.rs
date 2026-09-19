//! Workload API SVID source e2e (mesh milestone 3): a fake SPIRE
//! agent streams SVID A, then B, on the `FetchX509SVID` watch stream;
//! the proxy materializes both to the configured cache paths and
//! rotates the identity it presents upstream. The mTLS upstream
//! records the peer certificate of every connection.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener as StdListener};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};
use vane_proto::{pb, xds};

const SPIFFE_ID: &str = "spiffe://example.org/vane/proxy";

// ---------- identity material ----------

struct SvidMaterial {
    /// DER-encoded leaf certificate.
    cert_der: Vec<u8>,
    /// PKCS#8 DER private key.
    key_der: Vec<u8>,
}

struct Materials {
    /// PEM paths for the upstream's own server cert + key.
    srv_cert: String,
    srv_key: String,
    /// PEM path of the mesh CA (upstream trust store).
    ca: String,
    /// DER of the mesh CA (the Workload API bundle payload).
    ca_der: Vec<u8>,
    a: SvidMaterial,
    b: SvidMaterial,
}

/// Mesh CA; two workload SVIDs (A then B, distinct key pairs, same
/// SPIFFE ID); the upstream server cert (DNS mesh.local + SPIFFE URI
/// SAN) — all chaining to the one CA.
fn gen_materials(dir: &std::path::Path) -> Materials {
    use rcgen::{CertificateParams, KeyPair, SanType};
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(vec!["mesh-ca".into()]).expect("params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let gen_svid = || {
        let mut params = CertificateParams::new(vec!["svid".into()]).expect("params");
        params.subject_alt_names = vec![SanType::URI(
            rcgen::string::Ia5String::try_from(SPIFFE_ID).expect("uri"),
        )];
        let key = KeyPair::generate().expect("key");
        let cert = params.signed_by(&key, &issuer).expect("cert");
        SvidMaterial {
            cert_der: cert.der().to_vec(),
            key_der: key.serialize_der(),
        }
    };

    let mut up_params = CertificateParams::new(vec!["mesh.local".into()]).expect("params");
    up_params.subject_alt_names = vec![
        SanType::DnsName(rcgen::string::Ia5String::try_from("mesh.local").expect("dns")),
        SanType::URI(
            rcgen::string::Ia5String::try_from("spiffe://example.org/vane/upstream").expect("uri"),
        ),
    ];
    let up_key = KeyPair::generate().expect("up key");
    let up_cert = up_params.signed_by(&up_key, &issuer).expect("up cert");

    let srv_cert = dir.join("up.pem");
    let srv_key = dir.join("up-key.pem");
    let ca_pem = dir.join("ca.pem");
    std::fs::write(&srv_cert, up_cert.pem()).expect("write");
    std::fs::write(&srv_key, up_key.serialize_pem()).expect("write");
    std::fs::write(&ca_pem, ca.pem()).expect("write");

    Materials {
        srv_cert: srv_cert.display().to_string(),
        srv_key: srv_key.display().to_string(),
        ca: ca_pem.display().to_string(),
        ca_der: ca.der().to_vec(),
        a: gen_svid(),
        b: gen_svid(),
    }
}

/// `X509SVIDResponse` with one SVID entry (proto wire shape).
fn encode_svid_response(m: &SvidMaterial, bundle_der: &[u8]) -> Vec<u8> {
    let mut svid_msg = Vec::new();
    pb::string_field(&mut svid_msg, 1, SPIFFE_ID);
    pb::bytes_field(&mut svid_msg, 2, &m.cert_der);
    pb::bytes_field(&mut svid_msg, 3, &m.key_der);
    pb::bytes_field(&mut svid_msg, 4, bundle_der);
    let mut resp = Vec::new();
    pb::message_field(&mut resp, 1, &svid_msg);
    resp
}

// ---------- fake SPIRE agent ----------

/// Fake Workload API agent on a Unix socket: every `FetchX509SVID`
/// watch gets SVID A; once the test fires the trigger, the watch
/// stream (connection ≥ 2 — the first connection is the synchronous
/// startup fetch) also receives SVID B.
fn spawn_agent(
    socket_path: PathBuf,
    bundle_der: Arc<Vec<u8>>,
    a: Arc<SvidMaterial>,
    b: Arc<SvidMaterial>,
    trigger: std::sync::mpsc::Receiver<()>,
) {
    std::thread::spawn(move || {
        let listener = UnixListener::bind(&socket_path).expect("unix bind");
        for (idx, stream) in listener.incoming().enumerate() {
            let Ok(mut sock) = stream else { continue };
            let mut conn = Connection::new(Role::Server, ConnectionConfig::default());
            let mut backlog = Vec::new();
            let mut buf = [0u8; 16 * 1024];
            let mut stream_id: Option<u32> = None;
            let mut served = false;
            // The startup fetch (conn 1) does not wait for rotation.
            let mut watch_for_rotation = idx >= 1;
            'conn: loop {
                let pending = conn.take_pending_writes();
                if !pending.is_empty() && sock.write_all(&pending).is_err() {
                    break 'conn;
                }
                match sock.read(&mut buf) {
                    Ok(0) | Err(_) => break 'conn,
                    Ok(n) => {
                        backlog.extend_from_slice(&buf[..n]);
                        loop {
                            let mut events = Vec::new();
                            let consumed = conn.handle_read(&backlog, &mut events);
                            if consumed == 0 {
                                break;
                            }
                            backlog.drain(..consumed);
                            for ev in events {
                                match ev {
                                    Event::Headers { stream_id: id, .. } => {
                                        stream_id = Some(id);
                                        let headers = vec![
                                            (b":status".to_vec(), b"200".to_vec()),
                                            (
                                                b"content-type".to_vec(),
                                                b"application/grpc".to_vec(),
                                            ),
                                        ];
                                        conn.send_headers(id, &headers, false);
                                    }
                                    Event::Data { data, .. } => {
                                        if served {
                                            continue;
                                        }
                                        let (frames, _) = xds::grpc_unframe(&data);
                                        if frames.is_empty() {
                                            continue;
                                        }
                                        served = true;
                                        let Some(id) = stream_id else { continue };
                                        conn.send_data(
                                            id,
                                            &xds::grpc_frame(&encode_svid_response(
                                                &a,
                                                &bundle_der,
                                            )),
                                            false,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                // After A is served on a watch stream, wait for the
                // rotation trigger and push B on the same stream.
                if watch_for_rotation && served {
                    watch_for_rotation = false;
                    if trigger.recv_timeout(Duration::from_secs(20)).is_ok() {
                        let Some(id) = stream_id else { continue };
                        conn.send_data(
                            id,
                            &xds::grpc_frame(&encode_svid_response(&b, &bundle_der)),
                            false,
                        );
                    }
                }
            }
        }
    });
}

// ---------- mTLS upstream ----------

/// Requires a client cert chaining to the mesh CA; records (request,
/// peer leaf cert DER) per connection and answers one h1 200.
fn spawn_mtls_upstream(
    listener: StdListener,
    srv_cert: String,
    srv_key: String,
    ca: String,
) -> std::sync::mpsc::Receiver<(String, Vec<u8>)> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(&srv_cert).expect("srv cert"),
        ))
        .map(Result::unwrap)
        .collect();
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
            std::fs::File::open(&srv_key).expect("srv key"),
        ))
        .expect("pem")
        .expect("key");
        let client_cas: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(&ca).expect("ca"),
        ))
        .map(Result::unwrap)
        .collect();
        let mut roots = rustls::RootCertStore::empty();
        for c in client_cas {
            roots.add(c).expect("root");
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .expect("verifier");
        let mut cfg = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .expect("server cert");
        cfg.alpn_protocols = vec![b"vane-mesh".to_vec()];
        let cfg = Arc::new(cfg);
        let mut buf = [0u8; 4096];
        for stream in listener.incoming().flatten() {
            let mut tls = match rustls::ServerConnection::new(cfg.clone()) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let mut sock = stream;
            let mut plain = Vec::new();
            'conn: loop {
                let mut out = Vec::new();
                let _ = tls.write_tls(&mut out);
                if !out.is_empty() && sock.write_all(&out).is_err() {
                    break 'conn;
                }
                match sock.read(&mut buf) {
                    Ok(0) | Err(_) => break 'conn,
                    Ok(n) => {
                        let _ = tls.read_tls(&mut &buf[..n]);
                        if tls.process_new_packets().is_err() {
                            break 'conn;
                        }
                        let mut tmp = [0u8; 4096];
                        loop {
                            match tls.reader().read(&mut tmp) {
                                Ok(0) => break,
                                Ok(m) => plain.extend_from_slice(&tmp[..m]),
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(_) => break,
                            }
                        }
                    }
                }
                if !tls.is_handshaking() && !plain.is_empty() {
                    break;
                }
            }
            let peer = tls
                .peer_certificates()
                .and_then(|c| c.first())
                .map(|c| c.as_ref().to_vec())
                .unwrap_or_default();
            let _ = tx.send((String::from_utf8_lossy(&plain).into_owned(), peer));
            let resp = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
            let _ = tls.writer().write_all(resp);
            loop {
                let mut out = Vec::new();
                let _ = tls.write_tls(&mut out);
                if out.is_empty() {
                    break;
                }
                if sock.write_all(&out).is_err() {
                    break;
                }
            }
            let _ = sock.shutdown(std::net::Shutdown::Both);
        }
    });
    rx
}

// ---------- harness ----------

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

fn request(proxy: SocketAddr, path: &str) -> String {
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let req = format!("GET {path} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

/// Waits for the upstream to report a connection, then asserts the
/// request and identity.
fn recv_upstream(rx: &std::sync::mpsc::Receiver<(String, Vec<u8>)>, want_request: &str) -> Vec<u8> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "upstream never relayed {want_request}"
        );
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok((req, peer)) if req.contains(want_request) => return peer,
            Ok(_) => continue,
            Err(_) => continue,
        }
    }
}

/// The full rotation flow: A presented upstream → agent pushes B →
/// cache rewritten → next upstream dial presents B.
#[test]
fn workload_api_svid_rotation() {
    let _serial = lock_serial();
    let dir = tempfile::tempdir().expect("dir");
    let Materials {
        srv_cert,
        srv_key,
        ca,
        ca_der,
        a,
        b,
    } = gen_materials(dir.path());

    // Fake agent + cache paths (the proxy materializes here).
    let cache = dir.path().join("svid-cache");
    let socket_path = dir.path().join("workload.sock");
    let (trigger_tx, trigger_rx) = std::sync::mpsc::channel();
    spawn_agent(
        socket_path.clone(),
        Arc::new(ca_der),
        Arc::new(a),
        Arc::new(b),
        trigger_rx,
    );

    // mTLS upstream.
    let upstream = StdListener::bind("127.0.0.1:0").expect("bind");
    let upstream_addr: SocketAddr = upstream.local_addr().expect("addr");
    let rx = spawn_mtls_upstream(upstream, srv_cert, srv_key, ca);

    // Proxy: mesh upstream sourced from the Workload API.
    let proxy_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let proxy_addr: SocketAddr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);
    let (dir2, cfg) = {
        let d = tempfile::tempdir().expect("dir");
        let path = d.path().join("vane.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[[listeners]]
address = "127.0.0.1:{proxy}"

[clusters.mesh-up]
backends = ["{upstream}"]

[clusters.mesh-up.mesh]
svid_socket = "{socket}"
cert = "{cache}/cert.pem"
key = "{cache}/key.pem"
ca = "{cache}/ca.pem"
server_name = "mesh.local"
spiffe_prefix = "spiffe://example.org/vane/"

[[routes]]
pattern = "/*rest"
cluster = "mesh-up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#,
                proxy = proxy_addr.port(),
                upstream = upstream_addr,
                socket = socket_path.display(),
                cache = cache.display(),
            ),
        )
        .expect("write");
        let p = path.to_str().expect("utf8").to_owned();
        (d, p)
    };
    let _cfg_guard = dir2;
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
        }));
    });
    let cert_pem_path = cache.join("cert.pem");
    let first_pem = wait_cache_cert(&cert_pem_path);

    // 1. First request presents SVID A.
    let resp = request(proxy_addr, "/one");
    assert!(resp.contains("200 OK"), "first relay: {resp}");
    let peer = recv_upstream(&rx, "GET /one");
    let a_der = pem_to_der(&first_pem);
    assert_eq!(peer, a_der, "first dial presented SVID A");

    // 2. The agent pushes B on the watch stream.
    trigger_tx.send(()).expect("trigger");

    // 3. The watcher rewrites the cache.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pem = std::fs::read_to_string(&cert_pem_path).expect("cache cert");
        if pem != first_pem {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cache cert never rotated"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // 4. The next dial presents B.
    let resp = request(proxy_addr, "/two");
    assert!(resp.contains("200 OK"), "second relay: {resp}");
    let peer = recv_upstream(&rx, "GET /two");
    assert_ne!(peer, a_der, "second dial still presented SVID A");
}

/// PEM (single CERTIFICATE block) → DER, for comparing the cache file
/// against the peer cert the upstream reported.
fn pem_to_der(pem: &str) -> Vec<u8> {
    let mut buf = vec![0u8; pem.len()];
    let (_, der) = pem_rfc7468::decode(pem.as_bytes(), &mut buf).expect("pem decode");
    der.to_vec()
}

fn wait_cache_cert(path: &std::path::Path) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(pem) = std::fs::read_to_string(path) {
            return pem;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cache cert never materialized at {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
