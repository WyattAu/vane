//! Mesh mTLS upstream e2e: an upstream that REQUIRES a client
//! certificate (chain = mesh CA) and serves h1 over TLS; the proxy
//! presents its SVID, verifies the upstream's SPIFFE SAN, and relays.

use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

fn gen_mesh_materials(dir: &std::path::Path) -> MeshPaths {
    use rcgen::{CertificateParams, KeyPair, SanType};
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(vec!["mesh-ca".into()]).expect("params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");

    let spiffe = "spiffe://example.org/vane/upstream";
    // Upstream server cert: DNS "mesh.local" + SPIFFE URI SAN.
    let mut srv_params = CertificateParams::new(vec!["mesh.local".into()]).expect("params");
    srv_params.subject_alt_names = vec![
        SanType::DnsName(rcgen::string::Ia5String::try_from("mesh.local").expect("dns")),
        SanType::URI(rcgen::string::Ia5String::try_from(spiffe).expect("uri")),
    ];
    let srv_key = KeyPair::generate().expect("srv key");
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let srv = srv_params.signed_by(&srv_key, &issuer).expect("srv");

    // Proxy client SVID: SPIFFE URI SAN only.
    let mut cli_params = CertificateParams::new(vec!["proxy".into()]).expect("params");
    cli_params.subject_alt_names = vec![SanType::URI(
        rcgen::string::Ia5String::try_from("spiffe://example.org/vane/proxy").expect("uri"),
    )];
    let cli_key = KeyPair::generate().expect("cli key");
    let cli = cli_params.signed_by(&cli_key, &issuer).expect("cli");

    let srv_cert = dir.join("srv.pem");
    let srv_key_p = dir.join("srv-key.pem");
    let cli_cert = dir.join("cli.pem");
    let cli_key_p = dir.join("cli-key.pem");
    let ca_pem = dir.join("ca.pem");
    std::fs::write(&srv_cert, srv.pem()).expect("write");
    std::fs::write(&srv_key_p, srv_key.serialize_pem()).expect("write");
    std::fs::write(&cli_cert, cli.pem()).expect("write");
    std::fs::write(&cli_key_p, cli_key.serialize_pem()).expect("write");
    std::fs::write(&ca_pem, ca.pem()).expect("write");
    MeshPaths {
        srv_cert: srv_cert.display().to_string(),
        srv_key: srv_key_p.display().to_string(),
        cli_cert: cli_cert.display().to_string(),
        cli_key: cli_key_p.display().to_string(),
        ca: ca_pem.display().to_string(),
    }
}

struct MeshPaths {
    srv_cert: String,
    srv_key: String,
    cli_cert: String,
    cli_key: String,
    ca: String,
}

/// Upstream: rustls server requiring a client cert signed by the mesh
/// CA; serves one h1 response per mTLS connection.
fn spawn_mtls_upstream(
    listener: std::net::TcpListener,
    srv_cert: String,
    srv_key: String,
    ca: String,
) -> std::sync::mpsc::Receiver<String> {
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
            let _ = tx.send(String::from_utf8_lossy(&plain).into_owned());
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

/// Serializes server-heavy tests within this binary (same shape as
/// h2_upstream's lock; key includes the test binary path).
fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd as _;
    let key = {
        let exe = std::env::current_exe().unwrap_or_default();
        let mut h: u64 = 5381;
        for b in exe.to_string_lossy().as_bytes() {
            h = h.wrapping_mul(33).wrapping_add(u64::from(*b));
        }
        format!("{h:016x}")
    };
    let path = std::env::temp_dir().join(format!("vane-tests-serial-{key}.lock"));
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

// IN PROGRESS: the mTLS handshake with the upstream completes (790B
// flight processed, no intake errors) but the downstream closes before
// the response relays. Trace: DIAL fd=62 → RDNOW 790 → RDNOW 24 →
// client EOF. Next: probe the post-handshake intake branch (ciphertext
// flush + plaintext dispatch) and the h1 head send ordering (the head
// is rustls-buffered pre-handshake and drained with the Finished).
#[test]
fn mesh_mtls_upstream_roundtrip() {
    let _serial = lock_serial();
    let dir = tempfile::TempDir::new().expect("dir");
    let m = gen_mesh_materials(dir.path());

    let upstream = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let upstream_addr: SocketAddr = upstream.local_addr().expect("addr");
    let rx = spawn_mtls_upstream(
        upstream,
        m.srv_cert.clone(),
        m.srv_key.clone(),
        m.ca.clone(),
    );

    let proxy_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let proxy_addr: SocketAddr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);

    let (dir2, cfg) = {
        let d = tempfile::TempDir::new().expect("dir");
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
cert = "{cli_cert}"
key = "{cli_key}"
ca = "{ca}"
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
                cli_cert = m.cli_cert,
                cli_key = m.cli_key,
                ca = m.ca,
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
    for _ in 0..60 {
        if std::net::TcpStream::connect(proxy_addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let mut sock = std::net::TcpStream::connect(proxy_addr).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
    sock.write_all(b"GET /mesh/data HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
        .expect("write");
    let mut bytes = Vec::new();
    let _ = std::io::Read::read_to_end(&mut sock, &mut bytes);
    eprintln!(
        "CLIRSP {} bytes: {:02x?}",
        bytes.len(),
        &bytes[..bytes.len().min(40)]
    );
    let head = String::from_utf8_lossy(&bytes);
    assert!(head.contains("200 OK"), "proxy response: {head}");
    // The upstream saw the relayed request on the mTLS connection.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut saw_request = false;
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(seen) if seen.contains("GET /mesh/data") => {
                saw_request = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    assert!(saw_request, "upstream never saw the request");
}
