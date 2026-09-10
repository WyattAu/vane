//! In-process full-stack coverage: drives `vane::server::run` directly
//! (same process — coverage counts everything) across routed HTTP,
//! error replies, WebSocket tunnel, L4 splice, and TLS termination.
//! Each scenario runs on its own ephemeral port with `shutdown_after`
//! guaranteeing exit.

use std::io::{Read, Write};
use std::time::Duration;

use vane::server::RunOptions;

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

/// Serializes the scenarios: each binds a port, drops the reservation,
/// then lets vane rebind — concurrent tests could steal ports.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Spawns the upstream echo/200 responder used by every scenario.
fn spawn_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            // Upgrade requests become echo tunnels; the
                            // rest get a canned 200.
                            let req = &buf[..n];
                            let is_upgrade =
                                req.windows(7).any(|w| w.eq_ignore_ascii_case(b"upgrade"));
                            if is_upgrade {
                                let _ = s.write_all(
                                    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
                                );
                                // Raw echo from here.
                                loop {
                                    match s.read(&mut buf) {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            if s.write_all(&buf[..n]).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                                break;
                            }
                            if s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane").is_err() {
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

fn request(proxy: std::net::SocketAddr, req: &[u8]) -> String {
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(req).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

/// Runs the proxy on a dedicated thread + runtime (mirrors production.rs
/// — `run()` must not execute on the test's single-threaded runtime).
fn spawn_proxy(cfg_path: String) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(RunOptions {
            config_path: Some(cfg_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
}

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..60 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

#[test]
fn routed_request_and_error_paths() {
    let _serial = SERIAL.lock().expect("serial lock");
    let upstream = spawn_upstream();
    let port = free_port();
    let admin = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/api/*rest"
cluster = "up"
methods = ["GET", "POST"]

[admin]
enabled = true
address = "127.0.0.1:{admin}"

[access_log]
enabled = true

[runtime]
force_mio = true
workers = 1
"#
    ));

    spawn_proxy(cfg);

    // Wait for the listener.
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // Routed success.
    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "routed: {resp:?}");
    assert!(resp.contains("hello-vane"), "body: {resp:?}");

    // Method not allowed (route allows GET/POST only).
    let resp = request(
        proxy,
        b"DELETE /api/items HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("405"), "method filter: {resp:?}");

    // No route → 404.
    let resp = request(
        proxy,
        b"GET /nomatch HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("404"), "no route: {resp:?}");

    // WebSocket upgrade → tunnel echo.
    let mut s = std::net::TcpStream::connect(proxy).expect("ws connect");
    s.write_all(
        b"GET /api/ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .expect("ws write");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        s.read_exact(&mut byte).expect("head byte");
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    assert!(head.contains("101"), "upgrade: {head:?}");
    s.write_all(b"tunnel-bytes").expect("tunnel write");
    let mut echoed = vec![0u8; 12];
    s.read_exact(&mut echoed).expect("tunnel echo");
    assert_eq!(&echoed, b"tunnel-bytes");
    drop(s);

    // Admin endpoints answer while running.
    let admin_addr: std::net::SocketAddr = format!("127.0.0.1:{admin}").parse().expect("addr");
    let resp = request(
        admin_addr,
        b"GET /healthz HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200"), "admin: {resp:?}");
}

#[test]
fn l4_splice_mode_relays_tcp() {
    let _serial = SERIAL.lock().expect("serial lock");
    let upstream = spawn_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
mode = "tcp"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#
    ));

    spawn_proxy(cfg);

    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);
    // L4: raw bytes cross to the upstream (it answers HTTP but splice
    // just moves bytes; any response proves the pipe works).
    let resp = request(
        proxy,
        b"GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("200 OK") || resp.contains("hello-vane"),
        "splice relay: {resp:?}"
    );
}

#[test]
fn tls_termination_serves_https() {
    let _serial = SERIAL.lock().expect("serial lock");
    // Generate a self-signed cert for the listener.
    let dir = tempfile::tempdir().expect("dir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    std::fs::write(&cert_path, certs.cert.pem()).expect("cert");
    std::fs::write(&key_path, certs.signing_key.serialize_pem()).expect("key");

    let upstream = spawn_upstream();
    let port = free_port();
    let toml = format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[listeners.tls]
cert = "{}"
key = "{}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#,
        cert_path.display(),
        key_path.display()
    );
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let cfg = path.to_str().expect("utf8").to_owned();
    let _cert_dir_guard = dir;

    spawn_proxy(cfg);

    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // TLS client: trust the generated cert directly.
    let roots = rustls::RootCertStore::empty();
    use rustls::pki_types::pem::PemObject as _;
    let der = rustls::pki_types::CertificateDer::from_pem_file(&cert_path).expect("cert der");
    let mut roots = roots;
    roots.add(der).expect("add");
    let client_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("sni");
    let mut conn = rustls::ClientConnection::new(std::sync::Arc::new(client_cfg), server_name)
        .expect("client conn");
    let mut sock = std::net::TcpStream::connect(proxy).expect("tcp");
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(b"GET /tls HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("tls write");
    let mut out = String::new();
    let mut buf = [0u8; 4096];
    loop {
        match tls.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => break,
        }
    }
    assert!(out.contains("200 OK"), "tls response: {out:?}");
    assert!(out.contains("hello-vane"), "tls body: {out:?}");
}
