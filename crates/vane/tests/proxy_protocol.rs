//! PROXY protocol e2e: a listener with `proxy_protocol = true` requires
//! the header at connection start, attributes the connection to the
//! address it carries (client IP preservation behind an L4 load
//! balancer), and leaves every later byte — TLS handshake included —
//! to the normal intake. The header precedes the TLS handshake, which
//! is the case that breaks naive implementations.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd as _;
    let path = std::env::temp_dir().join("vane-tests-pp.lock");
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

fn spawn_proxy(cfg_path: String) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _ = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(cfg_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: None,
            force_mio: true,
        }));
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

/// Echoes the client-facing headers an upstream would see.
fn spawn_header_upstream() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let Ok(n) = s.read(&mut buf) else { return };
                let req = String::from_utf8_lossy(&buf[..n]).to_lowercase();
                let pick = |name: &str| {
                    req.lines()
                        .find(|l| l.starts_with(name))
                        .and_then(|l| l.split_once(':'))
                        .map(|(_, v)| v.trim().to_owned())
                        .unwrap_or_default()
                };
                let xff = pick("x-forwarded-for:");
                let body = format!("xff={xff}");
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            });
        }
    });
    addr
}

/// PROXY v2 binary header, TCP/IPv4.
fn v2_tcp4(src: [u8; 4], sport: u16) -> Vec<u8> {
    let mut out = b"\r\n\r\n\x00\r\nQUIT\n".to_vec();
    out.push(0x21); // v2, PROXY
    out.push(0x11); // TCP/IPv4
    out.extend_from_slice(&12u16.to_be_bytes());
    out.extend_from_slice(&src);
    out.extend_from_slice(&[10, 0, 0, 1]); // dst
    out.extend_from_slice(&sport.to_be_bytes());
    out.extend_from_slice(&443u16.to_be_bytes());
    out
}

fn v1(src: &str, sport: u16) -> Vec<u8> {
    format!("PROXY TCP4 {src} 10.0.0.1 {sport} 443\r\n").into_bytes()
}

/// v2 header + plain h1 request in one stream: the upstream must see
/// the CLAIMED client address in X-Forwarded-For, not the loopback
/// socket the connection really came from.
#[test]
fn proxy_protocol_v2_feeds_forwarded_for() {
    let _serial = lock_serial();
    let upstream = spawn_header_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
proxy_protocol = true

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = false
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut wire = v2_tcp4([203, 0, 113, 7], 56789);
    wire.extend_from_slice(b"GET /data HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    c.write_all(&wire).expect("write");
    let mut resp = String::new();
    let _ = c.read_to_string(&mut resp);
    assert!(
        resp.contains("xff=203.0.113.7"),
        "the claimed address is the client: {resp}"
    );
    assert!(
        !resp.contains("127.0.0.1"),
        "the real socket peer must not leak: {resp}"
    );
}

/// Same contract over the v1 text format.
#[test]
fn proxy_protocol_v1_feeds_forwarded_for() {
    let _serial = lock_serial();
    let upstream = spawn_header_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
proxy_protocol = true

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = false
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut wire = v1("203.0.113.9", 12345);
    wire.extend_from_slice(b"GET /data HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    c.write_all(&wire).expect("write");
    let mut resp = String::new();
    let _ = c.read_to_string(&mut resp);
    assert!(
        resp.contains("xff=203.0.113.9"),
        "v1 source claimed: {resp}"
    );
}

/// A malformed header on a header-required listener closes the
/// connection instead of guessing where the request begins.
#[test]
fn proxy_protocol_malformed_header_closes_the_connection() {
    let _serial = lock_serial();
    let upstream = spawn_header_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
proxy_protocol = true

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = false
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // Corrupt v2: wrong version nibble, declared after the signature.
    let mut bad = b"\r\n\r\n\x00\r\nQUIT\n".to_vec();
    bad.push(0x31); // version 3: not spoken
    bad.push(0x11);
    bad.extend_from_slice(&12u16.to_be_bytes());
    bad.extend_from_slice(&[0u8; 12]);
    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();
    c.write_all(&bad).expect("write");
    let mut out = Vec::new();
    let _ = c.read_to_end(&mut out);
    assert!(
        out.is_empty(),
        "a malformed header closes the connection with no response: {out:?}"
    );

    // And a plain request (no header at all) is equally refused — the
    // header is REQUIRED, which is the "do not mix modes" rule.
    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();
    c.write_all(b"GET /data HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut out = Vec::new();
    let _ = c.read_to_end(&mut out);
    assert!(out.is_empty(), "header-less requests are refused: {out:?}");
}

/// The header precedes the TLS handshake: after it is stripped, the
/// remaining bytes are a valid ClientHello and the TLS request flows —
/// with the claimed address in X-Forwarded-For.
#[cfg(feature = "h2")]
#[test]
fn proxy_protocol_precedes_the_tls_handshake() {
    let _serial = lock_serial();
    let upstream = spawn_header_upstream();

    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let cert_p = tempfile::NamedTempFile::new()
        .expect("tmp")
        .into_temp_path();
    let key_p = tempfile::NamedTempFile::new()
        .expect("tmp")
        .into_temp_path();
    std::fs::write(&cert_p, certs.cert.pem()).expect("write cert");
    std::fs::write(&key_p, certs.signing_key.serialize_pem()).expect("write key");

    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
proxy_protocol = true

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
"#,
        cert_p.display(),
        key_p.display(),
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut roots = rustls::RootCertStore::empty();
    let der = <rustls::pki_types::CertificateDer<'static> as rustls::pki_types::pem::PemObject>::from_pem_slice(
        certs.cert.pem().as_bytes(),
    )
    .expect("cert der");
    roots.add(der).expect("root");
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("name");
    let mut conn =
        rustls::ClientConnection::new(Arc::new(client_tls), server_name).expect("client conn");
    let mut sock = std::net::TcpStream::connect(proxy).expect("tcp");
    sock.set_read_timeout(Some(Duration::from_secs(10))).ok();

    // The PROXY header goes on the wire BEFORE the ClientHello.
    let header = v2_tcp4([203, 0, 113, 42], 34567);
    sock.write_all(&header).expect("write header");

    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(b"GET /data HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
        .expect("tls write");
    let mut out = Vec::new();
    let _ = tls.read_to_end(&mut out);
    let resp = String::from_utf8_lossy(&out);
    assert!(
        resp.contains("xff=203.0.113.42"),
        "claimed address survives the TLS hop: {resp}"
    );
}

use std::sync::Arc;
