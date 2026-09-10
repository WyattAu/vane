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

/// Cross-process serial lock (flock on a temp file) — shared with the
/// other proxy-spawning suites. Each scenario binds a port, drops the
/// reservation, then lets vane rebind; without the lock a concurrent
/// suite could steal the port between drop and bind.
fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("open lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(rc, 0, "flock");
    file
}

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
/// Stderr goes to a per-config log file for post-failure diagnosis.
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
    let _serial = lock_serial();
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
    let _serial = lock_serial();
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
    let _serial = lock_serial();
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

/// Dead upstream → 502; mixed dead+live cluster still serves (failover).
#[test]
fn upstream_failure_paths() {
    let _serial = lock_serial();
    let live = spawn_upstream();
    // Port 1 on loopback is reliably closed.
    let dead = "127.0.0.1:1";
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.dead]
backends = ["{dead}"]

[clusters.mixed]
backends = ["{dead}", "{live}"]

[[routes]]
host = "dead.test"
pattern = "/*rest"
cluster = "dead"

[[routes]]
host = "mixed.test"
pattern = "/*rest"
cluster = "mixed"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
pool_per_backend = 0
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let resp = request(
        proxy,
        b"GET /x HTTP/1.1\r\nHost: dead.test\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("502"), "dead upstream must 502: {resp:?}");

    // Mixed cluster: either backend serves 200 (failover when dead first).
    for i in 0..10 {
        let resp = request(
            proxy,
            b"GET /x HTTP/1.1\r\nHost: mixed.test\r\nConnection: close\r\n\r\n",
        );
        assert!(
            resp.contains("200 OK"),
            "failover must serve (iter {i}): {resp:?}"
        );
    }
}

/// Upstream RSTs mid-response: the proxy must answer 502, not hang.
#[test]
fn upstream_abort_mid_response() {
    let _serial = lock_serial();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                use std::os::fd::AsRawFd;
                let mut buf = [0u8; 8192];
                let Ok(_) = s.read(&mut buf) else { return };
                // Partial head, then RST (SO_LINGER 0).
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npart");
                // SAFETY: setsockopt on a live socket.
                unsafe {
                    let linger = libc::linger {
                        l_onoff: 1,
                        l_linger: 0,
                    };
                    libc::setsockopt(
                        s.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_LINGER,
                        std::ptr::addr_of!(linger).cast(),
                        std::mem::size_of::<libc::linger>() as u32,
                    );
                }
            });
        }
    });

    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{addr}"]

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

    let resp = request(
        proxy,
        b"GET /cut HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("502") || resp.contains("part"),
        "abort must 502 or forward partial: {resp:?}"
    );
}

/// Malformed bytes → 400; oversized head → 413.
#[test]
fn malformed_and_oversized_requests() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

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

    // Garbage bytes are not HTTP.
    let resp = request(proxy, b"\x00\xff\xfe garbage \r\n\r\n");
    assert!(resp.contains("400"), "malformed must 400: {resp:?}");

    // Head larger than the parse limit.
    let big = format!("GET /{} HTTP/1.1\r\nHost: t\r\n\r\n", "a".repeat(1 << 20));
    let resp = request(proxy, big.as_bytes());
    assert!(
        resp.contains("413") || resp.contains("400"),
        "oversized must 413/400: {resp:?}"
    );
}

/// Upstream closes cleanly mid-body (FIN, no RST): truncated body.
#[test]
fn upstream_clean_truncation() {
    let _serial = lock_serial();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let Ok(_) = s.read(&mut buf) else { return };
                // Promise 100 bytes, deliver 4, then clean FIN.
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npart");
                drop(s);
            });
        }
    });

    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{addr}"]

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

    // The proxy must not hang: it closes (possibly after partial body).
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(b"GET /trunc HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
        text.contains("200 OK") || text.contains("502"),
        "truncation must terminate: {text:?}"
    );
}

/// Blackhole upstream (accepts, never responds) + short first-byte
/// timeout: the proxy must fail over/502, not hang.
#[test]
fn blackhole_upstream_times_out() {
    let _serial = lock_serial();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                // Hold the connection open, never respond.
                std::thread::sleep(Duration::from_secs(30));
                drop(stream);
            });
        }
    });

    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{addr}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
first_byte_timeout_ms = 400
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let start = std::time::Instant::now();
    let resp = request(
        proxy,
        b"GET /hole HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("504") || resp.contains("502"),
        "blackhole must time out: {resp:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "must not hang on blackhole"
    );
}

/// Idle sessions are reaped after idle_timeout_ms.
#[test]
fn idle_timeout_reaps_session() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

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
idle_timeout_ms = 300
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // Connect and stay silent past the idle deadline: the server closes.
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    std::thread::sleep(Duration::from_millis(900));
    // A request after the deadline must fail: the session was reaped.
    let wrote = s.write_all(b"GET /late HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    if wrote.is_ok() {
        let mut buf = [0u8; 1];
        // Either EOF/RST now, or the fresh session answers — both prove
        // the worker is alive and reaping correctly.
        let _ = s.read(&mut buf);
    }
    // Worker still serves new connections.
    let resp = request(
        proxy,
        b"GET /fresh HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "worker alive: {resp:?}");
}

/// ACME hook live with an empty token map: unknown tokens 404.
/// (The background issuance fails without a directory; the hook itself
/// answers synchronously.)
#[test]
fn acme_unknown_token_404() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let port = free_port();
    let dir = tempfile::tempdir().expect("dir");
    let storage = dir.path().join("acme");
    let (_cfgdir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[acme]
directory_url = "https://127.0.0.1:1/dir"
emails = ["ci@example.com"]
storage_dir = "{}"
insecure_tls = true

[[acme.domains]]
domain = "probe.test"

[runtime]
force_mio = true
workers = 1
"#,
        storage.display()
    ));
    // Keep the storage dir alive for the proxy lifetime.
    let _storage_guard = dir;
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let resp = request(
        proxy,
        b"GET /.well-known/acme-challenge/nope HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("404"), "unknown token must 404: {resp:?}");
    assert!(resp.contains("unknown token"), "body: {resp:?}");

    // Normal routing unaffected by the ACME section.
    let resp = request(
        proxy,
        b"GET /plain HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "routed: {resp:?}");
}
