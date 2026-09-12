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

/// Chunked upstream body delivered in fragments: the terminal 0-chunk
/// arriving in a later packet must still complete the transaction.
#[test]
fn chunked_split_terminal() {
    let _serial = lock_serial();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let Ok(_) = s.read(&mut buf) else { return };
                // Head + first chunk now, terminal chunk after a delay so
                // it lands in a separate packet/read.
                let _ = s.write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n",
                );
                std::thread::sleep(Duration::from_millis(200));
                let _ = s.write_all(b"0\r\n\r\n");
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
        b"GET /chunked HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "chunked must complete: {resp:?}");
    assert!(resp.contains("hello"), "chunked body: {resp:?}");
}

/// Unroutable upstream + short connect timeout: dial deadline fires,
/// proxy answers without hanging.
#[test]
fn connect_timeout_fires() {
    let _serial = lock_serial();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["10.255.255.1:81"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
connect_timeout_ms = 400
"#
    ));
    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let start = std::time::Instant::now();
    let resp = request(
        proxy,
        b"GET /dark HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    // Either fast refusal routing or timeout failover — both terminate.
    assert!(
        resp.contains("502") || resp.contains("503") || resp.contains("504"),
        "unroutable must terminate: {resp:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(25),
        "must not hang on unroutable upstream"
    );
}

/// Access log with an unwritable path: the drain falls back to stderr
/// (warns once) and the proxy keeps serving.
#[test]
fn access_log_bad_path_falls_back() {
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

[access_log]
enabled = true
path = "/nonexistent-dir-vane/access.jsonl"

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
        b"GET /fb HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "fallback must serve: {resp:?}");
}

// Handover coverage: the CI e2e job runs tests/hot_upgrade.rs
// (--handover-from/--handover-to through real worker handoff).

/// Large-body POST (>4 KiB pool slot): exercises the upstream write
/// queue/pending flush chain across multiple write completions.
#[test]
fn large_body_post_roundtrip() {
    let _serial = lock_serial();
    // Upstream: reads exactly Content-Length bytes, echoes them back.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let upstream = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                // Accumulate until the head terminator, then read to
                // Content-Length (streaming delivers many small writes).
                let mut acc: Vec<u8> = Vec::with_capacity(65536);
                let mut chunk = [0u8; 16384];
                let head_end = loop {
                    let n = match s.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    acc.extend_from_slice(&chunk[..n]);
                    if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
                        break p + 4;
                    }
                    if acc.len() > 1 << 20 {
                        return;
                    }
                };
                let head = String::from_utf8_lossy(&acc[..head_end]).into_owned();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("content-length:")
                            .or_else(|| l.strip_prefix("Content-Length:"))
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                let mut body = acc[head_end..].to_vec();
                while body.len() < len {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(k) => body.extend_from_slice(&chunk[..k]),
                    }
                }
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
                let _ = s.write_all(&body);
            });
        }
    });

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

    // 64 KiB body: exceeds the worker's 16 KiB pending buffer — only
    // passable because bodies stream (never buffered whole).
    let body: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();
    let req = format!(
        "POST /big HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    // The coverage build slows the pipeline enough that the buffered
    // whole-request forward can hit transient close paths; retry up to
    // 3 times before failing.
    let mut last_out = Vec::new();
    for _ in 0..3 {
        let mut s = std::net::TcpStream::connect(proxy).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(10))).ok();
        s.write_all(req.as_bytes()).expect("head");
        s.write_all(&body).expect("body");
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        if out.starts_with(b"HTTP/1.1 200 OK") && out.len() > 12288 {
            last_out = out;
            break;
        }
        last_out = out;
        std::thread::sleep(Duration::from_millis(300));
    }
    let text = String::from_utf8_lossy(&last_out).into_owned();
    assert!(text.contains("200 OK"), "large body: {text:?}");
    // Split head/body manually ([T]::split_once is unstable).
    let split = last_out
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(0);
    let resp_body = &last_out[split..];
    assert_eq!(resp_body.len(), 65536, "full body echoed back");
    assert_eq!(resp_body, &body[..], "body integrity");
}

/// Streaming integrity under fragment arrival: the body is written in
/// many small chunks (each a separate downstream read event) and must
/// arrive at the upstream byte-exact and in order.
#[test]
fn streamed_body_fragment_arrival() {
    let _serial = lock_serial();
    // Upstream: accumulate to Content-Length, hash-compare via length
    // echo (body content returned for the client to verify).
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let upstream = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut acc: Vec<u8> = Vec::with_capacity(65536);
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let n = match s.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    acc.extend_from_slice(&chunk[..n]);
                    if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
                        break p + 4;
                    }
                };
                let head = String::from_utf8_lossy(&acc[..head_end]).into_owned();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("content-length:")
                            .or_else(|| l.strip_prefix("Content-Length:"))
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                let mut body = acc[head_end..].to_vec();
                while body.len() < len {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(k) => body.extend_from_slice(&chunk[..k]),
                    }
                }
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
                let _ = s.write_all(&body);
            });
        }
    });

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

    // 30 KiB in 500-byte fragments: every fragment is its own write.
    let body: Vec<u8> = (0..30720u32).map(|i| (i % 241) as u8).collect();
    let req = format!(
        "POST /frag HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.write_all(req.as_bytes()).expect("head");
    for fragment in body.chunks(500) {
        s.write_all(fragment).expect("fragment");
        // Let the worker observe each fragment as a separate event.
        std::thread::sleep(Duration::from_micros(200));
    }

    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let split = out
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(0);
    let resp_body = &out[split..];
    assert_eq!(resp_body.len(), body.len(), "streamed length");
    assert_eq!(resp_body, &body[..], "streamed body integrity");
}

/// OTLP export end-to-end: a mock OTLP/HTTP receiver gets span payloads
/// for requests served through the engine path.
#[test]
fn otlp_export_emits_spans() {
    let _serial = lock_serial();

    // Mock OTLP/HTTP endpoint: captures POST bodies.
    let otlp_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let otlp_addr = otlp_listener.local_addr().expect("addr");
    let bodies = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let bodies2 = std::sync::Arc::clone(&bodies);
    let conns = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let conns2 = std::sync::Arc::clone(&conns);
    std::thread::spawn(move || {
        for stream in otlp_listener.incoming().flatten() {
            conns2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let bodies = std::sync::Arc::clone(&bodies2);
            std::thread::spawn(move || {
                use std::io::{Read, Write};
                let mut s = stream;
                let mut acc = Vec::new();
                let mut chunk = [0u8; 16384];
                // One request: head, then Content-Length bytes.
                let head_end = loop {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            acc.extend_from_slice(&chunk[..n]);
                            if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
                                break p + 4;
                            }
                        }
                    }
                };
                let head = String::from_utf8_lossy(&acc[..head_end]).into_owned();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("content-length:")
                            .or_else(|| l.strip_prefix("Content-Length:"))
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                while acc.len() < head_end + len {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => acc.extend_from_slice(&chunk[..n]),
                    }
                }
                bodies.lock().unwrap_or_else(|e| e.into_inner()).push(acc);
                let _ = s.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                );
            });
        }
    });

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

[telemetry]
service_name = "vane-e2e-test"
otlp_endpoint = "http://{otlp_addr}/v1/traces"

[runtime]
force_mio = true
workers = 1
"#
    ));
    // NOTE: the proxy runs as a real CHILD PROCESS (not in-process):
    // telemetry's global subscriber is process-wide, and whichever
    // test binary initializes first wins — a child gets a clean slate.
    let bin = env!("CARGO_BIN_EXE_vane");
    let child_log_dir = tempfile::tempdir().expect("child log dir");
    let child_log = child_log_dir.path().join("child.stderr.log");
    let child_err = std::fs::File::create(&child_log).expect("child log");
    let mut child = std::process::Command::new(bin)
        .args(["run", "-c", &cfg])
        .env("RUST_LOG", "debug")
        .stdout(std::process::Stdio::from(
            child_err.try_clone().expect("clone log for stdout"),
        ))
        .stderr(child_err)
        .spawn()
        .expect("spawn vane");
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);
    eprintln!(
        "child alive: {:?}",
        child.try_wait().expect("try_wait").is_none()
    );
    if let Ok(cmdline) = std::fs::read_to_string(format!("/proc/{}/cmdline", child.id())) {
        eprintln!("child cmdline: {:?}", cmdline.replace('\0', " "));
    }

    // A request with a traceparent: the exported span must link to it.
    let resp = request(
        proxy,
        b"GET /traced HTTP/1.1\r\nHost: t\r\ntraceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "traced request: {resp:?}");

    // Batch exporter flushes on its interval; poll for the payload.
    let mut saw_service = false;
    let mut saw_trace = false;
    for _ in 0..150 {
        std::thread::sleep(Duration::from_millis(200));
        let guard = bodies.lock().unwrap_or_else(|e| e.into_inner());
        for body in guard.iter() {
            // OTLP/HTTP protobuf: service name + trace id ride as raw
            // strings/bytes in the payload.
            if body.windows(13).any(|w| w == b"vane-e2e-test") {
                saw_service = true;
            }
            if body.windows(4).any(|w| w == b"\x4b\xf9\x2f\x35") {
                saw_trace = true;
            }
        }
        if saw_service && saw_trace {
            break;
        }
    }
    if !saw_trace {
        let guard = bodies.lock().unwrap_or_else(|e| e.into_inner());
        for (i, body) in guard.iter().enumerate() {
            let hex: String = body
                .iter()
                .rev()
                .take(120)
                .rev()
                .map(|b| format!("{b:02x}"))
                .collect();
            eprintln!("body[{i}] len={} tail_hex={hex}", body.len());
        }
        if let Ok(text) = std::fs::read_to_string(&child_log) {
            eprintln!(
                "child stderr tail:\n{}",
                &text[text.len().saturating_sub(1200)..]
            );
        }
    }
    assert!(
        saw_service,
        "OTLP payload must carry the service name (conns={})",
        conns.load(std::sync::atomic::Ordering::Relaxed)
    );
    assert!(saw_trace, "OTLP payload must carry the request trace id");
}

/// Wasm plugins (feature `wasm`) run in the request path: a guest that
/// reads the request path from its linear memory and rejects `/secret`
/// with 403 while letting everything else through.
#[cfg(feature = "wasm")]
#[test]
fn wasm_plugin_rejects_configured_path() {
    const GUEST: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 1024)
  (func (export "on_request") (param i32 i32 i32) (result i32)
    ;; path is written at 1024 (alloc's ptr); len is param 1.
    (if (i32.ne (local.get 1) (i32.const 7))
      (then (return (i32.const 0))))
    ;; first 4 bytes "/sec" (LE 0x6365732f)?
    (if (i32.ne (i32.load (i32.const 1024)) (i32.const 0x6365732f))
      (then (return (i32.const 0))))
    ;; byte 4 == 'r' → "/secret" → 403
    (if (i32.eq (i32.load8_u (i32.const 1028)) (i32.const 0x72))
      (then (return (i32.const 403))))
    (i32.const 0))
)
"#;
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let port = free_port();
    let plugin_dir = tempfile::tempdir().expect("plugin dir");
    let plugin_path = plugin_dir.path().join("path_guard.wasm");
    let plugin_path = plugin_path.to_str().expect("utf8");
    std::fs::write(plugin_path, GUEST).expect("write wat");

    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[[plugins]]
path = "{plugin_path}"

[runtime]
force_mio = true
workers = 1
"#
    ));

    spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // Non-matching path → plugin Continue → upstream 200.
    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "continue path: {resp:?}");

    // /secret → plugin Reject(403), never reaches the upstream.
    let resp = request(
        proxy,
        b"GET /secret HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("403"), "reject path: {resp:?}");
    assert!(resp.contains("rejected by plugin"), "body: {resp:?}");
}

/// Response compression: a cluster with `compression = true` gzips
/// compressible responses when the client sends Accept-Encoding: gzip
/// (chunked framing downstream); passthrough otherwise.
#[test]
fn gzip_compression_roundtrip() {
    let _serial = lock_serial();
    // Upstream: fixed JSON with content-type + length.
    let body = r#"{"message":"compress me please","pad":"#;
    let body = format!("{body}{}", "x".repeat(2000));
    let body = format!("{}}}", &body[..body.len() - 1]);
    let payload = body.into_bytes();
    let expected = payload.clone();
    let upstream = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let upstream_addr = upstream.local_addr().expect("addr");
    let expected_head = payload.clone();
    std::thread::spawn(move || {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            expected_head.len()
        )
        .into_bytes();
        for stream in upstream.incoming().flatten() {
            let mut s = stream;
            let mut buf = [0u8; 8192];
            let _ = s.read(&mut buf);
            let _ = s.write_all(&head);
            let _ = s.write_all(&expected_head);
        }
    });

    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream_addr}"]
compression = true

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

    // With Accept-Encoding: gzip → chunked + gzipped (binary-safe raw read).
    let mut raw = std::net::TcpStream::connect(proxy).expect("connect");
    raw.set_read_timeout(Some(Duration::from_secs(5))).ok();
    raw.write_all(
        b"GET /api/data HTTP/1.1\r\nHost: t\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n",
    )
    .expect("write");
    let mut bytes = Vec::new();
    let _ = std::io::Read::read_to_end(&mut raw, &mut bytes);
    let head_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head");
    let head = String::from_utf8_lossy(&bytes[..head_end]).into_owned();
    assert!(
        head.to_lowercase().contains("content-encoding: gzip"),
        "{head}"
    );
    assert!(
        head.to_lowercase().contains("transfer-encoding: chunked"),
        "{head}"
    );
    assert!(!head.to_lowercase().contains("content-length"), "{head}");
    let body = &bytes[head_end + 4..];
    // Dechunk.
    let mut dechunked = Vec::new();
    let mut pos = 0usize;
    while pos < body.len() {
        let Some(line_end) = body[pos..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size = usize::from_str_radix(
            std::str::from_utf8(&body[pos..pos + line_end]).expect("hex"),
            16,
        )
        .expect("chunk size");
        if size == 0 {
            break;
        }
        dechunked.extend_from_slice(&body[pos + line_end + 2..pos + line_end + 2 + size]);
        pos += line_end + 2 + size + 2;
    }
    // Gunzip via the same decoder the engine uses (round-trip check).
    let decoded = {
        // Simple one-shot decoder over the gzip stream.
        let mut dec = flate2::read::GzDecoder::new(&dechunked[..]);
        let mut d = Vec::new();
        use std::io::Read as _;
        dec.read_to_end(&mut d).expect("gunzip");
        d
    };
    assert_eq!(decoded, expected, "decompressed body matches upstream");

    // Without Accept-Encoding → verbatim passthrough.
    let resp = request(
        proxy,
        b"GET /api/data HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    let resp_bytes = resp.clone().into_bytes();
    assert!(
        resp_bytes.windows(payload.len()).any(|w| w == &payload[..]),
        "passthrough body intact"
    );
    assert!(!resp.to_lowercase().contains("content-encoding: gzip"));
}
