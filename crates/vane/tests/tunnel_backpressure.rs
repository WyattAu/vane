//! Tunnel (101-upgrade) backpressure must behave like any other relay.
//!
//! A WebSocket-style tunnel is "raw bidirectional pump" to the relay —
//! but the downstream backpressure machinery (write-queue cap, upstream
//! read throttle, resume-on-drain) was built and tested against framed
//! bodies. Nothing proved the tunnel arm actually flows through it: on
//! loopback a stalled client still keeps up (buffers absorb), so the
//! environment hides any missing throttle as reliably as it hid the h1
//! one. This stalls deterministically instead.

use std::io::{Read, Write};
use std::time::Duration;

use vane::server::RunOptions;

/// Far past the socket buffers, so a stalled client reliably backs the
/// write queue up past the 8 KiB throttle threshold.
const TUNNEL_BYTES: usize = 8 * 1024 * 1024;

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-tunnel.lock");
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

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

/// Accepts the upgrade, answers 101, then:
///
/// - pumps `TUNNEL_BYTES` of a deterministic pattern downstream,
/// - echoes everything the client sends upstream (bidirectional proof),
///
/// in that order, on the same raw connection.
fn spawn_tunnel_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut s = stream;
                s.set_nodelay(true).ok();
                // Request head.
                let mut req = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match s.read(&mut byte) {
                        Ok(0) | Err(_) => return,
                        Ok(_) => req.push(byte[0]),
                    }
                    if req.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let head =
                    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
                if s.write_all(head).is_err() {
                    return;
                }
                let _ = s.flush();
                // Pump the downstream flood on a dedicated thread so the
                // echo direction stays live while the client is slow.
                let mut w = s.try_clone().expect("clone");
                let pump = std::thread::spawn(move || {
                    let chunk: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();
                    for _ in 0..TUNNEL_BYTES / chunk.len() {
                        if w.write_all(&chunk).is_err() {
                            return;
                        }
                    }
                    let _ = w.flush();
                });
                // Echo the client→upstream direction.
                let mut buf = [0u8; 16 * 1024];
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
                let _ = pump.join();
            });
        }
    });
    addr
}

struct ServerGuard {
    tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

fn spawn_proxy(cfg_path: String) -> ServerGuard {
    let (tx, rx) = tokio::sync::oneshot::channel();
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
            shutdown: Some(rx),
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    ServerGuard { tx: Some(tx) }
}

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

/// Byte i of the pumped pattern. The upstream's generator restarts the
/// `i % 251` sequence at every 64 KiB chunk boundary (it builds one
/// chunk and repeats it), so the expectation must too.
fn expected(i: usize) -> u8 {
    ((i % (64 * 1024)) % 251) as u8
}

/// Answers 101, then goes silent for `DELAY` (an upstream busy with its
/// own work), then echoes every byte the client sent. No flood on this
/// connection, so echoed bytes are unambiguous.
fn spawn_slow_echo_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut s = stream;
                s.set_nodelay(true).ok();
                let mut req = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match s.read(&mut byte) {
                        Ok(0) | Err(_) => return,
                        Ok(_) => req.push(byte[0]),
                    }
                    if req.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
                if s.write_all(head).is_err() {
                    return;
                }
                let _ = s.flush();
                // The busy period: a relay without upstream-side
                // backpressure buffers the client's flood here, a relay
                // with it pauses the client. Either is correct; data
                // loss or corruption is not.
                std::thread::sleep(Duration::from_millis(1200));
                let mut buf = [0u8; 16 * 1024];
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
            });
        }
    });
    addr
}

/// The client reads the 101 head, reads a little, then stalls long
/// enough for the relay's write queue to back up, then reads the rest
/// to the end. Every byte must arrive, in order, and the connection
/// must stay usable for the echo direction afterwards.
#[test]
fn stalled_reader_does_not_deadlock_a_tunnel() {
    let _serial = lock_serial();
    let upstream = spawn_tunnel_upstream();
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
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
"#
    ));
    let _server = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_nodelay(true).ok();
    c.write_all(
        b"GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
    )
    .expect("write upgrade");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = c.read(&mut byte).expect("read head");
        assert!(n > 0, "upstream closed during 101 head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        head.starts_with(b"HTTP/1.1 101"),
        "expected 101, got: {}",
        String::from_utf8_lossy(&head)
    );

    // Read a little, then stall: the relay buffers while neither side
    // reads. This is the moment the h1 throttle used to become a
    // one-way door.
    let mut got = 0usize;
    let mut buf = [0u8; 32 * 1024];
    while got < 256 * 1024 {
        let n = c.read(&mut buf).expect("read first slice");
        assert!(n > 0, "tunnel closed early at {got}");
        for (i, b) in buf[..n].iter().enumerate() {
            assert_eq!(*b, expected(got + i), "pattern mismatch at {}", got + i);
        }
        got += n;
    }
    std::thread::sleep(Duration::from_millis(700));

    // Resume: the remainder of the flood must arrive, in order.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while got < TUNNEL_BYTES {
        assert!(
            std::time::Instant::now() < deadline,
            "tunnel stalled at {got}/{TUNNEL_BYTES}"
        );
        let n = c.read(&mut buf).expect("read rest");
        assert!(n > 0, "tunnel closed early at {got}");
        for (i, b) in buf[..n].iter().enumerate() {
            assert_eq!(*b, expected(got + i), "pattern mismatch at {}", got + i);
        }
        got += n;
    }

    // The same raw connection still relays the other direction: the
    // upstream echo thread answers on this socket.
    let probe = b"echo-probe-after-flood";
    c.write_all(probe).expect("write probe");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut echo = vec![0u8; probe.len()];
    let mut seen = 0usize;
    while seen < echo.len() {
        let n = c.read(&mut echo[seen..]).expect("read echo");
        assert!(n > 0, "echo closed at {seen}");
        seen += n;
    }
    assert_eq!(&echo, probe, "echo direction corrupted");
}

/// The client→upstream direction must also relay cleanly when the
/// upstream is slow to read: the client floods a tunnel whose upstream
/// is silent for over a second, then expects every byte echoed back.
/// The relay may buffer or pause the client — either is correct; loss,
/// truncation, or corruption is not.
#[test]
fn tunnel_client_to_upstream_relay_survives_a_slow_upstream() {
    let _serial = lock_serial();
    let upstream = spawn_slow_echo_upstream();
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
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
"#
    ));
    let _server = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut c = std::net::TcpStream::connect(proxy).expect("connect");
    c.set_nodelay(true).ok();
    c.write_all(
        b"GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
    )
    .expect("write upgrade");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = c.read(&mut byte).expect("read head");
        assert!(n > 0, "upstream closed during 101 head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(head.starts_with(b"HTTP/1.1 101"));

    // 8 MiB: far past socket buffers, so the relay itself must carry it
    // while the upstream sleeps.
    let probe: Vec<u8> = (0..8 * 1024 * 1024usize).map(|i| (i % 253) as u8).collect();
    c.set_write_timeout(Some(Duration::from_secs(60))).ok();
    c.write_all(&probe).expect("write probe flood");
    // Halfway through the write, the upstream is still inside its busy
    // period; the write may block here and that is the backpressure
    // working. Keep going.

    let mut echo = vec![0u8; probe.len()];
    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    c.set_read_timeout(Some(Duration::from_secs(30))).ok();
    while seen < echo.len() {
        assert!(
            std::time::Instant::now() < deadline,
            "echo stalled at {seen}/{}",
            echo.len()
        );
        let n = c.read(&mut echo[seen..]).expect("read echo");
        assert!(n > 0, "echo closed at {seen}");
        seen += n;
    }
    assert_eq!(echo, probe, "client→upstream echo corrupted");
}
