//! Downstream backpressure must not permanently pause upstream reads.
//!
//! The worker throttles upstream reads while the client-bound write
//! queue is backed up (`pending_down` above two buffer sizes — 8 KiB).
//! If nothing resumes those reads once the queue drains, the throttle is
//! a one-way door: vane stops reading the upstream and never starts
//! again, so the client waits forever for the rest of a body that vane
//! will never fetch. Any client that briefly stops reading — a slow
//! mobile link, a GC pause, a busy loop — is enough to trigger it.
//!
//! This stalls deterministically by having the client stop reading while
//! a large response streams in.

use std::io::{Read, Write};
use std::time::Duration;

use vane::server::RunOptions;

/// Body size: far past the socket buffers, so a non-reading client
/// reliably backs the queue up past the 8 KiB throttle threshold.
const BODY: usize = 4 * 1024 * 1024;

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-backpressure.lock");
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

/// Streams `BODY` bytes of a repeating pattern as the response body, and
/// stays open for further keep-alive requests.
fn spawn_streaming_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut s = stream;
                let mut req = Vec::new();
                let mut byte = [0u8; 1];
                let chunk: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();
                // Keep-alive: one response per request head.
                loop {
                    req.clear();
                    loop {
                        match s.read(&mut byte) {
                            Ok(0) | Err(_) => return,
                            Ok(_) => req.push(byte[0]),
                        }
                        if req.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {BODY}\r\nContent-Type: application/octet-stream\r\n\r\n"
                    );
                    if s.write_all(head.as_bytes()).is_err() {
                        return;
                    }
                    for _ in 0..BODY / chunk.len() {
                        if s.write_all(&chunk).is_err() {
                            return;
                        }
                    }
                    let _ = s.flush();
                }
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

/// The client stops reading mid-response, then resumes. The full body
/// must still arrive: a paused reader may not deadlock the relay.
#[test]
fn slow_reader_does_not_deadlock_a_large_response() {
    let _serial = lock_serial();
    let upstream = spawn_streaming_upstream();
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

[runtime]
force_mio = true
workers = 1
"#
    ));
    let _guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut s = None;
    for _ in 0..40 {
        match std::net::TcpStream::connect(proxy) {
            Ok(s_) => {
                s = Some(s_);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut s = s.expect("connect after retries");
    s.write_all(b"GET /big HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .expect("write request");

    // Stall the reader: let vane's client-bound queue fill up. No reads
    // at all, long enough for 4 MiB to back up behind the socket buffer.
    std::thread::sleep(Duration::from_millis(700));

    // Resume, draining slowly. A read timeout here is the deadlock
    // symptom: vane stopped reading the upstream and never restarts.
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut raw = Vec::with_capacity(BODY + 256);
    let mut buf = [0u8; 64 * 1024];
    let body = loop {
        match s.read(&mut buf) {
            Ok(0) => panic!("closed after {} bytes (truncated)", raw.len()),
            Ok(n) => raw.extend_from_slice(&buf[..n]),
            Err(e) => panic!(
                "relay stalled after {} of {BODY} body bytes: {e} \
                 (upstream reads were never resumed)",
                raw.len()
            ),
        }
        // The head may span reads; count only what follows `\r\n\r\n`.
        if let Some(head_len) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let seen = raw.len() - (head_len + 4);
            if seen >= BODY {
                break seen;
            }
        }
    };

    assert_eq!(body, BODY, "truncated response");
    // Integrity: the payload is a repeating 64 KiB pattern.
    let chunk: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();
    let head_len = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("head") + 4;
    let payload = &raw[head_len..];
    for (i, b) in payload.chunks(chunk.len()).enumerate() {
        assert_eq!(
            b,
            &chunk[..b.len()],
            "corrupt at chunk {i} (offset {})",
            i * chunk.len()
        );
    }
}

/// Two sequential large responses over one keep-alive connection: the
/// first one's backlog must not strand the second transaction.
#[test]
fn sequential_large_responses_on_one_connection() {
    let _serial = lock_serial();
    let upstream = spawn_streaming_upstream();
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

[runtime]
force_mio = true
workers = 1
"#
    ));
    let _guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let mut s = None;
    for _ in 0..40 {
        match std::net::TcpStream::connect(proxy) {
            Ok(s_) => {
                s = Some(s_);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut s = s.expect("connect after retries");
    s.set_read_timeout(Some(Duration::from_secs(20))).ok();

    for round in 1..=2 {
        s.write_all(b"GET /big HTTP/1.1\r\nHost: t\r\n\r\n")
            .expect("write request");
        // Back the client-bound queue up before draining, so the relay
        // has to survive a paused reader on every transaction.
        std::thread::sleep(Duration::from_millis(700));
        // Accumulate the whole response: the head may span several reads,
        // and only the byte count past `\r\n\r\n` is the body.
        let mut raw = Vec::with_capacity(64 * 1024);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = s.read(&mut buf).unwrap_or_else(|e| {
                panic!(
                    "round {round}: stalled after {} bytes: {e} \
                     (upstream reads were never resumed)",
                    raw.len()
                )
            });
            assert!(n > 0, "round {round}: closed after {} bytes", raw.len());
            raw.extend_from_slice(&buf[..n]);
            let Some(head_len) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let body = raw.len() - (head_len + 4);
            if body >= BODY {
                assert_eq!(body, BODY, "round {round}: wrong body length");
                break;
            }
        }
        // Let the next transaction start from a quiet queue.
        std::thread::sleep(Duration::from_millis(50));
    }
}
