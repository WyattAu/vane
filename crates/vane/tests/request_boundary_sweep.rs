//! Request-side read-boundary sweep: drive the relay with every possible
//! client write split point on a chunked request body.
//!
//! The request path streams bodies straight upstream, and its framing
//! decision has the same two failure modes a content scan for
//! `0\r\n\r\n` always has — the sequence is legal inside chunk data, and
//! it can straddle two reads. On the request side the first one is worse
//! than truncation: ending the body early leaves the remaining bytes to
//! be parsed as the next request head on the same connection, which is a
//! desync (and a smuggling vector when the backend frames differently).
//!
//! The upstream echoes back the exact request-body bytes it received, so
//! any loss or truncation on the relay is visible without needing a
//! dechunker in the test.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use vane::server::RunOptions;
use vane_proto::chunked::ChunkedScanner;

/// Chunked request payload: `before-`, the terminal sequence, `-after`.
/// A relay that ends the body at the lookalike drops `-after`.
const LOOKALIKE: &[u8] = b"before-0\r\n\r\n-after";
/// Ordinary payload with no lookalike.
const PLAIN: &[u8] = b"boundary-sweep-payload-0123456789-0123456789-x";

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-req-sweep.lock");
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

/// Chunked request-body bytes for `payload` (single chunk).
fn chunked_body(payload: &[u8]) -> Vec<u8> {
    let mut out = format!("{:x}\r\n", payload.len()).into_bytes();
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n0\r\n\r\n");
    out
}

/// Upstream that reads a chunked request body to its terminal chunk and
/// echoes the exact bytes back. `SERVED` counts completed bodies so the
/// sweep can assert it ran to completion.
static SERVED: AtomicUsize = AtomicUsize::new(0);

fn spawn_echo_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut s = stream;
                let _ = s.set_nodelay(true);
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                let mut chunk = [0u8; 4096];
                // Keep-alive: one response per request on this
                // connection. A relay that mis-frames a request merges
                // the next one into its body, which shows up here as a
                // response for a body the client never sent.
                loop {
                    buf.clear();
                    loop {
                        match s.read(&mut byte) {
                            Ok(0) | Err(_) => return,
                            Ok(_) => buf.push(byte[0]),
                        }
                        if buf.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).into_owned();
                    if !head
                        .to_ascii_lowercase()
                        .contains("transfer-encoding: chunked")
                    {
                        let resp = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                        let _ = s.write_all(resp);
                        return;
                    }
                    let mut body = Vec::new();
                    let mut scan = ChunkedScanner::new();
                    loop {
                        match s.read(&mut chunk) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                let done = scan.feed(&chunk[..n]);
                                body.extend_from_slice(&chunk[..n]);
                                if done {
                                    break;
                                }
                            }
                        }
                    }
                    SERVED.fetch_add(1, Ordering::SeqCst);
                    let mut resp =
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len())
                            .into_bytes();
                    resp.extend_from_slice(&body);
                    if s.write_all(&resp).is_err() {
                        return;
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

/// Sweeps every split point of the chunked request body for `payload`.
fn sweep(payload: &[u8]) {
    let _serial = lock_serial();
    let upstream = spawn_echo_upstream();
    SERVED.store(0, Ordering::SeqCst);
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

    let body = chunked_body(payload);
    let req_head = b"POST /x HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    let full: Vec<u8> = req_head
        .iter()
        .copied()
        .chain(body.iter().copied())
        .collect();

    // Two requests per connection: a relay that misses a terminal chunk
    // leaves the *next* request's head inside the first request's body,
    // so the desync is only visible across transactions.
    for round in 0..full.len() + 1 {
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
        s.set_read_timeout(Some(Duration::from_secs(15))).ok();

        // Transaction 1: split at `round`. Transaction 2: split at the
        // complementary offset, so the two sweeps cover both.
        let split = round % (full.len() + 1);
        let other = (round * 7 + 3) % (full.len() + 1);
        for at in [split, other] {
            if at > 0 && s.write_all(&full[..at]).is_err() {
                panic!("round {round}: client write 1 failed");
            }
            std::thread::sleep(Duration::from_millis(2));
            if s.write_all(&full[at..]).is_err() {
                panic!("round {round}: client write 2 failed");
            }
            std::thread::sleep(Duration::from_millis(2));

            // Read exactly one response (Content-Length framed).
            let mut raw = Vec::new();
            let mut buf = [0u8; 8192];
            loop {
                match s.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => raw.extend_from_slice(&buf[..n]),
                    Err(e) => {
                        panic!(
                            "round {round}: relay stalled after {} bytes: {e}",
                            raw.len()
                        )
                    }
                }
                let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let head = String::from_utf8_lossy(&raw[..at]).into_owned();
                let cl = head
                    .to_ascii_lowercase()
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(usize::MAX);
                if raw.len() - (at + 4) >= cl {
                    break;
                }
            }
            let at = raw
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .unwrap_or_else(|| panic!("round {round}: no response head: {raw:?}"))
                + 4;
            let resp_head = String::from_utf8_lossy(&raw[..at]).into_owned();
            assert!(
                resp_head.starts_with("HTTP/1.1 200"),
                "round {round}: {resp_head:?}"
            );
            assert_eq!(
                &raw[at..],
                &body[..],
                "round {round}: upstream received {} of {} body bytes — \
                 the relay mis-framed the request",
                raw.len() - at,
                body.len()
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        SERVED.load(Ordering::SeqCst),
        2 * (full.len() + 1),
        "sweep served {} of {} requests",
        SERVED.load(Ordering::SeqCst),
        2 * (full.len() + 1)
    );
}

/// Ordinary chunked payload, every split point.
#[test]
fn chunked_request_body_split_sweep() {
    sweep(PLAIN);
}

/// Chunked payload containing the terminal byte sequence. A relay that
/// scans for `0\r\n\r\n` ends the body at the lookalike and leaves
/// `-after` to be parsed as the next request head.
#[test]
fn chunked_request_body_with_lookalike_split_sweep() {
    sweep(LOOKALIKE);
}
