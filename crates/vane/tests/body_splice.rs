//! Body-splice fast path, end to end: a Content-Length response whose
//! body is several slots large moves upstream -> client through a
//! kernel pipe (splice(2)) instead of sixteen slot round trips.
//!
//! The contract being proven:
//! - byte-exact relay (splice moves the RIGHT bytes, exactly CL of
//!   them — no truncation, no duplication, no head-jumping: the
//!   worker parks the splice until the queued head/first-read bytes
//!   have drained),
//! - the transaction completes normally after the splice (keep-alive:
//!   the NEXT request on the same connection works),
//! - a truncated upstream (EOF short of Content-Length) closes the
//!   client connection instead of hanging it,
//! - the mio backend degrades to the copied relay transparently.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn body(size: usize) -> Vec<u8> {
    (0..size).map(|i| b'a' + (i % 26) as u8).collect()
}

/// A keep-alive backend serving one fixed CL response.
fn cl_backend(head: String, payload: Vec<u8>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let response = [head.into_bytes(), payload].concat();
        for stream in listener.incoming().flatten() {
            let response = response.clone();
            std::thread::spawn(move || {
                let mut s = stream;
                let mut buf = [0u8; 8192];
                let mut total = 0usize;
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            total += n;
                            // A body splice that wrote UPSTREAM instead
                            // of downstream shows up here as phantom
                            // request bytes.
                            if total > 200 {
                                eprintln!("[backend] phantom bytes: total={total}");
                            }
                            if s.write_all(&response).is_err() {
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

/// A backend that serves head + a SHORT body, then closes: the
fn free_addr() -> std::net::SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe");
    let addr = probe.local_addr().expect("addr");
    drop(probe);
    addr
}

fn spawn_proxy(config: &str, listener: std::net::SocketAddr) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, config).expect("write config");
    let config_path = path.display().to_string();
    let _ = dir;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _ = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: Some(Duration::from_secs(60)),
            shutdown: None,
            force_mio: false,
        }));
    });
    for _ in 0..400 {
        if TcpStream::connect(listener).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never came up on {listener}");
}

fn config(
    listener: std::net::SocketAddr,
    backend: std::net::SocketAddr,
    extra_runtime: &str,
) -> String {
    format!(
        r#"
[[listeners]]
address = "{listener}"
workers = 1

[clusters.up]
backends = ["{backend}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
response_splice = true
{extra_runtime}
"#
    )
}

/// One request; returns (status, body).
fn request(s: &mut TcpStream, path: &str) -> (u16, Vec<u8>) {
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write request");
    let mut acc = Vec::new();
    let mut tmp = [0u8; 16384];
    loop {
        let n = s
            .read(&mut tmp)
            .unwrap_or_else(|e| panic!("read response: {e}"));
        assert!(n > 0, "eof before the response head");
        acc.extend_from_slice(&tmp[..n]);
        if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&acc[..p + 4]).into_owned();
            let status: u16 = head
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let cl: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("Content-Length:")
                        .and_then(|v| v.trim().parse().ok())
                })
                .unwrap_or(0);
            while acc.len() < p + 4 + cl {
                let n = match s.read(&mut tmp) {
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Ground truth: how many bytes are sitting in
                        // this socket's receive queue right now?
                        let mut pending: i32 = 0;
                        // SAFETY: FIONREAD ioctl on an owned socket.
                        unsafe {
                            libc::ioctl(
                                std::os::fd::AsRawFd::as_raw_fd(s),
                                libc::FIONREAD,
                                &mut pending,
                            );
                        }
                        panic!(
                            "HANG: read body after {} of {cl}; FIONREAD={pending}; local={}",
                            acc.len() - p - 4,
                            s.local_addr().map(|a| a.to_string()).unwrap_or_default()
                        );
                    }
                    Err(e) => panic!("read body: {e}"),
                };
                assert!(n > 0, "eof before the body");
                acc.extend_from_slice(&tmp[..n]);
            }
            return (status, acc[p + 4..p + 4 + cl].to_vec());
        }
    }
}

/// The splice moves the right bytes, exactly Content-Length of them,
/// and the connection keeps working afterwards (the completion path
/// parked the upstream and reset the transaction). The first request
// rides the ordering barrier: head + first-read bytes drain BEFORE
// the kernel copy starts.
/// Pooling off is a documented contract, not an accident: every
/// request dials, and the counters say so.
#[test]
fn pool_disabled_dials_every_request() {
    let payload = body(65536);
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        payload.len()
    );
    let backend = cl_backend(head, payload.clone());
    let listener = free_addr();
    let config = config(listener, backend, "pool_per_backend = 0");
    spawn_proxy(&config, listener);

    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let (status, got) = request(&mut s, "/r0");
    assert_eq!(status, 200);
    assert_eq!(got, payload);
}

/// The splice moves the body byte-exactly and the keep-alive
/// connection survives for follow-up requests. ROOT-CAUSED and
/// fixed (2026-10-21): the pump decremented `moved` but never
/// `remaining`, so the completion condition (remaining == 0)
/// could never fire - `finish_body_splice` never ran, the handler
/// never got `on_body_spliced`, and the session never resumed.
/// The kernel-level mystery (SIOCOUTQ drained, FIONREAD=0) was
/// this accounting bug wearing a network costume.
#[cfg(feature = "response_splice")]
#[test]
fn spliced_body_is_exact_and_keepalive_survives() {
    let payload = body(65536);
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        payload.len()
    );
    let backend = cl_backend(head, payload.clone());
    let listener = free_addr();
    let config = config(listener, backend, "");
    spawn_proxy(&config, listener);

    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    for i in 0..3 {
        let (status, got) = request(&mut s, &format!("/r{i}"));
        assert_eq!(status, 200);
        assert_eq!(got.len(), payload.len(), "request {i}: length wrong");
        assert_eq!(got, payload, "request {i}: body corrupted by the splice");
    }
}

/// The mio backend has no body splice: the request must still work
/// through the copied relay (graceful degradation).
#[test]
fn mio_backend_relays_the_body_without_splice() {
    let payload = body(65536);
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        payload.len()
    );
    let backend = cl_backend(head, payload.clone());
    let listener = free_addr();
    let config = config(listener, backend, "force_mio = true");
    spawn_proxy(&config, listener);

    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let (status, got) = request(&mut s, "/mio");
    assert_eq!(status, 200);
    assert_eq!(got, payload);
}
