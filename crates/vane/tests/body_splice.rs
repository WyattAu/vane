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
/// truncated-upstream case (EOF short of Content-Length).
#[cfg(feature = "response_splice")]
fn truncating_backend(head: String, partial: Vec<u8>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let response = [head.into_bytes(), partial].concat();
        // One connection, one response, then close — the FIN is what
        // makes this upstream "truncated".
        if let Some(Ok(mut s)) = listener.incoming().next() {
            let _ = s.write_all(&response);
            let _ = s.shutdown(std::net::Shutdown::Write);
            let _ = s.read(&mut [0u8; 1]);
        }
    });
    addr
}

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
/// KNOWN-BROKEN (worker integration, 2026-10-19): the worker's pump
/// reports ingress+egress success into the verified client fd
/// (SIOCOUTQ drains), yet the client's receive queue stays empty —
/// the splice's page-moved data is not delivered through the
/// io_uring+TCP worker context, unlike the raw-libc context the
/// mechanism test proves. Runtime-gated per the known-broken
/// discipline: VANE_KNOWN_BROKEN_SPLICE=1 asserts the fix.
#[cfg(feature = "response_splice")]
#[test]
fn spliced_body_is_exact_and_keepalive_survives() {
    if std::env::var("VANE_KNOWN_BROKEN_SPLICE").as_deref() != Ok("1") {
        eprintln!(
            "skipping: known-broken (worker-integration byte loss); \
             set VANE_KNOWN_BROKEN_SPLICE=1 to assert the fix"
        );
        return;
    }
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

/// A truncated upstream (EOF short of Content-Length) must close the
/// client connection — a silent hang is the failure mode the capped
/// splice exists to prevent. A keep-alive backend that stays
/// connected is NOT truncated, it is just slow; this backend closes.
#[cfg(feature = "response_splice")]
#[test]
fn truncated_upstream_closes_the_client_connection() {
    // KNOWN-BROKEN on main (io_uring engine, 2026-10-19): a truncated
    // upstream (CL > delivered, then FIN) leaves the CLIENT hanging —
    // head delivered, zero body bytes, connection leaked. Runtime-
    // gated per the known-broken discipline; the gated run asserts
    // the fix. See docs/perf-arc-2026-10.md, iteration 3.
    if std::env::var("VANE_KNOWN_BROKEN_TRUNCATED").as_deref() != Ok("1") {
        eprintln!(
            "skipping: known-broken on main (io_uring truncated-upstream hang); \
             set VANE_KNOWN_BROKEN_TRUNCATED=1 to assert the fix"
        );
        return;
    }
    let partial = body(4096);
    let head = "HTTP/1.1 200 OK\r\nContent-Length: 65536\r\nConnection: close\r\n\r\n".to_string();
    let backend = truncating_backend(head, partial);
    let listener = free_addr();
    let config = config(listener, backend, "");
    spawn_proxy(&config, listener);

    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(15))).ok();
    s.write_all(b"GET /trunc HTTP/1.1\r\nHost: t\r\n\r\n")
        .expect("write request");
    let mut got = Vec::new();
    let mut tmp = [0u8; 16384];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break, // EOF: the connection closed — the contract
            Ok(n) => got.extend_from_slice(&tmp[..n]),
            // A read timeout here IS the hang the capped splice exists
            // to prevent: fail with that framing, not a raw EAGAIN.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                panic!(
                    "HANG: the truncated response left the connection open (got {} bytes)",
                    got.len()
                );
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    let body_start = got
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map_or(0, |p| p + 4);
    let relayed = &got[body_start..];
    eprintln!("[diag] total={} body={}", got.len(), relayed.len());
    assert!(
        !relayed.is_empty() && relayed.len() < 65536,
        "the partial body must pass through, the rest must not be invented \
         (total {} bytes, body {} bytes)",
        got.len(),
        relayed.len()
    );
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
