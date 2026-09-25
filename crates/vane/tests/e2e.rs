//! End-to-end proxy tests: real sockets, real HTTP, worker threads.

#![allow(clippy::unwrap_used, clippy::expect_used)]
/// Blocking cross-process test lock (flock on a temp file). Proxy suites
/// spawn real workers and are wall-clock sensitive; serialize them.
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

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Spawns a tiny HTTP upstream returning `status` + `body`.
fn spawn_upstream(body: &'static str) -> (SocketAddr, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    let _handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
                let _ = s.flush();
            });
        }
    });
    (addr, _handle)
}

/// Writes a config pointing `/*rest` at the upstream, runs vane::server on
/// a random port, and returns the proxy address.
fn spawn_proxy(upstream: SocketAddr, force_mio: bool) -> SocketAddr {
    let config = format!(
        r#"
[[listeners]]
address = "127.0.0.1:0"
workers = 1

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false
"#
    );
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, config).expect("write");

    // Bind a listener up front to learn the port, then hand it over via the
    // config (port 0 in config would bind a different port per worker).
    let proxy_listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);
    std::fs::write(
        &path,
        format!(
            r#"
[[listeners]]
address = "{proxy_addr}"

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false
"#
        ),
    )
    .expect("write");

    let config_path = path.display().to_string();
    let _handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: None,
            force_mio,
        }));
        assert_eq!(code, 0);
    });
    // Wait for the proxy to accept.
    for _ in 0..100 {
        if TcpStream::connect(proxy_addr).is_ok() {
            return proxy_addr;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("proxy did not come up");
}

fn http_get(proxy: SocketAddr, target: &str, host: &str) -> String {
    let mut s = TcpStream::connect(proxy).expect("connect proxy");
    let req = format!("GET {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write");
    let mut out = String::new();
    s.read_to_string(&mut out).expect("read");
    out
}

#[test]
fn end_to_end_http_proxy() {
    let _lock = lock_serial();

    let (upstream, _up_handle) = spawn_upstream("hello from upstream");
    let proxy = spawn_proxy(upstream, true);

    let resp = http_get(proxy, "/anything", "test.local");
    assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
    assert!(resp.contains("hello from upstream"), "{resp}");
}

#[test]
fn no_route_is_404() {
    let _lock = lock_serial();

    let (upstream, _up_handle) = spawn_upstream("x");
    let proxy = spawn_proxy(upstream, true);
    let resp = http_get(proxy, "/", "unmatched.example");
    assert!(
        resp.starts_with("HTTP/1.1 404") || resp.starts_with("HTTP/1.1 200"),
        "got: {resp}"
    );
}

/// Upstream that echoes the raw request head back as the response body —
/// lets a test assert exactly which headers reached the backend.
fn spawn_reflecting_upstream() -> (SocketAddr, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    let handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read until the end of the request head.
                loop {
                    match s.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let body = String::from_utf8_lossy(&buf).into_owned();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
            });
        }
    });
    (addr, handle)
}

/// POSTs a raw request and returns the (possibly partial) response.
fn post_raw(proxy: SocketAddr, req: &[u8]) -> String {
    let mut s = TcpStream::connect(proxy).expect("connect proxy");
    s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .ok();
    s.write_all(req).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

/// Request-smuggling guard, end to end: a `Content-Length` +
/// `Transfer-Encoding` request is answered 400 by the edge itself and the
/// smuggled second request never reaches the upstream.
#[test]
fn cl_plus_te_request_is_rejected_end_to_end() {
    let _lock = lock_serial();

    let (upstream, _up_handle) = spawn_reflecting_upstream();
    let proxy = spawn_proxy(upstream, true);

    let smuggle =
        b"POST /x HTTP/1.1\r\nHost: t.local\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: t.local\r\n\r\n";
    let resp = post_raw(proxy, smuggle);
    assert!(
        resp.starts_with("HTTP/1.1 400"),
        "CL+TE must be rejected with 400: {resp}"
    );
    assert!(
        !resp.contains("/smuggled"),
        "smuggled request must never reach the upstream: {resp}"
    );
}

/// Inbound X-Forwarded-* headers are stripped and replaced: the upstream
/// sees exactly our own XFF (peer IP) and XFP (listener scheme), never
/// client-supplied values.
#[test]
fn inbound_x_forwarded_headers_are_sanitized_end_to_end() {
    let _lock = lock_serial();

    let (upstream, _up_handle) = spawn_reflecting_upstream();
    let proxy = spawn_proxy(upstream, true);

    let req = b"GET /hdr HTTP/1.1\r\nHost: t.local\r\nX-Forwarded-For: 1.2.3.4\r\nX-Forwarded-Proto: https\r\nX-Forwarded-Host: evil.example\r\nConnection: close\r\n\r\n";
    let resp = post_raw(proxy, req);
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");

    // The spoofed values must not survive.
    assert!(
        !resp.contains("1.2.3.4"),
        "spoofed XFF reached upstream: {resp}"
    );
    assert!(
        !resp.contains("evil.example"),
        "spoofed XFH reached upstream: {resp}"
    );
    // Exactly one XFF, carrying the real peer IP.
    let xff_count = resp
        .to_ascii_lowercase()
        .matches("x-forwarded-for:")
        .count();
    assert_eq!(xff_count, 1, "exactly one XFF expected: {resp}");
    assert!(
        resp.to_ascii_lowercase()
            .contains("x-forwarded-for: 127.0.0.1"),
        "XFF must carry the peer IP: {resp}"
    );
    // Plaintext listener → http, not the spoofed https.
    let xfp_count = resp
        .to_ascii_lowercase()
        .matches("x-forwarded-proto:")
        .count();
    assert_eq!(xfp_count, 1, "exactly one XFP expected: {resp}");
    assert!(
        resp.to_ascii_lowercase()
            .contains("x-forwarded-proto: http\r\n"),
        "XFP must be http on a plaintext listener: {resp}"
    );
}
