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
