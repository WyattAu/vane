//! Access-log e2e: `[access_log] enabled = true` routes a real request
//! through the engine and asserts a well-formed JSON line with the
//! request's fields lands in the drain.

use std::io::{Read, Write};
use std::sync::mpsc;
use std::time::Duration;

fn run_proxy_with_access_log() -> (
    std::process::Child,
    std::net::SocketAddr,
    mpsc::Receiver<String>,
    tempfile::TempDir,
) {
    // Upstream stub.
    let upstream = std::net::TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let upstream_addr = upstream.local_addr().expect("upstream addr");
    std::thread::spawn(move || {
        for stream in upstream.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let _ = s.write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                            );
                        }
                    }
                }
            });
        }
    });

    let proxy = std::net::TcpListener::bind("127.0.0.1:0").expect("bind proxy");
    let proxy_addr = proxy.local_addr().expect("proxy addr");
    // The engine takes ownership of the listener fd via AsRawFd — pass a
    // pre-bound listener through vane_core::tcp_listener semantics by
    // dropping ours and letting vane bind the same port (SO_REUSEADDR).
    let port = proxy_addr.port();
    drop(proxy);

    let dir = tempfile::tempdir().expect("dir");
    let log_path = dir.path().join("access.jsonl");
    let cfg = format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream_addr}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[access_log]
enabled = true
path = "{}"

[runtime]
force_mio = true
workers = 1
"#,
        log_path.display()
    );
    let cfg_path = dir.path().join("vane.toml");
    std::fs::write(&cfg_path, cfg).expect("write cfg");

    let bin = env!("CARGO_BIN_EXE_vane");
    let child = std::process::Command::new(bin)
        .args(["run", "-c", cfg_path.to_str().expect("utf8")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn vane");

    // Wait for the port.
    let mut bound = false;
    for _ in 0..40 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            bound = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(bound, "vane never bound {port}");

    let (tx, rx) = mpsc::channel();
    let log_for_reader = log_path.clone();
    std::thread::spawn(move || {
        for _ in 0..100 {
            if let Ok(text) = std::fs::read_to_string(&log_for_reader) {
                if !text.is_empty() {
                    let _ = tx.send(text);
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = tx.send(String::new());
    });

    (child, proxy_addr, rx, dir)
}

/// Cross-process serial lock shared with the proxy-spawning suites.
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

#[test]
fn access_log_records_routed_request() {
    let _serial = lock_serial();
    let (mut child, proxy_addr, rx, _dir) = run_proxy_with_access_log();

    // One request through the proxy.
    let mut s = std::net::TcpStream::connect(proxy_addr).expect("connect");
    s.write_all(b"GET /api/test HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut buf = String::new();
    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = s.read_to_string(&mut buf);
    assert!(buf.contains("HTTP/1.1 200 OK"), "proxy response: {buf:?}");

    // Drain thread flushes within ~50ms of the record; allow a few rounds.
    let text = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("access log written");

    child.kill().ok();
    let _ = child.wait();

    // Every line must be a JSON object with the expected fields.
    let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert!(!lines.is_empty(), "no access records: {text:?}");
    for line in &lines {
        assert!(line.starts_with('{') && line.ends_with('}'), "line: {line}");
        for field in [
            "\"ts_ns\":",
            "\"duration_us\":",
            "\"status\":200",
            "\"method\":\"GET\"",
            "\"host\":\"example.test\"",
            "\"path\":\"/api/test\"",
            "\"upstream\":\"",
        ] {
            assert!(line.contains(field), "field `{field}` missing from: {line}");
        }
    }
    // Exactly one record for exactly one request.
    assert_eq!(lines.len(), 1, "expected one record: {lines:?}");
}
