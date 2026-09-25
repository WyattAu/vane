//! Sustained-load regression gate: 8 client threads x 500 keep-alive
//! requests (4000 total) with framed response reads.
//!
//! History: this test exposed two defects — (1) the test client itself
//! read unframed responses (fixed here), and (2) a synchronous-connect
//! path that never invoked `on_upstream_connected` (fixed via
//! `UpstreamDial`). It guards the per-request engine/handler machinery
//! under real concurrency.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

fn spawn_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let _ = s.write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane",
                            );
                        }
                    }
                }
            });
        }
    });
    addr
}

#[test]
fn sustained_concurrent_load_no_failures_mio() {
    run_sustained(true, 1);
}

/// Multi-worker: SO_REUSEPORT distribution across 4 pinned workers must
/// serve the same traffic with zero failures.
#[test]
fn sustained_concurrent_load_no_failures_mio_4w() {
    run_sustained(true, 4);
}

/// Exercises the io_uring engine end-to-end when the kernel supports it
/// (the worker silently falls back to mio otherwise, so the test is
/// portable). Serialized with the other proxy suites.
#[test]
fn sustained_concurrent_load_no_failures_uring() {
    run_sustained(false, 1);
}

fn run_sustained(force_mio: bool, workers: usize) {
    // Wall-clock sensitive (many workers); serialize with the other suites.
    let _lock = lock_serial();
    let upstream = spawn_upstream();
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe");
    let proxy_addr = probe.local_addr().expect("addr");
    drop(probe);

    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[[listeners]]
address = "{proxy_addr}"
workers = {workers}

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = {force_mio}
"#
        ),
    )
    .expect("write");

    let config_path = path.display().to_string();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: Some(Duration::from_secs(20)),
            shutdown: None,
            force_mio,
        }));
        assert_eq!(code, 0);
    });

    // Wait for readiness.
    for _ in 0..40 {
        if TcpStream::connect(proxy_addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // 8 client threads x 500 keep-alive requests = 4000 requests.
    // Clients read *framed* responses (status line + Content-Length body):
    // TCP segmentation may split or coalesce response bytes arbitrarily,
    // so a single read() is never a complete response.
    let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut clients = Vec::new();
    for _ in 0..8 {
        let failures = Arc::clone(&failures);
        let total = Arc::clone(&total);
        clients.push(std::thread::spawn(move || {
            let mut s = TcpStream::connect(proxy_addr).expect("connect");
            s.set_read_timeout(Some(Duration::from_secs(15))).ok();
            let mut acc: Vec<u8> = Vec::with_capacity(4096);
            let mut tmp = [0u8; 4096];
            for i in 0..500 {
                let req = format!("GET /r{i} HTTP/1.1\r\nHost: t\r\n\r\n");
                if let Err(e) = s.write_all(req.as_bytes()) {
                    eprintln!("[client] req{i} write failed: {e}");
                    failures.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                // Drain one full response: head, then Content-Length body.
                let head_end = loop {
                    match s.read(&mut tmp) {
                        Ok(0) => {
                            eprintln!("[client] req{i} eof waiting for head");
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        Ok(n) => {
                            acc.extend_from_slice(&tmp[..n]);
                            if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
                                break p + 4;
                            }
                        }
                        Err(e) => {
                            eprintln!("[client] req{i} head read failed: {e}");
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                    }
                };
                let head = String::from_utf8_lossy(&acc[..head_end]).into_owned();
                if !head.starts_with("HTTP/1.1 200") {
                    eprintln!("[client] req{i} non-200: {head:?}");
                    failures.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                let cl: usize = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("Content-Length:")
                            .or_else(|| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                while acc.len() < head_end + cl {
                    match s.read(&mut tmp) {
                        Ok(0) => {
                            eprintln!("[client] req{i} eof waiting for body");
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        Ok(n) => acc.extend_from_slice(&tmp[..n]),
                        Err(e) => {
                            eprintln!("[client] req{i} body read failed: {e}");
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                    }
                }
                acc.drain(..head_end + cl);
                total.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for c in clients {
        c.join().expect("client thread");
    }
    let f = failures.load(Ordering::SeqCst);
    let t = total.load(Ordering::SeqCst);
    assert_eq!(f, 0, "{f} failures out of {} completed requests", t + f);
}

use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Blocking cross-process test lock (flock on a temp file).
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
