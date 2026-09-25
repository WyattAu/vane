//! Pooled-upstream load variant: same sustained keep-alive traffic as
//! `load.rs` but with upstream keep-alive pooling enabled
//! (`pool_per_backend > 0`).
//!
//! History: pooling under sustained concurrent load previously exhibited
//! detached-fd lifecycle races (EBADF on writes to closed/reused
//! descriptors). This suite is the acceptance gate for re-enabling it.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn pooled_sustained_load_no_failures_mio() {
    run_pooled(true, 1);
}

#[test]
fn pooled_sustained_load_no_failures_uring() {
    run_pooled(false, 1);
}

#[test]
fn pooled_sustained_load_no_failures_uring_4w() {
    run_pooled(false, 4);
}

fn run_pooled(force_mio: bool, workers: usize) {
    let _lock = lock_serial();

    // Keep-alive upstream: thread per connection, persistent response loop.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let upstream = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if s.write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane",
                            )
                            .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

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
pool_per_backend = 4
"#
        ),
    )
    .expect("write");

    let config_path = path.display().to_string();
    let _server = std::thread::spawn(move || {
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

    for _ in 0..40 {
        if TcpStream::connect(proxy_addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // 8 client threads x 300 keep-alive requests = 2400 through the pool.
    let failures = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut clients = Vec::new();
    for _ in 0..8 {
        let failures = Arc::clone(&failures);
        let total = Arc::clone(&total);
        clients.push(std::thread::spawn(move || {
            let mut s = TcpStream::connect(proxy_addr).expect("connect");
            s.set_read_timeout(Some(Duration::from_secs(15))).ok();
            let mut acc: Vec<u8> = Vec::with_capacity(4096);
            let mut tmp = [0u8; 4096];
            for i in 0..300 {
                let req = format!("GET /r{i} HTTP/1.1\r\nHost: t\r\n\r\n");
                if let Err(e) = s.write_all(req.as_bytes()) {
                    eprintln!("[client] req{i} write failed: {e}");
                    failures.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                // Framed read: head, then Content-Length body.
                let head_end = loop {
                    match s.read(&mut tmp) {
                        Ok(0) => {
                            eprintln!("[client] req{i} eof (head)");
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
                            eprintln!("[client] req{i} head read: {e}");
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
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                while acc.len() < head_end + cl {
                    match s.read(&mut tmp) {
                        Ok(0) => {
                            eprintln!("[client] req{i} eof (body)");
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        Ok(n) => acc.extend_from_slice(&tmp[..n]),
                        Err(e) => {
                            eprintln!("[client] req{i} body read: {e}");
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
    stop.store(true, Ordering::SeqCst);
    let f = failures.load(Ordering::SeqCst);
    let t = total.load(Ordering::SeqCst);
    assert_eq!(f, 0, "{f} failures out of {t} completed (pooled path)");
}

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
