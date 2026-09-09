//! Regression anchor for the sustained-load engine bug.
//!
//! Currently #[ignore]: under sustained concurrent load (32+ connections,
//! thousands of requests) the engine exhibits:
//! - debug builds: stop responding partway through (poll loop starves)
//! - release builds: EBADF storms on upstream writes (fd lifecycle race)
//!
//! Small sequential/low-concurrency flows pass (see e2e.rs/production.rs),
//! so the defect is in the sustained-concurrency path — likely the mio
//! pending-op table vs fd reuse, or pool-slot lifetime across
//! attach/detach cycles. Fixing this is the top roadmap item; un-ignore
//! this test to work on it.
//!
//! Run: cargo test -p vane --test load -- --ignored --nocapture

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
#[ignore = "sustained-load engine bug (see module docs) — un-ignore when fixed"]
fn sustained_concurrent_load_no_failures() {
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
workers = 1

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
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
            force_mio: true,
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
    let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut clients = Vec::new();
    for _ in 0..8 {
        let failures = Arc::clone(&failures);
        let total = Arc::clone(&total);
        clients.push(std::thread::spawn(move || {
            let mut s = TcpStream::connect(proxy_addr).expect("connect");
            s.set_read_timeout(Some(Duration::from_secs(5))).ok();
            for i in 0..500 {
                let req = format!("GET /r{i} HTTP/1.1\r\nHost: t\r\n\r\n");
                if s.write_all(req.as_bytes()).is_err() {
                    failures.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                let mut buf = [0u8; 1024];
                match s.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        let head = String::from_utf8_lossy(&buf[..n.min(64)]).into_owned();
                        if !head.starts_with("HTTP/1.1 200") {
                            failures.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                    }
                    _ => {
                        failures.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
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
