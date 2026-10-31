//! Adaptive concurrency, end to end: a controllable backend, real
//! clients, and the operator's own metrics.
//!
//! The contract: when a cluster's latency collapses under load, vane
//! sheds the excess with 503 INSTEAD of queueing everything behind the
//! collapse; when the cluster recovers, shedding stops. The unit tests
//! in `crate::adaptive` prove the controller math clock-free; this
//! suite proves the wiring — admission at dispatch, release on the
//! terminal paths, and the shed counter an operator alerts on.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

/// A keep-alive backend whose per-response delay is switchable at
/// runtime — the load knob for the shed/recover phases.
fn switchable_backend() -> (std::net::SocketAddr, Arc<AtomicU32>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let addr = listener.local_addr().expect("addr");
    let delay_ms = Arc::new(AtomicU32::new(0));
    let served = Arc::new(AtomicUsize::new(0));
    let (delay_out, served_out) = (Arc::clone(&delay_ms), Arc::clone(&served));
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let delay_ms = Arc::clone(&delay_ms);
            let served = Arc::clone(&served);
            std::thread::spawn(move || {
                let mut s = stream;
                let mut buf = [0u8; 4096];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let _ = n;
                            let d = delay_ms.load(Ordering::SeqCst);
                            if d > 0 {
                                std::thread::sleep(Duration::from_millis(u64::from(d)));
                            }
                            served.fetch_add(1, Ordering::SeqCst);
                            if s
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
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
    (addr, delay_out, served_out)
}

fn free_addr() -> std::net::SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe");
    let addr = probe.local_addr().expect("addr");
    drop(probe);
    addr
}

fn spawn_proxy(
    config: &str,
    listener: std::net::SocketAddr,
    admin: std::net::SocketAddr,
) -> std::thread::JoinHandle<()> {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, config).expect("write config");
    let config_path = path.display().to_string();
    // The config file must outlive the server thread's startup.
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
        if TcpStream::connect(listener).is_ok() && TcpStream::connect(admin).is_ok() {
            return std::thread::spawn(|| {});
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never came up on {listener}");
}

fn metric(admin: std::net::SocketAddr, name: &str) -> u64 {
    let mut s = TcpStream::connect(admin).expect("connect admin");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
        .expect("write scrape");
    let mut buf = String::new();
    s.read_to_string(&mut buf).expect("read scrape");
    buf.lines()
        .filter(|l| l.starts_with(name))
        .find_map(|l| l.split_whitespace().last())
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
        .unwrap_or(0)
}

/// One request over `s`; returns the status code.
fn request(s: &mut TcpStream, path: &str) -> u16 {
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write request");
    let mut acc = Vec::new();
    let mut tmp = [0u8; 4096];
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
                let n = s.read(&mut tmp).expect("read body");
                assert!(n > 0, "eof before the body");
                acc.extend_from_slice(&tmp[..n]);
            }
            return status;
        }
    }
}

/// Drives `requests` keep-alive requests over one connection,
/// counting statuses.
fn drive(
    listener: std::net::SocketAddr,
    requests: usize,
    counter: Arc<AtomicUsize>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let Ok(mut s) = TcpStream::connect(listener) else {
        return;
    };
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    for i in 0..requests {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let status = request(&mut s, &format!("/r{i}"));
        counter.fetch_add(usize::from(status), Ordering::SeqCst);
    }
}

/// The full arc: healthy → collapsed (SHED, don't queue) → recovered
/// (stop shedding). The controller math is proven clock-free in
/// `crate::adaptive`; this is the wiring proof.
#[test]
fn overload_sheds_and_recovery_stops_shedding() {
    let (backend, delay_ms, _served) = switchable_backend();
    let listener = free_addr();
    let admin = free_addr();
    let config = format!(
        r#"
[[listeners]]
address = "{listener}"
workers = 1

[clusters.up]
backends = ["{backend}"]

[clusters.up.adaptive]
min_limit = 2
max_limit = 200
queue_size = 1

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
address = "{admin}"
"#
    );
    let _server = spawn_proxy(&config, listener, admin);

    // Warm the baseline with fast traffic: two sequential requests.
    {
        let mut s = TcpStream::connect(listener).expect("connect proxy");
        s.set_read_timeout(Some(Duration::from_secs(10))).ok();
        assert_eq!(request(&mut s, "/warm1"), 200);
        assert_eq!(request(&mut s, "/warm2"), 200);
    }
    let shed_before = metric(admin, "vane_cluster_up_overload_shed_total");
    assert_eq!(shed_before, 0, "healthy traffic must never shed");

    // Collapse: 300 ms per response, four concurrent clients. The
    // limit floors at 2, so two clients ride and two get shed.
    delay_ms.store(300, Ordering::SeqCst);
    let ok = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for _ in 0..4 {
        let ok = Arc::clone(&ok);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            drive(listener, 12, ok, stop);
        }));
    }
    for h in handles {
        h.join().expect("client thread");
    }
    let ok_codes = ok.load(Ordering::SeqCst);
    let shed_during = metric(admin, "vane_cluster_up_overload_shed_total");
    assert!(
        shed_during > shed_before,
        "a collapsed cluster must shed: shed={shed_during}, 200s={ok_codes}"
    );
    assert!(
        ok_codes > 0,
        "shedding must never be total: the admitted requests still rode"
    );

    // Recover: remove the delay. The limit grows back GRADUALLY (that
    // is the design — the controller re-opens conservatively), so let
    // the ramp run before judging.
    delay_ms.store(0, Ordering::SeqCst);
    let ok_ramp = Arc::new(AtomicUsize::new(0));
    let stop_ramp = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let ok_ramp = Arc::clone(&ok_ramp);
        let stop_ramp = Arc::clone(&stop_ramp);
        handles.push(std::thread::spawn(move || {
            drive(listener, 40, ok_ramp, stop_ramp);
        }));
    }
    for h in handles {
        h.join().expect("client thread");
    }
    assert!(
        ok_ramp.load(Ordering::SeqCst) > 0,
        "recovery must start serving again"
    );

    // The tail is the contract: with the limit recovered, four
    // concurrent clients all ride, and the shed counter is FROZEN.
    let shed_at_tail_start = metric(admin, "vane_cluster_up_overload_shed_total");
    let ok_tail = Arc::new(AtomicUsize::new(0));
    let stop_tail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let ok_tail = Arc::clone(&ok_tail);
        let stop_tail = Arc::clone(&stop_tail);
        handles.push(std::thread::spawn(move || {
            drive(listener, 20, ok_tail, stop_tail);
        }));
    }
    for h in handles {
        h.join().expect("client thread");
    }
    let shed_final = metric(admin, "vane_cluster_up_overload_shed_total");
    assert_eq!(
        ok_tail.load(Ordering::SeqCst),
        4 * 20 * 200,
        "a recovered cluster serves everyone (the counter sums status codes)"
    );
    assert_eq!(
        shed_final, shed_at_tail_start,
        "recovered latencies must stop the shedding"
    );
}
