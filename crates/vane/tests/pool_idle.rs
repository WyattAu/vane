//! Upstream keep-alive pool: the reuse contract, end to end.
//!
//! Research arc iteration 1 (docs/perf-arc-2026-10.md): connection
//! reuse is the single biggest lever on proxy throughput — a fresh
//! upstream connection costs a TCP (and TLS) handshake per request —
//! and before this suite the reuse ratio was invisible. The tests
//! drive a real proxy against a real backend and read the metric an
//! operator reads, so "the pool works" and "the pool is observable"
//! are the same assertion.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A keep-alive HTTP backend that counts the connections that actually
/// served a request — the ground truth for reuse.
///
/// (The proxy's health checker TCP-probes every backend on a fixed 5 s
/// interval, so raw accept counts are noisy by design; only
/// connections that carried a request say anything about the pool.)
fn counting_backend() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let addr = listener.local_addr().expect("addr");
    let used = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&used);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                let mut s = stream;
                let mut buf = [0u8; 4096];
                let mut served = false;
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if !served {
                                served = true;
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                            if let Ok(text) = std::str::from_utf8(&buf[..n.min(64)]) {
                                let line = text.lines().next().unwrap_or("");
                                eprintln!("[backend] {line}");
                            }
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
    (addr, used)
}

/// A free loopback address (probe, read, drop).
fn free_addr() -> std::net::SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe");
    let addr = probe.local_addr().expect("addr");
    drop(probe);
    addr
}

/// Writes `config`, runs the server in-process, and waits until BOTH
/// the data listener and the admin listener accept — the same
/// addresses the config names, or the wait would prove nothing.
fn spawn_proxy(config: &str, listener: std::net::SocketAddr, admin: std::net::SocketAddr) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, config).expect("write config");

    let config_path = path.display().to_string();
    // The config file must outlive startup; the reload watcher only
    // needs it for changes, but a missing file is a silent failure.
    let _ = dir;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: Some(Duration::from_secs(60)),
            shutdown: None,
            force_mio: false,
        }));
        eprintln!("[test] vane exited with {code}");
    });

    // Startup includes building every worker; under a loaded CI box
    // that is not instant, so wait generously and report WHICH side
    // never came up.
    let mut listener_up = false;
    for _ in 0..400 {
        listener_up |= TcpStream::connect(listener).is_ok();
        if listener_up && TcpStream::connect(admin).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let admin_up = TcpStream::connect(admin).is_ok();
    panic!(
        "proxy never came up: listener {listener} up={listener_up}, admin {admin} up={admin_up}"
    );
}

/// One keep-alive request over `s`; returns the response head.
fn request(s: &mut TcpStream, path: &str) -> String {
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write request");
    let mut acc = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = s.read(&mut tmp).expect("read response");
        assert!(n > 0, "eof before the response head");
        acc.extend_from_slice(&tmp[..n]);
        if let Some(p) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&acc[..p + 4]).into_owned();
            assert!(head.starts_with("HTTP/1.1 200"), "unexpected: {head}");
            // Drain the framed body so the next request is clean.
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
            return head;
        }
    }
}

/// Scrapes a metric from the admin plane (Prometheus text).
fn metric(admin: std::net::SocketAddr, name: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let text = scrape(admin);
        if let Some(value) = text
            .lines()
            .filter(|l| l.starts_with(name))
            .find_map(|l| l.split_whitespace().last())
            .and_then(|v| v.parse::<f64>().ok())
        {
            return value as u64;
        }
        assert!(
            Instant::now() < deadline,
            "metric {name} never appeared; scrape was:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn scrape(admin: std::net::SocketAddr) -> String {
    use std::net::TcpStream;
    let mut s = TcpStream::connect(admin).expect("connect admin");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
        .expect("write scrape");
    let mut buf = String::new();
    s.read_to_string(&mut buf).expect("read scrape");
    buf
}

/// The pool's whole reason to exist, end to end: three requests over
/// one downstream connection cost the backend exactly ONE TCP
/// connection, the dials counter stays at 1, and the reuse is visible
/// in the metrics an operator already has open.
#[test]
fn sequential_requests_reuse_one_upstream_connection() {
    let (backend, used) = counting_backend();
    let (listener, admin) = spawn_proxy_pooled(backend, 30_000);
    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();

    for i in 0..3 {
        request(&mut s, &format!("/r{i}"));
        let _ = wait_idle(admin);
    }
    let idle = wait_idle(admin);

    assert_eq!(
        used.load(Ordering::SeqCst),
        1,
        "three requests must ride one upstream connection"
    );
    assert_eq!(metric(admin, "vane_upstream_dials_total"), 1);
    assert_eq!(metric(admin, "vane_upstream_pool_hits_total"), 2);
    assert_eq!(
        metric(admin, "vane_upstream_pool_misses_total"),
        1,
        "only the first request misses; the pool had nothing to give"
    );
    assert_eq!(idle, 1, "the finished connection is pooled, not dropped");
}

/// The deadline is the promise-keeper: past `pool_idle_ms` the pooled
/// connection is reaped, so the next request dials fresh instead of
/// checking out a connection the backend has already retired (the
/// failed-attach-plus-dial latency spike).
#[test]
fn idle_deadline_reaps_the_pool_instead_of_handing_out_a_dead_connection() {
    let (backend, used) = counting_backend();
    // A deadline short enough to test, long enough to be uncontested
    // by a loaded CI box; the wait below is a multiple of it.
    let ttl_ms: u64 = 1_500;
    let (listener, admin) = spawn_proxy_pooled(backend, ttl_ms);
    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();

    request(&mut s, "/first");
    assert_eq!(wait_idle(admin), 1, "the first connection is pooled");
    std::thread::sleep(Duration::from_millis(ttl_ms * 2));
    request(&mut s, "/second");

    assert_eq!(
        used.load(Ordering::SeqCst),
        2,
        "past the deadline the pooled connection must be reaped, not reused"
    );
    assert_eq!(
        metric(admin, "vane_upstream_pool_misses_total"),
        2,
        "both requests found nothing to reuse"
    );
    assert_eq!(metric(admin, "vane_upstream_dials_total"), 2);
    assert_eq!(
        metric(admin, "vane_upstream_pool_hits_total"),
        0,
        "an expired connection is never handed out"
    );
}

/// Pooling off is a documented contract, not an accident: every
/// request dials, and the counters say so.
#[test]
fn pool_disabled_dials_every_request() {
    let (backend, used) = counting_backend();
    let listener = free_addr();
    let admin = free_addr();
    let config = format!(
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
address = "{admin}"

[runtime]
pool_per_backend = 0
"#
    );
    spawn_proxy(&config, listener, admin);
    let mut s = TcpStream::connect(listener).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    for i in 0..3 {
        request(&mut s, &format!("/r{i}"));
    }
    assert_eq!(used.load(Ordering::SeqCst), 3);
    assert_eq!(metric(admin, "vane_upstream_dials_total"), 3);
    assert_eq!(metric(admin, "vane_upstream_pool_hits_total"), 0);
}

/// Spawns a proxy with pooling on and the given idle deadline.
fn spawn_proxy_pooled(
    backend: std::net::SocketAddr,
    pool_idle_ms: u64,
) -> (std::net::SocketAddr, std::net::SocketAddr) {
    let listener = free_addr();
    let admin = free_addr();
    let config = format!(
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
address = "{admin}"

[runtime]
pool_per_backend = 4
pool_idle_ms = {pool_idle_ms}
"#
    );
    spawn_proxy(&config, listener, admin);
    (listener, admin)
}

/// Polls the idle gauge until the parked connection shows up (the park
/// happens just after the response is written).
fn wait_idle(admin: std::net::SocketAddr) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let value = metric(admin, "vane_upstream_pool_idle");
        if value > 0 || Instant::now() > deadline {
            return value;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
