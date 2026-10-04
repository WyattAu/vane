//! Proxy throughput + latency percentile benchmark (run in release).
//!
//! Topology mirrors `scripts/bench.sh`: in-process canned upstream →
//! vane (release, 2 workers, upstream keep-alive pool) → N keep-alive
//! client threads. Each client records per-request latencies; the
//! summary reports aggregate rps plus P50/P99/P99.9.
//!
//! ```text
//! cargo test --release -p vane-proxy --test throughput -- --ignored --nocapture
//! ```
//!
//! Numbers are environment-sensitive (shared machine, client and
//! server colocated); treat them as regression baselines, not
//! absolute capacity claims.

use std::io::{Read, Write};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn spawn_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
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
                            if s
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane")
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
    addr
}

const REQ: &[u8] = b"GET /bench HTTP/1.1\r\nHost: bench\r\n\r\n";

/// One keep-alive client: records per-request latency (µs) for the
/// window, counts completed requests. Reads are framed by
/// Content-Length.
fn client(addr: std::net::SocketAddr, deadline: Instant, lat_us: &mut Vec<u64>) -> u64 {
    let mut s = std::net::TcpStream::connect(addr).expect("connect");
    s.set_nodelay(true).expect("nodelay");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut buf = [0u8; 4096];
    let mut n = 0usize;
    let mut done = 0u64;
    while Instant::now() < deadline {
        let start = Instant::now();
        s.write_all(REQ).expect("write");
        // Read exactly one response (head + Content-Length body).
        let body = loop {
            if let Some(hend) = buf[..n]
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|i| i + 4)
            {
                let cl: usize = std::str::from_utf8(&buf[..hend])
                    .ok()
                    .and_then(|h| {
                        h.split("\r\n")
                            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.split(':').nth(1))
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                if n >= hend + cl {
                    break hend + cl;
                }
            }
            if n == buf.len() {
                n = 0; // defensive reset (never hit with this response)
            }
            let read = s.read(&mut buf[n..]).expect("read");
            assert!(read > 0, "closed early at {n}");
            n += read;
        };
        // Consume the response bytes from the buffer.
        buf.copy_within(body..n, 0);
        n -= body;
        lat_us.push(start.elapsed().as_micros() as u64);
        done += 1;
    }
    done
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

#[test]
#[ignore = "release-only throughput benchmark; run with --release --ignored"]
fn proxy_throughput_percentiles() {
    let upstream = spawn_upstream();
    let port = free_port();
    let dir = tempfile::tempdir().expect("dir");
    let cfg_path = dir.path().join("vane.toml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.bench]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "bench"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 2
"#
        ),
    )
    .expect("config");

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(cfg_path.to_str().expect("utf8").to_owned()),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: Some(rx),
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    let _shutdown = tx;

    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    // Wait for the listener.
    let mut ready = false;
    for _ in 0..60 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(ready, "proxy never bound");

    const CLIENTS: usize = 8;
    const SECONDS: u64 = 5;
    // Warmup (single client, connection setup + pools).
    let warm_deadline = Instant::now() + Duration::from_secs(1);
    let mut scratch = Vec::new();
    client(proxy, warm_deadline, &mut scratch);

    let start = Instant::now();
    let deadline = start + Duration::from_secs(SECONDS);
    let handles: Vec<_> = (0..CLIENTS)
        .map(|_| {
            std::thread::spawn(move || {
                let mut lat = Vec::with_capacity(200_000);
                let done = client(proxy, deadline, &mut lat);
                (done, lat)
            })
        })
        .collect();
    let mut total = 0u64;
    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        let (done, lat) = h.join().expect("client");
        total += done;
        all.extend(lat);
    }
    let elapsed = start.elapsed();
    all.sort_unstable();

    let rps = total as f64 / elapsed.as_secs_f64();
    println!(
        "\n== proxy throughput (keep-alive, {CLIENTS} clients, {SECONDS}s, loaded dev machine) =="
    );
    println!(
        "requests: {total}  rps: {rps:.0}  p50: {}us  p99: {}us  p99.9: {}us  max: {}us",
        percentile(&all, 0.50),
        percentile(&all, 0.99),
        percentile(&all, 0.999),
        all[all.len() - 1]
    );
    assert!(total > 10_000, "throughput sanity: only {total} requests");
}
