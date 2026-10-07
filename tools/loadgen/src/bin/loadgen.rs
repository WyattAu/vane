//! `loadgen ADDR CONNS DURATION PATH [HOST] [--method M] [--body N]` —
//! HTTP/1.1 keep-alive flood client.
//!
//! Pure std threads + blocking I/O, closed-loop per connection (send →
//! full response → next): `CONNS` OS threads, wall-clock deadline,
//! latency recorded per complete response. There is no async runtime to
//! become the measurement, and no lock on the hot path — every thread
//! keeps its own latency pool, merged once at the end.

use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use vane_loadgen::{result_line, split_args, Counters};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (pos, flags) = split_args(&args);
    if pos.len() < 4 {
        eprintln!("usage: loadgen ADDR CONNS DURATION PATH [HOST] [--method M] [--body N]");
        std::process::exit(2);
    }
    let addr = pos[0].clone();
    let conns: usize = pos[1].parse().expect("conns");
    let secs: u64 = pos[2].parse().expect("duration");
    let path = pos[3].clone();
    let host = pos.get(4).cloned().unwrap_or_else(|| "localhost".into());
    let method = flags
        .get("method")
        .cloned()
        .unwrap_or_else(|| "GET".into())
        .to_uppercase();
    let body_bytes: usize = flags.get("body").and_then(|s| s.parse().ok()).unwrap_or(0);

    let req = Arc::new(build_request(&method, &path, &host, body_bytes));
    let counters = Arc::new(Counters::new());
    let (tx, rx) = mpsc::channel::<Vec<u64>>();
    let end = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(conns);
    for _ in 0..conns {
        let addr = addr.clone();
        let req = Arc::clone(&req);
        let counters = Arc::clone(&counters);
        let tx = tx.clone();
        handles.push(
            std::thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(move || run_conn(addr, &req, &counters, end, tx))
                .expect("spawn"),
        );
    }
    drop(tx);
    for h in handles {
        let _ = h.join();
    }

    let duration = start.elapsed().as_secs_f64();
    let mut pool: Vec<u64> = Vec::new();
    for samples in rx {
        pool.extend(samples);
    }
    println!(
        "{}",
        result_line("h1", conns, duration, &counters, pool)
    );
}

/// The whole per-connection loop: connect (with retry), then closed-loop
/// transactions until the deadline. Any read/write failure drops the
/// connection and reconnects — a keep-alive desync must not silently
/// poison the next response's status.
fn run_conn(
    addr: String,
    req: &Arc<Vec<u8>>,
    counters: &Arc<Counters>,
    end: Instant,
    tx: mpsc::Sender<Vec<u64>>,
) {
    let mut samples: Vec<u64> = Vec::with_capacity(1 << 16);
    let mut conn: Option<std::net::TcpStream> = None;
    while Instant::now() < end {
        if conn.is_none() {
            match std::net::TcpStream::connect(&addr) {
                Ok(s) => {
                    s.set_nodelay(true).ok();
                    // A load generator must terminate: a server that
                    // stops mid-response (or a desynced keep-alive)
                    // blocks in read() forever otherwise, and the
                    // deadline only gets checked between transactions.
                    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                    s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                    conn = Some(s);
                }
                Err(_) => {
                    counters.conn_err.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }
        }
        let s = conn.as_mut().expect("conn just set");
        let t0 = Instant::now();
        match transaction(s, req) {
            Ok(status_class) => {
                samples.push(t0.elapsed().as_micros() as u64);
                counters.total.fetch_add(1, Ordering::Relaxed);
                if status_class {
                    counters.ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    counters.non200.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(()) => {
                counters.read_err.fetch_add(1, Ordering::Relaxed);
                conn = None;
            }
        }
    }
    let _ = tx.send(samples);
}

/// One full request/response transaction. Returns `true` for a 2xx.
fn transaction(s: &mut std::net::TcpStream, req: &[u8]) -> Result<bool, ()> {
    s.write_all(req).map_err(|_| ())?;
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 16 * 1024];
    let head_end = loop {
        let n = s.read(&mut buf).map_err(|_| ())?;
        if n == 0 {
            return Err(()); // closed mid-response
        }
        head.extend_from_slice(&buf[..n]);
        if let Some(pos) = find_head_end(&head) {
            break pos;
        }
        if head.len() > 1 << 20 {
            return Err(());
        }
    };
    let status_ok = head.starts_with(b"HTTP/1.1 2") || head.starts_with(b"HTTP/1.0 2");
    // A Content-Length body must be consumed exactly, or keep-alive
    // desyncs and the next transaction reads garbage. (Cooperative bench
    // upstreams always send Content-Length; anything else — chunked,
    // read-to-close — is treated as a transport error and reconnects,
    // which is the honest reading anyway.)
    let content_length = content_length_of(&head[..head_end]);
    let have = head.len() - head_end;
    if have > content_length {
        return Err(()); // pipelined bytes: cannot trust this stream
    }
    let mut remaining = content_length - have;
    while remaining > 0 {
        let n = s.read(&mut buf).map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
        remaining -= n.min(remaining);
    }
    Ok(status_ok)
}

fn build_request(method: &str, path: &str, host: &str, body_bytes: usize) -> Vec<u8> {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\n");
    if body_bytes > 0 {
        req.push_str(&format!("Content-Length: {body_bytes}\r\n"));
    }
    req.push_str("Connection: keep-alive\r\n\r\n");
    let mut req = req.into_bytes();
    if body_bytes > 0 {
        req.extend(std::iter::repeat_n(b'x', body_bytes));
    }
    req
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    for i in 0..=buf.len() - 4 {
        if &buf[i..i + 4] == b"\r\n\r\n" {
            return Some(i + 4);
        }
    }
    None
}

fn content_length_of(head: &[u8]) -> usize {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n") {
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}