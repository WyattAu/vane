//! dhat zero-allocation gate (`MM-01`): drives the h1 relay hot path —
//! one keep-alive connection, one upstream, no filters — and asserts
//! the process performs **zero heap allocations** across the measured
//! window (after warmup).
//!
//! Everything inside the window is allocation-accounted: the client
//! runs on a `std` thread with stack-only buffers (no `String`, no
//! `Vec`), and the proxy is configured without access log, admin, rate
//! limiting, tracing, or active health probes. The date cache refresh
//! is allocation-free by construction.
//!
//! Run: `cargo test -p vane-proxy --test dhat_zeroalloc --release`

use std::io::{Read, Write};
use std::time::Duration;

// dhat replaces the global allocator for the WHOLE test process, so
// the counts below cover every thread (proxy workers + test client).
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Requests in the measured window.
const MEASURED: usize = 2_000;
/// Warmup requests on the measured connection (slot alloc, pool fill).
const WARMUP: usize = 256;

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Canned keep-alive 200 responder (stack buffers only).
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

fn spawn_proxy(cfg_path: String) -> tokio::sync::oneshot::Sender<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(cfg_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: Some(rx),
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    tx
}

const REQ: &[u8] = b"GET /api/items HTTP/1.1\r\nHost: t\r\n\r\n";

/// Stack offset of the head terminator.
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// Content-Length from a response head (stack parsing, no alloc).
fn content_length(head: &[u8]) -> u64 {
    for line in head.split(|&b| b == b'\n') {
        if let Some(rest) = line.strip_prefix(b"Content-Length:") {
            let s = std::str::from_utf8(rest).unwrap_or("");
            return s.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Reads exactly one response (head + Content-Length body) into stack
/// buffers — zero heap traffic on the client side.
fn read_response(s: &mut std::net::TcpStream) {
    let mut buf = [0u8; 4096];
    let mut len = 0usize;
    loop {
        if let Some(hend) = head_end(&buf[..len]) {
            let cl = content_length(&buf[..hend]) as usize;
            if len >= hend + cl {
                return;
            }
        }
        let n = s
            .read(&mut buf[len..])
            .or_else(|e| Err(e))
            .expect("upstream read");
        assert!(n > 0, "connection closed mid-response");
        len += n;
    }
}

fn one_request(s: &mut std::net::TcpStream) {
    s.write_all(REQ).expect("write");
    read_response(s);
}

#[test]
#[ignore = "dhat gate runs standalone (replaces the process allocator)"]
fn h1_relay_hot_path_is_allocation_free() {
    // Activates dhat's counting. `testing` keeps the data file off for
    // the CI gate; VH_DHAT_PROFILE=1 instead dumps a json heap profile
    // (with backtraces) for triage and skips the assert.
    let profiling = std::env::var_os("VH_DHAT_PROFILE").is_some();
    let profiler = if profiling {
        Some(
            dhat::Profiler::builder()
                .file_name("/tmp/opencode/vane-mm01/dhat-heap.json")
                .build(),
        )
    } else {
        Some(dhat::Profiler::builder().testing().build())
    };
    let _profiler = profiler;

    let upstream = spawn_upstream();
    let port = free_port();
    let (_dir, cfg) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/api/*rest"
cluster = "up"
methods = ["GET"]

[runtime]
force_mio = true
workers = 1
"#
    ));

    let shutdown = spawn_proxy(cfg);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");

    // Wait for the listener; this also warms the accept path (slot
    // alloc, listener registration) outside the measured window.
    let mut ready = None;
    for _ in 0..60 {
        if let Ok(s) = std::net::TcpStream::connect(proxy) {
            ready = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut conn = ready.expect("proxy never bound");
    conn.set_nodelay(true).expect("nodelay");
    conn.set_read_timeout(Some(Duration::from_secs(10))).ok();

    // Warmup: connection slot, upstream pool, scratch capacities.
    for _ in 0..WARMUP {
        one_request(&mut conn);
    }

    let before = dhat::HeapStats::get();
    for w in 0..4 {
        let wbefore = dhat::HeapStats::get();
        for _ in 0..(MEASURED / 4) {
            one_request(&mut conn);
        }
        let wa = dhat::HeapStats::get();
        eprintln!(
            "[dhat] window {w}: +{} blocks +{} bytes",
            wa.total_blocks - wbefore.total_blocks,
            wa.total_bytes - wbefore.total_bytes
        );
    }
    let after = dhat::HeapStats::get();

    let blocks = after.total_blocks - before.total_blocks;
    let bytes = after.total_bytes - before.total_bytes;
    // Drop inside the test body so the profile file lands
    // deterministically.
    drop(_profiler);
    if profiling {
        eprintln!("[dhat] profile dumped; triage mode skips the assert");
        let _ = shutdown.send(());
        return;
    }
    assert_eq!(
        blocks, 0,
        "MM-01 violated: {blocks} heap allocation(s) across {MEASURED} requests ({bytes} bytes)"
    );

    let _ = shutdown.send(());
}
