//! Read-boundary sweep: drive the relay with every possible upstream
//! write split point.
//!
//! TCP has no message boundaries, so an upstream response arrives in
//! whatever sized pieces the network hands over. Loopback almost always
//! coalesces a small response into one read, which hides every bug that
//! lives on a boundary — a truncated deflate stream, a dropped body
//! chunk, a head parsed from a prefix. Each of those passed the suite
//! for months and would have surfaced in production on the first
//! segmented link.
//!
//! This sweeps the split point across the entire response (head +
//! body) for each downstream protocol, so every alignment is covered
//! rather than the one loopback happens to produce. A three-way split
//! pass covers responses arriving in more than two pieces.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use vane::server::RunOptions;

/// Body size: small enough that every split point is reachable, large
/// enough to span several segments.
const BODY: &[u8] = b"boundary-sweep-payload-0123456789-0123456789-x";

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-sweep.lock");
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

/// Responses served, in order. The test asserts the sweep covered
/// `resp_len + 1` distinct split points.
static SERVED: AtomicUsize = AtomicUsize::new(0);

/// Upstream that writes each response split at a different offset:
/// every `0..=resp_len` boundary, plus a three-way split every 7th
/// response.
fn spawn_splitting_upstream(chunked: bool) -> std::net::SocketAddr {
    spawn_upstream_for(chunked, BODY)
}

/// Upstream serving `payload`, splitting each response at a rotating
/// offset so the sweep covers every boundary.
fn spawn_upstream_for(chunked: bool, payload: &[u8]) -> std::net::SocketAddr {
    SERVED.store(0, Ordering::SeqCst);
    let head = if chunked {
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n"
            .to_string()
    } else {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
            BODY.len()
        )
    };
    let resp: Vec<u8> = if chunked {
        let mut v = head.into_bytes();
        // One chunk, so the split points are inside the chunk payload.
        v.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
        v.extend_from_slice(payload);
        v.extend_from_slice(b"\r\n0\r\n\r\n");
        v
    } else {
        let mut v = head.into_bytes();
        v.extend_from_slice(payload);
        v
    };
    let resp_len = resp.len();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let resp = resp.clone();
            let resp_len = resp_len;
            std::thread::spawn(move || {
                eprintln!("DBG upstream accepted a connection");
                let mut s = stream;
                let _ = s.set_nodelay(true);
                let mut req = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    req.clear();
                    loop {
                        match s.read(&mut byte) {
                            Ok(0) | Err(_) => return,
                            Ok(_) => req.push(byte[0]),
                        }
                        if req.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let n = SERVED.fetch_add(1, Ordering::SeqCst);
                    // A three-way split every 7th response, otherwise
                    // sweep the two-way split point across the whole
                    // response.
                    let pieces: Vec<(usize, usize)> = if n % 7 == 6 {
                        let a = (n / 7) % (resp_len + 1);
                        let b = a + (resp_len - a) / 2;
                        vec![(0, a), (a, b), (b, resp_len)]
                    } else {
                        let a = n % (resp_len + 1);
                        vec![(0, a), (a, resp_len)]
                    };
                    for (start, end) in pieces {
                        if end > start && s.write_all(&resp[start..end]).is_err() {
                            return;
                        }
                        // Long enough that the two writes land in
                        // separate reads, short enough to keep the
                        // sweep quick.
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    let _ = s.flush();
                }
            });
        }
    });
    addr
}

/// Distinct two-way split points the sweep will cover.
fn expected_two_way_sweep(resp_len: usize) -> usize {
    // Split point `resp_len` means "write it all at once", so every
    // 0..=resp_len is distinct.
    resp_len + 1
}

struct ServerGuard {
    tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

fn spawn_proxy(cfg_path: String) -> ServerGuard {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(RunOptions {
            config_path: Some(cfg_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: Some(rx),
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    ServerGuard { tx: Some(tx) }
}

fn connect(proxy: std::net::SocketAddr) -> std::net::TcpStream {
    let mut s = None;
    for _ in 0..40 {
        match std::net::TcpStream::connect(proxy) {
            Ok(s_) => {
                s = Some(s_);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let s = s.expect("connect after retries");
    s.set_read_timeout(Some(Duration::from_secs(15))).ok();
    s
}

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

/// Splits a response into (head, body) at the blank line.
fn split_response(raw: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let at = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head terminator")
        + 4;
    (raw[..at].to_vec(), raw[at..].to_vec())
}

fn run_h1_sweep(chunked: bool, gzip: bool) {
    run_h1_sweep_for(BODY, chunked, gzip, 4096)
}

/// Sweeps every response split point for an h1 downstream.
fn run_h1_sweep_for(payload: &[u8], chunked: bool, gzip: bool, buffer_size: usize) {
    let _serial = lock_serial();
    let upstream = spawn_upstream_for(chunked, payload);
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]
compression = {gzip}

[[routes]]
pattern = "/*rest"
cluster = "up"

[runtime]
force_mio = true
workers = 1
buffer_size = {buffer_size}
"#
    ));
    let _guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let accept = if gzip {
        "accept-encoding: gzip\r\n"
    } else {
        ""
    };
    // The upstream's response length bounds the sweep. Recomputed here
    // from the same construction.
    let head_len = if chunked {
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n".len()
    } else {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        )
        .len()
    };
    let resp_len = if chunked {
        head_len + format!("{:x}\r\n", payload.len()).len() + payload.len() + "\r\n0\r\n\r\n".len()
    } else {
        head_len + payload.len()
    };
    let total = expected_two_way_sweep(resp_len) + (resp_len + 1) / 7 + 1;

    for round in 0..total {
        let mut s = connect(proxy);
        s.write_all(
            format!("GET /x HTTP/1.1\r\nHost: t\r\n{accept}Connection: close\r\n\r\n").as_bytes(),
        )
        .expect("write");
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => raw.extend_from_slice(&buf[..n]),
                Err(e) => panic!(
                    "round {round}: relay stalled after {} bytes: {e} \
                     (upstream reads were never resumed)",
                    raw.len()
                ),
            }
        }
        let (head, body) = split_response(&raw);
        let head_s = String::from_utf8_lossy(&head).into_owned();
        assert!(
            head_s.starts_with("HTTP/1.1 200 "),
            "round {round}: {head_s:?}"
        );
        let body = if gzip {
            assert!(
                head_s.to_lowercase().contains("content-encoding: gzip"),
                "round {round}: no gzip encoding: {head_s:?}"
            );
            // A gzipped h1 response is chunk-framed (the compressed
            // length is unknown), so unwrap the framing first.
            let body = if head_s.to_lowercase().contains("transfer-encoding: chunked") {
                dechunk(&body)
                    .unwrap_or_else(|| panic!("round {round}: bad chunk framing: {body:?}"))
            } else {
                body
            };
            let mut dec = flate2::read::GzDecoder::new(&body[..]);
            let mut plain = Vec::new();
            std::io::Read::read_to_end(&mut dec, &mut plain)
                .unwrap_or_else(|e| panic!("round {round}: gzip decode failed: {e}"));
            plain
        } else if chunked {
            dechunk(&body).unwrap_or_else(|| panic!("round {round}: bad chunking: {body:?}"))
        } else {
            body
        };
        assert_eq!(
            body,
            payload,
            "round {round}: body wrong ({} bytes) — a split boundary lost data",
            body.len()
        );
    }
    let served = SERVED.load(Ordering::SeqCst);
    assert_eq!(served, total, "sweep served {served} of {total}");
}

fn dechunk(body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let nl = rest.windows(2).position(|w| w == b"\r\n")?;
        let size = usize::from_str_radix(std::str::from_utf8(&rest[..nl]).ok()?.trim(), 16).ok()?;
        rest = &rest[nl + 2..];
        if size == 0 {
            return Some(out);
        }
        if rest.len() < size {
            return None;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size..];
        if rest.starts_with(b"\r\n") {
            rest = &rest[2..];
        }
    }
}

#[test]
fn content_length_split_sweep() {
    run_h1_sweep(false, false);
}

/// The same sweep against the smallest configured pool slots: read
/// lengths derive from `buffer_size`, so 512-byte slots quadruple the
/// number of reads a response is reassembled from — the strongest
/// environment for a boundary bug to surface in.
#[test]
fn min_buffer_size_split_sweep() {
    run_h1_sweep_for(BODY, false, false, 512);
    run_h1_sweep_for(BODY, true, false, 512);
}

#[test]
fn chunked_split_sweep() {
    run_h1_sweep(true, false);
}

#[test]
fn gzip_split_sweep() {
    run_h1_sweep(false, true);
}

/// Same sweep over an h2 downstream: every response-arrival alignment
/// must produce a correct, END_STREAM-terminated h2 response.
#[test]
fn h2_downstream_split_sweep() {
    let _serial = lock_serial();
    let upstream = spawn_splitting_upstream(false);
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
h2c = true

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[runtime]
force_mio = true
workers = 1
"#
    ));
    let _guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let head_len = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        BODY.len()
    )
    .len();
    let resp_len = head_len + BODY.len();
    let total = expected_two_way_sweep(resp_len) + (resp_len + 1) / 7 + 1;

    for round in 0..total {
        use vane::h2_client::{H2Upstream, UpstreamEvent};
        let mut sock = connect(proxy);
        let mut h2up = H2Upstream::new();
        sock.write_all(&h2up.pending_writes()).expect("preface");
        let req = b"GET /x HTTP/1.1\r\nhost: t\r\n\r\n".to_vec();
        h2up.send_request(&req, Some(0));
        sock.write_all(&h2up.pending_writes()).expect("request");

        let mut got = Vec::new();
        let mut status = None;
        let mut buf = [0u8; 8192];
        loop {
            match sock.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let mut events = Vec::new();
                    h2up.handle_read(&buf[..n], &mut events);
                    for ev in events {
                        match ev {
                            UpstreamEvent::ResponseHead(h) => {
                                status = Some(String::from_utf8_lossy(&h).into_owned())
                            }
                            UpstreamEvent::ResponseBody(b) => got.extend_from_slice(&b),
                            _ => {}
                        }
                    }
                    if h2up.response_complete() {
                        break;
                    }
                }
            }
        }
        assert!(
            status.is_some_and(|s| s.contains("200")),
            "round {round}: no response head"
        );
        assert_eq!(
            got,
            BODY,
            "round {round}: body wrong ({} bytes) — a split boundary lost data",
            got.len()
        );
        assert!(
            h2up.response_complete(),
            "round {round}: no END_STREAM (broken stream)"
        );
    }
    assert_eq!(SERVED.load(Ordering::SeqCst), total, "sweep incomplete");
}

/// Chunked payload containing the terminal byte sequence, swept across
/// every response split point.
///
/// The relay detects completion by scanning for `0\r\n\r\n`. That
/// sequence is legal *inside* chunk data (a base64 blob, a compressed
/// stream, any text with a NUL), so a content scan terminates the
/// response early and truncates the body whenever the sequence arrives
/// in an earlier read than the real terminal chunk.
#[test]
fn chunked_payload_containing_terminal_sequence() {
    let mut payload = b"before-".to_vec();
    payload.extend_from_slice(b"0\r\n\r\n");
    payload.extend_from_slice(b"-after");
    run_h1_sweep_for(&payload, true, false, 4096);
}
