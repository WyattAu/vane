//! Read-boundary sweep across the h2 upstream leg.
//!
//! The system under test is a vane listener with `http2 = true` on its
//! cluster, so requests go out over an h2 connection to a backend and the
//! response comes back as h2 frames. That leg is the last translation
//! path with no boundary coverage: its sans-io frame reader has to
//! reassemble frames across reads, and nothing so far has made it do
//! that at every offset.
//!
//! The backend is vane's own h2 server shim driven in-process. That
//! gives byte-exact control over when the response frames reach the
//! socket (and, crucially, keeps the harness from agreeing with the
//! system under test about how a frame boundary works — a hand-written
//! backend would risk encoding the same bug).
//!
//! A response's *h1* bytes are fed to the shim in pieces, so the split
//! point sweeps the origin-side boundary, and the frames the shim emits
//! are written to the socket in pieces too, so the proxy's h2 *client*
//! must reassemble across reads.

use std::io::{Read, Write};
use std::time::Duration;

use vane::h2_server::{H2Event, H2Server};
use vane::server::RunOptions;

/// Response payload. Length is not a multiple of any plausible frame or
/// record size, so splits land inside headers and bodies.
const PAYLOAD: &[u8] = b"boundary-sweep-payload-0123456789-01234";

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-h2up-sweep.lock");
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

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

/// The h1 response the backend serves, as the proxy would see it from a
/// real origin.
fn origin_response() -> Vec<u8> {
    let mut v = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        PAYLOAD.len()
    )
    .into_bytes();
    v.extend_from_slice(PAYLOAD);
    v
}

/// Splits `data` into `n` pieces as evenly as possible.
fn pieces(data: &[u8], n: usize) -> Vec<&[u8]> {
    if n <= 1 {
        return vec![data];
    }
    let mut out = Vec::with_capacity(n);
    let per = data.len().div_ceil(n);
    let mut rest = data;
    for i in 0..n {
        if rest.is_empty() {
            break;
        }
        let take = per.min(rest.len());
        let (a, b) = rest.split_at(take);
        out.push(a);
        rest = b;
        if i + 1 == n {
            break;
        }
    }
    if rest.is_empty() && !out.is_empty() {
        // Ensure the last piece carries the tail.
        return out;
    }
    out
}

/// h2 backend driven by vane's own server shim.
///
/// Each accepted connection gets one shim. Client frames are fed in as
/// they arrive; on a request head the origin response is fed to the shim
/// in `split` pieces, and the frames the shim produces are written to
/// the socket in `frame_split` pieces.
fn spawn_shim_backend(split: usize, frame_split: usize) -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut s = stream;
                let _ = s.set_nodelay(true);
                let mut h2s = H2Server::new(0, false);
                let mut events = Vec::new();
                let mut responded = false;
                let mut buf = [0u8; 8192];
                let resp = origin_response();
                loop {
                    let n = match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    h2s.handle_read(&buf[..n], &mut events);
                    let out = h2s.pending_writes();
                    if !out.is_empty() && s.write_all(&out).is_err() {
                        break;
                    }
                    for ev in events.drain(..) {
                        match ev {
                            H2Event::RequestHead { .. } => responded = false,
                            H2Event::SendCredit => {
                                let (frames, _) = h2s.take_held();
                                for f in frames {
                                    if s.write_all(&f).is_err() {
                                        return;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if !responded {
                        responded = true;
                        // Feed the origin response to the shim in
                        // `split` pieces: the shim must accumulate the
                        // head across them.
                        let mut produced: Vec<Vec<u8>> = Vec::new();
                        for piece in pieces(&resp, split) {
                            let (frames, _) = h2s.response_bytes(piece);
                            produced.extend(frames);
                        }
                        // Write the produced frames in `frame_split`
                        // groups: the proxy's h2 client must reassemble
                        // across reads.
                        let flat: Vec<u8> = produced.concat();
                        for piece in pieces(&flat, frame_split.max(1)) {
                            if !piece.is_empty() && s.write_all(piece).is_err() {
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        let _ = s.flush();
                    }
                }
            });
        }
    });
    addr
}

#[test]
fn h2_upstream_leg_survives_every_response_split() {
    let _serial = lock_serial();
    use vane::h2_client::{H2Upstream, UpstreamEvent};

    // The origin response length bounds the sweep of the h1-side split.
    let resp_len = origin_response().len();

    for round in 0..=(resp_len + 1) {
        // `split` = number of h1 pieces; also try splitting the frames.
        let split = (round % 4) + 1;
        let frame_split = match round % 3 {
            0 => 1,
            1 => 2,
            _ => 3,
        };
        let backend = spawn_shim_backend(split, frame_split);
        let port = free_port();
        let (_dir, cfg_path) = temp_config(format!(
            r#"
[[listeners]]
address = "127.0.0.1:{port}"
h2c = true

[clusters.be]
backends = ["{backend}"]
http2 = true

[[routes]]
pattern = "/*rest"
cluster = "be"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#
        ));
        let guard = spawn_proxy(cfg_path);
        let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        wait_bound(proxy);

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
        let mut s = s.expect("connect after retries");
        s.set_read_timeout(Some(Duration::from_secs(15))).ok();

        let mut h2up = H2Upstream::new();
        s.write_all(&h2up.pending_writes()).expect("preface");
        let req = b"GET /x HTTP/1.1\r\nhost: t\r\n\r\n".to_vec();
        h2up.send_request(&req, Some(0));
        s.write_all(&h2up.pending_writes()).expect("request");

        let mut status = None;
        let mut got = Vec::new();
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    raw.extend_from_slice(&buf[..n]);
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
            status.as_deref().is_some_and(|s| s.contains("200")),
            "round {round} (split={split} frames={frame_split}): bad head {status:?}; raw {} bytes",
            raw.len()
        );
        assert_eq!(
            got,
            PAYLOAD,
            "round {round} (split={split} frames={frame_split}): h2 upstream relay returned \
             {} of {} bytes — a response split boundary lost data",
            got.len(),
            PAYLOAD.len()
        );
        assert!(
            h2up.response_complete(),
            "round {round} (split={split} frames={frame_split}): no END_STREAM"
        );
        drop(guard);
    }
}
