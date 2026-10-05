//! Upstream response heads that arrive split across TCP segments.
//!
//! TCP gives no message boundaries: an upstream's response head can land
//! in the socket across several reads (Nagle off, a slow backend, TLS
//! record boundaries, a proxy in between). vane must accumulate until the
//! head is complete — answering 502 on the first fragment would fail
//! requests that are merely slow to arrive.

use std::io::{Read, Write};
use std::time::Duration;

use vane::server::RunOptions;

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-split-head.lock");
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

/// Writes `pieces` as separate `write` calls with a pause between each,
/// so the head genuinely arrives across multiple reads.
fn spawn_split_upstream(pieces: Vec<&'static [u8]>) -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let pieces = pieces.clone();
            std::thread::spawn(move || {
                let mut s = stream;
                let mut buf = [0u8; 8192];
                if s.read(&mut buf).is_err() {
                    return;
                }
                for piece in pieces {
                    // TCP_NODELAY: each write leaves as its own segment.
                    let _ = s.set_nodelay(true);
                    if s.write_all(piece).is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(60));
                }
                let _ = s.flush();
            });
        }
    });
    addr
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

fn request(proxy: std::net::SocketAddr, req: &[u8]) -> String {
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
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(req).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
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

fn start(upstream: std::net::SocketAddr) -> (ServerGuard, tempfile::TempDir, std::net::SocketAddr) {
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

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
    let guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);
    (guard, _dir, proxy)
}

/// The head split across three segments: status line, headers, terminator.
#[test]
fn head_split_across_segments_is_accumulated() {
    let _serial = lock_serial();
    let upstream = spawn_split_upstream(vec![
        b"HTTP/1.1 200 OK\r\n",
        b"Content-Length: 5\r\nContent-Type: text/plain\r\n",
        b"\r\nhello",
    ]);
    let (_g, _dir, proxy) = start(upstream);

    let resp = request(
        proxy,
        b"GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "split head rejected: {resp:?}");
    assert!(resp.ends_with("hello"), "body: {resp:?}");
}

/// One byte at a time — the pathological case. A head shorter than the
/// parse buffer must still assemble.
#[test]
fn head_split_byte_by_byte_is_accumulated() {
    let _serial = lock_serial();
    let head = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
    let pieces: Vec<&'static [u8]> = head
        .split(|b| *b == b'\n')
        .map(|l| Box::leak([l, b"\n"].concat().into_boxed_slice()) as &'static [u8])
        .collect();
    let upstream = spawn_split_upstream(pieces);
    let (_g, _dir, proxy) = start(upstream);

    let resp = request(
        proxy,
        b"GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("200 OK"),
        "byte-split head rejected: {resp:?}"
    );
    assert!(resp.ends_with("hi"), "body: {resp:?}");
}

/// A head split with the body in the first segment: the inline body must
/// be preserved once the head completes.
#[test]
fn head_split_with_body_in_first_segment() {
    let _serial = lock_serial();
    let upstream = spawn_split_upstream(vec![
        b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n",
        b"\r\nhello-",
        b"world",
    ]);
    let (_g, _dir, proxy) = start(upstream);

    let resp = request(
        proxy,
        b"GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "{resp:?}");
    assert!(resp.ends_with("hello-world"), "body: {resp:?}");
}

/// An upstream that sends a head fragment and then closes: a truncated
/// response must be an error, not a hang or a half-relayed response.
#[test]
fn truncated_head_upstream_close_is_an_error() {
    let _serial = lock_serial();
    let upstream = spawn_split_upstream(vec![b"HTTP/1.1 200 OK\r\nContent-Len"]);
    let (_g, _dir, proxy) = start(upstream);

    let resp = request(
        proxy,
        b"GET /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("502") || resp.contains("504"),
        "truncated head not reported: {resp:?}"
    );
}
