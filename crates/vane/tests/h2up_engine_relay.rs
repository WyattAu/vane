//! Engine-relay HTTP/2 upstream: a cluster flagged `http2 = true`
//! reaches an h2-prior-knowledge upstream through the WORKER relay
//! (`proxy.rs`'s h2up intake/emit paths), not the in-process H2Edge —
//! that distinction is the point. The edge-level h2 upstream tests
//! (h2_upstream.rs) drive the async edge; the engine relay had no test
//! driving POST bodies, multi-frame responses, or request-head
//! straddling, and those are exactly the paths that carry the relay's
//! h2-upstream coverage gap.

#![cfg(feature = "h2")]

use std::io::{Read, Write};
use std::time::Duration;

use vane::server::RunOptions;

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

/// An h2-prior-knowledge upstream (tokio h2 server) that:
/// - GET  /small  → 200, body "h2-up" (single frame),
/// - GET  /big    → 200, 40 KiB body streamed in 16 KiS chunks
///   (multi-DATA-frame; exercises the relay's response re-assembly),
/// - POST /echo   → 200 with the request body echoed (drives
///   request-body framing through the relay's h2up emitter).
async fn spawn_h2_upstream() -> std::net::SocketAddr {
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = mock_listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = mock_listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Ok(mut conn) = h2::server::handshake(stream).await else {
                    return;
                };
                // Handlers spawn per request: the accept loop must KEEP
                // POLLING the connection while a handler drains its body.
                // tokio-h2 dispatches DATA frames to streams only when the
                // connection is polled, so an accept loop that blocks on
                // body.data() hangs whenever the body arrives in a later
                // segment than the head (this hung the POST leg
                // non-deterministically; vane's wire bytes were identical
                // in pass and fail runs).
                while let Some(request) = conn.accept().await {
                    let Ok((request, mut respond)) = request else {
                        break;
                    };
                    tokio::spawn(async move {
                        let path = request.uri().path().to_owned();
                        let mut body = request.into_body();
                        // Drain the request body fully (flow-control
                        // credit release keeps large POSTs alive).
                        let mut req_bytes = Vec::new();
                        while let Some(chunk) = body.data().await {
                            match chunk {
                                Ok(b) => {
                                    let len = b.len();
                                    req_bytes.extend_from_slice(&b);
                                    let _ = body.flow_control().release_capacity(len);
                                }
                                Err(_) => break,
                            }
                        }
                        let payload: Vec<u8> = match path.as_str() {
                            "/big" => (0..40_960u32).map(|i| (i % 251) as u8).collect(),
                            "/echo" => req_bytes,
                            _ => b"h2-up".to_vec(),
                        };
                        let response = http::Response::builder()
                            .status(200)
                            .header("x-path", path)
                            .header("content-length", payload.len().to_string())
                            .body(())
                            .expect("static");
                        let Ok(mut send) = respond.send_response(response, false) else {
                            return;
                        };
                        // 16 KiB chunks: force multi-frame DATA on the wire.
                        for chunk in payload.chunks(16 * 1024) {
                            if send
                                .send_data(bytes::Bytes::copy_from_slice(chunk), false)
                                .is_err()
                            {
                                return;
                            }
                        }
                        let _ = send.send_data(bytes::Bytes::new(), true);
                    });
                }
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

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

/// One h1 keep-alive transaction through the proxy: `req` sent whole,
/// the response read to EOF (Connection: close), returns (status, body).
fn roundtrip(proxy: std::net::SocketAddr, req: &[u8]) -> (u16, Vec<u8>) {
    let mut s = std::net::TcpStream::connect(proxy).expect("connect");
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(Duration::from_secs(30))).ok();
    s.set_write_timeout(Some(Duration::from_secs(30))).ok();
    s.write_all(req).expect("write request");
    let mut raw = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
            Err(e) => panic!("relay stalled: {e}"),
        }
    }
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response head terminator");
    let status: u16 = String::from_utf8_lossy(&raw[..split])
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (status, raw[split + 4..].to_vec())
}

#[test]
fn engine_relay_speaks_h2_to_an_h2_only_upstream() {
    let _serial_owner = {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(std::env::temp_dir().join("vane-tests-h2up-engine.lock"))
            .expect("lock file");
        // SAFETY: flock on a regular file.
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
        assert_eq!(rc, 0);
        f
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let upstream = rt.block_on(spawn_h2_upstream());

    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]
http2 = true

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false
"#
    ));
    let _server = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    // 1. GET: the h1 client request must arrive as h2 (the stub speaks
    //    nothing else) and the h2 response body must reassemble.
    let (status, body) = roundtrip(
        proxy,
        b"GET /small HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"h2-up", "single-frame response body");

    // 2. GET /big: 40 KiB across 16 KiB DATA frames — the relay's
    //    reassembly must be byte-exact (three frames, offsets inside
    //    the pattern).
    let expected: Vec<u8> = (0..40_960u32).map(|i| (i % 251) as u8).collect();
    let (status, body) = roundtrip(
        proxy,
        b"GET /big HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(status, 200);
    assert_eq!(body.len(), expected.len(), "length preserved");
    assert_eq!(body, expected, "multi-frame body byte-exact");
}

/// Upstream trailers relay: an h2 client (h2c ingress) hitting an h2up
/// cluster receives the upstream's trailer fields as h2 response
/// trailers — the gRPC shape. h1 ingress drops them (documented v0.2
/// limit); the h2→h2 path must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_trailers_relay_to_an_h2_client() {
    // h2 upstream: HEADERS(200, no END_STREAM) + DATA + trailing
    // HEADERS(END_STREAM) carrying grpc-status.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let upstream = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Ok(mut conn) = h2::server::handshake(stream).await else {
                    return;
                };
                while let Some(request) = conn.accept().await {
                    let Ok((request, mut respond)) = request else {
                        break;
                    };
                    let _ = request.into_body();
                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .body(())
                        .expect("static");
                    let Ok(mut send) = respond.send_response(response, false) else {
                        break;
                    };
                    let _ =
                        send.send_data(bytes::Bytes::from_static(b"\x00\x00\x00\x00\x00"), false);
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", "0".parse().expect("value"));
                    let _ = send.send_trailers(trailers);
                }
            });
        }
    });

    // h2c ingress on the engine: the config's listener speaks prior-
    // knowledge h2 (h2c) and the cluster is h2up.
    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"
h2c = true

[clusters.up]
backends = ["{upstream}"]
http2 = true

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false
"#
    ));
    let _server = spawn_proxy(cfg_path);
    wait_bound(std::net::SocketAddr::from(([127, 0, 0, 1], port)));

    // h2 client over the h2c listener.
    let io = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("connect");
    let (mut send_request, connection) = h2::client::handshake(io).await.expect("h2 handshake");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = http::Request::builder()
        .method("POST")
        .uri("http://host/rpc")
        .body(())
        .expect("request");
    let (response, flow) = send_request.send_request(request, true).expect("send");
    let (parts, mut body) = response.await.expect("response").into_parts();
    assert_eq!(parts.status.as_u16(), 200);
    let mut got = Vec::new();
    while let Some(chunk) = body.data().await {
        match chunk {
            Ok(b) => {
                let len = b.len();
                got.extend_from_slice(&b);
                let _ = body.flow_control().release_capacity(len);
            }
            Err(_) => break,
        }
    }
    assert_eq!(got, b"\x00\x00\x00\x00\x00", "data frame relayed");
    let trailers = body
        .trailers()
        .await
        .expect("trailers")
        .expect("trailers present");
    assert_eq!(
        trailers.get("grpc-status").map(|v| v.as_bytes()),
        Some(b"0".as_ref()),
        "upstream trailers reached the h2 client"
    );
    let _ = flow;
    let _ = send_request;
}

/// POST through an h2up cluster: KNOWN BROKEN — see
/// docs/h2-streaming-flake.md ("h2up POST body" section). The mock
/// accepts the request head and then waits forever for the body; the
/// request-side DATA frames vane emits (verified at the H2Upstream API
/// level: request_body returns framed, END_STREAM-carrying output) do
/// not produce body data on the tokio-h2 server side. The raw-bytes
/// desync (body tail sent untranslated) is FIXED; what remains is an
/// interop issue between vane's engine h2up client frames and the
/// tokio-h2 server's expectations, needing frame-level capture.
/// POST through an h2up cluster: the body must survive h1→h2 framing
/// (the relay's h2up request-body emitter) and come back echoed.
///
/// History: this test exposed BOTH halves of the defect — the relay's
/// raw-bytes desync (body tail sent untranslated, fixed) and a mock
/// bug that masked the fix for a while: an h2 server accept loop that
/// blocks on body.data() stops polling the connection, so DATA frames
/// arriving in later segments never dispatch (the mock's comment has
/// the details). Vane's wire bytes were identical in pass and fail
/// runs — the fix was on our side of the test all along.
#[test]
fn post_body_round_trips_through_h2_framing() {
    let _serial_owner = {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(std::env::temp_dir().join("vane-tests-h2up-engine.lock"))
            .expect("lock file");
        // SAFETY: flock on a regular file.
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
        assert_eq!(rc, 0);
        f
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let upstream = rt.block_on(spawn_h2_upstream());

    let port = free_port();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]
http2 = true

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false
"#
    ));
    let _server = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let post_body: Vec<u8> = b"echo-me-".repeat(600); // 4.8 KiB
    let mut req = format!(
        "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        post_body.len()
    )
    .into_bytes();
    req.extend_from_slice(&post_body);
    let (status, body) = roundtrip(proxy, &req);
    assert_eq!(status, 200);
    assert_eq!(body, post_body, "POST body round-trips through h2 framing");
}
