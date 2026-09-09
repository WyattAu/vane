//! WebSocket tunnel e2e: 101 upgrade through the proxy, then raw
//! bidirectional frame relay (unmasked client frames, as browsers do).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

fn spawn_ws_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                // 1. read the upgrade request head
                let n = s.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                if !req.contains("Upgrade: websocket") && !req.contains("upgrade: websocket") {
                    let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
                    return;
                }
                // 2. accept: 101 with the client's Sec-WebSocket-Key
                let key = req
                    .lines()
                    .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
                    .unwrap_or("dGhlIHNhbXBsZSBub25jZQ==")
                    .trim()
                    .to_owned();
                let resp = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {key}\r\n\r\n"
                );
                if s.write_all(resp.as_bytes()).is_err() {
                    return;
                }
                // 3. echo raw frames until EOF
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if s.write_all(&buf[..n]).is_err() {
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

fn spawn_proxy(upstream: SocketAddr) -> SocketAddr {
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
    std::mem::forget(dir);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    for _ in 0..40 {
        if TcpStream::connect(proxy_addr).is_ok() {
            return proxy_addr;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy did not come up");
}

/// Minimal unmasked client frame (opcode 1 text, len < 126).
fn text_frame(payload: &[u8]) -> Vec<u8> {
    let mut f = vec![0x81u8];
    f.push(payload.len() as u8);
    f.extend_from_slice(payload);
    f
}

#[test]
fn websocket_upgrade_and_tunnel_echo() {
    let upstream = spawn_ws_upstream();
    let proxy = spawn_proxy(upstream);

    let mut s = TcpStream::connect(proxy).expect("connect proxy");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(
        b"GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .expect("upgrade write");

    // Read the 101 head (may arrive with the first echoed frame).
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = s.read(&mut byte).expect("read 101");
        assert!(n > 0, "proxy closed during upgrade");
        head.extend_from_slice(&byte[..n]);
        if head.len() > 4096 {
            panic!("no 101 response: {head:?}");
        }
    }
    let head_str = String::from_utf8_lossy(&head);
    assert!(head_str.starts_with("HTTP/1.1 101"), "{head_str}");

    // Tunnel: echo several frames.
    for payload in ["frame-one", "frame-two-longer-payload"] {
        s.write_all(&text_frame(payload.as_bytes()))
            .expect("frame write");
        let mut buf = vec![0u8; payload.len() + 2];
        s.read_exact(&mut buf).expect("frame read");
        assert_eq!(&buf[2..], payload.as_bytes());
    }

    // Close: client half-close should end the tunnel.
    drop(s);
    std::thread::sleep(Duration::from_millis(200));
}
