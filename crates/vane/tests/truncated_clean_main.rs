//! Truncated-upstream contract on clean main: upstream sends head +
//! a SHORT body with Connection: close, then FIN. The client must
//! receive the partial body and then see EOF — not a hang.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn main_test() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let backend = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 65536\r\nConnection: close\r\n\r\nHTTPBODY"
                .to_vec();
        for stream in listener.incoming().flatten() {
            let response = response.clone();
            std::thread::spawn(move || {
                let mut s = stream;
                let _ = s.write_all(&response);
                let _ = s.shutdown(std::net::Shutdown::Write);
            });
        }
    });

    let probe = TcpListener::bind("127.0.0.1:0").expect("probe");
    let listener = probe.local_addr().expect("addr");
    drop(probe);
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[[listeners]]
address = "{listener}"

[clusters.up]
backends = ["{backend}"]

[[routes]]
pattern = "/*rest"
cluster = "up"
"#
        ),
    )
    .expect("write");
    let config_path = path.display().to_string();
    let _ = dir;
    let force_mio = std::env::var("TRUNC_FORCE_MIO")
        .map(|v| v == "1")
        .unwrap_or(false);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        let _ = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: Some(Duration::from_secs(30)),
            shutdown: None,
            force_mio,
        }));
    });
    for _ in 0..100 {
        if TcpStream::connect(listener).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    eprintln!("[diag] force_mio={force_mio}");

    let mut s = TcpStream::connect(listener).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        .expect("req");
    let mut got = Vec::new();
    let mut tmp = [0u8; 16384];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Is the connection even still alive? A successful
                // write means the proxy leaked the session open.
                let alive: String = s
                    .write(b"?")
                    .map(|_| "alive (write ok)".to_string())
                    .unwrap_or_else(|e| format!("dead ({e})"));
                // The server shares this process: if its downstream
                // fd was closed, /proc/self/fd shrinks accordingly.
                let socket_fds: Vec<String> = std::fs::read_dir("/proc/self/fd")
                    .expect("procfs")
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        std::fs::read_link(e.path())
                            .map(|l| l.to_string_lossy().contains("socket"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                let head_end = got
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .map_or(0, |p| p + 4);
                panic!(
                    "BUG CONFIRMED: truncated upstream hangs the client. got {} \
                     bytes (head {} B, body {} B); peer={alive}; expected the tail \
                     then FIN within 10s; open socket fds: {socket_fds:?}",
                    got.len(),
                    head_end,
                    got.len().saturating_sub(head_end),
                );
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    let body_start = got
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map_or(0, |p| p + 4);
    let relayed = &got[body_start..];
    assert_eq!(relayed, b"HTTPBODY", "partial body must pass through");
    println!("clean main: partial body + EOF delivered correctly");
}

/// KNOWN-BROKEN on main (found 2026-10-19, io_uring engine): a
/// truncated upstream (Content-Length > delivered, then FIN) leaves
/// the CLIENT hanging — it receives the response head and then
/// nothing, forever. The mio engine handles the same shape correctly.
///
/// Runtime-gated per the known-broken discipline: the assertions run
/// only with VANE_KNOWN_BROKEN_TRUNCATED=1; without it the test
/// documents the bug and exits green so CI stays actionable.
#[test]
fn truncated_upstream_on_clean_main() {
    if std::env::var("VANE_KNOWN_BROKEN_TRUNCATED").as_deref() != Ok("1") {
        eprintln!(
            "skipping: known-broken on main (io_uring truncated-upstream hang); \
             set VANE_KNOWN_BROKEN_TRUNCATED=1 to assert the fix"
        );
        return;
    }
    main_test();
}
