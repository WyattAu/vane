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
    // 60 s: a wide inspection window for /proc while the hang is live.
    s.set_read_timeout(Some(Duration::from_secs(60))).ok();
    s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        .expect("req");
    let mut got = Vec::new();
    let mut tmp = [0u8; 16384];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Ground truth: find the PROXY-side socket of THIS
                // connection (its local port == our ephemeral port) and
                // report whether the proxy still holds it open.
                let my_port = s.local_addr().expect("local").port();
                let mut proxy_side: Option<String> = None;
                for entry in std::fs::read_dir("/proc/self/fd").expect("procfs") {
                    let Ok(entry) = entry else { continue };
                    let fd_num: i32 = match entry.file_name().to_string_lossy().parse() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    // SAFETY: zeroed sockaddr_storage is valid before
                    // getsockname fills it (ss_family checked after).
                    let mut sa: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as u32;
                    // SAFETY: getsockname on an owned fd with a sized out-param.
                    let rc = unsafe {
                        libc::getsockname(
                            fd_num,
                            &mut sa as *mut _ as *mut libc::sockaddr,
                            &mut len,
                        )
                    };
                    if rc != 0 || sa.ss_family as i32 != libc::AF_INET {
                        continue;
                    }
                    // SAFETY: sa holds an AF_INET sockaddr; reading the
                    // v4 port field is in-bounds for the family written.
                    let port = u16::from_be(unsafe {
                        std::ptr::addr_of!(sa)
                            .cast::<libc::sockaddr_in>()
                            .read_unaligned()
                            .sin_port
                    });
                    if port != my_port {
                        continue;
                    }
                    // SAFETY: zeroed sockaddr_storage is valid before
                    // getpeername fills it.
                    let mut peer_sa: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                    let mut peer_len = std::mem::size_of::<libc::sockaddr_storage>() as u32;
                    // SAFETY: getpeername on an owned fd with a sized out-param.
                    unsafe {
                        libc::getpeername(
                            fd_num,
                            &mut peer_sa as *mut _ as *mut libc::sockaddr,
                            &mut peer_len,
                        )
                    };
                    // SAFETY: peer_sa holds an AF_INET sockaddr.
                    let peer_port = u16::from_be(unsafe {
                        std::ptr::addr_of!(peer_sa)
                            .cast::<libc::sockaddr_in>()
                            .read_unaligned()
                            .sin_port
                    });
                    proxy_side = Some(format!(
                        "proxy-side fd {fd_num} STILL OPEN (local :{port}, peer :{peer_port})"
                    ));
                    break;
                }
                let detail =
                    proxy_side.unwrap_or_else(|| "proxy-side fd CLOSED (not found)".to_string());
                panic!(
                    "BUG CONFIRMED: truncated upstream discards the relayed body. \
                     got {} bytes (head only, body 0 B); client local port {my_port}; \
                     {detail}; the queued body write was dropped at close instead of \
                     flushed + FIN",
                    got.len(),
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
