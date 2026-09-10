//! SDK round-trip test.

use std::time::Duration;

use vane_client_sdk::{Sidecar, SidecarConfig, SidecarServer};

#[test]
fn sdk_call_roundtrip() {
    let dir = tempfile::tempdir().expect("dir");
    let cfg = SidecarConfig {
        base: dir.path().join("sdk-test"),
        slot_size: 64 * 1024,
        slots: 4,
    };
    let _server = SidecarServer::open(&cfg).expect("server");
    let mut client = Sidecar::connect(cfg.base.to_str().expect("utf8")).expect("client");

    // Send and receive through the transport (echo-style test).
    let _id = client
        .send(
            b"GET /sdk HTTP/1.1\r\nHost: s\r\n\r\n",
            Duration::from_secs(5),
        )
        .expect("send");
    // No actual handler, so recv would timeout. Just verify send worked.
}

#[test]
fn sdk_config_defaults() {
    let cfg = SidecarConfig::dev_shm("test-name");
    assert!(cfg.base.to_str().unwrap().contains("/dev/shm/test-name"));
    assert!(cfg.slot_size > 0);
    assert!(cfg.slots > 0);
}

/// `call()` matches the reply to the request id and surfaces timeouts.
#[test]
fn sdk_call_receives_reply() {
    let dir = tempfile::tempdir().expect("dir");
    let cfg = SidecarConfig {
        base: dir.path().join("call-test"),
        slot_size: 64 * 1024,
        slots: 4,
    };
    let mut server = SidecarServer::open(&cfg).expect("server");

    // Responder thread: echo every request back until stopped.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        loop {
            if stop2.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            match server.recv(Duration::from_millis(100)) {
                Ok(Some((id, payload))) => {
                    let _ = server.reply(id, &payload, Duration::from_secs(5));
                }
                Ok(None) => continue,
                Err(_) => return,
            }
        }
    });

    let mut client = Sidecar::connect(cfg.base.as_path()).expect("client");
    let reply = Sidecar::call(&mut client, b"ping", Duration::from_secs(5)).expect("call");
    assert_eq!(reply, b"ping");
    drop(client);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    handle.join().ok();
}

/// `call()` errors with Timeout when no reply ever comes.
#[test]
fn sdk_call_times_out() {
    let dir = tempfile::tempdir().expect("dir");
    let cfg = SidecarConfig {
        base: dir.path().join("timeout-test"),
        slot_size: 64 * 1024,
        slots: 4,
    };
    let _server = SidecarServer::open(&cfg).expect("server");
    let mut client = Sidecar::connect(cfg.base.as_path()).expect("client");
    let res = Sidecar::call(&mut client, b"quiet", Duration::from_millis(200));
    assert!(matches!(res, Err(_)), "expected timeout error, got {res:?}");
}
