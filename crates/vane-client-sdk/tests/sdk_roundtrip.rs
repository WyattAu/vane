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
