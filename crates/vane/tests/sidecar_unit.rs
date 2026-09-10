//! Sidecar bridge unit tests: config resolution, HTTP relay, and a full
//! bridge round-trip against a stub upstream.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::time::Duration;

use vane::sidecar::{resolve_bridge, spawn_bridge};
use vane_control::config::SidecarConfig as SidecarCfg;
use vane_control::{ClusterConfig, RouteConfig, VaneConfig};
use vane_shm::transport::{SidecarClient, SidecarConfig};

fn config_with_cluster(cluster: &str, backends: &[&str]) -> VaneConfig {
    let mut clusters = BTreeMap::new();
    clusters.insert(
        cluster.to_string(),
        ClusterConfig {
            backends: backends.iter().map(|s| (*s).to_string()).collect(),
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            http2: false,
        },
    );
    VaneConfig {
        clusters,
        routes: vec![RouteConfig {
            host: None,
            pattern: "/*rest".into(),
            methods: Vec::new(),
            cluster: cluster.to_string(),
            strip_prefix: None,
            timeout_ms: None,
            priority: 0,
        }],
        ..VaneConfig::default()
    }
}

/// Spawns a one-shot stub upstream that answers a single request then exits.
fn stub_upstream(response: &'static [u8]) -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(response);
        }
    });
    addr
}

#[test]
fn resolve_bridge_prefers_route_cluster() {
    let mut cfg = config_with_cluster("b", &["127.0.0.1:1"]);
    cfg.clusters.insert(
        "a".into(),
        ClusterConfig {
            backends: vec!["127.0.0.1:2".into()],
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            http2: false,
        },
    );
    cfg.routes = vec![RouteConfig {
        host: None,
        pattern: "/*rest".into(),
        methods: Vec::new(),
        cluster: "a".into(),
        strip_prefix: None,
        timeout_ms: None,
        priority: 0,
    }];
    let bridge = resolve_bridge(&cfg).expect("bridge");
    assert_eq!(
        bridge.backends,
        vec!["127.0.0.1:2".parse().expect("valid addr")]
    );
}

#[test]
fn resolve_bridge_none_without_backends() {
    let cfg = config_with_cluster("empty", &[]);
    assert!(resolve_bridge(&cfg).is_none());
}

#[test]
fn resolve_bridge_none_without_clusters() {
    let cfg = VaneConfig::default();
    assert!(resolve_bridge(&cfg).is_none());
}

/// Full bridge round-trip: spawn_bridge + client through real SHM.
#[test]
fn bridge_roundtrip() {
    let dir = tempfile::tempdir().expect("dir");
    let addr = stub_upstream(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone");

    let mut cfg = config_with_cluster("c", &[&addr.to_string()]);
    cfg.sidecar = SidecarCfg {
        enabled: true,
        base: dir.path().join("shm").to_string_lossy().into_owned(),
        slot_size: 64 * 1024,
        slots: 4,
    };

    spawn_bridge(&cfg).expect("spawn");

    let shm_cfg = SidecarConfig {
        base: dir.path().join("shm"),
        slot_size: 64 * 1024,
        slots: 4,
    };
    let mut client = SidecarClient::open(&shm_cfg).expect("client");
    let id = client
        .send(
            b"GET /via-sidecar HTTP/1.1\r\nHost: s\r\n\r\n",
            Duration::from_secs(5),
        )
        .expect("send");
    let (rid, data) = client
        .recv(Duration::from_secs(5))
        .expect("recv")
        .expect("response within timeout");
    assert_eq!(rid, id);
    assert!(data.starts_with(b"HTTP/1.1 200 OK"));
    assert!(data.ends_with(b"done"));
}

/// spawn_bridge errors when no backends resolve.
#[test]
fn spawn_bridge_errors_without_backends() {
    let cfg = config_with_cluster("empty", &[]);
    assert!(spawn_bridge(&cfg).is_err());
}
