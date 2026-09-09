//! Docker provider test: validates the UDS HTTP client and route
//! compilation against a mock Docker API server.

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use vane_control::providers::docker::DockerProvider;

use std::sync::OnceLock;

fn mock_json() -> &'static str {
    static MOCK: OnceLock<String> = OnceLock::new();
    MOCK.get_or_init(|| {
        let containers = serde_json::json!([
            {
                "Id": "a1b2c3",
                "Labels": {
                    "vane.enable": "true",
                    "vane.host": "mock.local",
                    "vane.path": "/api/*rest",
                    "vane.cluster": "mock",
                    "vane.port": "8080"
                },
                "Ports": [{"PrivatePort": 8080, "IP": "172.17.0.2"}]
            }
        ]);
        let body = serde_json::to_string(&containers).unwrap_or_default();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    })
}

/// Mock Docker API on a UDS socket.
async fn spawn_mock_uds(path: PathBuf) {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(path).expect("bind mock uds");
    tokio::spawn(async move {
        loop {
            if let Ok((mut sock, _)) = listener.accept().await {
                let resp = mock_json();
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
                // Give the reader time to receive the response before the
                // mock's accept loop can be torn down.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
}

#[tokio::test]
async fn docker_provider_parses_mock_response() {
    let sock_path =
        std::env::temp_dir().join(format!("vane-docker-mock-{}.sock", std::process::id()));
    spawn_mock_uds(sock_path.clone()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let provider = DockerProvider::new(sock_path, Duration::from_secs(1));
    let routes = provider.list_routes().await.expect("list_routes");

    assert_eq!(routes.len(), 1, "expected 1 route, got {routes:?}");
    let r = &routes[0];
    assert_eq!(r.host.as_deref(), Some("mock.local"));
    assert_eq!(r.pattern, "/api/*rest");
    assert_eq!(r.cluster, "mock");
    assert!(r.addr.to_string().starts_with("172.17."));
    assert_eq!(r.addr.port(), 8080);
}
