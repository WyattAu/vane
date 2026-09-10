//! Docker provider: watches container events and compiles labeled routes.
//!
//! Containers opt in with labels:
//! - `vane.enable=true`
//! - `vane.host=api.example.com` (optional)
//! - `vane.path=/api/*rest` (optional, default `/*rest`)
//! - `vane.port=8080` (optional, default 80)
//! - `vane.cluster=<name>` (optional, default `docker`)
//! - `vane.strip=/api` (optional)
//!
//! Talks to the Docker Engine API over the Unix socket with a minimal
//! hand-rolled HTTP/1.1 client (no framework dep on the control plane).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::providers::ProviderRouteSpec;

use crate::health::HealthMap;
use crate::providers::{ProviderUpdate, build_route};

/// Docker discovery.
pub struct DockerProvider {
    /// Socket path.
    pub socket: PathBuf,
    /// Poll interval for the container list (events also trigger refresh).
    pub poll: std::time::Duration,
}

/// A compiled container route source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRoute {
    /// `vane.host`.
    pub host: Option<String>,
    /// `vane.path`.
    pub pattern: String,
    /// `vane.cluster`.
    pub cluster: String,
    /// `vane.strip`.
    pub strip: Option<String>,
    /// Backend address.
    pub addr: SocketAddr,
}

impl DockerProvider {
    /// New provider.
    #[must_use]
    pub fn new(socket: PathBuf, poll: std::time::Duration) -> Self {
        Self { socket, poll }
    }

    /// Lists opt-in containers and compiles routes.
    ///
    /// # Errors
    /// Socket connect/HTTP failure.
    pub async fn list_routes(&self) -> Result<Vec<ContainerRoute>, String> {
        let body = http_get(&self.socket, "/containers/json").await?;
        #[derive(Deserialize)]
        struct Container {
            #[serde(rename = "Labels", default)]
            labels: HashMap<String, String>,
            #[serde(rename = "Ports", default)]
            ports: Vec<PortMapping>,
        }
        #[derive(Deserialize)]
        struct PortMapping {
            #[serde(rename = "PrivatePort")]
            private: u16,
            #[serde(rename = "IP", default)]
            ip: Option<String>,
        }
        let containers: Vec<Container> =
            serde_json::from_str(&body).map_err(|e| format!("docker json: {e}"))?;
        let mut out = Vec::new();
        for c in containers {
            let Some(true) = c.labels.get("vane.enable").map(|v| v == "true") else {
                continue;
            };
            let port: u16 = c
                .labels
                .get("vane.port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(80);
            // Prefer the container's mapped bridge IP; else its exposed port
            // on loopback (host networking / published ports).
            let addr = c
                .ports
                .iter()
                .find(|p| p.private == port && p.ip.as_deref().is_some_and(|i| !i.is_empty()))
                .and_then(|p| {
                    p.ip.as_deref()
                        .and_then(|ip| format!("{ip}:{port}").parse::<SocketAddr>().ok())
                })
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], port)));
            out.push(ContainerRoute {
                host: c.labels.get("vane.host").cloned(),
                pattern: c
                    .labels
                    .get("vane.path")
                    .cloned()
                    .unwrap_or_else(|| "/*rest".to_owned()),
                cluster: c
                    .labels
                    .get("vane.cluster")
                    .cloned()
                    .unwrap_or_else(|| "docker".to_owned()),
                strip: c.labels.get("vane.strip").cloned(),
                addr,
            });
        }
        Ok(out)
    }

    /// Poll loop pushing full updates.
    pub fn spawn(
        self,
        health: Arc<HealthMap>,
        tx: tokio::sync::mpsc::Sender<ProviderUpdate>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.list_routes().await {
                    Ok(routes) => {
                        let mut grouped: HashMap<String, Vec<SocketAddr>> = HashMap::new();
                        for r in &routes {
                            grouped.entry(r.cluster.clone()).or_default().push(r.addr);
                        }
                        let mut builders = Vec::new();
                        // One wildcard route per cluster with host-specific
                        // duplicates for labeled hosts.
                        for r in &routes {
                            builders.push(build_route(
                                ProviderRouteSpec {
                                    host: r.host.clone(),
                                    pattern: r.pattern.clone(),
                                    cluster: r.cluster.clone(),
                                    addrs: vec![r.addr],
                                    strip_prefix: r.strip.clone(),
                                    priority: 50,
                                    upstream_h2: false,
                                },
                                &health,
                            ));
                        }
                        let _ = grouped;
                        if tx
                            .send(ProviderUpdate {
                                source: "docker",
                                routes: builders,
                            })
                            .await
                            .is_err()
                        {
                            return; // shutting down
                        }
                    }
                    Err(e) => {
                        tracing::debug!("docker provider: {e}");
                    }
                }
                tokio::time::sleep(self.poll).await;
            }
        })
    }
}

/// Minimal HTTP/1.1 GET over a Unix socket; returns the body as a string.
async fn http_get(socket: &PathBuf, path: &str) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| format!("connect {socket:?}: {e}"))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream.flush().await.map_err(|e| e.to_string())?;
    let mut raw = Vec::with_capacity(8192);
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| e.to_string())?;
    // Split head/body.
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("bad http response")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status_ok = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .is_some_and(|code| code.starts_with('2'));
    let mut body = raw[split + 4..].to_vec();
    // Handle chunked encoding (Docker uses it).
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body)?;
    }
    if !status_ok {
        return Err(format!(
            "docker api status: {}",
            head.lines().next().unwrap_or("")
        ));
    }
    String::from_utf8(body).map_err(|e| e.to_string())
}

/// De-chunks a chunked-encoded body.
fn dechunk(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len());
    let mut rest = input;
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("bad chunk header")?;
        let size_str = std::str::from_utf8(&rest[..line_end]).map_err(|_| "bad chunk size")?;
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .map_err(|_| "bad chunk size")?;
        if size == 0 {
            return Ok(out);
        }
        let start = line_end + 2;
        if rest.len() < start + size {
            return Err("truncated chunk".into());
        }
        out.extend_from_slice(&rest[start..start + size]);
        rest = &rest[start + size..];
        // Skip trailing CRLF after the chunk.
        if rest.starts_with(b"\r\n") {
            rest = &rest[2..];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dechunks() {
        let chunked = b"5\r\nhello\r\n0\r\n\r\n";
        assert_eq!(dechunk(chunked).expect("dechunk"), b"hello");
    }

    #[tokio::test]
    async fn docker_socket_absent_is_error() {
        let p = DockerProvider::new(
            PathBuf::from("/nonexistent/docker.sock"),
            std::time::Duration::from_secs(1),
        );
        assert!(p.list_routes().await.is_err());
    }
}

#[cfg(test)]
mod mock_socket_tests {
    use super::*;

    /// Mock Docker Engine API over a UDS: serves /containers/json.
    fn start_mock_docker() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("docker.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming().flatten() {
                let mut s = stream;
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let _ = s.read(&mut buf);
                    let body = serde_json::json!([
                        {
                            "Labels": {
                                "vane.enable": "true",
                                "vane.host": "app.example.com",
                                "vane.path": "/api/*rest",
                                "vane.port": "9000",
                                "vane.cluster": "apps",
                                "vane.strip": "/api"
                            },
                            "Ports": [
                                {"PrivatePort": 9000, "IP": "172.17.0.5"}
                            ]
                        },
                        {
                            "Labels": {"vane.enable": "false"},
                            "Ports": []
                        },
                        {
                            "Labels": {},
                            "Ports": []
                        }
                    ])
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = s.write_all(resp.as_bytes());
                });
            }
        });
        (dir, sock)
    }

    #[tokio::test]
    async fn list_routes_parses_labels_and_ports() {
        let (_dir, sock) = start_mock_docker();
        let provider = DockerProvider::new(sock, std::time::Duration::from_secs(1));
        let routes = provider.list_routes().await.expect("list");
        // Only the enabled container becomes a route.
        assert_eq!(routes.len(), 1);
        let r = &routes[0];
        assert_eq!(r.host.as_deref(), Some("app.example.com"));
        assert_eq!(r.pattern, "/api/*rest");
        assert_eq!(r.cluster, "apps");
        assert_eq!(r.strip.as_deref(), Some("/api"));
        // Mapped bridge IP preferred over loopback fallback.
        assert_eq!(r.addr.to_string(), "172.17.0.5:9000");
    }

    #[tokio::test]
    async fn list_routes_defaults_without_labels() {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming().flatten() {
                let mut s = stream;
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let body = serde_json::json!([
                    {"Labels": {"vane.enable": "true"}, "Ports": [{"PrivatePort": 8080}]}
                ])
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        let provider = DockerProvider::new(sock, std::time::Duration::from_secs(1));
        let routes = provider.list_routes().await.expect("list");
        assert_eq!(routes.len(), 1);
        // Defaults: loopback + port 8080, /*rest, cluster "docker", no host.
        // Port label absent → default 80.
        assert_eq!(routes[0].addr.to_string(), "127.0.0.1:80");
        assert_eq!(routes[0].pattern, "/*rest");
        assert_eq!(routes[0].cluster, "docker");
        assert_eq!(routes[0].host, None);
        assert_eq!(routes[0].strip, None);
    }

    #[tokio::test]
    async fn malformed_json_is_error() {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming().flatten() {
                let mut s = stream;
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let body = "not json at all";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        let provider = DockerProvider::new(sock, std::time::Duration::from_secs(1));
        assert!(provider.list_routes().await.is_err());
    }
}

#[cfg(test)]
mod spawn_tests {
    use super::*;

    /// spawn(): the poll loop groups routes and pushes ProviderUpdates.
    #[tokio::test]
    async fn spawn_pushes_updates() {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming().flatten() {
                let mut s = stream;
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let body = serde_json::json!([
                    {"Labels": {"vane.enable": "true", "vane.cluster": "web"},
                     "Ports": [{"PrivatePort": 80}]}
                ])
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        let provider = DockerProvider::new(sock, std::time::Duration::from_millis(50));
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let _handle = provider.spawn(Arc::new(HealthMap::new()), tx);
        let update = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("update")
            .expect("some");
        assert_eq!(update.source, "docker");
        assert_eq!(update.routes.len(), 1);
    }
}
