//! SHM sidecar server mode: bridges shared-memory calls to HTTP upstreams.
//!
//! Payload contract (application-defined; this binary speaks HTTP/1.1):
//! the request payload is a full HTTP/1.1 request head (+ optional body);
//! the response payload is the upstream's response bytes verbatim.

use vane_shm::transport::{SidecarClient, SidecarConfig, SidecarServer};

/// Runs the sidecar bridge until the channel dies.
pub async fn run(config_path: Option<String>, base: String) -> i32 {
    let cfg = match crate::server::load_config(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vane sidecar: {e}");
            return 1;
        }
    };
    let shm_cfg = SidecarConfig::dev_shm(base.trim_start_matches("/dev/shm/"));
    let mut server = match SidecarServer::open(&shm_cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("vane sidecar: {e}");
            return 1;
        }
    };
    println!("vane sidecar: serving on {}", shm_cfg.base.display());

    // Bridge on a blocking thread; responses forwarded back through the SHM
    // transport. Requests are plain HTTP/1.1 bytes relayed to the first
    // healthy backend of the default cluster.
    let default_cluster = cfg
        .routes
        .first()
        .map(|r| r.cluster.clone())
        .or_else(|| cfg.clusters.keys().next().cloned())
        .unwrap_or_default();
    let backends: Vec<std::net::SocketAddr> = cfg
        .clusters
        .get(&default_cluster)
        .map(|c| c.backends.iter().filter_map(|b| b.parse().ok()).collect())
        .unwrap_or_default();
    if backends.is_empty() {
        eprintln!("vane sidecar: no backends for cluster `{default_cluster}`");
        return 1;
    }

    tokio::task::spawn_blocking(move || {
        let mut next_backend = 0usize;
        loop {
            match server.recv(std::time::Duration::from_millis(500)) {
                Ok(Some((id, request))) => {
                    let addr = backends[next_backend % backends.len()];
                    next_backend += 1;
                    let response = blocking_http_call(addr, &request);
                    let _ = server.reply(id, &response, std::time::Duration::from_secs(5));
                }
                Ok(None) => continue, // idle spin (500ms cadence)
                Err(e) => {
                    tracing::error!("sidecar recv: {e}");
                    return;
                }
            }
        }
    })
    .await
    .ok();
    0
}

/// Blocking minimal HTTP/1.1 relay to `addr`.
fn blocking_http_call(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
    use std::io::{Read, Write};
    let Ok(mut stream) =
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(3))
    else {
        return b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n".to_vec();
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    if stream.write_all(request).is_err() {
        return b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n".to_vec();
    }
    let mut response = Vec::with_capacity(4096);
    let _ = stream.read_to_end(&mut response);
    if response.is_empty() {
        return b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n".to_vec();
    }
    response
}

// Keep the client import referenced for doc builds.
#[allow(unused)]
fn _typecheck(_: SidecarClient) {}
