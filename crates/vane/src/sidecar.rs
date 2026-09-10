//! SHM sidecar server mode: bridges shared-memory calls to HTTP upstreams.
//!
//! Payload contract (application-defined; this binary speaks HTTP/1.1):
//! the request payload is a full HTTP/1.1 request head (+ optional body);
//! the response payload is the upstream's response bytes verbatim.
//!
//! The same bridge runs **in-process** when `[sidecar] enabled = true` in
//! the main proxy config — co-located services then talk to the running
//! proxy over shared memory with no extra binary.

use vane_control::VaneConfig;
use vane_shm::transport::{SidecarClient, SidecarConfig, SidecarServer};

/// Sidecar bridge configuration resolved from the main config.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// SHM transport base.
    pub base: String,
    /// Slot size.
    pub slot_size: u32,
    /// Slots per direction.
    pub slots: u32,
    /// Upstream backends (round-robin).
    pub backends: Vec<std::net::SocketAddr>,
}

/// Resolves the bridge config: `[sidecar]` section + default cluster
/// backends (first route's cluster, else first cluster).
#[must_use]
pub fn resolve_bridge(cfg: &VaneConfig) -> Option<BridgeConfig> {
    let cluster_name = cfg
        .routes
        .first()
        .map(|r| r.cluster.clone())
        .or_else(|| cfg.clusters.keys().next().cloned())?;
    let backends: Vec<std::net::SocketAddr> = cfg
        .clusters
        .get(&cluster_name)
        .map(|c| c.backends.iter().filter_map(|b| b.parse().ok()).collect())
        .unwrap_or_default();
    if backends.is_empty() {
        return None;
    }
    Some(BridgeConfig {
        base: cfg.sidecar.base.clone(),
        slot_size: cfg.sidecar.slot_size,
        slots: cfg.sidecar.slots,
        backends,
    })
}

/// Opens the transport and spawns the bridge on a blocking thread.
/// Returns immediately; the bridge runs until the process exits.
///
/// # Errors
/// Transport setup failure.
pub fn spawn_bridge(cfg: &VaneConfig) -> Result<(), String> {
    let Some(bridge) = resolve_bridge(cfg) else {
        return Err("no backends resolvable for the sidecar bridge".into());
    };
    let shm_cfg = SidecarConfig {
        base: bridge.base.clone().into(),
        slot_size: bridge.slot_size,
        slots: bridge.slots,
    };
    let mut server = SidecarServer::open(&shm_cfg).map_err(|e| e.to_string())?;
    tracing::info!("sidecar: serving on {}", shm_cfg.base.display());
    let backends = bridge.backends;
    std::thread::Builder::new()
        .name("vane-sidecar-bridge".into())
        .spawn(move || {
            let mut next_backend = 0usize;
            loop {
                match server.recv(std::time::Duration::from_millis(500)) {
                    Ok(Some((id, request))) => {
                        let addr = backends[next_backend % backends.len()];
                        next_backend += 1;
                        let response = blocking_http_call(addr, &request);
                        let _ = server.reply(id, &response, std::time::Duration::from_secs(5));
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::error!("sidecar recv: {e}");
                        return;
                    }
                }
            }
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Runs the sidecar bridge until the channel dies (standalone subcommand).
pub async fn run(config_path: Option<String>, base: String) -> i32 {
    let mut cfg = match crate::server::load_config(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vane sidecar: {e}");
            return 1;
        }
    };
    cfg.sidecar.base = base;
    cfg.sidecar.enabled = true;
    match spawn_bridge(&cfg) {
        Ok(()) => {
            println!("vane sidecar: serving on {}", cfg.sidecar.base);
            // Park forever; the bridge thread does the work.
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
        Err(e) => {
            eprintln!("vane sidecar: {e}");
            1
        }
    }
}

/// Blocking minimal HTTP/1.1 relay to `addr`.
fn blocking_http_call(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
    use std::io::{Read as _, Write as _};
    let bad_gw = b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n".to_vec();
    let Ok(mut stream) =
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(3))
    else {
        return bad_gw;
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    if stream.write_all(request).is_err() {
        return bad_gw;
    }
    let mut response = Vec::with_capacity(4096);
    let _ = stream.read_to_end(&mut response);
    if response.is_empty() {
        return bad_gw;
    }
    response
}

// Keep the client import referenced for doc builds.
#[allow(unused)]
fn _typecheck(_: SidecarClient) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_call_relays_response() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            use std::io::{Read as _, Write as _};
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
            }
        });
        let resp = blocking_http_call(addr, b"GET / HTTP/1.1\r\nHost: t\r\n\r\n");
        assert!(resp.starts_with(b"HTTP/1.1 200 OK"));
        assert!(resp.ends_with(b"hi"));
    }

    #[test]
    fn http_call_502_on_refused() {
        // Port 1 on loopback is reliably closed.
        let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let resp = blocking_http_call(addr, b"GET / HTTP/1.1\r\nHost: t\r\n\r\n");
        assert!(resp.starts_with(b"HTTP/1.1 502"));
    }
}
