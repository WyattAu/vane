//! Hot-upgrade handover: listener fd transfer + route state archive.
//!
//! Old binary: drains in-flight requests → `send_listeners` (SCM_RIGHTS) →
//! writes a state file → exits. New binary: `receive_listeners` → rebuilds
//! the router from the state file → serves. TCP connections are inherited
//! by the kernel; nothing is reset (`IP-02`).

use std::net::TcpListener;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::fdpass;

/// Flattened route record for the state archive (plain data, serde).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRecord {
    /// Host match.
    pub host: Option<String>,
    /// Path pattern.
    pub pattern: String,
    /// Cluster.
    pub cluster: String,
    /// Backend addresses.
    pub backends: Vec<String>,
    /// Strip prefix.
    pub strip_prefix: Option<String>,
}

/// Full handover state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HandoverState {
    /// Config generation.
    pub generation: u64,
    /// Route records (flattened route table).
    pub routes: Vec<RouteRecord>,
    /// ISO timestamp of handover (diagnostics).
    pub at: String,
}

/// Errors from handover.
#[derive(Debug, thiserror::Error)]
pub enum HandoverError {
    /// Fd passing failed.
    #[error("fd pass: {0}")]
    Fd(#[from] crate::fdpass::FdPassError),
    /// State file failure.
    #[error("state: {0}")]
    State(String),
}

/// Sends listeners + writes the state file.
///
/// # Errors
/// Fd passing or state persistence failure.
pub fn send_listeners(
    sock_path: &Path,
    listeners: &[TcpListener],
    state: &HandoverState,
    state_path: &Path,
) -> Result<(), HandoverError> {
    let fds: Vec<i32> = listeners
        .iter()
        .map(std::os::fd::AsRawFd::as_raw_fd)
        .collect();
    // Small delay lets the new binary bind its handover socket.
    for _ in 0..50 {
        if sock_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    fdpass::send_fds(sock_path, &fds)?;
    let json = serde_json::to_vec(state).map_err(|e| HandoverError::State(e.to_string()))?;
    std::fs::write(state_path, json).map_err(|e| HandoverError::State(e.to_string()))?;
    Ok(())
}

/// Received handover payload.
pub struct Received {
    /// Inherited listeners (already bound + listening).
    pub listeners: Vec<TcpListener>,
    /// State archive.
    pub state: HandoverState,
}

/// Receives listeners + reads the state file.
///
/// # Errors
/// Fd passing or state load failure.
pub fn receive_listeners(
    sock_path: &Path,
    state_path: &Path,
    expected_fds: usize,
) -> Result<Received, HandoverError> {
    let (raw_fds, _handshake) = fdpass::recv_fds(sock_path, expected_fds)?;
    if raw_fds.len() != expected_fds {
        return Err(HandoverError::State(format!(
            "expected {expected_fds} fds, got {}",
            raw_fds.len()
        )));
    }
    let listeners = raw_fds
        .into_iter()
        .map(|fd| {
            // SAFETY: fds arrived via SCM_RIGHTS from a previous vane
            // process; each is a listening TCP socket exactly once.
            unsafe { fdpass::tcp_listener_from_fd(fd) }
        })
        .collect();
    let bytes = std::fs::read(state_path).map_err(|e| HandoverError::State(e.to_string()))?;
    let state: HandoverState =
        serde_json::from_slice(&bytes).map_err(|e| HandoverError::State(e.to_string()))?;
    Ok(Received { listeners, state })
}

/// Prepares the handover socket path (unlinks stale sockets).
pub fn prepare_socket(sock_path: &PathBuf) {
    let _ = std::fs::remove_file(sock_path);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_serializes() {
        let state = HandoverState {
            generation: 3,
            routes: vec![RouteRecord {
                host: Some("a.example".into()),
                pattern: "/x/*rest".into(),
                cluster: "c".into(),
                backends: vec!["127.0.0.1:9".into()],
                strip_prefix: None,
            }],
            at: "now".into(),
        };
        let json = serde_json::to_vec(&state).expect("serde");
        let back: HandoverState = serde_json::from_slice(&json).expect("serde back");
        assert_eq!(back.generation, 3);
        assert_eq!(back.routes.len(), 1);
    }
}
