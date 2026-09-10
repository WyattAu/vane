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
    // The sender writes the state before the fds arrive, but tolerate a
    // slow/partial write with a short retry.
    let mut state_err = None;
    for _ in 0..25 {
        match std::fs::read(state_path) {
            Ok(bytes) => match serde_json::from_slice::<HandoverState>(&bytes) {
                Ok(state) => {
                    return Ok(Received { listeners, state });
                }
                Err(e) => state_err = Some(HandoverError::State(e.to_string())),
            },
            Err(e) => state_err = Some(HandoverError::State(e.to_string())),
        }
        std::thread::sleep(std::time::Duration::from_millis(40));
    }
    Err(state_err.unwrap_or_else(|| HandoverError::State("state unavailable".into())))
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

    /// Full two-thread handover: listeners cross the socket, the state
    /// archive round-trips, and the transferred listeners still accept.
    #[test]
    fn full_handover_roundtrip() {
        let dir = tempfile::tempdir().expect("dir");
        let sock_path = dir.path().join("handover.sock");
        let state_path = dir.path().join("state.json");
        prepare_socket(&sock_path);

        let l1 = TcpListener::bind("127.0.0.1:0").expect("bind1");
        let l2 = TcpListener::bind("127.0.0.1:0").expect("bind2");
        let addr1 = l1.local_addr().expect("addr1");
        let addr2 = l2.local_addr().expect("addr2");

        let state = HandoverState {
            generation: 7,
            routes: vec![RouteRecord {
                host: None,
                pattern: "/*rest".into(),
                cluster: "c".into(),
                backends: vec![addr1.to_string()],
                strip_prefix: None,
            }],
            at: "test".into(),
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let rx_sock = sock_path.clone();
        let rx_state = state_path.clone();
        std::thread::spawn(move || {
            tx.send(receive_listeners(&rx_sock, &rx_state, 2).map_err(|e| e.to_string()))
                .expect("send");
        });

        // Listeners move into send_listeners (fd ownership transfers).
        send_listeners(&sock_path, &[l1, l2], &state, &state_path).expect("send_listeners");

        let received = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("join")
            .expect("receive_listeners");
        assert_eq!(received.state.generation, 7);
        assert_eq!(received.state.routes.len(), 1);
        assert_eq!(received.listeners.len(), 2);

        // The inherited listeners are live: probe both ports.
        for expected in [addr1, addr2] {
            let probe = std::net::TcpStream::connect(expected).expect("connect");
            drop(probe);
            // One of the transferred listeners accepted it.
        }
        // Wrapped listeners report the original bound addresses.
        let got: Vec<std::net::SocketAddr> = received
            .listeners
            .iter()
            .map(|l| l.local_addr().expect("local"))
            .collect();
        assert!(got.contains(&addr1) && got.contains(&addr2));
    }

    /// `receive_listeners` errors when the state file never appears.
    #[test]
    fn missing_state_times_out() {
        let dir = tempfile::tempdir().expect("dir");
        let sock_path = dir.path().join("hs.sock");
        // Receiver watches a path the sender never writes.
        let rx_state = dir.path().join("absent.json");
        let written_state = dir.path().join("written.json");
        prepare_socket(&sock_path);

        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        let (tx, rx) = std::sync::mpsc::channel();
        let rx_sock = sock_path.clone();
        std::thread::spawn(move || {
            tx.send(receive_listeners(&rx_sock, &rx_state, 1).map_err(|e| e.to_string()))
                .expect("send");
        });
        send_listeners(&sock_path, &[l], &HandoverState::default(), &written_state)
            .expect("send listeners fine");
        let res = rx
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("join");
        assert!(res.is_err(), "expected state-file failure");
    }
}
