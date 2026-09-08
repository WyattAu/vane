//! # vane-client-sdk
//!
//! Public SDK for co-located services to talk to a Vane sidecar over
//! POSIX shared memory — no TCP, no loopback, sub-30 µs round trips
//! (`IP-01`).
//!
//! ```no_run
//! use std::time::Duration;
//! use vane_client_sdk::{Sidecar, SidecarClient};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = Sidecar::connect("/dev/shm/vane-sidecar")?;
//! let response = Sidecar::call(&mut client, b"GET /health", Duration::from_secs(1))?;
//! println!("sidecar says: {}", String::from_utf8_lossy(&response));
//! # Ok(())
//! # }
//! ```
//!
//! Non-Rust services use the C ABI in `vane_shm::cabi`
//! (header: `crates/vane-client-sdk/include/vane_sidecar.h`).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub use vane_shm::transport::{SidecarClient, SidecarConfig, SidecarServer};

/// Convenience alias matching SDK naming in other languages.
pub type Client = SidecarClient;

/// Errors surfaced to SDK users.
pub use vane_shm::transport::ShmError;

use std::path::Path;
use std::time::Duration;

/// SDK entry point (thin wrapper over [`SidecarClient`]).
pub struct Sidecar;

impl Sidecar {
    /// Connects to the sidecar transport at `base`.
    ///
    /// The vane sidecar must already be running (it owns the transport
    /// files). `base` matches the `sidecar.base` value in the proxy config.
    ///
    /// # Errors
    /// Transport files missing (sidecar not up) or shm failure.
    pub fn connect(base: impl AsRef<Path>) -> Result<SidecarClient, ShmError> {
        let config = SidecarConfig {
            base: base.as_ref().to_path_buf(),
            ..SidecarConfig::dev_shm("sdk")
        };
        SidecarClient::open(&config)
    }

    /// Request/response helper: send, then wait for the matching reply.
    ///
    /// Responses from other in-flight requests are buffered until `id`
    /// arrives (single-caller ordering keeps this trivial).
    ///
    /// # Errors
    /// Transport failure on send or recv.
    pub fn call(
        client: &mut SidecarClient,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, ShmError> {
        let id = client.send(payload, timeout)?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match client.recv(deadline.saturating_duration_since(std::time::Instant::now()))? {
                Some((rid, data)) if rid == id => return Ok(data),
                Some(_) => continue, // late reply for an older call
                None => return Err(ShmError::Timeout),
            }
        }
    }
}
