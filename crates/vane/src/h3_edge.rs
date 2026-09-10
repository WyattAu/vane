//! HTTP/3 edge — experimental (`h3` feature).
//!
//! HTTP/3 over QUIC using quinn as the transport and the `h3` crate for
//! HTTP/3 framing. Runs on a UDP port alongside the TCP listeners.
//!
//! ## Status: scaffold
//!
//! The `h3` crate (0.0.8) is pre-release and its API is unstable. This
//! module provides the structural skeleton so the H3 implementation can
//! land in a focused sprint. Key remaining work:
//!
//! 1. QUIC TLS configuration (quinn requires `QuicServerConfig` from
//!    rustls, not the standard `ServerConfig`)
//! 2. Request body handling (h3 uses `RecvStream` for bodies)
//! 3. Connection-level flow control and keep-alive
//! 4. 0-RTT support
//! 5. Integration with the shared routing table (same EBR snapshot as
//!    the h1/h2 paths)

use std::net::SocketAddr;

/// HTTP/3 ALPN protocol identifier.
pub const H3_ALPN: &[u8] = b"h3";

/// Returns the ALPN protocols that the h3 edge negotiates.
#[must_use]
pub fn alpn_protocols() -> Vec<Vec<u8>> {
    vec![H3_ALPN.to_vec()]
}

/// Returns the UDP port that the h3 edge should listen on (same as the
/// TCP HTTPS listener — QUIC and TCP coexist on the same port number).
#[must_use]
pub fn quinn_addr(tcp_addr: std::net::SocketAddr) -> SocketAddr {
    tcp_addr
}
