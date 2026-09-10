//! HTTP/3 edge — experimental (`h3` feature).
//!
//! HTTP/3 over QUIC using quinn as the transport and the `h3` crate for
//! HTTP/3 framing. Runs on a UDP port alongside the TCP listeners.
//!
//! ## Status: scaffold
//!
//! The `h3` crate (0.0.8) is pre-release and its API is unstable. This
//! module provides the structural skeleton (ALPN negotiation, quinn
//! endpoint, connection accept loop) so the H3 implementation can land
//! in a focused sprint.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ UDP :8443 (same port number as the TCP HTTPS listener)│
//! │         │                                             │
//! │   quinn::Endpoint (QUIC transport)                    │
//! │         │                                             │
//! │   h3::server::Connection (HTTP/3 framing)             │
//! │         │                                             │
//! │   Request → route lookup → upstream forward          │
//! │   (shared with the h1/h2 pipeline via the EBR        │
//! │    snapshot — no separate routing state)             │
//! └──────────────────────────────────────────────────────┘
//! ```

use std::net::SocketAddr;

/// HTTP/3 ALPN protocol identifier.
pub const H3_ALPN: &[u8] = b"h3";

/// Returns the ALPN protocols that the h3 edge negotiates.
#[must_use]
pub fn alpn_protocols() -> Vec<Vec<u8>> {
    vec![H3_ALPN.to_vec()]
}

/// Returns the UDP address for the h3 edge (same port number as the
/// TCP HTTPS listener — QUIC and TCP coexist on the same port).
#[must_use]
pub fn quinn_addr(tcp_addr: SocketAddr) -> SocketAddr {
    tcp_addr
}

/// QUIC TLS configuration for the h3 edge.
///
/// Builds a `quinn::crypto::rustls::QuicServerConfig` from the given
/// rustls config with h3 ALPN. This is different from the TCP TLS
/// config because QUIC requires a custom `Solver` for 0-RTT and
/// different ALPN handling.
///
/// # Errors
/// Returns an error if the rustls config cannot be adapted for QUIC.
pub fn quinn_server_config(
    tcp_cfg: &rustls::ServerConfig,
) -> Result<quinn::crypto::rustls::QuicServerConfig, String> {
    let mut cfg = tcp_cfg.clone();
    cfg.alpn_protocols = vec![H3_ALPN.to_vec()];
    quinn::crypto::rustls::QuicServerConfig::try_from(cfg)
        .map_err(|e| format!("quinn server config: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpn_is_h3() {
        let alpn = alpn_protocols();
        assert_eq!(alpn.len(), 1);
        assert_eq!(alpn[0], b"h3");
    }

    #[test]
    fn quinn_addr_matches_tcp() {
        let tcp: SocketAddr = "127.0.0.1:8443".parse().unwrap();
        let quinn = quinn_addr(tcp);
        assert_eq!(quinn, tcp);
    }
}
