//! vane proxy internals — exposed for integration testing and embedding.
//!
//! The binary (`main.rs`) is a thin CLI over [`server::run`].

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod admin;
#[cfg(feature = "h2")]
pub mod h2_client;
#[cfg(feature = "h2")]
pub mod h2_edge;
#[cfg(feature = "h2")]
pub mod h2_server;
#[cfg(feature = "h3")]
pub mod h3_edge;

pub mod proxy;
pub mod server;
pub mod sidecar;
pub mod tls_reload;
pub mod tracing_util;
