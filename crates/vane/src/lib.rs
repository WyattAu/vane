//! vane proxy internals — exposed for integration testing and embedding.
//!
//! The binary (`main.rs`) is a thin CLI over [`server::run`].

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod admin;
pub mod proxy;
pub mod server;
pub mod sidecar;
