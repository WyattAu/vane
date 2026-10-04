//! vane proxy internals — exposed for integration testing and embedding.
//!
//! The binary (`main.rs`) is a thin CLI over [`server::run`].

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

// Global allocator (MM-02): mimalloc, feature-gated (default off so
// embedders/tests keep the system allocator). `not(test)` keeps test
// binaries allocator-neutral — the dhat gate installs its own
// counting allocator, which cannot coexist with another one.
#[cfg(all(feature = "mimalloc", not(test)))]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
pub mod admin;
pub mod h1pool;
#[cfg(feature = "h2")]
pub mod h2_client;
#[cfg(feature = "h2")]
pub mod h2_edge;
#[cfg(feature = "h2")]
pub mod h2_server;
#[cfg(feature = "h3")]
#[cfg(feature = "h3")]
pub mod h3_bridge;
pub mod h3_client;
#[cfg(feature = "h3")]
pub mod h3_edge;
pub mod workload;
pub mod xds_client;

pub mod proxy;
pub mod server;
pub mod sidecar;
pub mod tls_reload;
pub mod tracing_util;
