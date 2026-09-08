//! Discovery providers (`CP-03`): file, docker, kubernetes.
//!
//! Each provider produces full [`ProviderUpdate`] payloads; the
//! [`Reconciler`](crate::Reconciler) merges them by source precedence.

#[cfg(feature = "docker-provider")]
pub mod docker;
#[cfg(feature = "file-provider")]
pub mod file;
#[cfg(feature = "k8s")]
pub mod k8s;

use std::net::SocketAddr;

use vane_router::{Backend, RouteBuilder};

/// One provider's view of the world (full replacement, not a delta —
/// reconciliation stays trivially idempotent).
/// One provider's full view of routing (see module docs).
#[derive(Debug, Clone)]
pub struct ProviderUpdate {
    /// Provider name (`file`, `docker`, `kubernetes`).
    pub source: &'static str,
    /// Compiled routes from this provider.
    pub routes: Vec<RouteBuilder>,
}

/// Helper for providers: builds a `RouteBuilder` with shared health flags.
#[must_use]
pub fn build_route(
    host: Option<String>,
    pattern: String,
    cluster: String,
    addrs: Vec<SocketAddr>,
    health: &crate::health::HealthMap,
    strip_prefix: Option<String>,
    priority: u32,
) -> RouteBuilder {
    let backends: Vec<Backend> = addrs
        .into_iter()
        .map(|addr| {
            let mut b = Backend::new(addr, 1);
            b.set_healthy(health.is_healthy(addr));
            b.attach_health(health.flag(addr));
            b
        })
        .collect();
    RouteBuilder {
        host,
        pattern,
        methods: Vec::new(),
        cluster,
        strip_prefix,
        timeout_ms: None,
        backends,
        policy: vane_router::Policy::P2C,
        priority,
    }
}
