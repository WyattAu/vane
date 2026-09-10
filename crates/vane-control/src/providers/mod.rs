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
/// Inputs for [`build_route`] (kept flat for call-site brevity).
pub struct ProviderRouteSpec {
    /// Host match.
    pub host: Option<String>,
    /// Path pattern.
    pub pattern: String,
    /// Target cluster.
    pub cluster: String,
    /// Backend addresses.
    pub addrs: Vec<SocketAddr>,
    /// Prefix strip.
    pub strip_prefix: Option<String>,
    /// Route priority.
    pub priority: u32,
    /// HTTP/2 upstream (prior knowledge).
    pub upstream_h2: bool,
}

/// Builds a route from provider-discovered backends, sharing health
/// flags with the checker.
#[must_use]
pub fn build_route(spec: ProviderRouteSpec, health: &crate::health::HealthMap) -> RouteBuilder {
    let backends: Vec<Backend> = spec
        .addrs
        .into_iter()
        .map(|addr| {
            let mut b = Backend::new(addr, 1);
            b.set_healthy(health.is_healthy(addr));
            b.attach_health(health.flag(addr));
            b
        })
        .collect();
    RouteBuilder {
        host: spec.host,
        pattern: spec.pattern,
        methods: Vec::new(),
        cluster: spec.cluster,
        strip_prefix: spec.strip_prefix,
        timeout_ms: None,
        backends,
        upstream_h2: spec.upstream_h2,
        policy: vane_router::Policy::P2C,
        priority: spec.priority,
    }
}
