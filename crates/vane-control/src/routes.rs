//! Compiles [`VaneConfig`] entries into [`RouteBuilder`]s for the router.

use std::net::SocketAddr;

use vane_router::{Backend, RouteBuilder};

use crate::config::{ClusterConfig, VaneConfig};
use crate::health::HealthMap;

/// Compiles all static routes from the config.
#[must_use]
pub fn static_routes(cfg: &VaneConfig, health: &HealthMap) -> Vec<RouteBuilder> {
    let mut out = Vec::new();
    for r in &cfg.routes {
        let Some(cluster) = cfg.clusters.get(&r.cluster) else {
            continue;
        };
        let backends = resolve_backends(cluster, health);
        if backends.is_empty() {
            continue;
        }
        out.push(RouteBuilder {
            host: r.host.clone(),
            pattern: r.pattern.clone(),
            methods: r.methods.clone(),
            cluster: r.cluster.clone(),
            strip_prefix: r.strip_prefix.clone(),
            timeout_ms: r.timeout_ms,
            backends,
            upstream_h2: cluster.http2,
            policy: cluster.policy.into(),
            priority: r.priority,
        });
    }
    out
}

/// Resolves a cluster's backend list, sharing health flags across
/// config generations via the [`HealthMap`].
#[must_use]
pub fn resolve_backends(cluster: &ClusterConfig, health: &HealthMap) -> Vec<Backend> {
    let mut out = Vec::new();
    for b in &cluster.backends {
        let addr: Option<SocketAddr> = b
            .parse::<SocketAddr>()
            .ok()
            .or_else(|| format!("127.0.0.1:{b}").parse().ok());
        if let Some(addr) = addr {
            let mut backend = Backend::new(addr, 1);
            backend.set_healthy(health.is_healthy(addr));
            // Re-arm the flag with the health map's shared cell so later
            // generations see probe results.
            let shared = health.flag(addr);
            backend.attach_health(shared);
            out.push(backend);
        }
    }
    out
}
