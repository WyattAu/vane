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
        let outlier = cluster.outlier.as_ref().map(|o| {
            let addrs: Vec<std::net::SocketAddr> = backends.iter().map(|b| b.addr).collect();
            vane_router::outlier::OutlierSet::new(addrs, o.consecutive_failures, o.ejection_ms)
                .shared()
        });
        out.push(RouteBuilder {
            host: r.host.clone(),
            pattern: r.pattern.clone(),
            methods: r.methods.clone(),
            cluster: r.cluster.clone(),
            strip_prefix: r.strip_prefix.clone(),
            timeout_ms: r.timeout_ms,
            backends,
            upstream_h2: cluster.http2,
            compression: cluster.compression,
            outlier,
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
    use std::net::ToSocketAddrs as _;
    let mut out = Vec::new();
    for b in &cluster.backends {
        let addr: Option<SocketAddr> = b
            .parse::<SocketAddr>()
            .ok()
            .or_else(|| format!("127.0.0.1:{b}").parse().ok())
            // DNS names (compose service names, K8s services): resolve via
            // the system resolver at control-plane cadence. First hit wins;
            // subsequent generations re-resolve, so failover works.
            .or_else(|| b.to_socket_addrs().ok().and_then(|mut it| it.next()));
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

#[cfg(test)]
mod resolve_tests {
    use super::*;

    #[test]
    fn resolves_ip_literal_and_bare_port() {
        let health = HealthMap::new();
        let cluster = ClusterConfig {
            backends: vec!["127.0.0.1:19001".into(), "19002".into()],
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            http2: false,
            compression: false,
            outlier: None,
        };
        let backends = resolve_backends(&cluster, &health);
        assert_eq!(backends.len(), 2);
    }

    #[test]
    fn resolves_dns_names() {
        let health = HealthMap::new();
        let cluster = ClusterConfig {
            backends: vec!["localhost:19003".into()],
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            http2: false,
            compression: false,
            outlier: None,
        };
        let backends = resolve_backends(&cluster, &health);
        assert_eq!(backends.len(), 1, "localhost must resolve");
        assert!(backends[0].is_healthy() || !backends[0].is_healthy());
    }

    #[test]
    fn drops_unresolvable_names() {
        let health = HealthMap::new();
        let cluster = ClusterConfig {
            backends: vec!["no-such-host.invalid:9".into()],
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            http2: false,
            compression: false,
            outlier: None,
        };
        let backends = resolve_backends(&cluster, &health);
        assert!(
            backends.is_empty(),
            "unresolvable backends are skipped, not fatal"
        );
    }
}
