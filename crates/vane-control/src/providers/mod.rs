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
    /// Gzip-compress responses on this route.
    pub compression: bool,
    /// Per-backend outlier ejection (absent = disabled).
    pub outlier: Option<crate::config::OutlierConfig>,
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
    let outlier = spec.outlier.map(|o| {
        let addrs: Vec<std::net::SocketAddr> = backends.iter().map(|b| b.addr).collect();
        vane_router::outlier::OutlierSet::new(addrs, o.consecutive_failures, o.ejection_ms).shared()
    });
    RouteBuilder {
        host: spec.host,
        pattern: spec.pattern,
        methods: Vec::new(),
        cluster: spec.cluster,
        strip_prefix: spec.strip_prefix,
        timeout_ms: None,
        backends,
        upstream_h2: spec.upstream_h2,
        compression: spec.compression,
        outlier,
        policy: vane_router::Policy::P2C,
        priority: spec.priority,
    }
}

#[cfg(test)]
mod build_route_tests {
    use super::*;

    #[test]
    fn build_route_shares_health_and_compiles() {
        let health = crate::HealthMap::new();
        let addr: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        health.set(addr, true);

        let builder = build_route(
            ProviderRouteSpec {
                host: Some("api.example".into()),
                pattern: "/v1/*rest".into(),
                cluster: "c".into(),
                addrs: vec![addr],
                strip_prefix: Some("/v1".into()),
                priority: 9,
                upstream_h2: true,
                compression: false,
                outlier: None,
            },
            &health,
        );
        let entry = builder.compile().expect("compile");
        assert_eq!(entry.host.as_deref(), Some("api.example"));
        assert_eq!(entry.strip_prefix.as_deref(), Some("/v1"));
        assert!(entry.upstream_h2);
        assert_eq!(entry.priority, 9);
        assert_eq!(entry.backends.len(), 1);
        assert!(entry.backends[0].is_healthy(), "healthy flag shared");
    }

    #[test]
    fn build_route_unhealthy_backend_reflected() {
        let health = crate::HealthMap::new();
        let addr: SocketAddr = "127.0.0.1:9002".parse().expect("addr");
        health.set(addr, false);
        let entry = build_route(
            ProviderRouteSpec {
                host: None,
                pattern: "/*rest".into(),
                cluster: "down".into(),
                addrs: vec![addr],
                strip_prefix: None,
                priority: 0,
                upstream_h2: false,
                compression: false,
                outlier: None,
            },
            &health,
        )
        .compile()
        .expect("compile");
        assert!(!entry.backends[0].is_healthy());
    }

    #[test]
    fn build_route_empty_backends_fails_compile() {
        let health = crate::HealthMap::new();
        let entry = build_route(
            ProviderRouteSpec {
                host: None,
                pattern: "/*rest".into(),
                cluster: "empty".into(),
                addrs: Vec::new(),
                strip_prefix: None,
                priority: 0,
                upstream_h2: false,
                compression: false,
                outlier: None,
            },
            &health,
        );
        assert!(entry.compile().is_err(), "zero backends must not compile");
    }
}
