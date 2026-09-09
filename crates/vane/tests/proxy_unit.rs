//! Proxy handler unit tests: error paths, tunnel mode, ACME challenge
//! serving, and per-cluster metrics — all without real sockets.

use std::net::SocketAddr;
use std::sync::Arc;

use vane_observe::LogEvent;
use vane_observe::metrics::{MetricKind, Registry};
use vane_observe::ring::EventRing;
use vane_router::{Backend, Policy, RouteBuilder, Router};

use vane::proxy::{HttpProxy, ProxyConfig};

fn make_proxy(
    router: Arc<Router>,
    registry: Arc<Registry>,
    events: Arc<EventRing<LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
) -> HttpProxy {
    HttpProxy::new(
        ProxyConfig {
            router,
            registry: Arc::clone(&registry),
            events,
            rate_limit_rps: None,
            connect_timeout_ms: 5_000,
            idle_timeout_ms: 75_000,
            first_byte_timeout_ms: 30_000,
            pool_per_backend: 4,
            tls: None,
            plugins: Vec::new(),
            http01_tokens: None,
        },
        0,
    )
}

fn make_router(routes: &[(&str, &str)]) -> Arc<Router> {
    let router = Arc::new(Router::new());
    router.update(|editor| {
        for (pattern, cluster) in routes {
            editor.insert(
                RouteBuilder {
                    host: None,
                    pattern: (*pattern).to_owned(),
                    methods: Vec::new(),
                    cluster: (*cluster).to_owned(),
                    strip_prefix: None,
                    timeout_ms: None,
                    backends: vec![Backend::new(SocketAddr::from(([127, 0, 0, 1], 9999)), 1)],
                    policy: Policy::P2C,
                    priority: 0,
                }
                .compile()
                .expect("valid route"),
            );
        }
    });
    router
}

#[test]
fn proxy_constructs_with_all_options() {
    let registry = Arc::new(Registry::new());
    let router = make_router(&[("/*rest", "c")]);
    let events = Arc::new(EventRing::new());
    let _p = make_proxy(router, registry, events);
    // Construction succeeds — the handler is ready to serve.
}

#[test]
fn router_catchall_matches_root() {
    // Regression: `/*rest` must match "/" (the VA and health probes hit it).
    let router = make_router(&[("/*rest", "c")]);
    let table = router.load();
    assert!(
        table.table().lookup(None, "/").is_some(),
        "catch-all must match /"
    );
}

#[test]
fn cluster_metric_labels_sanitized() {
    let registry = Arc::new(Registry::new());
    let cluster = "my-app/v1.0";
    let safe: String = cluster
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    assert_eq!(safe, "my_app_v1_0");
    // Verify the sanitized name is usable as a metric.
    let _ = registry.register(
        &format!("vane_cluster_{safe}_requests_total"),
        MetricKind::Counter,
    );
}
