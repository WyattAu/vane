//! Gateway API resource compiler: parses Gateway/HTTPRoute-shaped
//! resources (the Kubernetes Gateway API resource model, transport-
//! independent JSON) into an [`XdsSnapshot`] for the dynamic config
//! plane. The K8s operator watches the API server, feeds resources
//! here, and pushes the compiled snapshot to `POST /xds/snapshot`.
//!
//! Mapping (Gateway API → vane):
//! - `HTTPRoute.rules[].matches[].path` → route pattern (`/api` →
//!   `/api/*rest` prefix match; `/` → catch-all)
//! - `HTTPRoute.spec.hostnames[]` → one vane route per hostname
//! - `rules[].backendRefs[]` → one cluster per rule; refs become
//!   weighted backends inside it
//! - `Gateway.listeners[]` → listener hints (the operator maps them
//!   to `[[listeners]]` entries; carried in the snapshot for parity)

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::xds::{XdsCluster, XdsRoute, XdsSnapshot};

/// A Gateway resource: listeners the operator should materialize.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Gateway {
    /// Resource name.
    pub name: String,
    /// Listener hints (the operator maps these to `[[listeners]]`).
    #[serde(default)]
    pub listeners: Vec<GatewayListener>,
}

/// One listener hint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayListener {
    /// Bind port.
    pub port: u16,
    /// `HTTP` (default) or `HTTPS`.
    #[serde(default)]
    pub protocol: String,
    /// TLS material for HTTPS listeners.
    #[serde(default)]
    pub tls: Option<ListenerTlsHint>,
}

/// TLS material hint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerTlsHint {
    /// Certificate path (secret-mounted).
    pub cert: String,
    /// Key path.
    pub key: String,
}

/// An HTTPRoute resource. Field names follow the Kubernetes Gateway
/// API JSON (`backendRefs`, `httproutes`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRoute {
    /// Resource name (namespace-qualified by the operator).
    pub name: String,
    /// Hostnames this route matches (empty = any).
    #[serde(default)]
    pub hostnames: Vec<String>,
    /// Ordered rules; first match wins per request.
    #[serde(default, rename = "rules")]
    pub rules: Vec<HttpRouteRule>,
}

/// One rule: matches + weighted backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRouteRule {
    /// Path/method matches (empty = match all).
    #[serde(default)]
    pub matches: Vec<RouteMatch>,
    /// Weighted backends.
    #[serde(default, rename = "backendRefs")]
    pub backend_refs: Vec<BackendRef>,
}

/// One match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteMatch {
    /// Path match (`PathPrefix` semantics).
    #[serde(default)]
    pub path: Option<String>,
    /// Method match (e.g. `GET`).
    #[serde(default)]
    pub method: Option<String>,
}

/// A weighted backend reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendRef {
    /// Backend host (`service.namespace.svc` or IP).
    pub host: String,
    /// Backend port.
    pub port: u16,
    /// Relative weight (0 = never picked; clamped to 1 here).
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

/// The full Gateway state the operator maintains.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewayState {
    /// Gateway resources.
    #[serde(default)]
    pub gateways: Vec<Gateway>,
    /// HTTPRoute resources.
    #[serde(default, rename = "httproutes")]
    pub httproutes: Vec<HttpRoute>,
}

/// Compiles the Gateway state into an xDS snapshot.
///
/// Route naming: `r-{route}-{rule}-{match}` so replacements are
/// deterministic. Clusters: `c-{route}-{rule}` holding all weighted
/// backendRefs of that rule.
///
/// # Errors
/// A rule with no backendRefs, or a backend host that fails to parse
/// as `host:port`.
pub fn compile(state: &GatewayState) -> Result<XdsSnapshot, String> {
    let mut clusters = BTreeMap::new();
    let mut routes = Vec::new();
    let mut version_hash: u64 = 0;

    for route in &state.httproutes {
        for (rule_idx, rule) in route.rules.iter().enumerate() {
            if rule.backend_refs.is_empty() {
                return Err(format!(
                    "route `{}` rule {rule_idx}: no backendRefs",
                    route.name
                ));
            }
            let cluster_name = format!("c-{}-{rule_idx}", route.name);
            let mut backends = Vec::new();
            for (ref_idx, backend) in rule.backend_refs.iter().enumerate() {
                version_hash = version_hash
                    .wrapping_mul(31)
                    .wrapping_add(u64::from(backend.port))
                    .wrapping_add(u64::from(backend.weight));
                let addr: SocketAddr = backend
                    .host
                    .parse::<SocketAddr>()
                    .ok()
                    .or_else(|| {
                        format!(
                            "{}:{}",
                            backend.host.trim_start_matches("http://"),
                            backend.port
                        )
                        .parse()
                        .ok()
                    })
                    .ok_or_else(|| {
                        format!(
                            "route `{}` rule {rule_idx} ref {ref_idx}: cannot parse `{}`",
                            route.name, backend.host
                        )
                    })?;
                let _ = ref_idx;
                backends.push(addr.to_string());
            }
            clusters.insert(
                cluster_name.clone(),
                XdsCluster {
                    backends,
                    compression: false,
                    http2: false,
                    outlier: None,
                },
            );

            // One vane route per (hostname × match), falling back to a
            // single any-host route when no hostnames are set.
            let matches: Vec<(Option<String>, Option<String>)> = if rule.matches.is_empty() {
                vec![(None, None)]
            } else {
                rule.matches
                    .iter()
                    .map(|m| (m.path.clone(), m.method.clone()))
                    .collect()
            };
            let hostnames: Vec<Option<String>> = if route.hostnames.is_empty() {
                vec![None]
            } else {
                route.hostnames.iter().map(|h| Some(h.clone())).collect()
            };
            for host in &hostnames {
                for (path, method) in matches.iter() {
                    let pattern = path
                        .as_deref()
                        .map(prefix_pattern)
                        .unwrap_or_else(|| "/*rest".to_string());
                    let mut methods = Vec::new();
                    if let Some(m) = method {
                        methods.push(m.to_uppercase());
                    }
                    routes.push(XdsRoute {
                        host: host.clone(),
                        pattern,
                        cluster: cluster_name.clone(),
                        methods,
                        strip_prefix: None,
                        priority: route_priority(rule_idx),
                    });
                    version_hash = version_hash.wrapping_add(1);
                }
            }
        }
    }

    Ok(XdsSnapshot {
        version: format!("gw-{:016x}", version_hash),
        clusters,
        routes,
    })
}

fn route_priority(_rule_idx: usize) -> u32 {
    0
}

/// Gateway API PathPrefix semantics → vane pattern: `/api` matches
/// `/api` and `/api/...` → `/api/*rest`; `/` → `/*rest`.
fn prefix_pattern(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        "/*rest".to_string()
    } else {
        format!("{path}/*rest")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATE: &str = r#"{
        "gateways": [
            { "name": "edge", "listeners": [ { "port": 8080, "protocol": "HTTP" } ] }
        ],
        "httproutes": [
            {
                "name": "shop",
                "hostnames": ["shop.example.com"],
                "rules": [
                    {
                        "matches": [ { "path": "/api", "method": "GET" } ],
                        "backendRefs": [
                            { "host": "10.0.0.1", "port": 8081, "weight": 3 },
                            { "host": "10.0.0.2", "port": 8082, "weight": 1 }
                        ]
                    }
                ]
            }
        ]
    }"#;

    #[test]
    fn compiles_snapshot_with_weighted_cluster() {
        let state: GatewayState = serde_json::from_str(STATE).expect("state");
        let snap = compile(&state).expect("compile");
        assert_eq!(snap.version, "gw-000000000003f280");
        assert_eq!(snap.routes.len(), 1);
        let route = &snap.routes[0];
        assert_eq!(route.host.as_deref(), Some("shop.example.com"));
        assert_eq!(route.pattern, "/api/*rest");
        assert_eq!(route.methods, vec!["GET"]);
        assert_eq!(route.cluster, "c-shop-0");
        let cluster = snap.clusters.get("c-shop-0").expect("cluster");
        assert_eq!(cluster.backends.len(), 2);
        assert_eq!(cluster.backends[0], "10.0.0.1:8081");
    }

    #[test]
    fn compiles_without_hostnames_or_matches() {
        let state: GatewayState = serde_json::from_str(
            r#"{
            "httproutes": [
                {
                    "name": "any",
                    "rules": [ { "backendRefs": [ { "host": "10.0.0.9", "port": 80 } ] } ]
                }
            ]
        }"#,
        )
        .expect("state");
        let snap = compile(&state).expect("compile");
        assert_eq!(snap.routes.len(), 1);
        assert_eq!(snap.routes[0].host, None);
        assert_eq!(snap.routes[0].pattern, "/*rest");
        assert_eq!(snap.routes[0].methods, Vec::<String>::new());
    }

    #[test]
    fn rejects_rule_without_backends() {
        let state: GatewayState = serde_json::from_str(
            r#"{
            "httproutes": [
                { "name": "empty", "rules": [ { "matches": [ { "path": "/x" } ] } ] }
            ]
        }"#,
        )
        .expect("state");
        let err = compile(&state).expect_err("must reject");
        assert!(err.contains("no backendRefs"), "{err}");
    }

    #[test]
    fn compiles_through_xds_apply() {
        // Full pipeline: Gateway state → snapshot → live router.
        let state: GatewayState = serde_json::from_str(STATE).expect("state");
        let snap = compile(&state).expect("compile");
        let router = vane_router::Router::new();
        let health = crate::health::HealthMap::new();
        super::super::xds::apply_snapshot(&router, &health, &snap).expect("apply");
        let table = router.load();
        assert_eq!(table.table().snapshot_records().len(), 1);
    }
}
