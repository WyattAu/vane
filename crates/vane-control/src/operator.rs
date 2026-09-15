//! Kubernetes Gateway API operator: polls the API server for
//! Gateways and HTTPRoutes, maps the K8s JSON shapes into a
//! [`GatewayState`](crate::gateway::GatewayState), and hands it to the
//! compiler. The HTTP fetcher is injected — tests use canned JSON, the
//! binary wires an authenticated client (service-account token).

use serde::Deserialize;

use crate::gateway::{
    BackendRef, Gateway, GatewayListener, HttpRoute, HttpRouteRule, ListenerTlsHint, RouteMatch,
};

/// K8s list response for a Gateway API resource.
#[derive(Debug, Deserialize)]
struct K8sList<T> {
    items: Vec<T>,
}

/// K8s HTTPRoute object (fields we consume).
#[derive(Debug, Deserialize)]
struct K8sHttpRoute {
    metadata: K8sMeta,
    #[serde(default)]
    spec: K8sHttpRouteSpec,
}

#[derive(Debug, Deserialize)]
struct K8sMeta {
    name: String,
    #[serde(default)]
    namespace: String,
}

#[derive(Debug, Deserialize, Default)]
struct K8sHttpRouteSpec {
    #[serde(default)]
    hostnames: Vec<String>,
    #[serde(default, rename = "rules")]
    rules_k8s: Vec<K8sRule>,
}

/// K8s rule shape (camelCase + nested path object).
#[derive(Debug, Deserialize)]
struct K8sRule {
    #[serde(default)]
    matches: Vec<K8sMatch>,
    #[serde(default, rename = "backendRefs")]
    backend_refs: Vec<K8sBackendRef>,
}

#[derive(Debug, Deserialize)]
struct K8sMatch {
    #[serde(default)]
    path: Option<K8sPath>,
    #[serde(default)]
    method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct K8sPath {
    #[serde(default)]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct K8sBackendRef {
    name: String,
    port: u16,
    #[serde(default = "default_weight")]
    weight: u32,
}

fn default_weight() -> u32 {
    1
}

/// Maps a K8s Gateway list JSON into [`GatewayState`] gateways.
/// TLS cert/key references map to secret-mount convention paths
/// (`/etc/vane/certs/{secret}`) — the operator binary mounts them.
///
/// # Errors
/// JSON parse failure.
pub fn map_gateways(api_json: &str) -> Result<Vec<Gateway>, String> {
    #[derive(Debug, Deserialize)]
    struct K8sListener {
        port: u16,
        #[serde(default)]
        protocol: String,
        #[serde(default)]
        tls: Option<K8sTls>,
    }
    #[derive(Debug, Deserialize)]
    struct K8sTls {
        #[serde(default, rename = "certificateRefs")]
        cert_refs: Vec<K8sCertRef>,
        #[serde(default, rename = "keyRefs")]
        key_refs: Vec<K8sCertRef>,
    }
    #[derive(Debug, Deserialize)]
    struct K8sCertRef {
        name: String,
    }
    #[derive(Debug, Deserialize, Default)]
    struct K8sGatewaySpec {
        #[serde(default, rename = "listeners")]
        listeners_k8s: Vec<K8sListener>,
    }
    #[derive(Debug, Deserialize)]
    struct K8sGatewayObj {
        metadata: K8sMeta,
        #[serde(default)]
        spec: K8sGatewaySpec,
    }

    let list: K8sList<K8sGatewayObj> =
        serde_json::from_str(api_json).map_err(|e| format!("gateways: {e}"))?;
    Ok(list
        .items
        .into_iter()
        .map(|item| Gateway {
            name: format!("{}/{}", item.metadata.namespace, item.metadata.name),
            listeners: item
                .spec
                .listeners_k8s
                .into_iter()
                .map(|l| GatewayListener {
                    port: l.port,
                    protocol: l.protocol,
                    tls: l.tls.map(|t| ListenerTlsHint {
                        cert: t
                            .cert_refs
                            .first()
                            .map(|c| format!("/etc/vane/certs/{}", c.name))
                            .unwrap_or_default(),
                        key: t
                            .key_refs
                            .first()
                            .map(|c| format!("/etc/vane/certs/{}", c.name))
                            .unwrap_or_default(),
                    }),
                })
                .collect(),
        })
        .collect())
}

/// Maps a K8s HTTPRoute list JSON into [`GatewayState`] routes.
/// Backend refs become `{name}.{namespace}.svc:{port}` (kube-dns
/// resolves them; DNS-resolving upstreams are a documented feature).
///
/// # Errors
/// JSON parse failure.
pub fn map_httproutes(api_json: &str, default_namespace: &str) -> Result<Vec<HttpRoute>, String> {
    let list: K8sList<K8sHttpRoute> =
        serde_json::from_str(api_json).map_err(|e| format!("httproutes: {e}"))?;
    Ok(list
        .items
        .into_iter()
        .map(|item| {
            let ns = if item.metadata.namespace.is_empty() {
                default_namespace
            } else {
                &item.metadata.namespace
            };
            HttpRoute {
                name: format!("{}/{}", ns, item.metadata.name),
                hostnames: item.spec.hostnames,
                rules: item
                    .spec
                    .rules_k8s
                    .into_iter()
                    .map(|r| HttpRouteRule {
                        matches: r
                            .matches
                            .into_iter()
                            .map(|m| RouteMatch {
                                path: m.path.and_then(|p| p.value),
                                method: m.method,
                            })
                            .collect(),
                        backend_refs: r
                            .backend_refs
                            .into_iter()
                            .map(|b| BackendRef {
                                host: format!("{}.{}.svc", b.name, ns),
                                port: b.port,
                                weight: b.weight,
                            })
                            .collect(),
                    })
                    .collect(),
            }
        })
        .collect())
}

/// The URL path for HTTPRoute lists, relative to the API server.
pub const HTTPROUTES_PATH: &str = "/apis/gateway.networking.k8s.io/v1/httproutes";
/// The URL path for Gateway lists, relative to the API server.
pub const GATEWAYS_PATH: &str = "/apis/gateway.networking.k8s.io/v1/gateways";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::GatewayState;
    use crate::gateway::compile;

    const HTTPROUTES: &str = r#"{
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRouteList",
        "items": [
            {
                "metadata": { "name": "shop", "namespace": "app" },
                "spec": {
                    "hostnames": ["shop.example.com"],
                    "rules": [
                        {
                            "matches": [
                                { "path": { "type": "PathPrefix", "value": "/api" } }
                            ],
                            "backendRefs": [
                                { "name": "shop-svc", "port": 8080, "weight": 1 }
                            ]
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    fn maps_k8s_json_to_gateway_state() {
        let routes = map_httproutes(HTTPROUTES, "default").expect("map");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].name, "app/shop");
        assert_eq!(routes[0].hostnames, vec!["shop.example.com"]);
        assert_eq!(routes[0].rules[0].backend_refs[0].host, "shop-svc.app.svc");
        assert_eq!(routes[0].rules[0].backend_refs[0].port, 8080);
    }

    #[test]
    fn k8s_state_compiles_to_snapshot() {
        let routes = map_httproutes(HTTPROUTES, "default").expect("map");
        let state = GatewayState {
            gateways: Vec::new(),
            httproutes: routes,
        };
        let snap = compile(&state).expect("compile");
        assert_eq!(snap.routes.len(), 1);
        assert_eq!(snap.routes[0].pattern, "/api/*rest");
        assert_eq!(snap.routes[0].cluster, "c-app/shop-0");
        assert_eq!(
            snap.clusters.get("c-app/shop-0").expect("cluster").backends,
            vec!["shop-svc.app.svc:8080".to_string()]
        );
    }

    const GATEWAYS: &str = r#"{
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "GatewayList",
        "items": [
            {
                "metadata": { "name": "edge", "namespace": "ingress" },
                "spec": {
                    "listeners": [
                        {
                            "port": 8443,
                            "protocol": "HTTPS",
                            "tls": {
                                "certificateRefs": [{ "name": "edge-cert" }],
                                "keyRefs": [{ "name": "edge-key" }]
                            }
                        },
                        { "port": 8080, "protocol": "HTTP" }
                    ]
                }
            },
            {
                "metadata": { "name": "bare", "namespace": "other" },
                "spec": {}
            }
        ]
    }"#;

    /// Gateway-API list JSON maps to gateways with secret-mount
    /// convention TLS paths; listeners without TLS carry no hint and a
    /// gateway without listeners maps empty.
    #[test]
    fn maps_gateway_api_json_with_tls_hints() {
        let gateways = map_gateways(GATEWAYS).expect("map");
        assert_eq!(gateways.len(), 2);

        assert_eq!(gateways[0].name, "ingress/edge");
        assert_eq!(gateways[0].listeners.len(), 2);
        let tls = gateways[0].listeners[0]
            .tls
            .as_ref()
            .expect("tls hint present");
        assert_eq!(tls.cert, "/etc/vane/certs/edge-cert");
        assert_eq!(tls.key, "/etc/vane/certs/edge-key");
        assert!(gateways[0].listeners[1].tls.is_none(), "plain listener");

        assert_eq!(gateways[1].name, "other/bare");
        assert!(gateways[1].listeners.is_empty());
    }

    #[test]
    fn gateway_json_parse_error_is_mapped() {
        let err = map_gateways("{not json").expect_err("parse failure surfaces");
        assert!(err.contains("gateways:"), "{err}");
    }

    /// A backendRef without `weight` defaults to 1 and a namespaced
    /// route without a namespace falls back to the caller's default.
    #[test]
    fn httproutes_default_namespace_and_weight() {
        let json = r#"{
            "kind": "HTTPRouteList",
            "items": [
                {
                    "metadata": { "name": "anon" },
                    "spec": {
                        "hostnames": ["anon.example.com"],
                        "rules": [
                            { "backendRefs": [{ "name": "anon-svc", "port": 80 }] }
                        ]
                    }
                }
            ]
        }"#;
        let routes = map_httproutes(json, "team-a").expect("map");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].name, "team-a/anon");
        let backend = &routes[0].rules[0].backend_refs[0];
        assert_eq!(backend.host, "anon-svc.team-a.svc");
        assert_eq!(backend.port, 80);
        assert_eq!(backend.weight, 1, "weight defaults to 1");
    }
}
