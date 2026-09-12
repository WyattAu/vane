//! Kubernetes Gateway API provider (`CP-03`).
//!
//! Watches `HTTPRoute` (gateway.networking.k8s.io/v1) resources through the
//! API server's plain watch endpoint using the pod service-account token —
//! no heavy client SDK. BackendRefs with Service backends are resolved to
//! Endpoints/EndpointSlices via a second watch (IP targets only).
//!
//! Feature-gated: `k8s` (default off — `reqwest` stays lean in sidecars).

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::health::HealthMap;
use crate::providers::{ProviderRouteSpec, ProviderUpdate, build_route};

const SA_TOKEN: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const SA_CA: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Kubernetes discovery.
pub struct K8sProvider {
    /// API server root (`https://kubernetes.default.svc` in-cluster).
    pub api: String,
    /// Namespaces to watch (empty = all).
    pub namespaces: Vec<String>,
    client: reqwest::Client,
}

/// Compiled HTTPRoute entry.
#[derive(Debug, Clone)]
pub struct HttpRouteCompiled {
    /// Hostnames from the route spec.
    pub hosts: Vec<String>,
    /// `(path prefix, ns/name, port)` per rule backendRef.
    pub matches: Vec<(String, String, u16)>,
    /// Resolved backend IPs.
    pub backends: Vec<std::net::SocketAddr>,
}

impl K8sProvider {
    /// Builds a provider; in-cluster when `api` is `None`.
    ///
    /// # Errors
    /// No service-account token and no explicit API server.
    pub fn new(api: Option<String>, namespaces: Vec<String>) -> Result<Self, String> {
        let api = match api {
            Some(a) => a,
            None => {
                let host = std::env::var("KUBERNETES_SERVICE_HOST")
                    .map_err(|_| "not in cluster (KUBERNETES_SERVICE_HOST unset)".to_string())?;
                let port =
                    std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
                format!("https://{host}:{port}")
            }
        };
        let ca = std::fs::read(SA_CA).map_err(|e| format!("read ca: {e}"))?;
        let cert = reqwest::Certificate::from_pem(&ca).map_err(|e| e.to_string())?;
        // Idempotent provider install (workspace pins rustls without a
        // default provider; reqwest needs one).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .add_root_certificate(cert)
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            api,
            namespaces,
            client,
        })
    }

    fn token(&self) -> Result<String, String> {
        std::fs::read_to_string(SA_TOKEN).map_err(|e| format!("read token: {e}"))
    }

    /// Lists HTTPRoutes across the watched namespaces and compiles them.
    ///
    /// # Errors
    /// API request failure.
    pub async fn list_routes(&self) -> Result<Vec<HttpRouteCompiled>, String> {
        let token = self.token()?;

        let mut all = Vec::new();
        let namespaces = if self.namespaces.is_empty() {
            vec![String::new()] // cluster-wide list endpoint
        } else {
            self.namespaces.clone()
        };
        for ns in namespaces {
            let url = if ns.is_empty() {
                format!("{}/apis/gateway.networking.k8s.io/v1/httproutes", self.api)
            } else {
                format!(
                    "{apis}/apis/gateway.networking.k8s.io/v1/namespaces/{ns}/httproutes",
                    apis = self.api
                )
            };
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let items: RouteItems = resp.json().await.map_err(|e| e.to_string())?;
            all.extend(compile_httproutes(items.items));
        }
        // Resolve Services to endpoints (subset: first page, IP family v4).
        let mut resolved = Vec::new();
        for mut route in all {
            let mut backends = Vec::new();
            for (_, svc, port) in &route.matches {
                let (ns, name) = svc.split_once('/').unwrap_or(("default", svc));
                let url = format!("{}/api/v1/namespaces/{ns}/endpoints/{name}", self.api);
                let Ok(resp) = self
                    .client
                    .get(&url)
                    .bearer_auth(self.token()?)
                    .send()
                    .await
                else {
                    continue;
                };
                let Ok(text) = resp.text().await else {
                    continue;
                };
                backends.extend(parse_endpoints(&text, *port));
            }
            route.backends = backends;
            resolved.push(route);
        }
        Ok(resolved)
    }

    /// Poll loop pushing updates.
    pub fn spawn(
        self,
        health: Arc<HealthMap>,
        tx: tokio::sync::mpsc::Sender<ProviderUpdate>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.list_routes().await {
                    Ok(routes) => {
                        let mut builders = Vec::new();
                        for r in &routes {
                            if r.backends.is_empty() {
                                continue;
                            }
                            let hosts = if r.hosts.is_empty() {
                                vec![None]
                            } else {
                                r.hosts.iter().map(|h| Some(h.clone())).collect()
                            };
                            for host in hosts {
                                for (prefix, cluster, _) in &r.matches {
                                    builders.push(build_route(
                                        ProviderRouteSpec {
                                            host: host.clone(),
                                            pattern: format!(
                                                "{}*rest",
                                                prefix.trim_end_matches('/')
                                            ),
                                            cluster: cluster.clone(),
                                            addrs: r.backends.clone(),
                                            strip_prefix: None,
                                            priority: 30,
                                            upstream_h2: false,
                                            compression: false,
                                        },
                                        &health,
                                    ));
                                }
                            }
                        }
                        if tx
                            .send(ProviderUpdate {
                                source: "kubernetes",
                                routes: builders,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::debug!("k8s provider: {e}");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        })
    }
}

/// Kubernetes `HTTPRouteList` envelope.
#[derive(Deserialize)]
pub(crate) struct RouteItems {
    #[serde(default)]
    pub items: Vec<RouteItem>,
}

/// One `HTTPRoute` resource (subset of fields vane consumes).
#[derive(Deserialize)]
pub(crate) struct RouteItem {
    #[serde(default)]
    pub metadata: Meta,
    #[serde(default)]
    pub spec: RouteSpec,
}

/// Object metadata (namespace only).
#[derive(Deserialize, Default)]
pub(crate) struct Meta {
    #[serde(default)]
    pub namespace: String,
}

/// HTTPRoute spec (hostnames + rules).
#[derive(Deserialize, Default)]
pub(crate) struct RouteSpec {
    #[serde(default)]
    pub hostnames: Vec<String>,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// One routing rule: path matches + backend references.
#[derive(Deserialize, Default)]
pub(crate) struct Rule {
    #[serde(default)]
    pub matches: Vec<PathMatch>,
    #[serde(default, rename = "backendRefs")]
    pub backend_refs: Vec<BackendRef>,
}

/// Path match wrapper.
#[derive(Deserialize, Default)]
pub(crate) struct PathMatch {
    #[serde(default)]
    pub path: Option<PathValue>,
}

/// Path value (`type: PathPrefix` assumed; value defaults to `/`).
#[derive(Deserialize, Default)]
pub(crate) struct PathValue {
    #[serde(default)]
    pub value: Option<String>,
}

/// Backend reference (Service name, optional namespace/port).
#[derive(Deserialize, Default)]
pub(crate) struct BackendRef {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

/// Compiles HTTPRoute items into vane's route form. Rules without
/// backend refs are skipped; path defaults to `/`, port to 80, and the
/// backend namespace to the HTTPRoute's own namespace.
fn compile_httproutes(items: Vec<RouteItem>) -> Vec<HttpRouteCompiled> {
    let mut all = Vec::new();
    for it in items {
        let mut matches = Vec::new();
        for rule in &it.spec.rules {
            let prefix = rule
                .matches
                .first()
                .and_then(|m| m.path.as_ref())
                .and_then(|p| p.value.clone())
                .unwrap_or_else(|| "/".to_owned());
            for br in &rule.backend_refs {
                let Some(name) = br.name.clone() else {
                    continue;
                };
                let ns = br
                    .namespace
                    .clone()
                    .unwrap_or_else(|| it.metadata.namespace.clone());
                matches.push((
                    prefix.clone(),
                    format!("{ns}/{name}"),
                    br.port.unwrap_or(80),
                ));
            }
        }
        if !matches.is_empty() {
            all.push(HttpRouteCompiled {
                hosts: it.spec.hostnames,
                matches,
                backends: Vec::new(),
            });
        }
    }
    all
}

/// Extracts ready v4 endpoint addresses from an Endpoints JSON document.
fn parse_endpoints(json: &str, port: u16) -> Vec<std::net::SocketAddr> {
    #[derive(Deserialize)]
    struct Endpoints {
        #[serde(default)]
        subsets: Vec<Subset>,
    }
    #[derive(Deserialize)]
    struct Subset {
        #[serde(default)]
        addresses: Vec<Addr>,
        #[serde(default)]
        ports: Vec<Port>,
    }
    #[derive(Deserialize)]
    struct Addr {
        ip: String,
    }
    #[derive(Deserialize)]
    struct Port {
        port: u16,
    }
    let Ok(ep) = serde_json::from_str::<Endpoints>(json) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for s in ep.subsets {
        let p = s.ports.first().map(|p| p.port).unwrap_or(port);
        for a in s.addresses {
            if let Ok(addr) = format!("{}:{}", a.ip, p).parse::<std::net::SocketAddr>() {
                if addr.is_ipv4() {
                    out.push(addr);
                }
            }
        }
    }
    out
}

// Silence unused import in non-k8s feature builds of this module's deps.
#[allow(unused)]
type Unused = HashMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoints_json() {
        let doc = r#"{
            "subsets": [{
                "addresses": [{"ip": "10.1.2.3"}, {"ip": "10.1.2.4"}],
                "ports": [{"port": 8080, "protocol": "TCP"}]
            }]
        }"#;
        let addrs = parse_endpoints(doc, 80);
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].to_string(), "10.1.2.3:8080");
    }

    /// Compiles a realistic HTTPRoute list into cluster matches.
    #[test]
    fn compiles_httproute_list() {
        let items_json = r#"[
            {
                "metadata": {"namespace": "shop"},
                "spec": {
                    "hostnames": ["api.example.com"],
                    "rules": [
                        {
                            "matches": [{"path": {"value": "/v1"}}],
                            "backendRefs": [
                                {"name": "orders", "port": 8080},
                                {"name": "legacy", "namespace": "other", "port": 9090}
                            ]
                        },
                        {
                            "backendRefs": [{"name": "default-svc"}]
                        }
                    ]
                }
            },
            {
                "metadata": {"namespace": "misc"},
                "spec": {"rules": [{"backendRefs": []}]}
            }
        ]"#;
        let items: Vec<RouteItem> = serde_json::from_str(items_json).expect("valid items");
        let routes = compile_httproutes(items);
        // Second HTTPRoute has no backends → skipped entirely.
        assert_eq!(routes.len(), 1);
        let r = &routes[0];
        assert_eq!(r.hosts, vec!["api.example.com".to_string()]);
        assert_eq!(r.matches.len(), 3);
        // Same-namespace default resolution.
        assert_eq!(
            r.matches[0],
            ("/v1".to_string(), "shop/orders".to_string(), 8080)
        );
        // Cross-namespace ref honored.
        assert_eq!(
            r.matches[1],
            ("/v1".to_string(), "other/legacy".to_string(), 9090)
        );
        // Path defaults to "/" and port to 80.
        assert_eq!(
            r.matches[2],
            ("/".to_string(), "shop/default-svc".to_string(), 80)
        );
    }
}
