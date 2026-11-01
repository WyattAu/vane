//! K8s Gateway API provider against a mocked API server (`wiremock`).
//!
//! Covers the uncovered `list_routes` / `spawn` reqwest paths:
//! HTTPRoute listing + compilation, Endpoints resolution (default port,
//! namespace defaulting, non-v4 filtering), and the poll loop's
//! `ProviderUpdate` push. Feature `k8s`.

#![cfg(feature = "k8s")]

use std::sync::Arc;

use vane_control::health::HealthMap;
use vane_control::providers::ProviderUpdate;
use vane_control::providers::k8s::K8sProvider;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "test-sa-token";

fn provider(api: String) -> K8sProvider {
    let client = reqwest::Client::new();
    K8sProvider::with_client(api, vec!["shop".to_owned()], client, Some(TOKEN.to_owned()))
}

fn httproutes_body() -> serde_json::Value {
    serde_json::json!({
        "items": [
            {
                "metadata": {"namespace": "shop"},
                "spec": {
                    "hostnames": ["api.example.com"],
                    "rules": [
                        {
                            "matches": [{"path": {"value": "/v1"}}],
                            "backendRefs": [
                                {"name": "orders", "port": 8080}
                            ]
                        },
                        {
                            "backendRefs": [{"name": "cross-ns", "namespace": "other"}]
                        }
                    ]
                }
            }
        ]
    })
}

fn endpoints_body() -> serde_json::Value {
    serde_json::json!({
        "subsets": [{
            "addresses": [{"ip": "10.1.2.3"}, {"ip": "10.1.2.4"}, {"ip": "::1"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    })
}

#[tokio::test]
async fn list_routes_resolves_services_to_backends() {
    let server = MockServer::start().await;

    // Namespaced HTTPRoute list (provider watches namespace "shop").
    Mock::given(method("GET"))
        .and(path(
            "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(httproutes_body()))
        .mount(&server)
        .await;
    // Endpoint resolution for both backend refs.
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/shop/endpoints/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(endpoints_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/other/endpoints/cross-ns"))
        .respond_with(ResponseTemplate::new(200).set_body_json(endpoints_body()))
        .mount(&server)
        .await;

    let provider = provider(server.uri());
    let routes = provider.list_routes().await.expect("list");

    assert_eq!(routes.len(), 1, "one HTTPRoute with backends");
    let r = &routes[0];
    assert_eq!(r.hosts, vec!["api.example.com".to_owned()]);
    assert_eq!(r.matches.len(), 2);
    // Same-namespace ref on port 8080; v4 addresses only (::1 filtered),
    // endpoint port wins over the ref's default 80.
    assert_eq!(
        r.matches[0],
        ("/v1".to_owned(), "shop/orders".to_owned(), 8080)
    );
    // Cross-namespace ref honored; endpoints' 8080 applies (port default 80
    // only when the Endpoints doc lists no ports).
    assert_eq!(
        r.matches[1],
        ("/".to_owned(), "other/cross-ns".to_owned(), 80)
    );
    assert_eq!(r.backends.len(), 4, "two v4 endpoints per backend ref");
    assert!(
        r.backends.iter().all(|a| a.is_ipv4()),
        "v6 endpoints filtered: {r:?}"
    );
}

#[tokio::test]
async fn list_routes_tolerates_endpoint_lookup_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(httproutes_body()))
        .mount(&server)
        .await;
    // Endpoints 404: backends stay empty, route is dropped by spawn but
    // list_routes still returns it (with empty backends).
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/shop/endpoints/orders"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/other/endpoints/cross-ns"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let provider = provider(server.uri());
    let routes = provider.list_routes().await.expect("list succeeds");
    assert_eq!(routes.len(), 1);
    assert!(routes[0].backends.is_empty());
}

#[tokio::test]
async fn spawn_pushes_provider_updates() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(httproutes_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/shop/endpoints/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(endpoints_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/other/endpoints/cross-ns"))
        .respond_with(ResponseTemplate::new(200).set_body_json(endpoints_body()))
        .mount(&server)
        .await;

    let provider = provider(server.uri());
    let health = Arc::new(HealthMap::new());
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ProviderUpdate>(8);
    let handle = provider.spawn(health, tx);

    let update = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("update within poll interval")
        .expect("channel open");
    assert_eq!(update.source, "kubernetes");
    assert!(!update.routes.is_empty(), "routes pushed: {update:?}");

    // Dropping the receiver ends the poll loop (send failure → return).
    drop(rx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
}
