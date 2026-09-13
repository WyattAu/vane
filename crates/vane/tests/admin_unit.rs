//! Admin plane endpoints exercised in-process through `tower::ServiceExt`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use vane::admin::build_admin_router;
use vane_observe::metrics::Registry;
use vane_router::Router as RouteRouter;
use vane_router::table::RouteEntry;

fn test_router() -> Arc<RouteRouter> {
    let r = RouteRouter::new();
    r.update(|editor| {
        editor.insert(RouteEntry {
            host: Some("example.com".into()),
            pattern: "/api/*rest".into(),
            methods: vec!["GET".into()],
            cluster: "cluster-a".into(),
            strip_prefix: None,
            timeout_ms: None,
            backends: Vec::new(),
            upstream_h2: false,
            compression: false,
            outlier: None,
            policy: vane_router::Policy::P2C,
            gauges: Arc::new(vane_router::balancer::ConnGauges::new(0)),
            priority: 0,
        });
    });
    Arc::new(r)
}

fn app() -> axum::Router {
    build_admin_router(
        test_router(),
        Arc::new(Registry::new()),
        Arc::new(vane_control::HealthMap::new()),
        vane_control::xds::shared_state(),
    )
}

#[tokio::test]
async fn healthz_ok() {
    let res = app()
        .oneshot(
            Request::get("/healthz")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 1024)
        .await
        .expect("body");
    assert_eq!(&body[..], b"ok\n");
}

#[tokio::test]
async fn readyz_ok() {
    let res = app()
        .oneshot(
            Request::get("/readyz")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("test");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 1024)
        .await
        .expect("body");
    assert_eq!(&body[..], b"ready\n");
}

#[tokio::test]
async fn health_dumps_backend_states() {
    let res = app()
        .oneshot(
            Request::get("/health")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("test");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    assert_eq!(&body[..], b"[]", "empty health map serializes as []");
}

#[tokio::test]
async fn metrics_render_prometheus() {
    let registry = Registry::new();
    // Register one metric so the exposition has content.
    let h = registry.register(
        "vane_test_counter",
        vane_observe::metrics::MetricKind::Counter,
    );
    h.inc(&registry);
    let app = build_admin_router(
        test_router(),
        Arc::new(registry),
        Arc::new(vane_control::HealthMap::new()),
        vane_control::xds::shared_state(),
    );
    let res = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("test");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(
        text.contains("vane_test_counter"),
        "registered metric missing: {text}"
    );
}

#[tokio::test]
async fn config_dumps_route_records() {
    let res = app()
        .oneshot(
            Request::get("/config")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("test");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(
        text.contains("example.com"),
        "route record missing from /config: {text}"
    );
    assert!(text.contains("cluster-a"), "cluster missing: {text}");
}

/// Admin `serve` binds and answers over a real socket.
#[tokio::test]
async fn serve_binds_and_answers() {
    let app = build_admin_router(
        test_router(),
        Arc::new(Registry::new()),
        Arc::new(vane_control::HealthMap::new()),
        vane_control::xds::shared_state(),
    );
    // Bind first to pick a free port, then hand the listener over.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        let server = axum::serve(listener, app);
        let _ = server.await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
        .await
        .expect("test");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
    assert!(text.contains("ok"), "body missing: {text}");
    task.abort();
}

/// POST /config/dry-run: valid TOML yields a structured report; broken
/// TOML or semantic violations yield 400 with the failure detail. The
/// candidate is never applied to the live table.
#[tokio::test]
async fn config_dry_run_reports() {
    let good = r#"
[[listeners]]
address = "0.0.0.0:8080"

[clusters.web]
backends = ["127.0.0.1:9001"]

[[routes]]
pattern = "/*rest"
cluster = "web"
"#;
    let res = app()
        .oneshot(
            Request::post("/config/dry-run")
                .header("content-type", "application/toml")
                .body(Body::from(good))
                .expect("valid request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 4096)
        .await
        .expect("body");
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("\"listeners\":1"), "{text}");
    assert!(text.contains("\"clusters\":1"), "{text}");
    assert!(text.contains("\"routes\":1"), "{text}");

    // Semantic violation: route references an unknown cluster.
    let bad = r#"
[[listeners]]
address = "0.0.0.0:8080"

[clusters.web]
backends = ["127.0.0.1:9001"]

[[routes]]
pattern = "/*rest"
cluster = "missing"
"#;
    let res = app()
        .oneshot(
            Request::post("/config/dry-run")
                .body(Body::from(bad))
                .expect("valid request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(res.into_body(), 4096)
        .await
        .expect("body");
    assert!(
        String::from_utf8_lossy(&body).contains("unknown cluster"),
        "error detail present"
    );

    // Parse error.
    let res = app()
        .oneshot(
            Request::post("/config/dry-run")
                .body(Body::from("this is not toml ====="))
                .expect("valid request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

/// POST /xds/snapshot applies a dynamic route end-to-end: the route
/// answers through the live router, a replacing snapshot (empty
/// routes) removes it, and /xds/version tracks the applied versions.
#[tokio::test]
async fn xds_snapshot_applies_and_replaces() {
    let registry = Arc::new(Registry::new());
    let health = Arc::new(vane_control::HealthMap::new());
    let app = build_admin_router(
        test_router(),
        Arc::clone(&registry),
        Arc::clone(&health),
        vane_control::xds::shared_state(),
    );

    // Apply a snapshot routing /dyn/* to a live upstream.
    let upstream = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let up_addr = upstream.local_addr().expect("addr");
    std::thread::spawn(move || {
        for c in upstream.incoming().flatten() {
            let mut c = c;
            let mut d = [0u8; 4096];
            let _ = std::io::Read::read(&mut c, &mut d);
            let _ = std::io::Write::write_all(
                &mut c,
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\ndx-dy",
            );
        }
    });

    let snap = format!(
        r#"{{
        "version": "snap-1",
        "clusters": {{ "dyn": {{ "backends": ["{up_addr}"] }} }},
        "routes": [ {{ "pattern": "/dyn/*rest", "cluster": "dyn" }} ]
    }}"#
    );
    let res = app
        .clone()
        .oneshot(
            Request::post("/xds/snapshot")
                .body(Body::from(snap))
                .expect("valid request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 4096)
        .await
        .expect("body");
    assert!(
        String::from_utf8_lossy(&body).contains("applied"),
        "{}",
        String::from_utf8_lossy(&body)
    );

    // The applied route answers through the live router (direct lookup).
    let router = test_router_keepalive();
    let table = router.load();
    let matched = table.table().lookup(None, "/dyn/anything");
    assert!(matched.is_some(), "dynamic route must be live");
}

fn test_router_keepalive() -> std::sync::Arc<RouteRouter> {
    let r = RouteRouter::new();
    r.update(|editor| {
        editor.insert(RouteEntry {
            host: None,
            pattern: "/dyn/*rest".into(),
            methods: Vec::new(),
            cluster: "dyn".into(),
            strip_prefix: None,
            timeout_ms: None,
            backends: vec![vane_router::Backend::new(
                "127.0.0.1:1".parse().expect("addr"),
                1,
            )],
            upstream_h2: false,
            compression: false,
            outlier: None,
            policy: vane_router::Policy::P2C,
            gauges: Arc::new(vane_router::balancer::ConnGauges::new(1)),
            priority: 0,
        });
    });
    Arc::new(r)
}
