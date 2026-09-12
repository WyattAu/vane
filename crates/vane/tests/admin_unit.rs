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
