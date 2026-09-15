//! Admin plane: health probes, Prometheus metrics, config introspection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use vane_control::xds::XdsState;
use vane_observe::metrics::Registry;
use vane_router::Router as RouteRouter;

/// Process-wide xDS state handle (created by the server run).
#[must_use]
pub fn xds_state_handle() -> Arc<XdsState> {
    vane_control::xds::shared_state()
}

/// Builds the admin HTTP router: `/metrics`, `/healthz`, `/readyz`,
/// `/config`, `/xds/snapshot`, `/xds/version`.
///
/// `listeners_bound` flips once the data-plane listeners are bound; it
/// feeds [`readyz`] readiness. When `auth_token` is set, every route is
/// gated behind `Authorization: Bearer <token>` (constant-time compare;
/// anything else is `401`). The token is process-lifetime, so it is
/// passed as an `Arc<str>` to avoid per-request clones.
pub fn build_admin_router(
    router: Arc<RouteRouter>,
    registry: Arc<Registry>,
    health: Arc<vane_control::HealthMap>,
    xds_state: Arc<XdsState>,
    listeners_bound: Arc<AtomicBool>,
    auth_token: Option<Arc<str>>,
) -> Router {
    let health2 = Arc::clone(&health);
    let xds_for_snapshot = Arc::clone(&xds_state);
    let xds_for_version = Arc::clone(&xds_state);
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let health = Arc::clone(&health2);
                async move {
                    axum::Json(
                        health
                            .snapshot()
                            .iter()
                            .map(|(a, h)| (a.to_string(), *h))
                            .collect::<Vec<_>>(),
                    )
                }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let registry = Arc::clone(&registry);
                async move {
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        registry.render_prometheus(),
                    )
                }
            }),
        )
        .route(
            "/config",
            get({
                let router = Arc::clone(&router);
                move || {
                    let table = router.load();
                    let records = table.table().snapshot_records();
                    async move { axum::Json(records) }
                }
            }),
        )
        .route("/healthz", get(healthz))
        .route(
            "/readyz",
            get({
                let router = Arc::clone(&router);
                let listeners_bound = Arc::clone(&listeners_bound);
                move || {
                    let router = Arc::clone(&router);
                    let listeners_bound = Arc::clone(&listeners_bound);
                    async move { readyz(router, listeners_bound).await }
                }
            }),
        )
        .route("/config/dry-run", post(dry_run))
        .route(
            "/xds/snapshot",
            post(move |body: String| {
                let router = Arc::clone(&router);
                let health = Arc::clone(&health);
                let xds = Arc::clone(&xds_for_snapshot);
                async move { xds_snapshot(router, health, xds, body).await }
            }),
        )
        .route(
            "/xds/version",
            get(move || {
                let xds = Arc::clone(&xds_for_version);
                async move { axum::Json(xds.version()) }
            }),
        );
    match auth_token {
        Some(token) => app.layer(axum::middleware::from_fn_with_state(token, require_bearer)),
        None => app,
    }
}

/// Bearer-token gate: when an admin token is configured, requests without
/// `Authorization: Bearer <token>` (constant-time compared) get `401`.
/// Applies to the entire admin plane, probes included — operators serving
/// unauthenticated probes to load balancers should keep the token unset
/// and rely on the loopback-only default bind.
async fn require_bearer(
    axum::extract::State(token): axum::extract::State<Arc<str>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let supplied = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let ok = supplied.is_some_and(|s| constant_time_eq(s.as_bytes(), token.as_bytes()));
    if ok {
        next.run(req).await
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response()
    }
}

/// Constant-time byte equality (no early exit on data mismatch). Length
/// equality is checked first: token length is not treated as secret
/// material, and the fold itself is branch-free on data.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// `POST /config/dry-run`: TOML body in, JSON validation report out.
/// The candidate config is parsed and semantically validated but never
/// applied — the control-plane gate for GitOps pipelines and operator
/// rollouts. `400` with the failure detail on invalid configs.
async fn dry_run(
    body: String,
) -> Result<axum::Json<vane_control::config::DryRunReport>, (axum::http::StatusCode, String)> {
    vane_control::config::dry_run_toml(&body)
        .map(axum::Json)
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))
}

/// `POST /xds/snapshot`: JSON full-state snapshot in; atomic router
/// swap. `400` with the failure detail; the live table is untouched on
/// error.
async fn xds_snapshot(
    router: Arc<RouteRouter>,
    health: Arc<vane_control::HealthMap>,
    xds: Arc<vane_control::xds::XdsState>,
    body: String,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let snapshot: vane_control::xds::XdsSnapshot = serde_json::from_str(&body).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("snapshot parse: {e}"),
        )
    })?;
    vane_control::xds::apply_snapshot_state(&router, &health, &xds, &snapshot).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("snapshot rejected: {e}"),
        )
    })?;
    Ok(axum::Json(serde_json::json!({
        "status": "applied",
        "version": snapshot.version,
        "clusters": snapshot.clusters.len(),
        "routes": snapshot.routes.len(),
    })))
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// Readiness: live route table non-empty **and** at least one data-plane
/// listener bound. `503` with a JSON body naming the missing halves while
/// either is false (liveness stays `/healthz`, always `200`).
async fn readyz(router: Arc<RouteRouter>, listeners_bound: Arc<AtomicBool>) -> Response {
    let routes = router.load().table().len();
    let bound = listeners_bound.load(Ordering::Acquire);
    if routes > 0 && bound {
        (axum::http::StatusCode::OK, "ready\n").into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "not-ready",
                "routes": routes,
                "listeners_bound": bound,
            })),
        )
            .into_response()
    }
}

/// Serves the admin plane (axum-stack utilities).
///
/// # Errors
/// Bind/serve failure.
pub async fn serve(addr: std::net::SocketAddr, router: Router) -> Result<(), String> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| e.to_string())?;
    axum::serve(listener, router)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_matches_std_eq() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret "));
        assert!(!constant_time_eq(b"secret", b"sec"));
        assert!(constant_time_eq(b"", b""));
    }
}
