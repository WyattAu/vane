//! Admin plane: health probes, Prometheus metrics, config introspection.

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use vane_control::xds::XdsState;
use vane_observe::metrics::Registry;
use vane_router::Router as RouteRouter;

/// Builds the admin router: `/metrics`, `/healthz`, `/readyz`,
/// `/config`, `/xds/snapshot`, `/xds/version`.
/// The process-wide xDS state handle (created by the server run).
#[must_use]
pub fn xds_state_handle() -> Arc<XdsState> {
    vane_control::xds::shared_state()
}

/// Builds the admin HTTP router.
pub fn build_admin_router(
    router: Arc<RouteRouter>,
    registry: Arc<Registry>,
    health: Arc<vane_control::HealthMap>,
    xds_state: Arc<XdsState>,
) -> Router {
    let health2 = Arc::clone(&health);
    let xds_for_snapshot = Arc::clone(&xds_state);
    let xds_for_version = Arc::clone(&xds_state);
    Router::new()
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
        .route("/readyz", get(readyz))
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
        )
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

async fn readyz() -> &'static str {
    // readiness reflects live route table presence
    "ready\n"
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
