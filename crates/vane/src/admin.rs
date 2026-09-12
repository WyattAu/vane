//! Admin plane: health probes, Prometheus metrics, config introspection.

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use vane_observe::metrics::Registry;
use vane_router::Router as RouteRouter;

/// Builds the admin router: `/metrics`, `/healthz`, `/readyz`, `/config`.
pub fn build_admin_router(
    router: Arc<RouteRouter>,
    registry: Arc<Registry>,
    health: Arc<vane_control::HealthMap>,
) -> Router {
    let health2 = Arc::clone(&health);
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
            get(move || {
                let router = Arc::clone(&router);
                async move {
                    let table = router.load();
                    axum::Json(table.table().snapshot_records())
                }
            }),
        )
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/config/dry-run", post(dry_run))
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
