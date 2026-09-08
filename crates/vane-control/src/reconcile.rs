//! Reconciler — merges provider updates into router generations.
//!
//! Source precedence (later overrides on pattern conflicts):
//! `static config` < `file` < `docker` < `kubernetes`.
//! A full rebuild compiles through [`vane_router::Router::update`] —
//! structural sharing keeps generations cheap; publication is one
//! Release store that workers observe lock-free (`CP-02`).

use std::sync::Arc;
use std::time::Instant;

use vane_observe::metrics::{MetricHandle, MetricKind, Registry};
use vane_router::Router;

use crate::routes;

/// Merge/reconcile driver.
pub struct Reconciler {
    router: Arc<Router>,
    health: Arc<crate::health::HealthMap>,
    updates: tokio::sync::mpsc::Receiver<crate::providers::ProviderUpdate>,
    registry: Arc<Registry>,
    generation: MetricHandle,
    update_latency: MetricHandle,
    /// Latest update per source.
    latest: std::collections::HashMap<&'static str, crate::providers::ProviderUpdate>,
}

impl Reconciler {
    /// New reconciler over `router`.
    ///
    /// # Panics
    /// Metric registration failure (startup-only).
    #[must_use]
    pub fn new(
        router: Arc<Router>,
        health: Arc<crate::health::HealthMap>,
        registry: Arc<Registry>,
        updates: tokio::sync::mpsc::Receiver<crate::providers::ProviderUpdate>,
    ) -> Self {
        let generation = registry.register("vane_config_generation", MetricKind::Gauge);
        let update_latency = registry.register_histogram("vane_config_update_duration_us");
        Self {
            router,
            health,
            updates,
            registry,
            generation,
            update_latency,
            latest: std::collections::HashMap::new(),
        }
    }

    /// Publishes a fresh generation from the static config alone.
    pub fn publish_static(&mut self, cfg: &crate::config::VaneConfig) {
        let routes = routes::static_routes(cfg, &self.health);
        self.apply("static", routes);
    }

    fn apply(&mut self, source: &'static str, routes: Vec<vane_router::RouteBuilder>) {
        let start = Instant::now();
        // Compile in precedence order.
        let precedence = ["static", "file", "docker", "kubernetes"];
        let mut merged: Vec<vane_router::RouteBuilder> = Vec::new();
        for src in precedence {
            if let Some(u) = self.latest.get(src) {
                merged.extend(u.routes.iter().cloned());
            }
            if src == source {
                // Store current below.
            }
        }
        self.latest.insert(
            source,
            crate::providers::ProviderUpdate {
                source,
                routes: routes.clone(),
            },
        );
        // Re-merge including the just-stored source.
        merged.clear();
        for src in precedence {
            if let Some(u) = self.latest.get(src) {
                merged.extend(u.routes.iter().cloned());
            }
        }

        let router = Arc::clone(&self.router);
        router.update(|editor| {
            for rb in merged {
                match rb.compile() {
                    Ok(entry) => editor.insert(entry),
                    Err(e) => {
                        tracing::warn!("route compile failed: {e}");
                    }
                }
            }
        });
        self.generation.set(&self.registry, self.generation_raw());
        let elapsed = start.elapsed().as_micros() as u64;
        self.update_latency.observe_us(&self.registry, elapsed);
        tracing::debug!(
            source,
            routes = self.route_count(),
            "config applied in {elapsed}us"
        );
    }

    fn generation_raw(&self) -> u64 {
        self.generation_generation()
    }

    fn generation_generation(&self) -> u64 {
        // Monotonic local counter surfaced through the gauge.
        static GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn route_count(&self) -> usize {
        self.router.load().table().len()
    }

    /// Runs the merge loop until the channel closes.
    pub async fn run(mut self) {
        while let Some(update) = self.updates.recv().await {
            self.apply(update.source, update.routes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publishes_static_routes() {
        let router = Arc::new(Router::new());
        let registry = Arc::new(Registry::new());
        let health = Arc::new(crate::health::HealthMap::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(8);
        let mut rec = Reconciler::new(Arc::clone(&router), Arc::clone(&health), registry, rx);

        let cfg: crate::config::VaneConfig = toml::from_str(
            r#"
[clusters.api]
backends = ["127.0.0.1:9001"]

[[routes]]
pattern = "/api/*rest"
cluster = "api"
"#,
        )
        .expect("toml");
        rec.publish_static(&cfg);

        let table = router.load();
        let m = table
            .table()
            .lookup(Some("x.example"), "/api/users")
            .expect("match");
        assert_eq!(m.terminal.value.cluster, "api");
    }
}
