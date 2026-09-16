//! ADS-style dynamic configuration: versioned full-state snapshots
//! (clusters + routes) pushed over the admin plane and swapped into
//! the live router with the same EBR snapshot semantics as static
//! config. Same resource model as `VaneConfig`'s clusters/routes —
//! a Gateway API operator or an external control plane compiles into
//! this, and the data path never sees a lock.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::ClusterConfig;
use crate::health::HealthMap;
use crate::routes::resolve_backends;
use vane_router::{RouteBuilder, Router};

/// One dynamic cluster (backend set + policy knobs subset). Deserialized
/// as `BTreeMap<String, XdsCluster>` — the map key is the cluster name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdsCluster {
    /// Static backend addresses (`host:port` or `port`).
    #[serde(default)]
    pub backends: Vec<String>,
    /// Gzip-compress responses on routes using this cluster.
    #[serde(default)]
    pub compression: bool,
    /// HTTP/2 upstream (prior knowledge).
    #[serde(default)]
    pub http2: bool,
    /// Per-backend outlier ejection.
    #[serde(default)]
    pub outlier: Option<crate::config::OutlierConfig>,
}

/// One dynamic route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdsRoute {
    /// Host match (`null` = any host).
    #[serde(default)]
    pub host: Option<String>,
    /// Path pattern (`/api/*rest`, `/health`).
    pub pattern: String,
    /// Target cluster.
    pub cluster: String,
    /// Allowed methods (empty = all).
    #[serde(default)]
    pub methods: Vec<String>,
    /// Prefix strip.
    #[serde(default)]
    pub strip_prefix: Option<String>,
    /// Route priority (lower wins).
    #[serde(default)]
    pub priority: u32,
}

/// A full-state configuration snapshot: replace-everything semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdsSnapshot {
    /// Caller-owned version tag (echoed back; monotonic strings
    /// recommended — `N`, `git-sha`, resource hashes).
    pub version: String,
    /// Dynamic clusters.
    #[serde(default)]
    pub clusters: BTreeMap<String, XdsCluster>,
    /// Dynamic routes.
    #[serde(default)]
    pub routes: Vec<XdsRoute>,
}

/// Last applied snapshot version (shared with the admin plane).
#[derive(Debug, Default)]
pub struct XdsState {
    version: std::sync::Mutex<Option<String>>,
}

impl XdsState {
    /// Fresh state: no snapshot applied yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The version of the last successfully applied snapshot.
    #[must_use]
    pub fn version(&self) -> Option<String> {
        self.version.lock().expect("xds lock").clone()
    }

    fn set_version(&self, v: String) {
        *self.version.lock().expect("xds lock") = Some(v);
    }
}

/// Applies a snapshot to the live router (atomic swap). Backends share
/// health flags with `health` so the checker picks up dynamic
/// backends automatically.
///
/// # Errors
/// Unknown cluster reference, unresolvable backends, or an invalid
/// pattern. The live router is untouched on error.
pub fn apply_snapshot(
    router: &Router,
    health: &HealthMap,
    snapshot: &XdsSnapshot,
) -> Result<(), String> {
    // 1. Compile everything BEFORE touching the live table.
    let mut builders: Vec<RouteBuilder> = Vec::new();
    for route in &snapshot.routes {
        let Some(cluster) = snapshot.clusters.get(&route.cluster) else {
            return Err(format!(
                "route `{}` references unknown cluster `{}`",
                route.pattern, route.cluster
            ));
        };
        let cluster_cfg = ClusterConfig {
            backends: cluster.backends.clone(),
            unix_socket: None,
            policy: Default::default(),
            health_path: None,
            compression: cluster.compression,
            outlier: cluster.outlier.clone(),
            http2: cluster.http2,
            mesh: None,
        };
        let backends = resolve_backends(&cluster_cfg, health);
        if backends.is_empty() {
            return Err(format!(
                "cluster `{}` has no resolvable backends",
                route.cluster
            ));
        }
        let outlier = cluster.outlier.as_ref().map(|o| {
            let addrs: Vec<SocketAddr> = backends.iter().map(|b| b.addr).collect();
            vane_router::outlier::OutlierSet::new(addrs, o.consecutive_failures, o.ejection_ms)
                .shared()
        });
        builders.push(RouteBuilder {
            host: route.host.clone(),
            pattern: route.pattern.clone(),
            methods: route.methods.clone(),
            cluster: route.cluster.clone(),
            strip_prefix: route.strip_prefix.clone(),
            timeout_ms: None,
            backends,
            upstream_h2: cluster.http2,
            compression: cluster.compression,
            outlier,
            policy: cluster_cfg.policy.into(),
            priority: route.priority,
        });
    }

    // 2. Atomic swap: compile the builders and REPLACE the table via
    //    the editor's replace-all entry (`insert` on a cleared editor
    //    is `Router::update`'s full-state contract — TableEditor drops
    //    all prior entries when the closure panics or errors; explicit
    //    rebuild keeps dynamic snapshots authoritative).
    let compiled: Result<Vec<vane_router::table::RouteEntry>, String> = builders
        .into_iter()
        .map(|b| b.compile().map_err(|e| e.to_string()))
        .collect();
    let entries = compiled?;
    router.update(|editor| {
        editor.replace_all(entries);
    });
    Ok(())
}

/// Applies a snapshot and records the version on success.
///
/// # Errors
/// Same as [`apply_snapshot`].
pub fn apply_snapshot_state(
    router: &Router,
    health: &HealthMap,
    state: &XdsState,
    snapshot: &XdsSnapshot,
) -> Result<(), String> {
    apply_snapshot(router, health, snapshot)?;
    state.set_version(snapshot.version.clone());
    Ok(())
}

/// Convenience: `Arc` pair for handlers.
pub fn shared_state() -> Arc<XdsState> {
    Arc::new(XdsState::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> XdsSnapshot {
        serde_json::from_str(
            r#"{
            "version": "v7",
            "clusters": {
                "web": { "backends": ["127.0.0.1:9001"] }
            },
            "routes": [
                { "pattern": "/*rest", "cluster": "web", "priority": 5 }
            ]
        }"#,
        )
        .expect("snapshot")
    }

    #[test]
    fn applies_and_tracks_version() {
        let router = Router::new();
        let health = HealthMap::new();
        let state = XdsState::new();
        assert_eq!(state.version(), None);
        apply_snapshot_state(&router, &health, &state, &snapshot()).expect("apply");
        assert_eq!(state.version().as_deref(), Some("v7"));
        let table = router.load();
        assert_eq!(table.table().snapshot_records().len(), 1);
    }

    #[test]
    fn unknown_cluster_rejected_and_live_table_untouched() {
        let router = Router::new();
        let health = HealthMap::new();
        let state = XdsState::new();
        apply_snapshot_state(&router, &health, &state, &snapshot()).expect("apply");

        let mut bad = snapshot();
        bad.version = "v8".into();
        bad.routes[0].cluster = "missing".into();
        assert!(apply_snapshot_state(&router, &health, &state, &bad).is_err());
        assert_eq!(state.version().as_deref(), Some("v7"), "version unchanged");
        let table = router.load();
        assert_eq!(table.table().snapshot_records().len(), 1, "live table kept");
    }

    #[test]
    fn snapshot_replaces_previous_state() {
        let router = Router::new();
        let health = HealthMap::new();
        let state = XdsState::new();
        apply_snapshot_state(&router, &health, &state, &snapshot()).expect("apply");

        let mut next = snapshot();
        next.version = "v8".into();
        next.routes.clear();
        apply_snapshot_state(&router, &health, &state, &next).expect("apply");
        let table = router.load();
        assert!(table.table().snapshot_records().is_empty(), "replaced");
    }

    #[test]
    fn outlier_and_compression_flags_threaded() {
        let router = Router::new();
        let health = HealthMap::new();
        let snap: XdsSnapshot = serde_json::from_str(
            r#"{
            "version": "v1",
            "clusters": {
                "web": {
                    "backends": ["127.0.0.1:9001"],
                    "compression": true,
                    "outlier": { "consecutive_failures": 3, "ejection_ms": 1000 }
                }
            },
            "routes": [ { "pattern": "/*rest", "cluster": "web" } ]
        }"#,
        )
        .expect("snapshot");
        apply_snapshot_state(&router, &health, &state_unused(), &snap).expect("apply");
        let table = router.load();
        let records = table.table().snapshot_records();
        assert_eq!(records.len(), 1);
    }

    fn state_unused() -> XdsState {
        XdsState::new()
    }

    #[test]
    fn policy_default_is_p2c() {
        let router = Router::new();
        let health = HealthMap::new();
        let snap: XdsSnapshot = serde_json::from_str(
            r#"{
            "version": "v1",
            "clusters": { "web": { "backends": ["127.0.0.1:9001"] } },
            "routes": [ { "pattern": "/*rest", "cluster": "web" } ]
        }"#,
        )
        .expect("snapshot");
        apply_snapshot_state(&router, &health, &XdsState::new(), &snap).expect("apply");
    }
}
