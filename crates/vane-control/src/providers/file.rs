//! File provider: watches a directory of TOML route files (`CP-03`).
//!
//! Every `*.toml` file uses the same schema as the static `routes` +
//! `clusters` sections; the merged set replaces this provider's payload.
//! inotify (via `notify`) drives refreshes; a poll interval covers
//! filesystems where events are unreliable (NFS, some overlays).

use std::path::PathBuf;
use std::sync::Arc;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use vane_router::RouteBuilder;

use crate::config::{ClusterConfig, RouteConfig, VaneConfig};
use crate::health::HealthMap;
use crate::providers::ProviderUpdate;
use crate::routes;

/// File-based discovery.
pub struct FileProvider {
    directory: PathBuf,
    poll: std::time::Duration,
}

impl FileProvider {
    /// New provider for `directory`.
    #[must_use]
    pub fn new(directory: PathBuf, poll_ms: u64) -> Self {
        Self {
            directory,
            poll: std::time::Duration::from_millis(poll_ms.max(1)),
        }
    }

    /// Loads and compiles all `*.toml` files in the directory.
    ///
    /// Files that fail to parse are skipped (logged); a partial bad file
    /// must not take the proxy's routing down.
    ///
    /// # Errors
    /// Directory read failure.
    pub fn scan(&self, health: &HealthMap) -> std::io::Result<Vec<RouteBuilder>> {
        let mut routes = Vec::new();
        let mut clusters: std::collections::BTreeMap<String, ClusterConfig> =
            std::collections::BTreeMap::new();
        let entries = std::fs::read_dir(&self.directory)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "toml") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            match decode_file(&text) {
                Ok((rs, cs)) => {
                    clusters.extend(cs);
                    routes.extend(rs);
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), "file provider skipped bad file: {e}");
                }
            }
        }
        // Compile with the merged cluster table.
        let cfg = VaneConfig {
            clusters,
            routes,
            ..VaneConfig::default()
        };
        Ok(routes::static_routes(&cfg, health))
    }

    /// Runs the watch loop, sending updates into `tx` on every change.
    ///
    /// # Errors
    /// Watcher setup failure.
    pub fn spawn(
        self,
        health: Arc<HealthMap>,
        tx: tokio::sync::mpsc::Sender<ProviderUpdate>,
    ) -> Result<RecommendedWatcher, notify::Error> {
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(notify_tx)?;
        watcher.watch(&self.directory, RecursiveMode::NonRecursive)?;

        std::thread::Builder::new()
            .name("vane-file-provider".into())
            .spawn(move || {
                let mut last = InstantSnapshot::default();
                loop {
                    // Initial scan.
                    if let Ok(routes) = self.scan(&health) {
                        last = last.bump(&routes);
                        if last.changed
                            && tx
                                .blocking_send(ProviderUpdate {
                                    source: "file",
                                    routes,
                                })
                                .is_err()
                        {
                            return; // receiver gone: shutting down
                        }
                    }
                    // Wait for an event or the poll tick, whichever first.
                    match notify_rx.recv_timeout(self.poll) {
                        Ok(_event) => {} // rescan promptly
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                }
            })?;
        Ok(watcher)
    }
}

/// Change detector to avoid redundant publishes.
#[derive(Default)]
struct InstantSnapshot {
    changed: bool,
    fingerprint: u64,
}

impl InstantSnapshot {
    fn bump(&mut self, routes: &[RouteBuilder]) -> Self {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for r in routes {
            for b in &r.backends {
                h = (h ^ u64::from(b.addr.port()).rotate_left(17)).wrapping_mul(0x1000_0000_01b3);
            }
            for seg in r.pattern.split('/') {
                h = (h ^ u64::try_from(seg.len()).unwrap_or(0)).wrapping_mul(0x1000_0000_01b3);
            }
        }
        let changed = h != self.fingerprint;
        self.fingerprint = h;
        Self {
            changed,
            fingerprint: h,
        }
    }
}

fn decode_file(
    text: &str,
) -> Result<
    (
        Vec<RouteConfig>,
        std::collections::BTreeMap<String, ClusterConfig>,
    ),
    String,
> {
    #[derive(serde::Deserialize, Default)]
    struct FileSchema {
        #[serde(default)]
        clusters: std::collections::BTreeMap<String, ClusterConfig>,
        #[serde(default)]
        routes: Vec<RouteConfig>,
    }
    let f: FileSchema = toml::from_str(text).map_err(|e| e.to_string())?;
    Ok((f.routes, f.clusters))
}

// Path import used by the watcher.
#[allow(unused_imports)]
use std::path::Path as _PathAlias;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scans_directory() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::write(
            dir.path().join("routes.toml"),
            r#"
[clusters.web]
backends = ["127.0.0.1:8081"]

[[routes]]
pattern = "/site/*rest"
cluster = "web"
"#,
        )
        .expect("write");
        let provider = FileProvider::new(dir.path().to_path_buf(), 100);
        let routes = provider.scan(&HealthMap::new()).expect("scan");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].pattern, "/site/*rest");
        assert_eq!(routes[0].cluster, "web");
    }

    #[tokio::test]
    async fn skips_bad_files() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::write(dir.path().join("broken.toml"), "not [valid toml").expect("write");
        std::fs::write(
            dir.path().join("ok.toml"),
            "[clusters.a]\nbackends = [\"127.0.0.1:1\"]\n\n[[routes]]\npattern = \"/x\"\ncluster = \"a\"\n",
        )
        .expect("write");
        let provider = FileProvider::new(dir.path().to_path_buf(), 100);
        let routes = provider.scan(&HealthMap::new()).expect("scan");
        assert_eq!(routes.len(), 1);
    }

    #[tokio::test]
    async fn scan_missing_directory_errors() {
        let provider = FileProvider::new(std::path::PathBuf::from("/nonexistent/vane/routes"), 100);
        assert!(provider.scan(&HealthMap::new()).is_err());
    }

    #[tokio::test]
    async fn scan_ignores_non_toml_and_empty() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::write(dir.path().join("notes.txt"), "hello").expect("write");
        std::fs::write(dir.path().join("empty.toml"), "").expect("write");
        let provider = FileProvider::new(dir.path().to_path_buf(), 100);
        let routes = provider.scan(&HealthMap::new()).expect("scan");
        assert!(routes.is_empty());
    }

    #[tokio::test]
    async fn spawn_watches_and_pushes_updates() {
        let dir = tempfile::tempdir().expect("dir");
        let provider = FileProvider::new(dir.path().to_path_buf(), 50);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let _handle = provider
            .spawn(Arc::new(HealthMap::new()), tx)
            .expect("spawn");
        // Initial snapshot arrives without any files.
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("initial update")
            .expect("some");
        assert!(first.routes.is_empty());
        // Adding a file triggers an update.
        std::fs::write(
            dir.path().join("live.toml"),
            "[clusters.w]\nbackends = [\"127.0.0.1:1\"]\n\n[[routes]]\npattern = \"/w\"\ncluster = \"w\"\n",
        )
        .expect("write");
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("second update")
            .expect("some");
        assert_eq!(second.routes.len(), 1);
    }
}
