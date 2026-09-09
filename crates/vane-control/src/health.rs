//! Active health checks — shared health flags survive config generations.
//!
//! `HealthMap` owns one `Arc<AtomicU64>` per backend address; when routes
//! are (re)compiled, backends attach to the same cell, so a probe flipping
//! health affects the live router immediately without a config update.
//! The [`HealthChecker`] probes HTTP (via reqwest) or TCP on an interval
//! and flips those flags.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Shared per-address health flags.
#[derive(Default)]
pub struct HealthMap {
    entries: Mutex<HashMap<SocketAddr, Arc<AtomicU64>>>,
}

impl HealthMap {
    /// New empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets (or creates) the shared flag for `addr`.
    pub fn flag(&self, addr: SocketAddr) -> Arc<AtomicU64> {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(addr)
            .or_insert_with(|| Arc::new(AtomicU64::new(1)))
            .clone()
    }

    /// Current health of `addr` (default: healthy).
    #[must_use]
    pub fn is_healthy(&self, addr: SocketAddr) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&addr)
            .is_none_or(|f| f.load(Ordering::Relaxed) != 0)
    }

    /// Sets health for `addr`.
    pub fn set(&self, addr: SocketAddr, healthy: bool) {
        self.flag(addr).store(u64::from(healthy), Ordering::Relaxed);
    }

    /// All tracked addresses.
    #[must_use]
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// Snapshot of every tracked (address, healthy) pair (admin dump).
    #[must_use]
    pub fn snapshot(&self) -> Vec<(SocketAddr, bool)> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(a, f)| (*a, f.load(Ordering::Relaxed) != 0))
            .collect()
    }
}

/// Probes backends on an interval and flips [`HealthMap`] flags.
///
/// Flap-proofing: a backend is marked unhealthy only after
/// [`HealthChecker::DOWN_AFTER`] consecutive failed probes, and recovers
/// on the first success. This keeps container/K8s startup ordering (slow
/// backends, late endpoint propagation) from taking freshly-started
/// backends out of rotation on the first tick.
pub struct HealthChecker {
    map: std::sync::Arc<HealthMap>,
    /// Per-address HTTP probe path (None => TCP connect only).
    http_paths: HashMap<SocketAddr, String>,
    /// Probe interval.
    pub interval: Duration,
    /// HTTP client (connection pooled, rustls).
    client: reqwest::Client,
    /// Consecutive failures per address.
    failures: Mutex<HashMap<SocketAddr, u32>>,
}

impl HealthChecker {
    /// Consecutive failed probes before a backend is marked down.
    pub const DOWN_AFTER: u32 = 3;

    /// Creates a checker; call [`Self::spawn`] to start the loop.
    ///
    /// # Panics
    /// If the reqwest client cannot be built (TLS init failure).
    #[must_use]
    pub fn new(map: std::sync::Arc<HealthMap>, interval: Duration) -> Self {
        #[allow(clippy::expect_used, reason = "TLS init failure is fatal at startup")]
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("reqwest client");
        Self {
            map,
            http_paths: HashMap::new(),
            interval,
            client,
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Registers an HTTP probe path for a backend.
    pub fn set_http_probe(&mut self, addr: SocketAddr, path: String) {
        self.http_paths.insert(addr, path);
    }

    /// One probe round over all tracked addresses.
    pub async fn probe_once(&self) {
        for addr in self.map.addresses() {
            let healthy = if let Some(path) = self.http_paths.get(&addr) {
                let url = format!("http://{addr}{path}");
                match self.client.get(&url).send().await {
                    Ok(resp) => resp.status().is_success(),
                    Err(_) => false,
                }
            } else {
                tcp_probe(addr).await
            };
            if healthy {
                self.failures
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&addr);
                if !self.map.is_healthy(addr) {
                    tracing::info!(%addr, "health probe: backend recovered");
                }
                self.map.set(addr, true);
            } else {
                let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
                let count = failures.entry(addr).or_insert(0);
                *count += 1;
                if *count >= Self::DOWN_AFTER && self.map.is_healthy(addr) {
                    tracing::warn!(%addr, failures = *count, "health probe: backend down");
                    self.map.set(addr, false);
                }
            }
        }
    }

    /// Spawns the periodic probe loop on the control runtime.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let checker = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(checker.interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                checker.probe_once().await;
            }
        })
    }
}

async fn tcp_probe(addr: SocketAddr) -> bool {
    tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr))
        .await
        .is_ok_and(|r| r.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn flags_flip() {
        let map = Arc::new(HealthMap::new());
        let addr: SocketAddr = "127.0.0.1:1".parse().expect("addr");
        assert!(map.is_healthy(addr)); // default healthy
        map.set(addr, false);
        assert!(!map.is_healthy(addr));
        map.set(addr, true);
        assert!(map.is_healthy(addr));
    }

    #[tokio::test]
    async fn tcp_probe_down() {
        // Port 1 on loopback is virtually always closed.
        let addr: SocketAddr = "127.0.0.1:1".parse().expect("addr");
        assert!(!tcp_probe(addr).await);
    }
}
