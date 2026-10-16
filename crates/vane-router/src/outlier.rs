//! Per-backend outlier detection: consecutive-failure ejection with
//! time-based recovery. Independent of the active health checker —
//! an outlier cell captures *application-level* health (5xx responses,
//! dial failures) while `healthy` captures *liveness* (connectivity).
//!
//! Lock-free on the request path: per-backend atomics, no locks.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// One backend's ejection state.
#[derive(Debug, Default)]
pub struct OutlierCell {
    consecutive_failures: AtomicU32,
    /// Unix-milliseconds until which the backend is ejected (0 = live).
    ejected_until_ms: AtomicI64,
}

/// Shared per-route outlier state (clone-cheap `Arc` inside
/// [`RouteEntry`](crate::table::RouteEntry)).
#[derive(Debug)]
pub struct OutlierSet {
    cells: Vec<(SocketAddr, OutlierCell)>,
    /// Consecutive failures that trigger ejection.
    threshold: u32,
    /// Ejection duration in milliseconds.
    ejection_ms: u64,
    /// Maximum hosts ejectable at once (at least 1): a shared-cause
    /// transient must not eject the whole cluster into a guaranteed
    /// outage.
    max_ejected: usize,
}

impl OutlierSet {
    /// New set over `addrs` with the given policy.
    #[must_use]
    pub fn new(addrs: Vec<SocketAddr>, threshold: u32, ejection_ms: u64) -> Self {
        Self::with_max_ejected(addrs, threshold, ejection_ms, 100)
    }

    /// New set with an ejection cap: at most `max_ejection_percent`% of
    /// the cluster, and always at least one host. Envoy's rule (10%,
    /// at-least-one) — vane defaults to 50% for its typically small
    /// clusters, where 10% would usually mean "eject nobody".
    #[must_use]
    pub fn with_max_ejected(
        addrs: Vec<SocketAddr>,
        threshold: u32,
        ejection_ms: u64,
        max_ejection_percent: u32,
    ) -> Self {
        let pct = max_ejection_percent.clamp(1, 100);
        let max_ejected = ((addrs.len() * pct as usize) / 100).clamp(1, addrs.len().max(1));
        Self {
            cells: addrs
                .into_iter()
                .map(|a| (a, OutlierCell::default()))
                .collect(),
            threshold: threshold.max(1),
            ejection_ms,
            max_ejected,
        }
    }

    fn ejected_count(&self) -> usize {
        let now = Self::now_ms();
        self.cells
            .iter()
            .filter(|(_, c)| now < c.ejected_until_ms.load(Ordering::Relaxed))
            .count()
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn cell(&self, addr: SocketAddr) -> Option<&OutlierCell> {
        self.cells.iter().find(|(a, _)| *a == addr).map(|(_, c)| c)
    }

    /// Records a backend failure; ejects on reaching the threshold.
    /// Returns `true` when this failure triggered an ejection.
    pub fn record_failure(&self, addr: SocketAddr) -> bool {
        let Some(cell) = self.cell(addr) else {
            return false;
        };
        let n = cell.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if n >= self.threshold {
            // The ejection cap is the cluster-survival rule: a shared-
            // cause transient (network blip, config push) otherwise
            // ejects everything into a guaranteed total outage.
            if self.ejected_count() >= self.max_ejected {
                return false;
            }
            let until = Self::now_ms() + self.ejection_ms as i64;
            cell.ejected_until_ms.store(until, Ordering::Relaxed);
            cell.consecutive_failures.store(0, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Records a backend success: resets the failure streak.
    pub fn record_success(&self, addr: SocketAddr) {
        if let Some(cell) = self.cell(addr) {
            cell.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }

    /// Whether `addr` is currently ejected.
    #[must_use]
    pub fn is_ejected(&self, addr: SocketAddr) -> bool {
        let Some(cell) = self.cell(addr) else {
            return false;
        };
        Self::now_ms() < cell.ejected_until_ms.load(Ordering::Relaxed)
    }

    /// Wraps in an `Arc` for a route entry.
    #[must_use]
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::thread::sleep;
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, p))
    }

    #[test]
    fn ejects_after_consecutive_failures_and_recovers() {
        let set = OutlierSet::new(vec![addr(1), addr(2)], 3, 100).shared();
        // Two failures: not ejected.
        set.record_failure(addr(1));
        set.record_failure(addr(1));
        assert!(!set.is_ejected(addr(1)));
        // Third consecutive failure ejects.
        assert!(set.record_failure(addr(1)));
        assert!(set.is_ejected(addr(1)));
        // The other backend is untouched.
        assert!(!set.is_ejected(addr(2)));
        // Success resets the streak for the healthy path.
        set.record_failure(addr(2));
        set.record_failure(addr(2));
        set.record_success(addr(2));
        set.record_failure(addr(2));
        set.record_failure(addr(2));
        assert!(!set.is_ejected(addr(2)), "streak was reset by success");
        // Ejection expires.
        sleep(Duration::from_millis(150));
        assert!(!set.is_ejected(addr(1)), "ejection must expire");
    }

    #[test]
    fn unknown_addr_is_never_ejected() {
        let set = OutlierSet::new(vec![addr(1)], 1, 60_000).shared();
        assert!(!set.is_ejected(addr(99)));
        assert!(!set.record_failure(addr(99)));
    }

    /// The ejection cap: with max 50% of a 2-backend cluster (1 host),
    /// the second backend is NOT ejected even past its threshold — a
    /// shared-cause transient must not eject the whole cluster.
    #[test]
    fn ejection_cap_keeps_the_cluster_serving() {
        let set = OutlierSet::with_max_ejected(vec![addr(1), addr(2)], 1, 60_000, 50).shared();
        // Backend 1 hits the threshold: ejected (cap 1 of 2).
        assert!(set.record_failure(addr(1)));
        assert!(set.is_ejected(addr(1)));
        // Backend 2 hits the threshold too: NOT ejected (cap reached).
        assert!(!set.record_failure(addr(2)));
        assert!(
            !set.is_ejected(addr(2)),
            "the last healthy backend must stay in rotation"
        );
        // And the balancer can still reach it.
        let set2 = OutlierSet::with_max_ejected(vec![addr(1), addr(2)], 1, 60_000, 100).shared();
        assert!(set2.record_failure(addr(1)));
        assert!(set2.record_failure(addr(2)));
        assert!(
            set2.is_ejected(addr(1)) && set2.is_ejected(addr(2)),
            "100% allows a full ejection"
        );
    }
}
