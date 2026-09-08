//! Upstream selection — power-of-two-choices, round-robin, least-connections.
//!
//! Balancers are worker-local (constructed per worker); counters are
//! `Relaxed` atomics that tolerate skew. Choice happens after the router
//! picked a cluster and after unhealthy backends are filtered.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A backend endpoint with health/weight state.
#[derive(Debug, Clone)]
pub struct Backend {
    /// TCP address.
    pub addr: SocketAddr,
    /// Relative weight (RR2 tiebreak, weighted RR).
    pub weight: u32,
    /// Shared health flag (health checker flips it; workers read Relaxed).
    pub healthy: Arc<AtomicU64>,
}

impl Backend {
    /// New healthy backend.
    #[must_use]
    pub fn new(addr: SocketAddr, weight: u32) -> Self {
        Self {
            addr,
            weight: weight.max(1),
            healthy: Arc::new(AtomicU64::new(1)),
        }
    }

    /// `true` unless the health checker marked it down.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed) != 0
    }

    /// Marks health state.
    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(u64::from(healthy), Ordering::Relaxed);
    }

    /// Replaces this backend's health flag with a shared cell (health
    /// checker ownership across config generations).
    pub fn attach_health(&mut self, flag: std::sync::Arc<AtomicU64>) {
        flag.store(self.healthy.load(Ordering::Relaxed), Ordering::Relaxed);
        self.healthy = flag;
    }
}

/// Load-balancing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// Round-robin (deterministic fan-out).
    RoundRobin,
    /// Power-of-two-choices — pick two at random, take the less loaded.
    /// Best p99 under heterogeneous load.
    #[default]
    P2C,
    /// Least connections (requires per-backend connection gauges).
    LeastConn,
}

/// Cluster-level connection gauge shared between worker and health checker.
#[derive(Debug, Default)]
pub struct ConnGauges {
    /// One counter per backend index.
    counts: Vec<AtomicU64>,
}

impl ConnGauges {
    /// New gauges for `n` backends.
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            counts: (0..n).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Increments backend `i`'s in-flight count.
    pub fn inc(&self, i: usize) {
        if let Some(c) = self.counts.get(i) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrements backend `i`'s in-flight count.
    pub fn dec(&self, i: usize) {
        if let Some(c) = self.counts.get(i) {
            c.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Reads backend `i`'s in-flight count.
    #[must_use]
    pub fn get(&self, i: usize) -> u64 {
        self.counts.get(i).map_or(0, |c| c.load(Ordering::Relaxed))
    }
}

/// Per-worker balancer over a fixed backend list.
pub struct Balancer {
    backends: Vec<Backend>,
    gauges: Arc<ConnGauges>,
    policy: Policy,
    /// Worker-local RR cursor.
    rr: u64,
    /// Per-worker PRNG state (xorshift64*).
    rng: u64,
}

impl Balancer {
    /// New balancer for one worker.
    #[must_use]
    pub fn new(backends: Vec<Backend>, gauges: Arc<ConnGauges>, policy: Policy, seed: u64) -> Self {
        Self {
            backends,
            gauges,
            policy,
            rr: seed,
            rng: seed | 1,
        }
    }

    /// Backend list (for health dashboards).
    #[must_use]
    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    /// Connection gauges.
    #[must_use]
    pub fn gauges(&self) -> &Arc<ConnGauges> {
        &self.gauges
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Picks the next backend index among healthy backends.
    ///
    /// Returns `None` when every backend is unhealthy.
    pub fn pick(&mut self) -> Option<usize> {
        let healthy: Vec<usize> = self
            .backends
            .iter()
            .enumerate()
            .filter(|(_, b)| b.is_healthy())
            .map(|(i, _)| i)
            .collect();
        let first = *healthy.first()?;
        if healthy.len() == 1 {
            return Some(first);
        }
        match self.policy {
            Policy::RoundRobin => {
                self.rr = self.rr.wrapping_add(1);
                let idx = (self.rr as usize) % healthy.len();
                Some(healthy[idx])
            }
            Policy::P2C => {
                let a = healthy[(self.next_rand() as usize) % healthy.len()];
                let b = healthy[(self.next_rand() as usize) % healthy.len()];
                if a == b {
                    return Some(a);
                }
                let la = self.gauges.get(a);
                let lb = self.gauges.get(b);
                if la <= lb { Some(a) } else { Some(b) }
            }
            Policy::LeastConn => healthy.into_iter().min_by_key(|&i| {
                self.gauges
                    .get(i)
                    .wrapping_mul(u64::from(u32::MAX) / u64::from(self.backends[i].weight.max(1)))
            }),
        }
    }

    /// Picked backend address.
    pub fn pick_addr(&mut self) -> Option<SocketAddr> {
        self.pick().map(|i| self.backends[i].addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn be(port: u16) -> Backend {
        Backend::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port), 1)
    }

    #[test]
    fn skips_unhealthy() {
        let mut b = Balancer::new(
            vec![be(1), be(2)],
            Arc::new(ConnGauges::new(2)),
            Policy::RoundRobin,
            1,
        );
        b.backends()[0].set_healthy(false);
        for _ in 0..10 {
            assert_eq!(b.pick_addr().map(|a| a.port()), Some(2));
        }
        b.backends()[1].set_healthy(false);
        assert!(b.pick().is_none());
    }

    #[test]
    fn rr_cycles_all() {
        let mut b = Balancer::new(
            vec![be(1), be(2), be(3)],
            Arc::new(ConnGauges::new(3)),
            Policy::RoundRobin,
            1,
        );
        let mut seen = std::collections::HashSet::new();
        for _ in 0..9 {
            seen.insert(b.pick_addr().expect("addr").port());
        }
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn p2c_prefers_less_loaded() {
        // Backend 0 is loaded; 1 and 2 are idle. Two independent draws both
        // hit the loaded backend only ~1/9 of the time, so p2c should pick
        // an idle backend in ~8/9 of requests.
        let gauges = Arc::new(ConnGauges::new(3));
        for _ in 0..100 {
            gauges.inc(0);
        }
        let mut b = Balancer::new(
            vec![be(1), be(2), be(3)],
            Arc::clone(&gauges),
            Policy::P2C,
            7,
        );
        let mut idle = 0;
        for _ in 0..600 {
            let p = b.pick_addr().expect("addr").port();
            if p != 1 {
                idle += 1;
            }
        }
        assert!(idle > 450, "p2c underperforming: {idle}/600");
    }

    #[test]
    fn least_conn_picks_min() {
        let gauges = Arc::new(ConnGauges::new(3));
        gauges.inc(0);
        gauges.inc(1);
        gauges.inc(1);
        let mut b = Balancer::new(
            vec![be(1), be(2), be(3)],
            Arc::clone(&gauges),
            Policy::LeastConn,
            1,
        );
        assert_eq!(b.pick_addr().expect("addr").port(), 3);
    }
}
