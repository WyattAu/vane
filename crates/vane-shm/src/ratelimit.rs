//! Process-wide GCRA rate limiting: one lock-free cell shared by all
//! workers (threads) in the process, so a configured rate is the true
//! aggregate — not multiplied by worker count.
//!
//! Classic Generic Cell Rate Algorithm over a single `AtomicI64`
//! (the theoretical arrival time, nanoseconds). CAS loop, no locks,
//! one cache line.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

const NS_PER_SEC: i64 = 1_000_000_000;

/// Shared GCRA bucket.
pub struct SharedGcra {
    /// Theoretical arrival time of the next compliant cell (ns,
    /// `CLOCK_MONOTONIC`-style Instant origin). 0 = uninitialized.
    tat: AtomicI64,
    emission_interval_ns: i64,
    burst_offset_ns: i64,
}

impl SharedGcra {
    /// A bucket admitting `rps` requests/second sustained with `burst`
    /// instantaneous allowance (minimum 1).
    #[must_use]
    pub fn new(rps: u32, burst: u32) -> Self {
        let rps = rps.max(1);
        let burst = burst.max(1);
        Self {
            tat: AtomicI64::new(0),
            emission_interval_ns: NS_PER_SEC / i64::from(rps),
            burst_offset_ns: i64::from(burst) * (NS_PER_SEC / i64::from(rps)),
        }
    }

    /// Admits one request at `now`; `false` when over the rate.
    pub fn allow_at(&self, now_ns: i64) -> bool {
        let mut tat = self.tat.load(Ordering::Relaxed);
        loop {
            let (limit, new_tat) = if tat == 0 {
                // First cell: admit, anchor the schedule at `now`.
                (true, now_ns + self.emission_interval_ns)
            } else {
                // Compliant if we have not consumed the burst offset.
                if now_ns < tat - self.burst_offset_ns + self.emission_interval_ns {
                    return false;
                }
                (true, tat.max(now_ns) + self.emission_interval_ns)
            };
            if !limit {
                return false;
            }
            match self
                .tat
                .compare_exchange(tat, new_tat, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => tat = actual,
            }
        }
    }

    /// Admits one request at the current instant.
    pub fn allow(&self) -> bool {
        self.allow_at(monotonic_ns())
    }
}

thread_local! {
    static PROCESS_START: Instant = Instant::now();
}

/// Monotonic nanoseconds since first use in this process.
#[must_use]
pub fn monotonic_ns() -> i64 {
    PROCESS_START.with(|s| i64::try_from(s.elapsed().as_nanos()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn burst_admitted_then_throttled() {
        let g = SharedGcra::new(100, 10); // 10/s sustained, burst 10
        let now = monotonic_ns();
        let mut admitted = 0;
        for _ in 0..20 {
            if g.allow_at(now) {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 10, "burst of 10 admitted, rest rejected");
    }

    #[test]
    fn sustained_rate_matches_config() {
        let g = SharedGcra::new(1_000, 1); // 1000/s, burst 1
        let start = monotonic_ns();
        let mut admitted = 0;
        // ~50 ms window: expect ~50 admissions (+1 burst).
        while monotonic_ns() - start < 50_000_000 {
            if g.allow_at(monotonic_ns()) {
                admitted += 1;
            }
        }
        assert!(
            (30..=80).contains(&admitted),
            "sustained admissions near 1000/s x 50ms, got {admitted}"
        );
    }

    #[test]
    fn concurrent_workers_share_the_budget() {
        // Four workers hammering one bucket: the AGGREGATE must stay
        // near the configured rate — the cross-worker point of this
        // limiter.
        let g = std::sync::Arc::new(SharedGcra::new(2_000, 4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let g = std::sync::Arc::clone(&g);
            handles.push(thread::spawn(move || {
                let mut admitted = 0u32;
                let start = monotonic_ns();
                while monotonic_ns() - start < 100_000_000 {
                    if g.allow_at(monotonic_ns()) {
                        admitted += 1;
                    }
                }
                admitted
            }));
        }
        let total: u32 = handles.into_iter().map(|h| h.join().expect("join")).sum();
        // 2000/s x 100ms = 200 + burst 4 slack.
        assert!(
            (150..=260).contains(&total),
            "aggregate admissions near budget, got {total}"
        );
    }
}
