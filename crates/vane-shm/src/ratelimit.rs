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
        // Synthetic clock: `allow_at` takes the timestamp, so the test
        // drives a deterministic 1 ms-per-step schedule instead of a
        // wall-clock busy loop. The busy-loop version measured how often
        // the test THREAD got scheduled — on a loaded runner the loop
        // starves, admissions drop below the band, and a correct limiter
        // 'fails' (exactly what happened in CI). Same invariant, zero
        // scheduler dependence.
        let g = SharedGcra::new(1_000, 1); // 1000/s, burst 1
        let mut t: i64 = 1_000_000_000; // arbitrary anchor
        let mut admitted = 0;
        for _ in 0..2_000 {
            // 2000 steps x 1 ms = 2 s at 1000/s.
            if g.allow_at(t) {
                admitted += 1;
            }
            t += 1_000_000;
        }
        assert!(
            (1_950..=2_050).contains(&admitted),
            "sustained admissions near 1000/s over 2 s, got {admitted}"
        );
    }

    #[test]
    fn concurrent_workers_share_the_budget() {
        // Four workers hammering one bucket: the AGGREGATE must stay
        // near the configured rate — the cross-worker point of this
        // limiter. Threads share one synthetic clock (an AtomicU64 the
        // test advances), so admission decisions depend on the schedule,
        // not on scheduler slices.
        let g = std::sync::Arc::new(SharedGcra::new(2_000, 4));
        let clock = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1_000_000_000));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let g = std::sync::Arc::clone(&g);
            let clock = std::sync::Arc::clone(&clock);
            handles.push(thread::spawn(move || {
                let mut admitted = 0u32;
                // 4 threads x 4000 steps x 0.125 ms = 16000 calls spread
                // over 2 s: demand 8000/s against a 2000/s budget — 4x
                // oversubscribed.
                for _ in 0..4_000 {
                    let t = clock.fetch_add(125_000, std::sync::atomic::Ordering::Relaxed);
                    if g.allow_at(t) {
                        admitted += 1;
                    }
                }
                admitted
            }));
        }
        let total: u32 = handles.into_iter().map(|h| h.join().expect("join")).sum();
        // Budget: 2000/s x 2 s = 4000, plus the burst-4 allowance.
        assert!(
            (3_950..=4_010).contains(&total),
            "aggregate admissions near budget (4000 + burst slack), got {total}"
        );
    }
}
