//! Gradient-based adaptive concurrency limiting (one per cluster, per
//! worker).
//!
//! A fixed concurrency cap is either too low (wasted capacity) or too
//! high (queueing collapse): the right limit depends on the backend,
//! and it changes. This controller measures the GRADIENT of request
//! latency against the cluster's own smoothed baseline and moves the
//! limit between `min_limit` and `max_limit`:
//!
//! ```text
//! gradient  = clamp(0.5 .. 1.0, tolerance * baseline / latest)
//! new_limit = limit * gradient + queue_size
//! new_limit = limit * (1 - smoothing) + new_limit * smoothing
//! ```
//!
//! - Latency at baseline → gradient 1.0 → the limit grows by
//!   `queue_size` per update (slowly, through the smoothing).
//! - Latency 2× baseline → gradient 0.5 (clamped) → the limit falls
//!   toward `min_limit` — the cluster sheds instead of collapsing.
//!
//! This is Netflix's Gradient2 (concurrency-limits), also shipped as
//! Envoy's adaptive-concurrency filter. Two deliberate deviations,
//! both from the Netflix class documentation:
//!
//! - The baseline is an EXPONENTIALLY SMOOTHED average, not a minimum:
//!   RPC latency is bursty, and a minimum baseline biases toward
//!   impractically low limits and excessive shedding.
//! - When the baseline exceeds 2× the latest sample (latency has
//!   RETURNED to normal after a load episode), the baseline decays
//!   5% per sample instead of waiting for the smoothing — otherwise
//!   recovery from a load episode takes longer than the episode did.

/// One cluster's adaptive limit. Worker-local by design: the same
/// shared-nothing rule as the pool and the breaker gate's per-worker
/// halves — no atomics on the request path. An operator reads the
/// per-worker limit gauge and multiplies by workers.
#[derive(Debug)]
pub struct AdaptiveLimit {
    cfg: vane_control::config::AdaptiveConcurrencyConfig,
    estimated_limit: f64,
    /// The latest completed request's latency (the volatile sample).
    last_rtt_us: f64,
    /// The smoothed baseline (the stable reference).
    long_rtt_us: f64,
    /// Samples folded into the baseline so far (warmup = running
    /// average, then exponential decay).
    baseline_samples: u32,
    /// Requests currently admitted past this limit.
    inflight: u32,
}

/// Baseline smoothing: ~600 samples of memory (Netflix's long window).
const BASELINE_DECAY: f64 = 600.0;
/// Warmup samples before the decay kicks in (a running average until
/// then, so the first request doesn't anchor the baseline forever).
const BASELINE_WARMUP: u32 = 10;
/// The gradient's floor: one outlier must not halve the limit.
const GRADIENT_FLOOR: f64 = 0.5;

impl AdaptiveLimit {
    /// Builds the controller from its cluster's config; the limit
    /// starts at the floor (see the struct docs).
    pub fn new(cfg: vane_control::config::AdaptiveConcurrencyConfig) -> Self {
        // Start at the floor: the controller can only grow a limit by
        // observing healthy latencies, so the safe initial state is
        // the smallest limit we are willing to run.
        let initial = f64::from(cfg.min_limit.max(1));
        Self {
            estimated_limit: initial,
            last_rtt_us: 0.0,
            long_rtt_us: 0.0,
            baseline_samples: 0,
            inflight: 0,
            cfg,
        }
    }

    /// The current estimated limit (requests allowed in flight).
    pub fn limit(&self) -> u32 {
        self.estimated_limit.round() as u32
    }

    /// Requests currently admitted.
    pub fn inflight(&self) -> u32 {
        self.inflight
    }

    /// Attempts to admit one request: `false` means SHED (the caller
    /// answers 503 and the request never reaches a backend — a shed
    /// request does not count as in flight and does not sample
    /// latency, so rejections cannot poison the baseline).
    pub fn enter(&mut self) -> bool {
        if self.inflight >= self.limit() {
            return false;
        }
        self.inflight += 1;
        true
    }

    /// A admitted request completed: release its slot and feed the
    /// latency sample into the controller.
    ///
    /// The sample sees the in-flight count BEFORE this release (the
    /// concurrency that produced this latency). Sampling after the
    /// release would let the app-limited rule eat almost every update
    /// on a fast cluster — completions rarely overlap enough for the
    /// post-release count to clear `limit / 2`, and a limit that never
    /// grows is a limit that never recovers. (Netflix's limiter passes
    /// the pre-release in-flight count for the same reason.)
    pub fn exit(&mut self, rtt_us: u64) {
        let inflight_at_completion = self.inflight;
        self.inflight = self.inflight.saturating_sub(1);
        self.on_sample(rtt_us, inflight_at_completion);
    }

    /// An admitted request's transaction ended without a completion
    /// sample (the downstream connection died mid-flight): release the
    /// slot without pretending to know a latency.
    pub fn abandon(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
    }

    fn on_sample(&mut self, rtt_us: u64, inflight_at_completion: u32) {
        let sample = f64::from(u32::try_from(rtt_us).unwrap_or(u32::MAX)).max(1.0);
        self.last_rtt_us = sample;

        // Baseline: running average for the first samples, then
        // exponential decay.
        if self.baseline_samples < BASELINE_WARMUP {
            let n = f64::from(self.baseline_samples);
            self.long_rtt_us = (self.long_rtt_us * n + sample) / (n + 1.0);
            self.baseline_samples += 1;
        } else {
            self.long_rtt_us += (sample - self.long_rtt_us) / BASELINE_DECAY;
        }
        self.update_limit(inflight_at_completion);
    }

    /// The Gradient2 update. `inflight` is the concurrency that
    /// produced the latest sample (pre-release). The math is the whole
    /// feature and deserves direct tests.
    fn update_limit(&mut self, inflight: u32) {
        if self.baseline_samples == 0 || self.last_rtt_us <= 0.0 {
            return;
        }

        // Fast recovery: the baseline lags reality after a load
        // episode (long >> short). Decay it instead of waiting for the
        // smoothing, or recovery takes longer than the episode did.
        if self.long_rtt_us / self.last_rtt_us > 2.0 {
            self.long_rtt_us *= 0.95;
        }

        // App-limited: fewer requests in flight than half the limit
        // means the cluster is not the contended resource — growing
        // would be measuring nothing.
        if f64::from(inflight) < self.estimated_limit / 2.0 {
            return;
        }

        let gradient = (self.cfg.rtt_tolerance * self.long_rtt_us / self.last_rtt_us)
            .clamp(GRADIENT_FLOOR, 1.0);
        let mut new_limit = self.estimated_limit * gradient + f64::from(self.cfg.queue_size);
        new_limit =
            self.estimated_limit * (1.0 - self.cfg.smoothing) + new_limit * self.cfg.smoothing;
        self.estimated_limit =
            new_limit.clamp(f64::from(self.cfg.min_limit), f64::from(self.cfg.max_limit));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vane_control::config::AdaptiveConcurrencyConfig;

    fn cfg() -> AdaptiveConcurrencyConfig {
        // `AdaptiveConcurrencyConfig` is `#[non_exhaustive]` (it is a
        // parsed config struct), so tests build it by mutation.
        let mut cfg = AdaptiveConcurrencyConfig::default();
        cfg.min_limit = 2;
        cfg.max_limit = 200;
        cfg.queue_size = 1;
        cfg.smoothing = 0.2;
        cfg.rtt_tolerance = 1.5;
        cfg
    }

    /// A controller warmed up at `base` us, with `inflight` requests
    /// in flight (the app-limited rule needs in-flight work to move).
    fn warmed(base_us: u64, inflight: u32) -> AdaptiveLimit {
        let mut l = AdaptiveLimit::new(cfg());
        for _ in 0..(BASELINE_WARMUP + 5) {
            l.on_sample(base_us, inflight);
        }
        l
    }

    /// Steady latencies never shed: the limit may grow (the queue
    /// allowance while healthy) but only up to what the traffic could
    /// possibly use — twice the observed concurrency — and never down
    /// below where it started.
    #[test]
    fn steady_state_never_sheds_and_growth_converges() {
        let mut l = warmed(1_000, 10);
        let start = l.limit();
        for _ in 0..200 {
            l.inflight = 10; // sustained load at 10 concurrent
            l.exit(1_000);
        }
        assert!(
            l.limit() >= start,
            "steady latencies shed the limit: {start} -> {}",
            l.limit()
        );
        // App-limited convergence: growth stops near twice the actual
        // concurrency (the inflight < limit/2 rule).
        assert!(
            l.limit() <= 2 * 10 + l.cfg.queue_size + 1,
            "growth did not converge: {}",
            l.limit()
        );
    }

    /// A 4x latency collapse drives the limit to the floor.
    #[test]
    fn latency_collapse_sheds_toward_the_floor() {
        let mut l = warmed(1_000, 10);
        for _ in 0..60 {
            l.inflight = 10;
            l.exit(4_000);
        }
        assert_eq!(
            l.limit(),
            l.cfg.min_limit,
            "a sustained 4x slowdown must shed to the floor, got {}",
            l.limit()
        );
    }

    /// Recovery: latencies return to baseline and the limit grows back
    /// (bounded by max), even though the stale baseline starts high.
    #[test]
    fn recovery_grows_the_limit_back() {
        let mut l = warmed(1_000, 10);
        for _ in 0..60 {
            l.inflight = 10;
            l.exit(4_000);
        }
        let shed = l.limit();
        for _ in 0..200 {
            l.inflight = 10;
            l.exit(1_000);
        }
        assert!(
            l.limit() > shed * 4,
            "recovery must grow the limit well past the shed floor: {} -> {}",
            shed,
            l.limit()
        );
        assert!(l.limit() <= l.cfg.max_limit);
    }

    /// The gradient's floor: ONE pathological sample must not halve
    /// the limit (an outlier is noise, not a capacity signal).
    #[test]
    fn one_outlier_cannot_halve_the_limit() {
        let mut l = warmed(1_000, 10);
        let start = l.limit();
        l.exit(1_000_000); // a 1000x spike
        assert!(
            l.limit() as f64 >= start as f64 * 0.5,
            "one outlier moved the limit from {start} to {}",
            l.limit()
        );
    }

    /// App-limited: once in-flight work drops below half the limit,
    /// there is nothing to learn from the cluster — growth must stop.
    #[test]
    fn app_limited_stops_growth() {
        let mut l = warmed(1_000, 10);
        // Drain the in-flight count without refilling it.
        for _ in 0..10 {
            l.exit(1_000);
        }
        assert_eq!(l.inflight(), 0);
        let frozen = l.limit();
        for _ in 0..100 {
            l.exit(1_000); // every exit saturates at 0 in flight
        }
        assert_eq!(
            l.limit(),
            frozen,
            "an idle cluster grew its limit from {frozen} to {}",
            l.limit()
        );
    }

    /// Shedding does not poison the baseline: rejected requests never
    /// sample, and admitted-but-shedding traffic still steers the
    /// limit down rather than oscillating it up.
    #[test]
    fn shedding_does_not_raise_the_limit() {
        let mut l = warmed(1_000, 10);
        for _ in 0..40 {
            l.inflight = 10;
            l.exit(8_000);
        }
        let shed = l.limit();
        for _ in 0..40 {
            l.inflight = 10;
            l.exit(8_000);
        }
        assert!(
            l.limit() <= shed + 1,
            "sustained overload must not grow the limit: {shed} -> {}",
            l.limit()
        );
    }

    /// enter/exit bookkeeping: the in-flight count is exact, and the
    /// admission decision respects the current limit.
    #[test]
    fn enter_exit_tracks_inflight_exactly() {
        let mut l = AdaptiveLimit::new(cfg()); // min 2
        assert!(l.enter());
        assert!(l.enter());
        assert!(!l.enter(), "at the limit: shed");
        assert_eq!(l.inflight(), 2);
        l.abandon();
        assert_eq!(l.inflight(), 1);
        l.exit(100);
        assert_eq!(l.inflight(), 0);
        assert!(l.enter());
    }

    /// The floor is respected even under pathological input: the
    /// limit never falls below min_limit (no death spirals) and never
    /// exceeds max_limit (no runaway growth).
    #[test]
    fn limit_stays_within_the_configured_bounds() {
        let mut l = AdaptiveLimit::new(cfg());
        let mut rng_state: u64 = 0x9E3779B97F4A7C15;
        for _ in 0..2_000 {
            // xorshift: deterministic pseudo-latencies spanning 4 orders
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            let rtt = 100 + (rng_state % 1_000_000);
            l.inflight = 10;
            l.exit(rtt);
            let limit = l.limit();
            assert!((l.cfg.min_limit..=l.cfg.max_limit).contains(&limit));
        }
    }
}
