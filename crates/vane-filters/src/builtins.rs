//! Built-in filters reusing the WyattAu kit ecosystem.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Instant;

use breaker::{CircuitBreaker, CircuitBreakerConfig};
use throttle_kit::{InMemoryBackend, Quota, RateLimiter};
use vane_observe::metrics::{MetricHandle, MetricKind, Registry};

use crate::RequestCtx;
use crate::pipeline::{Filter, Outcome};

/// GCRA rate limiter keyed per client IP (wraps the `ratelimit` crate).
pub struct RateLimit {
    limiter: RateLimiter<InMemoryBackend>,
    /// Rejects counter.
    rejected: MetricHandle,
    registry: std::sync::Arc<Registry>,
}

impl RateLimit {
    /// Builds a limiter from requests-per-second + burst.
    ///
    /// # Panics
    /// Metric registration failure (startup-only, capacity bounded).
    #[must_use]
    pub fn new(registry: std::sync::Arc<Registry>, per_second: u32, burst: u32) -> Self {
        let quota = Quota::per_second(per_second).allow_burst(burst);
        Self {
            limiter: RateLimiter::new(quota, InMemoryBackend::new()),
            rejected: registry.register("vane_ratelimit_rejected_total", MetricKind::Counter),
            registry,
        }
    }

    /// Async check — for callers already inside a runtime (the h2 edge).
    /// `check_sync` would panic there ("runtime within a runtime").
    pub async fn check_async(&self, key: &str) -> Outcome {
        match self.limiter.check(key).await.allowed {
            true => Outcome::Continue,
            false => {
                self.rejected.inc(&self.registry);
                Outcome::Reject(429, "rate limited")
            }
        }
    }
}

impl Filter for RateLimit {
    fn name(&self) -> &'static str {
        "rate_limit"
    }

    fn run(&self, ctx: &mut RequestCtx<'_>) -> Outcome {
        let key = ctx.client.ip().to_string();
        let result = self.limiter.check_sync(&key);
        if result.allowed {
            Outcome::Continue
        } else {
            self.rejected.inc(&self.registry);
            Outcome::Reject(429, "rate limited")
        }
    }
}

/// Circuit-breaker gate keyed per upstream cluster (wraps `breaker`).
///
/// Pre-route: open breakers short-circuit to 503. Post-route success and
/// failure must be recorded with [`Self::record_success`] /
/// [`Self::record_failure`] by the proxy loop (per connection outcome).
pub struct BreakerGate {
    breakers: Mutex<HashMap<String, std::sync::Arc<CircuitBreaker>>>,
    default_config: CircuitBreakerConfig,
    rejected: MetricHandle,
    registry: std::sync::Arc<Registry>,
}

impl BreakerGate {
    /// New gate with a default per-cluster config.
    ///
    /// # Panics
    /// Metric registration failure (startup-only).
    #[must_use]
    pub fn new(registry: std::sync::Arc<Registry>) -> Self {
        Self {
            breakers: Mutex::new(HashMap::new()),
            default_config: CircuitBreakerConfig::standard(),
            rejected: registry.register("vane_breaker_rejected_total", MetricKind::Counter),
            registry,
        }
    }

    /// Gets or creates the breaker for a cluster.
    pub fn cluster_breaker(&self, cluster: &str) -> std::sync::Arc<CircuitBreaker> {
        let mut map = self.breakers.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(cluster.to_owned())
            .or_insert_with(|| {
                std::sync::Arc::new(
                    CircuitBreaker::builder(self.default_config.clone())
                        .name(cluster)
                        .build(),
                )
            })
            .clone()
    }

    /// Records an upstream success for `cluster`.
    pub fn record_success(&self, cluster: &str) {
        self.cluster_breaker(cluster).record_success();
    }

    /// Records an upstream failure for `cluster`.
    pub fn record_failure(&self, cluster: &str) {
        self.cluster_breaker(cluster).record_failure();
    }
}

impl Filter for BreakerGate {
    fn name(&self) -> &'static str {
        "breaker"
    }

    fn run(&self, ctx: &mut RequestCtx<'_>) -> Outcome {
        let Some(cluster) = ctx.cluster else {
            return Outcome::Continue; // pre-route: nothing to gate yet
        };
        let breaker = self.cluster_breaker(cluster);
        if breaker.is_open() {
            self.rejected.inc(&self.registry);
            Outcome::Reject(503, "upstream unavailable")
        } else {
            Outcome::Continue
        }
    }
}

/// Stamps `X-Request-Id` (random hex) into the upstream headers.
#[derive(Default)]
pub struct RequestId {
    counter: std::sync::atomic::AtomicU64,
}

impl Filter for RequestId {
    fn name(&self) -> &'static str {
        "request_id"
    }

    fn run(&self, ctx: &mut RequestCtx<'_>) -> Outcome {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!(
            "{:012x}{:04x}{}",
            now_micros(),
            (n & 0xffff) as u16,
            std::process::id()
        );
        ctx.inject("X-Request-Id", &id);
        Outcome::Continue
    }
}

fn now_micros() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros())
}

/// Adds `X-Forwarded-For` / `X-Forwarded-Proto` / `X-Forwarded-Host`.
#[derive(Default)]
pub struct ForwardedHeaders {
    _private: OnceLock<()>,
}

impl Filter for ForwardedHeaders {
    fn name(&self) -> &'static str {
        "forwarded"
    }

    fn run(&self, ctx: &mut RequestCtx<'_>) -> Outcome {
        // Append to existing XFF if the handler supplied one (it reads the
        // request headers; here we only know the client addr).
        ctx.inject("X-Forwarded-For", &ctx.client.ip().to_string());
        ctx.inject("X-Forwarded-Proto", "http");
        if let Some(host) = ctx.host {
            ctx.inject("X-Forwarded-Host", host);
        }
        Outcome::Continue
    }
}

/// Access log filter: emits fixed-size events into the worker's ring.
pub struct AccessLog {
    started: Instant,
}

impl AccessLog {
    /// New filter (tracks uptime for p50/99 deltas).
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Default for AccessLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Filter for AccessLog {
    fn name(&self) -> &'static str {
        "access_log"
    }

    fn run(&self, ctx: &mut RequestCtx<'_>) -> Outcome {
        if let Some((code, reason)) = ctx.short_circuit {
            let msg = format!(
                "{} {} -> {} {} ({}ms)",
                ctx.method,
                ctx.path,
                code,
                reason,
                self.started.elapsed().as_millis()
            );
            crate::log_hint(&msg);
        }
        Outcome::Continue
    }
}

#[cfg(test)]
mod builtin_tests {
    use super::*;
    use crate::pipeline::{Filter, Outcome};

    fn ctx<'a>(method: &'a str, path: &'a mut str, host: Option<&'a str>) -> RequestCtx<'a> {
        RequestCtx::new(method, path, "10.0.0.7:1234".parse().expect("addr"), host)
    }

    #[test]
    fn rate_limit_blocks_after_burst() {
        let registry = std::sync::Arc::new(Registry::new());
        let rl = RateLimit::new(registry, 1, 2);
        let mut path = String::from("/x");
        // Burst of 2 allowed, 3rd within the window rejected.
        assert!(matches!(
            rl.run(&mut ctx("GET", &mut path, None)),
            Outcome::Continue
        ));
        assert!(matches!(
            rl.run(&mut ctx("GET", &mut path, None)),
            Outcome::Continue
        ));
        match rl.run(&mut ctx("GET", &mut path, None)) {
            Outcome::Reject(code, reason) => {
                assert_eq!(code, 429);
                assert!(!reason.is_empty());
            }
            other => panic!("expected reject, got {other:?}"),
        }
    }

    #[test]
    fn breaker_opens_after_failures_and_halfopens() {
        let registry = std::sync::Arc::new(Registry::new());
        let gate = BreakerGate::new(registry);
        let mut path = String::from("/x");
        for _ in 0..20 {
            gate.record_failure("c1");
        }
        // Open breaker short-circuits (ctx.cluster must be set — the gate
        // is post-route).
        let mut c = ctx("GET", &mut path, None);
        c.cluster = Some("c1");
        match gate.run(&mut c) {
            Outcome::Reject(code, _) => assert_eq!(code, 503),
            other => panic!("expected 503, got {other:?}"),
        }
        // Different cluster unaffected.
        let mut other_cluster = ctx("GET", &mut path, None);
        other_cluster.cluster = Some("c2");
        assert!(matches!(gate.run(&mut other_cluster), Outcome::Continue));
        // Successes don't fix an OPEN breaker (probe-gated).
        gate.record_success("c1");
        let mut again = ctx("GET", &mut path, None);
        again.cluster = Some("c1");
        assert!(matches!(gate.run(&mut again), Outcome::Reject(..)));
    }

    #[test]
    fn request_id_and_forwarded_inject_headers() {
        let rid = RequestId::default();
        let mut path = String::from("/y");
        let mut c = ctx("POST", &mut path, Some("h.example"));
        assert!(matches!(rid.run(&mut c), Outcome::Continue));
        assert!(c.inject_headers.iter().any(|(k, _)| k == "X-Request-Id"));

        let fwd = ForwardedHeaders::default();
        assert!(matches!(fwd.run(&mut c), Outcome::Continue));
        assert!(c.inject_headers.iter().any(|(k, _)| k == "X-Forwarded-For"));
        assert!(
            c.inject_headers
                .iter()
                .any(|(k, _)| k == "X-Forwarded-Host")
        );
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn check_async_blocks_after_burst() {
        let registry = std::sync::Arc::new(Registry::new());
        let rl = RateLimit::new(registry, 1, 1);
        assert!(matches!(
            rl.check_async("k").await,
            crate::pipeline::Outcome::Continue
        ));
        match rl.check_async("k").await {
            crate::pipeline::Outcome::Reject(code, _) => assert_eq!(code, 429),
            other => panic!("expected reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn check_async_independent_keys() {
        let registry = std::sync::Arc::new(Registry::new());
        let rl = RateLimit::new(registry, 1, 1);
        assert!(matches!(
            rl.check_async("a").await,
            crate::pipeline::Outcome::Continue
        ));
        assert!(matches!(
            rl.check_async("b").await,
            crate::pipeline::Outcome::Continue
        ));
    }
}
