//! # vane-filters
//!
//! Monomorphized request pipeline (`PL-02`): filters compose at compile
//! time into a single inlined call chain — **no vtable dispatch on the hot
//! path** (this is the anti-Envoy pillar).
//!
//! ```text
//! let pipe = Pipeline::new(AccessLog::new(...))
//!     .then(RateLimit::new(...))
//!     .then(BreakerGate::new(...))
//!     .then(ForwardedHeaders);
//! // pipe.run(&mut ctx)  →  fully inlined A → B → C
//! ```
//!
//! Built-ins reuse the WyattAu kit ecosystem:
//! - [`RateLimit`] wraps the `ratelimit` crate's GCRA engine.
//! - [`BreakerGate`] wraps the `breaker` crate's circuit breaker.
//! - [`ForwardedHeaders`] implements `X-Forwarded-For` / `X-Forwarded-Proto`.
//! - [`RequestId`] stamps `X-Request-Id`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use std::net::SocketAddr;

use vane_observe::trace::TraceContext;

pub mod builtins;
pub mod jwt;
pub mod pipeline;

/// Worker log bridge hook (bin installs a thread-local sink).
pub fn log_hint(msg: &str) {
    let _ = msg;
}

pub use builtins::{BreakerGate, ForwardedHeaders, RateLimit, RequestId};
pub use pipeline::{Outcome, Pipeline};

/// Mutable per-request filter context.
///
/// Filters run *before* routing: `path` rewrites here feed the router;
/// `short_circuit` stops the pipeline and responds immediately.
pub struct RequestCtx<'a> {
    /// Request method (`GET`...).
    pub method: &'a str,
    /// Request path (mutable — rewrites apply before routing).
    pub path: &'a mut str,
    /// Client socket.
    pub client: SocketAddr,
    /// `Host` header value (undecoded).
    pub host: Option<&'a str>,
    /// Short-circuit response: `(status, reason)` set by a rejecting filter.
    pub short_circuit: Option<(u16, &'static str)>,
    /// W3C trace context for this hop.
    pub trace: TraceContext,
    /// Headers to inject into the upstream request (`name`, `value`).
    pub inject_headers: Vec<(String, String)>,
    /// Cluster the router selected (set after routing; visible to
    /// post-route filters like [`BreakerGate`]).
    pub cluster: Option<&'a str>,
}

impl<'a> RequestCtx<'a> {
    /// New context for a request.
    #[must_use]
    pub fn new(
        method: &'a str,
        path: &'a mut str,
        client: SocketAddr,
        host: Option<&'a str>,
    ) -> Self {
        Self {
            method,
            path,
            client,
            host,
            short_circuit: None,
            trace: TraceContext::generate(),
            inject_headers: Vec::new(),
            cluster: None,
        }
    }

    /// Injects an upstream header (dedup by name).
    pub fn inject(&mut self, name: &str, value: &str) {
        if let Some(slot) = self
            .inject_headers
            .iter_mut()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
        {
            slot.1 = value.to_owned();
        } else {
            self.inject_headers
                .push((name.to_owned(), value.to_owned()));
        }
    }
}
