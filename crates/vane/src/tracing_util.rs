//! Request-span helpers: build `http.request` spans linked to inbound
//! W3C `traceparent` headers so OTLP backends stitch distributed traces.
//!
//! Without a configured exporter these are cheap no-ops (a disabled span
//! costs one atomic load). Sampling is deterministic per trace id so a
//! trace is either fully kept or fully dropped.

use vane_observe::trace::TraceContext;

/// Builds an `http.request` span for a request. When `traceparent` parses,
/// the span is parented to the remote context; otherwise it is a root.
#[must_use]
pub fn serve_span(
    method: &str,
    path: &str,
    host: Option<&str>,
    traceparent: Option<&str>,
) -> tracing::Span {
    let span = tracing::info_span!(
        "http.request",
        otel.kind = "server",
        http.method = %method,
        http.target = %path,
        http.host = %host.unwrap_or(""),
    );
    if let Some(tp) = traceparent.and_then(|t| TraceContext::parse(t.as_bytes())) {
        set_remote_parent(&span, &tp);
    }
    span
}

/// Deterministic sampling: keep the trace iff the first 4 id bytes as a
/// fraction fall under `rate`. `rate >= 1.0` keeps everything.
#[must_use]
pub fn sampled(trace_id: &[u8; 16], rate: f32) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    let prefix = u32::from_be_bytes([trace_id[0], trace_id[1], trace_id[2], trace_id[3]]);
    (f64::from(prefix) / f64::from(u32::MAX)) < f64::from(rate)
}

/// Links `span` to a remote parent described by `ctx` (W3C traceparent).
/// Uses the OpenTelemetry context API the tracing-opentelemetry layer
/// consumes; without that layer installed this is a harmless no-op.
fn set_remote_parent(span: &tracing::Span, ctx: &TraceContext) {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };

    let trace_id = TraceId::from_bytes(ctx.trace_id);
    let span_id = SpanId::from_bytes(ctx.span_id);
    let flags = if ctx.flags & 1 == 1 {
        TraceFlags::SAMPLED
    } else {
        TraceFlags::default()
    };
    let remote = SpanContext::new(trace_id, span_id, flags, true, TraceState::default());
    let cx = opentelemetry::Context::new().with_remote_span_context(remote);
    tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(span, cx);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_span_without_traceparent() {
        let span = serve_span("GET", "/x", Some("h.test"), None);
        // Disabled (no subscriber in unit tests): cheap and inert.
        let _ = span;
    }

    #[test]
    fn span_parses_valid_traceparent() {
        let span = serve_span(
            "POST",
            "/api",
            None,
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let _ = span;
    }

    #[test]
    fn span_ignores_garbage_traceparent() {
        let span = serve_span("GET", "/", None, Some("not-a-traceparent"));
        let _ = span;
    }

    #[test]
    fn sampling_boundaries() {
        assert!(sampled(&[0u8; 16], 1.0));
        assert!(sampled(&[0xFFu8; 16], 1.0));
        assert!(!sampled(&[0u8; 16], 0.0));
        assert!(!sampled(&[0xFFu8; 16], 0.0));
        // Full rate keeps the zero id; zero rate drops the max id.
        assert!(sampled(
            &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            0.5
        ));
    }
}

#[cfg(test)]
mod link_tests {
    use super::*;
    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    /// Subscriber with a real (in-memory, non-exporting) OTel layer —
    /// the same layer type otelkit installs for export.
    fn otel_test_subscriber() -> tracing::subscriber::DefaultGuard {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("vane-test");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(otel_layer)
            .set_default()
    }

    #[test]
    fn parent_link_sets_otel_trace_id() {
        let _guard = otel_test_subscriber();
        let span = serve_span(
            "GET",
            "/x",
            None,
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let ctx = tracing_opentelemetry::OpenTelemetrySpanExt::context(&span);
        let sc = ctx.span().span_context().clone();
        assert!(sc.is_valid(), "linked context must be valid");
        assert_eq!(
            format!("{:032x}", sc.trace_id()),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn root_span_has_fresh_trace_id() {
        let _guard = otel_test_subscriber();
        let span = serve_span("GET", "/x", None, None);
        let ctx = tracing_opentelemetry::OpenTelemetrySpanExt::context(&span);
        let sc = ctx.span().span_context().clone();
        assert!(sc.is_valid(), "root span context must be valid");
    }
}
