//! Per-route CORS enforcement at the edge.
//!
//! Two shapes of work, both driven by the route's [`CorsPolicy`]:
//!
//! * **Preflight** — `OPTIONS` carrying `Access-Control-Request-Method`.
//!   Answered at the edge; the upstream is never dialed. The route's
//!   method allowlist does not apply (see `handle_request`): a preflight
//!   carries no credentials, and the browser will not send the actual
//!   request unless the preflight succeeded.
//! * **Actual** — the request is proxied normally and the CORS response
//!   headers are injected into the upstream response head on the way out.
//!
//! A request with no `Origin`, or an origin outside the policy, is
//! proxied **unchanged**: the browser enforces the failure, so vane does
//! not need to. That keeps the common non-CORS path allocation-free —
//! this module allocates only when a policy is configured *and* the
//! origin is allowed.
//!
//! Spec references: Fetch/CORS §4.8 (preflight), §5.3 (actual response),
//! RFC 9110 §10.3.7 (wildcard + credentials are mutually exclusive).

use vane_router::CorsPolicy;

/// What the proxy should do with a request under its route's policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsAction {
    /// Not a CORS request under this policy: relay unchanged.
    Pass,
    /// Answer at the edge with these headers; never reach the upstream.
    Preflight(Vec<(&'static str, String)>),
    /// Proxy normally, then inject these headers into the response head.
    Actual(Vec<(&'static str, String)>),
}

const ACAO: &str = "Access-Control-Allow-Origin";
const ACAM: &str = "Access-Control-Allow-Methods";
const ACAH: &str = "Access-Control-Allow-Headers";
const ACEH: &str = "Access-Control-Expose-Headers";
const ACAC: &str = "Access-Control-Allow-Credentials";
const ACMA: &str = "Access-Control-Max-Age";

/// True when `origin` is in the policy's allowlist (`"*"` matches any).
fn origin_allowed(policy: &CorsPolicy, origin: &str) -> bool {
    policy
        .allow_origins
        .iter()
        .any(|o| o == "*" || o.eq_ignore_ascii_case(origin))
}

/// The `Access-Control-Allow-Origin` value to emit for `origin`.
///
/// The wildcard is invalid alongside credentials (fetch spec: a
/// credentialed request must name the origin), so a credentialed policy
/// always echoes the origin back even when `*` is allowed.
fn allow_origin_value(policy: &CorsPolicy, origin: &str) -> String {
    if !policy.allow_credentials && policy.allow_origins.iter().any(|o| o == "*") {
        "*".to_owned()
    } else {
        origin.to_owned()
    }
}

/// The methods a preflight may request: the explicit list, else the
/// route's own method allowlist, else the method being requested.
fn methods_allowed(policy: &CorsPolicy, route_methods: &[String], requested: &str) -> bool {
    match &policy.allow_methods {
        Some(list) => list.iter().any(|m| m.eq_ignore_ascii_case(requested)),
        None => {
            route_methods.is_empty()
                || route_methods
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(requested))
        }
    }
}

/// The `Access-Control-Allow-Methods` value: explicit list, else the
/// route's methods, else reflect the requested method.
fn allow_methods_value(policy: &CorsPolicy, route_methods: &[String], requested: &str) -> String {
    if let Some(list) = &policy.allow_methods {
        return list.join(", ");
    }
    if route_methods.is_empty() {
        return requested.to_owned();
    }
    route_methods.join(", ")
}

/// True when every header in the preflight's `Access-Control-Request-
/// Headers` is allowed. An unset policy allows any request header.
fn headers_allowed(policy: &CorsPolicy, requested: &str) -> bool {
    let Some(list) = &policy.allow_headers else {
        return true;
    };
    requested
        .split(',')
        .map(str::trim)
        .all(|h| h.is_empty() || list.iter().any(|a| a.eq_ignore_ascii_case(h) || a == "*"))
}

/// The `Access-Control-Allow-Headers` value: explicit list, else reflect
/// the requested headers.
fn allow_headers_value(policy: &CorsPolicy, requested: Option<&str>) -> Option<String> {
    if let Some(list) = &policy.allow_headers {
        return if list.is_empty() {
            None
        } else {
            Some(list.join(", "))
        };
    }
    let requested = requested?.trim();
    if requested.is_empty() {
        None
    } else {
        Some(requested.to_owned())
    }
}

/// Evaluates a request against a route's CORS policy.
///
/// `route_methods` is the matched route's method allowlist, used as the
/// default preflight method set when the policy does not name one.
/// `requested_method` / `requested_headers` come from the
/// `Access-Control-Request-*` preflight headers.
#[must_use]
pub fn evaluate(
    policy: &CorsPolicy,
    route_methods: &[String],
    method: &str,
    origin: Option<&str>,
    requested_method: Option<&str>,
    requested_headers: Option<&str>,
) -> CorsAction {
    // No Origin: same-origin request, or not a browser fetch. Nothing to
    // enforce and nothing to advertise.
    let Some(origin) = origin else {
        return CorsAction::Pass;
    };
    // Origin outside the policy: relay untouched so the browser blocks.
    // Answering here with a synthetic response would leak policy shape
    // and change cache behavior for non-browser clients.
    if !origin_allowed(policy, origin) {
        return CorsAction::Pass;
    }

    if method == "OPTIONS" {
        if let Some(req_method) = requested_method {
            if !methods_allowed(policy, route_methods, req_method.trim()) {
                return CorsAction::Pass;
            }
            if let Some(h) = requested_headers {
                if !headers_allowed(policy, h) {
                    return CorsAction::Pass;
                }
            }
            let mut out = Vec::with_capacity(6);
            out.push((ACAO, allow_origin_value(policy, origin)));
            out.push((
                ACAM,
                allow_methods_value(policy, route_methods, req_method.trim()),
            ));
            if let Some(v) = allow_headers_value(policy, requested_headers) {
                out.push((ACAH, v));
            }
            if policy.allow_credentials {
                out.push((ACAC, "true".to_owned()));
            }
            out.push((ACMA, policy.max_age_secs.to_string()));
            // Caches must key on the preflight's request headers too,
            // not just the Origin.
            out.push((
                "Vary",
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers".to_owned(),
            ));
            return CorsAction::Preflight(out);
        }
        // OPTIONS without Access-Control-Request-Method is an ordinary
        // request: proxy it and attach the actual-response headers.
    }

    let mut out = Vec::with_capacity(4);
    out.push((ACAO, allow_origin_value(policy, origin)));
    if let Some(list) = &policy.expose_headers {
        if !list.is_empty() {
            out.push((ACEH, list.join(", ")));
        }
    }
    if policy.allow_credentials {
        out.push((ACAC, "true".to_owned()));
    }
    // Shared caches would otherwise serve one origin's header to another.
    out.push(("Vary", "Origin".to_owned()));
    CorsAction::Actual(out)
}

/// Byte range of an existing `Vary:` line in `head` — name, value and
/// CRLF — so the value can be rewritten in place (read-modify-write
/// rather than a second `Vary` line, which caches treat as last-wins).
fn vary_line_span(head: &[u8]) -> Option<(usize, usize)> {
    let mut start = 0usize;
    while let Some(nl) = head[start..].iter().position(|b| *b == b'\n') {
        let line_len = if nl > 0 && head[start + nl - 1] == b'\r' {
            nl - 1
        } else {
            nl
        };
        let line = &head[start..start + line_len];
        // An empty line is the end of the header block.
        if line.is_empty() {
            return None;
        }
        if let Some(colon) = line.iter().position(|b| *b == b':') {
            if line[..colon].eq_ignore_ascii_case(b"vary") {
                return Some((start, start + nl + 1));
            }
        }
        start += nl + 1;
    }
    None
}

/// Merges our `Vary` values into the upstream's, case-insensitively and
/// without duplicates. `Vary: *` from the upstream subsumes ours.
fn merge_vary(existing: &str, ours: &str) -> String {
    let existing = existing.trim();
    if existing.split(',').map(str::trim).any(|h| h == "*") {
        return existing.to_owned();
    }
    let mut acc = existing.to_owned();
    for want in ours.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let present = existing
            .split(',')
            .map(str::trim)
            .any(|have| have.eq_ignore_ascii_case(want));
        if !present {
            if !acc.is_empty() {
                acc.push_str(", ");
            }
            acc.push_str(want);
        }
    }
    acc
}

/// Injects CORS response headers into an upstream response head in place.
///
/// `Vary` is merged rather than duplicated: an upstream `Vary: Accept-
/// Encoding` becomes `Vary: Accept-Encoding, Origin`. A head without a
/// terminator (a partial read that has not yet reached `\r\n\r\n`) is
/// left untouched — the caller re-evaluates once the whole head arrives.
///
/// This does not strip upstream `Access-Control-*` headers: a backend
/// that sets its own CORS policy keeps it, and vane's header goes after
/// it. When both name an origin the browser reads the first.
pub fn inject_into_head(head: &mut Vec<u8>, headers: &[(&'static str, String)]) {
    if headers.is_empty() {
        return;
    }
    // Insert before the head terminator; bail on a head that has none.
    let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") else {
        return;
    };

    // `Vary` needs a read-modify-write against the upstream's own value,
    // so it is held back from the plain append.
    let mut vary: Option<&str> = None;
    let mut insert: Vec<u8> = Vec::with_capacity(headers.len() * 48);
    for (name, value) in headers {
        if *name == "Vary" {
            vary = Some(value.as_str());
            continue;
        }
        insert.extend_from_slice(name.as_bytes());
        insert.extend_from_slice(b": ");
        insert.extend_from_slice(value.as_bytes());
        insert.extend_from_slice(b"\r\n");
    }
    let vary_rewrite = vary.map(|ours| {
        let merged = match vane_proto::compression::head_header(head, b"vary") {
            Some(existing) => merge_vary(&String::from_utf8_lossy(existing), ours),
            None => ours.to_owned(),
        };
        // Read the span off the pre-splice head: inserting at `end` moves
        // no index below it, so the span stays valid.
        (merged, vary_line_span(head))
    });
    if let Some((merged, None)) = &vary_rewrite {
        insert.extend_from_slice(b"Vary: ");
        insert.extend_from_slice(merged.as_bytes());
        insert.extend_from_slice(b"\r\n");
    }
    head.splice(end..end, insert);
    // Rewrite the upstream's own Vary line in place, preserving its name
    // bytes and field order.
    if let Some((merged, Some((s, e)))) = vary_rewrite {
        if let Some(rel) = head[s..e].iter().position(|b| *b == b':') {
            let colon = s + rel;
            let mut line = Vec::with_capacity(16 + merged.len());
            line.extend_from_slice(&head[s..=colon]);
            line.push(b' ');
            line.extend_from_slice(merged.as_bytes());
            line.extend_from_slice(b"\r\n");
            head.splice(s..e, line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(origins: &[&str]) -> CorsPolicy {
        CorsPolicy {
            allow_origins: origins.iter().map(|s| (*s).to_owned()).collect(),
            ..CorsPolicy::default()
        }
    }

    #[test]
    fn no_origin_passes() {
        let p = policy(&["https://a.example"]);
        assert_eq!(evaluate(&p, &[], "GET", None, None, None), CorsAction::Pass);
    }

    #[test]
    fn disallowed_origin_passes() {
        let p = policy(&["https://a.example"]);
        assert_eq!(
            evaluate(&p, &[], "GET", Some("https://evil.example"), None, None),
            CorsAction::Pass
        );
    }

    #[test]
    fn allowed_origin_gets_actual_headers() {
        let p = policy(&["https://a.example"]);
        let CorsAction::Actual(h) = evaluate(&p, &[], "GET", Some("https://a.example"), None, None)
        else {
            panic!("expected actual");
        };
        assert_eq!(h[0], (ACAO, "https://a.example".to_owned()));
        assert!(h.contains(&("Vary", "Origin".to_owned())));
    }

    #[test]
    fn wildcard_emits_wildcard() {
        let p = policy(&["*"]);
        let CorsAction::Actual(h) = evaluate(&p, &[], "GET", Some("https://x"), None, None) else {
            panic!("expected actual");
        };
        assert_eq!(h[0], (ACAO, "*".to_owned()));
    }

    #[test]
    fn wildcard_with_credentials_echoes_origin() {
        let p = CorsPolicy {
            allow_origins: vec!["*".into()],
            allow_credentials: true,
            ..CorsPolicy::default()
        };
        let CorsAction::Actual(h) = evaluate(&p, &[], "GET", Some("https://x"), None, None) else {
            panic!("expected actual");
        };
        // `*` is invalid with credentials; the origin must be named.
        assert_eq!(h[0], (ACAO, "https://x".to_owned()));
        assert!(h.contains(&(ACAC, "true".to_owned())));
    }

    #[test]
    fn preflight_answered_at_edge() {
        let p = policy(&["https://a.example"]);
        let CorsAction::Preflight(h) = evaluate(
            &p,
            &[],
            "OPTIONS",
            Some("https://a.example"),
            Some("POST"),
            Some("content-type"),
        ) else {
            panic!("expected preflight");
        };
        assert_eq!(h[0], (ACAO, "https://a.example".to_owned()));
        assert_eq!(h[1], (ACAM, "POST".to_owned()));
        // No allow_headers policy: reflect what the browser asked for.
        assert!(h.contains(&(ACAH, "content-type".to_owned())));
        assert!(h.iter().any(|(n, v)| *n == ACMA && v == "86400"));
        assert!(h.iter().any(|(n, _)| *n == "Vary"));
    }

    #[test]
    fn preflight_reflects_route_methods_by_default() {
        let p = policy(&["*"]);
        let route_methods = vec!["GET".to_owned(), "POST".to_owned()];
        let CorsAction::Preflight(h) = evaluate(
            &p,
            &route_methods,
            "OPTIONS",
            Some("https://x"),
            Some("GET"),
            None,
        ) else {
            panic!("expected preflight");
        };
        assert_eq!(h[1], (ACAM, "GET, POST".to_owned()));
    }

    #[test]
    fn preflight_blocks_method_outside_policy() {
        let p = CorsPolicy {
            allow_origins: vec!["*".into()],
            allow_methods: Some(vec!["GET".into()]),
            ..CorsPolicy::default()
        };
        assert_eq!(
            evaluate(&p, &[], "OPTIONS", Some("https://x"), Some("DELETE"), None),
            CorsAction::Pass
        );
    }

    #[test]
    fn preflight_blocks_header_outside_policy() {
        let p = CorsPolicy {
            allow_origins: vec!["*".into()],
            allow_headers: Some(vec!["content-type".into()]),
            ..CorsPolicy::default()
        };
        assert_eq!(
            evaluate(
                &p,
                &[],
                "OPTIONS",
                Some("https://x"),
                Some("GET"),
                Some("content-type, x-secret")
            ),
            CorsAction::Pass
        );
        assert!(matches!(
            evaluate(
                &p,
                &[],
                "OPTIONS",
                Some("https://x"),
                Some("GET"),
                Some("content-type")
            ),
            CorsAction::Preflight(_)
        ));
    }

    #[test]
    fn options_without_request_method_is_an_actual_request() {
        let p = policy(&["*"]);
        assert!(matches!(
            evaluate(&p, &[], "OPTIONS", Some("https://x"), None, None),
            CorsAction::Actual(_)
        ));
    }

    #[test]
    fn expose_headers_emitted() {
        let p = CorsPolicy {
            allow_origins: vec!["*".into()],
            expose_headers: Some(vec!["X-Total".into(), "X-Page".into()]),
            ..CorsPolicy::default()
        };
        let CorsAction::Actual(h) = evaluate(&p, &[], "GET", Some("https://x"), None, None) else {
            panic!("expected actual");
        };
        assert!(h.contains(&(ACEH, "X-Total, X-Page".to_owned())));
    }

    fn head_of(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn inject_appends_headers() {
        let mut head = head_of("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
        let h = vec![(ACAO, "*".to_owned()), ("Vary", "Origin".to_owned())];
        inject_into_head(&mut head, &h);
        let s = String::from_utf8(head).expect("utf8");
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Access-Control-Allow-Origin: *\r\n"));
        assert!(s.contains("Vary: Origin\r\n"));
        assert!(s.ends_with("\r\n\r\nhi"));
    }

    #[test]
    fn inject_merges_existing_vary() {
        let mut head = head_of("HTTP/1.1 200 OK\r\nVary: Accept-Encoding\r\n\r\n");
        inject_into_head(&mut head, &[("Vary", "Origin".to_owned())]);
        let s = String::from_utf8(head).expect("utf8");
        // Exactly one Vary, merged, not duplicated.
        assert_eq!(s.matches("Vary:").count(), 1);
        assert!(s.contains("Vary: Accept-Encoding, Origin\r\n"), "{s}");
    }

    #[test]
    fn inject_dedupes_vary_repeat() {
        let mut head = head_of("HTTP/1.1 200 OK\r\nVary: Origin\r\n\r\n");
        inject_into_head(&mut head, &[("Vary", "Origin".to_owned())]);
        let s = String::from_utf8(head).expect("utf8");
        assert_eq!(s.matches("Origin").count(), 1, "{s}");
    }

    #[test]
    fn inject_wildcard_vary_absorbs_ours() {
        let mut head = head_of("HTTP/1.1 200 OK\r\nVary: *\r\n\r\n");
        inject_into_head(&mut head, &[("Vary", "Origin".to_owned())]);
        let s = String::from_utf8(head).expect("utf8");
        assert_eq!(s.matches("Vary:").count(), 1);
        assert!(s.contains("Vary: *\r\n"), "{s}");
    }

    #[test]
    fn inject_leaves_partial_head_untouched() {
        let mut head = head_of("HTTP/1.1 200 OK\r\nContent-Len");
        let before = head.clone();
        inject_into_head(&mut head, &[(ACAO, "*".to_owned())]);
        assert_eq!(head, before);
    }

    #[test]
    fn inject_is_a_noop_without_headers() {
        let mut head = head_of("HTTP/1.1 200 OK\r\n\r\n");
        let before = head.clone();
        inject_into_head(&mut head, &[]);
        assert_eq!(head, before);
    }
}
