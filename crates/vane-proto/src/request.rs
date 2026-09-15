//! Borrowed request views over the fixed read buffer.
//!
//! [`RequestView::parse`] runs `httparse` across the accumulated buffer and
//! returns the *byte length of the head* plus borrowed views of every part.
//! Nothing is copied; the view borrows the caller's buffer for `'h`.

use httparse::{Header, Status as HpStatus};

/// Parse outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parsed<'h> {
    /// Complete head: view + total head byte length (incl. CRLFCRLF).
    Complete(RequestView<'h>, usize),
    /// Need more bytes.
    Partial,
    /// Malformed request.
    Error(ParseError),
}

/// Parse failures worth surfacing to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// Header block exceeded the buffer.
    #[error("headers too large")]
    TooLarge,
    /// Not HTTP semantics we speak.
    #[error("malformed request")]
    Malformed,
    /// Framing headers are ambiguous or contradictory (duplicate or
    /// divergent `Content-Length`, `Content-Length` + `Transfer-Encoding`
    /// together, multiple `Transfer-Encoding` headers, or a transfer
    /// coding that does not end in `chunked`). Classic request-smuggling
    /// vectors: the request is rejected instead of guessed at.
    #[error("conflicting framing headers")]
    ConflictingFraming,
}

/// Borrowed view of a parsed request head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestView<'h> {
    /// Request method (`GET`, `POST`, ...).
    pub method: &'h str,
    /// Request target (`/path?query`) — as received, undecoded.
    pub path: &'h str,
    /// HTTP major version (0 = 1.0, 1 = 1.1).
    pub version: u8,
    /// Borrowed header list (backed by the caller's storage array).
    pub headers: &'h [Header<'h>],
}

/// Capacity of the caller-provided header storage.
pub const MAX_HEADERS: usize = 64;

impl<'h> RequestView<'h> {
    /// Parses a request head from `buf` using caller-provided `storage`.
    ///
    /// The storage array is worker-local and reused per parse; the returned
    /// view borrows `buf` for header *values* and `storage` for the header
    /// list, and is valid only for the duration of the handler call —
    /// exactly the window the pipeline needs.
    #[must_use]
    pub fn parse_in(buf: &'h [u8], storage: &'h mut [Header<'h>; MAX_HEADERS]) -> Parsed<'h> {
        let mut req = httparse::Request::new(storage.as_mut());
        match req.parse(buf) {
            Ok(HpStatus::Complete(len)) => {
                let Some(method) = req.method else {
                    return Parsed::Error(ParseError::Malformed);
                };
                let Some(path) = req.path else {
                    return Parsed::Error(ParseError::Malformed);
                };
                let version = req.version.unwrap_or(0);
                if version > 1 {
                    return Parsed::Error(ParseError::Malformed);
                }
                let view = RequestView {
                    method,
                    path,
                    version,
                    headers: req.headers,
                };
                if framing_is_ambiguous(&view) {
                    return Parsed::Error(ParseError::ConflictingFraming);
                }
                Parsed::Complete(view, len)
            }
            Ok(HpStatus::Partial) => {
                if buf.len() >= MAX_HEAD_BYTES {
                    Parsed::Error(ParseError::TooLarge)
                } else {
                    Parsed::Partial
                }
            }
            Err(_) => Parsed::Error(ParseError::Malformed),
        }
    }

    /// Case-insensitive header lookup (first match wins).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&'h [u8]> {
        self.headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(name))
            .map(|h| h.value)
    }

    /// Parses `Content-Length` (`None` when absent; `Some(None)` malformed).
    #[must_use]
    pub fn content_length(&self) -> Option<Option<u64>> {
        self.header("content-length").map(|v| {
            std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
    }

    /// `true` when the request declares chunked transfer coding.
    #[must_use]
    pub fn is_chunked(&self) -> bool {
        self.header("transfer-encoding")
            .is_some_and(|v| v.to_ascii_lowercase().windows(7).any(|w| w == b"chunked"))
    }

    /// `Connection` semantics: HTTP/1.0 defaults to close unless
    /// `keep-alive`; HTTP/1.1 defaults to keep-alive unless `close`.
    #[must_use]
    pub fn wants_close(&self) -> bool {
        let conn = self
            .header("connection")
            .map(|v| v.to_ascii_lowercase())
            .unwrap_or_default();
        if self.version == 0 {
            !conn
                .windows(10)
                .any(|w| w.eq_ignore_ascii_case(b"keep-alive"))
        } else {
            conn.windows(5).any(|w| w.eq_ignore_ascii_case(b"close"))
        }
    }

    /// Whether the request carries a body.
    #[must_use]
    pub fn has_body(&self) -> bool {
        if self.is_chunked() {
            return true;
        }
        matches!(self.content_length(), Some(Some(n)) if n > 0)
    }

    /// Body bytes that arrived together with the head.
    #[must_use]
    pub fn inline_body<'b>(&self, buf: &'b [u8], head_len: usize) -> &'b [u8]
    where
        'h: 'b,
    {
        &buf[head_len.min(buf.len())..]
    }
}

/// Request-smuggling guard (RFC 9112 §6): the framing headers must name
/// exactly one body-framing mechanism, unambiguously.
///
/// Rejects:
/// - any `Content-Length` together with any `Transfer-Encoding`
/// - duplicate `Content-Length` headers (even self-consistent values —
///   frontends and backends disagree on first-vs-last, which is the
///   smuggling primitive)
/// - multiple `Transfer-Encoding` headers
/// - a transfer coding list whose final coding is not `chunked`
///   (chunked must be applied last; anything else is not a framing we
///   can relay faithfully)
fn framing_is_ambiguous(view: &RequestView<'_>) -> bool {
    let mut content_lengths = 0usize;
    let mut transfer_encodings = 0usize;
    let mut te_value: Option<&[u8]> = None;
    for h in view.headers {
        let lname = h.name.to_ascii_lowercase();
        match lname.as_str() {
            "content-length" => content_lengths += 1,
            "transfer-encoding" => {
                transfer_encodings += 1;
                te_value = Some(h.value);
            }
            _ => {}
        }
    }
    if content_lengths > 1 || transfer_encodings > 1 {
        return true;
    }
    if content_lengths > 0 && transfer_encodings > 0 {
        return true;
    }
    match te_value {
        Some(v) => !ends_with_chunked(v),
        None => false,
    }
}

/// `true` when the (single) `Transfer-Encoding` value's final coding is
/// `chunked` (case-insensitive, comma-separated list, e.g. `gzip, chunked`).
fn ends_with_chunked(value: &[u8]) -> bool {
    let lowered = value.to_ascii_lowercase();
    let last = match lowered.iter().rposition(|&b| b == b',') {
        Some(pos) => &lowered[pos + 1..],
        None => &lowered[..],
    };
    let last = last.trim_ascii();
    last == b"chunked"
}

/// Maximum accepted head size.
pub const MAX_HEAD_BYTES: usize = 32 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    const GET: &[u8] = b"GET /foo?bar=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: t\r\n\r\n";

    #[test]
    fn parses_complete_get() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(GET, &mut storage) {
            Parsed::Complete(v, len) => {
                assert_eq!(v.method, "GET");
                assert_eq!(v.path, "/foo?bar=1");
                assert_eq!(v.version, 1);
                assert_eq!(len, GET.len());
                assert_eq!(v.header("host"), Some(&b"example.com"[..]));
                assert_eq!(v.header("HOST"), Some(&b"example.com"[..]));
                assert!(!v.has_body());
                assert!(!v.wants_close());
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn partial_then_complete() {
        // Accumulate manually (the worker does the same across reads).
        let mut acc = Vec::new();
        acc.extend_from_slice(&GET[..20]);
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert!(matches!(
            RequestView::parse_in(&acc, &mut storage),
            Parsed::Partial
        ));
        acc.extend_from_slice(&GET[20..]);
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(&acc, &mut storage) {
            Parsed::Complete(v, len) => {
                assert_eq!(v.path, "/foo?bar=1");
                assert_eq!(len, GET.len());
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn content_length_and_chunked() {
        let with_len = b"POST /x HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(with_len, &mut storage) {
            Parsed::Complete(v, len) => {
                assert_eq!(v.content_length(), Some(Some(5)));
                assert!(v.has_body());
                assert_eq!(v.inline_body(with_len, len), b"hello");
            }
            _ => panic!("complete expected"),
        }
        let chunked =
            b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(chunked, &mut storage) {
            Parsed::Complete(v, _) => assert!(v.is_chunked()),
            _ => panic!("complete expected"),
        }
    }

    #[test]
    fn connection_semantics() {
        let h10 = b"GET / HTTP/1.0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(h10, &mut storage) {
            Parsed::Complete(v, _) => assert!(v.wants_close()),
            _ => panic!(),
        }
        let h10ka = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(h10ka, &mut storage) {
            Parsed::Complete(v, _) => assert!(!v.wants_close()),
            _ => panic!(),
        }
        let h11close = b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(h11close, &mut storage) {
            Parsed::Complete(v, _) => assert!(v.wants_close()),
            _ => panic!(),
        }
    }

    #[test]
    fn garbage_is_error() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert!(matches!(
            RequestView::parse_in(b"NOT-HTTP\r\n\r\n\r\n", &mut storage),
            Parsed::Error(_)
        ));
    }

    // ---- Request-smuggling guards ------------------------------------
    // (Parsed borrows the caller's storage array, so each test declares
    // its own — same pattern as the tests above.)

    #[test]
    fn cl_plus_te_is_rejected() {
        // CL + TE: the classic smuggling ambiguity (RFC 9112 §6.1).
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn te_plus_cl_is_rejected_regardless_of_order() {
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn duplicate_content_length_divergent_is_rejected() {
        let raw =
            b"POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 11\r\n\r\nhello";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn duplicate_content_length_identical_is_rejected() {
        // Even self-consistent duplicates are refused: implementations
        // disagree on first-vs-last, which is the smuggling primitive.
        let raw =
            b"POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn multiple_transfer_encoding_headers_are_rejected() {
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn transfer_encoding_not_ending_in_chunked_is_rejected() {
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }

    #[test]
    fn chunked_with_prefix_codings_is_accepted() {
        let raw =
            b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(raw, &mut storage) {
            Parsed::Complete(v, _) => assert!(v.is_chunked()),
            other => panic!("expected complete, got {other:?}"),
        }
    }

    #[test]
    fn single_content_length_still_accepted() {
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(raw, &mut storage) {
            Parsed::Complete(v, _) => assert_eq!(v.content_length(), Some(Some(5))),
            other => panic!("expected complete, got {other:?}"),
        }
    }

    #[test]
    fn no_framing_headers_still_accepted() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        match RequestView::parse_in(GET, &mut storage) {
            Parsed::Complete(v, _) => assert!(!v.has_body()),
            other => panic!("expected complete, got {other:?}"),
        }
    }

    #[test]
    fn case_insensitive_framing_header_names_are_caught() {
        // Obfuscated casing must not slip past the guard.
        let raw = b"POST /x HTTP/1.1\r\nHost: h\r\ncontent-length: 5\r\ntRaNsFeR-eNcOdInG: chunked\r\n\r\n0\r\n\r\n";
        let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
        assert_eq!(
            RequestView::parse_in(raw, &mut storage),
            Parsed::Error(ParseError::ConflictingFraming)
        );
    }
}
