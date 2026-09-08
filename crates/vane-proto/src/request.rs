//! Borrowed request views over the fixed read buffer.
//!
//! [`RequestView::parse`] runs `httparse` across the accumulated buffer and
//! returns the *byte length of the head* plus borrowed views of every part.
//! Nothing is copied; the view borrows the caller's buffer for `'h`.

use httparse::{Header, Status as HpStatus};

/// Parse outcome.
#[derive(Debug)]
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
}

/// Borrowed view of a parsed request head.
#[derive(Debug, Clone, Copy)]
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
                Parsed::Complete(
                    RequestView {
                        method,
                        path,
                        version,
                        headers: req.headers,
                    },
                    len,
                )
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
}
