//! Response serialization — straight into caller-provided buffers, no
//! allocation on the hot path.

use crate::date::DateCache;
use std::io;

/// Common status codes with their reason phrases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Status {
    /// 200 OK.
    Ok = 200,
    /// 201 Created.
    Created = 201,
    /// 204 No Content.
    NoContent = 204,
    /// 301 Moved Permanently.
    MovedPermanently = 301,
    /// 302 Found.
    Found = 302,
    /// 304 Not Modified.
    NotModified = 304,
    /// 400 Bad Request.
    BadRequest = 400,
    /// 401 Unauthorized.
    Unauthorized = 401,
    /// 403 Forbidden.
    Forbidden = 403,
    /// 404 Not Found.
    NotFound = 404,
    /// 405 Method Not Allowed.
    MethodNotAllowed = 405,
    /// 408 Request Timeout.
    RequestTimeout = 408,
    /// 413 Payload Too Large.
    PayloadTooLarge = 413,
    /// 429 Too Many Requests.
    TooManyRequests = 429,
    /// 500 Internal Server Error.
    InternalServerError = 500,
    /// 502 Bad Gateway.
    BadGateway = 502,
    /// 503 Service Unavailable.
    ServiceUnavailable = 503,
    /// 504 Gateway Timeout.
    GatewayTimeout = 504,
}

impl Status {
    /// Maps a numeric code onto the known set (fallback: 500).
    #[must_use]
    pub fn from_code(code: u16) -> Self {
        match code {
            200 => Self::Ok,
            201 => Self::Created,
            204 => Self::NoContent,
            301 => Self::MovedPermanently,
            302 => Self::Found,
            304 => Self::NotModified,
            400 => Self::BadRequest,
            401 => Self::Unauthorized,
            403 => Self::Forbidden,
            404 => Self::NotFound,
            405 => Self::MethodNotAllowed,
            408 => Self::RequestTimeout,
            413 => Self::PayloadTooLarge,
            429 => Self::TooManyRequests,
            500 => Self::InternalServerError,
            502 => Self::BadGateway,
            503 => Self::ServiceUnavailable,
            504 => Self::GatewayTimeout,
            _ => Self::InternalServerError,
        }
    }

    /// Numeric code.
    #[must_use]
    pub fn code(self) -> u16 {
        self as u16
    }

    /// Canonical reason phrase.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Created => "Created",
            Self::NoContent => "No Content",
            Self::MovedPermanently => "Moved Permanently",
            Self::Found => "Found",
            Self::NotModified => "Not Modified",
            Self::BadRequest => "Bad Request",
            Self::Unauthorized => "Unauthorized",
            Self::Forbidden => "Forbidden",
            Self::NotFound => "Not Found",
            Self::MethodNotAllowed => "Method Not Allowed",
            Self::RequestTimeout => "Request Timeout",
            Self::PayloadTooLarge => "Payload Too Large",
            Self::TooManyRequests => "Too Many Requests",
            Self::InternalServerError => "Internal Server Error",
            Self::BadGateway => "Bad Gateway",
            Self::ServiceUnavailable => "Service Unavailable",
            Self::GatewayTimeout => "Gateway Timeout",
        }
    }
}

/// Writes an HTTP/1.1 response head (status line, headers, CRLF) into `out`.
///
/// This is one `write` batch: format into the buffer, engine sends it.
/// `extra_headers` are raw `Name: value\r\n` lines already CRLF-terminated.
///
/// # Errors
/// Fails when the head exceeds `out` (buffers are fixed; a 4 KB head limit
/// is generous and enforced upstream anyway).
pub fn write_head(
    out: &mut [u8],
    status: Status,
    headers: &[(&str, &[u8])],
    date: &DateCache,
    content_length: Option<u64>,
) -> io::Result<usize> {
    let mut w = 0usize;
    let put = |src: &[u8], out: &mut [u8], w: &mut usize| -> io::Result<()> {
        if *w + src.len() > out.len() {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "head overflow"));
        }
        out[*w..*w + src.len()].copy_from_slice(src);
        *w += src.len();
        Ok(())
    };

    // Status line: "HTTP/1.1 200 OK\r\n"
    let code = status.code();
    let line = format!("HTTP/1.1 {} {}\r\n", code, status.reason());
    put(line.as_bytes(), out, &mut w)?;

    for (name, value) in headers {
        put(name.as_bytes(), out, &mut w)?;
        put(b": ", out, &mut w)?;
        put(value, out, &mut w)?;
        put(b"\r\n", out, &mut w)?;
    }
    put(date.line(), out, &mut w)?;
    if let Some(cl) = content_length {
        let cl_line = format!("Content-Length: {cl}\r\n");
        put(cl_line.as_bytes(), out, &mut w)?;
    }
    put(b"\r\n", out, &mut w)?;
    Ok(w)
}

/// Formats a complete minimal response (head + body) into `out`.
///
/// # Errors
/// Buffer overflow.
pub fn write_full(
    out: &mut [u8],
    status: Status,
    body: &[u8],
    extra: &[(&str, &[u8])],
    date: &DateCache,
) -> io::Result<usize> {
    let mut headers: [(&str, &[u8]); 8] = [("Server", b"vane" as &[u8]); 8];
    headers[0] = ("Server", b"vane" as &[u8]);
    let mut n_headers = 1usize;
    for (i, h) in extra.iter().enumerate().take(7) {
        headers[n_headers] = *h;
        n_headers += 1;
        let _ = i;
    }
    let head = write_head(
        out,
        status,
        &headers[..n_headers],
        date,
        Some(body.len() as u64),
    )?;
    if head + body.len() > out.len() {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "body overflow"));
    }
    out[head..head + body.len()].copy_from_slice(body);
    Ok(head + body.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_minimal_response() {
        let date = DateCache::new();
        let mut buf = [0u8; 512];
        let n = write_full(&mut buf, Status::NotFound, b"nope", &[], &date).expect("fits");
        let s = std::str::from_utf8(&buf[..n]).expect("utf8");
        assert!(s.starts_with("HTTP/1.1 404 Not Found\r\n"));
        assert!(s.contains("Server: vane\r\n"));
        assert!(s.contains("Content-Length: 4\r\n"));
        assert!(s.ends_with("\r\n\r\nnope"));
        assert!(s.contains("Date: "));
    }

    #[test]
    fn head_overflow_is_error() {
        let date = DateCache::new();
        let mut buf = [0u8; 32];
        assert!(write_full(&mut buf, Status::Ok, b"", &[], &date).is_err());
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;

    #[test]
    fn status_roundtrips_all_codes() {
        for code in [
            200, 201, 204, 301, 304, 400, 401, 403, 404, 405, 413, 500, 502, 503, 504,
        ] {
            let s = Status::from_code(code);
            assert_eq!(s.code(), code, "code {code}");
            assert!(!s.reason().is_empty());
        }
    }

    #[test]
    fn unknown_codes_map_to_500() {
        assert_eq!(Status::from_code(599).code(), 500);
        assert_eq!(Status::from_code(99).code(), 500);
        assert_eq!(Status::from_code(418).code(), 500);
    }

    #[test]
    fn write_head_with_extra_headers() {
        let date = DateCache::new();
        let mut buf = [0u8; 512];
        let n = write_head(
            &mut buf,
            Status::Ok,
            &[
                ("x-test", b"yes".as_slice()),
                ("connection", b"close".as_slice()),
            ],
            &date,
            Some(5),
        )
        .expect("fits");
        let s = std::str::from_utf8(&buf[..n]).expect("utf8");
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Content-Length: 5\r\n"));
        assert!(s.contains("x-test: yes\r\n"));
        assert!(s.ends_with("\r\n"));
    }
}

/// Maximum response headers parsed.
pub const MAX_RESPONSE_HEADERS: usize = 64;

/// Result of parsing an upstream response head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamHead {
    /// Bytes consumed by the head (including the blank line).
    pub head_len: usize,
    /// Numeric status code.
    pub code: u16,
    /// Declared Content-Length (`None` = absent/invalid).
    pub content_length: Option<u64>,
    /// `Transfer-Encoding: chunked` present.
    pub chunked: bool,
    /// `Connection: close` present.
    pub close: bool,
}

/// Parses an upstream HTTP/1.1 response head from `data`.
///
/// Returns `Ok(None)` when more bytes are needed (incomplete head) and
/// `Err(())` on malformed input. Never panics on arbitrary bytes — this
/// is the fuzzing boundary for all upstream response bytes.
///
/// # Errors
/// Malformed response head (httparse error).
pub fn parse_upstream_head<'a>(
    data: &'a [u8],
    storage: &mut [httparse::Header<'a>; MAX_RESPONSE_HEADERS],
) -> Result<Option<UpstreamHead>, httparse::Error> {
    let mut resp = httparse::Response::new(storage);
    match resp.parse(data) {
        Ok(httparse::Status::Complete(head_len)) => {
            let code = resp.code.unwrap_or(500);
            let mut content_length: Option<u64> = None;
            let mut chunked = false;
            let mut close = false;
            for h in resp.headers {
                let name = h.name.to_ascii_lowercase();
                if name == "content-length" {
                    content_length = std::str::from_utf8(h.value)
                        .ok()
                        .and_then(|s| s.trim().parse().ok());
                } else if name == "transfer-encoding"
                    && h.value
                        .to_ascii_lowercase()
                        .windows(7)
                        .any(|w| w == b"chunked")
                {
                    chunked = true;
                } else if name == "connection"
                    && h.value
                        .to_ascii_lowercase()
                        .windows(5)
                        .any(|w| w.eq_ignore_ascii_case(b"close"))
                {
                    close = true;
                }
            }
            Ok(Some(UpstreamHead {
                head_len,
                code,
                content_length,
                chunked,
                close,
            }))
        }
        Ok(httparse::Status::Partial) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod upstream_head_tests {
    use super::*;

    #[test]
    fn parses_complete_head() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let head = parse_upstream_head(
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nbody",
            &mut storage,
        )
        .expect("ok")
        .expect("complete");
        assert_eq!(head.code, 200);
        assert_eq!(head.content_length, Some(5));
        assert!(head.close);
        assert!(!head.chunked);
        assert!(head.head_len <= 80);
    }

    #[test]
    fn incomplete_head_is_none() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let head = parse_upstream_head(b"HTTP/1.1 200 OK\r\n", &mut storage).expect("ok");
        assert!(head.is_none());
    }

    #[test]
    fn malformed_head_is_error() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        assert!(parse_upstream_head(b"HTTP/1.1 garbage\r\n\r\n", &mut storage).is_err());
    }

    #[test]
    fn chunked_detected() {
        let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let head = parse_upstream_head(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            &mut storage,
        )
        .expect("ok")
        .expect("complete");
        assert!(head.chunked);
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        // Deterministic pseudo-fuzz: pseudo-random byte strings.
        let mut x: u64 = 0x1234_5678_9abc_def0;
        for len in 0..600u64 {
            let mut data = Vec::with_capacity(len as usize);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                data.push(x as u8);
            }
            let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
            let _ = parse_upstream_head(&data, &mut storage);
        }
    }
}
