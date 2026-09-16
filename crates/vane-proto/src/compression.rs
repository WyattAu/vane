//! Response compression: streaming gzip for h1 downstream relay.
//!
//! [`GzipStream`] wraps flate2's deflate encoder: body chunks go in,
//! compressed bytes come out; [`GzipStream::finish`] flushes the
//! trailer. Helpers decide applicability (Accept-Encoding,
//! Content-Type) and frame output as HTTP/1.1 chunked bodies.

use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write as _;

/// Streaming gzip encoder (level 6 — the deflate sweet spot for
/// latency-sensitive relays).
pub struct GzipStream {
    inner: GzEncoder<Vec<u8>>,
}

impl GzipStream {
    /// New encoder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: GzEncoder::new(Vec::new(), Compression::new(6)),
        }
    }

    /// Feeds body bytes; returns newly compressed bytes (may be empty
    /// while the deflate window fills).
    /// # Errors
    /// Deflate write failures (practically infallible over a Vec).
    pub fn feed(&mut self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        self.inner.write_all(data)?;
        Ok(std::mem::take(self.inner.get_mut()))
    }

    /// Flushes the gzip trailer; returns the remaining bytes.
    /// # Errors
    /// Deflate finish failures (practically infallible over a Vec).
    pub fn finish(self) -> std::io::Result<Vec<u8>> {
        self.inner.finish()
    }
}

impl Default for GzipStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Frames `data` as one HTTP/1.1 chunk.
#[must_use]
pub fn chunk(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 18);
    out.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

/// The terminal chunk sequence.
#[must_use]
pub fn final_chunk() -> Vec<u8> {
    b"0\r\n\r\n".to_vec()
}

/// Whether the request's Accept-Encoding allows gzip (presence check;
/// q-values are not weighed — documented limitation).
#[must_use]
pub fn accepts_gzip(accept_encoding: Option<&[u8]>) -> bool {
    let Some(v) = accept_encoding else {
        return false;
    };
    for token in v.split(|b| *b == b',') {
        let token = trim_ascii(token);
        let lower = token.to_ascii_lowercase();
        if lower.starts_with(b"gzip") {
            // q=0 explicitly disables.
            let rest = &lower[4..];
            let q0 = rest.trim_ascii().strip_prefix(b";").is_some_and(|params| {
                params.iter().take_while(|b| **b != b',').count() > 0
                    && params.split(|b| *b == b';').any(|p| {
                        let p = trim_ascii(p);
                        p == b"q=0" || p == b"q=0.0"
                    })
            });
            if !q0 {
                return true;
            }
        }
    }
    false
}

fn trim_ascii(mut b: &[u8]) -> &[u8] {
    while let Some(f) = b.first() {
        if f.is_ascii_whitespace() {
            b = &b[1..];
        } else {
            break;
        }
    }
    while let Some(l) = b.last() {
        if l.is_ascii_whitespace() {
            b = &b[..b.len() - 1];
        } else {
            break;
        }
    }
    b
}

/// Whether a Content-Type is worth compressing (text-first set).
#[must_use]
pub fn is_compressible(content_type: Option<&[u8]>) -> bool {
    let Some(ct) = content_type else {
        return true; // unknown types: err on the side of compression
    };
    let lower = ct.to_ascii_lowercase();
    let base = lower.split(|b| *b == b';').next().unwrap_or(&lower);
    let base = trim_ascii(base);
    base.starts_with(b"text/")
        || matches!(
            base,
            b"application/json"
                | b"application/javascript"
                | b"application/xml"
                | b"application/xhtml+xml"
                | b"application/rss+xml"
                | b"application/wasm"
                | b"image/svg+xml"
        )
}

/// Extracts a header value from a response head (first match,
/// case-insensitive name check).
#[must_use]
pub fn head_header<'a>(head: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let split = head.windows(4).position(|w| w == b"\r\n\r\n")?;
    let (lines, _) = head.split_at(split);
    for (i, line) in lines.split(|b| *b == b'\n').enumerate() {
        if i == 0 {
            continue;
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let lower = line.to_ascii_lowercase();
        let Some(colon) = lower.iter().position(|b| *b == b':') else {
            continue;
        };
        if trim_ascii(&lower[..colon]) == name {
            return Some(trim_ascii(&line[colon + 1..]));
        }
    }
    None
}

/// Whether the head already carries an encoding (skip re-compression).
#[must_use]
pub fn head_already_encoded(head: &[u8]) -> bool {
    head_header(head, b"content-encoding").is_some()
}

/// Rewrites an upstream response head for gzip relay: drops
/// Content-Length, adds Content-Encoding + Transfer-Encoding: chunked.
/// Returns `None` when the head is unparseable.
#[must_use]
pub fn rewrite_head_for_gzip(head: &[u8]) -> Option<Vec<u8>> {
    let split = head.windows(4).position(|w| w == b"\r\n\r\n")?;
    let (lines, _) = head.split_at(split);
    let mut out = Vec::with_capacity(head.len() + 64);
    for (i, line) in lines.split(|b| *b == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if i == 0 {
            // Status line.
            out.extend_from_slice(line);
            out.extend_from_slice(b"\r\n");
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with(b"content-length") || lower.starts_with(b"transfer-encoding") {
            continue;
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Content-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n");
    Some(out)
}

/// h2 variant of [`rewrite_head_for_gzip`]: strips Content-Length and
/// Transfer-Encoding (h2 bodies delimit via END_STREAM — no chunked
/// framing) and appends the gzip content-encoding.
#[must_use]
pub fn rewrite_head_for_gzip_h2(head: &[u8]) -> Option<Vec<u8>> {
    let split = head.windows(4).position(|w| w == b"\r\n\r\n")?;
    let (lines, _) = head.split_at(split);
    let mut out = Vec::with_capacity(head.len() + 40);
    for (i, line) in lines.split(|b| *b == b'\n').enumerate() {
        // Lines carry their original CR; strip it before re-appending
        // CRLF (a doubled CR breaks strict head parsers downstream).
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if i == 0 {
            out.extend_from_slice(line);
            out.extend_from_slice(b"\r\n");
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with(b"content-length") || lower.starts_with(b"transfer-encoding") {
            continue;
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"content-encoding: gzip\r\n\r\n");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The h2 variant strips CL/TE and appends the encoding without
    /// chunked framing.
    #[test]
    fn h2_head_rewrite_strips_framing_and_adds_encoding() {
        let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\n";
        let rewritten = rewrite_head_for_gzip_h2(head).expect("rewrite");
        let s = String::from_utf8_lossy(&rewritten);
        assert!(s.contains("content-encoding: gzip"), "{s}");
        assert!(!s.contains("content-length"), "{s}");
        assert!(!s.to_ascii_lowercase().contains("transfer-encoding"), "{s}");
    }

    fn roundtrip(data: &[u8]) -> Vec<u8> {
        let mut gz = GzipStream::new();
        let a = gz.feed(data).expect("feed");
        let b = gz.finish().expect("finish");
        let mut compressed = a;
        compressed.extend_from_slice(&b);
        let mut dec = flate2::read::GzDecoder::new(&compressed[..]);
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut dec, &mut out).expect("decode");
        out
    }

    #[test]
    fn gzip_roundtrip() {
        let data = b"hello compressible world ".repeat(100);
        assert_eq!(roundtrip(&data), data);
    }

    #[test]
    fn empty_body_finishes() {
        let gz = GzipStream::new();
        let out = gz.finish().expect("finish");
        // Valid gzip stream has a nontrivial header+trailer.
        assert!(out.len() > 10);
        let mut dec = flate2::read::GzDecoder::new(&out[..]);
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut dec, &mut decoded).expect("decode");
        assert!(decoded.is_empty());
    }

    #[test]
    fn chunk_framing() {
        assert_eq!(chunk(b"hi"), b"2\r\nhi\r\n".to_vec());
        assert_eq!(chunk(b""), b"0\r\n\r\n".to_vec());
        assert_eq!(final_chunk(), b"0\r\n\r\n".to_vec());
    }

    #[test]
    fn accept_encoding_parsing() {
        assert!(accepts_gzip(Some(b"gzip, br")));
        assert!(accepts_gzip(Some(b"deflate, gzip;q=1.0")));
        assert!(accepts_gzip(Some(b"GZIP")));
        assert!(!accepts_gzip(Some(b"gzip;q=0")));
        assert!(!accepts_gzip(Some(b"br, zstd")));
        assert!(!accepts_gzip(None));
    }

    #[test]
    fn compressible_types() {
        assert!(is_compressible(Some(b"application/json")));
        assert!(is_compressible(Some(b"text/html; charset=utf-8")));
        assert!(is_compressible(None));
        assert!(!is_compressible(Some(b"image/png")));
        assert!(!is_compressible(Some(b"video/mp4")));
    }

    #[test]
    fn head_rewrite() {
        let head =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1234\r\nX-Keep: y\r\n\r\n";
        let out = rewrite_head_for_gzip(head).expect("rewrite");
        let text = String::from_utf8(out).expect("utf8");
        assert!(text.contains("Content-Encoding: gzip"));
        assert!(text.contains("Transfer-Encoding: chunked"));
        assert!(!text.contains("Content-Length"));
        assert!(text.contains("X-Keep: y"));
        assert!(text.ends_with("\r\n\r\n"));
    }
}
