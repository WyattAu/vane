//! W3C Trace Context (`traceparent`) generation and propagation (`OB-02`).
//!
//! Format: `00-<32 hex trace-id>-<16 hex parent-id>-<2 hex flags>`.
//! Parsing and formatting are allocation-free over fixed-size buffers;
//! generation uses a `rand`-backed PRNG seeded per thread.

use rand::Rng;

/// A parsed W3C trace context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// 16-byte trace id (never all-zero).
    pub trace_id: [u8; 16],
    /// 8-byte span id for the current span.
    pub span_id: [u8; 8],
    /// Trace flags (bit 0 = sampled).
    pub flags: u8,
}

const HEX: &[u8; 16] = b"0123456789abcdef";

impl TraceContext {
    /// Generates a fresh root context (new trace id, sampled).
    #[must_use]
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        rng.fill(&mut trace_id);
        rng.fill(&mut span_id);
        // Version/uuid-ish variant bits to look sane in UIs.
        trace_id[6] = (trace_id[6] & 0x0f) | 0x40;
        trace_id[8] = (trace_id[8] & 0x3f) | 0x80;
        Self {
            trace_id,
            span_id,
            flags: 0x01,
        }
    }

    /// Derives a child context (same trace id, new span id).
    #[must_use]
    pub fn child(&self) -> Self {
        let mut rng = rand::rng();
        let mut span_id = [0u8; 8];
        rng.fill(&mut span_id);
        Self {
            trace_id: self.trace_id,
            span_id,
            flags: self.flags,
        }
    }

    /// Parses an inbound `traceparent` header value.
    #[must_use]
    pub fn parse(value: &[u8]) -> Option<Self> {
        // "00-" + 32 + "-" + 16 + "-" + 2 = 55 bytes.
        if value.len() != 55 || &value[..3] != b"00-" {
            return None;
        }
        let hex_id = |bytes: &[u8]| -> Option<[u8; 16]> {
            let mut out = [0u8; 16];
            for (i, chunk) in bytes.chunks(2).enumerate() {
                out[i] = (hex_val(chunk[0])? << 4) | hex_val(chunk[1])?;
            }
            Some(out)
        };
        let trace_id = hex_id(&value[3..35])?;
        if trace_id == [0u8; 16] {
            return None;
        }
        let mut span_id = [0u8; 8];
        for (i, chunk) in value[36..52].chunks(2).enumerate() {
            span_id[i] = (hex_val(chunk[0])? << 4) | hex_val(chunk[1])?;
        }
        if span_id == [0u8; 8] {
            return None;
        }
        let flags = (hex_val(value[53])? << 4) | hex_val(value[54])?;
        Some(Self {
            trace_id,
            span_id,
            flags,
        })
    }

    /// Renders the context into a `traceparent` header value (55 bytes).
    /// # Panics
    /// Never; `out` is statically 55 bytes.
    pub fn format_into(&self, out: &mut [u8; 55]) {
        out[..3].copy_from_slice(b"00-");
        let mut w = 3usize;
        for b in self.trace_id {
            out[w] = HEX[usize::from(b >> 4)];
            out[w + 1] = HEX[usize::from(b & 0x0f)];
            w += 2;
        }
        out[w] = b'-';
        w += 1;
        for b in self.span_id {
            out[w] = HEX[usize::from(b >> 4)];
            out[w + 1] = HEX[usize::from(b & 0x0f)];
            w += 2;
        }
        out[w] = b'-';
        w += 1;
        out[w] = HEX[usize::from(self.flags >> 4)];
        out[w + 1] = HEX[usize::from(self.flags & 0x0f)];
    }

    /// Renders to a freshly allocated string (control plane / tests).
    #[must_use]
    pub fn to_header(&self) -> String {
        let mut buf = [0u8; 55];
        self.format_into(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let ctx = TraceContext::generate();
        let hdr = ctx.to_header();
        assert_eq!(hdr.len(), 55);
        assert!(hdr.starts_with("00-"));
        let parsed = TraceContext::parse(hdr.as_bytes()).expect("parses");
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.span_id, ctx.span_id);
        assert_eq!(parsed.flags, ctx.flags);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(TraceContext::parse(b"").is_none());
        assert!(
            TraceContext::parse(b"00-00000000000000000000000000000000-1234567890abcdef-01")
                .is_none()
        );
        let mut bad = *b"00-463ac35c9f6413ad48b85f9d3224fe31-00f067aa0ba902b7-xy";
        assert!(TraceContext::parse(&bad).is_none());
        bad[53] = b'0';
        bad[54] = b'1';
        assert!(TraceContext::parse(&bad).is_some());
    }

    #[test]
    fn child_keeps_trace() {
        let root = TraceContext::generate();
        let child = root.child();
        assert_eq!(root.trace_id, child.trace_id);
        assert_ne!(root.span_id, child.span_id);
    }
}
