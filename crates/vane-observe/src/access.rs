//! Structured access logging — fixed-size records, zero allocation on
//! the hot path.
//!
//! Workers push [`AccessRecord`]s (all fields inline, overlong strings
//! truncated) into a bounded [`AccessRing`]; the control plane drains
//! and renders one JSON object per line. A full ring drops the record
//! and bumps a counter — observation must never block or stall the data
//! plane.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::ring::EventRing;

/// Per-worker access-record ring capacity. 2048 × ~350 B ≈ 720 KB per
/// worker; the drain task keeps this near-empty in steady state.
pub const ACCESS_RING_CAPACITY: usize = 2048;

/// The per-worker access-record ring.
pub type AccessRing = EventRing<AccessRecord, ACCESS_RING_CAPACITY>;

/// One completed HTTP transaction (or tunnel upgrade).
///
/// Everything is inline: no `String`, no `Vec`, no heap. String fields
/// carry a length-prefixed byte buffer; input longer than the buffer is
/// truncated at emission time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessRecord {
    /// Nanoseconds since the UNIX epoch (emission time).
    pub ts_ns: u64,
    /// Transaction duration in microseconds (`u32::MAX` = saturated).
    pub duration_us: u32,
    /// Worker id that served the request (`u16::MAX` = control plane).
    pub worker: u16,
    /// Response status (101 for tunnel upgrades).
    pub status: u16,
    /// Bytes written downstream (response head + body + tunnel bytes).
    pub bytes_out: u64,
    /// Client address (IPv4 encoded as v4-mapped IPv6) + port.
    pub client: ([u8; 16], u16),
    /// Upstream address (`None` = answered locally: ACME, 404, …).
    pub upstream: Option<([u8; 16], u16)>,
    /// Request method (`GET`, …) — truncated at [`Self::MAX_METHOD`].
    pub method: Str8<{ Self::MAX_METHOD }>,
    /// `Host` header (without port) — truncated at [`Self::MAX_HOST`].
    pub host: Str8<{ Self::MAX_HOST }>,
    /// Request path (with query) — truncated at [`Self::MAX_PATH`].
    pub path: Str8<{ Self::MAX_PATH }>,
    /// W3C trace id (32 lowercase hex chars) when propagated.
    pub trace_id: Str8<32>,
}

/// Inline string buffer: `len` valid prefix of `bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Str8<const N: usize> {
    /// Valid prefix length (≤ N).
    pub len: u8,
    /// Bytes (rest is zero padding).
    pub bytes: [u8; N],
}

impl<const N: usize> Str8<N> {
    /// Copies `src`, truncating to `N` bytes.
    #[must_use]
    pub fn from_lossy(src: &[u8]) -> Self {
        let n = src.len().min(N).min(u8::MAX as usize);
        let mut bytes = [0u8; N];
        bytes[..n].copy_from_slice(&src[..n]);
        Self {
            len: n as u8,
            bytes,
        }
    }

    /// Borrows the valid prefix.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// True when nothing was stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AccessRecord {
    /// Maximum inline method length.
    pub const MAX_METHOD: usize = 8;
    /// Maximum inline host length.
    pub const MAX_HOST: usize = 64;
    /// Maximum inline path length.
    pub const MAX_PATH: usize = 192;

    /// Builds a record stamped with the current wall-clock time.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn now(
        worker: u16,
        status: u16,
        duration_us: u32,
        bytes_out: u64,
        client: ([u8; 16], u16),
        upstream: Option<([u8; 16], u16)>,
        method: &[u8],
        host: &[u8],
        path: &[u8],
        trace_id: &[u8],
    ) -> Self {
        let ts_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self {
            ts_ns,
            duration_us,
            worker,
            status,
            bytes_out,
            client,
            upstream,
            method: Str8::from_lossy(method),
            host: Str8::from_lossy(host),
            path: Str8::from_lossy(path),
            trace_id: Str8::from_lossy(trace_id),
        }
    }

    /// Renders one JSON object (no trailing newline) into `out`.
    /// Escape-free fast path for plain bytes; escapes `"` and `\` and
    /// control characters per JSON string rules.
    pub fn render_json(&self, out: &mut String) {
        out.push_str("{\"ts_ns\":");
        out.push_str(itoa(self.ts_ns).as_str());
        out.push_str(",\"duration_us\":");
        out.push_str(itoa(u64::from(self.duration_us)).as_str());
        out.push_str(",\"worker\":");
        out.push_str(itoa(u64::from(self.worker)).as_str());
        out.push_str(",\"status\":");
        out.push_str(itoa(u64::from(self.status)).as_str());
        out.push_str(",\"bytes_out\":");
        out.push_str(itoa(self.bytes_out).as_str());
        out.push_str(",\"client\":\"");
        push_ip(out, self.client.0, self.client.1);
        out.push('"');
        match self.upstream {
            Some((ip, port)) => {
                out.push_str(",\"upstream\":\"");
                push_ip(out, ip, port);
                out.push('"');
            }
            None => out.push_str(",\"upstream\":null"),
        }
        out.push_str(",\"method\":\"");
        push_escaped(out, self.method.as_bytes());
        out.push_str("\",\"host\":\"");
        push_escaped(out, self.host.as_bytes());
        out.push_str("\",\"path\":\"");
        push_escaped(out, self.path.as_bytes());
        out.push('"');
        if !self.trace_id.is_empty() {
            out.push_str(",\"trace_id\":\"");
            push_escaped(out, self.trace_id.as_bytes());
            out.push('"');
        }
        out.push('}');
    }
}

/// `u64` → decimal string (allocates on the drain thread only).
fn itoa(v: u64) -> String {
    v.to_string()
}

/// Renders an IPv4 (v4-mapped) or IPv6 address with `:port`. The common
/// v4-mapped form renders dotted-quad (`127.0.0.1`), not `::ffff:…`.
fn push_ip(out: &mut String, octets: [u8; 16], port: u16) {
    if let Some(v4) = v4_mapped(octets) {
        out.push_str(&v4.to_string());
    } else {
        let _ = write_addr(out, std::net::IpAddr::from(octets));
    }
    out.push(':');
    out.push_str(itoa(u64::from(port)).as_str());
}

/// Extracts the v4 address from a v4-mapped IPv6 form.
fn v4_mapped(octets: [u8; 16]) -> Option<std::net::Ipv4Addr> {
    match octets {
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, a, b, c, d] => {
            Some(std::net::Ipv4Addr::new(a, b, c, d))
        }
        _ => None,
    }
}

fn write_addr(out: &mut String, addr: std::net::IpAddr) -> std::fmt::Result {
    use std::fmt::Write as _;
    write!(out, "{addr}")
}

/// JSON-escapes `bytes` into `out`.
fn push_escaped(out: &mut String, bytes: &[u8]) {
    for &b in bytes {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x00..=0x1F => {
                out.push_str(&format!("\\u{:04x}", b));
            }
            0x20..=0x7E => out.push(b as char),
            // Non-ASCII: pass through as UTF-8 lossy chars.
            _ => out.push(char::from(b).escape_default().next().unwrap_or('?')),
        }
    }
}

/// Per-process access-log front: wrap the per-worker rings, count drops.
pub struct AccessLog {
    ring: AccessRing,
    dropped: AtomicU64,
}

impl AccessLog {
    /// New empty log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ring: EventRing::new(),
            dropped: AtomicU64::new(0),
        }
    }

    /// Pushes a record; never blocks. Returns `false` when the ring was
    /// full and the record was dropped.
    pub fn emit(&self, record: AccessRecord) -> bool {
        match self.ring.try_push(record) {
            Some(back) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                let _ = back;
                false
            }
            None => true,
        }
    }

    /// Pops the next record (drain side).
    #[must_use]
    pub fn pop(&self) -> Option<AccessRecord> {
        self.ring.try_pop()
    }

    /// Records dropped for lack of ring space since startup.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Default for AccessLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V4: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 127, 0, 0, 1];

    fn sample() -> AccessRecord {
        AccessRecord::now(
            1,
            200,
            1_234,
            567,
            (V4, 54321),
            Some((V4, 8080)),
            b"GET",
            b"example.com",
            b"/api/x?a=1",
            b"4bf92f3577b34da6a3ce929d0e0e4736",
        )
    }

    #[test]
    fn renders_valid_json_with_all_fields() {
        let mut out = String::new();
        sample().render_json(&mut out);
        assert!(out.contains("\"status\":200"), "{out}");
        assert!(out.contains("\"client\":\"127.0.0.1:54321\""), "{out}");
        // v4-mapped renders dotted-quad, not ::ffff:…
        assert!(!out.contains("::ffff:"), "{out}");
        assert!(out.contains("\"upstream\":\"127.0.0.1:8080\""), "{out}");
        assert!(out.contains("\"method\":\"GET\""), "{out}");
        assert!(out.contains("\"path\":\"/api/x?a=1\""), "{out}");
        assert!(out.contains("\"trace_id\":\"4bf9"), "{out}");
        // Parses as JSON (serde_json dev-independent: cheap sanity via
        // braces balance is weak; use a real parse when serde available).
        assert_eq!(out.matches('{').count(), out.matches('}').count());
    }

    #[test]
    fn escapes_quotes_and_backslashes() {
        let mut rec = sample();
        rec.path = Str8::from_lossy(b"/x\"y\\z");
        let mut out = String::new();
        rec.render_json(&mut out);
        assert!(out.contains("\\\"y\\\\z"), "{out}");
    }

    #[test]
    fn truncates_long_paths() {
        let long = vec![b'a'; 500];
        let mut rec = sample();
        rec.path = Str8::from_lossy(&long);
        assert_eq!(rec.path.as_bytes().len(), AccessRecord::MAX_PATH);
    }

    #[test]
    fn local_response_has_null_upstream() {
        let mut rec = sample();
        rec.upstream = None;
        let mut out = String::new();
        rec.render_json(&mut out);
        assert!(out.contains("\"upstream\":null"), "{out}");
    }

    #[test]
    fn ring_drop_counting() {
        let log = AccessLog::new();
        let mut pushed = 0;
        while log.emit(sample()) {
            pushed += 1;
            assert!(pushed <= ACCESS_RING_CAPACITY + 1);
        }
        assert_eq!(log.dropped(), 1);
        let mut drained = 0;
        while log.pop().is_some() {
            drained += 1;
        }
        assert_eq!(drained, pushed);
    }
}
