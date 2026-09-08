//! RFC 7231 `Date` header formatting — cached per second.
//!
//! The common path is a single `Relaxed` load + memcpy of a preformatted
//! 29-byte `Date: ...` line. A background refresh (control plane) rebuilds
//! the cached line once per second; workers never format dates.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Cached `Date:` header line, refreshed once per wall-clock second.
///
/// Worker-local (not `Sync`): each worker owns one cache, so the inline
/// refresh in `line()` is a plain `&self -> &self` mutation through
/// `UnsafeCell` on a single thread.
pub struct DateCache {
    /// Packed: high 32 bits = unix second the line is valid for.
    stamp: AtomicU64,
    /// The 37-byte line: `Date: Fri, 08 Sep 2026 12:00:00 GMT\r\n`.
    line: UnsafeCell<[u8; 37]>,
}

impl DateCache {
    /// New cache with the current time baked in.
    #[must_use]
    pub fn new() -> Self {
        let now = unix_now();
        let c = Self {
            stamp: AtomicU64::new(0),
            line: UnsafeCell::new(format_date_line(now)),
        };
        c.stamp.store((now as u64) << 32 | 1, Ordering::Release);
        c
    }

    /// Returns the cached `Date:` header line (37 bytes, includes CRLF).
    ///
    /// If the cached second is stale, refreshes inline (still allocation-
    /// free; the format loop is ~40 ns and runs at most once per second
    /// per worker).
    #[must_use]
    pub fn line(&self) -> &[u8; 37] {
        let now = unix_now();
        let stamp = self.stamp.load(Ordering::Relaxed);
        let cached_sec = (stamp >> 32) as u32;
        // SAFETY: DateCache is !Sync (worker-local); no aliasing possible.
        let line = unsafe { &mut *self.line.get() };
        if cached_sec != now {
            *line = format_date_line(now);
            self.stamp.store(
                (now as u64) << 32 | (stamp & 0xffff_ffff).wrapping_add(1),
                Ordering::Relaxed,
            );
        }
        line
    }
}

impl Default for DateCache {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: the cache is per-worker; `line()` mutates only through &self on
// a single thread.
unsafe impl Send for DateCache {}

fn unix_now() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as u32)
}

/// Days-from-civil algorithm (Howard Hinnant) — no lookup tables, no libc.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Formats `Date: Wed, 08 Sep 2026 14:31:05 GMT\r\n` (37 bytes).
#[must_use]
pub fn format_date_line(unix_secs: u32) -> [u8; 37] {
    let days = i64::from(unix_secs / 86_400);
    let secs_of_day = unix_secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    let weekday = ((days % 7) + 11) % 7; // 1970-01-01 = Thursday(4)
    let (hh, mm, ss) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    let mut out = [0u8; 37];
    let s = format!(
        "Date: {}, {:02} {} {:04} {:02}:{:02}:{:02} GMT\r\n",
        DAYS[weekday as usize],
        d,
        MONTHS[(m - 1) as usize],
        y,
        hh,
        mm,
        ss
    );
    // `s` is exactly 37 bytes for all valid years (4-digit).
    let bytes = s.as_bytes();
    let n = bytes.len().min(37);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        // 2026-09-08 00:00:00 UTC = 1_788_825_600 (Tuesday)
        let line = format_date_line(1_788_825_600);
        let s = std::str::from_utf8(&line).expect("utf8");
        assert!(
            s.starts_with("Date: Tue, 08 Sep 2026 00:00:00 GMT\r\n"),
            "{s}"
        );
        // Epoch: 1970-01-01 = Thursday
        let line = format_date_line(0);
        let s = std::str::from_utf8(&line).expect("utf8");
        assert!(
            s.starts_with("Date: Thu, 01 Jan 1970 00:00:00 GMT\r\n"),
            "{s}"
        );
        // Leap-year date: 2024-02-29 12:00:00 UTC = 1709208000 (Thursday)
        let line = format_date_line(1_709_208_000);
        let s = std::str::from_utf8(&line).expect("utf8");
        assert!(
            s.starts_with("Date: Thu, 29 Feb 2024 12:00:00 GMT\r\n"),
            "{s}"
        );
    }

    #[test]
    fn cache_updates_per_second() {
        let c = DateCache::new();
        let l1 = *c.line();
        let l2 = *c.line();
        assert_eq!(l1, l2); // same second
    }
}
