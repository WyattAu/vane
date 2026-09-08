//! Lock-free observability primitives for the Vane data plane.
//!
//! Three concerns, one rule: **the data plane never blocks, allocates, or
//! takes a lock to observe itself.**
//!
//! - [`metrics`] — 64-byte-aligned `AtomicU64` counters/gauges/histograms
//!   with a Prometheus text exposition (`OB-01`). Recording is a single
//!   `fetch_add` on a cache-padded cell; the scrape path (control plane)
//!   is the only place strings are built.
//! - [`ring`] — Vyukov bounded MPMC ring used as the non-blocking log and
//!   event drain (`OB-03`). Workers `try_push` (O(1), allocation-free,
//!   drops-never-block); the control plane drains at its leisure.
//! - [`trace`] — W3C `traceparent` generation and propagation (`OB-02`).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod metrics;
pub mod ring;
pub mod trace;

/// Maximum number of metrics the static registry can hold.
pub const MAX_METRICS: usize = 4096;

/// Capacity of a worker's event ring (power of two).
pub const EVENT_RING_CAPACITY: usize = 8192;

/// A structured, fixed-size log event suitable for ring transport.
///
/// Everything is inline: no `String`, no `Vec`, no heap. `message` carries
/// up to [`Self::MAX_MSG`] bytes of the rendered message; longer messages
/// are truncated (observability must never cost the hot path an allocation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEvent {
    /// Nanoseconds since the UNIX epoch.
    pub ts_ns: u64,
    /// Worker id that produced the event (`u16::MAX` = control plane).
    pub worker: u16,
    /// `tracing`-compatible verbosity level (see [`LogLevel`]).
    pub level: u8,
    /// Length of the valid prefix of `message`.
    pub msg_len: u16,
    /// Message bytes.
    pub message: [u8; LogEvent::MAX_MSG],
}

impl LogEvent {
    /// Maximum inline message size in bytes.
    pub const MAX_MSG: usize = 232;

    /// Builds an event with the current wall-clock timestamp.
    ///
    /// Wall-clock (not monotonic) because these events are consumed by
    /// humans and log aggregators; the single `SystemTime::now` syscall
    /// (vDSO) is ~20ns and only paid when a log line is actually emitted.
    #[must_use]
    pub fn now(worker: u16, level: LogLevel, msg: &[u8]) -> Self {
        let ts_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self::with_ts(ts_ns, worker, level, msg)
    }

    /// Builds an event with an explicit timestamp (deterministic tests).
    #[must_use]
    pub fn with_ts(ts_ns: u64, worker: u16, level: LogLevel, msg: &[u8]) -> Self {
        let mut message = [0u8; LogEvent::MAX_MSG];
        let n = msg.len().min(LogEvent::MAX_MSG);
        message[..n].copy_from_slice(&msg[..n]);
        Self {
            ts_ns,
            worker,
            level: level as u8,
            msg_len: n as u16,
            message,
        }
    }

    /// Borrows the valid message bytes.
    #[must_use]
    pub fn msg(&self) -> &[u8] {
        &self.message[..self.msg_len as usize]
    }
}

/// `tracing`-compatible log levels for [`LogEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogLevel {
    /// Error.
    Error = 1,
    /// Warning.
    Warn = 2,
    /// Informational.
    Info = 3,
    /// Debug.
    Debug = 4,
    /// Trace.
    Trace = 5,
}

impl LogLevel {
    /// Maps a `tracing` level byte back into the enum (0 => Info).
    #[must_use]
    pub fn from_u8(b: u8) -> Self {
        match b {
            1 => Self::Error,
            2 => Self::Warn,
            4 => Self::Debug,
            5 => Self::Trace,
            _ => Self::Info,
        }
    }

    /// Human-readable name used by the Prometheus/log bridge.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}
