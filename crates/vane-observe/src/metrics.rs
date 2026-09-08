//! Lock-free metric registry with Prometheus exposition (`OB-01`).
//!
//! Every metric is an [`AtomicU64`] inside a `#[repr(align(64))]` cell, so
//! hot counters never share a cache line (no false sharing between the
//! workers bumping them, and between counters themselves). Recording is one
//! relaxed `fetch_add` — no locks, no allocation, ~2 ns.
//!
//! Registration is control-plane-only (append-only, capacity-bounded), so
//! the worker-side read path is a bounds-checked array index.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Kind of a registered metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Monotonic counter (requests, bytes, errors).
    Counter,
    /// Point-in-time gauge (active connections, pool occupancy).
    Gauge,
    /// Fixed-bucket latency histogram (microsecond bounds).
    Histogram,
}

/// Cache-padded atomic cell — the entire storage of a metric.
#[repr(align(64))]
#[derive(Default)]
struct Cell(AtomicU64);

/// A handle recording into a registered metric. Copy + `Send` + `Sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricHandle {
    index: usize,
    kind: MetricKind,
}

impl MetricHandle {
    /// Increments a counter by `n` (no-op on gauges/histograms).
    #[inline]
    pub fn add(&self, reg: &Registry, n: u64) {
        if let MetricKind::Counter = self.kind {
            reg.cells[self.index].0.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Increments a counter by one.
    #[inline]
    pub fn inc(&self, reg: &Registry) {
        self.add(reg, 1);
    }

    /// Sets a gauge to `v` (no-op on counters/histograms).
    #[inline]
    pub fn set(&self, reg: &Registry, v: u64) {
        if let MetricKind::Gauge = self.kind {
            reg.cells[self.index].0.store(v, Ordering::Relaxed);
        }
    }

    /// Observes a latency (microseconds) into a histogram bucket.
    #[inline]
    pub fn observe_us(&self, reg: &Registry, micros: u64) {
        if let MetricKind::Histogram = self.kind {
            let base = self.index;
            let buckets = &HISTOGRAM_BUCKETS_US;
            let last = buckets.len();
            let mut idx = last;
            for (i, b) in buckets.iter().enumerate() {
                if micros <= *b {
                    idx = i;
                    break;
                }
            }
            reg.cells[base + idx].0.fetch_add(1, Ordering::Relaxed);
            // Total counter lives in the cell right after the buckets.
            reg.cells[base + last].0.fetch_add(1, Ordering::Relaxed);
            // Sum cell (microseconds) right after total.
            reg.cells[base + last + 1]
                .0
                .fetch_add(micros, Ordering::Relaxed);
        }
    }
}

/// Latency histogram bucket upper bounds in microseconds.
pub const HISTOGRAM_BUCKETS_US: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000,
];

/// Cells consumed by a histogram: buckets + `_sum` + `_count`.
pub const HISTOGRAM_CELLS: usize = HISTOGRAM_BUCKETS_US.len() + 2;

/// Append-only, capacity-bounded metric registry.
///
/// Locks here are control-plane-only (registration/scrape); the data plane
/// touches only the lock-free cells.
pub struct Registry {
    /// Metric names, parallel to `kinds`.
    names: Mutex<Vec<Box<str>>>,
    /// Metric kinds, parallel to `names` (read hot, appended cold).
    kinds: Mutex<Vec<MetricKind>>,
    /// Metric storage. Histograms occupy `HISTOGRAM_CELLS` consecutive cells.
    cells: Box<[Cell]>,
    /// Next free cell index (registration is serialized by the mutex).
    next_cell: Mutex<usize>,
}

impl Registry {
    /// Maximum number of addressable cells.
    pub const MAX_CELLS: usize = 16384;

    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            names: Mutex::new(Vec::new()),
            kinds: Mutex::new(Vec::new()),
            cells: (0..Self::MAX_CELLS)
                .map(|_| Cell(AtomicU64::new(0)))
                .collect(),
            next_cell: Mutex::new(0),
        }
    }

    /// Registers a counter or gauge; returns a stable handle.
    ///
    /// # Panics
    /// Panics if the registry is exhausted — a programming error surfaced
    /// at startup, never on the hot path.
    #[must_use]
    pub fn register(&self, name: &str, kind: MetricKind) -> MetricHandle {
        let idx = self.alloc_cells(1);
        self.names
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(name.into());
        self.kinds
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(kind);
        MetricHandle { index: idx, kind }
    }

    /// Registers a latency histogram (microsecond buckets).
    #[must_use]
    pub fn register_histogram(&self, name: &str) -> MetricHandle {
        let idx = self.alloc_cells(HISTOGRAM_CELLS);
        self.names
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(name.into());
        self.kinds
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(MetricKind::Histogram);
        MetricHandle {
            index: idx,
            kind: MetricKind::Histogram,
        }
    }

    fn alloc_cells(&self, n: usize) -> usize {
        let mut next = self.next_cell.lock().unwrap_or_else(|e| e.into_inner());
        let idx = *next;
        assert!(
            idx + n <= Self::MAX_CELLS,
            "metric registry exhausted ({idx} + {n} > {})",
            Self::MAX_CELLS
        );
        *next += n;
        idx
    }

    /// Snapshots all metrics as Prometheus text exposition format.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let names = self.names.lock().unwrap_or_else(|e| e.into_inner());
        let kinds = self.kinds.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = self.next_cell.lock().unwrap_or_else(|e| e.into_inner());
        let _ = &mut next;
        let mut out = String::with_capacity(names.len() * 64);
        for (i, name) in names.iter().enumerate() {
            let idx = self.cell_index(&names[..i], &kinds[..i]);
            match kinds[i] {
                MetricKind::Counter | MetricKind::Gauge => {
                    let v = self.cells[idx].0.load(Ordering::Relaxed);
                    let ty = if kinds[i] == MetricKind::Counter {
                        "counter"
                    } else {
                        "gauge"
                    };
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push(' ');
                    out.push_str(ty);
                    out.push('\n');
                    out.push_str(name);
                    out.push_str(&format!(" {v}\n"));
                }
                MetricKind::Histogram => {
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" histogram\n");
                    let mut cumulative = 0u64;
                    for (b, bound) in HISTOGRAM_BUCKETS_US.iter().enumerate() {
                        cumulative += self.cells[idx + b].0.load(Ordering::Relaxed);
                        out.push_str(name);
                        out.push_str(&format!("_bucket{{le=\"{bound}\"}} {cumulative}\n"));
                    }
                    // +Inf bucket == total count.
                    let count = self.cells[idx + HISTOGRAM_BUCKETS_US.len()]
                        .0
                        .load(Ordering::Relaxed);
                    out.push_str(name);
                    out.push_str(&format!("_bucket{{le=\"+Inf\"}} {count}\n"));
                    let sum = self.cells[idx + HISTOGRAM_BUCKETS_US.len() + 1]
                        .0
                        .load(Ordering::Relaxed);
                    out.push_str(name);
                    out.push_str(&format!("_sum {sum}\n"));
                    out.push_str(name);
                    out.push_str(&format!("_count {count}\n"));
                }
            }
        }
        out
    }

    /// Reads the raw value of a counter/gauge (tests, admin introspection).
    #[must_use]
    pub fn get(&self, handle: MetricHandle) -> u64 {
        self.cells[handle.index].0.load(Ordering::Relaxed)
    }

    /// Recomputes the starting cell index of the `i`-th metric.
    fn cell_index(&self, prior_names: &[Box<str>], prior_kinds: &[MetricKind]) -> usize {
        let mut idx = 0usize;
        for &k in prior_kinds.iter().take(prior_names.len()) {
            idx += match k {
                MetricKind::Histogram => HISTOGRAM_CELLS,
                _ => 1,
            };
        }
        idx
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_gauge_basic() {
        let reg = Registry::new();
        let c = reg.register("vane_test_total", MetricKind::Counter);
        let g = reg.register("vane_test_gauge", MetricKind::Gauge);
        c.add(&reg, 5);
        c.inc(&reg);
        g.set(&reg, 42);
        assert_eq!(reg.get(c), 6);
        assert_eq!(reg.get(g), 42);
        let text = reg.render_prometheus();
        assert!(text.contains("vane_test_total 6"));
        assert!(text.contains("vane_test_gauge 42"));
    }

    #[test]
    fn histogram_buckets_cumulative() {
        let reg = Registry::new();
        let h = reg.register_histogram("vane_latency_us");
        h.observe_us(&reg, 3);
        h.observe_us(&reg, 300);
        h.observe_us(&reg, 3_000);
        let text = reg.render_prometheus();
        assert!(text.contains("_bucket{le=\"5\"} 1"));
        assert!(text.contains("_bucket{le=\"500\"} 2"));
        assert!(text.contains("_count 3"));
        // Sum in microseconds.
        assert!(text.contains("_sum 3303"));
    }
}
