//! Vyukov-style bounded MPMC ring buffer — the non-blocking event/log drain.
//!
//! Same algorithm as Dmitry Vyukov's bounded MPMC queue: each slot carries a
//! `sequence` counter; producers `compare_exchange` the enqueue cursor,
//! consumers the dequeue cursor. Producers that find a full ring return
//! `None` immediately (backpressure-free observation: metrics and logs must
//! never block the data plane — dropping a *log line* is always preferable
//! to stalling a request).
//!
//! Loom model-checking lives in `tests/loom_ring.rs` (`loom` feature).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cache-line size on x86-64 and modern ARM.
const CACHE_LINE: usize = 64;

/// Pads `T` to a full cache line so the head/tail cursors never share one.
#[repr(align(64))]
#[derive(Debug, Default)]
struct CachePadded(AtomicUsize);

/// A bounded, lock-free multi-producer multi-consumer ring.
///
/// `CAP` must be a power of two (masked indexing, no modulo).
pub struct EventRing<T, const CAP: usize> {
    /// Bitwise-AND mask (`CAP - 1`).
    mask: usize,
    /// Enqueue cursor.
    head: CachePadded,
    /// Dequeue cursor.
    tail: CachePadded,
    /// Slot storage with per-slot sequence counters.
    slots: Box<[Slot<T>]>,
}

struct Slot<T> {
    /// Sequence number: equals `pos` when writable, `pos + 1` when readable.
    sequence: AtomicUsize,
    /// The payload (valid only when the slot is readable).
    value: UnsafeCell<Option<T>>,
}

// SAFETY: exclusive access to a slot's value is guaranteed by the sequence
// protocol: a value is readable only after the producer's release-store, and
// is taken (set to `None`) by the single consumer that won the dequeue CAS.
unsafe impl<T: Send, const CAP: usize> Send for EventRing<T, CAP> {}
// SAFETY: all shared access is through atomics; payloads move exclusively.
unsafe impl<T: Send, const CAP: usize> Sync for EventRing<T, CAP> {}

impl<T, const CAP: usize> EventRing<T, CAP> {
    /// Creates a ring with capacity `CAP`.
    #[must_use]
    pub fn new() -> Self {
        const {
            assert!(CAP.is_power_of_two(), "CAP must be a power of two");
            assert!(CAP > 0, "CAP must be non-zero");
        }
        let slots = (0..CAP)
            .map(|i| Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(None),
            })
            .collect::<Vec<_>>();
        Self {
            mask: CAP - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            slots: slots.into_boxed_slice(),
        }
    }

    /// Attempts to enqueue `value` without blocking.
    ///
    /// Returns `Some(value)` back if the ring is full (caller decides:
    /// drop the log line, increment a dropped counter, etc.).
    pub fn try_push(&self, value: T) -> Option<T> {
        let mut pos = self.head.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;
            if diff == 0 {
                // Slot is writable; try to claim it.
                match self.head.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: this producer won exclusive access to the
                        // slot (sequence == pos, claimed via CAS). No reader
                        // will touch it until the release-store below.
                        let cell = slot.value.get();
                        // SAFETY: exclusive slot ownership per sequence CAS.
                        unsafe { (*cell) = Some(value) };
                        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return None;
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                // Ring is full.
                return Some(value);
            } else {
                // Another producer advanced the head; reload and retry.
                pos = self.head.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Attempts to dequeue one value, if any.
    pub fn try_pop(&self) -> Option<T> {
        let mut pos = self.tail.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos.wrapping_add(1) as isize;
            if diff == 0 {
                match self.tail.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: sequence == pos + 1 proves the slot holds a
                        // value published by the producer's release-store;
                        // this consumer won exclusive take via CAS.
                        let cell = slot.value.get();
                        // SAFETY: sequence == pos + 1 proves published value.
                        let value = unsafe { (*cell).take() };
                        slot.sequence
                            .store(pos.wrapping_add(CAP), Ordering::Release);
                        return value;
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                // Ring is empty.
                return None;
            } else {
                pos = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Approximate number of items currently buffered (Relaxed — hint only).
    #[must_use]
    pub fn len(&self) -> usize {
        self.head
            .0
            .load(Ordering::Relaxed)
            .wrapping_sub(self.tail.0.load(Ordering::Relaxed))
    }

    /// `true` when the ring appears empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T, const CAP: usize> Default for EventRing<T, CAP> {
    fn default() -> Self {
        Self::new()
    }
}

// Keep the padding constant referenced (silences dead-code in odd configs).
const _: () = assert!(CACHE_LINE == 64);
