//! Cacheline-padded, lock-free SPSC ring for cross-thread signaling (`TH-02`).
//!
//! Used for the control-plane → worker command path and worker → control
//! result path. One producer, one consumer; `try_push`/`try_pop` only, so
//! neither side ever blocks. Zero `SeqCst`; the handoff edge is the standard
//! acquire/release message-passing pair.
//!
//! Loom model-checking: `tests/loom_spsc.rs`.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pads to a cache line so the two cursors never false-share.
#[repr(align(64))]
#[derive(Default)]
struct Padded(AtomicUsize);

/// A bounded SPSC ring. `CAP` must be a power of two.
pub struct SpscRing<T, const CAP: usize> {
    mask: usize,
    head: Padded, // producer cursor (slots written)
    tail: Padded, // consumer cursor (slots read)
    slots: Box<[Slot<T>]>,
}

struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: single producer + single consumer, enforced by &mut / &self split.
unsafe impl<T: Send, const CAP: usize> Send for SpscRing<T, CAP> {}
// SAFETY: shared access is atomics-only; payloads move via the sequence
// protocol (one consumer takes ownership).
unsafe impl<T: Send, const CAP: usize> Sync for SpscRing<T, CAP> {}

impl<T, const CAP: usize> SpscRing<T, CAP> {
    /// Creates the ring.
    #[must_use]
    pub fn new() -> Self {
        const {
            assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        }
        let slots = (0..CAP)
            .map(|i| Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(None),
            })
            .collect::<Vec<_>>();
        Self {
            mask: CAP - 1,
            head: Padded::default(),
            tail: Padded::default(),
            slots: slots.into_boxed_slice(),
        }
    }

    /// Producer: attempts to enqueue, returns `Some(value)` when full.
    #[inline]
    pub fn try_push(&self, value: T) -> Option<T> {
        let pos = self.head.0.load(Ordering::Relaxed);
        let slot = &self.slots[pos & self.mask];
        if slot.sequence.load(Ordering::Acquire) != pos {
            return Some(value); // full
        }
        // SAFETY: sequence == pos means the slot is empty and exclusively
        // claimable by the (single) producer.
        unsafe {
            *slot.value.get() = Some(value);
        }
        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
        self.head.0.store(pos.wrapping_add(1), Ordering::Relaxed);
        None
    }

    /// Consumer: attempts to dequeue, `None` when empty.
    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        let pos = self.tail.0.load(Ordering::Relaxed);
        let slot = &self.slots[pos & self.mask];
        if slot.sequence.load(Ordering::Acquire) != pos + 1 {
            return None; // empty
        }
        // SAFETY: sequence == pos + 1 means a value was published (release)
        // and this (single) consumer owns the take.
        let value = unsafe { (*slot.value.get()).take() };
        slot.sequence
            .store(pos.wrapping_add(CAP), Ordering::Release);
        self.tail.0.store(pos.wrapping_add(1), Ordering::Relaxed);
        value
    }

    /// Approximate occupancy (Relaxed, hint only).
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

impl<T, const CAP: usize> Default for SpscRing<T, CAP> {
    fn default() -> Self {
        Self::new()
    }
}

/// Pairs a ring with its two ends for handing to separate threads.
#[must_use]
pub fn channel<T, const CAP: usize>() -> (SpscSender<T, CAP>, SpscReceiver<T, CAP>) {
    let ring = std::sync::Arc::new(SpscRing::new());
    (
        SpscSender {
            ring: std::sync::Arc::clone(&ring),
        },
        SpscReceiver { ring },
    )
}

/// Producer handle.
pub struct SpscSender<T, const CAP: usize> {
    ring: std::sync::Arc<SpscRing<T, CAP>>,
}

impl<T, const CAP: usize> SpscSender<T, CAP> {
    /// Enqueues without blocking; `Err(value)` when full.
    #[inline]
    pub fn send(&self, value: T) -> Result<(), T> {
        match self.ring.try_push(value) {
            None => Ok(()),
            Some(back) => Err(back),
        }
    }
}

/// Consumer handle.
pub struct SpscReceiver<T, const CAP: usize> {
    ring: std::sync::Arc<SpscRing<T, CAP>>,
}

impl<T, const CAP: usize> SpscReceiver<T, CAP> {
    /// Dequeues without blocking.
    #[inline]
    pub fn recv(&self) -> Option<T> {
        self.ring.try_pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spsc_basic() {
        let (tx, rx) = channel::<u64, 4>();
        tx.send(1).expect("fits");
        tx.send(2).expect("fits");
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn full_returns_back() {
        let (tx, rx) = channel::<u8, 2>();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(tx.send(3), Err(3));
        assert_eq!(rx.recv(), Some(1));
        tx.send(3).expect("space now");
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
    }

    #[test]
    fn wraparound() {
        let (tx, rx) = channel::<usize, 4>();
        for i in 0..10_000usize {
            tx.send(i).expect("loop drains");
            assert_eq!(rx.recv(), Some(i));
        }
    }

    #[test]
    fn cross_thread_sum() {
        let (tx, rx) = channel::<u64, 1024>();
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0..100_000u64 {
                    while tx.send(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            });
            let got: u64 = (0..100_000u64)
                .map(|_| {
                    loop {
                        if let Some(v) = rx.recv() {
                            break v;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
                .sum();
            assert_eq!(got, 100_000u64 * 99_999 / 2);
        });
    }
}
