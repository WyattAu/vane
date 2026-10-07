//! Fixed buffer pool (`IORING_REGISTER_BUFFERS` semantics, `IO-02`).
//!
//! Every worker pre-allocates `CAP` slots of `BUF_SIZE` bytes up front —
//! the request hot path never calls `malloc`. Slots have *stable addresses*,
//! which is what allows the io_uring backend to register them with the ring
//! (`IORING_REGISTER_BUFFERS`) and read with `buf_index = slot`. The mio
//! backend dereferences the same slots at event time.

/// Default per-slot buffer size (single read batch).
pub const DEFAULT_BUF_SIZE: usize = 4096;

/// Default slots per worker (power of two, io_uring registration-friendly).
pub const DEFAULT_POOL_SIZE: usize = 1024;

/// Smallest configurable slot size. Below this, a read syscall amortizes
/// nothing and the fixed-buffer registration is pure overhead.
pub const MIN_BUF_SIZE: usize = 512;

/// Largest configurable slot size. Past this, per-connection memory
/// (`pool_slots * buf_size`) and the backpressure threshold
/// (`2 * buf_size`, worker.rs) both inflate without a syscall win on
/// this class of workload.
pub const MAX_BUF_SIZE: usize = 65536;

/// Pre-allocated, fixed-size buffer pool owned by one worker.
///
/// The slot size is configurable (`[runtime] buffer_size`): every
/// consumer already works in slices, io_uring's
/// `IORING_REGISTER_BUFFERS` takes (ptr, len) iovecs of any length, and
/// `ReadFixed` caps its read by the passed length — so the size was only
/// ever pinned by the old `[u8; DEFAULT_BUF_SIZE]` slot type.
pub struct BufferPool {
    /// Stable-address slot storage: `slots[i]` is exactly `buf_size`
    /// bytes (`Box<[u8]>` keeps the stable address the kernel writes
    /// into; the length is uniform by construction).
    slots: Box<[Box<[u8]>]>,
    /// LIFO free list of slot indices (worker-local, single thread).
    free: Vec<u32>,
    /// Slot size in bytes.
    buf_size: usize,
}

impl BufferPool {
    /// Pre-allocates `capacity` slots of `buf_size` bytes.
    ///
    /// Returns `None` for a zero size or a size outside
    /// `[MIN_BUF_SIZE, MAX_BUF_SIZE]` — the bounds exist so a config
    /// typo cannot silently allocate megabytes per slot or make every
    /// read a page-farcing no-op.
    #[must_use]
    pub fn new(capacity: usize, buf_size: usize) -> Option<Self> {
        if !(MIN_BUF_SIZE..=MAX_BUF_SIZE).contains(&buf_size) {
            return None;
        }
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(vec![0u8; buf_size].into_boxed_slice());
            free.push(slots.len() as u32 - 1);
        }
        Some(Self {
            slots: slots.into_boxed_slice(),
            free,
            buf_size,
        })
    }

    /// Slot size.
    #[must_use]
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Number of slots.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Free slot count.
    #[must_use]
    #[inline]
    pub fn free_slots(&self) -> usize {
        self.free.len()
    }

    /// Takes a slot, or `None` when the pool is exhausted (callers stop
    /// reading — TCP backpressure does the rest).
    #[inline]
    pub fn take(&mut self) -> Option<u32> {
        self.free.pop()
    }

    /// Returns a slot to the pool.
    #[inline]
    pub fn release(&mut self, slot: u32) {
        debug_assert!((slot as usize) < self.slots.len(), "slot out of range");
        self.free.push(slot);
    }

    /// Reads the slot's bytes (stable address, safe to hand to the kernel).
    #[must_use]
    #[inline]
    pub fn slot(&self, slot: u32) -> &[u8] {
        &self.slots[slot as usize]
    }

    /// Mutable access to a slot's bytes.
    #[inline]
    pub fn slot_mut(&mut self, slot: u32) -> &mut [u8] {
        &mut self.slots[slot as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_release() {
        let mut pool = BufferPool::new(4, DEFAULT_BUF_SIZE).expect("default size");
        assert_eq!(pool.free_slots(), 4);
        let a = pool.take().expect("slot");
        let b = pool.take().expect("slot");
        assert_eq!(pool.free_slots(), 2);
        pool.release(a);
        assert_eq!(pool.free_slots(), 3);
        let _c = pool.take();
        let d = pool.take();
        assert!(d.is_some());
        pool.release(b); // b was taken by value; return it directly
    }

    #[test]
    fn exhaustion() {
        let mut pool = BufferPool::new(2, DEFAULT_BUF_SIZE).expect("default size");
        let _ = pool.take();
        let _ = pool.take();
        assert!(pool.take().is_none(), "pool exhausted");
    }

    #[test]
    fn custom_sizes_are_accepted_and_bounded() {
        // The size is a knob ([runtime] buffer_size): every consumer
        // works in slices and both engines derive their lengths from
        // pool.buf_size().
        let pool = BufferPool::new(4, 16384).expect("16 KiB slots");
        assert_eq!(pool.buf_size(), 16384);
        assert_eq!(pool.slot(0).len(), 16384);
        assert_eq!(pool.slot(3).len(), 16384);

        assert!(BufferPool::new(4, 0).is_none(), "zero-size slots");
        assert!(
            BufferPool::new(4, MIN_BUF_SIZE - 1).is_none(),
            "below the floor"
        );
        assert!(
            BufferPool::new(4, MAX_BUF_SIZE + 1).is_none(),
            "above the ceiling"
        );
        // The bounds themselves are valid.
        assert!(BufferPool::new(1, MIN_BUF_SIZE).is_some());
        assert!(BufferPool::new(1, MAX_BUF_SIZE).is_some());
    }
}
