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

/// Pre-allocated, fixed-size buffer pool owned by one worker.
pub struct BufferPool {
    /// Stable-address slot storage: `slots[i]` is `BUF_SIZE` bytes.
    slots: Box<[Box<[u8; DEFAULT_BUF_SIZE]>]>,
    /// LIFO free list of slot indices (worker-local, single thread).
    free: Vec<u32>,
    /// Slot size in bytes.
    buf_size: usize,
}

impl BufferPool {
    /// Pre-allocates `capacity` slots of `buf_size` bytes.
    ///
    /// Returns `None` when `buf_size != DEFAULT_BUF_SIZE` and the io_uring
    /// feature would require matching registration granularity — callers
    /// should keep the default size.
    #[must_use]
    pub fn new(capacity: usize, buf_size: usize) -> Option<Self> {
        if buf_size != DEFAULT_BUF_SIZE {
            return None; // registration granularity is fixed
        }
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(vec![0u8; buf_size].into_boxed_slice().try_into().ok()?);
            free.push(i as u32);
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
    pub fn slot(&self, slot: u32) -> &[u8; DEFAULT_BUF_SIZE] {
        &self.slots[slot as usize]
    }

    /// Mutable access to a slot's bytes.
    #[inline]
    pub fn slot_mut(&mut self, slot: u32) -> &mut [u8; DEFAULT_BUF_SIZE] {
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
}
