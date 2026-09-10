//! Thread-local generational slab for connection sessions (`MM-01`).
//!
//! Session slots are pre-reserved up front and reused via free list; the
//! 16-bit generation counter guards against ABA when a completion for a
//! closed session races the slot's reuse. Single-threaded by design: the
//! worker that owns a slab is the only thread that touches it.

/// Maximum sessions a slab tracks (24-bit slot index in [`crate::token::Token`]).
pub const MAX_SLOTS: usize = 1 << 24;

/// Errors from [`SessionSlab::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SlabError {
    /// Capacity is zero or exceeds [`MAX_SLOTS`].
    #[error("capacity {0} out of range 1..={MAX_SLOTS}")]
    Capacity(usize),
    /// Slab is exhausted.
    #[error("session slab exhausted")]
    Exhausted,
}

struct Entry<T> {
    value: Option<T>,
    generation: u16,
    next_free: u32,
    occupied: bool,
}

/// Generational slab of sessions.
pub struct SessionSlab<T> {
    entries: Box<[Entry<T>]>,
    free_head: u32,
    free_count: usize,
}

impl<T> SessionSlab<T> {
    /// Pre-allocates `capacity` slots (no `T` constructed yet).
    ///
    /// # Errors
    /// [`SlabError::Capacity`] when `capacity` is 0 or exceeds `MAX_SLOTS`.
    pub fn new(capacity: usize) -> Result<Self, SlabError> {
        if capacity == 0 || capacity > MAX_SLOTS {
            return Err(SlabError::Capacity(capacity));
        }
        let mut entries = Vec::with_capacity(capacity);
        for i in 0..capacity {
            entries.push(Entry {
                value: None,
                generation: 0,
                next_free: (i + 1) as u32,
                occupied: false,
            });
        }
        Ok(Self {
            entries: entries.into_boxed_slice(),
            free_head: 0,
            free_count: capacity,
        })
    }

    /// Inserts a value, returning `(slot, generation)`.
    ///
    /// # Errors
    /// [`SlabError::Exhausted`] when every slot is live.
    pub fn insert(&mut self, value: T) -> Result<(u32, u16), SlabError> {
        if self.free_count == 0 {
            return Err(SlabError::Exhausted);
        }
        let slot = self.free_head as usize;
        let entry = &mut self.entries[slot];
        self.free_head = entry.next_free;
        entry.occupied = true;
        entry.value = Some(value);
        self.free_count -= 1;
        Ok((slot as u32, entry.generation))
    }

    /// Removes a value. The slot's generation increments immediately so any
    /// in-flight token for the old generation becomes stale on reuse.
    pub fn remove(&mut self, slot: u32) -> Option<T> {
        let entry = self.entries.get_mut(slot as usize)?;
        if !entry.occupied {
            return None;
        }
        entry.occupied = false;
        entry.generation = entry.generation.wrapping_add(1);
        entry.next_free = self.free_head;
        self.free_head = slot;
        self.free_count += 1;
        entry.value.take()
    }

    /// Immutable access.
    #[must_use]
    pub fn get(&self, slot: u32) -> Option<&T> {
        let e = self.entries.get(slot as usize)?;
        if e.occupied { e.value.as_ref() } else { None }
    }

    /// Mutable access.
    pub fn get_mut(&mut self, slot: u32) -> Option<&mut T> {
        let e = self.entries.get_mut(slot as usize)?;
        if e.occupied { e.value.as_mut() } else { None }
    }

    /// Generation currently assigned to a slot.
    #[must_use]
    pub fn generation(&self, slot: u32) -> u16 {
        self.entries.get(slot as usize).map_or(0, |e| e.generation)
    }

    /// Number of live sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len() - self.free_count
    }

    /// `true` when no sessions are live.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True when the slot is live with the given generation (token check).
    #[must_use]
    pub fn matches(&self, slot: u32, generation: u16) -> bool {
        self.entries
            .get(slot as usize)
            .is_some_and(|e| e.occupied && e.generation == generation)
    }

    /// Iterates all live `(slot, generation)` pairs.
    #[must_use]
    pub fn live(&self) -> Vec<(u32, u16)> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.occupied)
            .map(|(i, e)| (i as u32, e.generation))
            .collect()
    }
}

/// Bumps a slot's generation (called by the worker right before reuse).
pub trait GenerationBump<T> {
    /// Increments the slot generation, invalidating stale tokens.
    fn bump_generation(&mut self, slot: u32);
}

impl<T> GenerationBump<T> for SessionSlab<T> {
    fn bump_generation(&mut self, slot: u32) {
        if let Some(e) = self.entries.get_mut(slot as usize) {
            e.generation = e.generation.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_remove_reuse() {
        let mut slab: SessionSlab<&str> = SessionSlab::new(4).expect("ok");
        let (a, gen_a) = slab.insert("a").expect("fits");
        let (b, _) = slab.insert("b").expect("fits");
        assert_eq!(slab.get(a), Some(&"a"));
        let old = slab.remove(a);
        assert_eq!(old, Some("a"));
        let (c, gen_c) = slab.insert("c").expect("reuses slot a");
        assert_eq!(c, a);
        assert_ne!(gen_c, gen_a);
        assert!(slab.matches(a, gen_c));
        assert!(!slab.matches(a, gen_a));
        assert_eq!(slab.remove(b), Some("b"));
        assert_eq!(slab.remove(c), Some("c"));
        assert!(slab.is_empty());
    }

    #[test]
    fn exhaustion() {
        let mut slab: SessionSlab<u8> = SessionSlab::new(2).expect("ok");
        slab.insert(1).expect("1");
        slab.insert(2).expect("2");
        assert_eq!(slab.insert(3), Err(SlabError::Exhausted));
    }
}
