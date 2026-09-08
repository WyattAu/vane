//! Wire descriptor — the only thing that travels through the rings.
//!
//! `Copy + FromBytes + Immutable` satisfies `shm-rings`' slot bounds: any
//! bit pattern is valid (fresh page or lapped slot), and readers hold
//! `&self` while the producer writes.

use zerocopy::{FromBytes, Immutable};

/// Ring message: references a payload region in the shared arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, Immutable)]
#[repr(C)]
pub struct MsgDesc {
    /// Monotonic request id (set by the client).
    pub id: u64,
    /// Nanoseconds since epoch (publish timestamp).
    pub ts_ns: u64,
    /// Byte offset into the direction's arena.
    pub offset: u64,
    /// Payload length in bytes.
    pub len: u32,
    /// Reserved / aligns the struct to 32 bytes.
    pub flags: u32,
}

impl MsgDesc {
    /// Maximum payload the transport accepts per message.
    pub const MAX_PAYLOAD: u32 = 1 << 20; // 1 MiB

    /// New descriptor.
    #[must_use]
    pub fn new(id: u64, offset: u64, len: u32, ts_ns: u64) -> Self {
        Self {
            id,
            ts_ns,
            offset,
            len,
            flags: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_32_bytes() {
        assert_eq!(std::mem::size_of::<MsgDesc>(), 32);
        let d = MsgDesc::new(7, 4096, 128, 1);
        // Size/layout checks (descriptor crosses the ring as raw bytes).
        assert_eq!(std::mem::size_of::<MsgDesc>(), 32);
        let _ = d;
    }
}
