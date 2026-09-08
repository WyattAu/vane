//! Completion tokens: routing CQEs back to the session and operation that
//! produced them.
//!
//! Layout (64 bits): `[ op: 8 | generation: 16 | slot: 24 | aux: 16 ]`.
//! The 16-bit generation guards against stale completions for a slot whose
//! session was closed and its slot reused.

use std::fmt;

/// Operation kind encoded in a [`Token`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    /// Listener readiness (a new connection can be accepted).
    Accept = 1,
    /// Read from the downstream (client-facing) socket.
    DownstreamRead = 2,
    /// Write to the downstream socket.
    DownstreamWrite = 3,
    /// Read from the upstream (backend) socket.
    UpstreamRead = 4,
    /// Write to the upstream socket.
    UpstreamWrite = 5,
    /// Nonblocking connect to an upstream completed.
    Connect = 6,
    /// Splice pump progress (L4 passthrough).
    Splice = 7,
}

/// Uniquely identifies an in-flight engine operation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(u64);

impl Token {
    const OP_BITS: u64 = 8;
    const GEN_BITS: u64 = 16;
    const SLOT_BITS: u64 = 24;
    const SLOT_SHIFT: u64 = Self::OP_BITS + Self::GEN_BITS;
    const AUX_SHIFT: u64 = Self::SLOT_SHIFT + Self::SLOT_BITS;

    /// Packs `op`, `slot`, `generation`, and an auxiliary value.
    #[must_use]
    #[inline]
    pub fn new(op: Op, slot: u32, generation: u16, aux: u16) -> Self {
        debug_assert!(slot < (1 << Self::SLOT_BITS), "slot overflows token");
        Self(
            (op as u64)
                | ((generation as u64) << Self::OP_BITS)
                | ((slot as u64) << Self::SLOT_SHIFT)
                | ((aux as u64) << Self::AUX_SHIFT),
        )
    }

    /// Listener token (aux = listener index).
    #[must_use]
    #[inline]
    pub fn accept(listener: u16) -> Self {
        Self::new(Op::Accept, 0, 0, listener)
    }

    /// Decodes the operation kind.
    #[must_use]
    #[inline]
    pub fn op(self) -> Op {
        // SAFETY-free: all byte patterns 0..=7 are valid `Op` discriminants
        // we ever encode; anything else maps to a panic-free fallback.
        match self.0 & ((1 << Self::OP_BITS) - 1) {
            1 => Op::Accept,
            2 => Op::DownstreamRead,
            3 => Op::DownstreamWrite,
            4 => Op::UpstreamRead,
            5 => Op::UpstreamWrite,
            6 => Op::Connect,
            _ => Op::Splice,
        }
    }

    /// Decodes the session slot.
    #[must_use]
    #[inline]
    pub fn slot(self) -> u32 {
        ((self.0 >> Self::SLOT_SHIFT) & ((1 << Self::SLOT_BITS) - 1)) as u32
    }

    /// Decodes the session generation.
    #[must_use]
    #[inline]
    pub fn generation(self) -> u16 {
        ((self.0 >> Self::OP_BITS) & ((1 << Self::GEN_BITS) - 1)) as u16
    }

    /// Decodes the auxiliary payload.
    #[must_use]
    #[inline]
    pub fn aux(self) -> u16 {
        (self.0 >> Self::AUX_SHIFT) as u16
    }

    /// Raw bits (engine backends store tokens directly).
    #[must_use]
    #[inline]
    pub fn bits(self) -> u64 {
        self.0
    }

    /// Rebuilds a token from raw bits.
    #[must_use]
    #[inline]
    pub fn from_bits(bits: u64) -> Self {
        Self(bits)
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Token({:?}, slot {}, generation {}, aux {})",
            self.op(),
            self.slot(),
            self.generation(),
            self.aux()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let t = Token::new(Op::DownstreamRead, 123_456, 0xBEEF, 7);
        assert_eq!(t.op(), Op::DownstreamRead);
        assert_eq!(t.slot(), 123_456);
        assert_eq!(t.generation(), 0xBEEF);
        assert_eq!(t.aux(), 7);
    }

    #[test]
    fn accept_token() {
        let t = Token::accept(3);
        assert_eq!(t.op(), Op::Accept);
        assert_eq!(t.aux(), 3);
    }
}
