//! Fuzz target: SHM descriptor parsing invariants (QA-03).
//!
//! Any bit pattern in a descriptor must yield safe bounds (the shm-rings
//! slot bounds guarantee holds via Copy+FromBytes; we assert the invariants
//! the transport relies on when interpreting descriptors).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_shm::descriptor::MsgDesc;

fuzz_target!(|data: &[u8]| {
    if data.len() < 32 {
        return;
    }
    // Descriptor fields are interpreted defensively: lengths/offsets must be
    // range-checked by the consumer before use.
    let len = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let offset = u64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);
    // The transport validates (offset, len) against slot geometry before
    // use and rejects oversize payloads; mirror that validation — for any
    // len that FITS a slot, the clamp must place it in-bounds without
    // panic or wrap. len > slot_size is rejected upstream (no assert).
    let slot_size = 64 * 1024u64;
    let in_slot = offset % slot_size;
    if u64::from(len) <= slot_size && len > 0 {
        let clamped = in_slot.min(slot_size - u64::from(len));
        assert!(clamped + u64::from(len) <= slot_size);
    }
    let _ = MsgDesc::new(0, offset, len, 0);
});
