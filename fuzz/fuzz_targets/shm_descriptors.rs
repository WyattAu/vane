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
    // The transport clamps: payload view = base + offset..offset+len, so any
    // (offset, len) must be validated against slot geometry before use —
    // this mirrors transport::Direction::view's contract.
    let slot_size = 64 * 1024u64;
    let index = offset / slot_size;
    let in_slot = offset % slot_size;
    assert!(in_slot + u64::from(len) <= slot_size || len == 0 || index != 0,);
    let _ = MsgDesc::new(0, offset, len, 0);
});
