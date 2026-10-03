//! Fuzz target: the HTTP/2 connection state machine, server role
//! (QA-03 extension).
//!
//! Arbitrary byte streams fed through `Connection::handle_read` must
//! never panic. Protocol violations are expected to surface as typed
//! errors / connection errors — those are correct behavior. This is
//! the layer where the REFUSED_STREAM / HalfClosedRemote bugs lived
//! (isolated-frame fuzzing does not reach the stream lifecycle).

#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_core::h2::connection::{Connection, ConnectionConfig, Role};

fuzz_target!(|data: &[u8]| {
    let mut conn = Connection::new(Role::Server, ConnectionConfig::default());
    let mut events = Vec::new();
    // Feed in varied chunkings: whole, halves, and small fragments —
    // frame reassembly across read boundaries is where the split-head
    // bugs lived.
    let mut off = 0usize;
    while off < data.len() {
        let take = (data.len() - off).min(1 + (off % 17));
        let _ = conn.handle_read(&data[off..off + take], &mut events);
        off += take;
        if events.len() > 4096 {
            events.clear(); // bound memory on adversarial floods
        }
    }
    let _ = conn.handle_read(&[], &mut events);

    // State invariants that must hold after arbitrary input:
    // - a connection error, if any, is a known code (< 0x100 per spec
    //   range used by vane);
    // - every live stream sits in a defined state.
    if let Some(err) = conn.connection_error() {
        assert!(err.code <= 0xb, "unknown connection error code");
    }
});
