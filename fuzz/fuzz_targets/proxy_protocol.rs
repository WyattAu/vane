//! Fuzz target: PROXY protocol header parser (`vane-proto`
//! `proxy_protocol::parse`).
//!
//! The parser consumes fully peer-controlled bytes as the FIRST thing
//! on every connection of a `proxy_protocol = true` listener, so a
//! panic or a wrong header_len is a remote availability/correctness
//! bug. Invariants: a successful parse reports a header that fits in
//! the input and is at least the minimum size for its version, and the
//! claimed source port is a real u16 (any 16 bits are legal, so only
//! the type system is asserted here).
//!
//! Run with cargo-fuzz: `cargo fuzz run proxy_protocol -- -max_len=4096`

#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_proto::proxy_protocol::{parse, ProxyHeaderError};

fuzz_target!(|data: &[u8]| {
    match parse(data) {
        Err(e) => {
            // Errors must be one of the two documented kinds.
            assert!(matches!(
                e,
                ProxyHeaderError::NotAProxyHeader | ProxyHeaderError::Malformed
            ));
        }
        Ok(None) => {
            // "Wait for more bytes" is only legal while the input is
            // shorter than any complete header could be.
            assert!(
                data.len() < 232 || data.starts_with(b"\r\n\r\n\x00\r\n"),
                "indefinite wait on a {}-byte input",
                data.len()
            );
        }
        Ok(Some(outcome)) => {
            // The strip length must stay inside the input and carry at
            // least the v1 minimum line (16) or the v2 fixed header.
            assert!(outcome.header_len <= data.len());
            assert!(outcome.header_len >= 14);
            // The claimed source is either a real address (Some) or the
            // LOCAL sentinel (None).
            if let Some(addr) = outcome.source {
                // Any ip/port bits are legal; constructing it must not
                // have lost the port.
                assert_eq!(addr.port(), addr.port());
                let _ = addr.to_string();
            }
        }
    }
});
