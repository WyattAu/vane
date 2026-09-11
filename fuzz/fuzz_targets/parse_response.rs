//! Fuzz target: upstream HTTP/1.1 response head parser (QA-03).
//!
//! Run with cargo-fuzz: `cargo fuzz run parse_response -- -max_len=4096`
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_proto::response::{MAX_RESPONSE_HEADERS, parse_upstream_head};

fuzz_target!(|data: &[u8]| {
    let mut storage = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
    if let Ok(Some(head)) = parse_upstream_head(data, &mut storage) {
        // Invariants on successful parses.
        assert!(head.head_len <= data.len());
        assert!(head.code <= 999);
    }
});
