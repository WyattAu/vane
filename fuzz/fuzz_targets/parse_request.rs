//! Fuzz target: HTTP/1.1 request parser (QA-03).
//!
//! Run with cargo-fuzz: `cargo fuzz run parse_request -- -max_len=4096`
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_proto::request::{RequestView, MAX_HEADERS};

fuzz_target!(|data: &[u8]| {
    let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
    if let vane_proto::request::Parsed::Complete(view, head_len) =
        RequestView::parse_in(data, &mut storage)
    {
        // Invariants on successful parses.
        assert!(head_len <= data.len());
        assert!(!view.method.is_empty());
        assert!(view.version <= 1);
        // Header lookups never panic on arbitrary bytes.
        let _ = view.header("host");
        let _ = view.content_length();
        let _ = view.is_chunked();
        let _ = view.wants_close();
        let _ = view.has_body();
    }
});
