//! Fuzz target: the h1pool upstream response parser (QA-03
//! extension). Arbitrary bytes must never panic; a successful parse
//! implies a 3-digit status was present.

#![no_main]

use libfuzzer_sys::fuzz_target;
use vane::h1pool;

fuzz_target!(|data: &[u8]| {
    if let Some(resp) = h1pool::parse_response(data) {
        assert!(
            (100..=599).contains(&resp.status),
            "parsed status out of range: {}",
            resp.status
        );
    }
});
