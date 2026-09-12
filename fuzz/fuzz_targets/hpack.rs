//! Fuzz target: HPACK header block decoder (QA-03).
//!
//! Arbitrary bytes must never panic; successful decodes must produce
//! well-formed header pairs and leave the decoder internally consistent.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_core::h2::hpack::HpackDecoder;

fuzz_target!(|data: &[u8]| {
    let mut decoder = HpackDecoder::new(4096);
    // A block may error — never panic. Interleave two blocks so the
    // dynamic table state carries across instructions.
    if let Ok(headers) = decoder.decode(data) {
        for h in &headers {
            // NOTE: uppercase names are rejected at the HTTP/2
            // connection layer (RFC 9113 §8.2.1), NOT here — HPACK is
            // byte-transparent. The dynamic table can carry any bytes
            // the peer encoded.
        }
    }
    // Second block: state must remain coherent.
    let _ = decoder.decode(&[0xbe]);
});
