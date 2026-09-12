//! Fuzz target: HTTP/2 frame header + payload validation (QA-03).
//!
//! Arbitrary byte strings must never panic when fed through
//! `parse_header` + `validate_payload` + typed parse helpers, and any
//! successful parse must agree on payload bounds.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_core::h2::frame::{
    parse_header, parse_rst_stream, parse_settings, parse_window_update, validate_payload,
    DEFAULT_MAX_FRAME_SIZE,
};

fuzz_target!(|data: &[u8]| {
    let Ok(hdr) = parse_header(data) else { return };
    let Some(payload) = data.get(9..) else { return };
    // Skip declared bytes we don't have (truncated).
    let payload = &payload[..payload.len().min(hdr.length as usize)];

    if let Ok(split) = validate_payload(&hdr, payload, DEFAULT_MAX_FRAME_SIZE) {
        // Content bounds must always be inside the payload.
        assert!(split.content_start <= split.content_end);
        assert!(split.content_end <= payload.len());
    }

    match hdr.kind.as_u8() {
        0x4 => {
            let _ = parse_settings(payload);
        }
        0x8 => {
            let _ = parse_window_update(payload);
        }
        0x3 => {
            let _ = parse_rst_stream(payload);
        }
        _ => {}
    }
});
