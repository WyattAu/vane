//! Fuzz target: access-log JSON rendering (escaping correctness, QA-03).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_observe::access::{AccessRecord, Str8};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let mut path = &data[..];
    // Split data into path/host/method-ish slices.
    let (host, rest) = path.split_at(path.len() / 2);
    let (method, _trail) = rest.split_at(rest.len().min(8));
    path = host;

    let client = ([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 127, 0, 0, 1], 12345);
    let rec = AccessRecord::now(
        1,
        200,
        1,
        1,
        client,
        None,
        method,
        host,
        path,
        &[],
    );
    let mut out = String::new();
    rec.render_json(&mut out);
    // Invariants: output is valid JSON — no raw control characters,
    // and every backslash starts a legal JSON escape.
    assert!(
        !out.chars().any(|c| c.is_control() && c != '\n'),
        "raw control char: {out:?}"
    );
    let mut chars = out.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            let esc = chars.next().unwrap_or('X');
            assert!(
                matches!(esc, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' | 'u'),
                "invalid JSON escape in: {out:?}"
            );
            if esc == 'u' {
                let hex: String = chars.by_ref().take(4).collect();
                assert!(
                    hex.chars().all(|h| h.is_ascii_hexdigit()),
                    "invalid unicode escape in: {out:?}"
                );
            }
        }
    }

});
