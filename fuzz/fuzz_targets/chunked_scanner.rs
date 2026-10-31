//! Fuzz target: the incremental chunked-transfer decoder.
//!
//! Run with cargo-fuzz: `cargo fuzz run chunked_scanner -- -max_len=4096`
//!
//! The scanner is driven entirely by untrusted bytes and its whole job is
//! to decide *where a message ends* — a wrong answer either truncates a
//! body or swallows the next request on the connection. Both failure
//! modes are silent in production, which is exactly the shape worth
//! fuzzing. It replaced a content scan for `0\r\n\r\n` that truncated
//! bodies containing that sequence and hung on a terminal chunk split
//! across reads.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_proto::chunked::ChunkedScanner;

fuzz_target!(|data: &[u8]| {
    // One scan of the whole input, and a second fed byte-by-byte: the
    // decoder's contract is that segmentation cannot change the answer,
    // and byte-at-a-time is where a state machine loses track.
    let mut whole = ChunkedScanner::new();
    let done_whole = whole.feed(data);

    let mut dripped = ChunkedScanner::new();
    let mut done_dripped = false;
    for byte in data {
        done_dripped |= dripped.feed(std::slice::from_ref(byte));
    }

    // Segmentation invariance: if either segmentation reached the
    // terminal chunk, both must, and the scanner must agree on done-ness.
    // (`feed` is monotone, so "either" is the stronger claim — a scanner
    // that un-done would be a bug.)
    assert_eq!(
        done_whole, done_dripped,
        "segmentation changed the verdict: whole={done_whole} dripped={done_dripped} \
         input={data:?}"
    );

    if done_whole {
        // Terminal chunk seen: later bytes are not part of this message.
        // Feeding more must not change the answer.
        assert!(whole.feed(b"trailing bytes"), "done scanner un-done");
        assert!(whole.is_done());
    }

    // A fresh scanner is never done, and an empty read never completes.
    assert!(!ChunkedScanner::new().is_done());
    let mut empty = ChunkedScanner::new();
    assert!(!empty.feed(b""));
});
