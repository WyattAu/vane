//! Native HTTP/2 engine (`PR-02`): HPACK, frame codec, and the
//! connection/stream state machines over the completion engine.

pub mod hpack;
pub mod huffman;
