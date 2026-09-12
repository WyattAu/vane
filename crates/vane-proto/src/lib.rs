//! # vane-proto
//!
//! Zero-copy HTTP/1.1 engine (`PR-01`): header parsing via `httparse`
//! (SIMD-accelerated) over the worker's fixed buffer, with borrowed views
//! for every field — no allocation between kernel and routing decision.
//!
//! ```text
//! kernel → fixed buffer slot → [u8] ─┬─ RequestView (borrows)
//!                                    └─ header walk (zero copy)
//! ```
//!
//! Response serialization writes straight into pool slots: status line +
//! headers are formatted by hand (single-digit-ns `format`-free paths for
//! the common cases).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod compression;
pub mod date;
pub mod request;
pub mod response;

pub use request::RequestView;
pub use response::{Status, UpstreamHead, parse_upstream_head, write_full, write_head};
