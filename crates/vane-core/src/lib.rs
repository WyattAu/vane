//! # vane-core
//!
//! The deterministic transport engine: pinned thread-per-core workers over
//! `io_uring` (with a mio fallback), fixed buffer pools, a generational
//! session slab, lock-free SPSC command paths, and kernel splice passthrough.
//!
//! Layering:
//!
//! ```text
//! ┌────────────────────────────────────────────┐
//! │ vane (bin): HTTP handler, routing, filters │
//! ├────────────────────────────────────────────┤
//! │ vane-core: Handler trait + Worker runtime  │
//! │   engine::uring  engine::mio_engine        │
//! │   buffer pool · session slab · SPSC · splice│
//! └────────────────────────────────────────────┘
//! ```
//!
//! The engine never allocates on the hot path: sessions live in a
//! pre-reserved slab, IO buffers come from a fixed pool (registered with the
//! kernel on io_uring), and cross-thread signaling is lock-free.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod buffer;
pub mod engine;
pub mod h2;
pub mod handler;
pub mod net;
pub mod slab;
pub mod splice;
pub mod spsc;
pub mod token;
pub mod worker;

pub use buffer::{BufferPool, DEFAULT_BUF_SIZE, DEFAULT_POOL_SIZE};
pub use engine::{Cqe, Engine, Poll};
pub use handler::{Handler, HandlerFactory, Mode, SessionIo};
pub use net::{set_keepalive, set_nodelay, tcp_listener};
pub use slab::{SessionSlab, SlabError};
pub use spsc::{SpscReceiver, SpscRing, SpscSender};
pub use token::{Op, Token};
pub use worker::{WorkerCmd, WorkerConfig, WorkerCtx, WorkerHandle, spawn as spawn_worker};

#[cfg(test)]
mod tests {
    #[test]
    fn workspace_compiles() {
        // Marker test: the crate links end-to-end.
    }
}
