//! # vane-shm
//!
//! Shared-memory sidecar transport (`IP-01`) and zero-loss hot upgrade
//! (`IP-02`).
//!
//! ## Transport topology
//!
//! ```text
//! client process                          vane sidecar
//! ┌────────────────────┐                 ┌────────────────────┐
//! │  SidecarClient     │──req ring──────▶│  SidecarServer     │
//! │                    │                 │                    │
//! │  alloc◀─free ring──│                 │  alloc◀─free ring──│
//! │  (resp arena)      │──resp ring─────▶│  (req arena)       │
//! └────────────────────┘                 └────────────────────┘
//! ```
//!
//! Two `shm-rings` SPMC rings carry fixed-size descriptors
//! (`{id, offset, len}`); payloads live in a shared byte arena. Each
//! direction's arena is recycled via a *free ring*: the consumer pushes
//! freed offsets back to the producer, so allocation is "wait until the
//! peer released enough space" — lock-free, no heap, zero copies after
//! the handler writes its response straight into shared memory.
//!
//! ## Hot upgrade
//!
//! `handover::send` passes listening socket fds over `SCM_RIGHTS` and
//! archives the route table into a state file; the new binary
//! `handover::receive`s the fds and rebuilds the router with **zero
//! dropped packets** (connections never terminate; accepts just pause
//! for microseconds).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod cabi;
pub mod descriptor;
pub mod fdpass;
pub mod handover;
pub mod transport;

pub use descriptor::MsgDesc;
pub use transport::{SidecarClient, SidecarConfig, SidecarServer};
