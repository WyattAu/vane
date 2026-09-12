//! # vane-router
//!
//! Lock-free dynamic routing (`CP-01`, `CP-02`).
//!
//! Routes live in an immutable, structurally-shared radix trie. The control
//! plane builds the next generation by cloning only the nodes along each new
//! route's path (path-copying), then swaps the root pointer under
//! `crossbeam-epoch`. Workers read with an epoch pin: **zero locks, zero
//! atomic RMWs, ~O(path segments) lookup**.
//!
//! ```text
//! control:  new = old.insert(path, route)   // O(depth) clones
//!           epoch.atomic.store(new)          // one Release store
//! workers:  guard = epoch.pin();             // ~5 ns
//!           root = epoch.atomic.load()       // one Acquire load
//!           root.lookup(path)                // no interference
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod balancer;
pub mod outlier;
pub mod table;
pub mod trie;

pub use balancer::{Backend, Balancer, Policy};
pub use table::{RouteBuilder, RouteEntry, RouteTable, Router, TableEditor};
pub use trie::PathTrie;
