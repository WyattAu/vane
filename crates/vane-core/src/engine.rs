//! The `AsyncEngine` abstraction — completion-based transport (`IO-01`, `IO-05`).
//!
//! Two backends implement the same interface:
//!
//! - [`io_uring backend`](uring) (`IO-01`..`03`): one ring per worker,
//!   `IORING_SETUP_SQPOLL` optional, fixed buffers (`IORING_REGISTER_BUFFERS`)
//!   so steady-state reads/writes never enter the kernel allocator.
//! - [`mio backend`](mio_engine): edge-triggered epoll/kqueue emulation of
//!   the same completion semantics, selected automatically when io_uring is
//!   unavailable or administratively disabled.
//!
//! The worker drives either through identical CQE dispatch, which is what
//! keeps the HTTP/L4 state machines engine-agnostic.

pub mod mio_engine;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod uring;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use crate::buffer::BufferPool;
use crate::token::Token;

/// Slow-path warning; vane-core stays dependency-light by design.
macro_rules! tracing_slow_warn {
    ($($arg:tt)*) => {
        eprintln!("[vane:warn] {}", format_args!($($arg)*))
    };
}

/// Result of an engine operation that may complete inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Poll {
    /// Completed immediately with the given byte/fd count.
    Done(u32),
    /// Queued; completion will arrive as a CQE with the same token.
    Pending,
}

/// A completion event.
#[derive(Debug)]
pub struct Cqe {
    /// Token submitted with the operation.
    pub token: Token,
    /// Kernel result: bytes transferred, new fd (accept), or 0.
    pub result: io::Result<u32>,
}

/// Transport engine — owned and driven by a single worker thread.
pub trait Engine {
    /// Backend name for diagnostics (`io_uring` / `mio`).
    fn kind(&self) -> &'static str;

    /// Registers a nonblocking listening socket for accept readiness.
    ///
    /// # Errors
    /// Backend registration failure.
    fn add_listener(&mut self, fd: std::os::fd::RawFd, token: Token) -> io::Result<()>;

    /// Registers a connected socket for read/write readiness.
    ///
    /// # Errors
    /// Backend registration failure.
    fn add_stream(&mut self, fd: std::os::fd::RawFd, token: Token) -> io::Result<()>;

    /// Queues one read into pool `slot`. The completion yields bytes read;
    /// `0` means EOF.
    ///
    /// # Errors
    /// Submission failure (not `EAGAIN` — that becomes `Pending`).
    fn read(&mut self, token: Token, fd: std::os::fd::RawFd, slot: u32) -> io::Result<Poll>;

    /// Queues a write of `slot[0..len]`, resuming from a prior partial write
    /// when the backend tracks one for this token.
    ///
    /// # Errors
    /// Submission failure (not `EAGAIN`).
    fn write(
        &mut self,
        token: Token,
        fd: std::os::fd::RawFd,
        slot: u32,
        len: usize,
        offset: usize,
    ) -> io::Result<Poll>;

    /// Starts a nonblocking `connect(2)`; completion is a CQE where
    /// `result == Ok(0)` means established.
    ///
    /// # Errors
    /// Socket creation or connect submission failure.
    fn connect(
        &mut self,
        token: Token,
        addr: std::net::SocketAddr,
    ) -> io::Result<(std::os::fd::RawFd, Poll)>;

    /// Starts a nonblocking UDS `connect(2)` to `path`.
    ///
    /// # Errors
    /// Socket creation or connect submission failure.
    fn connect_unix(
        &mut self,
        token: Token,
        path: &std::path::Path,
    ) -> io::Result<(std::os::fd::RawFd, Poll)>;

    /// Attempts an accept on a registered listener; `Ok(Some(..))` completes
    /// inline, `Ok(None)` waits for a CQE on the listener token.
    ///
    /// # Errors
    /// Fatal (non-`EAGAIN`) accept error.
    fn accept(
        &mut self,
        lfd: std::os::fd::RawFd,
        ltoken: Token,
    ) -> io::Result<Option<(std::os::fd::RawFd, SocketAddr)>>;

    /// Starts a bidirectional zero-copy splice pump between two registered
    /// streams (`IO-04`). Completions on either token report bytes moved;
    /// `Ok(0)` signals EOF for that direction.
    ///
    /// # Errors
    /// Submission failure.
    fn splice_pump(&mut self, a: Token, afd: i32, b: Token, bfd: i32) -> io::Result<()>;

    /// Removes a descriptor (before the worker closes it).
    fn remove(&mut self, fd: std::os::fd::RawFd);

    /// Drives the backend, filling `out` with completions. Blocks up to
    /// `timeout` (or indefinitely when `None`).
    ///
    /// # Errors
    /// Backend event-loop failure (unrecoverable; worker exits).
    fn poll(&mut self, timeout: Option<Duration>, out: &mut Vec<Cqe>) -> io::Result<()>;

    /// Pops addresses of accepted connections reported by accept CQEs.
    fn take_accepted(&mut self, fd: std::os::fd::RawFd) -> Option<SocketAddr>;
}

/// Creates the best available engine for this system (`IO-05` fallback).
///
/// Prefers io_uring (when compiled in and permitted — `SQPOLL` needs a
/// privileged or unbounded user); falls back to mio otherwise.
pub fn create_engine(
    entries: u32,
    buffers: Option<&BufferPool>,
    sqpoll: bool,
) -> io::Result<Box<dyn Engine>> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        match uring::UringEngine::new(entries, buffers, sqpoll) {
            Ok(e) => return Ok(Box::new(e)),
            Err(err) => {
                tracing_slow_warn!("io_uring unavailable ({}), falling back to mio", err);
            }
        }
    }
    let _ = (entries, sqpoll);
    let fallback = buffers.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "mio fallback requires a buffer pool",
        )
    })?;
    Ok(Box::new(mio_engine::MioEngine::new(fallback)?))
}
