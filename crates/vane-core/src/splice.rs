//! L4 passthrough plumbing — zero user-space data copies (`IO-04`).
//!
//! `pump(from, to)` moves bytes `from → to` through a kernel pipe with
//! `splice(2)` + `SPLICE_F_MOVE`. Data never touches user space: the pipe
//! buffer pages are remapped between sockets. Both engine backends call this
//! on readability; the function is backend-agnostic.

use std::io;
use std::os::fd::RawFd;

/// Result of one pump round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpResult {
    /// Bytes moved this round.
    Moved(u64),
    /// Source exhausted (EOF).
    Eof,
    /// No data / no pipe space right now (backpressure).
    WouldBlock,
    /// Fatal error for this direction.
    Err(i32),
}

thread_local! {
    static PIPES: (RawFd, RawFd) = {
        let mut fds = [0 as RawFd; 2];
        // SAFETY: plain pipe2 with valid out-array.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
        debug_assert_eq!(rc, 0, "pipe2");
        (fds[0], fds[1])
    };
}

fn pipes() -> (RawFd, RawFd) {
    PIPES.with(|p| *p)
}

/// Moves up to `max` bytes `from -> to` without user-space copies.
#[must_use]
pub fn pump(from: RawFd, to: RawFd, max: u64) -> PumpResult {
    let (rp, wp) = pipes();
    let mut total = 0u64;
    while total < max {
        // SAFETY: all fds live; offsets null => current file position for
        // sockets/pipes; flags keep the operation nonblocking with page moves.
        let inn = unsafe {
            libc::splice(
                from,
                std::ptr::null_mut(),
                wp,
                std::ptr::null_mut(),
                (max - total).min(1 << 16) as usize,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
            )
        };
        if inn < 0 {
            let err = io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if err == libc::EAGAIN {
                return if total > 0 {
                    PumpResult::Moved(total)
                } else {
                    PumpResult::WouldBlock
                };
            }
            if err == libc::EINTR {
                continue;
            }
            return PumpResult::Err(err);
        }
        if inn == 0 {
            return PumpResult::Eof;
        }
        // Drain exactly `inn` bytes out of the pipe into the destination.
        let mut left = inn as usize;
        while left > 0 {
            // SAFETY: same as above.
            let out = unsafe {
                libc::splice(
                    rp,
                    std::ptr::null_mut(),
                    to,
                    std::ptr::null_mut(),
                    left,
                    libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
                )
            };
            if out < 0 {
                let err = io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if err == libc::EINTR {
                    continue;
                }
                if err == libc::EAGAIN {
                    // Destination backpressured; spin briefly — the socket
                    // buffer drains at line rate and the loop stays hot-path.
                    std::hint::spin_loop();
                    continue;
                }
                return PumpResult::Err(err);
            }
            if out == 0 {
                return PumpResult::Eof;
            }
            left -= out as usize;
        }
        total += inn as u64;
    }
    PumpResult::Moved(total)
}
