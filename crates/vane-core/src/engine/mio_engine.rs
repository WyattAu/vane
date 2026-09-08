//! mio (epoll/kqueue) fallback backend — completion semantics over readiness
//! events (`IO-05`).
//!
//! Each descriptor is registered **once** (read+write interest). Pending
//! operations are queued per fd; on readiness the syscalls run inline
//! against the worker's stable-address buffer pool (the same slots the
//! io_uring backend registers as fixed buffers) and synthetic CQEs are
//! emitted. Upper layers cannot distinguish the two engines.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{IntoRawFd, RawFd};
use std::path::Path;
use std::time::Duration;

use mio::{Events, Interest, Token as MioToken, Waker};

use super::{Cqe, Engine, Poll};
use crate::buffer::BufferPool;
use crate::token::Token;

/// Internal event capacity.
const EVENTS_CAP: usize = 1024;

/// Waker token (shutdown signal from another thread).
const WAKER: usize = u64::MAX as usize;

/// A queued operation waiting for readiness. Keyed by fd, dispatched on
/// matching readiness.
enum Pending {
    /// Socket read into `slot`.
    Read { slot: u32, token: Token },
    /// Write `slot[offset..len]`.
    Write {
        slot: u32,
        len: usize,
        offset: usize,
        token: Token,
    },
    /// Nonblocking connect in progress.
    Connect { token: Token },
    /// One direction of an L4 splice pump: `from -> to`.
    Splice {
        from: RawFd,
        to: RawFd,
        token: Token,
    },
    /// Listener accept readiness.
    Listener { lfd: RawFd, token: Token },
}

/// mio-backed [`Engine`].
pub struct MioEngine {
    poller: mio::Poll,
    #[allow(dead_code)] // future cross-thread wake (admin triggers)
    waker: Waker,
    /// Slot base pointers (stable for the pool's lifetime).
    slot_bases: Vec<*mut u8>,
    buf_size: usize,
    /// RawFd -> armed ops.
    pending: HashMap<RawFd, Vec<Pending>>,
    events: Events,
    cqes: Vec<Cqe>,
}

// SAFETY: slot base pointers point into the worker-owned BufferPool; the
// engine lives and dies on the worker thread which outlives the pool usage.
unsafe impl Send for MioEngine {}

impl MioEngine {
    /// Creates the engine bound to the worker's buffer pool.
    ///
    /// # Errors
    /// Poller creation failure.
    pub fn new(pool: &BufferPool) -> io::Result<Self> {
        let poller = mio::Poll::new()?;
        let waker = Waker::new(poller.registry(), MioToken(WAKER))?;
        // Base addresses only (no dereference here); slots stay valid for
        // the engine's lifetime (worker owns both, same thread).
        let slot_bases = {
            (0..pool.capacity() as u32)
                .map(|i| pool.slot(i).as_ptr() as *mut u8)
                .collect::<Vec<_>>()
        };
        Ok(Self {
            poller,
            waker,
            slot_bases,
            buf_size: pool.buf_size(),
            pending: HashMap::new(),
            events: Events::with_capacity(EVENTS_CAP),
            cqes: Vec::with_capacity(EVENTS_CAP),
        })
    }

    fn slot_ptr(&self, slot: u32) -> *mut u8 {
        self.slot_bases[slot as usize]
    }

    fn register(&mut self, fd: RawFd) -> io::Result<()> {
        let mut src = mio::unix::SourceFd(&fd);
        self.poller.registry().register(
            &mut src,
            MioToken(fd as usize),
            Interest::READABLE.add(Interest::WRITABLE),
        )
    }

    fn push(&mut self, fd: RawFd, op: Pending) {
        self.pending.entry(fd).or_default().push(op);
    }

    fn finish_read(&mut self, fd: RawFd, slot: u32, token: Token) {
        let ptr = self.slot_ptr(slot);
        // SAFETY: slot exclusively ours while its read is in flight; the
        // worker does not touch it until the CQE lands.
        let res = unsafe { libc::read(fd, ptr.cast(), self.buf_size) };
        if res >= 0 {
            self.cqes.push(Cqe {
                token,
                result: Ok(res as u32),
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Keep armed: requeue.
                self.push(fd, Pending::Read { slot, token });
            } else {
                self.cqes.push(Cqe {
                    token,
                    result: Err(err),
                });
            }
        }
    }

    fn finish_write(&mut self, fd: RawFd, slot: u32, len: usize, offset: usize, token: Token) {
        let ptr = self.slot_ptr(slot);
        // SAFETY: slot bytes serialized by the session pre-submit.
        let res = unsafe { libc::write(fd, ptr.add(offset).cast(), len - offset) };
        if res >= 0 {
            let n = res as usize;
            if offset + n >= len {
                self.cqes.push(Cqe {
                    token,
                    result: Ok(len as u32),
                });
            } else {
                self.push(
                    fd,
                    Pending::Write {
                        slot,
                        len,
                        offset: offset + n,
                        token,
                    },
                );
            }
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                self.push(
                    fd,
                    Pending::Write {
                        slot,
                        len,
                        offset,
                        token,
                    },
                );
            } else {
                self.cqes.push(Cqe {
                    token,
                    result: Err(err),
                });
            }
        }
    }

    fn pump_splice(&mut self, from: RawFd, to: RawFd, token: Token) {
        match crate::splice::pump(from, to, 1 << 20) {
            crate::splice::PumpResult::Moved(n) => {
                self.cqes.push(Cqe {
                    token,
                    result: Ok(n as u32),
                });
            }
            crate::splice::PumpResult::Eof => {
                self.cqes.push(Cqe {
                    token,
                    result: Ok(0),
                });
            }
            crate::splice::PumpResult::WouldBlock => { /* rearm below */ }
            crate::splice::PumpResult::Err(code) => {
                self.cqes.push(Cqe {
                    token,
                    result: Err(io::Error::from_raw_os_error(code)),
                });
                return;
            }
        }
        // Stay armed: readiness refires if data remains.
        self.push(from, Pending::Splice { from, to, token });
    }

    /// Runs all armed ops matching an event.
    fn dispatch(&mut self, fd: RawFd, readable: bool, writable: bool) {
        let ops = self.pending.remove(&fd).unwrap_or_default();
        // Priority: connect completion first, then listener/read/write.
        for op in ops {
            match op {
                Pending::Connect { token } => {
                    let err = sock_error(fd);
                    self.cqes.push(Cqe {
                        token,
                        result: if err == 0 {
                            Ok(0)
                        } else {
                            Err(io::Error::from_raw_os_error(err))
                        },
                    });
                }
                Pending::Listener { lfd, token } => {
                    let _ = lfd;
                    if readable {
                        self.cqes.push(Cqe {
                            token,
                            result: Ok(0),
                        });
                    } else {
                        // Keep armed.
                        // (lfd may differ from the event fd only in tests.)
                        self.push(fd, Pending::Listener { lfd, token });
                    }
                }
                Pending::Read { slot, token } => {
                    if readable {
                        self.finish_read(fd, slot, token);
                    } else {
                        self.push(fd, Pending::Read { slot, token });
                    }
                }
                Pending::Write {
                    slot,
                    len,
                    offset,
                    token,
                } => {
                    if writable {
                        self.finish_write(fd, slot, len, offset, token);
                    } else {
                        self.push(
                            fd,
                            Pending::Write {
                                slot,
                                len,
                                offset,
                                token,
                            },
                        );
                    }
                }
                Pending::Splice { from, to, token } => {
                    if readable {
                        self.pump_splice(from, to, token);
                    } else {
                        self.push(fd, Pending::Splice { from, to, token });
                    }
                }
            }
        }
    }
}

fn sock_error(fd: RawFd) -> i32 {
    let mut err: i32 = 0;
    let mut len = std::mem::size_of::<i32>() as libc::socklen_t;
    // SAFETY: fd is a live socket; out-buffer correctly sized.
    unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            std::ptr::addr_of_mut!(err).cast(),
            std::ptr::addr_of_mut!(len),
        );
    }
    err
}

impl Engine for MioEngine {
    fn kind(&self) -> &'static str {
        "mio"
    }

    fn add_listener(&mut self, fd: RawFd, token: Token) -> io::Result<()> {
        let mut src = mio::unix::SourceFd(&fd);
        self.poller
            .registry()
            .register(&mut src, MioToken(fd as usize), Interest::READABLE)?;
        self.push(fd, Pending::Listener { lfd: fd, token });
        Ok(())
    }

    fn add_stream(&mut self, fd: RawFd, _token: Token) -> io::Result<()> {
        // One registration per fd; ops dispatch via the pending table.
        self.register(fd)?;
        Ok(())
    }

    fn read(&mut self, token: Token, fd: RawFd, slot: u32) -> io::Result<Poll> {
        // Edge-triggered: attempt inline; only park on WouldBlock (a data
        // edge will re-fire readiness).
        self.finish_read(fd, slot, token);
        Ok(Poll::Pending)
    }

    fn write(
        &mut self,
        token: Token,
        fd: RawFd,
        slot: u32,
        len: usize,
        offset: usize,
    ) -> io::Result<Poll> {
        // Edge-triggered: attempt inline; park the remainder on WouldBlock.
        self.finish_write(fd, slot, len, offset, token);
        Ok(Poll::Pending)
    }

    fn connect(&mut self, token: Token, addr: SocketAddr) -> io::Result<(RawFd, Poll)> {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let sock =
            socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        sock.set_nonblocking(true)?;
        sock.set_tcp_nodelay(true)?;
        let sa: socket2::SockAddr = addr.into();
        match sock.connect(&sa) {
            Ok(()) => {
                let fd = sock.into_raw_fd();
                self.register(fd)?;
                Ok((fd, Poll::Done(0)))
            }
            Err(e)
                if e.raw_os_error() == Some(libc::EINPROGRESS)
                    || e.kind() == io::ErrorKind::WouldBlock =>
            {
                let fd = sock.into_raw_fd();
                self.register(fd)?;
                self.push(fd, Pending::Connect { token });
                Ok((fd, Poll::Pending))
            }
            Err(e) => Err(e),
        }
    }

    fn connect_unix(&mut self, token: Token, path: &Path) -> io::Result<(RawFd, Poll)> {
        let sock = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        sock.set_nonblocking(true)?;
        let addr = socket2::SockAddr::unix(path)?;
        match sock.connect(&addr) {
            Ok(()) => {
                let fd = sock.into_raw_fd();
                self.register(fd)?;
                Ok((fd, Poll::Done(0)))
            }
            Err(e)
                if e.raw_os_error() == Some(libc::EINPROGRESS)
                    || e.kind() == io::ErrorKind::WouldBlock =>
            {
                let fd = sock.into_raw_fd();
                self.register(fd)?;
                self.push(fd, Pending::Connect { token });
                Ok((fd, Poll::Pending))
            }
            Err(e) => Err(e),
        }
    }

    fn accept(&mut self, lfd: RawFd, ltoken: Token) -> io::Result<Option<(RawFd, SocketAddr)>> {
        // SAFETY: zeroed sockaddr_storage is a valid (unspecified) address.
        let mut sa: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut sa_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        // SAFETY: lfd live; out params sized correctly.
        let fd = unsafe {
            libc::accept4(
                lfd,
                std::ptr::addr_of_mut!(sa).cast(),
                &mut sa_len,
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        };
        if fd >= 0 {
            return Ok(Some((fd, parse_sockaddr(&sa))));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK => {
                // Re-arm accept readiness.
                self.pending
                    .entry(lfd)
                    .or_default()
                    .push(Pending::Listener { lfd, token: ltoken });
                Ok(None)
            }
            Some(libc::EINTR) => self.accept(lfd, ltoken),
            _ => Err(err),
        }
    }

    fn splice_pump(&mut self, a: Token, afd: i32, b: Token, bfd: i32) -> io::Result<()> {
        self.push(
            afd,
            Pending::Splice {
                from: afd,
                to: bfd,
                token: a,
            },
        );
        self.push(
            bfd,
            Pending::Splice {
                from: bfd,
                to: afd,
                token: b,
            },
        );
        Ok(())
    }

    fn remove(&mut self, fd: RawFd) {
        self.pending.remove(&fd);
    }

    fn poll(&mut self, timeout: Option<Duration>, out: &mut Vec<Cqe>) -> io::Result<()> {
        // Inline completions queued since the last round go out first; do
        // not wait behind epoll for them.
        if !self.cqes.is_empty() {
            out.append(&mut self.cqes);
            return Ok(());
        }
        self.poller.poll(&mut self.events, timeout)?;
        // Snapshot readiness (events buffer is reused by the poller).
        let mut ready: Vec<(RawFd, bool, bool)> = Vec::with_capacity(64);
        for ev in self.events.iter() {
            if ev.token() == MioToken(WAKER) {
                continue;
            }
            let fd = ev.token().0 as RawFd;
            ready.push((fd, ev.is_readable(), ev.is_writable()));
        }
        for (fd, readable, writable) in ready {
            self.dispatch(fd, readable, writable);
        }
        out.append(&mut self.cqes);
        Ok(())
    }

    fn take_accepted(&mut self, _fd: RawFd) -> Option<SocketAddr> {
        None // mio hands the peer address back from `accept` directly
    }
}

fn parse_sockaddr(sa: &libc::sockaddr_storage) -> SocketAddr {
    match sa.ss_family as i32 {
        libc::AF_INET => {
            // SAFETY: AF_INET guarantees sockaddr_in layout.
            let a: &libc::sockaddr_in =
                unsafe { &*(sa as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            SocketAddr::from((
                std::net::Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr)),
                u16::from_be(a.sin_port),
            ))
        }
        _ => {
            // SAFETY: AF_INET6 guarantees sockaddr_in6 layout.
            let a: &libc::sockaddr_in6 =
                unsafe { &*(sa as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>() };
            SocketAddr::from((
                std::net::Ipv6Addr::from(a.sin6_addr.s6_addr),
                u16::from_be(a.sin6_port),
            ))
        }
    }
}
