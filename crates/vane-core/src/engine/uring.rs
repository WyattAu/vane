//! io_uring backend (`IO-01`..`03`): one ring per worker, fixed registered
//! buffers, SQPOLL optional.
//!
//! Reads use `IORING_OP_READ_FIXED` (`opcode::ReadFixed`) with
//! `buf_index = slot`, so the kernel writes directly into the worker's
//! pre-allocated pool — zero per-op buffer mapping. Writes use
//! `WRITE_FIXED` symmetrically. L4 splice pumping arms multishot `PollAdd`
//! readiness and runs kernel-only `splice(2)` loops inline — request bytes
//! never enter user space (`IO-04`).

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{IntoRawFd, RawFd};
use std::path::Path;
use std::time::Duration;

use io_uring::types::{Fd, SubmitArgs, Timespec};

use super::{Cqe, Engine, Poll};
use crate::buffer::BufferPool;
use crate::splice;
use crate::token::Token;

/// Listener state (accept re-arms after each connection).
struct Listener {
    fd: RawFd,
    #[allow(dead_code)] // re-arm identity (kept for symmetry with mio)
    token: Token,
    /// sockaddr output buffer the kernel fills for each accepted connection.
    sa: Box<[u8; 128]>,
    sa_len: Box<libc::socklen_t>,
}

/// io_uring-backed [`Engine`].
pub struct UringEngine {
    ring: io_uring::IoUring,
    /// Slot base pointers (registered as kernel fixed buffers).
    slot_bases: Vec<*mut u8>,
    buf_size: usize,
    listeners: HashMap<u64, Listener>,
    /// Splice direction per token: bits -> (from_fd, to_fd).
    splice_dirs: HashMap<u64, (RawFd, RawFd)>,
    /// Completed accepts awaiting pickup: fd -> peer.
    accepted: HashMap<RawFd, SocketAddr>,
}

// SAFETY: slot pointers live in the worker-owned pool; single-thread use.
unsafe impl Send for UringEngine {}

impl UringEngine {
    /// Builds the ring and registers the buffer pool as kernel fixed buffers.
    ///
    /// # Errors
    /// Ring creation or registration failure (e.g., SQPOLL denied for the
    /// current user — the runtime falls back per `IO-05`).
    pub fn new(entries: u32, pool: Option<&BufferPool>, sqpoll: bool) -> io::Result<Self> {
        let ring = if sqpoll {
            io_uring::IoUring::builder()
                .setup_sqpoll(2_000)
                .build(entries)?
        } else {
            io_uring::IoUring::new(entries)?
        };
        let (slot_bases, buf_size) = match pool {
            Some(pool) => {
                // Base addresses only (no dereference); slots outlive the ring.
                let bases = (0..pool.capacity() as u32)
                    .map(|i| pool.slot(i).as_ptr() as *mut u8)
                    .collect::<Vec<_>>();
                let iovecs: Vec<libc::iovec> = bases
                    .iter()
                    .map(|p| libc::iovec {
                        // SAFETY: pointer valid for buf_size bytes.
                        iov_base: (*p).cast(),
                        iov_len: pool.buf_size(),
                    })
                    .collect();
                // SAFETY: iovecs reference stable slot storage.
                unsafe {
                    ring.submitter().register_buffers(&iovecs)?;
                }
                (bases, pool.buf_size())
            }
            None => (Vec::new(), 0),
        };
        Ok(Self {
            ring,
            slot_bases,
            buf_size,
            listeners: HashMap::new(),
            splice_dirs: HashMap::new(),
            accepted: HashMap::new(),
        })
    }

    /// Queues an SQE (no syscall); `poll` batches the submit. Under SQPOLL
    /// the kernel thread picks entries up without any syscall at all.
    ///
    /// Caller contract (checked at each call site): the entry's buffers and
    /// fds must remain valid until its CQE is consumed on this thread.
    fn push(&mut self, entry: io_uring::squeue::Entry, token: Token) {
        let entry = entry.user_data(token.bits());
        loop {
            // SAFETY: entry pushed exactly once; completion consumed here.
            unsafe {
                if self.ring.submission().push(&entry).is_ok() {
                    return;
                }
            }
            // SQ full: flush to the kernel and retry.
            let _ = self.ring.submit();
            std::hint::spin_loop();
        }
    }

    fn slot_ptr(&self, slot: u32) -> *mut u8 {
        self.slot_bases[slot as usize]
    }

    fn arm_accept(&mut self, bits: u64) {
        let Some(l) = self.listeners.get_mut(&bits) else {
            return;
        };
        let fd = l.fd;
        let sa_ptr = l.sa.as_mut_ptr();
        let len_ptr: *mut libc::socklen_t = &mut *l.sa_len;
        // SAFETY contract for `push`: sa/sa_len are stable worker-owned
        // buffers, valid until the CQE is consumed on this thread.
        let entry = io_uring::opcode::Accept::new(Fd(fd), sa_ptr.cast(), len_ptr)
            .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
            .build();
        self.push(entry, Token::from_bits(bits));
    }

    fn arm_readiness(&mut self, fd: RawFd, token: Token) {
        // SAFETY contract for `push`: fd live until session close; multishot
        // poll re-arms itself.
        let entry = io_uring::opcode::PollAdd::new(Fd(fd), libc::POLLIN as u32)
            .multi(true)
            .build();
        self.push(entry, token);
    }
}

impl Engine for UringEngine {
    fn kind(&self) -> &'static str {
        "io_uring"
    }

    fn add_listener(&mut self, fd: RawFd, token: Token) -> io::Result<()> {
        let bits = token.bits();
        self.listeners.insert(
            bits,
            Listener {
                fd,
                token,
                sa: Box::new([0u8; 128]),
                sa_len: Box::new(128),
            },
        );
        self.arm_accept(bits);
        Ok(())
    }

    fn add_stream(&mut self, _fd: RawFd, _token: Token) -> io::Result<()> {
        // Connected sockets pass per-op; IORING_REGISTER_FILES is a
        // follow-up optimization needing stable fd slots per session.
        Ok(())
    }

    fn read(&mut self, token: Token, fd: RawFd, slot: u32) -> io::Result<Poll> {
        let ptr = self.slot_ptr(slot);
        // SAFETY contract for `push`: the fixed-buffer slot is exclusively
        // owned while the op is in flight; the kernel writes the registered
        // buffer directly.
        let entry =
            io_uring::opcode::ReadFixed::new(Fd(fd), ptr, self.buf_size as u32, slot as u16)
                .offset(0)
                .build();
        self.push(entry, token);
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
        let ptr = self.slot_ptr(slot);
        // SAFETY: fixed buffer (bytes serialized by the session pre-submit);
        // pointer arithmetic stays within the registered slot.
        let entry = unsafe {
            io_uring::opcode::WriteFixed::new(
                Fd(fd),
                ptr.add(offset),
                (len - offset) as u32,
                slot as u16,
            )
            .offset(0)
            .build()
        };
        self.push(entry, token);
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
        let fd = sock.into_raw_fd();
        let sa: socket2::SockAddr = addr.into();
        // SAFETY contract for `push`: sockaddr bytes are copied at
        // submission time by the kernel.
        let entry = io_uring::opcode::Connect::new(Fd(fd), sa.as_ptr().cast(), sa.len()).build();
        self.push(entry, token);
        Ok((fd, Poll::Pending))
    }

    fn connect_unix(&mut self, token: Token, path: &Path) -> io::Result<(RawFd, Poll)> {
        let sock = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        sock.set_nonblocking(true)?;
        let fd = sock.into_raw_fd();
        let sa = socket2::SockAddr::unix(path)?;
        // SAFETY contract for `push`: as above.
        let entry = io_uring::opcode::Connect::new(Fd(fd), sa.as_ptr().cast(), sa.len()).build();
        self.push(entry, token);
        Ok((fd, Poll::Pending))
    }

    fn accept(&mut self, _lfd: RawFd, ltoken: Token) -> io::Result<Option<(RawFd, SocketAddr)>> {
        // Return one completed accept if the poll loop queued it.
        if let Some((&fd, &addr)) = self.accepted.iter().next() {
            self.accepted.remove(&fd);
            return Ok(Some((fd, addr)));
        }
        // Not ready yet — (re)arm the accept SQE.
        self.arm_accept(ltoken.bits());
        Ok(None)
    }

    fn splice_pump(&mut self, a: Token, afd: i32, b: Token, bfd: i32) -> io::Result<()> {
        self.splice_dirs.insert(a.bits(), (afd, bfd));
        self.splice_dirs.insert(b.bits(), (bfd, afd));
        self.arm_readiness(afd, a);
        self.arm_readiness(bfd, b);
        Ok(())
    }

    fn remove(&mut self, fd: RawFd) {
        self.accepted.remove(&fd);
    }

    fn poll(&mut self, timeout: Option<Duration>, out: &mut Vec<Cqe>) -> io::Result<()> {
        // Flush submissions (no-op under SQPOLL — kernel thread drains).
        self.ring.submit()?;

        match timeout {
            None => match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            },
            Some(d) => {
                // Bounded wait via IORING_ENTER_EXT_ARG; ETIME = clean timeout.
                let ms = d.as_millis().min(60_000) as u64;
                let ts = Timespec::new()
                    .sec(ms / 1_000)
                    .nsec((ms % 1_000) as u32 * 1_000_000);
                let args = SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(e) if e.raw_os_error() == Some(libc::ETIME) => return Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        // Fallback for kernels without EXT_ARG: plain wait.
                        self.ring.submit_and_wait(0)?;
                    }
                }
            }
        }

        // Drain completions.
        let mut accept_hits: Vec<(Token, RawFd)> = Vec::new();
        for cqe in self.ring.completion() {
            let token = Token::from_bits(cqe.user_data());
            let raw = cqe.result();
            let result = if raw >= 0 {
                Ok(raw as u32)
            } else {
                Err(io::Error::from_raw_os_error(-raw))
            };
            match token.op() {
                crate::token::Op::Accept => {
                    if raw >= 0 {
                        accept_hits.push((token, raw as RawFd));
                    } else if -raw != libc::ECANCELED {
                        out.push(Cqe { token, result });
                    }
                }
                crate::token::Op::Splice => {
                    if raw < 0 {
                        if -raw != libc::ECANCELED {
                            out.push(Cqe { token, result });
                        }
                    } else if let Some(&(from, to)) = self.splice_dirs.get(&token.bits()) {
                        match splice::pump(from, to, 1 << 20) {
                            splice::PumpResult::Moved(n) => {
                                out.push(Cqe {
                                    token,
                                    result: Ok(n as u32),
                                });
                            }
                            splice::PumpResult::Eof => {
                                out.push(Cqe {
                                    token,
                                    result: Ok(0),
                                });
                            }
                            splice::PumpResult::WouldBlock => {}
                            splice::PumpResult::Err(code) => out.push(Cqe {
                                token,
                                result: Err(io::Error::from_raw_os_error(code)),
                            }),
                        }
                    }
                }
                _ => out.push(Cqe { token, result }),
            }
        }

        // Materialize accepted connections and re-arm listeners.
        for (token, fd) in accept_hits {
            let bits = token.bits();
            let addr = self.listeners.get(&bits).map_or_else(
                || SocketAddr::from(([0, 0, 0, 0], 0)),
                |l| parse_sockaddr(&l.sa),
            );
            self.accepted.insert(fd, addr);
            self.arm_accept(bits);
        }
        Ok(())
    }

    fn take_accepted(&mut self, fd: RawFd) -> Option<SocketAddr> {
        self.accepted.remove(&fd)
    }
}

fn parse_sockaddr(buf: &[u8; 128]) -> SocketAddr {
    // SAFETY: buffer is sockaddr_storage sized.
    let sa: &libc::sockaddr_storage = unsafe { &*buf.as_ptr().cast() };
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
