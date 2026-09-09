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
    /// Owned sockaddr storage per in-flight connect (token bits -> (addr, len)).
    /// io_uring copies the sockaddr at SUBMISSION time, not at SQE build
    /// time — a stack-local sockaddr would dangle between `push` and
    /// `submit` (use-after-free manifesting as EAFNOSUPPORT under load).
    connect_addrs: HashMap<u64, Box<ConnectAddr>>,
}

/// Owned connect address for one in-flight `Connect` SQE.
struct ConnectAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

/// Serializes a `SocketAddr` into owned storage, returning the box plus a
/// pointer/len pair valid for as long as the box lives.
///
/// # Safety
/// The caller must keep the returned box alive (in `connect_addrs`) until
/// the op completes. The heap address is stable across moves.
unsafe fn connect_addr_boxed(
    addr: SocketAddr,
) -> (Box<ConnectAddr>, *const libc::sockaddr, libc::socklen_t) {
    // SAFETY: fully initialized for the active family below.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            // SAFETY: family matches the written layout.
            let sa: &mut libc::sockaddr_in =
                unsafe { &mut *std::ptr::addr_of_mut!(storage).cast::<libc::sockaddr_in>() };
            sa.sin_family = libc::AF_INET as _;
            sa.sin_port = v4.port().to_be();
            sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            // SAFETY: family matches the written layout.
            let sa: &mut libc::sockaddr_in6 =
                unsafe { &mut *std::ptr::addr_of_mut!(storage).cast::<libc::sockaddr_in6>() };
            sa.sin6_family = libc::AF_INET6 as _;
            sa.sin6_port = v6.port().to_be();
            sa.sin6_addr.s6_addr = v6.ip().octets();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    };
    let boxed = Box::new(ConnectAddr { storage, len });
    // The heap address is stable for the box's lifetime (per the function
    // contract the caller stores it in `connect_addrs`); `addr_of!` on a
    // place expression needs no unsafe block.
    let ptr = std::ptr::addr_of!(boxed.storage).cast::<libc::sockaddr>();
    let out_len = boxed.len;
    (boxed, ptr, out_len)
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
            connect_addrs: HashMap::new(),
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
        // The sockaddr must live in owned storage until the op completes:
        // io_uring copies it at submit time, not when the SQE is built.
        // SAFETY: the box is stored in `connect_addrs` for the op lifetime.
        let (owned, ptr, len) = unsafe { connect_addr_boxed(addr) };
        let entry = io_uring::opcode::Connect::new(Fd(fd), ptr, len).build();
        self.connect_addrs.insert(token.bits(), owned);
        self.push(entry, token);
        Ok((fd, Poll::Pending))
    }

    fn connect_unix(&mut self, token: Token, path: &Path) -> io::Result<(RawFd, Poll)> {
        let sock = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        sock.set_nonblocking(true)?;
        let fd = sock.into_raw_fd();
        let sa = socket2::SockAddr::unix(path)?;
        // SAFETY: raw sockaddr bytes are copied into owned storage, kept in
        // `connect_addrs` for the op lifetime.
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let bytes = sa.as_ptr().cast::<u8>();
        let copy_len = (sa.len() as usize).min(std::mem::size_of::<libc::sockaddr_storage>());
        // SAFETY: sa is a valid sockaddr of `sa.len()` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes, std::ptr::addr_of_mut!(storage).cast(), copy_len)
        };
        let len = sa.len();
        let owned = Box::new(ConnectAddr { storage, len });
        // Heap address is stable while `owned` lives in `connect_addrs`.
        let ptr = std::ptr::addr_of!(owned.storage).cast::<libc::sockaddr>();
        let entry = io_uring::opcode::Connect::new(Fd(fd), ptr, len).build();
        self.connect_addrs.insert(token.bits(), owned);
        self.push(entry, token);
        Ok((fd, Poll::Pending))
    }

    fn accept(&mut self, _lfd: RawFd, ltoken: Token) -> io::Result<Option<(RawFd, SocketAddr)>> {
        // Return one completed accept if the poll loop queued it.
        let keys: Vec<RawFd> = self.accepted.keys().copied().collect();
        if let Some(fd) = keys.into_iter().next() {
            let addr = self.accepted.remove(&fd).expect("just listed");
            return Ok(Some((fd, addr)));
        }
        // Not ready yet. Do NOT re-arm here: exactly one accept SQE is
        // outstanding per listener at all times (armed in `add_listener`,
        // re-armed on every completion in `poll`). Re-arming per call would
        // accumulate unbounded SQEs and exhaust the submission queue.
        let _ = ltoken;
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
            // In-flight connect storage is safe to release once its CQE
            // has been observed.
            if token.op() == crate::token::Op::Connect {
                self.connect_addrs.remove(&token.bits());
            }
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
