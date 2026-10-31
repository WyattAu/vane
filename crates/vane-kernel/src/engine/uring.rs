//! io_uring backend (`IO-01`..`03`): one ring per worker, fixed registered
//! buffers, SQPOLL optional.
//!
//! Reads use `IORING_OP_READ_FIXED` (`opcode::ReadFixed`) with
//! `buf_index = slot`, so the kernel writes directly into the worker's
//! pre-allocated pool — zero per-op buffer mapping. Writes use
//! `WRITE_FIXED` symmetrically. L4 splice pumping arms multishot `PollAdd`
//! readiness and runs kernel-only `splice(2)` loops inline — request bytes
//! never enter user space (`IO-04`).
//!
//! PARKED io_uring features (research loop iteration 7, 2026-10; see
//! docs/research-loop-2026-10.md):
//!
//! * `SEND_ZC` (kernel 6.4+): zero-copy send, 1.9-2.4x over msg_zerocopy
//!   in kernel selftests, best at large payloads. Trigger: a measured
//!   large-body (≥64 KiB) CPU regression on the chunked relay path.
//!   Plays well with our registered fixed buffers (the patchset skips
//!   get/put_page for them).
//! * Zero-copy RECEIVE (`RECV_ZC`, kernel 6.14+): removes the
//!   kernel-to-user copy but binds an ifq per NIC hardware queue with
//!   flow steering — in conflict with vane's SO_REUSEPORT
//!   thread-per-core accept model, where any worker takes any flow.
//!   Trigger: NIC + measured memcpy cost on the receive path.

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
    fn watch(&mut self, fd: RawFd, token: Token, events: u32) -> io::Result<()> {
        // SAFETY contract for `push`: fd live until session close;
        // multishot poll re-arms itself.
        let entry = io_uring::opcode::PollAdd::new(Fd(fd), events)
            .multi(true)
            .build();
        self.push(entry, token);
        Ok(())
    }

    fn unwatch(&mut self, fd: RawFd, token: Token) -> io::Result<()> {
        // PollRemove targets the poll's OWN user_data; the completion
        // comes back under the cancel token and is filtered.
        let _ = fd;
        let remove = io_uring::opcode::PollRemove::new(token.bits()).build();
        let done = Token::new(
            crate::token::Op::BodySpliceCancel,
            token.slot(),
            token.generation(),
            0,
        );
        self.push(remove, done);
        Ok(())
    }

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
        // Directions are keyed by token bits: identical tokens would
        // silently overwrite one direction.
        debug_assert_ne!(a.bits(), b.bits(), "splice directions need distinct tokens");
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
                crate::token::Op::BodySpliceCancel => {
                    // Removal completion: Ok(0) removed, -ENOENT nothing
                    // was armed. Either way, nothing to dispatch.
                }
                op @ (crate::token::Op::Splice
                | crate::token::Op::BodySpliceIn
                | crate::token::Op::BodySpliceOut) => {
                    if raw < 0 {
                        if -raw != libc::ECANCELED {
                            out.push(Cqe { token, result });
                        }
                    } else if op == crate::token::Op::Splice {
                        if let Some(&(from, to)) = self.splice_dirs.get(&token.bits()) {
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
                    } else {
                        // Body-splice readiness: the worker issues the
                        // capped splices itself (it owns the pipe and
                        // the remaining-byte budget).
                        out.push(Cqe {
                            token,
                            result: Ok(raw as u32),
                        });
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

#[cfg(test)]
mod fault_tests {
    use super::*;
    use crate::buffer::DEFAULT_BUF_SIZE;
    use crate::token::Op;

    fn test_engine() -> (BufferPool, UringEngine) {
        let pool = BufferPool::new(8, DEFAULT_BUF_SIZE).expect("pool");
        let engine = UringEngine::new(64, Some(&pool), false).expect("uring available");
        (pool, engine)
    }

    fn tok(op: Op) -> Token {
        Token::new(op, 0, 0, 0)
    }

    fn sockpair() -> (RawFd, RawFd) {
        let mut fds = [0 as RawFd; 2];
        // SAFETY: plain socketpair with valid out-array.
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        for fd in fds {
            // SAFETY: fcntl on a live fd.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            // SAFETY: same live fd; only adds O_NONBLOCK.
            unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        }
        (fds[0], fds[1])
    }

    fn close(fd: RawFd) {
        // SAFETY: test owns the fd.
        unsafe { libc::close(fd) };
    }

    fn drain(engine: &mut UringEngine, secs: u64) -> Vec<Cqe> {
        let mut out = Vec::new();
        engine
            .poll(Some(std::time::Duration::from_secs(secs)), &mut out)
            .expect("poll");
        out
    }

    /// Polls until `pred` matches a CQE or the attempt budget runs out
    /// (kernels race completion delivery against `submit_and_wait`).
    fn drain_until(engine: &mut UringEngine, mut pred: impl FnMut(&Cqe) -> bool) -> Vec<Cqe> {
        let mut all = Vec::new();
        for _ in 0..60 {
            let mut out = Vec::new();
            engine
                .poll(Some(std::time::Duration::from_millis(100)), &mut out)
                .expect("poll");
            if out.iter().any(&mut pred) {
                all.extend(out);
                return all;
            }
            all.extend(out);
        }
        all
    }

    #[test]
    fn write_then_read_roundtrip() {
        let (_pool, mut engine) = test_engine();
        let (a, b) = sockpair();
        let wt = tok(Op::DownstreamWrite);
        assert!(matches!(
            engine.write(wt, a, 0, 32, 0).expect("write"),
            Poll::Pending
        ));
        let cqes = drain(&mut engine, 2);
        assert!(
            cqes.iter().any(|c| c.token == wt && c.result.is_ok()),
            "write CQE missing: {cqes:?}"
        );
        // Read the bytes back through the ring into slot 1.
        let rt = tok(Op::DownstreamRead);
        assert!(matches!(
            engine.read(rt, b, 1).expect("read"),
            Poll::Pending
        ));
        let cqes = drain(&mut engine, 2);
        let got = cqes.iter().find(|c| c.token == rt).expect("read CQE");
        assert!(matches!(got.result, Ok(32)));
        close(a);
        close(b);
    }

    #[test]
    fn connect_refused_completes_with_error() {
        let (_pool, mut engine) = test_engine();
        let addr: SocketAddr = "127.0.0.1:1".parse().expect("addr");
        let t = tok(Op::Connect);
        let (fd, poll) = engine.connect(t, addr).expect("connect issued");
        match poll {
            Poll::Done(_) => {}
            Poll::Pending => {
                let cqes = drain_until(&mut engine, |c| c.token == t);
                assert!(
                    cqes.iter().any(|c| c.token == t && c.result.is_err()),
                    "refused connect must error: {cqes:?}"
                );
            }
        }
        close(fd);
    }

    #[test]
    fn connect_unix_missing_path_errors() {
        let (_pool, mut engine) = test_engine();
        let t = tok(Op::Connect);
        let dir = tempfile::tempdir().expect("dir");
        let missing = dir.path().join("no.sock");
        let res = engine.connect_unix(t, &missing);
        assert!(res.is_err() || matches!(res, Ok((_, Poll::Pending))));
    }

    #[test]
    fn accept_flow_materializes_connection() {
        let (_pool, mut engine) = test_engine();
        let listener =
            crate::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
        let lfd = std::os::fd::AsRawFd::as_raw_fd(&listener);
        engine.add_listener(lfd, Token::accept(0)).expect("add");
        // No client yet: accept reports None (single outstanding SQE).
        assert!(
            engine
                .accept(lfd, Token::accept(0))
                .expect("accept")
                .is_none()
        );
        let addr = listener.local_addr().expect("addr");
        let _client = std::net::TcpStream::connect(addr).expect("connect");
        // Accept completions land in the engine's accepted map (not the
        // CQE out-vec): poll until the retrieval API reports the peer.
        let mut got = None;
        for _ in 0..60 {
            let mut out = Vec::new();
            engine
                .poll(Some(std::time::Duration::from_millis(100)), &mut out)
                .expect("poll");
            got = engine.accept(lfd, Token::accept(0)).expect("accept2");
            if got.is_some() {
                break;
            }
        }
        // The completed accept is retrievable through the engine API.
        assert!(got.is_some(), "materialized connection expected");
        let (fd, peer) = got.expect("some");
        assert!(peer.port() != 0);
        close(fd);
    }

    #[test]
    fn splice_moved_and_eof_paths() {
        let (_pool, mut engine) = test_engine();
        let mut fds = [0 as RawFd; 2];
        // SAFETY: plain pipe2 with valid out-array.
        assert_eq!(
            // SAFETY: out-array is a valid 2-element fd buffer.
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) },
            0
        );
        let (pr, pw) = (fds[0], fds[1]);
        let payload = b"uring-splice";
        // SAFETY: write to a live pipe.
        unsafe { libc::write(pw, payload.as_ptr().cast(), payload.len()) };
        let (sa, sb) = sockpair();
        let t = tok(Op::Splice);
        let t_rev = Token::new(Op::Splice, 0, 0, 1);
        engine.splice_pump(t, pr, t_rev, sb).expect("splice_pump");
        let cqes = drain_until(&mut engine, |c| c.token == t);
        assert!(
            cqes.iter()
                .any(|c| c.token == t && matches!(c.result, Ok(n) if n as usize == payload.len())),
            "splice moved CQE: {cqes:?}"
        );
        // Drain the pipe, then the pump must report EOF.
        let mut buf = [0u8; 64];
        // SAFETY: read into a live buffer.
        let n = unsafe { libc::read(sa, buf.as_mut_ptr().cast(), 64) };
        assert_eq!(&buf[..n as usize], payload);
        close(pw); // writer gone: next pump sees EOF
        let t2 = Token::new(Op::Splice, 1, 0, 0);
        let t2_rev = Token::new(Op::Splice, 1, 0, 1);
        engine
            .splice_pump(t2, pr, t2_rev, sb)
            .expect("splice_pump2");
        let cqes = drain_until(&mut engine, |c| c.token == t2);
        assert!(
            cqes.iter()
                .any(|c| c.token == t2 && matches!(c.result, Ok(0))),
            "splice EOF CQE: {cqes:?}"
        );
        close(pr);
        close(sa);
        close(sb);
    }

    #[test]
    fn remove_clears_tracked_fd() {
        let (_pool, mut engine) = test_engine();
        let (a, b) = sockpair();
        engine
            .add_stream(a, tok(Op::DownstreamRead))
            .expect("add_stream");
        engine.remove(a);
        // No panic; op state dropped.
        close(a);
        close(b);
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;
    use crate::token::{Op, Token};
    use std::os::fd::RawFd;

    /// A multishot watch must fire when the watched socket becomes
    /// readable — the body-splice fast path hangs forever if it does
    /// not (this is the exact failure the e2e suite saw first).
    #[test]
    fn watch_fires_on_readability() {
        let mut engine = UringEngine::new(64, None, false).expect("engine");
        let mut fds = [0 as RawFd; 2];
        // SAFETY: socketpair fills both descriptors or fails.
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        let (a, b) = (fds[0], fds[1]);
        // Nonblocking both ends.
        // SAFETY: fcntl on owned descriptors.
        for fd in [a, b] {
            // SAFETY: fcntl on owned descriptors.
            unsafe {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
            }
        }
        let token = Token::new(Op::BodySpliceIn, 0, 1, 0);
        engine.watch(a, token, libc::POLLIN as u32).expect("watch");

        // Write from b: `a` becomes readable.
        // SAFETY: write to an owned connected descriptor.
        let n = unsafe { libc::write(b, "x".as_ptr().cast(), 1) };
        assert_eq!(n, 1);

        // First readiness.
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline && out.is_empty() {
            engine
                .poll(Some(std::time::Duration::from_millis(200)), &mut out)
                .expect("poll");
        }
        assert!(
            out.iter().any(|c| c.token.op() == Op::BodySpliceIn),
            "the watch never fired the first time: {out:?}"
        );

        // MULTISHOT contract: a second readiness on the same watch must
        // also fire — EOF detection depends on it (after the body
        // bytes, the FIN is the next event).
        // SAFETY: write to an owned connected descriptor.
        let n = unsafe { libc::write(b, "y".as_ptr().cast(), 1) };
        assert_eq!(n, 1);
        out.clear();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline && out.is_empty() {
            engine
                .poll(Some(std::time::Duration::from_millis(200)), &mut out)
                .expect("poll");
        }
        assert!(
            out.iter().any(|c| c.token.op() == Op::BodySpliceIn),
            "the watch never fired a SECOND time (multishot broken): {out:?}"
        );
        // SAFETY: owned test descriptors.
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }
}

#[cfg(test)]
mod body_splice_mechanism_tests {
    //! The re-entry criterion from docs/perf-arc-2026-10.md, iteration
    //! 3: reproduce (or refute) the vanishing bytes with the exact
    //! splice(2) sequence the worker issues — two TCP legs, one pipe,
    //! ingress then egress — WITHOUT the worker. If this delivers, the
    //! loss lives in the worker's interaction with concurrent uring
    //! ops on the same socket; if it does not, the mechanism itself is
    //! the bug.

    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::{AsRawFd, RawFd};
    use std::time::Duration;

    /// Connected TCP pair (backend_leg, client_leg) with both sockets
    /// nonblocking — the two fds the worker splices between.
    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).expect("connect");
        let (backend, _) = listener.accept().expect("accept");
        for fd in [backend.as_raw_fd(), client.as_raw_fd()] {
            // SAFETY: fcntl on owned descriptors.
            unsafe {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
            }
        }
        (backend, client)
    }

    fn pipe_nonblocking() -> (RawFd, RawFd) {
        let mut fds = [0 as RawFd; 2];
        // SAFETY: pipe2 with valid out-array.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
        assert_eq!(rc, 0, "pipe2");
        (fds[0], fds[1])
    }

    fn splice(from: RawFd, to: RawFd, want: usize) -> isize {
        // SAFETY: live nonblocking descriptors; null offsets = current
        // position (sockets/pipes).
        unsafe {
            libc::splice(
                from,
                std::ptr::null_mut(),
                to,
                std::ptr::null_mut(),
                want,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
            )
        }
    }

    /// The worker's exact sequence: 4 KiB relayed normally FIRST (the
    /// head + first-read body bytes), then the 61506-byte tail via
    /// pipe splice. The client must see body[0..65536] in order.
    ///
    /// Wiring: `up_leg` is the proxy's upstream socket (the response
    /// is written to its peer), `down_leg` is the proxy's client
    /// socket (the splice writes into it), and `test_reader` is
    /// `down_leg`'s peer.
    #[test]
    fn splice_after_normal_write_delivers_the_tail_in_order() {
        let body = (0..65536)
            .map(|i| b'a' + (i % 26) as u8)
            .collect::<Vec<_>>();
        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 65536\r\n\r\n".to_vec();
        let (up_leg, resp_writer) = tcp_pair();
        let (down_leg, mut test_reader) = tcp_pair();
        let (rp, wp) = pipe_nonblocking();

        // The backend application writes the response; it becomes
        // readable on the upstream leg.
        let all = [head.as_slice(), body.as_slice()].concat();
        // SAFETY: write to an owned connected descriptor.
        unsafe { libc::write(resp_writer.as_raw_fd(), all.as_ptr().cast(), all.len()) };

        // Worker read #1: 4096 bytes (slot-sized) off the upstream.
        let mut slot = [0u8; 4096];
        // SAFETY: read into an owned buffer.
        let n1 = unsafe { libc::read(up_leg.as_raw_fd(), slot.as_mut_ptr().cast(), slot.len()) };
        assert_eq!(n1 as usize, 4096, "first read must fill the slot");
        assert_eq!(&slot[..head.len()], &head[..], "head first");
        // The handler relays those bytes to the real client socket.
        // SAFETY: write to an owned connected descriptor.
        unsafe { libc::write(down_leg.as_raw_fd(), slot.as_ptr().cast(), 4096) };

        // Splice the tail: ingress (upstream -> pipe) then egress
        // (pipe -> downstream), exactly the worker's pump rounds.
        let mut moved_in = 0u64;
        let tail = (body.len() - (4096 - head.len())) as u64;
        while moved_in < tail {
            let n = splice(
                up_leg.as_raw_fd(),
                wp,
                (tail - moved_in).min(1 << 16) as usize,
            );
            assert!(n > 0, "ingress: {n}");
            moved_in += n as u64;
        }
        let mut moved_out = 0u64;
        while moved_out < moved_in {
            let n = splice(rp, down_leg.as_raw_fd(), (moved_in - moved_out) as usize);
            eprintln!("[mech] egress n={n} moved_in={moved_in}");
            assert!(n > 0, "egress: {n}");
            moved_out += n as u64;
        }
        // How much did the reader's side actually receive?
        std::thread::sleep(Duration::from_millis(200));
        let mut pending: i32 = 0;
        // SAFETY: FIONREAD ioctl on an owned socket.
        unsafe {
            libc::ioctl(test_reader.as_raw_fd(), libc::FIONREAD, &mut pending);
        }
        eprintln!("[mech] after egress: test_reader FIONREAD={pending}");

        // The client reads everything and hashes it.
        let mut got = Vec::new();
        test_reader
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout");
        let mut tmp = [0u8; 16384];
        let expected = head.len() + body.len();
        while got.len() < expected {
            let n = test_reader.read(&mut tmp).expect("client read");
            assert!(n > 0, "eof at {} of {expected}", got.len());
            got.extend_from_slice(&tmp[..n]);
        }
        assert_eq!(&got[..head.len()], &head[..], "head intact");
        assert_eq!(got[head.len()], b'a', "body starts at 'a'");
        assert_eq!(
            &got[head.len()..],
            &body[..],
            "the spliced tail is the body, in order, complete"
        );
        // SAFETY: owned pipe ends.
        unsafe {
            libc::close(rp);
            libc::close(wp);
        }
    }
}
