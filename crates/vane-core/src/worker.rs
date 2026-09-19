//! Worker — the pinned, share-nothing thread that owns an engine, a buffer
//! pool, a session slab, and a handler (`TH-01`, `MM-01`).
//!
//! State is split into [`WorkerState`] (transport) and the [`Handler`] so a
//! handler callback can re-enter the transport through [`SessionIo`] without
//! aliasing: `SessionIo` borrows only the transport half.

use std::collections::BinaryHeap;
use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use vane_observe::LogEvent;
use vane_observe::metrics::Registry;
use vane_observe::ring::EventRing;

use crate::buffer::{BufferPool, DEFAULT_BUF_SIZE, DEFAULT_POOL_SIZE};
use crate::engine::{Engine, create_engine};
use crate::handler::{Handler, HandlerFactory, Mode, SessionIo};
use crate::net::{StreamFd, set_nodelay, shutdown_write};

/// Per-session queued-write budget (burst cap). Steady-state flow
/// control is read throttling (2 slots); this bounds a single handler
/// burst (e.g. a large pre-connect body queue).
const WRITE_PENDING_CAP: usize = 1024 * 1024;
use crate::slab::SessionSlab;
use crate::spsc::{SpscReceiver, SpscSender};
use crate::token::{Op, Token};

/// Commands the control plane sends a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCmd {
    /// Stop accepting; drain sessions; exit by the deadline.
    Shutdown {
        /// Hard exit horizon in milliseconds from receipt.
        deadline_ms: u64,
    },
}

/// Worker configuration knobs.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// CPU core index to pin to (physical order from `core_affinity`).
    pub core: Option<usize>,
    /// Max concurrent sessions.
    pub max_sessions: usize,
    /// Buffer pool slots (double as io_uring fixed buffers).
    pub pool_slots: usize,
    /// io_uring queue depth.
    pub ring_entries: u32,
    /// Use `IORING_SETUP_SQPOLL` (zero-syscall submission).
    pub sqpoll: bool,
    /// Prefer mio even when io_uring is available.
    pub force_mio: bool,
    /// Accept backlog per listener.
    pub backlog: i32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            core: None,
            max_sessions: 16_384,
            pool_slots: DEFAULT_POOL_SIZE,
            ring_entries: 4096,
            sqpoll: false,
            force_mio: false,
            backlog: 4096,
        }
    }
}

/// Immutable context handed to a [`HandlerFactory::build`].
pub struct WorkerCtx {
    /// Worker index (0..N).
    pub id: usize,
    /// Shared metric registry.
    pub registry: Arc<Registry>,
    /// Worker-local log/event ring (drained by the control plane).
    pub events: Arc<EventRing<LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
    /// Effective worker config.
    pub config: WorkerConfig,
    /// Live-session gauge (worker-maintained).
    pub connections: vane_observe::metrics::MetricHandle,
}

/// Live handle to a running worker thread.
pub struct WorkerHandle {
    /// Worker index.
    pub id: usize,
    /// Command sender (shutdown).
    pub cmd: SpscSender<WorkerCmd, 64>,
    /// Set when the worker exits.
    done: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl WorkerHandle {
    /// `true` once the worker thread has exited.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Joins the worker thread.
    pub fn join(&mut self) {
        if let Some(h) = self.join.take() {
            let _ = h.join();
        }
    }
}

/// Internal per-session state.
struct Session {
    downstream: StreamFd,
    peer: SocketAddr,
    upstream: Option<StreamFd>,
    /// Read slot held across an in-flight downstream read.
    rslot: Option<u32>,
    /// Downstream write queue: `(slot, len, offset)`.
    wq: std::collections::VecDeque<(u32, u32, u32)>,
    /// Upstream read slot.
    urslot: Option<u32>,
    /// Upstream write queue.
    uwq: std::collections::VecDeque<(u32, u32, u32)>,
    /// Bytes queued while the pool was exhausted (runaway-guarded).
    pending_down: Vec<u8>,
    pending_up: Vec<u8>,
    read_inflight: bool,
    upstream_read_inflight: bool,
    write_inflight: bool,
    upstream_write_inflight: bool,
    /// Nonblocking connect still in progress (diagnostics).
    #[allow(dead_code)] // consumed by CQE dispatch; kept for state clarity
    connect_inflight: bool,
    /// Bumped whenever the upstream fd is replaced; rides the token aux so
    /// stale completions for a discarded fd are dropped.
    upstream_epoch: u8,
    /// Request head already serialized to the upstream (no failover past it).
    request_sent: bool,
    downstream_eof: bool,
    upstream_eof: bool,
    /// Half-close (FIN) requested while writes are still flushing.
    fin_queued: bool,
    splice: bool,
    deadline: Option<Instant>,
}

/// Timer heap entry.
#[derive(PartialEq, Eq)]
struct Timer {
    at: Instant,
    slot: u32,
    generation: u16,
    reason_aux: u16,
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.at.cmp(&self.at) // min-heap
    }
}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Transport half of the worker. Every handler callback receives a
/// [`SessionIo`] that borrows exactly this struct.
pub(crate) struct WorkerState {
    ctx: WorkerCtx,
    engine: Box<dyn Engine>,
    pool: BufferPool,
    slab: SessionSlab<Session>,
    listeners: Vec<StdTcpListener>,
    timers: BinaryHeap<Timer>,
    draining: bool,
    drain_deadline: Option<Instant>,
    #[allow(dead_code)] // kept for per-mode logging in follow-ups
    mode: Mode,
    /// Connects that completed inline (UDS always; TCP rarely) and need
    /// their `on_upstream_connected` callback delivered on the next loop
    /// pass — the handler is not reachable from the connect call site.
    inline_connected: Vec<(u32, u16)>,
}

impl WorkerState {
    fn io_for(&mut self, slot: u32, generation: u16) -> SessionIo<'_> {
        SessionIo {
            worker: self,
            slot,
            generation,
        }
    }

    fn valid(&self, slot: u32, generation: u16) -> bool {
        self.slab.matches(slot, generation)
    }

    // ----- SessionIo surface (called by handlers) ----------------------------

    pub(crate) fn downstream_write(&mut self, slot: u32, generation: u16, bytes: &[u8]) {
        if !self.valid(slot, generation) || bytes.is_empty() {
            return;
        }
        // Direct write requires the full queue to be idle: pending_down
        // may hold OLDER bytes (pool exhaustion defers them), and
        // writing the new batch first would reorder the stream.
        let can_direct = self
            .slab
            .get(slot)
            .is_some_and(|s| !s.write_inflight && s.wq.is_empty() && s.pending_down.is_empty());
        if can_direct {
            if let Some(ws) = self.pool.take() {
                let Some(s) = self.slab.get_mut(slot) else {
                    return;
                };
                let n = bytes.len().min(self.pool.buf_size());
                self.pool.slot_mut(ws)[..n].copy_from_slice(&bytes[..n]);
                if bytes.len() > n {
                    s.pending_down.extend_from_slice(&bytes[n..]);
                }
                s.wq.push_back((ws, n as u32, 0));
                s.write_inflight = true;
                let fd = s.downstream.fd();
                let token = Token::new(Op::DownstreamWrite, slot, generation, 0);
                let _ = self.engine.write(token, fd, ws, n, 0);
                return;
            }
        }
        // Queue path (pool exhausted or write already in flight).
        let overflow = self
            .slab
            .get(slot)
            .is_some_and(|s| s.pending_down.len() + bytes.len() > WRITE_PENDING_CAP);
        if overflow {
            self.close_session(slot, generation, "write-queue-overflow");
            return;
        }
        if let Some(s) = self.slab.get_mut(slot) {
            s.pending_down.extend_from_slice(bytes);
        }
    }

    pub(crate) fn upstream_write(&mut self, slot: u32, generation: u16, bytes: &[u8]) {
        if !self.valid(slot, generation) || bytes.is_empty() {
            return;
        }
        // Same queue-jump guard as downstream_write: pending_up may
        // hold older deferred bytes.
        let can_direct = self.slab.get(slot).is_some_and(|s| {
            s.upstream.is_some()
                && !s.upstream_write_inflight
                && s.uwq.is_empty()
                && s.pending_up.is_empty()
        });
        if can_direct {
            if let Some(ws) = self.pool.take() {
                let Some(s) = self.slab.get_mut(slot) else {
                    return;
                };
                let Some(up) = &s.upstream else { return };
                let fd = up.fd();
                let n = bytes.len().min(self.pool.buf_size());
                self.pool.slot_mut(ws)[..n].copy_from_slice(&bytes[..n]);
                if bytes.len() > n {
                    s.pending_up.extend_from_slice(&bytes[n..]);
                }
                s.uwq.push_back((ws, n as u32, 0));
                s.upstream_write_inflight = true;
                let token = Token::new(Op::UpstreamWrite, slot, generation, 0);
                let _ = self.engine.write(token, fd, ws, n, 0);
                return;
            }
        }
        let overflow = self
            .slab
            .get(slot)
            .is_some_and(|s| s.pending_up.len() + bytes.len() > WRITE_PENDING_CAP);
        if overflow {
            self.close_session(slot, generation, "write-queue-overflow");
            return;
        }
        if let Some(s) = self.slab.get_mut(slot) {
            s.pending_up.extend_from_slice(bytes);
        }
    }

    pub(crate) fn connect_upstream(
        &mut self,
        slot: u32,
        generation: u16,
        addr: SocketAddr,
    ) -> bool {
        if !self.valid(slot, generation) {
            return false;
        }
        let token = Token::new(Op::Connect, slot, generation, 0);
        match self.engine.connect(token, addr) {
            Ok((fd, poll)) => {
                let Some(s) = self.slab.get_mut(slot) else {
                    return false;
                };
                crate::dbg_trace!("DIAL fd={fd} slot={slot}");
                s.upstream = Some(StreamFd(fd));
                if poll == crate::engine::Poll::Done(0) {
                    self.do_upstream_connected(slot, generation);
                    self.inline_connected.push((slot, generation));
                }
                true
            }
            Err(_) => false,
        }
    }

    pub(crate) fn connect_upstream_unix(
        &mut self,
        slot: u32,
        generation: u16,
        path: PathBuf,
    ) -> bool {
        if !self.valid(slot, generation) {
            return false;
        }
        let token = Token::new(Op::Connect, slot, generation, 0);
        match self.engine.connect_unix(token, &path) {
            Ok((fd, poll)) => {
                let Some(s) = self.slab.get_mut(slot) else {
                    return false;
                };
                crate::dbg_trace!("DIAL fd={fd} slot={slot}");
                s.upstream = Some(StreamFd(fd));
                if poll == crate::engine::Poll::Done(0) {
                    self.do_upstream_connected(slot, generation);
                    self.inline_connected.push((slot, generation));
                }
                true
            }
            Err(_) => false,
        }
    }

    pub(crate) fn start_splice(&mut self, slot: u32, generation: u16) -> bool {
        if !self.valid(slot, generation) {
            return false;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return false;
        };
        let Some(up) = &s.upstream else { return false };
        let (a, b) = (s.downstream.fd(), up.fd());
        // Splice bypasses the buffer pool. Slots with in-flight reads are
        // KEPT: their CQEs will land after the pump starts and must be
        // forwarded raw (see the splice branch in the read dispatchers) —
        // releasing them now would drop the bytes they hold.
        if !s.read_inflight {
            if let Some(rs) = s.rslot.take() {
                self.pool.release(rs);
            }
        }
        if !s.upstream_read_inflight {
            if let Some(rs) = s.urslot.take() {
                self.pool.release(rs);
            }
        }
        s.splice = true;
        let ta = Token::new(Op::Splice, slot, generation, 1);
        let tb = Token::new(Op::Splice, slot, generation, 2);
        self.engine.splice_pump(ta, a, tb, b).is_ok()
    }

    pub(crate) fn shutdown_downstream_write(&mut self, slot: u32, generation: u16) {
        if !self.valid(slot, generation) {
            return;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        if s.wq.is_empty() && s.pending_down.is_empty() && !s.write_inflight {
            shutdown_write(s.downstream.fd());
        } else {
            s.fin_queued = true; // flush paths complete it
        }
    }

    pub(crate) fn shutdown_upstream_write(&mut self, slot: u32, generation: u16) {
        if !self.valid(slot, generation) {
            return;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        if let Some(up) = &s.upstream {
            if s.uwq.is_empty() && s.pending_up.is_empty() && !s.upstream_write_inflight {
                shutdown_write(up.fd());
            }
        }
    }

    /// Closes and drops a session. `reason` documents the call site for
    /// crash triage (compiled out in release... kept as a parameter so the
    /// next debug session can re-enable the trace in one line).
    pub(crate) fn close_session(&mut self, slot: u32, generation: u16, reason: &str) {
        // vane-core stays dependency-light: no tracing here.
        let _ = reason;
        if !self.valid(slot, generation) {
            return;
        }
        let Some(s) = self.slab.remove(slot) else {
            return;
        }; // generation bumps inside
        if let Some(rs) = s.rslot {
            self.pool.release(rs);
        }
        for (ws, _, _) in s.wq {
            self.pool.release(ws);
        }
        if let Some(rs) = s.urslot {
            self.pool.release(rs);
        }
        for (ws, _, _) in s.uwq {
            self.pool.release(ws);
        }
        self.engine.remove(s.downstream.fd());
        if let Some(up) = &s.upstream {
            self.engine.remove(up.fd());
        }
        // StreamFd Drop closes the descriptors.
    }

    pub(crate) fn set_deadline(
        &mut self,
        slot: u32,
        generation: u16,
        at: Option<Instant>,
        reason: crate::handler::DeadlineReason,
    ) {
        if !self.valid(slot, generation) {
            return;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        s.deadline = at;
        if let Some(at) = at {
            self.timers.push(Timer {
                at,
                slot,
                generation,
                reason_aux: reason.aux(),
            });
        }
    }

    pub(crate) fn peer_of(&self, slot: u32, generation: u16) -> Option<SocketAddr> {
        if self.valid(slot, generation) {
            self.slab.get(slot).map(|s| s.peer)
        } else {
            None
        }
    }

    pub(crate) fn has_upstream(&self, slot: u32, generation: u16) -> bool {
        self.valid(slot, generation) && self.slab.get(slot).is_some_and(|s| s.upstream.is_some())
    }

    /// Discards a dead upstream: removes it from the engine, closes the fd,
    /// releases its slots, and bumps the epoch so stale CQEs are dropped.
    pub(crate) fn discard_upstream(&mut self, slot: u32, generation: u16) {
        if !self.valid(slot, generation) {
            return;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        let Some(old) = s.upstream.take() else { return };
        let fd = old.fd();
        // StreamFd Drop closes it — ownership is simply dropped.
        drop(old);
        self.engine.remove(fd);
        if let Some(rs) = s.urslot.take() {
            self.pool.release(rs);
        }
        for (ws, _, _) in s.uwq.drain(..) {
            self.pool.release(ws);
        }
        s.pending_up.clear();
        s.upstream_read_inflight = false;
        s.upstream_write_inflight = false;
        s.upstream_eof = false;
        s.request_sent = false;
        s.upstream_epoch = s.upstream_epoch.wrapping_add(1);
    }

    /// Detaches the upstream fd (no close) for connection pooling.
    pub(crate) fn detach_upstream(&mut self, slot: u32, generation: u16) -> Option<RawFd> {
        if !self.valid(slot, generation) {
            return None;
        }
        let fd = {
            let s = self.slab.get_mut(slot)?;
            let owned = s.upstream.take()?;
            let fd = owned.fd();
            // Ownership transfers to the caller (pool) — forget the guard
            // so Drop does not close the descriptor.
            std::mem::forget(owned);

            fd
        };
        // Stop tracking it in the engine and return every slot it holds.
        self.engine.remove(fd);
        if let Some(s) = self.slab.get_mut(slot) {
            if let Some(rs) = s.urslot.take() {
                self.pool.release(rs);
            }
            s.upstream_read_inflight = false;
            for (ws, _, _) in s.uwq.drain(..) {
                self.pool.release(ws);
            }
            s.pending_up.clear();
            s.upstream_write_inflight = false;
            s.upstream_eof = false;
            s.request_sent = false;
        }
        Some(fd)
    }

    /// Attaches a pooled fd as this session's upstream and arms its read.
    pub(crate) fn attach_upstream(&mut self, slot: u32, generation: u16, fd: RawFd) -> bool {
        if !self.valid(slot, generation) {
            return false;
        }
        let token = Token::new(Op::UpstreamRead, slot, generation, 0);
        if self.engine.add_stream(fd, token).is_err() {
            return false;
        }
        let Some(s) = self.slab.get_mut(slot) else {
            return false;
        };
        crate::dbg_trace!("DIAL fd={fd} slot={slot}");
        s.upstream = Some(StreamFd(fd));
        if s.urslot.is_none() {
            if let Some(uslot) = self.pool.take() {
                let epoch = u16::from(s.upstream_epoch);
                s.urslot = Some(uslot);
                s.upstream_read_inflight = true;
                let token = Token::new(Op::UpstreamRead, slot, generation, epoch);
                let _ = self.engine.read(token, fd, uslot);
            }
        }
        true
    }

    /// Raw upstream descriptor (diagnostics).
    pub(crate) fn upstream_fd(&self, slot: u32, generation: u16) -> Option<RawFd> {
        if self.valid(slot, generation) {
            self.slab
                .get(slot)
                .and_then(|s| s.upstream.as_ref())
                .map(StreamFd::fd)
        } else {
            None
        }
    }

    /// Request-head-written flag (failover guard).
    pub(crate) fn request_sent(&self, slot: u32, generation: u16) -> bool {
        self.valid(slot, generation) && self.slab.get(slot).is_some_and(|s| s.request_sent)
    }

    pub(crate) fn mark_request_sent(&mut self, slot: u32, generation: u16) {
        if !self.valid(slot, generation) {
            return;
        }
        if let Some(s) = self.slab.get_mut(slot) {
            s.request_sent = true;
        }
    }

    // ----- internal event handling (called with the handler split out) -------

    fn do_upstream_connected(&mut self, slot: u32, generation: u16) {
        {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            if s.urslot.is_none() && !s.splice {
                if let Some(uslot) = self.pool.take() {
                    let epoch = u16::from(s.upstream_epoch);
                    let fd = s.upstream.as_ref().map_or(-1, StreamFd::fd);
                    s.urslot = Some(uslot);
                    s.upstream_read_inflight = true;
                    let token = Token::new(Op::UpstreamRead, slot, generation, epoch);
                    let _ = self.engine.read(token, fd, uslot);
                }
            }
        }
        // Bytes buffered before the dial completed (early client data):
        // kick the write queue now that the upstream exists, or they
        // would sit until the next write-completion event (which may
        // never come — nothing was in flight).
        let needs_kick = self
            .slab
            .get(slot)
            .is_some_and(|s| !s.pending_up.is_empty() && !s.upstream_write_inflight);
        if needs_kick {
            self.kick_pending_upstream(slot, generation);
        }
    }

    fn arm_downstream_read(&mut self, slot: u32, generation: u16) {
        // NOTE: no upstream-write backpressure pause here. Skipping the
        // read while paused loses epoll ET edges (a readable event
        // consumed with no parked read op is gone forever), which
        // deadlocks streaming responses. Upstream buffering is already
        // bounded by WRITE_PENDING_CAP at write_upstream time.
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        if s.read_inflight || s.splice {
            crate::dbg_trace!(
                "ARMDbg skip slot={slot} inflight={} splice={}",
                s.read_inflight,
                s.splice
            );
            return;
        }
        if s.rslot.is_none() {
            let Some(rs) = self.pool.take() else {
                crate::dbg_trace!("ARMDbg pool-exhausted slot={slot}");
                return;
            }; // backpressure
            s.rslot = Some(rs);
        }
        let Some(rs) = s.rslot else { return };
        s.read_inflight = true;
        let fd = s.downstream.fd();
        let token = Token::new(Op::DownstreamRead, slot, generation, 0);
        let _ = self.engine.read(token, fd, rs);
    }

    fn arm_upstream_read(&mut self, slot: u32, generation: u16) {
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        // The handler may have detached (parked) or discarded the upstream
        // during its callback — there is nothing to read anymore. Re-arming
        // here would submit a read on fd -1 / a closed descriptor whose
        // EBADF completion then kills the idle keep-alive session.
        if s.upstream.is_none() {
            return;
        }
        // Read throttling: pause while the client-bound write queue is
        // backed up (resumed from the downstream write-completion path).
        if s.pending_down.len() > 2 * self.pool.buf_size() {
            return;
        }
        if s.upstream_read_inflight || s.splice {
            return;
        }
        if s.urslot.is_none() {
            let Some(rs) = self.pool.take() else { return };
            s.urslot = Some(rs);
        }
        let Some(rs) = s.urslot else { return };
        s.upstream_read_inflight = true;
        let epoch = u16::from(s.upstream_epoch);
        let fd = s.upstream.as_ref().map_or(-1, StreamFd::fd);
        let token = Token::new(Op::UpstreamRead, slot, generation, epoch);
        let _ = self.engine.read(token, fd, rs);
    }

    fn continue_downstream_write(&mut self, slot: u32, generation: u16, h: &mut dyn Handler) {
        let next = {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            if let Some((ws, _, _)) = s.wq.front() {
                let ws = *ws;
                s.wq.pop_front();
                self.pool.release(ws);
            }
            if let Some(&(ws, len, off)) = s.wq.front() {
                s.write_inflight = true;
                Some((ws, len, off))
            } else if !s.pending_down.is_empty() {
                let Some(ws) = self.pool.take() else {
                    s.write_inflight = false;
                    return;
                };
                let n = s.pending_down.len().min(self.pool.buf_size());
                self.pool.slot_mut(ws)[..n].copy_from_slice(&s.pending_down[..n]);
                s.pending_down.drain(..n);
                s.wq.push_back((ws, n as u32, 0));
                s.write_inflight = true;
                Some((ws, n as u32, 0))
            } else {
                s.write_inflight = false;
                None
            }
        };
        if let Some((ws, len, off)) = next {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            let fd = s.downstream.fd();
            let token = Token::new(Op::DownstreamWrite, slot, generation, 0);
            let _ = self.engine.write(token, fd, ws, len as usize, off as usize);
        } else {
            // Queue fully flushed: complete a deferred FIN.
            let fin = {
                let Some(s) = self.slab.get_mut(slot) else {
                    return;
                };
                if s.fin_queued {
                    s.fin_queued = false;
                    shutdown_write(s.downstream.fd());
                    true
                } else {
                    false
                }
            };
            if !fin {
                let mut io = self.io_for(slot, generation);
                h.on_downstream_flushed(&mut io);
            }
        }
        // Queue drained: resume client reads paused by backpressure.
        let drained = self
            .slab
            .get(slot)
            .is_some_and(|s| s.pending_up.len() <= 2 * self.pool.buf_size());
        if drained {
            self.arm_downstream_read(slot, generation);
        }
    }

    /// Kicks pending_up bytes into the upstream write queue (used after
    /// a dial completes with buffered early data). Public within the
    /// worker for `do_upstream_connected`.
    fn kick_pending_upstream(&mut self, slot: u32, generation: u16) {
        let next = {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            if s.uwq.front().is_some() || s.pending_up.is_empty() || s.upstream_write_inflight {
                return;
            }
            let Some(ws) = self.pool.take() else {
                return; // backpressure: flushed on the next completion
            };
            let n = s.pending_up.len().min(self.pool.buf_size());
            self.pool.slot_mut(ws)[..n].copy_from_slice(&s.pending_up[..n]);
            s.pending_up.drain(..n);
            s.uwq.push_back((ws, n as u32, 0));
            s.upstream_write_inflight = true;
            Some((ws, n as u32, 0))
        };
        if let Some((ws, len, off)) = next {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            let Some(up) = &s.upstream else { return };
            let fd = up.fd();
            let token = Token::new(Op::UpstreamWrite, slot, generation, 0);
            let _ = self.engine.write(token, fd, ws, len as usize, off as usize);
        }
    }

    fn continue_upstream_write(&mut self, slot: u32, generation: u16, h: &mut dyn Handler) {
        let next = {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            if let Some((ws, _, _)) = s.uwq.front() {
                let ws = *ws;
                s.uwq.pop_front();
                self.pool.release(ws);
            }
            if let Some(&(ws, len, off)) = s.uwq.front() {
                s.upstream_write_inflight = true;
                Some((ws, len, off))
            } else if !s.pending_up.is_empty() {
                let Some(ws) = self.pool.take() else {
                    s.upstream_write_inflight = false;
                    return;
                };
                let n = s.pending_up.len().min(self.pool.buf_size());
                self.pool.slot_mut(ws)[..n].copy_from_slice(&s.pending_up[..n]);
                s.pending_up.drain(..n);
                s.uwq.push_back((ws, n as u32, 0));
                s.upstream_write_inflight = true;
                Some((ws, n as u32, 0))
            } else {
                s.upstream_write_inflight = false;
                None
            }
        };
        if let Some((ws, len, off)) = next {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            let Some(up) = &s.upstream else { return };
            let fd = up.fd();
            let token = Token::new(Op::UpstreamWrite, slot, generation, 0);
            let _ = self.engine.write(token, fd, ws, len as usize, off as usize);
        } else {
            // Queue fully drained: resume client reads that were paused
            // by the pending_up backpressure (arm_downstream_read's
            // pause condition). Without this the pause never lifts —
            // the client's socket goes unread and the connection
            // deadlocks once its send window exhausts.
            self.arm_downstream_read(slot, generation);
            let mut io = self.io_for(slot, generation);
            h.on_upstream_flushed(&mut io);
        }
    }

    fn maybe_finish(&mut self, slot: u32, generation: u16) {
        let done = self.slab.get(slot).is_some_and(|s| {
            s.downstream_eof
                // A session whose upstream half never existed (client
                // connected + dropped before any dial) must close too —
                // otherwise it leaks its fd and its mio registration,
                // and the REUSED fd number later collides with the
                // stale registration (dial failures, hung sessions).
                && (s.upstream_eof || s.upstream.is_none())
                && s.wq.is_empty()
                && s.pending_down.is_empty()
                && s.uwq.is_empty()
                && s.pending_up.is_empty()
                && !s.write_inflight
                && !s.upstream_write_inflight
        });
        if done {
            self.close_session(slot, generation, "both-eof-flushed");
        }
    }

    fn dispatch_cqe(&mut self, cqe: crate::engine::Cqe, h: &mut dyn Handler) {
        let (slot, generation) = (cqe.token.slot(), cqe.token.generation());
        if !self.valid(slot, generation) {
            return; // stale token for a closed/reused session
        }
        match cqe.token.op() {
            Op::Accept => {}
            Op::Connect => match cqe.result {
                Ok(_) => {
                    self.do_upstream_connected(slot, generation);
                    let mut io = self.io_for(slot, generation);
                    h.on_upstream_connected(&mut io);
                }
                Err(e) => {
                    let mut io = self.io_for(slot, generation);
                    h.on_upstream_error(&mut io, e);
                }
            },
            Op::DownstreamRead => {
                {
                    let Some(s) = self.slab.get_mut(slot) else {
                        return;
                    };
                    s.read_inflight = false;
                    // Splice takeover: forward the in-flight bytes raw to
                    // the upstream (the kernel pump handles everything
                    // after) and do not re-arm.
                    let res = match &cqe.result {
                        Ok(n) => Ok(*n),
                        Err(e) => Err(io::Error::from_raw_os_error(e.raw_os_error().unwrap_or(0))),
                    };
                    if s.splice {
                        self.splice_forward_inflight(slot, generation, true, res);
                        return;
                    }
                }
                match cqe.result {
                    Ok(n) if n > 0 => {
                        let data = {
                            let Some(s) = self.slab.get_mut(slot) else {
                                return;
                            };
                            let Some(rs) = s.rslot.take() else { return };
                            let v = self.pool.slot(rs)[..n as usize].to_vec();
                            self.pool.release(rs);
                            v
                        };
                        let mut io = self.io_for(slot, generation);
                        h.on_downstream_data(&mut io, &data);
                        self.arm_downstream_read(slot, generation);
                    }
                    Ok(0) => {
                        let Some(s) = self.slab.get_mut(slot) else {
                            return;
                        };
                        if let Some(rs) = s.rslot.take() {
                            self.pool.release(rs);
                        }
                        s.downstream_eof = true;
                        let mut io = self.io_for(slot, generation);
                        h.on_downstream_eof(&mut io);
                        self.maybe_finish(slot, generation);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let mut io = self.io_for(slot, generation);
                        h.on_downstream_error(&mut io, e);
                    }
                }
            }
            Op::DownstreamWrite => match cqe.result {
                Ok(n) if n > 0 => self.continue_downstream_write(slot, generation, h),
                Ok(_) => {}
                Err(e) => {
                    let mut io = self.io_for(slot, generation);
                    h.on_downstream_error(&mut io, e);
                }
            },
            Op::UpstreamRead | Op::UpstreamWrite => {
                // Stale completions for a discarded upstream fd (failover
                // replaced it) must not touch the session: the epoch rides
                // the token aux and must match.
                let epoch = u8::try_from(cqe.token.aux()).unwrap_or(u8::MAX);
                let epoch_ok = self
                    .slab
                    .get(slot)
                    .is_some_and(|s| s.upstream_epoch == epoch);
                if !epoch_ok {
                    return;
                }
                match cqe.token.op() {
                    Op::UpstreamRead => self.dispatch_upstream_read(cqe, slot, generation, h),
                    _ => self.dispatch_upstream_write(cqe, slot, generation, h),
                }
            }
            Op::Splice => match cqe.result {
                Ok(0) => {
                    let aux = cqe.token.aux();
                    let Some(s) = self.slab.get_mut(slot) else {
                        return;
                    };
                    if aux == 1 {
                        if let Some(up) = &s.upstream {
                            shutdown_write(up.fd());
                        }
                        s.downstream_eof = true;
                    } else {
                        shutdown_write(s.downstream.fd());
                        s.upstream_eof = true;
                    }
                    self.maybe_finish(slot, generation);
                }
                Ok(_) => { /* bytes moved; multishot readiness continues */ }
                Err(_) => self.close_session(slot, generation, "splice-err"),
            },
        }
    }

    /// Forwards the bytes held by an in-flight read slot to the opposite
    /// side after splice takeover. `from_downstream` selects direction.
    /// `result` is the read CQE: Ok(n) forwards, Ok(0) half-closes,
    /// Err kills the session. The slot rides the write queue so it is
    /// released on write completion (zero copy).
    fn splice_forward_inflight(
        &mut self,
        slot: u32,
        generation: u16,
        from_downstream: bool,
        result: io::Result<u32>,
    ) {
        let held = if from_downstream {
            self.slab.get_mut(slot).and_then(|s| s.rslot.take())
        } else {
            self.slab.get_mut(slot).and_then(|s| s.urslot.take())
        };
        let Some(rs) = held else { return }; // no in-flight slot: pump owns it
        let Some(s) = self.slab.get_mut(slot) else {
            return;
        };
        match result {
            Ok(n) if n > 0 => {
                // Queue the held slot toward the opposite fd (zero copy).
                if from_downstream {
                    let Some(up) = &s.upstream else {
                        self.pool.release(rs);
                        return;
                    };
                    let fd = up.fd();
                    s.uwq.push_back((rs, n, 0));
                    s.upstream_write_inflight = true;
                    let token = Token::new(Op::UpstreamWrite, slot, generation, 0);
                    let _ = self.engine.write(token, fd, rs, n as usize, 0);
                } else {
                    let fd = s.downstream.fd();
                    s.wq.push_back((rs, n, 0));
                    s.write_inflight = true;
                    let token = Token::new(Op::DownstreamWrite, slot, generation, 0);
                    let _ = self.engine.write(token, fd, rs, n as usize, 0);
                }
            }
            // Only Ok(0) reaches here (n > 0 matched above): EOF —
            // half-close the opposite write side.
            Ok(_) => {
                if from_downstream {
                    if let Some(up) = &s.upstream {
                        shutdown_write(up.fd());
                    }
                    s.downstream_eof = true;
                } else {
                    shutdown_write(s.downstream.fd());
                    s.upstream_eof = true;
                }
                self.pool.release(rs);
                self.maybe_finish(slot, generation);
            }
            Err(_) => {
                self.pool.release(rs);
                self.close_session(slot, generation, "splice-forward-err");
            }
        }
    }

    fn dispatch_upstream_read(
        &mut self,
        cqe: crate::engine::Cqe,
        slot: u32,
        generation: u16,
        h: &mut dyn Handler,
    ) {
        {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            s.upstream_read_inflight = false;
            // Splice takeover: forward in-flight bytes raw to the client.
            let res = match &cqe.result {
                Ok(n) => Ok(*n),
                Err(e) => Err(io::Error::from_raw_os_error(e.raw_os_error().unwrap_or(0))),
            };
            if s.splice {
                self.splice_forward_inflight(slot, generation, false, res);
                return;
            }
        }
        match cqe.result {
            Ok(n) if n > 0 => {
                let data = {
                    let Some(s) = self.slab.get_mut(slot) else {
                        return;
                    };
                    let Some(rs) = s.urslot.take() else { return };
                    let v = self.pool.slot(rs)[..n as usize].to_vec();
                    self.pool.release(rs);
                    v
                };
                let mut io = self.io_for(slot, generation);
                h.on_upstream_data(&mut io, &data);
                self.arm_upstream_read(slot, generation);
            }
            Ok(0) => {
                let Some(s) = self.slab.get_mut(slot) else {
                    return;
                };
                if let Some(rs) = s.urslot.take() {
                    self.pool.release(rs);
                }
                s.upstream_eof = true;
                let mut io = self.io_for(slot, generation);
                h.on_upstream_eof(&mut io);
                self.maybe_finish(slot, generation);
            }
            Ok(_) => {}
            Err(e) => {
                let mut io = self.io_for(slot, generation);
                h.on_upstream_error(&mut io, e);
            }
        }
    }

    fn dispatch_upstream_write(
        &mut self,
        cqe: crate::engine::Cqe,
        slot: u32,
        generation: u16,
        h: &mut dyn Handler,
    ) {
        match cqe.result {
            Ok(n) if n > 0 => self.continue_upstream_write(slot, generation, h),
            Ok(_) => {}
            Err(e) => {
                let mut io = self.io_for(slot, generation);
                h.on_upstream_error(&mut io, e);
            }
        }
    }

    fn run_timers(&mut self, now: Instant, h: &mut dyn Handler) {
        while self.timers.peek().is_some() {
            if self.timers.peek().is_some_and(|t| t.at > now) {
                break;
            }
            #[allow(clippy::expect_used, reason = "peeked non-empty above")]
            let t = self.timers.pop().expect("peeked non-empty");
            if self.valid(t.slot, t.generation) {
                let Some(s) = self.slab.get(t.slot) else {
                    continue;
                };
                let Some(deadline) = s.deadline else { continue };
                if deadline > t.at {
                    continue; // superseded
                }
                let reason = crate::handler::DeadlineReason::from_aux(t.reason_aux);
                let mut io = self.io_for(t.slot, t.generation);
                h.on_deadline(&mut io, reason);
            }
        }
    }

    fn next_timeout(&self) -> Option<Duration> {
        if let Some(t) = self.timers.peek() {
            return Some(t.at.saturating_duration_since(Instant::now()));
        }
        if self.draining {
            Some(Duration::from_millis(5))
        } else {
            Some(Duration::from_millis(50))
        }
    }

    fn drain_finished(&mut self) -> bool {
        if !self.draining {
            return false;
        }
        if self.slab.live().is_empty() {
            return true;
        }
        if let Some(deadline) = self.drain_deadline {
            if Instant::now() >= deadline {
                for (slot, generation) in self.slab.live() {
                    self.close_session(slot, generation, "generic");
                }
                return true;
            }
        }
        false
    }

    fn apply_cmd(&mut self, cmd: WorkerCmd, h: &mut dyn Handler) {
        match cmd {
            WorkerCmd::Shutdown { deadline_ms } => {
                self.draining = true;
                self.drain_deadline = Some(Instant::now() + Duration::from_millis(deadline_ms));
                for (slot, generation) in self.slab.live() {
                    let mut io = self.io_for(slot, generation);
                    h.on_shutdown_hint(&mut io);
                }
            }
        }
    }

    fn accept_pending(&mut self, lidx: usize, h: &mut dyn Handler) {
        let Some(l) = self.listeners.get(lidx) else {
            return;
        };
        let lfd = l.as_raw_fd();
        let ltoken = Token::accept(lidx as u16);
        // Drain the current backlog; `Err`/`None` ends the round.
        #[allow(clippy::while_let_loop)]
        while let Ok(Some((fd, peer))) = self.engine.accept(lfd, ltoken) {
            self.accept_connection(fd, peer, h);
        }
    }

    fn accept_connection(&mut self, fd: i32, peer: SocketAddr, h: &mut dyn Handler) {
        crate::dbg_trace!("ACCEPT fd={fd} peer={peer}");
        let _ = set_nodelay(fd);
        self.ctx.connections.inc(&self.ctx.registry);
        if self.draining || self.slab.live().len() >= self.ctx.config.max_sessions {
            // Overload / drain: refuse.
            // SAFETY: owned, unregistered descriptor.
            unsafe { libc::close(fd) };
            return;
        }
        struct FdGuard(i32);
        impl Drop for FdGuard {
            fn drop(&mut self) {
                // SAFETY: single close of an owned descriptor.
                unsafe { libc::close(self.0) };
            }
        }
        let guard = FdGuard(fd);
        let session = Session {
            downstream: StreamFd(-1),
            peer,
            upstream: None,
            rslot: None,
            wq: Default::default(),
            urslot: None,
            uwq: Default::default(),
            pending_down: Vec::new(),
            pending_up: Vec::new(),
            read_inflight: false,
            upstream_read_inflight: false,
            write_inflight: false,
            upstream_write_inflight: false,
            connect_inflight: false,
            upstream_epoch: 0,
            request_sent: false,
            downstream_eof: false,
            upstream_eof: false,
            fin_queued: false,
            splice: false,
            deadline: None,
        };
        let Ok((slot, generation)) = self.slab.insert(session) else {
            return; // guard closes fd
        };
        {
            let Some(s) = self.slab.get_mut(slot) else {
                return;
            };
            s.downstream = StreamFd(fd);
        }
        std::mem::forget(guard); // ownership moved into the session
        let token = Token::new(Op::DownstreamRead, slot, generation, 0);
        if self.engine.add_stream(fd, token).is_err() {
            self.close_session(slot, generation, "add-stream-failed");
            return;
        }
        {
            let mut io = self.io_for(slot, generation);
            h.on_connected(&mut io);
        }
        self.arm_downstream_read(slot, generation);
    }
}

/// Worker thread entry — owns transport state and the handler, drives the
/// loop until drain completes.
struct WorkerInner {
    state: WorkerState,
    handler: Box<dyn Handler>,
}

impl WorkerInner {
    /// Main loop: commands → accepts → engine CQEs → timers.
    pub fn run(mut self, cmd_rx: SpscReceiver<WorkerCmd, 64>) {
        for (i, l) in self.state.listeners.iter().enumerate() {
            let _ = self
                .state
                .engine
                .add_listener(l.as_raw_fd(), Token::accept(i as u16));
        }
        let mut cqes: Vec<crate::engine::Cqe> = Vec::with_capacity(512);
        loop {
            while let Some(cmd) = cmd_rx.recv() {
                let WorkerInner { state, handler } = &mut self;
                state.apply_cmd(cmd, handler.as_mut());
            }
            if self.state.drain_finished() {
                break;
            }
            for lidx in 0..self.state.listeners.len() {
                let WorkerInner { state, handler } = &mut self;
                state.accept_pending(lidx, handler.as_mut());
            }
            if self.state.drain_finished() {
                break;
            }

            let timeout = self.state.next_timeout();
            match self.state.engine.poll(timeout, &mut cqes) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => break, // engine failure: exit worker
            }
            for cqe in cqes.drain(..) {
                if cqe.token.op() == Op::Accept {
                    let aux = usize::from(cqe.token.aux());
                    let WorkerInner { state, handler } = &mut self;
                    state.accept_pending(aux, handler.as_mut());
                } else {
                    let WorkerInner { state, handler } = &mut self;
                    state.dispatch_cqe(cqe, handler.as_mut());
                }
            }
            // Inline-completed connects: deliver the deferred callback.
            if !self.state.inline_connected.is_empty() {
                let queued = std::mem::take(&mut self.state.inline_connected);
                for (slot, generation) in queued {
                    if !self.state.valid(slot, generation) {
                        continue;
                    }
                    let WorkerInner { state, handler } = &mut self;
                    state.do_upstream_connected(slot, generation);
                    let mut io = state.io_for(slot, generation);
                    handler.on_upstream_connected(&mut io);
                }
            }
            let now = Instant::now();
            let WorkerInner { state, handler } = &mut self;
            state.run_timers(now, handler.as_mut());
            if self.state.drain_finished() {
                break;
            }
        }
    }
}

/// Spawns a worker thread (pinned when `config.core` is set) over a
/// pre-bound listener.
///
/// # Errors
/// Thread spawn or engine creation failure.
#[allow(
    clippy::expect_used,
    reason = "startup-only allocation failures abort the worker, never a live request"
)]
pub fn spawn(
    id: usize,
    config: WorkerConfig,
    listener: StdTcpListener,
    registry: Arc<Registry>,
    events: Arc<EventRing<LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
    factory: &dyn HandlerFactory,
) -> io::Result<WorkerHandle> {
    let connections = registry.register(
        "vane_active_connections",
        vane_observe::metrics::MetricKind::Gauge,
    );
    let ctx = WorkerCtx {
        id,
        registry,
        events,
        config: config.clone(),
        connections,
    };
    let handler = factory.build(&ctx);
    let (cmd_tx, cmd_rx) = crate::spsc::channel::<WorkerCmd, 64>();
    let done = Arc::new(AtomicBool::new(false));
    let done2 = Arc::clone(&done);
    let mode = factory.mode();
    let core = config.core;
    let pool_slots = config.pool_slots;
    let ring_entries = config.ring_entries;
    let sqpoll = config.sqpoll;
    let force_mio = config.force_mio;
    let max_sessions = config.max_sessions;

    let builder = std::thread::Builder::new().name(format!("vane-worker-{id}"));
    let handle = builder.spawn(move || {
        if let Some(core) = core {
            // Pin to the requested core (TH-01). Failure is non-fatal.
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            if let Some(c) = cores.get(core) {
                let _ = core_affinity::set_for_current(*c);
            }
        }
        let pool = BufferPool::new(pool_slots, DEFAULT_BUF_SIZE).expect("fixed pool allocation");
        let engine: Box<dyn Engine> = if force_mio {
            Box::new(crate::engine::mio_engine::MioEngine::new(&pool).expect("mio engine init"))
        } else {
            match create_engine(ring_entries, Some(&pool), sqpoll) {
                Ok(e) => e,
                Err(_) => Box::new(
                    crate::engine::mio_engine::MioEngine::new(&pool).expect("mio fallback"),
                ),
            }
        };
        let slab = SessionSlab::new(max_sessions).expect("session slab");
        let inner = WorkerInner {
            state: WorkerState {
                ctx,
                engine,
                pool,
                slab,
                listeners: vec![listener],
                timers: BinaryHeap::new(),
                draining: false,
                drain_deadline: None,
                mode,
                inline_connected: Vec::new(),
            },
            handler,
        };
        inner.run(cmd_rx);
        done2.store(true, Ordering::Release);
    })?;

    Ok(WorkerHandle {
        id,
        cmd: cmd_tx,
        done,
        join: Some(handle),
    })
}
