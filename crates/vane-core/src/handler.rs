//! Handler trait — the L4/L7 plug-in point between the transport engine and
//! protocol logic (HTTP parsing/routing lives above this; L4 splice below).

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;

/// Why a session deadline fired — handlers branch on this to decide
/// between closing idle conns and failing in-flight requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeadlineReason {
    /// No traffic within the idle window.
    Idle = 0,
    /// Upstream connect did not complete in time.
    Connect = 1,
    /// Upstream has not produced the response head in time.
    FirstByte = 2,
    /// Handler-defined (aux payload rides the timer).
    Custom(u8) = 3,
}

impl DeadlineReason {
    /// Packs a reason into the timer's aux payload.
    #[must_use]
    pub fn aux(self) -> u16 {
        match self {
            Self::Idle => 0,
            Self::Connect => 1,
            Self::FirstByte => 2,
            Self::Custom(b) => u16::from(b) | (1 << 8),
        }
    }

    /// Unpacks from the aux payload.
    #[must_use]
    pub fn from_aux(aux: u16) -> Self {
        match aux {
            0 => Self::Idle,
            1 => Self::Connect,
            2 => Self::FirstByte,
            other => Self::Custom((other & 0xff) as u8),
        }
    }
}

use crate::worker::WorkerCtx;

/// Proxy mode a handler serves (chosen per listener).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// L7: HTTP/1.1 parsing + filter pipeline + upstream relay.
    Http,
    /// L4: kernel splice passthrough (`IO-04`), no parsing.
    L4,
}

/// Outcome of an upstream dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamDial {
    /// Connect queued; completion arrives asynchronously.
    Initiated,
    /// Connected synchronously — the handler may proceed immediately.
    Established,
}

/// Per-connection handler. One instance is shared by all sessions of a
/// worker (single-threaded dispatch; per-connection state lives in the
/// session, reached through [`SessionIo`]).
pub trait Handler: Send + 'static {
    /// A new downstream connection was accepted.
    fn on_connected(&mut self, io: &mut SessionIo<'_>) {
        let _ = io;
    }

    /// Bytes arrived from the downstream (client) socket.
    fn on_downstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]);

    /// The queued downstream write batch fully flushed.
    fn on_downstream_flushed(&mut self, io: &mut SessionIo<'_>) {
        let _ = io;
    }

    /// Upstream connect established.
    fn on_upstream_connected(&mut self, io: &mut SessionIo<'_>);

    /// Bytes arrived from the upstream (backend) socket.
    fn on_upstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]);

    /// The queued upstream write batch fully flushed.
    fn on_upstream_flushed(&mut self, io: &mut SessionIo<'_>) {
        let _ = io;
    }

    /// Downstream EOF (client half-closed).
    fn on_downstream_eof(&mut self, io: &mut SessionIo<'_>);

    /// Upstream EOF.
    fn on_upstream_eof(&mut self, io: &mut SessionIo<'_>);

    /// Downstream I/O error (session should close).
    fn on_downstream_error(&mut self, io: &mut SessionIo<'_>, err: io::Error) {
        let _ = err;
        io.close();
    }

    /// Upstream I/O error.
    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, err: io::Error);

    /// Worker is draining (shutdown): finish in-flight work promptly.
    fn on_shutdown_hint(&mut self, io: &mut SessionIo<'_>) {
        let _ = io;
    }

    /// Session deadline expired; `reason` says which deadline.
    fn on_deadline(&mut self, io: &mut SessionIo<'_>, reason: DeadlineReason) {
        let _ = reason;
        io.close();
    }
}

/// Builds a [`Handler`] for each worker.
pub trait HandlerFactory: Send + Sync + 'static {
    /// Handler mode (affects listener wiring).
    fn mode(&self) -> Mode;

    /// Constructs the per-worker handler.
    fn build(&self, ctx: &WorkerCtx) -> Box<dyn Handler>;
}

/// Handler-facing control surface for one session.
///
/// The handler uses this to respond, relay, dial upstreams, and manage the
/// session lifecycle. Buffer management is automatic: writes copy into
/// pre-allocated pool slots (one user-space copy, zero `malloc`).
pub struct SessionIo<'a> {
    pub(crate) worker: &'a mut super::worker::WorkerState,
    pub(crate) slot: u32,
    pub(crate) generation: u16,
}

impl<'a> SessionIo<'a> {
    /// Session slot index (metrics/log correlation).
    #[must_use]
    pub fn slot_index(&self) -> u32 {
        self.slot
    }

    /// Queues bytes to the downstream socket.
    ///
    /// # Panics
    /// Never panics; on pool exhaustion the write is queued as pending
    /// bytes (bounded by `max_buffered` — beyond that the session is
    /// killed as runaway).
    pub fn respond(&mut self, bytes: &[u8]) {
        self.worker
            .downstream_write(self.slot, self.generation, bytes);
    }

    /// Queues bytes to the upstream socket (must be connected).
    pub fn write_upstream(&mut self, bytes: &[u8]) {
        self.worker
            .upstream_write(self.slot, self.generation, bytes);
    }

    /// Dials a TCP upstream for this session.
    ///
    /// Returns `false` when the connect could not even start; the
    /// completion arrives via [`Handler::on_upstream_connected`] or
    /// [`Handler::on_upstream_error`].
    pub fn connect_upstream(&mut self, addr: SocketAddr) -> bool {
        self.worker
            .connect_upstream(self.slot, self.generation, addr)
    }

    /// Dials a Unix-domain-socket upstream.
    pub fn connect_upstream_unix(&mut self, path: PathBuf) -> bool {
        self.worker
            .connect_upstream_unix(self.slot, self.generation, path)
    }

    /// Starts the zero-copy L4 splice pump (requires a connected upstream).
    pub fn start_splice(&mut self) -> bool {
        self.worker.start_splice(self.slot, self.generation)
    }

    /// Half-closes the downstream write side (streaming completion).
    pub fn downstream_eof_write(&mut self) {
        self.worker
            .shutdown_downstream_write(self.slot, self.generation);
    }

    /// Half-closes the upstream write side (end of request body).
    pub fn upstream_eof_write(&mut self) {
        self.worker
            .shutdown_upstream_write(self.slot, self.generation);
    }

    /// Closes the whole session.
    pub fn close(&mut self) {
        self.worker
            .close_session(self.slot, self.generation, "handler");
    }

    /// Arms (or clears) the session's single deadline slot with a reason.
    pub fn set_deadline(&mut self, at: Option<Instant>, reason: DeadlineReason) {
        self.worker
            .set_deadline(self.slot, self.generation, at, reason);
    }

    /// Detaches the upstream fd without closing it (connection pooling).
    /// Returns the raw descriptor; the caller owns it from here.
    #[must_use]
    pub fn detach_upstream(&mut self) -> Option<std::os::fd::RawFd> {
        self.worker.detach_upstream(self.slot, self.generation)
    }

    /// Discards a dead upstream (close + epoch bump) so stale completions
    /// for its descriptor can never touch the session again.
    pub fn discard_upstream(&mut self) {
        self.worker.discard_upstream(self.slot, self.generation);
    }

    /// Attaches a previously detached fd as this session's upstream
    /// (connection pooling checkout) and arms its read.
    pub fn attach_upstream(&mut self, fd: std::os::fd::RawFd) -> bool {
        self.worker.attach_upstream(self.slot, self.generation, fd)
    }

    /// Raw upstream descriptor (diagnostics).
    #[must_use]
    pub fn upstream_fd(&self) -> Option<std::os::fd::RawFd> {
        self.worker.upstream_fd(self.slot, self.generation)
    }

    /// `true` once the request head has been written to the upstream
    /// (failover decisions must not re-send after this point).
    #[must_use]
    pub fn request_sent_upstream(&self) -> bool {
        self.worker.request_sent(self.slot, self.generation)
    }

    /// Marks the request head as written upstream.
    pub fn mark_request_sent(&mut self) {
        self.worker.mark_request_sent(self.slot, self.generation);
    }

    /// Peer address of the downstream connection.
    #[must_use]
    pub fn peer(&self) -> Option<SocketAddr> {
        self.worker.peer_of(self.slot, self.generation)
    }

    /// `true` when the session has a connected upstream.
    #[must_use]
    pub fn has_upstream(&self) -> bool {
        self.worker.has_upstream(self.slot, self.generation)
    }
}
