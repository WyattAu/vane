//! Handler trait — the L4/L7 plug-in point between the transport engine and
//! protocol logic (HTTP parsing/routing lives above this; L4 splice below).

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;

use crate::worker::WorkerCtx;

/// Proxy mode a handler serves (chosen per listener).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// L7: HTTP/1.1 parsing + filter pipeline + upstream relay.
    Http,
    /// L4: kernel splice passthrough (`IO-04`), no parsing.
    L4,
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

    /// Session deadline expired (idle timeout / drain deadline).
    fn on_deadline(&mut self, io: &mut SessionIo<'_>) {
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
        self.worker.close_session(self.slot, self.generation);
    }

    /// Arms (or clears) the session deadline.
    pub fn set_deadline(&mut self, at: Option<Instant>) {
        self.worker.set_deadline(self.slot, self.generation, at);
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
