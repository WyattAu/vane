//! Minimal vane-core worker test: TCP echo through the mio engine.

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use vane_core::handler::{Handler, HandlerFactory, Mode, SessionIo};
use vane_core::{WorkerConfig, spawn_worker};
use vane_observe::metrics::Registry;

struct Echo;

impl Handler for Echo {
    fn on_downstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        io.respond(data);
    }

    fn on_upstream_connected(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_data(&mut self, _io: &mut SessionIo<'_>, _data: &[u8]) {}
    fn on_downstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, _e: std::io::Error) {
        io.close();
    }
}

struct EchoFactory;

impl HandlerFactory for EchoFactory {
    fn mode(&self) -> Mode {
        Mode::Http
    }

    fn build(&self, _ctx: &vane_core::WorkerCtx) -> Box<dyn Handler> {
        Box::new(Echo)
    }
}

fn run_echo(force_mio: bool) {
    let listener =
        vane_core::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let registry = Arc::new(Registry::new());
    let events = Arc::new(vane_observe::ring::EventRing::new());
    let cfg = WorkerConfig {
        force_mio,
        ..WorkerConfig::default()
    };
    let factory = EchoFactory;
    let mut handle = spawn_worker(0, cfg, listener, registry, events, &factory).expect("spawn");

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"ping").expect("write");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let mut buf = [0u8; 4];
    match client.read_exact(&mut buf) {
        Ok(()) => assert_eq!(&buf, b"ping"),
        Err(e) => panic!("no echo: {e}"),
    }
    let _ = handle
        .cmd
        .send(vane_core::WorkerCmd::Shutdown { deadline_ms: 100 });
    handle.join();
}

#[test]
fn echo_through_worker_mio() {
    run_echo(true);
}

#[test]
fn echo_through_worker_uring() {
    run_echo(false);
}

/// Client RSTs mid-session: the worker must reap the session without
/// panicking or wedging (engine error path).
#[test]
fn client_reset_is_reaped() {
    let listener =
        vane_core::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let registry = Arc::new(Registry::new());
    let events = Arc::new(vane_observe::ring::EventRing::new());
    let cfg = WorkerConfig {
        force_mio: true,
        ..WorkerConfig::default()
    };
    let factory = EchoFactory;
    let mut handle = spawn_worker(0, cfg, listener, registry, events, &factory).expect("spawn");

    // Connect, exchange, then abort with SO_LINGER 0 (RST on close).
    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"ping").expect("write");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).expect("echo");
    // SAFETY: standard setsockopt on a live socket.
    unsafe {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of!(linger).cast(),
            std::mem::size_of::<libc::linger>() as u32,
        );
    }
    drop(client); // RST

    // Worker still serves a fresh connection afterwards.
    let mut fresh = TcpStream::connect(addr).expect("reconnect");
    fresh.write_all(b"ok!").expect("write");
    fresh
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let mut buf2 = [0u8; 3];
    fresh.read_exact(&mut buf2).expect("echo2");
    assert_eq!(&buf2, b"ok!");

    let _ = handle
        .cmd
        .send(vane_core::WorkerCmd::Shutdown { deadline_ms: 100 });
    handle.join();
}

/// Client half-close (FIN) after a request: worker observes EOF and
/// closes the session; a subsequent connection still works.
#[test]
fn client_half_close_is_handled() {
    let listener =
        vane_core::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let registry = Arc::new(Registry::new());
    let events = Arc::new(vane_observe::ring::EventRing::new());
    let cfg = WorkerConfig {
        force_mio: true,
        ..WorkerConfig::default()
    };
    let factory = EchoFactory;
    let mut handle = spawn_worker(0, cfg, listener, registry, events, &factory).expect("spawn");

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"fin").expect("write");
    // Half-close: FIN after data.
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("half close");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let _buf = [0u8; 3];
    // The echo handler may or may not reply after EOF — either way the
    // session must be reaped: shutdown of our read side too, then verify
    // the worker stays alive with a new connection.
    drop(client);

    let mut fresh = TcpStream::connect(addr).expect("reconnect after fin");
    fresh.write_all(b"yes").expect("write");
    fresh
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let mut buf2 = [0u8; 3];
    fresh.read_exact(&mut buf2).expect("echo after fin session");
    assert_eq!(&buf2, b"yes");

    let _ = handle
        .cmd
        .send(vane_core::WorkerCmd::Shutdown { deadline_ms: 100 });
    handle.join();
}

/// Handler that buffers upstream-bound bytes without ever connecting:
/// exercises the pending_up overflow guard.
struct Flood;

impl Handler for Flood {
    fn on_downstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        let _ = data;
        io.write_upstream(&[0xABu8; 8192]);
    }

    fn on_upstream_connected(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_data(&mut self, _io: &mut SessionIo<'_>, _data: &[u8]) {}
    fn on_downstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, _e: std::io::Error) {
        io.close();
    }
}

struct FloodFactory;

impl HandlerFactory for FloodFactory {
    fn mode(&self) -> Mode {
        Mode::Http
    }

    fn build(&self, _ctx: &vane_core::WorkerCtx) -> Box<dyn Handler> {
        Box::new(Flood)
    }
}

/// Unbounded upstream buffering trips the overflow guard: the session is
/// reaped (client sees EOF) and the worker keeps serving.
#[test]
fn upstream_buffer_overflow_reaps_session() {
    let listener =
        vane_core::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let registry = Arc::new(Registry::new());
    let events = Arc::new(vane_observe::ring::EventRing::new());
    let cfg = WorkerConfig {
        force_mio: true,
        ..WorkerConfig::default()
    };
    let factory = FloodFactory;
    let mut handle = spawn_worker(0, cfg, listener, registry, events, &factory).expect("spawn");

    // Three 8 KiB writes exceed 4 x 4 KiB pool slots → guard fires.
    // Each client write is one downstream event buffering 8 KiB.
    let mut client = TcpStream::connect(addr).expect("connect");
    for _ in 0..4 {
        client.write_all(b"go").expect("write");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    // The session dies: reads return EOF (possibly after WouldBlock spins).
    let mut buf = [0u8; 8];
    let mut eof = false;
    for _ in 0..50 {
        match client.read(&mut buf) {
            Ok(0) => {
                eof = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    assert!(eof, "overflowed session must be reaped");

    // Worker still accepts new sessions.
    let probe = TcpStream::connect(addr);
    assert!(probe.is_ok(), "worker must stay alive");

    let _ = handle
        .cmd
        .send(vane_core::WorkerCmd::Shutdown { deadline_ms: 100 });
    handle.join();
}

/// Handler that dials a UDS upstream on connect (exercises
/// SessionIo::connect_upstream_unix through the worker).
struct UnixDialer {
    path: std::path::PathBuf,
}

impl Handler for UnixDialer {
    fn on_connected(&mut self, io: &mut SessionIo<'_>) {
        if !io.connect_upstream_unix(self.path.clone()) {
            io.close();
        }
    }

    fn on_downstream_data(&mut self, _io: &mut SessionIo<'_>, _data: &[u8]) {}

    fn on_upstream_connected(&mut self, io: &mut SessionIo<'_>) {
        io.write_upstream(b"uds-ping");
    }
    fn on_upstream_data(&mut self, io: &mut SessionIo<'_>, data: &[u8]) {
        io.respond(data);
    }
    fn on_downstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_eof(&mut self, _io: &mut SessionIo<'_>) {}
    fn on_upstream_error(&mut self, io: &mut SessionIo<'_>, _e: std::io::Error) {
        io.close();
    }
}

struct UnixDialerFactory {
    path: std::path::PathBuf,
}

impl HandlerFactory for UnixDialerFactory {
    fn mode(&self) -> Mode {
        Mode::Http
    }

    fn build(&self, _ctx: &vane_core::WorkerCtx) -> Box<dyn Handler> {
        Box::new(UnixDialer {
            path: self.path.clone(),
        })
    }
}

/// UDS upstream echo through the worker (connect_upstream_unix path).
#[test]
fn unix_upstream_roundtrip() {
    let dir = tempfile::tempdir().expect("dir");
    let sock_path = dir.path().join("up.sock");

    // UDS echo upstream.
    let uds_listener = std::os::unix::net::UnixListener::bind(&sock_path).expect("uds bind");
    std::thread::spawn(move || {
        for stream in uds_listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                use std::io::{Read, Write};
                let mut buf = [0u8; 4096];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if s.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

    let listener =
        vane_core::tcp_listener("127.0.0.1:0".parse().expect("addr"), true, 64).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let registry = Arc::new(Registry::new());
    let events = Arc::new(vane_observe::ring::EventRing::new());
    let cfg = WorkerConfig {
        force_mio: true,
        ..WorkerConfig::default()
    };
    let factory = UnixDialerFactory { path: sock_path };
    let mut handle = spawn_worker(0, cfg, listener, registry, events, &factory).expect("spawn");

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .ok();
    let mut buf = [0u8; 8];
    // UDS connect completes async: poll for the echoed bytes.
    let mut got = false;
    for _ in 0..40 {
        match client.read(&mut buf) {
            Ok(8) => {
                got = true;
                break;
            }
            Ok(_) | Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
    assert!(got, "uds round-trip: {:?}", buf);
    assert_eq!(&buf, b"uds-ping");

    let _ = handle
        .cmd
        .send(vane_core::WorkerCmd::Shutdown { deadline_ms: 100 });
    handle.join();
}
