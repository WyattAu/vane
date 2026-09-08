//! Minimal vane-core worker test: TCP echo through the mio engine.

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::io::{Read, Write};
use std::net::TcpStream;
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

#[test]
fn echo_through_worker() {
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
