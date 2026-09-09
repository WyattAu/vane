//! Engine-level upstream connect test (mio + io_uring parity).

use std::net::SocketAddr;
use std::time::Duration;

use vane_core::buffer::{BufferPool, DEFAULT_BUF_SIZE};
#[allow(unused_imports)]
use vane_core::engine::Engine as _;
use vane_core::engine::create_engine;
use vane_core::token::Token;

#[test]
fn engine_connects_upstream() {
    // Listener to dial.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    let accept_thread = std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            drop(s);
        }
    });

    let pool = BufferPool::new(16, DEFAULT_BUF_SIZE).expect("pool");
    let mut engine = create_engine(64, Some(&pool), false).expect("engine");

    let token = Token::new(vane_core::Op::Connect, 0, 0, 0);
    let (fd, poll) = engine.connect(token, addr).expect("connect submit");
    assert_eq!(poll, vane_core::Poll::Pending);

    let mut cqes = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        engine
            .poll(Some(Duration::from_millis(50)), &mut cqes)
            .expect("poll");
        for cqe in cqes.drain(..) {
            if cqe.token.op() == vane_core::Op::Connect {
                cqe.result.expect("connect ok");
                connected = true;
            }
        }
        if connected {
            break;
        }
    }
    assert!(connected, "connect CQE never arrived");
    engine.remove(fd);
    // SAFETY: owned fd from the engine.
    unsafe { libc::close(fd) };
    // The acceptor loops forever; detach instead of join.
    std::mem::forget(accept_thread);
}
