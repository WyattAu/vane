//! Minimal reproduction of the stub-accept stall: a tokio listener with
//! a pending accept task must wake when a peer connects on the same
//! multi-thread runtime.

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stub_accept_wakes_on_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        eprintln!("DBG stub: task started, pending accept");
        let res = listener.accept().await;
        eprintln!("DBG stub: accept resolved");
        let _ = tx.send(res.map(|_| ()).map_err(|e| e.to_string()));
    });

    // Peer connects from a separate task (mirrors the edge's dial).
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tokio::net::TcpStream::connect(addr).await;
        eprintln!("DBG peer: connected");
    });

    match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(Ok(_))) => eprintln!("PASS: accept woke"),
        other => panic!("accept never resolved: {other:?}"),
    }
}

/// Same shape as large_body: the peer connects SYNCHRONOUSLY from the
/// main test task right after spawning the stub (no yield in between —
/// tests whether the runtime polls the stub task without a yield).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stub_accept_no_yield_before_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let res = listener.accept().await;
        let _ = tx.send(res.map(|_| ()).map_err(|e| e.to_string()));
    });

    // No yield: connect immediately from the main task.
    let _stream = tokio::net::TcpStream::connect(addr).await.expect("connect");

    match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(Ok(_))) => eprintln!("PASS: accept woke (no-yield variant)"),
        other => panic!("accept never resolved (no-yield): {other:?}"),
    }
}
