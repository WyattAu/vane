//! SHM sidecar round-trip latency (`IP-01` target: <30 us).
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

//! SHM sidecar round-trip latency (`IP-01` target: <30 µs).

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;

use vane_shm::transport::{SidecarClient, SidecarConfig, SidecarServer};

fn bench_roundtrip(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("dir");
    let cfg = SidecarConfig {
        base: dir.path().join("bench"),
        slot_size: 64 * 1024,
        slots: 8,
    };
    let mut server = SidecarServer::open(&cfg).expect("server");
    let mut client = SidecarClient::open(&cfg).expect("client");

    // Server pump thread: echo.
    let pump = std::thread::spawn(move || {
        loop {
            match server.recv(Duration::from_millis(1)) {
                Ok(Some((id, data))) => {
                    let _ = server.reply(id, &data, Duration::from_millis(100));
                }
                _ => continue,
            }
        }
    });

    let payload = vec![0u8; 512];
    c.bench_function("shm_sidecar_rtt_512b", |b| {
        b.iter(|| {
            let id = client
                .send(black_box(&payload), Duration::from_secs(1))
                .expect("send");
            loop {
                if let Some((rid, resp)) = client.recv(Duration::from_secs(1)).expect("recv") {
                    assert_eq!(rid, id);
                    black_box(&resp);
                    break;
                }
            }
        });
    });

    pump.join().ok();
}

criterion_group!(benches, bench_roundtrip);
criterion_main!(benches);
