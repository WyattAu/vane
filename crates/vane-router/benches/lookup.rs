//! Route lookup latency (`CP-01` target: <80 ns @ 10^5 routes).
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

//! Route lookup latency (`CP-01` target: <80 ns @ 10^5 routes).

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::net::SocketAddr;
use vane_router::{Policy, RouteBuilder, RouteEntry, Router};

fn entry(host: Option<String>, pattern: String) -> RouteEntry {
    RouteBuilder {
        host,
        pattern,
        methods: Vec::new(),
        cluster: "bench".into(),
        strip_prefix: None,
        timeout_ms: None,
        backends: vec![vane_router::Backend::new(
            SocketAddr::from(([127, 0, 0, 1], 9000)),
            1,
        )],
        upstream_h2: false,
        compression: false,
        outlier: None,
        policy: Policy::P2C,
        priority: 0,
    }
    .compile()
    .expect("valid route")
}

fn bench_lookup(c: &mut Criterion) {
    // 10k route generation.
    let router = Router::new();
    router.update(|editor| {
        for i in 0..10_000usize {
            let host = format!("host{i}.example");
            editor.insert(entry(Some(host), format!("/app{i}/:id")));
        }
    });
    let guard = router.load();

    c.bench_function("router_lookup_10k_routes", |b| {
        b.iter(|| {
            let m = guard.table().lookup(
                Some(black_box("host5000.example")),
                black_box("/app5000/123"),
            );
            black_box(m.map(|x| x.terminal.value.cluster.clone()));
        });
    });
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
