//! Header parse latency (`PR-01` target: <15 ns hot path).
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

//! Header parse latency (`PR-01` target: <15 ns hot path).

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use vane_proto::request::{MAX_HEADERS, RequestView};

const REQ: &[u8] = b"GET /api/v1/users/42/posts?limit=10 HTTP/1.1\r\nHost: api.example.com\r\nUser-Agent: bench\r\nAccept: */*\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n";

fn bench_parse(c: &mut Criterion) {
    c.bench_function("http1_parse_head", |b| {
        b.iter(|| {
            let req: &[u8] = REQ;
            let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
            match RequestView::parse_in(black_box(req), &mut storage) {
                vane_proto::request::Parsed::Complete(_, len) => black_box(len),
                _ => panic!("parse failed"),
            }
        });
    });
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
