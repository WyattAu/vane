//! Fuzz target: router lookup with arbitrary paths (QA-03).
#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_router::{Policy, RouteBuilder, Router};
use std::net::SocketAddr;

fn build_router() -> Router {
    let router = Router::new();
    let routes = [
        ("/", "root"),
        ("/users/:id", "user"),
        ("/users/:id/posts/:pid", "post"),
        ("/files/*rest", "files"),
        ("/health", "health"),
    ];
    router.update(|editor| {
        for (pattern, cluster) in routes {
            let builder = RouteBuilder {
                host: None,
                pattern: pattern.to_string(),
                methods: Vec::new(),
                cluster: cluster.to_string(),
                strip_prefix: None,
                timeout_ms: None,
                upstream_h2: false,
                backends: vec![vane_router::Backend::new(
                    SocketAddr::from(([127, 0, 0, 1], 1)),
                    1,
                )],
                policy: Policy::P2C,
                compression: false,
                outlier: None,
                allowed_spiffe_prefixes: Vec::new(),
                priority: 0,
                retry: Default::default(),
                mirror: None,
                mirror_backends: Vec::new(),
                cors: None,
            };
            if let Ok(entry) = builder.compile() {
                editor.insert(entry);
            }
        }
    });
    router
}

/// Built once per process, not once per input.
///
/// The router snapshot is a crossbeam-epoch allocation, and rebuilding it
/// inside the target churned a fresh table (and leaked the old epoch
/// snapshot) on every single input: libFuzzer's allocator saw 241k
/// allocations for a 2-byte path and the job died with an out-of-memory
/// that had nothing to do with routing. The target is about lookup.
fn router() -> &'static Router {
    static R: std::sync::OnceLock<Router> = std::sync::OnceLock::new();
    R.get_or_init(build_router)
}

fuzz_target!(|data: &[u8]| {
    let path = String::from_utf8_lossy(data);
    let table = router().load();
    if let Some(m) = table.table().lookup(None, &path) {
        // Invariants on successful matches.
        assert!(!m.terminal.value.cluster.is_empty());
        for (start, end) in m.params.iter().map(|(_, s, e)| (*s as usize, *e as usize)) {
            assert!(start <= end);
            assert!(end <= path.len());
        }
        if let Some((s, e)) = m.wildcard {
            assert!(s <= e);
            assert!(e as usize <= path.len());
        }
    }
});
