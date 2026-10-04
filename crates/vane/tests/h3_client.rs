//! h3 client e2e (experimental `h3` feature): drives `h3_client::request`
//! (the milestone-4 client-side piece) against the vane h3 edge —
//! the same roundtrip the h3spec-era test exercised, now through the
//! shipped library API.
#![cfg(feature = "h3")]

use std::sync::Arc;

use vane_router::Router;

fn test_router(upstream: std::net::SocketAddr) -> Arc<Router> {
    let router = Arc::new(Router::new());
    router.update(|editor| {
        editor.replace_all(vec![]);
        let builder = vane_router::RouteBuilder {
            host: None,
            pattern: "/*rest".into(),
            methods: Vec::new(),
            cluster: "up".into(),
            strip_prefix: None,
            timeout_ms: None,
            backends: vec![vane_router::Backend::new(upstream, 1)],
            upstream_h2: false,
            compression: false,
            outlier: None,
            policy: vane_router::Policy::P2C,
            priority: 0,
            allowed_spiffe_prefixes: Vec::new(),
            retry: Default::default(),
            mirror: None,
            mirror_backends: Vec::new(),
        };
        editor.insert(builder.compile().expect("route"));
    });
    router
}

fn spawn_upstream() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                use std::io::{Read as _, Write as _};
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\nconnection: keep-alive\r\n\r\nh3-works").is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// Plain-thread harness (2026-09-28): the h3 client's internal
/// runtime thread times out under `#[tokio::test]` — the test's own
/// tokio context interferes with quinn. The bridge tests cover the
/// production shape; this drives the same library call from a plain
/// thread. The URI-authority fix resolved the header-build failure
/// the original note tracked.
#[test]
fn h3_client_roundtrip() {
    let upstream = spawn_upstream();
    let router = test_router(upstream);
    let edge = Arc::new(vane::h3_edge::H3Edge::new(
        router,
        Arc::new(vane_observe::metrics::Registry::new()),
        None,
    ));

    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let cert_der = certs.cert.der().clone();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        certs.signing_key.serialize_pem().as_bytes(),
    ))
    .expect("key pem")
    .expect("key");
    let mut server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key)
        .expect("server cert");
    server_tls.alpn_protocols = vane::h3_edge::alpn_protocols();

    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let local: std::net::SocketAddr = udp.local_addr().expect("addr");
    // The edge spawns onto tokio: host it on a dedicated runtime
    // thread (no test-runtime context — see the harness note above).
    std::thread::Builder::new()
        .name("h3-edge-host".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("edge runtime");
            rt.block_on(async move {
                vane::h3_edge::spawn(udp, Arc::new(server_tls), edge);
                std::future::pending::<()>().await;
            });
        })
        .expect("edge thread");
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Client TLS material trusting the server cert.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs.cert.der().clone()).expect("root");
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let cfg = vane::h3_client::H3ClientConfig {
        client_cert: None,
        server_certs: vec![certs.cert.der().clone()],
        server_name: "localhost".into(),
        alpn: vec![b"h3".to_vec()],
    };
    let _ = client_tls;

    let resp = vane::h3_client::request(
        local,
        &cfg,
        "GET",
        "/data",
        "localhost",
        &[],
        std::time::Duration::from_secs(10),
    )
    .expect("h3 request");
    assert_eq!(resp.status, 200, "h3 status");
    assert_eq!(resp.body, b"h3-works", "h3 body");
}
