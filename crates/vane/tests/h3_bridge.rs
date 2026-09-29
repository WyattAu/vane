//! HTTP/3 upstream bridge e2e (feature `h3`): the bridge's
//! engine-facing loopback h1 side re-originates requests over QUIC to
//! a real h3 backend — milestone-4 acceptance (docs/h3-design.md).
//!
//! Everything runs on plain OS threads (the h3 client/edge stack
//! nests badly under `#[tokio::test]` — see the note in
//! tests/h3_client.rs).

#![cfg(feature = "h3")]

use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use vane_router::Router;

fn init_log() {
    let _ = tracing_subscriber::fmt::try_init();
}

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
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\nconnection: keep-alive\r\n\r\nh3-edge!").is_err() {
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

/// Hosts the vane h3 edge on a dedicated current-thread runtime on a
/// plain OS thread. Returns the QUIC address and the server cert
/// (DER) for trust material.
fn spawn_h3_edge_backend(router: Arc<Router>) -> (SocketAddr, String) {
    let edge = Arc::new(vane::h3_edge::H3Edge::new(
        router,
        Arc::new(vane_observe::metrics::Registry::new()),
        None,
    ));
    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let cert_pem = certs.cert.pem();
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
    server_tls.alpn_protocols = vec![b"h3".to_vec()];

    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let local: SocketAddr = udp.local_addr().expect("addr");
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
    (local, cert_pem)
}

/// One raw h1 request to the bridge; returns the full response bytes
/// (empty = connection closed without a response).
fn h1_get(addr: SocketAddr, path: &str) -> Vec<u8> {
    let mut s = std::net::TcpStream::connect(addr).expect("bridge connect");
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write");
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    out
}

fn bridge_tls(ca: &std::path::Path) -> vane_control::config::H3UpstreamTls {
    vane_control::config::H3UpstreamTls {
        ca: ca.display().to_string(),
        server_name: "localhost".into(),
        client_cert: None,
        client_key: None,
        alpn: None,
    }
}

/// The engine-facing side: plain h1 in, the bridge re-originates over
/// h3 to the vane h3 edge, the response comes back as h1.
#[test]
fn h3_bridge_roundtrip() {
    init_log();
    let upstream = spawn_upstream();
    let (quic_addr, cert_pem) = spawn_h3_edge_backend(test_router(upstream));

    let dir = tempfile::tempdir().expect("dir");
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, cert_pem).expect("write ca");

    let bridge = vane::h3_bridge::spawn(
        vec![quic_addr],
        Some(bridge_tls(&ca)),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .expect("bridge");

    let resp = h1_get(bridge, "/x");
    let resp = String::from_utf8_lossy(&resp).into_owned();
    assert!(resp.contains("200 OK"), "status via h3 bridge: {resp:?}");
    assert!(resp.contains("h3-edge!"), "body via h3 bridge: {resp:?}");
}

/// An h3 backend that never answers: the bridge closes abruptly so
/// the worker's failover sees premature EOF (no synthetic 502).
#[test]
fn h3_bridge_dead_backend_closes() {
    // A bound-then-dropped UDP port: QUIC dials black-hole → timeout.
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let dead: SocketAddr = udp.local_addr().expect("addr");
    drop(udp);

    let dir = tempfile::tempdir().expect("dir");
    let ca = dir.path().join("ca.pem");
    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    std::fs::write(&ca, certs.cert.pem()).expect("write ca");

    let bridge = vane::h3_bridge::spawn(
        vec![dead],
        Some(bridge_tls(&ca)),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .expect("bridge");

    let resp = h1_get(bridge, "/x");
    assert!(
        resp.is_empty(),
        "dead backend must close without a response, got {resp:?}"
    );
}

/// mTLS: an h3 backend requiring a client cert; the bridge presents
/// its SVID material (`h3_tls.client_cert/key`). Without the cert the
/// handshake is rejected → abrupt close.
#[test]
fn h3_bridge_mtls_client_cert() {
    use rcgen::{CertificateParams, KeyPair};

    let dir = tempfile::tempdir().expect("dir");
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(vec!["bridge-ca".into()]).expect("params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    // Backend server cert: DNS localhost.
    let srv_params = CertificateParams::new(vec!["localhost".into()]).expect("params");
    let srv_key = KeyPair::generate().expect("srv key");
    let srv = srv_params.signed_by(&srv_key, &issuer).expect("srv");

    // Client SVID for the bridge.
    let cli_params = CertificateParams::new(vec!["bridge".into()]).expect("params");
    let cli_key = KeyPair::generate().expect("cli key");
    let cli = cli_params.signed_by(&cli_key, &issuer).expect("cli");

    let ca_pem = dir.path().join("ca.pem");
    let cli_pem = dir.path().join("cli.pem");
    let cli_key_pem = dir.path().join("cli-key.pem");
    std::fs::write(&ca_pem, ca_cert.pem()).expect("write");
    std::fs::write(&cli_pem, cli.pem()).expect("write");
    std::fs::write(&cli_key_pem, cli_key.serialize_pem()).expect("write");

    // The h3 backend: raw quinn + h3 requiring a client cert from the
    // CA, serving a canned h3 response, on its own runtime thread.
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let quic_addr: SocketAddr = udp.local_addr().expect("addr");
    let srv_certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(srv.pem().as_bytes()))
            .map(Result::unwrap)
            .collect();
    let server_key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        srv_key.serialize_pem().as_bytes(),
    ))
    .expect("pem")
    .expect("key");
    let client_cas: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(&ca_pem).expect("ca pem"),
    ))
    .map(Result::unwrap)
    .collect();
    let mut cas_roots = rustls::RootCertStore::empty();
    for c in client_cas {
        cas_roots.add(c).expect("ca root");
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(cas_roots))
        .build()
        .expect("verifier");
    let mut server_tls = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(srv_certs, server_key)
        .expect("server cert");
    server_tls.alpn_protocols = vec![b"h3".to_vec()];

    let tls_cfg = Arc::new(server_tls);
    std::thread::Builder::new()
        .name("h3-mtls-backend".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("backend runtime");
            rt.block_on(async move {
                let qcfg = quinn::crypto::rustls::QuicServerConfig::try_from((*tls_cfg).clone())
                    .expect("quinn server");
                let endpoint = quinn::Endpoint::new(
                    quinn::EndpointConfig::default(),
                    Some(quinn::ServerConfig::with_crypto(Arc::new(qcfg))),
                    udp,
                    Arc::new(quinn::TokioRuntime),
                )
                .expect("endpoint");
                while let Some(incoming) = endpoint.accept().await {
                    let Ok(conn) = incoming.await else { continue };
                    tokio::spawn(async move {
                        let Ok(mut send) =
                            h3::server::Connection::new(h3_quinn::Connection::new(conn)).await
                        else {
                            return;
                        };
                        while let Ok(Some(resolver)) = send.accept().await {
                            let Ok((req, mut stream)) = resolver.resolve_request().await else {
                                return;
                            };
                            let _ = req;
                            let resp = http::Response::builder()
                                .status(200)
                                .header("content-type", "text/plain")
                                .body(())
                                .expect("resp");
                            if stream.send_response(resp).await.is_err() {
                                return;
                            }
                            let _ = stream
                                .send_data(bytes::Bytes::from_static(b"mtls-h3!"))
                                .await;
                            let _ = stream.finish().await;
                        }
                    });
                }
            });
        })
        .expect("backend thread");

    // Bridge WITH the SVID: roundtrip works.
    let tls = vane_control::config::H3UpstreamTls {
        ca: ca_pem.display().to_string(),
        server_name: "localhost".into(),
        client_cert: Some(cli_pem.display().to_string()),
        client_key: Some(cli_key_pem.display().to_string()),
        alpn: None,
    };
    let bridge = vane::h3_bridge::spawn(
        vec![quic_addr],
        Some(tls),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .expect("bridge");
    let resp = String::from_utf8_lossy(&h1_get(bridge, "/y")).into_owned();
    assert!(resp.contains("200 OK"), "mTLS bridge status: {resp:?}");
    assert!(resp.contains("mtls-h3!"), "mTLS bridge body: {resp:?}");

    // Bridge WITHOUT the cert: handshake rejected → abrupt close.
    let anon_bridge = vane::h3_bridge::spawn(
        vec![quic_addr],
        Some(bridge_tls(&ca_pem)),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .expect("anon bridge");
    let resp = h1_get(anon_bridge, "/y");
    assert!(
        resp.is_empty(),
        "anon bridge must be rejected without a response, got {}",
        String::from_utf8_lossy(&resp)
    );
}

/// Health-aware routing: one live backend, one black-holed. The
/// bridge's active probes mark the dead one down and the in-bridge
/// retry covers the optimistic-start window — every request must be
/// served by the live backend.
#[test]
fn h3_bridge_health_routes_around_dead_backend() {
    init_log();
    let upstream = spawn_upstream();
    let (live, cert_pem) = spawn_h3_edge_backend(test_router(upstream));

    let dead_udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let dead: SocketAddr = dead_udp.local_addr().expect("addr");
    drop(dead_udp);

    let dir = tempfile::tempdir().expect("dir");
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, cert_pem).expect("write ca");

    let bridge = vane::h3_bridge::spawn(
        vec![live, dead],
        Some(bridge_tls(&ca)),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(2),
            health_path: Some("/healthz".into()),
            probe_interval: Duration::from_millis(200),
        },
    )
    .expect("bridge");

    // Warm-up: let a probe cycle mark the dead backend down.
    std::thread::sleep(Duration::from_millis(700));

    for i in 0..8 {
        let resp = h1_get(bridge, &format!("/h{i}"));
        let resp = String::from_utf8_lossy(&resp).into_owned();
        assert!(
            resp.contains("200 OK") && resp.contains("h3-edge!"),
            "request {i} must be served by the live backend: {resp:?}"
        );
    }
}

/// Connection reuse: N requests (each on its own h1 side-connection)
/// ride ONE pooled QUIC connection — the backend sees exactly one
/// accept (deterministic: counted server-side).
#[test]
fn h3_bridge_reuses_one_quic_connection() {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    let accepts = Arc::new(AtomicUsize::new(0));

    // Raw quinn+h3 backend with an accept counter.
    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let server_key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        certs.signing_key.serialize_pem().as_bytes(),
    ))
    .expect("pem")
    .expect("key");
    let mut server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certs.cert.der().clone()], server_key)
        .expect("server cert");
    server_tls.alpn_protocols = vec![b"h3".to_vec()];
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let quic_addr: SocketAddr = udp.local_addr().expect("addr");
    let tls_cfg = Arc::new(server_tls);
    let accept_count = Arc::clone(&accepts);
    std::thread::Builder::new()
        .name("h3-reuse-backend".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("backend runtime");
            rt.block_on(async move {
                let qcfg = quinn::crypto::rustls::QuicServerConfig::try_from((*tls_cfg).clone())
                    .expect("quinn server");
                let endpoint = quinn::Endpoint::new(
                    quinn::EndpointConfig::default(),
                    Some(quinn::ServerConfig::with_crypto(Arc::new(qcfg))),
                    udp,
                    Arc::new(quinn::TokioRuntime),
                )
                .expect("endpoint");
                while let Some(incoming) = endpoint.accept().await {
                    accept_count.fetch_add(1, AtomicOrdering::Relaxed);
                    let Ok(conn) = incoming.await else { continue };
                    tokio::spawn(async move {
                        let Ok(mut send) =
                            h3::server::Connection::new(h3_quinn::Connection::new(conn)).await
                        else {
                            return;
                        };
                        while let Ok(Some(resolver)) = send.accept().await {
                            let Ok((req, mut stream)) = resolver.resolve_request().await else {
                                return;
                            };
                            let _ = req;
                            let resp = http::Response::builder()
                                .status(200)
                                .header("content-type", "text/plain")
                                .body(())
                                .expect("resp");
                            if stream.send_response(resp).await.is_err() {
                                return;
                            }
                            let _ = stream
                                .send_data(bytes::Bytes::from_static(b"reused!"))
                                .await;
                            let _ = stream.finish().await;
                        }
                    });
                }
            });
        })
        .expect("backend thread");

    let dir = tempfile::tempdir().expect("dir");
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, certs.cert.pem()).expect("write ca");

    let bridge = vane::h3_bridge::spawn(
        vec![quic_addr],
        Some(bridge_tls(&ca)),
        vane::h3_bridge::BridgeOptions {
            first_byte_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .expect("bridge");

    for i in 0..4 {
        let resp = h1_get(bridge, &format!("/r{i}"));
        let resp = String::from_utf8_lossy(&resp).into_owned();
        assert!(resp.contains("200 OK"), "request {i}: {resp:?}");
        assert!(resp.contains("reused!"), "request {i} body: {resp:?}");
    }
    // Four h1 requests (four loopback TCP connections), one QUIC
    // accept: the pool multiplexes instead of re-handshaking.
    assert_eq!(
        accepts.load(AtomicOrdering::Relaxed),
        1,
        "expected exactly one QUIC handshake for all requests"
    );
}
