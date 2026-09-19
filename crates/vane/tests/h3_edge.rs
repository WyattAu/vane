//! HTTP/3 edge e2e (experimental `h3` feature): a quinn client
//! speaks HTTP/3 to the edge; the edge routes on the live snapshot
//! and forwards to an h1 upstream over the shared reqwest path.
#![cfg(feature = "h3")]

use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::time::Duration;

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
        };
        editor.insert(builder.compile().expect("route"));
    });
    router
}

/// h1 upstream: responds with a canned body per request.
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

#[tokio::test]
async fn h3_edge_roundtrip() {
    let _ = tracing_subscriber::fmt::try_init();
    let upstream = spawn_upstream();
    let router = test_router(upstream);
    let edge = Arc::new(vane::h3_edge::H3Edge::new(
        router,
        Arc::new(vane_observe::metrics::Registry::new()),
        None,
    ));

    // Server TLS material (self-signed, DNS localhost) with h3 ALPN.
    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let cert_der = certs.cert.der().clone();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        certs.signing_key.serialize_pem().as_bytes(),
    ))
    .expect("key pem")
    .expect("key");
    let mut server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)
        .expect("server cert");
    server_tls.alpn_protocols = vane::h3_edge::alpn_protocols();

    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("udp bind");
    let local: std::net::SocketAddr = udp.local_addr().expect("addr");
    vane::h3_edge::spawn(udp, Arc::new(server_tls), edge);

    // Client: rustls trusting the server cert, h3 ALPN.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).expect("root");
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vane::h3_edge::alpn_protocols();
    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).expect("quinn client");
    let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
    client_cfg.transport_config(Arc::new({
        let mut t = quinn::TransportConfig::default();
        t.max_idle_timeout(Some(Duration::from_secs(10).try_into().expect("idle")));
        t
    }));
    let mut client_endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().expect("addr")).expect("client endpoint");
    client_endpoint.set_default_client_config(client_cfg);

    let quinn_conn = client_endpoint
        .connect(local, "localhost")
        .expect("connect")
        .await
        .expect("quinn connect");
    let (mut driver, mut send_request) = h3::client::new(h3_quinn::Connection::new(quinn_conn))
        .await
        .expect("h3 handshake");
    tokio::spawn(async move {
        // Drive control frames until the peer closes the connection.
        let err = driver.wait_idle().await;
        eprintln!("h3 driver: {err:?}");
    });

    let req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/data")
        .body(())
        .expect("request");
    let mut stream = send_request.send_request(req).await.expect("send");
    // GET has no body: close the request side so the server's
    // recv_data loop sees the FIN.
    let _ = stream.finish().await;
    let response = stream.recv_response().await.expect("response");
    assert_eq!(response.status(), 200, "h3 status");

    let mut body = Vec::new();
    while let Ok(Some(mut chunk)) = stream.recv_data().await {
        use bytes::Buf as _;
        body.extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
    }
    assert_eq!(body, b"h3-edge!", "h3 body relayed");
}
