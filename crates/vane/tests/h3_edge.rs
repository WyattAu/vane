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

/// Full-stack: a TLS listener with `h3 = true` advertises
/// `alt-svc: h3=":port"` on h1 responses and actually serves h3 on
/// the same port number.
#[tokio::test]
async fn h3_alt_svc_advertised_and_served() {
    let _ = tracing_subscriber::fmt::try_init();
    let upstream = spawn_upstream();
    let proxy_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let proxy: std::net::SocketAddr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);
    let upstream_addr = upstream;

    let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let dir = tempfile::tempdir().expect("dir");
    let cert_p = dir.path().join("cert.pem");
    let key_p = dir.path().join("key.pem");
    std::fs::write(&cert_p, certs.cert.pem()).expect("write");
    std::fs::write(&key_p, certs.signing_key.serialize_pem()).expect("write");

    let (dir2, cfg) = {
        let d = tempfile::tempdir().expect("dir");
        let path = d.path().join("vane.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[[listeners]]
address = "127.0.0.1:{proxy}"

[listeners.tls]
cert = "{cert_p}"
key = "{key_p}"
h3 = true

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#,
                proxy = proxy.port(),
                cert_p = cert_p.display(),
                key_p = key_p.display(),
                upstream = upstream_addr,
            ),
        )
        .expect("write");
        let p = path.to_str().expect("utf8").to_owned();
        (d, p)
    };
    let _cfg_guard = dir2;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _ = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(cfg),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
            shutdown: None,
        }));
    });
    for _ in 0..60 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // TLS h1 request: the response advertises alt-svc on this port.
    let certs_client = rcgen::generate_simple_self_signed(vec!["client".into()]).expect("c");
    let _ = certs_client;
    let mut roots = rustls::RootCertStore::empty();
    let der = {
        let mut buf = Vec::new();
        buf.extend_from_slice(&std::fs::read(&cert_p).expect("cert read"));
        let mut buf_slice = buf.as_slice();
        rustls_pemfile::certs(&mut buf_slice)
            .next()
            .expect("cert")
            .expect("cert der")
    };
    roots.add(der).expect("root");
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots.clone())
        .with_no_client_auth();
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("server name");
    let mut conn = rustls::ClientConnection::new(Arc::new(client_tls), server_name).expect("conn");
    let mut sock = std::net::TcpStream::connect(proxy).expect("tcp");
    sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(b"GET /data HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
        .expect("write");
    let mut out = Vec::new();
    let _ = tls.read_to_end(&mut out);
    let head = String::from_utf8_lossy(&out).into_owned();
    assert!(
        head.contains(&format!("alt-svc: h3=\":{}\"", proxy.port())),
        "alt-svc advertised: {head}"
    );

    // The advertised endpoint serves h3 on that same port.
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vane::h3_edge::alpn_protocols();
    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).expect("quinn");
    let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
    client_cfg.transport_config(Arc::new(quinn::TransportConfig::default()));
    let mut endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().expect("addr")).expect("client endpoint");
    endpoint.set_default_client_config(client_cfg);
    let quinn_conn = endpoint
        .connect(proxy, "localhost")
        .expect("connect")
        .await
        .expect("quinn connect");
    let (mut driver, mut send_request) = h3::client::new(h3_quinn::Connection::new(quinn_conn))
        .await
        .expect("h3 handshake");
    tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });
    let req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/data")
        .body(())
        .expect("request");
    let mut stream = send_request.send_request(req).await.expect("send");
    let _ = stream.finish().await;
    let response = stream.recv_response().await.expect("h3 response");
    assert_eq!(response.status(), 200, "h3 via advertised port");
}
