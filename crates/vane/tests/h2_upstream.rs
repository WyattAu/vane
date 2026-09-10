//! HTTP/2 upstream e2e (`h2` feature): a cleartext h2-only upstream
//! (prior knowledge — no ALPN) that would reject HTTP/1.1 entirely.
//! The h2 edge forwards with `http2_prior_knowledge` when the cluster
//! sets `http2 = true`, so a 200 here proves the upstream spoke h2.

#![cfg(feature = "h2")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use vane_observe::metrics::Registry;
use vane_router::table::{RouteEntry, Router};

/// Spawns an h2-prior-knowledge server: answers every request `200`
/// with body `h2-upstream` and the request path echoed in `x-path`.
/// An HTTP/1.1 client reaching it gets a handshake failure.
async fn spawn_h2_upstream() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                // Cleartext h2 (prior knowledge): the handshake consumes
                // the client preface; an h1 request fails it.
                let mut conn = match h2::server::handshake(stream).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Some(request) = conn.accept().await {
                    let Ok((request, mut respond)) = request else {
                        break;
                    };
                    let path = request.uri().path().to_owned();
                    let response = http::Response::builder()
                        .status(200)
                        .header("x-path", path)
                        .body(())
                        .expect("static");
                    let Ok(mut send) = respond.send_response(response, false) else {
                        break;
                    };
                    let _ = send.send_data(bytes::Bytes::from("h2-upstream"), true);
                }
            });
        }
    });
    addr
}

/// Spawns an HTTP/1.1 keep-alive stub answering `h1-upstream`.
async fn spawn_h1_upstream() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut s = stream;
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nh1-upstream").await.is_err() {
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

fn router_with(upstream: SocketAddr, h2_upstream: bool) -> Arc<Router> {
    let r = Router::new();
    r.update(|editor| {
        editor.insert(RouteEntry {
            host: None,
            pattern: "/*rest".into(),
            methods: Vec::new(),
            cluster: "up".into(),
            strip_prefix: None,
            timeout_ms: None,
            backends: vec![vane_router::Backend::new(upstream, 1)],
            upstream_h2: h2_upstream,
            policy: vane_router::Policy::P2C,
            gauges: Arc::new(vane_router::balancer::ConnGauges::new(1)),
            priority: 0,
        });
    });
    Arc::new(r)
}

/// Stands up an h2 edge on a local listener (cleartext h2 in — the edge
/// itself is protocol-agnostic on the ingress side) and issues one GET.
async fn request_via_edge(router: Arc<Router>, path: &str) -> (u16, String, String) {
    let edge = Arc::new(vane::h2_edge::H2Edge::new(
        router,
        Arc::new(Registry::new()),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("edge bind");
    let edge_addr = listener.local_addr().expect("edge addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let edge = Arc::clone(&edge);
            tokio::spawn(async move {
                let _ = edge.serve_connection(stream).await;
            });
        }
    });

    let io = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(edge_addr),
    )
    .await
    .expect("edge connect")
    .expect("edge tcp");

    let (mut send_request, connection) = h2::client::handshake(io).await.expect("h2 client");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = http::Request::builder()
        .method("GET")
        .uri(format!("http://edge{path}"))
        .body(())
        .expect("request");
    let (response, _flow) = send_request.send_request(request, true).expect("send");
    let (parts, mut body) = tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .expect("response in time")
        .expect("response")
        .into_parts();
    let status = parts.status.as_u16();
    let path_hdr = parts
        .headers
        .get("x-path")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let mut buf = Vec::new();
    while let Some(chunk) = body.data().await {
        match chunk {
            Ok(b) => {
                let len = b.len();
                buf.extend_from_slice(&b);
                let _ = body.flow_control().release_capacity(len);
            }
            Err(_) => break,
        }
    }
    (status, path_hdr, String::from_utf8_lossy(&buf).into_owned())
}

#[tokio::test]
async fn h2_upstream_end_to_end() {
    let upstream = spawn_h2_upstream().await;

    // Cluster flagged http2: the edge must use the prior-knowledge
    // client — this stub cannot answer anything but h2.
    let router = router_with(upstream, true);
    let (status, path_hdr, body) = request_via_edge(router, "/api/hello").await;
    assert_eq!(status, 200, "h2 upstream round-trip failed");
    assert_eq!(path_hdr, "/api/hello");
    assert_eq!(body, "h2-upstream");
}

#[tokio::test]
async fn h1_upstream_default_still_works() {
    let upstream = spawn_h1_upstream().await;

    // Default (http2 = false): h1 upstream client, h1 stub answers.
    let router = router_with(upstream, false);
    let (status, _, body) = request_via_edge(router, "/h1").await;
    assert_eq!(status, 200);
    assert_eq!(body, "h1-upstream");
}

#[tokio::test]
async fn h2_flag_on_h1_upstream_yields_502() {
    // http2 = true against an h1-only upstream: the prior-knowledge
    // client cannot downgrade, so the edge reports 502 instead of
    // silently speaking the wrong protocol.
    let upstream = spawn_h1_upstream().await;
    let router = router_with(upstream, true);
    let (status, _, _) = request_via_edge(router, "/nope").await;
    assert_eq!(status, 502, "expected 502, got {status}");
}
