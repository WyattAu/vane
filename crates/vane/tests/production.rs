//! M1 production-core e2e tests: TLS, connection pooling, failover,
//! timeouts, and the access-log drain.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Counts accepted connections; responds with `body` to every request.
fn spawn_counting_upstream(
    body: &'static str,
) -> (SocketAddr, Arc<AtomicUsize>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("addr");
    let conns = Arc::new(AtomicUsize::new(0));
    let conns2 = Arc::clone(&conns);
    let handle = std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            conns2.fetch_add(1, Ordering::SeqCst);
            let mut s = stream;
            let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = s.write_all(resp.as_bytes());
                        }
                    }
                }
            });
        }
    });
    (addr, conns, handle)
}

/// Writes the e2e config and runs vane::server on a fixed port.
fn spawn_proxy(config: String) -> SocketAddr {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");

    // Learn a free port, then substitute it into the config.
    let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let proxy_addr = probe.local_addr().expect("addr");
    drop(probe);
    let config = config.replace("LISTEN_PORT", &proxy_addr.port().to_string());
    std::fs::write(&path, config).expect("write");

    let config_path = path.display().to_string();
    // Keep the tempdir alive for the process lifetime via a leak.
    std::mem::forget(dir);
    let _handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(config_path),
            handover_from: None,
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    for _ in 0..100 {
        if TcpStream::connect(proxy_addr).is_ok() {
            return proxy_addr;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy did not come up");
}

fn http_get(proxy: SocketAddr, target: &str, host: &str) -> String {
    let mut s = TcpStream::connect(proxy).expect("connect proxy");
    let req = format!("GET {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).expect("write");
    let mut out = String::new();
    s.read_to_string(&mut out).expect("read");
    out
}

#[test]
fn keep_alive_pool_reuses_upstream_connections() {
    let (upstream, conns, _h) = spawn_counting_upstream("pool-body");
    let config = format!(
        r#"
[[listeners]]
address = "127.0.0.1:LISTEN_PORT"
workers = 1

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
pool_per_backend = 4
"#
    );
    let proxy = spawn_proxy(config);

    // 5 sequential requests over client keep-alive connections.
    for i in 0..5 {
        let mut s = TcpStream::connect(proxy).expect("connect");
        let req = format!("GET /r{i} HTTP/1.1\r\nHost: t\r\n\r\n");
        s.write_all(req.as_bytes()).expect("write");
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let mut buf = [0u8; 1024];
        let n = s.read(&mut buf).expect("read head");
        let head = String::from_utf8_lossy(&buf[..n]).into_owned();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        drop(s);
    }

    // The pool must have dialed far fewer upstream conns than requests
    // (relaxed bound: pooling working == <5; deterministic == 1 in-process).
    let upstream_conns = conns.load(Ordering::SeqCst);
    assert!(
        upstream_conns < 5,
        "pooling ineffective: {upstream_conns} upstream conns for 5 requests"
    );
}

#[test]
fn failover_serves_from_second_backend() {
    // Backend A: port that refuses connections.
    let dead = TcpListener::bind("127.0.0.1:0").expect("bind");
    let dead_addr = dead.local_addr().expect("addr");
    drop(dead); // nothing listens -> connect refused

    let (alive, _conns, _h) = spawn_counting_upstream("failover-body");
    let config = format!(
        r#"
[[listeners]]
address = "127.0.0.1:LISTEN_PORT"

[clusters.e2e]
backends = ["{dead_addr}", "{alive}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
connect_timeout_ms = 1000
"#
    );
    let proxy = spawn_proxy(config);
    let resp = http_get(proxy, "/x", "t");
    // First backend fails -> failover serves the request.
    assert!(
        resp.starts_with("HTTP/1.1 200") && resp.contains("failover-body"),
        "failover did not serve: {resp}"
    );
}

#[test]
fn upstream_timeout_yields_504() {
    // Upstream that accepts but never responds.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let silent = listener.local_addr().expect("addr");
    let _h = std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(30));
                drop(s);
            });
        }
    });

    let config = format!(
        r#"
[[listeners]]
address = "127.0.0.1:LISTEN_PORT"

[clusters.e2e]
backends = ["{silent}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
first_byte_timeout_ms = 500
connect_timeout_ms = 1000
"#
    );
    let proxy = spawn_proxy(config);

    let t0 = Instant::now();
    let mut s = TcpStream::connect(proxy).expect("connect");
    s.write_all(b"GET /slow HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .expect("write");
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = [0u8; 1024];
    let n = s.read(&mut buf).unwrap_or(0);
    let elapsed = t0.elapsed();
    let head = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(
        head.starts_with("HTTP/1.1 504"),
        "expected 504, got: {head}"
    );
    assert!(elapsed < Duration::from_secs(4), "timeout took {elapsed:?}");
}

#[test]
fn tls_termination_roundtrip() {
    // Self-signed cert for the test.
    let key = rcgen::KeyPair::generate().expect("key");
    let params = rcgen::CertificateParams::new(vec!["localhost".into()]).expect("params");
    let cert = params.self_signed(&key).expect("cert");
    let dir = tempfile::tempdir().expect("dir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, key.serialize_pem()).expect("write key");
    let (upstream, _conns, _h) = spawn_counting_upstream("tls-body");

    let config = format!(
        r#"
[[listeners]]
address = "127.0.0.1:LISTEN_PORT"
[listeners.tls]
cert = "{}"
key = "{}"

[clusters.e2e]
backends = ["{}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
"#,
        cert_path.display(),
        key_path.display(),
        upstream
    );
    let proxy = spawn_proxy(config);

    // rustls client -> vane (TLS) -> plain HTTP upstream.
    use rustls::pki_types::pem::PemObject;
    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_file_iter(&cert_path).expect("load certs") {
        root_store.add(cert.expect("cert der")).expect("add cert");
    }
    let client_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("sni");
    let mut conn =
        rustls::ClientConnection::new(Arc::new(client_cfg), server_name).expect("client conn");
    let mut sock = TcpStream::connect(proxy).expect("tcp");
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    let req = b"GET /secure HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let mut written = 0;
    while written < req.len() {
        match tls.write(&req[written..]) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("tls write: {e}"),
        }
    }
    let mut out = String::new();
    let mut buf = [0u8; 4096];
    loop {
        match tls.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => break,
        }
    }
    assert!(
        out.starts_with("HTTP/1.1 200") && out.contains("tls-body"),
        "tls roundtrip failed: {out}"
    );
}

#[test]
fn env_overrides_apply() {
    let (upstream, _c, _h) = spawn_counting_upstream("env-body");
    let config = r#"
[[listeners]]
address = "127.0.0.1:LISTEN_PORT"

[clusters.envtest]
backends = ["127.0.0.1:1"]

[[routes]]
pattern = "/*rest"
cluster = "envtest"

[admin]
enabled = false

[runtime]
force_mio = true
"#
    .to_string();
    // SAFETY: single-threaded test process env mutation (std allows; the
    // other tests do not read these keys concurrently).
    // SAFETY: tests run serially for this binary (CI); no concurrent reader.
    unsafe {
        std::env::set_var("VANE_CLUSTER_ENVTEST", upstream.to_string());
    }
    let proxy = spawn_proxy(config);
    let resp = http_get(proxy, "/via-env", "t");
    assert!(
        resp.contains("env-body"),
        "env override did not reroute: {resp}"
    );
    // SAFETY: single-threaded test env cleanup; no concurrent readers.
    unsafe {
        std::env::remove_var("VANE_CLUSTER_ENVTEST");
    }
}
