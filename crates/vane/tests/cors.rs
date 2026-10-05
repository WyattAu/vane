//! Per-route CORS at the edge: preflight answered without dialing the
//! upstream, response headers injected on actual requests, and no
//! leakage between transactions on a keep-alive connection.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use vane::server::RunOptions;

/// Requests the shared stub upstream has answered. A preflight answered
/// at the edge must not move this counter.
static UPSTREAM_HITS: AtomicUsize = AtomicUsize::new(0);

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-cors.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("open lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(rc, 0, "flock");
    file
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn temp_config(toml: String) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("vane.toml");
    std::fs::write(&path, toml).expect("write");
    let p = path.to_str().expect("utf8").to_owned();
    (dir, p)
}

/// Stub upstream: 200 with `Vary: Accept-Encoding` (so the head-merge
/// path is always exercised) and a fresh hit counter.
fn spawn_upstream() -> std::net::SocketAddr {
    UPSTREAM_HITS.store(0, Ordering::SeqCst);
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
                        Ok(n) => {
                            if n == 0 {
                                break;
                            }
                            UPSTREAM_HITS.fetch_add(1, Ordering::SeqCst);
                            let is_options = buf[..n].starts_with(b"OPTIONS");
                            let body: &[u8] = if is_options {
                                b"upstream-options"
                            } else {
                                b"ok"
                            };
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nVary: Accept-Encoding\r\nConnection: keep-alive\r\n\r\n",
                                body.len()
                            )
                            .into_bytes();
                            if s.write_all(&resp).is_err() || s.write_all(body).is_err() {
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

struct ServerGuard {
    tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

fn spawn_proxy(cfg_path: String) -> ServerGuard {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(RunOptions {
            config_path: Some(cfg_path),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            shutdown: Some(rx),
            force_mio: true,
        }));
        assert_eq!(code, 0);
    });
    ServerGuard { tx: Some(tx) }
}

fn connect(proxy: std::net::SocketAddr) -> std::net::TcpStream {
    let mut s = None;
    for _ in 0..40 {
        match std::net::TcpStream::connect(proxy) {
            Ok(s_) => {
                s = Some(s_);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    s.expect("connect after retries")
}

fn request(proxy: std::net::SocketAddr, req: &[u8]) -> String {
    let mut s = connect(proxy);
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.write_all(req).expect("write");
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    out
}

fn wait_bound(proxy: std::net::SocketAddr) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(proxy).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy never bound {proxy}");
}

/// Starts a proxy over one cluster/route pair, returning its address.
fn start(
    upstream: std::net::SocketAddr,
    cors_block: &str,
    methods: &str,
) -> (ServerGuard, tempfile::TempDir, std::net::SocketAddr) {
    let port = free_port();
    let cfg = format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/api/*rest"
cluster = "up"
methods = [{methods}]

{cors_block}

[runtime]
force_mio = true
workers = 1
"#
    );
    let (dir, cfg_path) = temp_config(cfg);
    let guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);
    (guard, dir, proxy)
}

const CORS_EXACT: &str = r#"
[routes.cors]
allow_origins = ["https://app.example"]
allow_methods = ["GET", "POST"]
allow_headers = ["content-type"]
expose_headers = ["X-Total"]
allow_credentials = true
max_age_secs = 600
"#;

#[test]
fn preflight_is_answered_at_the_edge_without_dialing_upstream() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let before = UPSTREAM_HITS.load(Ordering::SeqCst);
    let resp = request(
        proxy,
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\n\
          Access-Control-Request-Method: POST\r\n\
          Access-Control-Request-Headers: content-type\r\n\
          Connection: close\r\n\r\n",
    );

    assert!(resp.contains("204 No Content"), "status: {resp:?}");
    assert!(
        resp.contains("Access-Control-Allow-Origin: https://app.example\r\n"),
        "{resp:?}"
    );
    assert!(
        resp.contains("Access-Control-Allow-Methods: GET, POST\r\n"),
        "{resp:?}"
    );
    assert!(
        resp.contains("Access-Control-Allow-Headers: content-type\r\n"),
        "{resp:?}"
    );
    assert!(
        resp.contains("Access-Control-Allow-Credentials: true\r\n"),
        "{resp:?}"
    );
    assert!(resp.contains("Access-Control-Max-Age: 600\r\n"), "{resp:?}");
    // RFC 9110: no Content-Length on a 204.
    assert!(!resp.contains("Content-Length"), "{resp:?}");
    // Vary names the preflight's request headers, not just the Origin.
    assert!(resp.contains("Access-Control-Request-Method"), "{resp:?}");
    // The upstream was never contacted.
    assert_eq!(
        UPSTREAM_HITS.load(Ordering::SeqCst),
        before,
        "preflight reached the upstream"
    );
}

#[test]
fn actual_request_gets_cors_headers_and_is_proxied() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let before = UPSTREAM_HITS.load(Ordering::SeqCst);
    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "{resp:?}");
    assert!(resp.ends_with("ok"), "body relayed: {resp:?}");
    assert!(
        resp.contains("Access-Control-Allow-Origin: https://app.example\r\n"),
        "{resp:?}"
    );
    assert!(
        resp.contains("Access-Control-Expose-Headers: X-Total\r\n"),
        "{resp:?}"
    );
    // The upstream's own Vary is merged, not duplicated: caches must key
    // on Accept-Encoding AND Origin.
    assert_eq!(resp.matches("Vary:").count(), 1, "duplicate Vary: {resp:?}");
    assert!(
        resp.contains("Vary: Accept-Encoding, Origin\r\n"),
        "{resp:?}"
    );
    assert_eq!(
        UPSTREAM_HITS.load(Ordering::SeqCst),
        before + 1,
        "not proxied"
    );
}

#[test]
fn disallowed_origin_is_relayed_untouched() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let before = UPSTREAM_HITS.load(Ordering::SeqCst);
    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://evil.example\r\nConnection: close\r\n\r\n",
    );
    // Proxied normally, but with no Access-Control-* header: the browser
    // is the enforcement point.
    assert!(resp.contains("200 OK"), "{resp:?}");
    assert!(!resp.contains("Access-Control"), "leaked headers: {resp:?}");
    assert_eq!(resp.matches("Vary:").count(), 1, "{resp:?}");
    assert!(resp.contains("Vary: Accept-Encoding\r\n"), "{resp:?}");
    assert_eq!(
        UPSTREAM_HITS.load(Ordering::SeqCst),
        before + 1,
        "not proxied"
    );
}

#[test]
fn request_without_origin_gets_no_cors_headers() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("200 OK"), "{resp:?}");
    assert!(!resp.contains("Access-Control"), "{resp:?}");
}

#[test]
fn preflight_for_a_method_outside_the_policy_is_relayed() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let before = UPSTREAM_HITS.load(Ordering::SeqCst);
    let resp = request(
        proxy,
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\n\
          Access-Control-Request-Method: DELETE\r\n\
          Connection: close\r\n\r\n",
    );
    // Not answered at the edge: it falls through to the route, where
    // DELETE is outside the CORS method allowlist, so no allow-origin is
    // advertised and the browser blocks.
    assert_eq!(
        UPSTREAM_HITS.load(Ordering::SeqCst),
        before + 1,
        "not relayed"
    );
    assert!(!resp.contains("Access-Control-Allow-Origin"), "{resp:?}");
}

#[test]
fn preflight_for_a_disallowed_header_is_relayed() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let resp = request(
        proxy,
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\n\
          Access-Control-Request-Method: POST\r\n\
          Access-Control-Request-Headers: content-type, x-secret\r\n\
          Connection: close\r\n\r\n",
    );
    assert!(!resp.contains("Access-Control-Allow-Origin"), "{resp:?}");
}

#[test]
fn wildcard_policy_echoes_origin_when_credentials_are_allowed() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let wildcard = r#"
[routes.cors]
allow_origins = ["*"]
allow_credentials = true
"#;
    let (_g, _dir, proxy) = start(upstream, wildcard, r#""GET""#);

    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://anything.example\r\nConnection: close\r\n\r\n",
    );
    // `*` is invalid with credentials, so the origin must be named.
    assert!(
        resp.contains("Access-Control-Allow-Origin: https://anything.example\r\n"),
        "{resp:?}"
    );
    assert!(
        resp.contains("Access-Control-Allow-Credentials: true\r\n"),
        "{resp:?}"
    );
}

#[test]
fn wildcard_without_credentials_emits_the_wildcard() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let wildcard = r#"
[routes.cors]
allow_origins = ["*"]
"#;
    let (_g, _dir, proxy) = start(upstream, wildcard, r#""GET""#);

    let resp = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://anything.example\r\nConnection: close\r\n\r\n",
    );
    assert!(
        resp.contains("Access-Control-Allow-Origin: *\r\n"),
        "{resp:?}"
    );
    assert!(
        !resp.contains("Access-Control-Allow-Credentials"),
        "{resp:?}"
    );
}

/// A preflight and a follow-up request on one keep-alive connection: the
/// second transaction must not inherit the first's CORS state, and the
/// edge must not close the connection for the preflight.
#[test]
fn cors_state_does_not_leak_between_keep_alive_transactions() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST", "OPTIONS""#);

    let mut s = connect(proxy);
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();

    // Transaction 1: preflight, answered at the edge.
    s.write_all(
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\n\
          Access-Control-Request-Method: GET\r\n\r\n",
    )
    .expect("write preflight");
    let mut buf = [0u8; 8192];
    let n = s.read(&mut buf).expect("read preflight");
    let first = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(first.contains("204 No Content"), "{first:?}");
    assert!(
        !first.contains("Connection: close"),
        "preflight closed: {first:?}"
    );

    // Transaction 2: same connection, NO Origin. The CORS headers from
    // the preflight must not reappear.
    s.write_all(b"GET /api/items HTTP/1.1\r\nHost: t\r\n\r\n")
        .expect("write second");
    let n = s.read(&mut buf).expect("read second");
    let second = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(second.contains("200 OK"), "{second:?}");
    assert!(
        !second.contains("Access-Control"),
        "cors state leaked into the next transaction: {second:?}"
    );
}

/// A preflight is exempt from the route's method allowlist: a browser
/// will not send the real request if the preflight fails, so an
/// allowlist that omitted OPTIONS would break every cross-origin call.
#[test]
fn preflight_bypasses_the_route_method_allowlist() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let resp = request(
        proxy,
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\n\
          Origin: https://app.example\r\n\
          Access-Control-Request-Method: GET\r\n\
          Connection: close\r\n\r\n",
    );
    assert!(resp.contains("204 No Content"), "not exempt: {resp:?}");
    assert_eq!(UPSTREAM_HITS.load(Ordering::SeqCst), 0, "dialed upstream");
}

/// OPTIONS without `Access-Control-Request-Method` is an ordinary
/// request, so it is subject to the method allowlist like any other.
#[test]
fn plain_options_still_obeys_the_method_allowlist() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST""#);

    let resp = request(
        proxy,
        b"OPTIONS /api/items HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(resp.contains("405"), "allowed through: {resp:?}");
    assert_eq!(UPSTREAM_HITS.load(Ordering::SeqCst), 0, "dialed upstream");
}

/// The policy is per-route: a second route with no CORS block must serve
/// requests without any CORS headers even on the same connection.
#[test]
fn policy_is_scoped_to_its_route() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let port = free_port();
    let cors_block = CORS_EXACT.to_owned();
    let (_dir, cfg_path) = temp_config(format!(
        r#"
[[listeners]]
address = "127.0.0.1:{port}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/api/*rest"
cluster = "up"
{cors_block}

[[routes]]
pattern = "/*rest"
cluster = "up"

[runtime]
force_mio = true
workers = 1
"#
    ));
    let _guard = spawn_proxy(cfg_path);
    let proxy: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    wait_bound(proxy);

    let with = request(
        proxy,
        b"GET /api/items HTTP/1.1\r\nHost: t\r\nOrigin: https://app.example\r\nConnection: close\r\n\r\n",
    );
    assert!(with.contains("Access-Control-Allow-Origin"), "{with:?}");

    let without = request(
        proxy,
        b"GET /other HTTP/1.1\r\nHost: t\r\nOrigin: https://app.example\r\nConnection: close\r\n\r\n",
    );
    assert!(without.contains("200 OK"), "{without:?}");
    assert!(
        !without.contains("Access-Control"),
        "leaked across routes: {without:?}"
    );
}
/// The mechanism behind every edge-answered response: intake appends the
/// request head to the connection's parse buffer and only the upstream
/// path drains it. An edge response that leaves it in place makes the
/// NEXT keep-alive transaction re-parse the stale head and replay the
/// first response. Locked here through the preflight (the deterministic
/// edge-answered path).
#[test]
fn edge_response_does_not_leave_a_stale_head_for_the_next_transaction() {
    let _serial = lock_serial();
    let upstream = spawn_upstream();
    let (_g, _dir, proxy) = start(upstream, CORS_EXACT, r#""GET", "POST", "OPTIONS""#);

    let mut s = connect(proxy);
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = [0u8; 8192];

    // Transaction 1: answered at the edge.
    s.write_all(b"OPTIONS /api/first HTTP/1.1\r\nHost: t\r\nOrigin: https://app.example\r\nAccess-Control-Request-Method: GET\r\n\r\n")
        .expect("write 1");
    let n = s.read(&mut buf).expect("read 1");
    let first = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(first.contains("204 No Content"), "{first:?}");
    // The distinct path proves the head reached the parser.
    assert!(
        first.contains("Access-Control-Allow-Methods: GET, POST"),
        "{first:?}"
    );

    // Transaction 2 on the same connection: a different, normal request.
    s.write_all(b"GET /api/second HTTP/1.1\r\nHost: t\r\n\r\n")
        .expect("write 2");
    let n = s.read(&mut buf).expect("read 2");
    let second = String::from_utf8_lossy(&buf[..n]).into_owned();
    // A replayed head would answer with the transaction-1 preflight
    // (204 + CORS headers) instead of the proxied 200.
    assert!(second.contains("200 OK"), "replayed stale head: {second:?}");
    assert!(!second.contains("204 No Content"), "replayed: {second:?}");
    assert!(!second.contains("Access-Control"), "replayed: {second:?}");
}
