//! Full-stack ACME e2e: pebble issues a certificate for vane itself
//! (vane serves its own HTTP-01 answers), then a second vane instance
//! installs the issued material and serves HTTPS through the proxy.
//!
//! Requires docker (pebble + host networking). Gated behind
//! `acme-pebble` + serial lock.

#![cfg(feature = "acme-pebble")]

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires docker (pebble container)"]
async fn acme_full_stack_issue_and_serve() {
    let _lock = lock_serial();
    // Kill leaked vane children from prior failed runs — they squat the
    // ACME ports and break fresh runs.
    let _ = Command::new("pkill")
        .args(["-9", "-f", "target/debug/vane"])
        .output();

    // Reserve ephemeral ports up-front (bind-then-drop) so concurrent
    // runs / foreign services on this shared host can't collide.
    let reserve = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        l.local_addr().expect("addr").port()
    };
    let mgmt_port = reserve(); // pebble ACME directory (default 14000)
    let http_port = reserve(); // pebble dials here for HTTP-01 validation
    let api_port = reserve(); // pebble management endpoint (default 15000)

    // Pebble config: custom directory port + HTTP-01 validation port,
    // plus the management endpoint the test fetches the root CA from.
    // Cert/key paths are the stock test certs shipped inside the image.
    let pebble_cfg = format!(
        r#"{{"pebble":{{"listenAddress":"0.0.0.0:{mgmt_port}","managementListenAddress":"0.0.0.0:{api_port}","certificate":"test/certs/localhost/cert.pem","privateKey":"test/certs/localhost/key.pem","httpPort":{http_port}}}}}"#
    );
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let cfg_path = cfg_dir.path().join("pebble-config.json");
    std::fs::write(&cfg_path, pebble_cfg).expect("write pebble config");
    let cfg_mount = format!("{}:/test/pebble-config.json", cfg_path.display());

    let container = format!("vane-pebble-stack-{}", std::process::id());
    let pebble = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container,
            "--network",
            "host",
            "-v",
            &cfg_mount,
            "ghcr.io/letsencrypt/pebble:latest",
            "-config",
            "/test/pebble-config.json",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start pebble");
    // pebble needs a moment to listen on the directory port.
    let dir_url = format!("https://127.0.0.1:{mgmt_port}/dir");
    for _ in 0..20 {
        if let Ok(out) = Command::new("curl")
            .args([
                "-sk",
                "--max-time",
                "2",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                &dir_url,
            ])
            .output()
        {
            let code = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if code == "200" {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let _ = pebble; // left running; removed at test end

    // Upstream: keep-alive 200 responder.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let upstream = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let _ = s.write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nhello-vane",
                            );
                        }
                    }
                }
            });
        }
    });

    let dir = tempfile::tempdir().expect("dir");
    let acme_storage = dir.path().join("acme");
    let proxy_addr: SocketAddr = format!("127.0.0.1:{http_port}").parse().expect("addr");
    let tls_probe = TcpListener::bind("127.0.0.1:0").expect("probe tls");
    let tls_addr = tls_probe.local_addr().expect("tls addr");
    let tls_port = tls_addr.port();
    drop(tls_probe);

    // ---- Phase 1: vane with [acme]; HTTP-01 served on the reserved port ----
    let storage_display = acme_storage.display().to_string();
    let phase1 = format!(
        r#"
[[listeners]]
address = "127.0.0.1:{http_port}"

[[listeners]]
address = "[::1]:{http_port}"

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[acme]
directory_url = "https://127.0.0.1:{mgmt_port}/dir"
emails = ["ci@example.com"]
storage_dir = "{storage_display}"
insecure_tls = true

[[acme.domains]]
domain = "localhost"

[admin]
enabled = false

[runtime]
force_mio = true
"#
    );
    let p1 = dir.path().join("phase1.toml");
    std::fs::write(&p1, phase1).expect("write phase1");
    let p1s = p1.display().to_string();

    // Run phase-1 vane as a real child process (true two-process topology).
    let bin = env!("CARGO_BIN_EXE_vane");
    std::fs::create_dir_all("/tmp/opencode/acme-e2e").ok();
    let err_log = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("/tmp/opencode/acme-e2e/phase1.stderr")
        .expect("open stderr file");
    let mut server1 = Command::new(bin)
        .args(["run", "-c", &p1s])
        .stdout(Stdio::null())
        .stderr(err_log)
        .spawn()
        .expect("spawn phase-1 vane");

    // Readiness: wait up to 15s for the ACME listener.
    let mut ready = false;
    for _ in 0..150 {
        if TcpStream::connect(proxy_addr).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(ready, "phase-1 vane never bound {proxy_addr}");

    // ACME responder sanity: unknown token must be answered by vane (404),
    // proving the HTTP-01 hook is live before pebble starts validating.
    {
        let mut s = TcpStream::connect(proxy_addr).expect("connect for acme probe");
        s.write_all(
            b"GET /.well-known/acme-challenge/probe HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
        )
        .expect("write probe");
        // The server may RST after a fast close; capture what arrives.
        let mut out = String::new();
        let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
        let _ = s.read_to_string(&mut out);
        assert!(
            out.contains("HTTP/1.1 404") && out.contains("unknown token"),
            "acme responder probe failed: {out:?}"
        );
    }

    // Wait for the certificate to be issued and installed.
    let cert_path = acme_storage.join("cert.pem");
    let mut issued = false;
    for _ in 0..120 {
        if let Ok(text) = std::fs::read_to_string(&cert_path) {
            if text.contains("BEGIN CERTIFICATE") {
                issued = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let key = std::fs::read_to_string(acme_storage.join("privkey.pem")).unwrap_or_default();
    // Stop phase-1 vane (SIGKILL; the port frees via SO_REUSEADDR on rebind).
    // NOTE: we cannot signal the inner thread directly; the test process
    // owns it via the detached thread — instead we kill by port owner.
    // Simplest: the server observes no shutdown and keeps running; phase 2
    // binds a DIFFERENT port so no conflict.
    assert!(issued, "certificate was not issued within 60s");
    assert!(
        key.contains("BEGIN") || !key.is_empty(),
        "privkey.pem missing"
    );

    // ---- Phase 2: TLS listener with the issued material ----
    let key_path_display = acme_storage.join("privkey.pem").display().to_string();
    let cert_path_display = cert_path.display().to_string();
    let phase2 = format!(
        r#"
[[listeners]]
address = "127.0.0.1:{tls_port}"
[listeners.tls]
cert = "{cert_path_display}"
key = "{key_path_display}"

[clusters.e2e]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "e2e"

[admin]
enabled = false

[runtime]
force_mio = true
"#
    );
    let p2 = dir.path().join("phase2.toml");
    std::fs::write(&p2, phase2).expect("write phase2");
    let p2s = p2.display().to_string();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let code = rt.block_on(vane::server::run(vane::server::RunOptions {
            config_path: Some(p2s),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
        }));
        let _ = code;
    });
    for _ in 0..40 {
        if TcpStream::connect(tls_addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    server1.kill().expect("kill phase-1 vane");
    let _ = server1.wait();

    // ---- Serve HTTPS through the proxy using the issued certificate ----
    // Fetch pebble's root CA from its management endpoint.
    let root_url = format!("https://127.0.0.1:{api_port}/roots/0");
    let root_pem: Vec<u8> = std::process::Command::new("curl")
        .args(["-sk", "--max-time", "3", &root_url])
        .output()
        .expect("root fetch")
        .stdout;
    assert!(
        root_pem.starts_with(b"-----BEGIN CERTIFICATE-----"),
        "pebble root fetch returned no PEM: {:?}",
        String::from_utf8_lossy(&root_pem[..60.min(root_pem.len())])
    );
    let mut roots = rustls::RootCertStore::empty();
    // Parse each PEM section of the chain.
    let mut rest = &root_pem[..];
    while let Some(pos) = rest
        .windows(25)
        .position(|w| w == b"-----END CERTIFICATE-----")
    {
        let end = pos + 25;
        let block = &rest[..end];
        use rustls::pki_types::pem::PemObject as _;
        if let Ok(der) = rustls::pki_types::CertificateDer::from_pem_slice(block) {
            roots.add(der).expect("add root cert");
        }
        rest = &rest[end..];
    }
    let client_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("sni");
    let mut conn =
        rustls::ClientConnection::new(Arc::new(client_cfg), server_name).expect("client");
    let mut sock = TcpStream::connect(tls_addr).expect("tls tcp");
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(b"GET /acme-served HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("tls write");
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
        out.starts_with("HTTP/1.1 200") && out.contains("hello-vane"),
        "https roundtrip failed: {out:?}"
    );

    // Cleanup: remove the pebble container.
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .output();
}

/// Blocking cross-process test lock (flock on a temp file).
fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
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
