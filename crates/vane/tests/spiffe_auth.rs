//! Per-route SPIFFE caller authorization (docs/mesh-mtls-design.md,
//! milestone 4): an mTLS listener (client_ca set) requires downstream
//! client certificates; routes with `allowed_spiffe_prefixes` answer
//! 403 to callers whose SPIFFE URI SAN matches no prefix, and accept
//! callers within an allowed prefix. Handshakes without a client
//! certificate fail at TLS (required client auth).

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener as StdListener};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::CertificateDer;

struct CallerIdentity {
    cert_pem: String,
    key_pem: String,
}

struct Material {
    ca_pem: String,
    server_cert_path: String,
    server_key_path: String,
    ca_path: String,
    allowed: CallerIdentity,
    denied: CallerIdentity,
}

/// Listener cert (DNS localhost), mesh CA, and two caller SVIDs: one
/// under the allowed prefix, one outside it — all chained to the CA.
fn gen_materials(dir: &std::path::Path) -> Material {
    use rcgen::{CertificateParams, KeyPair, SanType};
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(vec!["mesh-ca".into()]).expect("params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let mut srv_params = CertificateParams::new(vec!["localhost".into()]).expect("params");
    srv_params.subject_alt_names = vec![SanType::DnsName(
        rcgen::string::Ia5String::try_from("localhost").expect("dns"),
    )];
    let srv_key = KeyPair::generate().expect("srv key");
    let srv = srv_params.signed_by(&srv_key, &issuer).expect("srv");

    let gen_caller = |spiffe: &str| {
        let mut params = CertificateParams::new(vec!["caller".into()]).expect("params");
        params.subject_alt_names = vec![SanType::URI(
            rcgen::string::Ia5String::try_from(spiffe).expect("uri"),
        )];
        let key = KeyPair::generate().expect("key");
        let cert = params.signed_by(&key, &issuer).expect("cert");
        CallerIdentity {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        }
    };

    let write = |name: &str, data: &str| {
        let p = dir.join(name);
        std::fs::write(&p, data).expect("write");
        p.display().to_string()
    };

    Material {
        ca_pem: ca.pem(),
        server_cert_path: write("srv.pem", &srv.pem()),
        server_key_path: write("srv-key.pem", &srv_key.serialize_pem()),
        ca_path: write("ca.pem", &ca.pem()),
        allowed: gen_caller("spiffe://example.org/vane/caller-a"),
        denied: gen_caller("spiffe://example.org/other/caller-b"),
    }
}

fn spawn_upstream() -> SocketAddr {
    let listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            std::thread::spawn(move || {
                let _ = s.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                );
                let _ = s.shutdown(std::net::Shutdown::Both);
            });
        }
    });
    addr
}

/// PEM (single CERTIFICATE block) → DER.
fn pem_to_der(pem: &str) -> Vec<u8> {
    let mut buf = vec![0u8; pem.len()];
    let (_, der) = pem_rfc7468::decode(pem.as_bytes(), &mut buf).expect("pem decode");
    der.to_vec()
}

/// One HTTPS/1.1 request over rustls with (optional) client auth.
/// Handshake or write failures surface as Err (the no-cert case).
fn tls_request(
    addr: SocketAddr,
    server_ca_pem: &str,
    client: Option<&CallerIdentity>,
) -> Result<String, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(pem_to_der(server_ca_pem)))
        .map_err(|e| format!("root add: {e}"))?;

    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    let cfg = match client {
        Some(id) => {
            let cert = CertificateDer::from(pem_to_der(&id.cert_pem));
            let key =
                rustls_pemfile::private_key(&mut std::io::BufReader::new(id.key_pem.as_bytes()))
                    .map_err(|e| format!("key pem: {e}"))?
                    .ok_or_else(|| "no key".to_string())?;
            builder
                .with_client_auth_cert(vec![cert], key)
                .map_err(|e| format!("client auth: {e}"))?
        }
        None => builder.with_no_client_auth(),
    };

    let server =
        rustls::pki_types::ServerName::try_from("localhost".to_string()).expect("server name");
    let mut conn = rustls::ClientConnection::new(Arc::new(cfg), server)
        .map_err(|e| format!("client conn: {e}"))?;
    let mut sock = std::net::TcpStream::connect(addr).map_err(|e| format!("tcp: {e}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("timeout: {e}"))?;
    sock.set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("timeout: {e}"))?;
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);

    tls.write_all(b"GET /data HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
        .map_err(|e| format!("write: {e}"))?;
    let mut out = Vec::new();
    match tls.read_to_end(&mut out) {
        Ok(_) => Ok(String::from_utf8_lossy(&out).into_owned()),
        // EOF with some bytes read is fine (close_notify optional).
        Err(_) if !out.is_empty() => Ok(String::from_utf8_lossy(&out).into_owned()),
        Err(e) => Err(format!("read: {e}")),
    }
}

fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd as _;
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    file
}

#[test]
fn spiffe_prefix_authorizes_callers() {
    let _serial = lock_serial();
    let dir = tempfile::tempdir().expect("dir");
    let m = gen_materials(dir.path());
    let upstream = spawn_upstream();

    let proxy_listener = StdListener::bind("127.0.0.1:0").expect("bind");
    let proxy: SocketAddr = proxy_listener.local_addr().expect("addr");
    drop(proxy_listener);

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
cert = "{srv_cert}"
key = "{srv_key}"
client_ca = "{ca}"

[clusters.up]
backends = ["{upstream}"]

[[routes]]
pattern = "/*rest"
cluster = "up"
allowed_spiffe_prefixes = ["spiffe://example.org/vane/"]

[admin]
enabled = false

[runtime]
force_mio = true
workers = 1
"#,
                proxy = proxy.port(),
                srv_cert = m.server_cert_path,
                srv_key = m.server_key_path,
                ca = m.ca_path,
                upstream = upstream,
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
        std::thread::sleep(Duration::from_millis(50));
    }

    // Caller inside the allowed prefix: relayed.
    let resp = tls_request(proxy, &m.ca_pem, Some(&m.allowed)).expect("allowed caller handshake");
    assert!(resp.contains("200 OK"), "allowed caller: {resp}");

    // Caller outside every allowed prefix: 403.
    let resp = tls_request(proxy, &m.ca_pem, Some(&m.denied)).expect("denied caller handshake");
    assert!(resp.contains("403"), "denied caller: {resp}");

    // No client certificate: the TLS handshake itself must fail
    // (required client auth).
    let resp = tls_request(proxy, &m.ca_pem, None);
    assert!(
        resp.is_err() || !resp.expect("resp").contains("200 OK"),
        "cert-less caller must not be relayed"
    );
}
