//! SPIFFE Workload API SVID source (docs/mesh-mtls-design.md,
//! milestone 3): `FetchX509SVID` over the agent's Unix socket —
//! prior-knowledge h2 + gRPC framing, hand-rolled like the ADS client
//! (no prost/tonic; see docs/xds-grpc-transport.md).
//!
//! SVIDs materialize to the mesh connector's configured PEM paths, so
//! the phase-1 file mechanism is also the rotation path for
//! workload-sourced identities: the watcher rewrites the files
//! atomically on every X509SVIDResponse and the per-dial file read
//! picks the new identity up without connection churn.

use std::io::{Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};
use vane_proto::pb;
use vane_proto::xds::{grpc_frame, grpc_unframe};

/// gRPC path of the server-streaming `FetchX509SVID` RPC.
pub const FETCH_X509_SVID_PATH: &str = "/spiffe.api.workload.SpiffeWorkloadAPI/FetchX509SVID";

/// One X.509 SVID from the Workload API.
#[derive(Debug, Clone)]
pub struct WorkloadSvid {
    /// The workload's SPIFFE ID (URIs from the cert must match).
    pub spiffe_id: String,
    /// DER-encoded X.509 certificate chain (leaf first).
    pub cert_der: Vec<u8>,
    /// PKCS#8 DER private key.
    pub key_der: Vec<u8>,
    /// DER-encoded trust bundle (root) for the trust domain.
    pub bundle_der: Vec<u8>,
}

/// A streaming `FetchX509SVID` watch over the agent's Unix socket.
pub struct WorkloadClient {
    sock: UnixStream,
    conn: Connection,
    stream: Option<u32>,
    backlog: Vec<u8>,
    eof: bool,
}

impl WorkloadClient {
    /// Connects to the agent socket (UDS + h2 preface).
    ///
    /// # Errors
    /// Connect / handshake write failures.
    pub fn connect(socket: &str, read_timeout: Duration) -> std::io::Result<Self> {
        let sock = UnixStream::connect(socket)?;
        sock.set_read_timeout(Some(read_timeout)).ok();
        let mut conn = Connection::new(Role::Client, ConnectionConfig::default());
        let preface = conn.take_pending_writes();
        let mut client = Self {
            sock,
            conn,
            stream: None,
            backlog: Vec::new(),
            eof: false,
        };
        client.sock.write_all(&preface)?;
        Ok(client)
    }

    /// Opens the `FetchX509SVID` stream: POST + one empty
    /// `X509SVIDRequest` message with END_STREAM (server-streaming —
    /// the response side stays open for pushes).
    ///
    /// # Errors
    /// Underlying socket write failure.
    pub fn fetch(&mut self) -> std::io::Result<()> {
        let id = self.conn.alloc_stream_id();
        self.stream = Some(id);
        let headers = vec![
            (b":method".to_vec(), b"POST".to_vec()),
            (b":scheme".to_vec(), b"http".to_vec()),
            (b":authority".to_vec(), b"localhost".to_vec()),
            (b":path".to_vec(), FETCH_X509_SVID_PATH.as_bytes().to_vec()),
            (b"content-type".to_vec(), b"application/grpc".to_vec()),
            (b"te".to_vec(), b"trailers".to_vec()),
        ];
        self.conn.send_headers(id, &headers, false);
        let request = grpc_frame(&[]);
        let _ = self.conn.send_data(id, &request, true);
        self.flush()
    }

    /// Reads socket bytes for up to `timeout`; returns the newest SVID
    /// from any `X509SVIDResponse` received this round (`None` = idle).
    ///
    /// # Errors
    /// Socket failures other than read timeouts.
    pub fn poll(&mut self, timeout: Duration) -> std::io::Result<Option<WorkloadSvid>> {
        self.sock.set_read_timeout(Some(timeout)).ok();
        let mut buf = [0u8; 16 * 1024];
        let n = match self.sock.read(&mut buf) {
            Ok(0) => {
                self.eof = true;
                return Ok(None);
            }
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        };
        self.backlog.extend_from_slice(&buf[..n]);
        let mut app_bytes = Vec::new();
        loop {
            let mut events = Vec::new();
            let consumed = self.conn.handle_read(&self.backlog, &mut events);
            if consumed == 0 {
                break;
            }
            self.backlog.drain(..consumed);
            for ev in events {
                if let Event::Data { data, .. } = ev {
                    app_bytes.extend_from_slice(&data);
                }
            }
            // SETTINGS/WINDOW_UPDATE ACKs must go out or the agent
            // stalls.
            let acks = self.conn.take_pending_writes();
            if !acks.is_empty() {
                self.sock.write_all(&acks)?;
            }
        }
        let (frames, _) = grpc_unframe(&app_bytes);
        // The newest response supersedes earlier ones.
        let mut newest = None;
        for (_, msg) in frames {
            if let Some(svid) = decode_svid_response(&msg) {
                newest = Some(svid);
            }
        }
        Ok(newest)
    }

    /// The connection hit EOF or an h2 error (watcher must reconnect).
    #[must_use]
    pub fn dead(&self) -> bool {
        self.eof || self.conn.connection_error().is_some()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let out = self.conn.take_pending_writes();
        if !out.is_empty() {
            self.sock.write_all(&out)?;
        }
        Ok(())
    }
}

/// Connects, issues `FetchX509SVID`, and blocks until the first SVID
/// arrives (bounded by `deadline`). Retries while the agent socket
/// isn't up yet.
///
/// # Errors
/// Connect/write failures, agent closure, or deadline expiry.
pub fn fetch_first(socket: &str, deadline: std::time::Instant) -> Result<WorkloadSvid, String> {
    let mut last_err = String::from("deadline expired");
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(format!("workload api {socket}: {last_err}"));
        }
        let mut client = match WorkloadClient::connect(socket, Duration::from_millis(500)) {
            Ok(c) => c,
            Err(e) => {
                last_err = e.to_string();
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        if let Err(e) = client.fetch() {
            return Err(format!("workload api {socket}: fetch: {e}"));
        }
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(format!("workload api {socket}: no SVID within deadline"));
            }
            let svid = client
                .poll(Duration::from_millis(250))
                .map_err(|e| e.to_string())?;
            if let Some(svid) = svid {
                return Ok(svid);
            }
            if client.dead() {
                last_err = "connection closed before SVID".to_owned();
                break;
            }
        }
    }
}

/// Materializes the SVID to the connector's PEM paths (atomic: write
/// `.tmp` + rename; the key gets 0600). The bundle is optional — the
/// CA path is left untouched when the agent sends none.
///
/// # Errors
/// Filesystem failures.
pub fn materialize(svid: &WorkloadSvid, cert: &Path, key: &Path, ca: &Path) -> Result<(), String> {
    write_atomic(cert, &pem("CERTIFICATE", &svid.cert_der), 0o644)?;
    write_atomic(key, &pem("PRIVATE KEY", &svid.key_der), 0o600)?;
    if !svid.bundle_der.is_empty() {
        write_atomic(ca, &pem("CERTIFICATE", &svid.bundle_der), 0o644)?;
    }
    Ok(())
}

/// Watches the Workload API forever, materializing every SVID update.
/// Reconnects with a short backoff while the agent restarts. Never
/// returns under normal operation; errors are logged, not fatal — the
/// last materialized identity stays valid until it expires.
pub fn watch(socket: &str, cert: &Path, key: &Path, ca: &Path) {
    loop {
        match run_watch(socket, cert, key, ca) {
            Ok(()) => {}
            Err(e) => tracing::warn!("mesh svid watcher: {e}; retrying in 1s"),
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_watch(socket: &str, cert: &Path, key: &Path, ca: &Path) -> Result<(), String> {
    let mut client =
        WorkloadClient::connect(socket, Duration::from_secs(5)).map_err(|e| e.to_string())?;
    client.fetch().map_err(|e| e.to_string())?;
    loop {
        match client
            .poll(Duration::from_secs(5))
            .map_err(|e| e.to_string())?
        {
            Some(svid) => {
                materialize(&svid, cert, key, ca)?;
                tracing::info!(
                    "mesh svid watcher: materialized {} ({})",
                    svid.spiffe_id,
                    cert.display()
                );
            }
            None if client.dead() => {
                return Err(format!("workload api {socket}: connection closed"));
            }
            None => {}
        }
    }
}

/// DER → PEM.
fn pem(tag: &str, der: &[u8]) -> String {
    pem_rfc7468::encode_string(tag, pem_rfc7468::LineEnding::LF, der).expect("pem encode")
}

fn write_atomic(path: &Path, data: &str, mode: u32) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let tmp = path.with_extension("vane-tmp");
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(mode)
            .open(&tmp)
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
        f.write_all(data.as_bytes())
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
}

/// `X509SVIDResponse`: repeated `X509SVID svids = 1`; the last entry
/// wins (agents send one per trust domain in practice).
fn decode_svid_response(msg: &[u8]) -> Option<WorkloadSvid> {
    let mut last = None;
    let mut pos = 0;
    while pos < msg.len() {
        let (field, n) = pb::decode_field(&msg[pos..])?;
        pos += n;
        if field.number == 1 {
            last = decode_svid(field.bytes);
        }
    }
    last
}

/// `X509SVID`: `spiffe_id = 1` (string), `x509_svid = 2` (DER bytes),
/// `x509_svid_key = 3` (PKCS#8 DER), `bundle = 4` (DER).
fn decode_svid(msg: &[u8]) -> Option<WorkloadSvid> {
    let mut spiffe_id = String::new();
    let mut cert_der = Vec::new();
    let mut key_der = Vec::new();
    let mut bundle_der = Vec::new();
    let mut pos = 0;
    while pos < msg.len() {
        let (field, n) = pb::decode_field(&msg[pos..])?;
        pos += n;
        match field.number {
            1 => spiffe_id = String::from_utf8_lossy(field.bytes).into_owned(),
            2 => cert_der = field.bytes.to_vec(),
            3 => key_der = field.bytes.to_vec(),
            4 => bundle_der = field.bytes.to_vec(),
            _ => {}
        }
    }
    if cert_der.is_empty() || key_der.is_empty() {
        return None;
    }
    Some(WorkloadSvid {
        spiffe_id,
        cert_der,
        key_der,
        bundle_der,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wire-shape decode: one SVID with all four fields.
    #[test]
    fn decodes_svid_response() {
        let mut svid_msg = Vec::new();
        pb::string_field(&mut svid_msg, 1, "spiffe://example.org/vane/proxy");
        pb::bytes_field(&mut svid_msg, 2, b"cert-der-bytes");
        pb::bytes_field(&mut svid_msg, 3, b"pkcs8-der-bytes");
        pb::bytes_field(&mut svid_msg, 4, b"bundle-der-bytes");
        let mut resp = Vec::new();
        pb::message_field(&mut resp, 1, &svid_msg);

        let decoded = decode_svid_response(&resp).expect("svid");
        assert_eq!(decoded.spiffe_id, "spiffe://example.org/vane/proxy");
        assert_eq!(decoded.cert_der, b"cert-der-bytes");
        assert_eq!(decoded.key_der, b"pkcs8-der-bytes");
        assert_eq!(decoded.bundle_der, b"bundle-der-bytes");
    }

    /// A SVID without cert/key is not usable.
    #[test]
    fn rejects_incomplete_svid() {
        let mut svid_msg = Vec::new();
        pb::string_field(&mut svid_msg, 1, "spiffe://example.org/vane/proxy");
        let mut resp = Vec::new();
        pb::message_field(&mut resp, 1, &svid_msg);
        assert!(decode_svid_response(&resp).is_none());
    }

    /// Materialize writes PEM with the right tags; the key file is
    /// 0600.
    #[test]
    fn materializes_pem_files() {
        let dir = tempfile::tempdir().expect("dir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("nested").join("ca.pem");
        let svid = WorkloadSvid {
            spiffe_id: "spiffe://example.org/vane/proxy".into(),
            cert_der: vec![0x30, 0x01, 0x02],
            key_der: vec![0x30, 0x02, 0x03],
            bundle_der: vec![0x30, 0x03, 0x04],
        };
        materialize(&svid, &cert, &key, &ca).expect("materialize");
        let cert_pem = std::fs::read_to_string(&cert).expect("cert");
        assert!(cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
        let key_pem = std::fs::read_to_string(&key).expect("key");
        assert!(key_pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&key).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "key mode");
        let ca_pem = std::fs::read_to_string(&ca).expect("ca");
        assert!(ca_pem.contains("-----BEGIN CERTIFICATE-----"));
        // No leftover temp files.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("dir")
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".vane-tmp"))
            .collect();
        assert!(leftovers.is_empty(), "leftover tmp files: {leftovers:?}");
    }
}
