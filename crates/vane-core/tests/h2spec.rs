//! External conformance: runs summerwind/h2spec's `generic` suite
//! against a raw-TCP h2c server driven by the sans-io
//! [`Connection`](vane_core::h2::connection::Connection) state machine.
//!
//! The harness models a real driver: it buffers partial frames (using
//! the consumed-bytes contract of `handle_read`), responds only once a
//! request is fully received (END_STREAM), and retries pending response
//! bodies when the peer's WINDOW_UPDATE grants more credit.
//!
//! Skipped when the `h2spec` binary is not available (CI downloads it;
//! locally: /tmp/opencode/h2spec/h2spec). The identical server logic
//! lives in `examples/h2spec_server.rs` for manual runs.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::Command;

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};

/// Pseudo-header / connection-header validation (RFC 9113 §8.3): the
/// harness rejects malformed requests with 400, as h2spec's 8.1.2.x
/// tests require.
fn request_is_malformed(headers: &[vane_core::h2::hpack::Header]) -> bool {
    let (mut method, mut path, mut scheme) = (false, false, false);
    for header in headers {
        let n = header.name.to_ascii_lowercase();
        if n.starts_with(b":") {
            match n.as_slice() {
                b":method" => method = true,
                b":path" => path = true,
                b":scheme" => scheme = true,
                _ => {}
            }
        } else {
            match n.as_slice() {
                b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
                | b"upgrade" => return true,
                _ => {}
            }
        }
    }
    !(method && path && scheme)
}

/// Per-connection harness state: response bodies the flow-control
/// window would not let us finish yet.
struct Driver {
    conn: Connection,
    /// stream_id → next offset into the body still to send.
    pending_body: HashMap<u32, usize>,
}

const BODY: &[u8] = b"ok";

impl Driver {
    fn try_send_pending(&mut self) {
        let ids: Vec<u32> = self.pending_body.keys().copied().collect();
        for id in ids {
            let Some(start) = self.pending_body.get(&id).copied() else {
                continue;
            };
            let sent = self.conn.send_data(id, &BODY[start..], true);
            let next = start + sent;
            if next >= BODY.len() {
                self.pending_body.remove(&id);
            } else {
                self.pending_body.insert(id, next);
            }
        }
    }

    fn respond(&mut self, stream_id: u32, status: &[u8]) {
        self.conn.send_headers(
            stream_id,
            &[
                (b":status".to_vec(), status.to_vec()),
                (
                    b"content-length".to_vec(),
                    BODY.len().to_string().into_bytes(),
                ),
            ],
            false,
        );
        let sent = self.conn.send_data(stream_id, BODY, true);
        if sent < BODY.len() {
            self.pending_body.insert(stream_id, sent);
        }
    }

    fn react(&mut self, ev: Event) {
        match ev {
            Event::Headers {
                stream_id,
                end_stream,
                headers,
            } => {
                // Respond only once the request is fully received —
                // h2spec requires body frames to come after END_STREAM.
                if end_stream {
                    let status: &[u8] = if request_is_malformed(&headers) {
                        b"400"
                    } else {
                        b"200"
                    };
                    self.respond(stream_id, status);
                }
            }
            Event::Data {
                stream_id,
                end_stream,
                data,
            } => {
                self.conn.release_capacity(stream_id, data.len());
                if end_stream {
                    self.respond(stream_id, b"200");
                }
            }
            Event::Reset { .. } | Event::GoAway { .. } | Event::SettingsAck => {}
        }
    }
}

/// One h2c connection: pump socket → state machine → socket until the
/// peer or an engine connection-error ends it.
fn serve(mut stream: TcpStream) {
    let mut driver = Driver {
        conn: Connection::new(Role::Server, ConnectionConfig::default()),
        pending_body: HashMap::new(),
    };
    let mut backlog: Vec<u8> = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let pending = driver.conn.take_pending_writes();
        if !pending.is_empty() && stream.write_all(&pending).is_err() {
            return;
        }
        if driver.conn.connection_error().is_some() {
            let _ = stream.shutdown(Shutdown::Both);
            return;
        }
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                backlog.extend_from_slice(&buf[..n]);
                let mut events = Vec::new();
                let consumed = driver.conn.handle_read(&backlog, &mut events);
                backlog.drain(..consumed);
                for ev in events {
                    driver.react(ev);
                }
                driver.try_send_pending();
            }
        }
    }
}

fn find_h2spec() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("H2SPEC") {
        let p = std::path::PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    for candidate in [
        "/tmp/opencode/h2spec/h2spec",
        "/usr/local/bin/h2spec",
        "h2spec",
    ] {
        let p = std::path::PathBuf::from(candidate);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

#[test]
fn h2spec_generic_suite_passes() {
    let Some(h2spec) = find_h2spec() else {
        eprintln!("h2spec binary not found — skipping external conformance");
        return;
    };

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || serve(stream));
        }
    });

    let output = Command::new(&h2spec)
        .args(["generic", "-p", &port.to_string(), "-o", "5"])
        .output()
        .expect("spawn h2spec");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!(
        "h2spec exit={}\n{stdout}\n{stderr}",
        output.status.code().unwrap_or(-1)
    );
    assert!(
        output.status.success(),
        "h2spec generic found failures (exit={:?})",
        output.status.code()
    );
}
