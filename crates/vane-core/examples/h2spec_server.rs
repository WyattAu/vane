//! Raw-TCP h2c server driving the sans-io Connection — a standalone
//! target for `h2spec generic` and manual probing:
//!
//! ```text
//! cargo run -p vane-core --example h2spec_server -- 127.0.0.1:8080
//! h2spec generic -p 8080
//! ```
//!
//! The conformance-covered logic is identical to tests/h2spec.rs.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};

use vane_core::h2::connection::{Connection, ConnectionConfig, Event, Role};

const BODY: &[u8] = b"ok";

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

struct Driver {
    conn: Connection,
    pending_body: HashMap<u32, usize>,
}

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
            _ => {}
        }
    }
}

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

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into());
    let listener = TcpListener::bind(&addr).expect("bind");
    eprintln!("listening on {addr}");
    for stream in listener.incoming().flatten() {
        std::thread::spawn(move || serve(stream));
    }
}
