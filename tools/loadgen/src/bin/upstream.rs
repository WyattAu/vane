//! `upstream ADDR [BODY_BYTES]` — threaded HTTP/1.1 keep-alive upstream.
//!
//! Single-process threaded (not async) on purpose: it must never be the
//! bottleneck a benchmark measures, and a thread per connection with a
//! 10-byte or 64 KiB static body keeps the serving cost flat. Serves GET
//! and POST alike: POST bodies are drained (Content-Length), never
//! echoed, so the response size is constant regardless of method.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| {
        eprintln!("usage: upstream ADDR [BODY_BYTES]");
        std::process::exit(2);
    });
    let body: Arc<Vec<u8>> = Arc::new(match args.get(2).map(|s| s.parse::<usize>()) {
        Some(Ok(n)) if n > 0 => vec![b'x'; n],
        _ => b"hello-vane".to_vec(),
    });
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("bind {addr}: {e}");
        std::process::exit(1);
    });
    eprintln!("upstream on {addr} (body {} bytes)", body.len());
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let body = Arc::clone(&body);
        let counter = Arc::clone(&counter);
        std::thread::spawn(move || {
            let _ = handle(stream, body, counter);
        });
    }
}

fn handle(
    mut stream: TcpStream,
    body: Arc<Vec<u8>>,
    counter: Arc<std::sync::atomic::AtomicUsize>,
) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut buf = [0u8; 8192];
    let mut pending: Vec<u8> = Vec::with_capacity(8192);
    let head = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: keep-alive\r\n";
    loop {
        // Parse one request head from `pending`.
        let head_end = loop {
            if let Some(pos) = find_head_end(&pending) {
                break pos;
            }
            let n = stream.read(&mut buf)?;
            if n == 0 {
                return Ok(()); // client closed
            }
            pending.extend_from_slice(&buf[..n]);
            if pending.len() > 1 << 20 {
                return Ok(()); // absurd head; drop
            }
        };
        let req = &pending[..head_end];
        let content_length = content_length_of(req);
        let have_body = pending.len() - head_end;
        let mut consumed = head_end + content_length.min(have_body);
        // Drain any body bytes not yet arrived.
        let mut still_needed = content_length.saturating_sub(have_body);
        while still_needed > 0 {
            let n = stream.read(&mut buf)?;
            if n == 0 {
                return Ok(());
            }
            let take = n.min(still_needed);
            still_needed -= take;
            if take < n {
                pending.extend_from_slice(&buf[take..n]);
            }
        }
        pending.drain(..consumed);
        consumed = 0;
        let _ = consumed;
        let mut resp = Vec::with_capacity(head.len() + 40 + body.len());
        resp.extend_from_slice(head);
        resp.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        resp.extend_from_slice(&body);
        stream.write_all(&resp)?;
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    for i in 0..=buf.len() - 4 {
        if &buf[i..i + 4] == b"\r\n\r\n" {
            return Some(i + 4);
        }
    }
    None
}

fn content_length_of(head: &[u8]) -> usize {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n") {
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            return rest.trim().parse().unwrap_or(0);
        }
        if let Some(rest) = line.strip_prefix("content-length:") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}