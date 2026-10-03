//! Minimal pooled HTTP/1.1 client for the edges' plain-h1 upstreams.
//!
//! The edges historically dialed upstreams through `reqwest`, whose
//! per-request URL/header-map machinery dominated the edge's hot path
//! (bench_bridge: the h3 edge leg ran 2.4x off the h1 baseline). This
//! client is the measured replacement: prebuilt head bytes over pooled
//! keep-alive `TcpStream`s, Content-Length-framed bodies.
//!
//! Scope: `http://` upstreams only (no TLS in this client), no chunked
//! responses (the engine's upstreams answer with Content-Length).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A parsed upstream response (head + fully buffered body).
pub struct RawResponse {
    /// Response status code.
    pub status: u16,
    /// Header pairs (lowercase names), as received.
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    /// Fully buffered response body.
    pub body: Vec<u8>,
}

/// Pooled client for ONE upstream address. Cheap to clone.
#[derive(Clone)]
pub struct H1Pool {
    addr: std::net::SocketAddr,
    idle: Arc<Mutex<VecDeque<TcpStream>>>,
    first_byte_timeout: Duration,
}

const MAX_IDLE: usize = 64;

impl H1Pool {
    /// A pool for `addr`.
    #[must_use]
    pub fn new(addr: std::net::SocketAddr, first_byte_timeout: Duration) -> Self {
        Self {
            addr,
            idle: Arc::new(Mutex::new(VecDeque::new())),
            first_byte_timeout,
        }
    }

    /// Sends one request and returns the buffered response. Healthy
    /// connections return to the pool; on any transport error the
    /// connection is discarded (the pool self-heals on the next
    /// request).
    ///
    /// # Errors
    /// Dial, write, or response-parse failures as strings.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        host: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<RawResponse, String> {
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\n");
        for (n, v) in headers {
            head.push_str(n);
            head.push_str(": ");
            head.push_str(v);
            head.push_str("\r\n");
        }
        head.push_str(&format!("content-length: {}\r\n\r\n", body.len()));
        let head = head.into_bytes();

        let mut stream = self.checkout().await?;
        match self.exchange(&mut stream, &head, &body).await {
            Ok(resp) => {
                self.park(stream);
                Ok(resp)
            }
            Err(e) => Err(e), // stream dropped: connection discarded
        }
    }

    async fn checkout(&self) -> Result<TcpStream, String> {
        if let Some(s) = self.idle.lock().expect("pool lock").pop_front() {
            // A parked socket may have died while idle; the caller's
            // exchange error path discards it and the next request
            // dials fresh.
            return Ok(s);
        }
        TcpStream::connect(self.addr)
            .await
            .map_err(|e| format!("upstream dial: {e}"))
            .inspect(|s| {
                let _ = s.set_nodelay(true);
            })
    }

    fn park(&self, stream: TcpStream) {
        let mut idle = self.idle.lock().expect("pool lock");
        if idle.len() < MAX_IDLE {
            idle.push_back(stream);
        }
    }

    async fn exchange(
        &self,
        stream: &mut TcpStream,
        head: &[u8],
        body: &[u8],
    ) -> Result<RawResponse, String> {
        stream.write_all(head).await.map_err(write_err)?;
        if !body.is_empty() {
            stream.write_all(body).await.map_err(write_err)?;
        }
        let timeout = self.first_byte_timeout;
        let mut buf = vec![0u8; 16 * 1024];
        let mut got: Vec<u8> = Vec::new();
        let head_end;
        let content_length;
        loop {
            let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return Err("upstream read failed".into()),
                Ok(Ok(n)) => n,
            };
            got.extend_from_slice(&buf[..n]);
            if let Some(pos) = find(&got, b"\r\n\r\n") {
                head_end = pos + 4;
                let head = String::from_utf8_lossy(&got[..pos]).to_ascii_lowercase();
                content_length = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok());
                break;
            }
        }
        let cl = content_length.unwrap_or(0);
        let mut body_have = got.len() - head_end;
        while body_have < cl {
            let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return Err("upstream read failed".into()),
                Ok(Ok(n)) => n,
            };
            got.extend_from_slice(&buf[..n]);
            body_have += n;
        }
        parse_response(&got[..head_end + cl]).ok_or_else(|| "bad upstream response".into())
    }
}

fn write_err(e: std::io::Error) -> String {
    format!("upstream write failed: {e}")
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn parse_response(buf: &[u8]) -> Option<RawResponse> {
    let pos = find(buf, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..pos]);
    let mut lines = head.lines();
    let status_line = lines.next()?;
    let status: u16 = status_line.split_whitespace().nth(1)?.parse().ok()?;
    let mut headers = Vec::new();
    for l in lines {
        if let Some((n, v)) = l.split_once(':') {
            headers.push((
                n.trim().to_ascii_lowercase().into_bytes(),
                v.trim().as_bytes().to_vec(),
            ));
        }
    }
    Some(RawResponse {
        status,
        headers,
        body: buf[pos + 4..].to_vec(),
    })
}
