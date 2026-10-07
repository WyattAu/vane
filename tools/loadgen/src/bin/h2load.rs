//! `h2load ADDR CONNS DURATION PATH SNI CERT [extra-ignored]` — HTTP/2
//! over TLS, rustls with the bench CA, ALPN `h2`.
//!
//! Same 13-field output contract as `loadgen`. Each connection runs one
//! h2 stream in flight (closed-loop, matching the h1 client) so leg
//! comparisons stay apples-to-apples across protocols.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use vane_loadgen::{client_config, result_line, split_args, Counters};
use tokio::net::TcpStream;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (pos, flags) = split_args(&args);
    if pos.len() < 6 {
        eprintln!("usage: h2load ADDR CONNS DURATION PATH SNI CERT [--method M] [--body N]");
        std::process::exit(2);
    }
    let addr = pos[0].clone();
    let conns: usize = pos[1].parse().expect("conns");
    let secs: u64 = pos[2].parse().expect("duration");
    let path = pos[3].clone();
    let sni = pos[4].clone();
    let cert = pos[5].clone();
    let method = flags
        .get("method")
        .cloned()
        .unwrap_or_else(|| "GET".into())
        .to_uppercase();
    let body_bytes: usize = flags.get("body").and_then(|s| s.parse().ok()).unwrap_or(0);

    let tls = client_config(&cert, &[b"h2"]).expect("client config");
    let counters = Arc::new(Counters::new());
    let end = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((conns / 8).clamp(2, 8))
        .enable_all()
        .build()
        .expect("runtime");
    let mut handles = Vec::with_capacity(conns);
    for _ in 0..conns {
        let addr = addr.clone();
        let sni = sni.clone();
        let tls = Arc::clone(&tls);
        let counters = Arc::clone(&counters);
        let path = path.clone();
        let method = method.clone();
        handles.push(rt.spawn(run_conn(
            addr, sni, tls, path, method, body_bytes, counters, end,
        )));
    }
    let mut pool: Vec<u64> = Vec::new();
    rt.block_on(async {
        for h in handles {
            if let Ok(samples) = h.await {
                pool.extend(samples);
            }
        }
    });
    rt.shutdown_timeout(Duration::from_secs(2));

    let duration = start.elapsed().as_secs_f64();
    println!("{}", result_line("h2", conns, duration, &counters, pool));
}

#[allow(clippy::too_many_arguments)]
async fn run_conn(
    addr: String,
    sni: String,
    tls: Arc<rustls::ClientConfig>,
    path: String,
    method: String,
    body_bytes: usize,
    counters: Arc<Counters>,
    end: Instant,
) -> Vec<u64> {
    let mut samples: Vec<u64> = Vec::with_capacity(1 << 16);
    'outer: while Instant::now() < end {
        let tcp = match TcpStream::connect(&addr).await {
            Ok(t) => t,
            Err(_) => {
                counters.conn_err.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
        };
        tcp.set_nodelay(true).ok();
        let server_name = rustls::pki_types::ServerName::try_from(sni.to_string())
            .expect("sni");
        let Ok(tls_stream) = tokio_rustls::TlsConnector::from(std::sync::Arc::clone(&tls))
            .connect(server_name, tcp)
            .await
        else {
            counters.conn_err.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        // `handshake` consumes the stream: the returned Connection owns
        // the IO and must be polled for frames to flow.
        let Ok((send_request, connection)) = h2::client::handshake(tls_stream).await else {
            counters.conn_err.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        // Driver for the h2 connection.
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request;
        while Instant::now() < end {
            let request = match http::Request::builder()
                .method(http::Method::from_bytes(method.as_bytes()).expect("method"))
                .uri(format!("https://{sni}{path}"))
                .body(())
            {
                Ok(r) => r,
                Err(_) => break 'outer,
            };
            let t0 = Instant::now();
            // A bodiless request must pass `end_of_stream` directly: the
            // h2 crate queues an empty `send_data(Bytes::new(), true)`
            // without emitting END_STREAM, and the server then waits for
            // a request body that never comes (found in the smoke test —
            // every response future hung forever).
            let Ok((response, mut body_sender)) =
                send_request.send_request(request, body_bytes == 0)
            else {
                counters.read_err.fetch_add(1, Ordering::Relaxed);
                continue 'outer;
            };
            if body_bytes > 0 {
                let chunk = bytes::Bytes::from(vec![b'x'; body_bytes]);
                let _ = body_sender.send_data(chunk, true);
            }
            let Ok(response) = response.await else {
                counters.read_err.fetch_add(1, Ordering::Relaxed);
                continue 'outer;
            };
            let status = response.status();
            // Drain to END_STREAM, releasing flow-control credit as we
            // go (skipping release deadlocks the window on big bodies).
            let mut body = response.into_body();
            while let Some(chunk) = body.data().await {
                match chunk {
                    Ok(b) => {
                        let _ = body.flow_control().release_capacity(b.len());
                    }
                    Err(_) => {
                        counters.read_err.fetch_add(1, Ordering::Relaxed);
                        continue 'outer;
                    }
                }
            }
            samples.push(t0.elapsed().as_micros() as u64);
            counters.total.fetch_add(1, Ordering::Relaxed);
            if status.is_success() {
                counters.ok.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.non200.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    samples
}