//! `h3load ADDR CONNS DURATION PATH SNI CERT [marker]` — HTTP/3 over
//! QUIC, rustls (ring) trusting the bench CA, ALPN `h3`.
//!
//! Same 13-field output contract as `loadgen`/`h2load`, one in-flight
//! request per QUIC connection (closed-loop, matching the other
//! clients). `ADDR` is a socket address (the bench always passes
//! `127.0.0.1:port`); `SNI` names the TLS handshake. The trailing
//! `marker` argument the bench scripts pass is ignored.
//!
//! QUIC config mirrors `vane_proxy`'s own h3 client (the same
//! `QuicClientConfig::try_from` construction) so the client stack is
//! the one vane ships, not a laboratory variant.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use vane_loadgen::{result_line, split_args, Counters};
use quinn::crypto::rustls::QuicClientConfig;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (pos, _flags) = split_args(&args);
    if pos.len() < 6 {
        eprintln!("usage: h3load ADDR CONNS DURATION PATH SNI CERT [marker]");
        std::process::exit(2);
    }
    let addr: SocketAddr = pos[0].parse().expect("ADDR must be ip:port");
    let conns: usize = pos[1].parse().expect("conns");
    let secs: u64 = pos[2].parse().expect("duration");
    let path = pos[3].clone();
    let sni = pos[4].clone();
    let cert = pos[5].clone();

    // Trust exactly the bench CA; ALPN h3. Built from raw material here
    // (not `client_config`) because quinn consumes the rustls config
    // wholesale via `QuicClientConfig::try_from`.
    let mut roots = rustls::RootCertStore::empty();
    for cert in vane_loadgen::pem_certs(&cert).expect("ca pem") {
        roots.add(cert).expect("add ca");
    }
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];

    let qcc = QuicClientConfig::try_from(client_tls).expect("quinn client config");
    let mut client_cfg = quinn::ClientConfig::new(Arc::new(qcc));
    client_cfg.transport_config(Arc::new({
        let mut t = quinn::TransportConfig::default();
        t.keep_alive_interval(Some(Duration::from_millis(200)));
        t
    }));
    // quinn grabs the current tokio runtime at endpoint creation, so the
    // endpoint must be built inside the runtime (panics with "no async
    // runtime found" otherwise — found in the smoke test).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((conns / 8).clamp(2, 8))
        .enable_all()
        .build()
        .expect("runtime");
    // quinn grabs the current tokio runtime at endpoint creation, so the
    // endpoint must be built inside it (panics with "no async runtime
    // found" otherwise — found in the smoke test).
    let endpoint = Arc::new(rt.block_on(async {
        let mut endpoint =
            quinn::Endpoint::client("127.0.0.1:0".parse().expect("bind")).expect("endpoint");
        endpoint.set_default_client_config(client_cfg);
        endpoint
    }));

    let counters = Arc::new(Counters::new());
    let end = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let mut handles = Vec::with_capacity(conns);
    for _ in 0..conns {
        let endpoint = Arc::clone(&endpoint);
        let counters = Arc::clone(&counters);
        let sni = sni.clone();
        let path = path.clone();
        handles.push(rt.spawn(run_conn(endpoint, addr, sni, path, counters, end)));
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
    println!("{}", result_line("h3", conns, duration, &counters, pool));
}

#[allow(clippy::too_many_arguments)]
async fn run_conn(
    endpoint: Arc<quinn::Endpoint>,
    addr: SocketAddr,
    sni: String,
    path: String,
    counters: Arc<Counters>,
    end: Instant,
) -> Vec<u64> {
    let mut samples: Vec<u64> = Vec::with_capacity(1 << 16);
    'outer: while Instant::now() < end {
        let Ok(connecting) = endpoint.connect(addr, &sni) else {
            counters.conn_err.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let Ok(quinn_conn) = connecting.await else {
            counters.conn_err.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let Ok((mut h3_driver, mut send_request)) =
            h3::client::new(h3_quinn::Connection::new(quinn_conn)).await
        else {
            counters.conn_err.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        // The h3 driver must be polled for the connection to progress.
        let driver = tokio::spawn(async move {
            let _ = h3_driver.wait_idle().await;
        });
        let uri = format!("https://{sni}{path}");
        while Instant::now() < end {
            let Ok(request) = http::Request::builder().method("GET").uri(&uri).body(()) else {
                break 'outer;
            };
            let t0 = Instant::now();
            let Ok(mut stream) = send_request.send_request(request).await else {
                counters.read_err.fetch_add(1, Ordering::Relaxed);
                continue 'outer;
            };
            // END_STREAM on the request: without `finish` the server
            // waits for a request body that never arrives (vane's own h3
            // client finishes explicitly for the same reason).
            let _ = stream.finish().await;
            let Ok(response) = stream.recv_response().await else {
                counters.read_err.fetch_add(1, Ordering::Relaxed);
                continue 'outer;
            };
            let status = response.status();
            // Drain the body to completion: per-request cost includes
            // END_STREAM, and skipping it would understate latency.
            loop {
                match stream.recv_data().await {
                    Ok(Some(_chunk)) => {}
                    Ok(None) => break,
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
        driver.abort();
    }
    samples
}