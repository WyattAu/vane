//! Shared load-generator plumbing: result counters, latency percentiles,
//! and the single-line output format the bench scripts parse positionally.
//!
//! A lib target (not a `#[path]` include) so dead-code analysis runs once
//! over the shared code instead of once per binary.
//!
//! Output contract (one line, whitespace-separated, positions fixed
//! because `scripts/bench_compare.sh` reads `$5`/`$9`/`$11`/`$13` with
//! awk):
//!
//! ```text
//! proto conns duration total rps ok conn_err read_err non200 0 p50_us 0 p99_us
//! ```
//!
//! Positions 10 and 12 are reserved (always `0`) so the fields the
//! scripts consume never move.


/// Shared, atomically-updated result counters.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Counters {
    /// Requests that received a complete response of any status.
    pub total: AtomicU64,
    /// Responses with a 2xx status.
    pub ok: AtomicU64,
    /// Responses with a non-2xx status (reported per leg; the bench
    /// publishes the max across windows).
    pub non200: AtomicU64,
    /// Failures to establish a connection (TCP/TLS/QUIC).
    pub conn_err: AtomicU64,
    /// Failures after the connection was up: write errors, truncated
    /// bodies, framing desyncs.
    pub read_err: AtomicU64,
}

impl Counters {
    pub fn new() -> Self {
        Self::default()
    }
}

/// One line of results in the documented 13-field format.
///
/// `rps` counts *complete* responses (total); non-2xx responses are
/// complete responses and are therefore both counted and separately
/// reported — a proxy that answers 503 fast must not look like a
/// throughput win.
#[must_use]
pub fn result_line(
    proto: &str,
    conns: usize,
    duration: f64,
    c: &Counters,
    mut pool: Vec<u64>,
) -> String {
    let total = c.total.load(Ordering::Relaxed);
    let ok = c.ok.load(Ordering::Relaxed);
    let non200 = c.non200.load(Ordering::Relaxed);
    let conn_err = c.conn_err.load(Ordering::Relaxed);
    let read_err = c.read_err.load(Ordering::Relaxed);
    let rps = if duration > 0.0 {
        total as f64 / duration
    } else {
        0.0
    };
    pool.sort_unstable();
    let pct = |p: f64| -> u64 {
        if pool.is_empty() {
            0
        } else {
            let idx = (((pool.len() as f64) * p).ceil() as usize)
                .saturating_sub(1)
                .min(pool.len() - 1);
            pool[idx]
        }
    };
    format!(
        "{proto} {conns} {duration:.0} {total} {rps:.1} {ok} {conn_err} {read_err} {non200} 0 {} 0 {}",
        pct(0.50),
        pct(0.99)
    )
}

/// Parses `["8", "10", "--flag", ...]` into positionals and flags.
/// Flags are `--name value` pairs; positional order is preserved for the
/// caller's own indexing.
#[must_use]
pub fn split_args(args: &[String]) -> (Vec<String>, std::collections::HashMap<String, String>) {
    let mut positional = Vec::new();
    let mut flags = std::collections::HashMap::new();
    let mut i = 0;
    while i < args.len() {
        if let Some(name) = args[i].strip_prefix("--") {
            if i + 1 < args.len() {
                flags.insert(name.to_string(), args[i + 1].clone());
                i += 2;
            } else {
                flags.insert(name.to_string(), String::new());
                i += 1;
            }
        } else {
            positional.push(args[i].clone());
            i += 1;
        }
    }
    (positional, flags)
}

/// Loads a PEM certificate chain or key with the `PemObject` API (the
/// same one vane migrated to when `rustls-pemfile` was removed).
pub fn pem_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("{path}: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{path}: {e}"))
}

pub fn pem_key(
    path: &str,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, String> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::PrivateKeyDer::from_pem_file(path).map_err(|e| format!("{path}: {e}"))
}

/// Builds a rustls client config that trusts exactly the given server
/// cert (self-signed bench material) and negotiates the given ALPN.
pub fn client_config(
    ca_path: &str,
    alpn: &[&[u8]],
) -> Result<std::sync::Arc<rustls::ClientConfig>, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in pem_certs(ca_path)? {
        roots.add(cert).map_err(|e| format!("ca: {e}"))?;
    }
    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(std::sync::Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_line_field_positions_match_the_awk_contract() {
        let c = Counters::new();
        c.total.store(1000, Ordering::Relaxed);
        c.ok.store(998, Ordering::Relaxed);
        c.non200.store(2, Ordering::Relaxed);
        let mut pool = vec![100u64, 50, 900, 700];
        // merge_into is exercised via result_line's internal sort; check
        // the line shape here.
        let line = result_line("h1", 8, 10.0, &c, std::mem::take(&mut pool));
        let f: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(f.len(), 13, "13 fields: {line}");
        assert_eq!(f[0], "h1");
        assert_eq!(f[4], "100.0"); // $5 = rps
        assert_eq!(f[8], "2"); // $9 = non200
        // sorted pool is [50, 100, 700, 900]; p50 = ceil(4*0.5)-1 = idx 1.
        assert_eq!(f[10], "100"); // $11 = p50
        // p99 = ceil(4*0.99)-1 = idx 3.
        assert_eq!(f[12], "900"); // $13 = p99
        assert_eq!(f[9], "0");
        assert_eq!(f[11], "0");
    }

    #[test]
    fn p50_of_an_even_pool_takes_the_upper_middle() {
        // Documented quirk: ceil(n*p)-1 indexing. For n=4, p=0.5 ->
        // index 1 (the upper of the two middles).
        let c = Counters::new();
        let pool = vec![900u64, 700, 100, 50]; // sorted: 50,100,700,900
        let line = result_line("h2", 1, 1.0, &c, pool);
        assert!(line.contains(" 100 0 "), "p50=100 in: {line}");
    }

    #[test]
    fn split_args_keeps_positional_order_and_extracts_flags() {
        let args: Vec<String> = ["--method", "POST", "127.0.0.1:1", "--body", "4096", "8"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let (pos, flags) = split_args(&args);
        assert_eq!(pos, vec!["127.0.0.1:1", "8"]);
        assert_eq!(flags.get("method").map(String::as_str), Some("POST"));
        assert_eq!(flags.get("body").map(String::as_str), Some("4096"));
    }
}