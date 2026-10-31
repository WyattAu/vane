//! `certgen DIR` — writes `cert.pem`/`key.pem` (self-signed, SANs
//! `localhost` + `127.0.0.1`) into DIR. The bench proxies all load this
//! same pair so TLS legs measure the proxy, not differing cert chains.

use std::path::Path;

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: certgen DIR");
        std::process::exit(2);
    });
    let dir = Path::new(&dir);
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("generate cert");
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).expect("write cert.pem");
    std::fs::write(dir.join("key.pem"), cert.signing_key.serialize_pem())
        .expect("write key.pem");
    println!("{}", dir.join("cert.pem").display());
}