//! Round-trip against a running vane sidecar.
//!
//! Start the proxy with `sidecar.enabled = true`, then:
//! `cargo run -p vane-client-sdk --example sidecar_roundtrip`

use std::time::Duration;

use vane_client_sdk::Sidecar;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Sidecar::connect("/dev/shm/vane-sidecar")?;
    let t0 = std::time::Instant::now();
    let response = Sidecar::call(&mut client, b"GET /hello", Duration::from_secs(1))?;
    let elapsed = t0.elapsed();
    println!(
        "response: {} ({} µs)",
        String::from_utf8_lossy(&response),
        elapsed.as_micros()
    );
    Ok(())
}
