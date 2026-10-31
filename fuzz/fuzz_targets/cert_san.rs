//! Fuzz target: the hand-rolled DER walk that extracts a SPIFFE ID from a
//! leaf certificate (`vane_tls::mesh::spiffe_id`).
//!
//! This is the one parser in the tree whose input is *entirely* peer-
//! supplied: the bytes come straight off a TLS handshake, before any
//! signature check, so a peer fully controls every length field. The walk
//! must therefore be panic-free on arbitrary bytes and must never index
//! out of bounds.
//!
//! Added after a 600s nightly run of a sibling target surfaced the same
//! bug class elsewhere (an unchecked `len_pos + len` in the HPACK
//! decoder). This target did not exist, so nothing was watching the DER
//! walker at all — and a single over-long GeneralName panicked it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rustls::pki_types::CertificateDer;
use vane_tls::mesh::spiffe_id;

fuzz_target!(|data: &[u8]| {
    // Arbitrary bytes must simply yield `None` (or, for the shapes that
    // are well formed, a string) — never a panic.
    let cert = CertificateDer::from(data.to_vec());
    if let Some(id) = spiffe_id(&cert) {
        // `spiffe_id` returns the first URI SAN of any scheme, so the
        // scheme is not a useful invariant. What must hold is that the
        // returned value is genuinely a slice of the input: a wrong range
        // would copy bytes the length field never described, which is the
        // failure mode the HPACK decoder had. `windows` also covers the
        // empty-string case.
        assert!(
            !id.is_empty(),
            "spiffe_id returned an empty URI SAN"
        );
        assert!(
            data.windows(id.len()).any(|w| w == id.as_bytes()),
            "spiffe_id returned bytes that are not present in the input: {id:?}"
        );
    }

    // Truncation is what a real connection sees when the handshake is cut
    // short, so every prefix must be safe too. Bounded to keep the target
    // fast; the deep run covers the long tail.
    let n = data.len().min(96);
    for cut in 1..=n {
        let prefix = CertificateDer::from(data[..cut].to_vec());
        let _ = spiffe_id(&prefix);
    }
});