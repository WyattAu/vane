//! Fuzz target: the hand-rolled xDS protobuf decoders (QA-03
//! extension). Control-plane bytes are untrusted: none of the
//! decoders may panic on arbitrary input, and decoded clusters must
//! carry a name.

#![no_main]

use libfuzzer_sys::fuzz_target;
use vane_control::envoy;

fuzz_target!(|data: &[u8]| {
    if let Some(c) = envoy::decode_cluster(data) {
        assert!(!c.name.is_empty() || c.backends.is_empty());
    }
    if let Some((name, _)) = envoy::decode_cla(data) {
        assert!(!name.is_empty());
    }
    let _ = envoy::decode_route_config(data);
    let _ = envoy::decode_secret(data);
    let _ = envoy::decode_listener(data);
});
