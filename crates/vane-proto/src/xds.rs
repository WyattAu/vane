//! xDS v3 wire encoding: gRPC message framing + the
//! `AggregatedDiscoveryService` message subset vane consumes.
//!
//! Hand-rolled protobuf (see [`crate::pb`]) — no protoc, no prost.
//! Only the fields vane's control plane sends or reads are encoded;
//! unknown peer fields decode-skip per the wire spec.

use crate::pb;

/// Wraps a protobuf message in the gRPC length-prefixed frame:
/// 1 byte compression flag (0) + 4-byte big-endian length + payload.
#[must_use]
pub fn grpc_frame(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + message.len());
    out.push(0); // uncompressed
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    out
}

/// Splits a gRPC byte stream into (compression_flag, message) frames.
/// Returns the frames and the number of bytes consumed (a trailing
/// partial frame stays in the caller's buffer).
pub fn grpc_unframe(buf: &[u8]) -> (Vec<(bool, Vec<u8>)>, usize) {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos + 5 <= buf.len() {
        let compressed = buf[pos] & 0x01 == 1;
        let len = u32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]);
        let end = pos + 5 + len as usize;
        if end > buf.len() {
            break; // partial frame
        }
        frames.push((compressed, buf[pos + 5..end].to_vec()));
        pos = end;
    }
    (frames, pos)
}

/// gRPC path for ADS: `/envoy.service.discovery.v3.AggregatedDiscovery
/// Service/StreamAggregatedResources`.
pub const ADS_PATH: &str =
    "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources";

/// `envoy.service.discovery.v3.DiscoveryRequest` — the fields vane
/// sends (encoding is field-number exact).
pub struct DiscoveryRequest<'a> {
    /// `version_info`: the version of the last accepted response (""
    /// on first connect / reconnect-from-scratch).
    pub version_info: &'a str,
    /// Node identity: `id` + `cluster` (field 1, embedded message).
    pub node_id: &'a str,
    /// Node cluster name (inside the Node message).
    pub node_cluster: &'a str,
    /// `resource_names`: which resources of `type_url` we want (empty =
    /// all, for ADS).
    pub resource_names: &'a [&'a str],
    /// `type_url`: the resource type this request addresses.
    pub type_url: &'a str,
    /// `response_nonce`: echoed from the response being ACKed/NACKed.
    pub response_nonce: &'a str,
    /// `error_detail.message` — present on NACK (field 2 embedded).
    pub error_message: Option<&'a str>,
}

impl DiscoveryRequest<'_> {
    /// Encodes the request protobuf.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128);
        // field 1: Node { id = 1 (string), cluster = 2 (string) }
        let mut node = Vec::with_capacity(32);
        pb::string_field(&mut node, 1, self.node_id);
        pb::string_field(&mut node, 2, self.node_cluster);
        pb::message_field(&mut out, 1, &node);
        // field 2: version_info
        pb::string_field(&mut out, 2, self.version_info);
        // field 3: resource_names (repeated string)
        for name in self.resource_names {
            pb::string_field(&mut out, 3, name);
        }
        // field 4: type_url
        pb::string_field(&mut out, 4, self.type_url);
        // field 5: response_nonce
        pb::string_field(&mut out, 5, self.response_nonce);
        // field 6: error_detail { message = 3 } — NACKs only
        if let Some(msg) = self.error_message {
            let mut status = Vec::with_capacity(32);
            pb::string_field(&mut status, 3, msg);
            pb::message_field(&mut out, 6, &status);
        }
        out
    }
}

/// The xDS resource type URLs vane consumes.
pub mod type_url {
    /// Cluster Discovery Service.
    pub const CLUSTER: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
    /// Endpoint Discovery Service (ClusterLoadAssignment).
    pub const ENDPOINT: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
    /// Listener Discovery Service.
    pub const LISTENER: &str = "type.googleapis.com/envoy.config.listener.v3.Listener";
    /// Route Configuration Service.
    pub const ROUTE: &str = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
    /// Scoped Routes (secret discovery analogs are analogous).
    pub const SECRET: &str = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret";
}

/// A decoded `DiscoveryResponse` (the subset vane reads).
#[derive(Debug, Clone)]
pub struct DiscoveryResponse {
    /// `version_info`: the resource set version.
    pub version_info: String,
    /// Raw resource messages (Any-packed: type_url + value bytes are
    /// NOT decoded here — the consumer matches on its own type).
    pub resources: Vec<Vec<u8>>,
    /// `type_url` of the resources in this response.
    pub type_url: String,
    /// `nonce` to echo in the ACK.
    pub nonce: String,
}

/// Decodes a `DiscoveryResponse` protobuf (best-effort; unknown
/// fields skipped).
#[must_use]
pub fn decode_discovery_response(buf: &[u8]) -> Option<DiscoveryResponse> {
    let mut out = DiscoveryResponse {
        version_info: String::new(),
        resources: Vec::new(),
        type_url: String::new(),
        nonce: String::new(),
    };
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = pb::decode_field(&buf[pos..])?;
        pos += n;
        match f.number {
            1 => out.version_info = String::from_utf8_lossy(f.bytes).into_owned(),
            2 => {
                // resources: google.protobuf.Any { type_url = 1, value = 2 }
                out.resources.push(f.bytes.to_vec());
            }
            4 => out.type_url = String::from_utf8_lossy(f.bytes).into_owned(),
            5 => out.nonce = String::from_utf8_lossy(f.bytes).into_owned(),
            _ => {}
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_frame_roundtrip() {
        let frames = grpc_unframe(&grpc_frame(b"hello"));
        assert_eq!(frames.0.len(), 1);
        assert!(!frames.0[0].0);
        assert_eq!(frames.0[0].1, b"hello");
        assert_eq!(frames.1, 5 + 5);
        // Partial frame stays unconsumed.
        let full = grpc_frame(b"abc");
        let (frames, consumed) = grpc_unframe(&full[..full.len() - 1]);
        assert!(frames.is_empty());
        assert_eq!(consumed, 0);
    }

    #[test]
    fn discovery_request_encodes_fields() {
        let req = DiscoveryRequest {
            version_info: "v1",
            node_id: "vane-edge-1",
            node_cluster: "vane",
            resource_names: &["r1", "r2"],
            type_url: type_url::CLUSTER,
            response_nonce: "n-1",
            error_message: None,
        };
        let buf = req.encode();
        let mut pos = 0;
        let mut saw_node = false;
        let mut saw_version = false;
        let mut names = Vec::new();
        while pos < buf.len() {
            let (f, n) = pb::decode_field(&buf[pos..]).expect("field");
            pos += n;
            match f.number {
                1 => saw_node = true,
                2 => {
                    assert_eq!(f.bytes, b"v1");
                    saw_version = true;
                }
                3 => names.push(String::from_utf8_lossy(f.bytes).into_owned()),
                4 => assert_eq!(f.bytes, type_url::CLUSTER.as_bytes()),
                5 => assert_eq!(f.bytes, b"n-1"),
                _ => {}
            }
        }
        assert!(saw_node && saw_version);
        assert_eq!(names, vec!["r1", "r2"]);
    }

    #[test]
    fn discovery_response_roundtrip() {
        // Hand-encode: version_info=1, resources=2 (Any bytes), type_url=4,
        // nonce=5.
        let mut buf = Vec::new();
        pb::string_field(&mut buf, 1, "v7");
        pb::bytes_field(&mut buf, 2, b"\x0a\x02C1\x12\x04host");
        pb::string_field(&mut buf, 4, type_url::CLUSTER);
        pb::string_field(&mut buf, 5, "nonce-9");
        let decoded = decode_discovery_response(&buf).expect("decode");
        assert_eq!(decoded.version_info, "v7");
        assert_eq!(decoded.resources.len(), 1);
        assert_eq!(decoded.type_url, type_url::CLUSTER);
        assert_eq!(decoded.nonce, "nonce-9");
    }

    #[test]
    fn nack_encodes_error_detail() {
        let req = DiscoveryRequest {
            version_info: "",
            node_id: "n",
            node_cluster: "c",
            resource_names: &[],
            type_url: type_url::ROUTE,
            response_nonce: "bad",
            error_message: Some("decode failed"),
        };
        let buf = req.encode();
        let mut saw_error = false;
        let mut pos = 0;
        while pos < buf.len() {
            let (f, n) = pb::decode_field(&buf[pos..]).expect("field");
            pos += n;
            if f.number == 6 {
                saw_error = true;
            }
        }
        assert!(saw_error);
    }
}
