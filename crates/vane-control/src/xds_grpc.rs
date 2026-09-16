//! ADS (AggregatedDiscoveryService) session state machine.
//!
//! Pure logic — no I/O. Tracks per-type version/nonce, encodes
//! DiscoveryRequests (initial, ACK, NACK), and classifies responses.
//! The network driver (vane binary) owns the h2/gRPC transport and
//! feeds frames through [`AdsSession::on_response`].

use vane_proto::pb;

/// The resource types vane subscribes to, in subscription order
/// (LDS → RDS → CDS → EDS per the xDS-transport design).
pub const SUBSCRIBED_TYPES: [&str; 4] = [
    vane_proto::xds::type_url::LISTENER,
    vane_proto::xds::type_url::ROUTE,
    vane_proto::xds::type_url::CLUSTER,
    vane_proto::xds::type_url::ENDPOINT,
];

/// Per-type ADS subscription state.
#[derive(Debug, Clone)]
struct TypeState {
    /// Last accepted `version_info` ("" until the first accept).
    version: String,
    /// Last response nonce (echoed in ACK/NACK).
    nonce: String,
}

/// ADS session: node identity + per-type subscription state.
#[derive(Debug, Clone)]
pub struct AdsSession {
    node_id: String,
    node_cluster: String,
    states: Vec<(&'static str, TypeState)>,
}

/// The outcome of feeding a response into the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdsDecision {
    /// The resource set is accepted — send this ACK request.
    Ack {
        /// The accepted version.
        version: String,
        /// Raw resource messages for the consumer to map.
        resources: Vec<Vec<u8>>,
    },
    /// The resource set was rejected — send this NACK request.
    Nack {
        /// Why the set was rejected.
        error: String,
    },
}

impl AdsSession {
    /// Creates a session for the given node identity.
    #[must_use]
    pub fn new(node_id: &str, node_cluster: &str) -> Self {
        Self {
            node_id: node_id.to_owned(),
            node_cluster: node_cluster.to_owned(),
            states: SUBSCRIBED_TYPES
                .map(|t| {
                    (
                        t,
                        TypeState {
                            version: String::new(),
                            nonce: String::new(),
                        },
                    )
                })
                .to_vec(),
        }
    }

    /// Encodes the initial DiscoveryRequest for one type (no version,
    /// no nonce, all resources).
    #[must_use]
    pub fn initial_request(&self, type_url: &str) -> Vec<u8> {
        self.request(type_url, "", "", None)
    }

    /// Encodes an ACK request for `type_url` at its current state.
    #[must_use]
    pub fn ack_request(&self, type_url: &str) -> Vec<u8> {
        let st = &self
            .states
            .iter()
            .find(|(t, _)| *t == type_url)
            .expect("subscribed type")
            .1;
        self.request(type_url, &st.version, &st.nonce, None)
    }

    /// Feeds one decoded `DiscoveryResponse` for `type_url`. The
    /// consumer's `validate` decides acceptance (compile errors →
    /// NACK). Returns the decision; the caller sends the resulting
    /// request bytes.
    pub fn on_response(
        &mut self,
        type_url: &str,
        version: &str,
        nonce: &str,
        resources: Vec<Vec<u8>>,
        validate: impl FnOnce(&[Vec<u8>]) -> Result<(), String>,
    ) -> Option<AdsDecision> {
        let st = &mut self
            .states
            .iter_mut()
            .find(|(t, _)| *t == type_url)
            .or_else(|| {
                eprintln!("ads: response for unsubscribed type {type_url}");
                None
            })?
            .1;
        st.nonce = nonce.to_owned();
        match validate(&resources) {
            Ok(()) => {
                st.version = version.to_owned();
                Some(AdsDecision::Ack {
                    version: version.to_owned(),
                    resources,
                })
            }
            Err(error) => Some(AdsDecision::Nack { error }),
        }
    }

    /// Encodes a DiscoveryRequest for `type_url` at the given
    /// version/nonce, optionally NACKing with `error`.
    #[must_use]
    fn request(&self, type_url: &str, version: &str, nonce: &str, error: Option<&str>) -> Vec<u8> {
        let names: Vec<&str> = Vec::new(); // ADS: subscribe to all
        let req = vane_proto::xds::DiscoveryRequest {
            version_info: version,
            node_id: &self.node_id,
            node_cluster: &self.node_cluster,
            resource_names: &names,
            type_url,
            response_nonce: nonce,
            error_message: error,
        };
        req.encode()
    }

    /// Encodes a NACK request for `type_url` with `error`.
    #[must_use]
    pub fn nack_request(&self, type_url: &str, error: &str) -> Vec<u8> {
        let st = &self
            .states
            .iter()
            .find(|(t, _)| *t == type_url)
            .expect("subscribed type")
            .1;
        self.request(type_url, &st.version, &st.nonce, Some(error))
    }

    /// The nonce currently tracked for `type_url`.
    #[must_use]
    pub fn nonce(&self, type_url: &str) -> &str {
        &self
            .states
            .iter()
            .find(|(t, _)| *t == type_url)
            .expect("subscribed type")
            .1
            .nonce
    }

    /// The accepted version for `type_url`.
    #[must_use]
    pub fn version(&self, type_url: &str) -> &str {
        &self
            .states
            .iter()
            .find(|(t, _)| *t == type_url)
            .expect("subscribed type")
            .1
            .version
    }
}

/// Decodes a gRPC-unframed DiscoveryResponse and splits out its fields
/// (thin wrapper over [`vane_proto::xds::decode_discovery_response`]).
#[must_use]
pub fn decode_response(buf: &[u8]) -> Option<(String, String, String, Vec<Vec<u8>>)> {
    let r = vane_proto::xds::decode_discovery_response(buf)?;
    Some((r.version_info, r.type_url, r.nonce, r.resources))
}

/// Convenience: gRPC-frame an encoded request.
#[must_use]
pub fn frame_request(encoded: &[u8]) -> Vec<u8> {
    vane_proto::xds::grpc_frame(encoded)
}

/// Decodes (version_info, nonce, type_url) from an encoded
/// DiscoveryRequest.
#[must_use]
pub fn decode_request_fields(buf: &[u8]) -> (String, String, String) {
    let mut version = String::new();
    let mut nonce = String::new();
    let mut type_url = String::new();
    let mut pos = 0;
    while pos < buf.len() {
        match pb::decode_field(&buf[pos..]) {
            Some((f, n)) => {
                pos += n;
                match f.number {
                    2 => version = String::from_utf8_lossy(f.bytes).into_owned(),
                    4 => type_url = String::from_utf8_lossy(f.bytes).into_owned(),
                    5 => nonce = String::from_utf8_lossy(f.bytes).into_owned(),
                    _ => {}
                }
            }
            None => break,
        }
    }
    (version, nonce, type_url)
}

/// Extracts the raw Any-value bytes from a resource `Any` message
/// (`type_url` = field 1, `value` = field 2).
#[must_use]
pub fn any_value(any: &[u8]) -> Option<(String, Vec<u8>)> {
    let mut pos = 0;
    let mut url = String::new();
    let mut value = Vec::new();
    while pos < any.len() {
        let (f, n) = pb::decode_field(&any[pos..])?;
        pos += n;
        match f.number {
            1 => url = String::from_utf8_lossy(f.bytes).into_owned(),
            2 => value = f.bytes.to_vec(),
            _ => {}
        }
    }
    Some((url, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: fn(&[Vec<u8>]) -> Result<(), String> = |_| Ok(());
    const BAD: fn(&[Vec<u8>]) -> Result<(), String> = |_| Err("compile failed".into());

    #[test]
    fn initial_request_has_empty_version_and_nonce() {
        let s = AdsSession::new("vane-1", "vane");
        let buf = s.initial_request(SUBSCRIBED_TYPES[0]);
        let (version, nonce, type_url) = decode_request_fields(&buf);
        assert_eq!(version, "");
        assert_eq!(nonce, "");
        assert_eq!(type_url, SUBSCRIBED_TYPES[0]);
    }

    #[test]
    fn accept_advances_version_and_acks() {
        let mut s = AdsSession::new("vane-1", "vane");
        let t = SUBSCRIBED_TYPES[2]; // CDS
        let decision = s
            .on_response(t, "v3", "nonce-3", vec![b"res".to_vec()], OK)
            .expect("decision");
        match decision {
            AdsDecision::Ack { version, resources } => {
                assert_eq!(version, "v3");
                assert_eq!(resources.len(), 1);
            }
            other => panic!("expected ack: {other:?}"),
        }
        assert_eq!(s.version(t), "v3");
        assert_eq!(s.nonce(t), "nonce-3");
        // The ACK request echoes both.
        let ack = s.ack_request(t);
        let (version, nonce, _) = decode_request_fields(&ack);
        assert_eq!(version, "v3");
        assert_eq!(nonce, "nonce-3");
    }

    #[test]
    fn reject_nacks_without_advancing() {
        let mut s = AdsSession::new("vane-1", "vane");
        let t = SUBSCRIBED_TYPES[1];
        let decision = s
            .on_response(t, "bad", "n1", Vec::new(), BAD)
            .expect("decision");
        assert_eq!(
            decision,
            AdsDecision::Nack {
                error: "compile failed".into()
            }
        );
        assert_eq!(s.version(t), "", "version must not advance on NACK");
        assert_eq!(s.nonce(t), "n1", "nonce still echoes");
        let nack = s.nack_request(t, "compile failed");
        let (version, nonce, _) = decode_request_fields(&nack);
        assert_eq!(nonce, "n1");
        assert_eq!(version, "", "NACK re-requests the old version");
    }

    #[test]
    fn unsubscribed_type_is_ignored() {
        let mut s = AdsSession::new("vane-1", "vane");
        assert!(
            s.on_response("type.googleapis.com/bogus", "v", "n", Vec::new(), OK)
                .is_none(),
            "unsubscribed types produce no decision"
        );
    }

    #[test]
    fn any_value_splits_url_and_bytes() {
        let mut any = Vec::new();
        pb::string_field(&mut any, 1, "type.googleapis.com/x.Cluster");
        pb::bytes_field(&mut any, 2, b"payload");
        let (url, value) = any_value(&any).expect("split");
        assert_eq!(url, "type.googleapis.com/x.Cluster");
        assert_eq!(value, b"payload");
    }
}
