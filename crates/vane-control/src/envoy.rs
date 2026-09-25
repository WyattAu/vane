//! Envoy xDS resource decoding: the protobuf subset vane maps into
//! [`XdsSnapshot`](crate::xds::XdsSnapshot) — Clusters (name +
//! socket-address backends) and RouteConfigurations (domain/prefix →
//! cluster). Field numbers are Envoy API v3 exact.

use crate::xds::{XdsCluster, XdsRoute, XdsSnapshot};
use vane_proto::pb;

/// A decoded Envoy `Cluster` (the vane-relevant subset).
#[derive(Debug, Clone, Default)]
pub struct EnvoyCluster {
    /// Cluster name.
    pub name: String,
    /// Static backend `host:port` addresses from load_assignment.
    pub backends: Vec<String>,
    /// http2_protocol_options present (field 20) → upstream h2.
    pub http2: bool,
}

/// Decodes an Envoy `Cluster` message.
#[must_use]
pub fn decode_cluster(buf: &[u8]) -> Option<EnvoyCluster> {
    let mut out = EnvoyCluster::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = pb::decode_field(&buf[pos..])?;
        pos += n;
        match f.number {
            1 => out.name = String::from_utf8_lossy(f.bytes).into_owned(),
            // cluster_type = 2 (group STANDARD=1); http2_protocol_options
            // = 20 (message, present ⇒ http2 upstream).
            20 => out.http2 = !f.bytes.is_empty(),
            // load_assignment = 33 in the current v3 API (connect_timeout
            // occupies field 4).
            33 => decode_load_assignment(f.bytes, &mut out.backends),
            _ => {}
        }
    }
    Some(out)
}

/// `ClusterLoadAssignment`: endpoints (field 2) → locality endpoints →
/// lb_endpoints → endpoint.address.socket_address.
fn decode_load_assignment(buf: &[u8], backends: &mut Vec<String>) {
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        if f.number == 2 {
            decode_locality(f.bytes, backends);
        }
    }
}

fn decode_locality(buf: &[u8], backends: &mut Vec<String>) {
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        // LocalityLbEndpoints.lb_endpoints = 2 (locality = 1, metadata
        // = 9, proximity = 6 are ignored).
        if f.number == 2 {
            decode_lb_endpoint(f.bytes, backends);
        }
    }
}

fn decode_lb_endpoint(buf: &[u8], backends: &mut Vec<String>) {
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        if f.number == 1 {
            decode_endpoint(f.bytes, backends);
        }
    }
}

fn decode_endpoint(buf: &[u8], backends: &mut Vec<String>) {
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        if f.number == 1 {
            decode_address(f.bytes, backends);
        }
    }
}

fn decode_address(buf: &[u8], backends: &mut Vec<String>) {
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        // Address.socket_address = 1 (the address oneof; pipe = 2).
        if f.number == 1 {
            decode_socket_address(f.bytes, backends);
        }
    }
}

/// `SocketAddress`: address = 2 (string), port_value = 3 (the
/// port_specifier oneof) or named_port = 4.
fn decode_socket_address(buf: &[u8], backends: &mut Vec<String>) {
    let mut address = String::new();
    let mut port = 0u16;
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        match f.number {
            2 => address = String::from_utf8_lossy(f.bytes).into_owned(),
            // SocketAddress.port_value = 3 (the port_specifier oneof);
            // named_port = 4 is a string alternative.
            3 => port = f.varint as u16,
            5 => port = String::from_utf8_lossy(f.bytes).parse().unwrap_or(0),
            _ => {}
        }
    }
    if !address.is_empty() && port != 0 {
        backends.push(format!("{address}:{port}"));
    }
}

/// A decoded Envoy `RouteConfiguration` subset.
#[derive(Debug, Clone, Default)]
pub struct EnvoyRouteConfig {
    /// Virtual hosts: (name, domains, routes as (prefix, cluster)).
    pub vhosts: Vec<EnvoyVirtualHost>,
}

/// A decoded Envoy `VirtualHost`.
#[derive(Debug, Clone, Default)]
pub struct EnvoyVirtualHost {
    /// Virtual host name.
    pub name: String,
    /// Domains (host match).
    pub domains: Vec<String>,
    /// Routes: (prefix, cluster name).
    pub routes: Vec<(String, String)>,
}

/// Decodes a standalone `ClusterLoadAssignment` (an EDS resource):
/// `cluster_name` = 1, `endpoints` = 2 → locality endpoints chain.
/// Returns `(cluster_name, backends)`.
#[must_use]
pub fn decode_cla(buf: &[u8]) -> Option<(String, Vec<String>)> {
    let mut name = String::new();
    let mut backends = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = pb::decode_field(&buf[pos..])?;
        pos += n;
        match f.number {
            1 => name = String::from_utf8_lossy(f.bytes).into_owned(),
            2 => decode_locality(f.bytes, &mut backends),
            _ => {}
        }
    }
    if name.is_empty() || backends.is_empty() {
        return None;
    }
    Some((name, backends))
}

/// Decodes an Envoy `RouteConfiguration` message.
#[must_use]
pub fn decode_route_config(buf: &[u8]) -> Option<EnvoyRouteConfig> {
    let mut out = EnvoyRouteConfig::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = pb::decode_field(&buf[pos..])?;
        pos += n;
        if f.number == 2 {
            out.vhosts.push(decode_virtual_host(f.bytes));
        }
    }
    Some(out)
}

fn decode_virtual_host(buf: &[u8]) -> EnvoyVirtualHost {
    let mut vh = EnvoyVirtualHost::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        match f.number {
            1 => vh.name = String::from_utf8_lossy(f.bytes).into_owned(),
            2 => vh
                .domains
                .push(String::from_utf8_lossy(f.bytes).into_owned()),
            3 => {
                if let Some((prefix, cluster)) = decode_route(f.bytes) {
                    vh.routes.push((prefix, cluster));
                }
            }
            _ => {}
        }
    }
    vh
}

/// `Route`: match.prefix (1.1) + route.cluster (2.1).
fn decode_route(buf: &[u8]) -> Option<(String, String)> {
    let mut prefix = String::new();
    let mut cluster = String::new();
    let mut pos = 0;
    while pos < buf.len() {
        let (f, n) = match pb::decode_field(&buf[pos..]) {
            Some(x) => x,
            None => break,
        };
        pos += n;
        match f.number {
            1 => {
                // RouteMatch: prefix = field 1.
                let mut mpos = 0;
                while mpos < f.bytes.len() {
                    let (mf, mn) = pb::decode_field(&f.bytes[mpos..])?;
                    mpos += mn;
                    if mf.number == 1 {
                        prefix = String::from_utf8_lossy(mf.bytes).into_owned();
                    }
                }
            }
            2 => {
                // RouteAction: cluster = field 1.
                let mut apos = 0;
                while apos < f.bytes.len() {
                    let (af, an) = pb::decode_field(&f.bytes[apos..])?;
                    apos += an;
                    if af.number == 1 {
                        cluster = String::from_utf8_lossy(af.bytes).into_owned();
                    }
                }
            }
            _ => {}
        }
    }
    if prefix.is_empty() || cluster.is_empty() {
        return None;
    }
    Some((prefix, cluster))
}

/// Converts an Envoy path prefix to a vane route pattern: `/api` →
/// `/api/*rest` (Envoy prefix semantics ≈ vane's prefix pattern);
/// exact-style prefixes ending without a trailing segment are treated
/// as prefix matches.
#[must_use]
pub fn envoy_prefix_to_pattern(prefix: &str) -> String {
    if prefix.ends_with('/') {
        format!("{prefix}*rest")
    } else {
        format!("{prefix}/*rest")
    }
}

/// Maps decoded Envoy resources into a snapshot (clusters by name;
/// routes from all virtual hosts, first domain = host match).
#[must_use]
pub fn map_snapshot(clusters: &[EnvoyCluster], route_config: &EnvoyRouteConfig) -> XdsSnapshot {
    let mut snapshot = XdsSnapshot {
        version: String::new(),
        clusters: std::collections::BTreeMap::new(),
        routes: Vec::new(),
    };
    for c in clusters {
        snapshot.clusters.insert(
            c.name.clone(),
            XdsCluster {
                backends: c.backends.clone(),
                compression: false,
                http2: c.http2,
                outlier: None,
            },
        );
    }
    for vh in &route_config.vhosts {
        let host = vh.domains.first().cloned();
        let host = match host.as_deref() {
            None => continue,
            Some("*") => None,
            Some(d) => Some(d.to_owned()),
        };
        for (prefix, cluster) in &vh.routes {
            snapshot.routes.push(XdsRoute {
                host: host.clone(),
                pattern: envoy_prefix_to_pattern(prefix),
                cluster: cluster.clone(),
                methods: Vec::new(),
                strip_prefix: None,
                priority: 0,
                allowed_spiffe_prefixes: Vec::new(),
            });
        }
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-encodes an Envoy Cluster: name + one socket-address
    /// endpoint + http2 options.
    #[test]
    fn decodes_cluster_with_backends_and_http2() {
        let mut cluster = Vec::new();
        pb::string_field(&mut cluster, 1, "shop");
        // load_assignment (4): endpoints(2) → locality(2) → lb(1) →
        // endpoint(1) → address(1) → socket_address(2){ address=2,
        // port_value=4 }.
        let mut sock = Vec::new();
        pb::string_field(&mut sock, 2, "10.0.0.7");
        // SocketAddress.port_value = 3 per envoy/config/core/v3/address.proto.
        pb::varint_field(&mut sock, 3, 8080);
        let mut address = Vec::new();
        // Address.socket_address = 1 (the address oneof).
        pb::message_field(&mut address, 1, &sock);
        let mut endpoint = Vec::new();
        pb::message_field(&mut endpoint, 1, &address);
        let mut lb = Vec::new();
        pb::message_field(&mut lb, 1, &endpoint);
        let mut locality = Vec::new();
        // LocalityLbEndpoints.lb_endpoints = 2.
        pb::message_field(&mut locality, 2, &lb);
        let mut cla = Vec::new();
        pb::message_field(&mut cla, 2, &locality);
        pb::message_field(&mut cluster, 33, &cla); // load_assignment = 33
        let mut http2 = Vec::new();
        pb::bool_field(&mut http2, 1, true);
        pb::message_field(&mut cluster, 20, &http2);

        let decoded = decode_cluster(&cluster).expect("decode");
        assert_eq!(decoded.name, "shop");
        assert_eq!(decoded.backends, vec!["10.0.0.7:8080"]);
        assert!(decoded.http2);
    }

    /// Hand-encodes a RouteConfiguration: one vhost, one prefix route.
    #[test]
    fn decodes_route_config_with_prefix_match() {
        let mut rc = Vec::new();
        let mut vh = Vec::new();
        pb::string_field(&mut vh, 1, "shop-vh");
        pb::string_field(&mut vh, 2, "shop.example.com");
        let mut route_match = Vec::new();
        pb::string_field(&mut route_match, 1, "/api");
        let mut route_action = Vec::new();
        pb::string_field(&mut route_action, 1, "shop");
        let mut route = Vec::new();
        pb::message_field(&mut route, 1, &route_match);
        pb::message_field(&mut route, 2, &route_action);
        pb::message_field(&mut vh, 3, &route);
        pb::message_field(&mut rc, 2, &vh);

        let decoded = decode_route_config(&rc).expect("decode");
        assert_eq!(decoded.vhosts.len(), 1);
        assert_eq!(decoded.vhosts[0].domains, vec!["shop.example.com"]);
        assert_eq!(
            decoded.vhosts[0].routes,
            vec![("/api".into(), "shop".into())]
        );
    }

    /// Prefix → pattern conversion.
    #[test]
    fn prefix_pattern_conversion() {
        assert_eq!(envoy_prefix_to_pattern("/api"), "/api/*rest");
        assert_eq!(envoy_prefix_to_pattern("/api/"), "/api/*rest");
    }

    /// The full mapping: clusters + routes → snapshot shape.
    #[test]
    fn maps_envoy_resources_to_snapshot() {
        let cluster = EnvoyCluster {
            name: "shop".into(),
            backends: vec!["10.0.0.7:8080".into()],
            http2: true,
        };
        let rc = EnvoyRouteConfig {
            vhosts: vec![EnvoyVirtualHost {
                name: "shop-vh".into(),
                domains: vec!["shop.example.com".into()],
                routes: vec![("/api".into(), "shop".into())],
            }],
        };
        let snap = map_snapshot(&[cluster], &rc);
        assert_eq!(
            snap.clusters.get("shop").expect("cluster").backends,
            vec!["10.0.0.7:8080"]
        );
        assert!(snap.clusters.get("shop").expect("cluster").http2);
        assert_eq!(snap.routes.len(), 1);
        assert_eq!(snap.routes[0].pattern, "/api/*rest");
        assert_eq!(snap.routes[0].cluster, "shop");
        assert_eq!(snap.routes[0].host.as_deref(), Some("shop.example.com"));
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    /// The exact bytes go-control-plane v0.14 sent on the wire.
    #[test]
    fn decodes_real_gcpx_cluster() {
        let wire: Vec<u8> = vec![
            0x0a, 0x04, 0x73, 0x68, 0x6f, 0x70, 0x22, 0x02, 0x08, 0x02, 0x8a, 0x02, 0x1f, 0x0a,
            0x04, 0x73, 0x68, 0x6f, 0x70, 0x12, 0x17, 0x12, 0x15, 0x0a, 0x13, 0x0a, 0x11, 0x0a,
            0x0f, 0x12, 0x09, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x18, 0xa1,
            0x8d, 0x01,
        ];
        let c = decode_cluster(&wire).expect("decode");
        assert_eq!(c.name, "shop");
        assert_eq!(c.backends, vec!["127.0.0.1:18081"]);
    }
}

#[cfg(test)]
mod cla_trace {
    use super::*;
    #[test]
    fn decode_cla_direct() {
        let cla: Vec<u8> = vec![
            0x0a, 0x04, 0x73, 0x68, 0x6f, 0x70, 0x12, 0x17, 0x12, 0x15, 0x0a, 0x13, 0x0a, 0x11,
            0x0a, 0x0f, 0x12, 0x09, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x18,
            0xa1, 0x8d, 0x01,
        ];
        let mut backends = Vec::new();
        decode_load_assignment(&cla, &mut backends);
        eprintln!("CLA backends: {backends:?}");
        assert_eq!(backends, vec!["127.0.0.1:18081"]);

        let mut wire: Vec<u8> = vec![
            0x0a, 0x04, 0x73, 0x68, 0x6f, 0x70, 0x22, 0x02, 0x08, 0x02, 0x8a, 0x02, 0x1f,
        ];
        wire.extend_from_slice(&cla);
        let mut pos = 0;
        while pos < wire.len() {
            let (f, n) = pb::decode_field(&wire[pos..]).expect("field");
            pos += n;
            eprintln!("field {} wire {:?} len {}", f.number, f.wire, f.bytes.len());
        }
        assert_eq!(pos, wire.len());
    }
}
