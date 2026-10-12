//! PROXY protocol (v1 text, v2 binary) header parsing — the client-IP
//! preservation contract for listeners deployed behind an L4 load
//! balancer (AWS NLB, GCP, HAProxy, Cloudflare Spectrum).
//!
//! Trust is a CONFIG property, not a wire property: a PROXY header is
//! only parsed on listeners whose config enables it, because anyone who
//! can open a TCP connection can send one. When enabled, the header is
//! REQUIRED — the first bytes of every connection must be a valid
//! header ("do not mix modes"); anything else closes the connection
//! rather than guessing.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// Why a PROXY header could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyHeaderError {
    /// The first bytes are not a v1 line (`PROXY `) or the v2 signature
    /// — with the header required, this connection cannot be served.
    NotAProxyHeader,
    /// The header declares more payload than its family allows, or the
    /// v1 line exceeds 107 bytes, or the family/command bits are
    /// reserved values. Malformed input closes the connection.
    Malformed,
}

/// The v2 binary signature: 12 constant bytes.
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// A v1 line is `PROXY ... CRLF`, at most 107 bytes per the spec.
const V1_MAX: usize = 107;
/// v2: 16-byte fixed header + up to 64 KiB of address+TLV payload. The
/// spec's practical headers are ≤ 232 bytes; the bound here only exists
/// so a peer cannot pin memory with a huge declared length.
const V2_MAX_PAYLOAD: usize = 4096;

/// Parses a PROXY protocol header from the START of `data`.
///
/// - `Ok(None)` — not enough bytes yet; call again with more.
/// - `Ok(Some((source, header_len)))` — header consumed; `source` is
///   the connection's true client address (the destination is vane and
///   is ignored) and `header_len` bytes must be stripped from the
///   stream. A v2 LOCAL header (health checks, unproxied internal
///   connections) parses successfully but reports the REAL socket peer
///   via the `source` the caller passes in — see [`ProxyOutcome`].
/// - `Err(_)` — malformed or not a PROXY header: close the connection.
///
/// # Errors
/// [`ProxyHeaderError::NotAProxyHeader`] when the first bytes match
/// neither version; [`ProxyHeaderError::Malformed`] when a header that
/// started correctly violates the format.
pub fn parse(data: &[u8]) -> Result<Option<ProxyOutcome>, ProxyHeaderError> {
    if data.is_empty() {
        return Ok(None);
    }
    // Only v2 headers begin with CRLF (v1 lines start with "PROXY "):
    // dispatch on the first byte, then wait or fail on the signature
    // prefix already in hand. Deciding v1-vs-v2 on more than one byte
    // would misread a v2 prefix ("\r\n" is an empty v1 "line").
    if data[0] == 0x0D {
        let n = data.len().min(V2_SIGNATURE.len());
        if data[..n] != V2_SIGNATURE[..n] {
            return Err(ProxyHeaderError::NotAProxyHeader);
        }
        if data.len() < V2_SIGNATURE.len() {
            return Ok(None);
        }
        return parse_v2(data);
    }
    parse_v1(data)
}

/// A successfully parsed header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyOutcome {
    /// Address to attribute the connection to. `None` for a v2 LOCAL
    /// header (health checks, unproxied internal connections): no
    /// address is claimed, so the caller keeps the REAL socket peer.
    pub source: Option<SocketAddr>,
    /// Bytes the header occupied — strip them from the stream.
    pub header_len: usize,
}

fn parse_v1(data: &[u8]) -> Result<Option<ProxyOutcome>, ProxyHeaderError> {
    // The line is complete only at CRLF. Until then, wait — bounded.
    let Some(crlf) = data.windows(2).position(|w| w == b"\r\n") else {
        if data.len() > V1_MAX {
            return Err(ProxyHeaderError::Malformed);
        }
        // A v1 line always starts with "PROXY "; if the prefix already
        // diverges, this was never a proxy header.
        let sig = b"PROXY ";
        let comparable = data.len().min(sig.len());
        if data[..comparable] != sig[..comparable] {
            return Err(ProxyHeaderError::NotAProxyHeader);
        }
        return Ok(None);
    };
    let line = &data[..crlf];
    if line.len() > V1_MAX {
        return Err(ProxyHeaderError::Malformed);
    }
    // A complete line that is not a v1 header at all (a plain request
    // arrived on a header-required listener): name the condition
    // precisely rather than hiding it behind "malformed".
    if !line.starts_with(b"PROXY ") {
        return Err(ProxyHeaderError::NotAProxyHeader);
    }
    let Ok(text) = std::str::from_utf8(line) else {
        return Err(ProxyHeaderError::Malformed);
    };
    let mut parts = text.split(' ');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("PROXY"), Some(proto), Some(src), Some(_dst), Some(sport), Some(dport))
            if parts.next().is_none() =>
        {
            if !matches!(proto, "TCP4" | "TCP6") {
                // TCP6, TCP4, or the UNKNOWN special case (v1 has no
                // UNKNOWN with addresses; "PROXY UNKNOWN" closes the
                // address fields and carries no client information —
                // treat it as not-a-header rather than guessing).
                return Err(ProxyHeaderError::Malformed);
            }
            let port: u16 = sport.parse().map_err(|_| ProxyHeaderError::Malformed)?;
            if dport.parse::<u16>().is_err() {
                return Err(ProxyHeaderError::Malformed);
            }
            let source = if proto == "TCP4" {
                let ip: Ipv4Addr = src.parse().map_err(|_| ProxyHeaderError::Malformed)?;
                SocketAddr::from((ip, port))
            } else {
                let ip: Ipv6Addr = src.parse().map_err(|_| ProxyHeaderError::Malformed)?;
                SocketAddr::from((ip, port))
            };
            Ok(Some(ProxyOutcome {
                source: Some(source),
                header_len: crlf + 2,
            }))
        }
        _ => Err(ProxyHeaderError::Malformed),
    }
}

fn parse_v2(data: &[u8]) -> Result<Option<ProxyOutcome>, ProxyHeaderError> {
    if data.len() < 16 {
        return Ok(None);
    }
    let ver_cmd = data[12];
    let fam_proto = data[13];
    let len = u16::from_be_bytes([data[14], data[15]]) as usize;
    if ver_cmd >> 4 != 2 {
        // Only version 2 is spoken here.
        return Err(ProxyHeaderError::Malformed);
    }
    if len > V2_MAX_PAYLOAD {
        return Err(ProxyHeaderError::Malformed);
    }
    if data.len() < 16 + len {
        return Ok(None);
    }
    let payload = &data[16..16 + len];
    let header_len = 16 + len;
    let command = ver_cmd & 0x0F;
    // LOCAL (0): the connection is not proxied — health checks and
    // internal traffic. The header carries no addresses to trust.
    if command == 0 {
        return Ok(Some(ProxyOutcome {
            source: None,
            header_len,
        }));
    }
    // PROXY (1) is the only other defined command.
    if command != 1 {
        return Err(ProxyHeaderError::Malformed);
    }
    let source = match fam_proto {
        // TCP over IPv4: 4 + 4 + 2 + 2.
        0x11 => {
            if payload.len() < 12 {
                return Err(ProxyHeaderError::Malformed);
            }
            let ip = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
            let port = u16::from_be_bytes([payload[8], payload[9]]);
            SocketAddr::from((ip, port))
        }
        // TCP over IPv6: 16 + 16 + 2 + 2.
        0x21 => {
            if payload.len() < 36 {
                return Err(ProxyHeaderError::Malformed);
            }
            let octets: [u8; 16] = payload[..16].try_into().expect("16 checked");
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([payload[32], payload[33]]);
            SocketAddr::from((ip, port))
        }
        // AF_UNIX (0x31/0x32) has no SocketAddr here; UNSPEC (0x00)
        // carries no addresses. Both are outside the proxy's use.
        _ => return Err(ProxyHeaderError::Malformed),
    };
    Ok(Some(ProxyOutcome {
        source: Some(source),
        header_len,
    }))
}

#[cfg(test)]
mod pp_tests {
    use super::*;

    fn v1(line: &str) -> Vec<u8> {
        let mut out = line.as_bytes().to_vec();
        out.extend_from_slice(b"\r\n");
        out
    }

    fn v2(fam: u8, src: &[u8], sport: u16) -> Vec<u8> {
        // Payload per spec: src addr + dst addr + src port + dst port
        // (same width for both addresses within a family). The family
        // is the HIGH nibble: 0x11 = TCP/IPv4 (4-byte addrs), 0x21 =
        // TCP/IPv6 (16-byte addrs).
        let alen = if fam >> 4 == 1 { 4 } else { 16 };
        let mut payload = src.to_vec();
        payload.extend_from_slice(&[0u8; 16][..alen]); // dst addr
        payload.extend_from_slice(&sport.to_be_bytes());
        payload.extend_from_slice(&8080u16.to_be_bytes()); // dst port
        let mut out = V2_SIGNATURE.to_vec();
        out.push(0x21); // v2, PROXY
        out.push(fam);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        out.extend_from_slice(&payload);
        out
    }

    #[test]
    fn v1_tcp4_parses_source_and_length() {
        let wire = v1("PROXY TCP4 203.0.113.7 198.51.100.9 56789 443");
        let out = parse(&wire).expect("parse").expect("complete");
        assert_eq!(
            out.source,
            Some("203.0.113.7:56789".parse::<SocketAddr>().expect("addr"))
        );
        assert_eq!(out.header_len, wire.len());
    }

    #[test]
    fn v1_tcp6_parses() {
        let wire = v1("PROXY TCP6 2001:db8::1 2001:db8::2 4444 80");
        let out = parse(&wire).expect("parse").expect("complete");
        assert_eq!(
            out.source,
            Some("[2001:db8::1]:4444".parse::<SocketAddr>().expect("addr"))
        );
    }

    #[test]
    fn v1_splits_across_reads_and_prefix_divergence_is_an_error() {
        let wire = v1("PROXY TCP4 203.0.113.7 198.51.100.9 56789 443");
        // Feed one byte at a time: always "wait", never a wrong answer.
        for n in 1..wire.len() {
            assert_eq!(parse(&wire[..n]).expect("wait"), None, "at {n}");
        }
        assert!(parse(&wire).expect("complete").is_some());
        // A plain HTTP request is not a proxy header: explicit error.
        assert_eq!(
            parse(b"GET / HTTP/1.1\r\n"),
            Err(ProxyHeaderError::NotAProxyHeader)
        );
    }

    #[test]
    fn v1_overlong_line_is_malformed() {
        let long = format!("PROXY TCP4 {} bar 1 2", "1.2.3.4 ".repeat(20));
        assert_eq!(
            parse(v1(&long).as_slice()),
            Err(ProxyHeaderError::Malformed)
        );
    }

    #[test]
    fn v1_bad_family_and_garbage_fields_are_malformed() {
        assert_eq!(
            parse(v1("PROXY UDP 1.2.3.4 5.6.7.8 1 2").as_slice()),
            Err(ProxyHeaderError::Malformed)
        );
        assert_eq!(
            parse(v1("PROXY TCP4 not-an-ip 5.6.7.8 1 2").as_slice()),
            Err(ProxyHeaderError::Malformed)
        );
        assert_eq!(
            parse(v1("PROXY TCP4 1.2.3.4 5.6.7.8 notaport 2").as_slice()),
            Err(ProxyHeaderError::Malformed)
        );
    }

    #[test]
    fn v2_tcp4_and_tcp6_parse() {
        let src: [u8; 4] = [203, 0, 113, 7];
        let wire = v2(0x11, &src, 56789);
        eprintln!("V2DBG len={} wire={wire:02x?}", wire.len());
        let out = parse(&wire).expect("parse").expect("done");
        assert_eq!(
            out.source,
            Some("203.0.113.7:56789".parse::<SocketAddr>().expect("addr"))
        );

        let src6: [u8; 16] = std::array::from_fn(|i| (i % 251) as u8);
        let out = parse(&v2(0x21, &src6, 443)).expect("parse").expect("done");
        let ip = Ipv6Addr::from(src6);
        assert_eq!(out.source, Some(SocketAddr::from((ip, 443))));
    }

    #[test]
    fn v2_splits_across_reads() {
        let wire = v2(0x11, &[10, 0, 0, 1], 8080);
        for n in 0..wire.len() {
            assert_eq!(parse(&wire[..n]).expect("wait"), None, "at {n}");
        }
        assert!(parse(&wire).expect("complete").is_some());
    }

    #[test]
    fn v2_local_command_reports_the_placeholder() {
        // Health checks: v2, LOCAL, no addresses to trust.
        let mut wire = V2_SIGNATURE.to_vec();
        wire.push(0x20); // v2, LOCAL
        wire.push(0x00); // UNSPEC
        wire.extend_from_slice(&0u16.to_be_bytes());
        let out = parse(&wire).expect("parse").expect("done");
        assert_eq!(out.source, None, "LOCAL claims no address");
        assert_eq!(out.header_len, 16);
    }

    #[test]
    fn v2_reserved_command_and_family_are_malformed() {
        let mut wire = V2_SIGNATURE.to_vec();
        wire.push(0x27); // v2, command 7 (reserved)
        wire.push(0x11);
        wire.extend_from_slice(&4u16.to_be_bytes());
        wire.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(parse(&wire), Err(ProxyHeaderError::Malformed));

        let mut wire = V2_SIGNATURE.to_vec();
        wire.push(0x21);
        wire.push(0x31); // AF_UNIX: no SocketAddr mapping here
        wire.extend_from_slice(&108u16.to_be_bytes());
        wire.extend_from_slice(&[0u8; 108]);
        assert_eq!(parse(&wire), Err(ProxyHeaderError::Malformed));
    }

    #[test]
    fn v2_overlong_declared_payload_is_malformed() {
        let mut wire = V2_SIGNATURE.to_vec();
        wire.push(0x21);
        wire.push(0x11);
        wire.extend_from_slice(&(V2_MAX_PAYLOAD as u16 + 1).to_be_bytes());
        wire.extend_from_slice(&[0u8; 8]);
        assert_eq!(parse(&wire), Err(ProxyHeaderError::Malformed));
    }
}
