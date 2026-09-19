//! HPACK header compression (RFC 7541) — decoder, encoder, integer and
//! string codecs, static + dynamic tables.
//!
//! Design notes:
//! - Decoder is allocation-bounded: dynamic table is size-capped per the
//!   negotiated `SETTINGS_HEADER_TABLE_SIZE`; decoded headers allocate
//!   one `Vec` per name/value (arena-backed later).
//! - Encoder v1 is *stateless with respect to the dynamic table*: exact
//!   matches against the static table become indexed fields; everything
//!   else is literal-without-indexing. Fully wire-valid; incremental
//!   indexing lands with the stream layer (tracked).
//! - All decode paths are panic-free on arbitrary bytes (fuzz target in
//!   `fuzz/fuzz_targets/hpack.rs`).

/// Static table (RFC 7541 Appendix A), indices 1..=61.
const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// One decoded header field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Lowercased-on-the-wire name as received (bytes — HTTP/2 does not
    /// mandate lowercase for values obtained via HPACK, but the protocol
    /// rejects uppercase).
    pub name: Vec<u8>,
    /// Field value.
    pub value: Vec<u8>,
}

/// HPACK decode/encode errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HpackError {
    /// Buffer ended mid-instruction.
    Truncated,
    /// Index 0 or index beyond static+dynamic tables.
    InvalidIndex,
    /// Integer overflow or invalid continuation.
    InvalidInteger,
    /// Invalid string literal (bad huffman/padding).
    InvalidString,
    /// Dynamic table size update beyond the protocol maximum.
    TableSizeViolation,
    /// Input contained trailing bytes after the block.
    TrailingBytes,
}

/// Dynamic table: newest entries at the front (index 62 = front).
struct DynamicTable {
    entries: std::collections::VecDeque<(Vec<u8>, Vec<u8>)>,
    size: usize,
    max_size: usize,
}

impl DynamicTable {
    fn new(max_size: u32) -> Self {
        Self {
            entries: std::collections::VecDeque::new(),
            size: 0,
            max_size: max_size as usize,
        }
    }

    fn entry(&self, index: u64) -> Option<(&[u8], &[u8])> {
        // index >= 62; 62 is the newest (front).
        let offset = index - 62;
        self.entries
            .get(offset as usize)
            .map(|(n, v)| (n.as_slice(), v.as_slice()))
    }

    fn insert(&mut self, name: &[u8], value: &[u8]) {
        let entry_size = name.len() + value.len() + 32;
        self.entries.push_front((name.to_vec(), value.to_vec()));
        self.size += entry_size;
        while self.size > self.max_size {
            let Some((n, v)) = self.entries.pop_back() else {
                break;
            };
            self.size -= n.len() + v.len() + 32;
        }
        // An entry larger than the whole table empties it (RFC 7541 §4.4).
        if entry_size > self.max_size {
            self.entries.clear();
            self.size = 0;
        }
    }

    fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        while self.size > self.max_size {
            let Some((n, v)) = self.entries.pop_back() else {
                break;
            };
            self.size -= n.len() + v.len() + 32;
        }
    }
}

/// Decodes a prefix-coded integer (RFC 7541 §5.1).
///
/// `prefix_bits` is the prefix length N (1..=8); the high `8-N` bits of
/// the first byte are flags consumed by the caller.
fn decode_int(buf: &[u8], pos: &mut usize, prefix_bits: u8) -> Result<u64, HpackError> {
    let mask = (1u16 << prefix_bits) - 1;
    let Some(&first) = buf.get(*pos) else {
        return Err(HpackError::Truncated);
    };
    *pos += 1;
    let mut value = u64::from(first & (mask as u8));
    if value < u64::from(mask) {
        return Ok(value);
    }
    // Continuation bytes: 7 bits each, MSB = more. Checked arithmetic:
    // malformed inputs can carry arbitrarily long chains (a DoS vector
    // if this overflowed — found by the hpack fuzz target).
    let mut shift = 0u32;
    loop {
        let Some(&b) = buf.get(*pos) else {
            return Err(HpackError::Truncated);
        };
        *pos += 1;
        // Checked arithmetic: malformed inputs can carry arbitrarily long
        // continuation chains (a DoS vector if this overflowed — found
        // by the hpack fuzz target).
        let contribution = u64::from(b & 0x7f);
        let shifted = contribution
            .checked_shl(shift)
            .ok_or(HpackError::InvalidInteger)?;
        value = value
            .checked_add(shifted)
            .ok_or(HpackError::InvalidInteger)?;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(HpackError::InvalidInteger);
        }
    }
    Ok(value)
}

/// Encodes `value` as a prefix-coded integer; the high `8-N` bits of the
/// first byte are set to `flags`.
fn encode_int(out: &mut Vec<u8>, value: u64, prefix_bits: u8, flags: u8) {
    let mask = (1u16 << prefix_bits) - 1;
    if value < u64::from(mask) {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | mask as u8);
    let mut rest = value - u64::from(mask);
    while rest >= 128 {
        out.push((rest & 0x7f) as u8 | 0x80);
        rest >>= 7;
    }
    out.push(rest as u8);
}

/// Decodes a string literal (Huffman or raw) into `out`; the caller
/// tracks the new `pos`.
fn decode_string_into(buf: &[u8], pos: &mut usize, out: &mut Vec<u8>) -> Result<(), HpackError> {
    let Some(&first) = buf.get(*pos) else {
        return Err(HpackError::Truncated);
    };
    let huffman = first & 0x80 != 0;
    let mut len_pos = *pos;
    let len = decode_int(buf, &mut len_pos, 7)?;
    let Some(slice) = buf.get(len_pos..len_pos + len as usize) else {
        return Err(HpackError::Truncated);
    };
    if huffman {
        crate::h2::huffman::decode(slice, out).map_err(|_| HpackError::InvalidString)?;
    } else {
        out.extend_from_slice(slice);
    }
    *pos = len_pos + len as usize;
    Ok(())
}

/// Decodes a value string at `pos`; returns the decoded bytes.
fn decode_string_value(
    buf: &[u8],
    pos: &mut usize,
    out: &mut Vec<u8>,
) -> Result<Vec<u8>, HpackError> {
    let start = out.len();
    decode_string_into(buf, pos, out)?;
    Ok(out.split_off(start))
}

/// HPACK decoder: owns the dynamic table across header blocks.
pub struct HpackDecoder {
    dynamic: DynamicTable,
    /// Protocol maximum (our SETTINGS_HEADER_TABLE_SIZE).
    protocol_max: u32,
}

impl HpackDecoder {
    /// New decoder with `max_table_size` as both the protocol maximum
    /// and the current table size.
    #[must_use]
    pub fn new(max_table_size: u32) -> Self {
        Self {
            dynamic: DynamicTable::new(max_table_size),
            protocol_max: max_table_size,
        }
    }

    /// Applies a peer `SETTINGS_HEADER_TABLE_SIZE` update (their new max
    /// for blocks WE send — kept for symmetry; our encoder has no table).
    /// Size updates in header blocks must not exceed the protocol max.
    pub fn set_max_table_size(&mut self, size: u32) {
        self.dynamic.set_max_size(size as usize);
    }

    /// Decodes one complete header block into headers.
    ///
    /// # Errors
    /// [`HpackError`] on any malformed input (never panics on arbitrary
    /// bytes).
    pub fn decode(&mut self, buf: &[u8]) -> Result<Vec<Header>, HpackError> {
        let mut headers = Vec::new();
        let mut out = Vec::new(); // string scratch
        let mut pos = 0usize;

        while pos < buf.len() {
            out.clear();
            let b = buf[pos];

            if b & 0x80 != 0 {
                // 1xxxxxxx: indexed header field.
                let index = decode_int(buf, &mut pos, 7)?;
                if index == 0 {
                    return Err(HpackError::InvalidIndex);
                }
                let (name, value) = self.lookup(index)?;
                headers.push(Header {
                    name: name.to_vec(),
                    value: value.to_vec(),
                });
            } else if b & 0xc0 == 0x40 {
                // 01xxxxxx: literal with incremental indexing.
                let index = decode_int(buf, &mut pos, 6)?;
                let name = self.decode_name(buf, &mut pos, index, &mut out)?;
                let value = decode_string_value(buf, &mut pos, &mut out)?;
                self.dynamic.insert(&name, &value);
                headers.push(Header { name, value });
            } else if b & 0xe0 == 0x20 {
                // 001xxxxx: dynamic table size update.
                let size = decode_int(buf, &mut pos, 5)?;
                if size > u64::from(self.protocol_max) {
                    return Err(HpackError::TableSizeViolation);
                }
                self.dynamic.set_max_size(size as usize);
            } else {
                // 0000xxxx / 0001xxxx: literal without indexing / never
                // indexed. Same wire format; never-indexed semantics are
                // a policy hint for intermediaries (we never store).
                let index = decode_int(buf, &mut pos, 4)?;
                let name = self.decode_name(buf, &mut pos, index, &mut out)?;
                let value = decode_string_value(buf, &mut pos, &mut out)?;
                headers.push(Header { name, value });
            }
        }
        Ok(headers)
    }

    fn lookup(&self, index: u64) -> Result<(&[u8], &[u8]), HpackError> {
        if index == 0 || index > 61 + u64::from(u32::MAX) {
            return Err(HpackError::InvalidIndex);
        }
        if index <= 61 {
            let (n, v) = STATIC_TABLE[(index - 1) as usize];
            return Ok((n.as_bytes(), v.as_bytes()));
        }
        self.dynamic.entry(index).ok_or(HpackError::InvalidIndex)
    }

    fn decode_name(
        &self,
        buf: &[u8],
        pos: &mut usize,
        index: u64,
        out: &mut Vec<u8>,
    ) -> Result<Vec<u8>, HpackError> {
        if index == 0 {
            decode_string_value(buf, pos, out)
        } else {
            let (name, _) = self.lookup(index)?;
            Ok(name.to_vec())
        }
    }
}

/// HPACK encoder: stateless (static-table indexed fields + literal
/// without indexing). Wire-valid against any conformant decoder.
#[derive(Debug, Default)]
pub struct HpackEncoder;

impl HpackEncoder {
    /// New stateless encoder.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Encodes headers into `out`.
    pub fn encode(&mut self, headers: &[(Vec<u8>, Vec<u8>)], out: &mut Vec<u8>) {
        for (name, value) in headers {
            if let Some(index) = static_exact_match(name, value) {
                encode_int(out, u64::from(index), 7, 0x80);
                continue;
            }
            // Literal without indexing, name by static reference where
            // possible.
            let name_index = static_name_match(name);
            match name_index {
                Some(i) => {
                    // Literal without indexing, name by static index.
                    encode_int(out, u64::from(i), 4, 0x00);
                    encode_string(out, value, false);
                }
                None => {
                    // Literal without indexing, literal name.
                    encode_int(out, 0, 4, 0x00);
                    encode_string(out, name, false);
                    encode_string(out, value, false);
                }
            }
        }
    }
}

fn static_exact_match(name: &[u8], value: &[u8]) -> Option<u32> {
    STATIC_TABLE
        .iter()
        .position(|(n, v)| n.as_bytes() == name && v.as_bytes() == value)
        .map(|i| i as u32 + 1)
}

fn static_name_match(name: &[u8]) -> Option<u32> {
    STATIC_TABLE
        .iter()
        .position(|(n, _)| n.as_bytes() == name)
        .map(|i| i as u32 + 1)
}

/// Encodes a string literal: raw (Huffman off) — huffman-on is a
/// refinement tracked for the perf phase.
fn encode_string(out: &mut Vec<u8>, s: &[u8], huffman: bool) {
    debug_assert!(!huffman, "huffman encode path lands with stream layer");
    encode_int(out, s.len() as u64, 7, 0x00);
    out.extend_from_slice(s);
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 7541 C.1.1: 10 with 5-bit prefix.
    #[test]
    fn int_10_5bit() {
        let mut pos = 0;
        assert_eq!(decode_int(&[0x0a], &mut pos, 5).unwrap(), 10);
        assert_eq!(pos, 1);
    }

    // RFC 7541 C.1.2: 1337 with 5-bit prefix.
    #[test]
    fn int_1337_5bit() {
        let mut pos = 0;
        assert_eq!(decode_int(&[0x1f, 0x9a, 0x0a], &mut pos, 5).unwrap(), 1337);
        assert_eq!(pos, 3);
        let mut out = Vec::new();
        encode_int(&mut out, 1337, 5, 0x1f & !0x1f); // flags zeroed then set
        // canonical: flags mask byte = 0x1f prefix with value bits
        let mut out2 = Vec::new();
        encode_int(&mut out2, 1337, 5, 0x00);
        assert_eq!(out, vec![0x1f, 0x9a, 0x0a]);
        assert_eq!(out2, vec![0x1f, 0x9a, 0x0a]);
    }

    // RFC 7541 C.1.3: 42 with 8-bit prefix.
    #[test]
    fn int_42_8bit() {
        let mut pos = 0;
        assert_eq!(decode_int(&[0x2a], &mut pos, 8).unwrap(), 42);
    }

    #[test]
    fn int_roundtrip_wide() {
        for v in [
            0u64,
            1,
            62,
            63,
            64,
            126,
            127,
            128,
            4096,
            u32::MAX as u64,
            u64::from(u32::MAX) + 1,
        ] {
            for bits in [4, 5, 6, 7, 8] {
                let mut out = Vec::new();
                encode_int(&mut out, v, bits, 0);
                let mut pos = 0;
                assert_eq!(
                    decode_int(&out, &mut pos, bits).unwrap(),
                    v,
                    "v={v} bits={bits}"
                );
            }
        }
    }

    // RFC 7541 C.2.1/C.2.2-ish: raw vs huffman string shape.
    #[test]
    fn string_raw_shape() {
        let mut pos = 0;
        let mut out = Vec::new();
        // "www.example.com" raw: 0x0f 0x00? No — 15 fits 7-bit prefix: 0x0f then bytes.
        let block = [
            0x0f, b'w', b'w', b'w', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm',
        ];
        decode_string_into(&block, &mut pos, &mut out).unwrap();
        assert_eq!(out, b"www.example.com");
        assert_eq!(pos, block.len());
    }

    // Huffman string via the decode path used by header blocks: the
    // length prefix counts ENCODED bytes, then the payload follows.
    #[test]
    fn string_huffman_via_decode_string_into() {
        let mut encoded = Vec::new();
        crate::h2::huffman::encode(b"www.example.com", &mut encoded);
        assert_eq!(encoded.len(), 12);

        let mut block = vec![0x80 | encoded.len() as u8];
        block.extend_from_slice(&encoded);

        let mut pos = 0;
        let mut out = Vec::new();
        decode_string_into(&block, &mut pos, &mut out).unwrap();
        assert_eq!(out, b"www.example.com");
        assert_eq!(pos, block.len());
    }

    // RFC 7541 C.6.1 huffman sample: "www.example.com" →
    // f1 e3 c2 e5 f2 3a 6b a0 ab 90 f4 ff (12 bytes, prefix 0x8c).
    #[test]
    fn string_huffman_www_example() {
        let mut pos = 0;
        let mut out = Vec::new();
        let block = [
            0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        decode_string_into(&block, &mut pos, &mut out).unwrap();
        assert_eq!(out, b"www.example.com");
    }

    /// Full C.3-style block through the public API with assertions per
    /// header (kept loose: exact vectors are in the RFC text above).
    #[test]
    fn decode_routed_request_headers() {
        let mut decoder = HpackDecoder::new(4096);
        // :method GET (82), :scheme https (87), :path / (84)
        let block = [0x82, 0x87, 0x84];
        let headers = decoder.decode(&block).unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].value, b"GET");
        assert_eq!(headers[1].value, b"https");
        assert_eq!(headers[2].value, b"/");
    }

    /// Dynamic table: literal with incremental indexing inserts and can
    /// be recalled by its assigned index (62 = first inserted).
    #[test]
    fn dynamic_table_indexing() {
        let mut decoder = HpackDecoder::new(4096);
        // Literal with incremental indexing, new name + value:
        // 0x40, name "custom-key", value "custom-header" (RFC C.4.1).
        let block: Vec<u8> = [
            0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e', b'y', 0x0d, b'c',
            b'u', b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
        ]
        .to_vec();
        let headers = decoder.decode(&block).unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b"custom-key");
        assert_eq!(headers[0].value, b"custom-header");

        // Indexed reference to the new entry: index 62.
        let headers2 = decoder.decode(&[0xbe]).unwrap();
        assert_eq!(headers2.len(), 1);
        assert_eq!(headers2[0].name, b"custom-key");
        assert_eq!(headers2[0].value, b"custom-header");
    }

    /// Table size update eviction (RFC C.5/C.6 dynamics).
    #[test]
    fn table_size_update_evicts() {
        let mut decoder = HpackDecoder::new(4096);
        let block: Vec<u8> = [0x40, 0x01, b'a', 0x01, b'b'].to_vec();
        decoder.decode(&block).unwrap();
        assert_eq!(decoder.dynamic.entries.len(), 1);

        // Shrink to 0 → table empties; index 62 now invalid.
        let update = [0x20]; // size update to 0 (5-bit prefix int 0, flags 001)
        decoder.decode(&update).unwrap();
        assert!(decoder.decode(&[0xbe]).is_err());
    }

    /// Oversize table update violates the protocol max.
    #[test]
    fn table_size_update_over_max_is_error() {
        let mut decoder = HpackDecoder::new(4096);
        // Update to 4097: 0x3f (prefix 31) + continuation bytes.
        let mut pos = 0;
        let v = decode_int(&[0x3f, 0xe2, 0x1f], &mut pos, 5).unwrap();
        assert_eq!(v, 4097); // sanity on the encoding
        assert!(decoder.decode(&[0x3f, 0xe2, 0x1f]).is_err());
    }

    /// Encoder roundtrip: static-exact becomes indexed; literals encode
    /// without indexing and decode to identical pairs.
    #[test]
    fn encoder_roundtrip() {
        let mut enc = HpackEncoder::new();
        let headers: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b":method".to_vec(), b"GET".to_vec()), // static exact → indexed
            (b"x-custom".to_vec(), b"1".to_vec()),  // literal
            (b"content-length".to_vec(), b"42".to_vec()), // name indexed, value literal
        ];
        let mut block = Vec::new();
        enc.encode(&headers, &mut block);

        let mut decoder = HpackDecoder::new(4096);
        let decoded = decoder.decode(&block).unwrap();
        assert_eq!(decoded.len(), 3);
        for (want, got) in headers.iter().zip(&decoded) {
            assert_eq!(want, &(got.name.clone(), got.value.clone()));
        }
    }

    /// Arbitrary garbage must never panic (mirror of the fuzz target).
    #[test]
    fn decode_garbage_never_panics() {
        let mut decoder = HpackDecoder::new(4096);
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for len in 0..600u64 {
            let mut data = Vec::with_capacity(len as usize);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                data.push(x as u8);
            }
            let _ = decoder.decode(&data);
        }
    }

    /// Dynamic table eviction honors the size cap.
    #[test]
    fn dynamic_eviction_respects_cap() {
        let mut decoder = HpackDecoder::new(4096);
        // Insert 100 headers of ~100 bytes each: total far above cap.
        for i in 0..100u8 {
            let mut block = vec![0x40, 0x20];
            block.extend_from_slice(&[b'k'; 32]);
            block.push(0x20);
            block.extend_from_slice(&[b'v'; 31]);
            block.push(i);
            decoder.decode(&block).unwrap();
        }
        assert!(decoder.dynamic.size <= 4096);
    }
}
