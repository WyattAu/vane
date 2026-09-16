//! Protobuf wire-format primitives (encode + minimal decode).
//!
//! Hand-rolled — no prost/tonic. Covers the subset xDS needs:
//! varint (uint32/uint64/bool/enum), length-delimited (string/bytes/
//! embedded messages), and 64/32-bit fixed. Decode is best-effort:
//! unknown fields are skipped per the wire spec (field 3 = wire type).

/// Wire types (protobuf encoding spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireType {
    /// Varint (int32, int64, uint32, uint64, sint, bool, enum).
    Varint = 0,
    /// 64-bit fixed (fixed64, sfixed64, double).
    Fixed64 = 1,
    /// Length-delimited (string, bytes, embedded messages, packed).
    Len = 2,
    /// 32-bit fixed (fixed32, sfixed32, float).
    Fixed32 = 5,
}

/// Encodes a field tag (field number + wire type).
pub fn tag(out: &mut Vec<u8>, field: u32, wire: WireType) {
    varint(out, ((u64::from(field) << 3) | wire as u64) as u64);
}

/// Appends a base-128 varint.
pub fn varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Appends a length-delimited payload (string, bytes, submessage).
pub fn bytes_field(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    tag(out, field, WireType::Len);
    varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

/// Appends a UTF-8 string field.
pub fn string_field(out: &mut Vec<u8>, field: u32, value: &str) {
    bytes_field(out, field, value.as_bytes());
}

/// Appends a uint64/uint32/enum/bool varint field (skipped when zero —
/// proto3 omits defaults).
pub fn varint_field(out: &mut Vec<u8>, field: u32, value: u64) {
    if value == 0 {
        return;
    }
    tag(out, field, WireType::Varint);
    varint(out, value);
}

/// Appends a bool field (proto3 default-omitted).
pub fn bool_field(out: &mut Vec<u8>, field: u32, value: bool) {
    if value {
        varint_field(out, field, 1);
    }
}

/// Appends a nested message (its encoded bytes as a Len field).
pub fn message_field(out: &mut Vec<u8>, field: u32, encoded: &[u8]) {
    bytes_field(out, field, encoded);
}

/// A decoded field: number, wire type, and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field<'a> {
    /// Field number.
    pub number: u32,
    /// Wire type.
    pub wire: WireType,
    /// Varint payload (Varint wire type).
    pub varint: u64,
    /// Length-delimited payload (Len wire type).
    pub bytes: &'a [u8],
}

/// Decodes one field from `buf`; returns the field and bytes consumed.
/// Unknown wire types map to `None` (caller stops or skips per spec —
/// groups are deprecated and unsupported).
pub fn decode_field(buf: &[u8]) -> Option<(Field<'_>, usize)> {
    let (key, mut pos) = decode_varint(buf)?;
    let number = (key >> 3) as u32;
    let wire = match key & 0x7 {
        0 => WireType::Varint,
        1 => WireType::Fixed64,
        2 => WireType::Len,
        5 => WireType::Fixed32,
        _ => return None,
    };
    match wire {
        WireType::Varint => {
            let (v, n) = decode_varint(&buf[pos..])?;
            pos += n;
            Some((
                Field {
                    number,
                    wire,
                    varint: v,
                    bytes: &[],
                },
                pos,
            ))
        }
        WireType::Fixed64 => {
            if buf.len() < pos + 8 {
                return None;
            }
            let mut v = 0u64;
            for (i, b) in buf[pos..pos + 8].iter().enumerate() {
                v |= u64::from(*b) << (8 * i);
            }
            Some((
                Field {
                    number,
                    wire,
                    varint: v,
                    bytes: &[],
                },
                pos + 8,
            ))
        }
        WireType::Len => {
            let (len, n) = decode_varint(&buf[pos..])?;
            pos += n;
            let end = pos + len as usize;
            if buf.len() < end {
                return None;
            }
            Some((
                Field {
                    number,
                    wire,
                    varint: 0,
                    bytes: &buf[pos..end],
                },
                end,
            ))
        }
        WireType::Fixed32 => {
            if buf.len() < pos + 4 {
                return None;
            }
            let mut v = 0u64;
            for (i, b) in buf[pos..pos + 4].iter().enumerate() {
                v |= u64::from(*b) << (8 * i);
            }
            Some((
                Field {
                    number,
                    wire,
                    varint: v,
                    bytes: &[],
                },
                pos + 4,
            ))
        }
    }
}

/// Decodes a varint; returns (value, bytes consumed).
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0;
    for (i, b) in buf.iter().enumerate() {
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 65_536, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            varint(&mut buf, v);
            let (decoded, n) = decode_varint(&buf).expect("decode");
            assert_eq!(decoded, v);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn field_roundtrip() {
        let mut buf = Vec::new();
        string_field(&mut buf, 1, "grpc");
        varint_field(&mut buf, 2, 7);
        bool_field(&mut buf, 3, true);
        varint_field(&mut buf, 4, 0); // omitted (default)
        let (f1, n) = decode_field(&buf).expect("f1");
        assert_eq!(f1.number, 1);
        assert_eq!(f1.bytes, b"grpc");
        let (f2, n2) = decode_field(&buf[n..]).expect("f2");
        assert_eq!(f2.varint, 7);
        let (f3, n3) = decode_field(&buf[n + n2..]).expect("f3");
        assert!(f3.varint == 1);
        // field 4 omitted entirely.
        assert_eq!(n + n2 + n3, buf.len());
    }
}
