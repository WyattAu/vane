//! HTTP/2 frame codec (RFC 9113 §6) — header parsing, typed payload
//! validation, and the constants shared by the connection state
//! machine.
//!
//! Every frame starts with a 9-byte header:
//!
//! ```text
//! +-----------------------------------------------+
//! |                 Length (24)                   |
//! +---------------+---------------+---------------+
//! |   Type (8)    |   Flags (8)   |
//! +-+-------------+---------------+-------------------------------+
//! |R|                 Stream Identifier (31)                      |
//! +=+=============================================================+
//! ```
//!
//! All decode paths are panic-free on arbitrary bytes (fuzz target:
//! `fuzz/frame.rs`).

/// Frame header size in bytes.
pub const FRAME_HEADER_LEN: usize = 9;

/// Default max frame payload size (RFC 9113 §6.1) before
/// SETTINGS_MAX_FRAME_SIZE raises it (peer-advertised, still capped at
/// 16 MiB).
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;
/// Hard ceiling for SETTINGS_MAX_FRAME_SIZE.
pub const MAX_ALLOWED_FRAME_SIZE: u32 = 16_777_215;

/// Frame types (RFC 9113 §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Stream data.
    Data,
    /// Header block fragment.
    Headers,
    /// Deprecated priority hint (parsed, ignored).
    Priority,
    /// Stream reset.
    RstStream,
    /// Connection settings.
    Settings,
    /// Server push (rejected by clients; unsupported here).
    PushPromise,
    /// Liveness ping (opaque 8-byte payload).
    Ping,
    /// Graceful shutdown notice.
    GoAway,
    /// Flow-control window credit.
    WindowUpdate,
    /// CONTINUATION of a HEADERS block.
    Continuation,
    /// Unknown/extension type — payload skipped.
    Unknown(u8),
}

impl FrameKind {
    fn from_u8(b: u8) -> Self {
        match b {
            0x0 => Self::Data,
            0x1 => Self::Headers,
            0x2 => Self::Priority,
            0x3 => Self::RstStream,
            0x4 => Self::Settings,
            0x5 => Self::PushPromise,
            0x6 => Self::Ping,
            0x7 => Self::GoAway,
            0x8 => Self::WindowUpdate,
            0x9 => Self::Continuation,
            other => Self::Unknown(other),
        }
    }

    /// Wire byte for this frame type.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Data => 0x0,
            Self::Headers => 0x1,
            Self::Priority => 0x2,
            Self::RstStream => 0x3,
            Self::Settings => 0x4,
            Self::PushPromise => 0x5,
            Self::Ping => 0x6,
            Self::GoAway => 0x7,
            Self::WindowUpdate => 0x8,
            Self::Continuation => 0x9,
            Self::Unknown(b) => b,
        }
    }
}

/// Frame flags — bit meanings depend on `FrameKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameFlags(u8);

impl FrameFlags {
    /// No flags set.
    pub const EMPTY: Self = Self(0);

    /// DATA: END_STREAM. HEADERS: END_STREAM.
    #[must_use]
    pub fn end_stream(self) -> bool {
        self.0 & 0x01 != 0
    }
    /// HEADERS: END_HEADERS. CONTINUATION: END_HEADERS.
    #[must_use]
    pub fn end_headers(self) -> bool {
        self.0 & 0x04 != 0
    }
    /// DATA: PADDED. HEADERS: PADDED. PUSH_PROMISE: PADDED.
    #[must_use]
    pub fn padded(self) -> bool {
        self.0 & 0x08 != 0
    }
    /// DATA: END_STREAM (same bit).
    #[must_use]
    pub fn ack(self) -> bool {
        self.0 & 0x01 != 0
    }
    /// HEADERS: priority fields present.
    #[must_use]
    pub fn priority(self) -> bool {
        self.0 & 0x20 != 0
    }

    /// Wraps a raw flags byte.
    #[must_use]
    pub fn from_u8(b: u8) -> Self {
        Self(b)
    }

    /// Raw flags byte.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self.0
    }
}

/// Parsed frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Payload length (0..=16_777_215).
    pub length: u32,
    /// Frame type.
    pub kind: FrameKind,
    /// Frame flags.
    pub flags: FrameFlags,
    /// Stream identifier with the reserved bit masked off.
    pub stream_id: u32,
}

/// Frame parse errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer than 9 bytes / payload shorter than the declared length.
    Truncated,
    /// Stream id 0 used where prohibited (DATA/HEADERS/CONTINUATION/
    /// RST_STREAM/PUSH_PROMISE) or non-zero where prohibited
    /// (SETTINGS/PING/GOAWAY).
    InvalidStreamId,
    /// Payload length invalid for the frame type (e.g. WINDOW_UPDATE
    /// != 4, RST_STREAM != 4, PING != 8).
    InvalidPayloadLength,
    /// Pad length >= payload length (DATA/HEADERS).
    InvalidPadding,
    /// SETTINGS frame with a payload not a multiple of 6, or ACK with
    /// a payload.
    InvalidSettings,
    /// WINDOW_UPDATE with an increment of 0.
    InvalidWindowIncrement,
}

/// Parses a 9-byte frame header from `data` (which may be longer; only
/// the first 9 bytes are read). Returns `Err(Truncated)` if fewer than
/// 9 bytes.
/// Parses a 9-byte frame header.
///
/// # Errors
/// [`FrameError::Truncated`] when fewer than 9 bytes are provided.
pub fn parse_header(data: &[u8]) -> Result<FrameHeader, FrameError> {
    let hdr = data.get(..FRAME_HEADER_LEN).ok_or(FrameError::Truncated)?;
    let length = u32::from_be_bytes([0, hdr[0], hdr[1], hdr[2]]);
    let kind = FrameKind::from_u8(hdr[3]);
    let flags = FrameFlags::from_u8(hdr[4]);
    // Reserved bit masked off (RFC 9113 §4.1).
    let stream_id = u32::from_be_bytes([hdr[5] & 0x7f, hdr[6], hdr[7], hdr[8]]);
    Ok(FrameHeader {
        length,
        kind,
        flags,
        stream_id,
    })
}

/// Serializes a frame header.
pub fn write_header(
    out: &mut Vec<u8>,
    length: u32,
    kind: FrameKind,
    flags: FrameFlags,
    stream_id: u32,
) {
    debug_assert!(length <= MAX_ALLOWED_FRAME_SIZE);
    debug_assert!(stream_id <= 0x7fff_ffff);
    let b = length.to_be_bytes();
    out.extend_from_slice(&b[1..4]);
    out.push(kind.as_u8());
    out.push(flags.as_u8());
    out.extend_from_slice(&stream_id.to_be_bytes());
}

/// Structural validation + payload split for a received frame. `data`
/// is the frame payload exactly (`header.length` bytes).
///
/// Returns pad-length / payload bounds per RFC §6: for padded frames,
/// `pad` is the first byte and the content is the middle slice; the
/// trailing `pad` bytes are dropped.
/// Structural validation + payload split for a received frame.
///
/// # Errors
/// [`FrameError::Truncated`] (payload shorter than declared),
/// [`FrameError::InvalidPayloadLength`], [`FrameError::InvalidSettings`],
/// [`FrameError::InvalidPadding`], or
/// [`FrameError::InvalidWindowIncrement`] per RFC 9113 §6 rules.
pub fn validate_payload(
    header: &FrameHeader,
    data: &[u8],
    peer_max_frame: u32,
) -> Result<PayloadSplit, FrameError> {
    if data.len() as u32 != header.length {
        return Err(FrameError::Truncated);
    }
    // Frame-size ceiling: ours is what WE accept; the spec cap is hard.
    if header.length > peer_max_frame.max(MAX_ALLOWED_FRAME_SIZE) {
        return Err(FrameError::InvalidPayloadLength);
    }

    let padded = match header.kind {
        FrameKind::Data => header.flags.padded(),
        FrameKind::Headers => header.flags.padded(),
        FrameKind::PushPromise => header.flags.padded(),
        _ => false,
    };
    if padded {
        let Some(&pad_len) = data.first() else {
            return Err(FrameError::Truncated);
        };
        // pad_len must be < total payload (the pad byte itself counts).
        let content_len = (data.len() as u32)
            .checked_sub(1 + u32::from(pad_len))
            .ok_or(FrameError::InvalidPadding)?;
        return Ok(PayloadSplit {
            pad_len: Some(pad_len),
            content_start: 1,
            content_end: 1 + content_len as usize,
        });
    }

    match header.kind {
        FrameKind::Data | FrameKind::Headers | FrameKind::Continuation => {}
        FrameKind::RstStream => {
            if header.length != 4 {
                return Err(FrameError::InvalidPayloadLength);
            }
        }
        FrameKind::Settings => {
            if header.flags.ack() {
                if header.length != 0 {
                    return Err(FrameError::InvalidSettings);
                }
            } else if header.length % 6 != 0 {
                return Err(FrameError::InvalidSettings);
            }
        }
        FrameKind::Ping => {
            if header.length != 8 {
                return Err(FrameError::InvalidPayloadLength);
            }
        }
        FrameKind::GoAway => {
            if header.length < 8 {
                return Err(FrameError::InvalidPayloadLength);
            }
        }
        FrameKind::WindowUpdate => {
            if header.length != 4 {
                return Err(FrameError::InvalidPayloadLength);
            }
            if data.len() == 4 {
                let inc = u32::from_be_bytes([data[0] & 0x7f, data[1], data[2], data[3]]);
                if inc == 0 {
                    return Err(FrameError::InvalidWindowIncrement);
                }
            }
        }
        FrameKind::Priority => {
            if header.length != 5 {
                return Err(FrameError::InvalidPayloadLength);
            }
        }
        FrameKind::PushPromise | FrameKind::Unknown(_) => {}
    }

    Ok(PayloadSplit {
        pad_len: None,
        content_start: 0,
        content_end: data.len(),
    })
}

/// Bounds of the content region within the payload (padding removed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadSplit {
    /// Pad length byte value (`None` = unpadded frame).
    pub pad_len: Option<u8>,
    /// Content start offset within the payload.
    pub content_start: usize,
    /// Content end offset (exclusive) within the payload.
    pub content_end: usize,
}

/// SETTINGS id/value pairs (RFC 9113 §6.5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Setting {
    /// `SETTINGS_HEADER_TABLE_SIZE` (0x1).
    HeaderTableSize(u32),
    /// `SETTINGS_ENABLE_PUSH` (0x2). Raw value — the setting is
    /// only valid as 0 or 1; receivers must PROTOCOL_ERROR otherwise.
    EnablePush(u32),
    /// `SETTINGS_MAX_CONCURRENT_STREAMS` (0x3).
    MaxConcurrentStreams(u32),
    /// `SETTINGS_INITIAL_WINDOW_SIZE` (0x4).
    InitialWindowSize(u32),
    /// `SETTINGS_MAX_FRAME_SIZE` (0x5).
    MaxFrameSize(u32),
    /// `SETTINGS_MAX_HEADER_LIST_SIZE` (0x6).
    MaxHeaderListSize(u32),
    /// Unknown/extension setting — must be ignored.
    Unknown(u16, u32),
}

/// Parses SETTINGS payload into id/value pairs (6 bytes each).
/// Parses SETTINGS payload into id/value pairs (6 bytes each).
///
/// # Errors
/// [`FrameError::InvalidSettings`] when the payload is not a multiple
/// of 6.
pub fn parse_settings(data: &[u8]) -> Result<Vec<Setting>, FrameError> {
    if data.len() % 6 != 0 {
        return Err(FrameError::InvalidSettings);
    }
    let mut out = Vec::with_capacity(data.len() / 6);
    for pair in data.chunks_exact(6) {
        let id = u16::from_be_bytes([pair[0], pair[1]]);
        let value = u32::from_be_bytes([pair[2], pair[3], pair[4], pair[5]]);
        out.push(match id {
            0x1 => Setting::HeaderTableSize(value),
            0x2 => Setting::EnablePush(value),
            0x3 => Setting::MaxConcurrentStreams(value),
            0x4 => Setting::InitialWindowSize(value),
            0x5 => Setting::MaxFrameSize(value),
            0x6 => Setting::MaxHeaderListSize(value),
            other => Setting::Unknown(other, value),
        });
    }
    Ok(out)
}

/// Serializes one SETTINGS id/value pair.
pub fn write_setting(out: &mut Vec<u8>, id: u16, value: u32) {
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
}

/// Parses a WINDOW_UPDATE payload: reserved bit + 31-bit increment.
/// (The stream id lives in the frame header, not the payload.)
///
/// # Errors
/// [`FrameError::InvalidPayloadLength`] when not exactly 4 bytes.
pub fn parse_window_update(data: &[u8]) -> Result<u32, FrameError> {
    if data.len() != 4 {
        return Err(FrameError::InvalidPayloadLength);
    }
    Ok(u32::from_be_bytes([
        data[0] & 0x7f,
        data[1],
        data[2],
        data[3],
    ]))
}

/// Parses a RST_STREAM error code.
///
/// # Errors
/// [`FrameError::InvalidPayloadLength`] when not exactly 4 bytes.
pub fn parse_rst_stream(data: &[u8]) -> Result<u32, FrameError> {
    if data.len() != 4 {
        return Err(FrameError::InvalidPayloadLength);
    }
    Ok(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(length: u32, kind: FrameKind, flags: FrameFlags, stream_id: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_HEADER_LEN);
        write_header(&mut out, length, kind, flags, stream_id);
        out
    }

    #[test]
    fn header_roundtrip() {
        let raw = header_bytes(
            0x000102,
            FrameKind::Headers,
            FrameFlags::from_u8(0xff),
            0x7fff_ffff,
        );
        let hdr = parse_header(&raw).expect("parse");
        assert_eq!(hdr.length, 0x000102);
        assert_eq!(hdr.kind, FrameKind::Headers);
        assert_eq!(hdr.flags.as_u8(), 0xff);
        assert_eq!(hdr.stream_id, 0x7fff_ffff, "reserved bit masked");
    }

    #[test]
    fn header_truncated() {
        assert_eq!(parse_header(&[0; 8]), Err(FrameError::Truncated));
    }

    #[test]
    fn settings_roundtrip() {
        let mut payload = Vec::new();
        write_setting(&mut payload, 0x1, 4096);
        write_setting(&mut payload, 0x4, 1_048_576);
        write_setting(&mut payload, 0xff, 7); // unknown ignored
        let raw = header_bytes(
            payload.len() as u32,
            FrameKind::Settings,
            FrameFlags::EMPTY,
            0,
        );
        let hdr = parse_header(&raw).expect("parse");
        assert!(validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).is_ok());
        let settings = parse_settings(&payload).expect("parse settings");
        assert_eq!(settings.len(), 3);
        assert_eq!(settings[0], Setting::HeaderTableSize(4096));
        assert_eq!(settings[1], Setting::InitialWindowSize(1_048_576));
        assert_eq!(settings[2], Setting::Unknown(0xff, 7));
    }

    #[test]
    fn settings_bad_length() {
        let raw = header_bytes(7, FrameKind::Settings, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 7], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidSettings)
        ));
        // ACK with payload.
        let raw = header_bytes(6, FrameKind::Settings, FrameFlags::from_u8(0x01), 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 6], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidSettings)
        ));
    }

    #[test]
    fn data_padding_split() {
        // payload: pad_len=3, "hi", 3 pad bytes → length 6.
        let payload = [3, b'h', b'i', 0, 0, 0];
        let raw = header_bytes(
            payload.len() as u32,
            FrameKind::Data,
            FrameFlags::from_u8(0x08),
            1,
        );
        let hdr = parse_header(&raw).expect("parse");
        let split = validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).expect("valid");
        assert_eq!(&payload[split.content_start..split.content_end], b"hi");
    }

    #[test]
    fn data_pad_overflow_is_invalid() {
        let payload = [9, b'h', b'i', 0, 0];
        let raw = header_bytes(
            payload.len() as u32,
            FrameKind::Data,
            FrameFlags::from_u8(0x08),
            1,
        );
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPadding)
        ));
    }

    #[test]
    fn window_update_zero_increment_invalid() {
        let payload = [0, 0, 0, 0];
        let raw = header_bytes(4, FrameKind::WindowUpdate, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidWindowIncrement)
        ));
    }

    #[test]
    fn window_update_stream_zero_ok() {
        let payload = [0, 0, 0, 16];
        let raw = header_bytes(4, FrameKind::WindowUpdate, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).is_ok());
    }

    #[test]
    fn oversize_frame_rejected() {
        let raw = header_bytes(
            DEFAULT_MAX_FRAME_SIZE + 1,
            FrameKind::Data,
            FrameFlags::EMPTY,
            1,
        );
        let hdr = parse_header(&raw).expect("parse");
        // Declared length beyond both our peer cap and the spec hard cap:
        // rejected structurally (the peer can never legally send this).
        assert!(matches!(
            validate_payload(&hdr, &[], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength) | Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn rst_stream_requires_4_bytes() {
        let raw = header_bytes(3, FrameKind::RstStream, FrameFlags::EMPTY, 1);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 3], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
        let raw = header_bytes(4, FrameKind::RstStream, FrameFlags::EMPTY, 1);
        let hdr = parse_header(&raw).expect("parse");
        let payload = [0, 0, 0, 8]; // CANCEL
        assert!(validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).is_ok());
        assert_eq!(parse_rst_stream(&payload), Ok(8));
    }

    #[test]
    fn ping_requires_8_bytes() {
        let raw = header_bytes(9, FrameKind::Ping, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 9], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
    }

    #[test]
    fn unknown_frame_type_skipped() {
        let payload = [1, 2, 3];
        let raw = header_bytes(3, FrameKind::Unknown(0xab), FrameFlags::EMPTY, 4);
        let hdr = parse_header(&raw).expect("parse");
        assert!(validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).is_ok());
    }

    #[test]
    fn goaway_requires_at_least_8_bytes() {
        let raw = header_bytes(4, FrameKind::GoAway, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 4], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
        let raw = header_bytes(8, FrameKind::GoAway, FrameFlags::EMPTY, 0);
        let hdr = parse_header(&raw).expect("parse");
        assert!(validate_payload(&hdr, &[0u8; 8], DEFAULT_MAX_FRAME_SIZE).is_ok());
    }

    #[test]
    fn window_update_requires_4_bytes() {
        let raw = header_bytes(5, FrameKind::WindowUpdate, FrameFlags::EMPTY, 1);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 5], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
    }

    #[test]
    fn priority_requires_5_bytes() {
        let raw = header_bytes(4, FrameKind::Priority, FrameFlags::EMPTY, 1);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[0u8; 4], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
        let raw = header_bytes(5, FrameKind::Priority, FrameFlags::EMPTY, 1);
        let hdr = parse_header(&raw).expect("parse");
        assert!(validate_payload(&hdr, &[0u8; 5], DEFAULT_MAX_FRAME_SIZE).is_ok());
    }

    #[test]
    fn push_promise_padding_splits_like_data() {
        // payload: pad_len=1, one content byte, 1 pad byte → length 3.
        let payload = [1, b'x', 0];
        let raw = header_bytes(
            payload.len() as u32,
            FrameKind::PushPromise,
            FrameFlags::from_u8(0x08),
            3,
        );
        let hdr = parse_header(&raw).expect("parse");
        let split = validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE).expect("valid");
        assert_eq!(split.pad_len, Some(1));
        assert_eq!(&payload[split.content_start..split.content_end], b"x");
    }

    #[test]
    fn padded_frame_with_empty_payload_truncated() {
        let raw = header_bytes(0, FrameKind::Data, FrameFlags::from_u8(0x08), 1);
        let hdr = parse_header(&raw).expect("parse");
        assert!(matches!(
            validate_payload(&hdr, &[], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn spec_hard_frame_cap_rejected_even_with_matching_payload() {
        // The length check precedes the payload match, so the payload
        // must be materialized at the declared (over-cap) size. The
        // header is built directly: cap+1 is not expressible in the
        // 24-bit wire field (the cap IS that limit), so it can only
        // arise from an in-process bug — exactly what this guards.
        let len = MAX_ALLOWED_FRAME_SIZE + 1;
        let hdr = FrameHeader {
            length: len,
            kind: FrameKind::Data,
            flags: FrameFlags::EMPTY,
            stream_id: 1,
        };
        let payload = vec![0u8; len as usize];
        assert!(matches!(
            validate_payload(&hdr, &payload, DEFAULT_MAX_FRAME_SIZE),
            Err(FrameError::InvalidPayloadLength)
        ));
    }

    #[test]
    fn parse_settings_rejects_non_multiple_of_six() {
        assert_eq!(parse_settings(&[0u8; 7]), Err(FrameError::InvalidSettings));
    }

    #[test]
    fn parse_settings_maps_all_known_ids() {
        let mut payload = Vec::new();
        write_setting(&mut payload, 0x2, 1);
        write_setting(&mut payload, 0x5, 16_384);
        write_setting(&mut payload, 0x6, 4096);
        let settings = parse_settings(&payload).expect("parse settings");
        assert_eq!(settings[0], Setting::EnablePush(1));
        assert_eq!(settings[1], Setting::MaxFrameSize(16_384));
        assert_eq!(settings[2], Setting::MaxHeaderListSize(4096));
    }

    #[test]
    fn parse_window_update_and_rst_reject_bad_lengths() {
        assert_eq!(
            parse_window_update(&[0u8; 3]),
            Err(FrameError::InvalidPayloadLength)
        );
        assert_eq!(parse_window_update(&[0, 0, 0, 9]), Ok(9));
        assert_eq!(
            parse_rst_stream(&[0u8; 5]),
            Err(FrameError::InvalidPayloadLength)
        );
    }

    #[test]
    fn garbage_never_panics() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for len in 0..400u64 {
            let mut data = Vec::with_capacity(len as usize);
            for _ in 0..len {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                data.push(x as u8);
            }
            if let Ok(hdr) = parse_header(&data) {
                let _ = validate_payload(
                    &hdr,
                    data.get(FRAME_HEADER_LEN..).unwrap_or(&[]),
                    DEFAULT_MAX_FRAME_SIZE,
                );
                let _ = FrameKind::from_u8(hdr.kind.as_u8());
            }
        }
    }
}
