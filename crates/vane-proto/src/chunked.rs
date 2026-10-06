//! Incremental HTTP/1.1 chunked-transfer decoding.
//!
//! vane relays a chunked upstream response verbatim (pass-through: the
//! size lines, CRLFs and trailers go downstream untouched), so it does
//! not need the decoded body — only **where the terminal chunk is**, to
//! know the response is over.
//!
//! Finding that by scanning for `0\r\n\r\n` is wrong twice over:
//!
//! * The sequence is legal inside chunk *data* (any base64 blob,
//!   compressed stream, or text containing a NUL). A content scan ends
//!   the response at the first lookalike and truncates the body.
//! * TCP carries no message boundaries, so the sequence can straddle
//!   two reads and a per-buffer scan never sees it at all — the
//!   response then never completes.
//!
//! [`ChunkedScanner`] tracks the actual grammar across reads, so both
//! are handled. Reads may split anywhere, including inside a size line
//! or between the CR and the LF of the trailing CRLF.

/// Cap on a buffered size/trailer line. Chunk extensions are
/// unbounded in the grammar; anything past this is treated as malformed
/// rather than buffered without limit.
const LINE_CAP: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Reading a chunk-size line.
    Size,
    /// Relaying chunk payload (`remaining` bytes left).
    Payload,
    /// Expecting the CRLF that closes a chunk's payload.
    DataCrlf,
    /// Reading trailer lines up to the empty one.
    Trailers,
    /// Terminal chunk seen (plus trailers).
    Done,
}

/// Streaming chunked-framing scanner. Feed response bytes in any
/// segmentation; `feed` returns `true` once the terminal chunk and any
/// trailers have been consumed.
#[derive(Debug, Clone)]
pub struct ChunkedScanner {
    state: State,
    /// Payload bytes still expected for the current chunk.
    remaining: u64,
    /// Bytes of the current line not yet terminated by CRLF.
    line: Vec<u8>,
    /// Bytes of the post-payload CRLF still expected (2, then 1).
    crlf_left: u8,
}

impl Default for ChunkedScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedScanner {
    /// A scanner positioned at the first chunk-size line.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: State::Size,
            remaining: 0,
            line: Vec::new(),
            crlf_left: 0,
        }
    }

    /// Feeds response bytes. Returns `true` once the terminal chunk and
    /// its trailers have been seen.
    ///
    /// Bytes past the terminal chunk are ignored: they belong to the
    /// next response (or are a protocol violation), and the caller
    /// stops relaying once this returns `true`.
    pub fn feed(&mut self, mut data: &[u8]) -> bool {
        loop {
            match self.state {
                State::Done => return true,
                State::Size => {
                    let Some((line, consumed)) = take_line(&mut self.line, data) else {
                        if self.line.len() > LINE_CAP {
                            self.state = State::Done;
                            return true;
                        }
                        return false;
                    };
                    data = &data[consumed..];
                    match parse_chunk_size(&line) {
                        Some(0) => self.state = State::Trailers,
                        Some(n) => {
                            self.remaining = n;
                            self.state = State::Payload;
                        }
                        // A size line that is not hex is a protocol
                        // error; stop rather than scan on.
                        None => {
                            self.state = State::Done;
                            return true;
                        }
                    }
                }
                State::Payload => {
                    // Consumed straight out of `data`, never buffered:
                    // a multi-megabyte chunk costs nothing here.
                    if data.is_empty() {
                        return false;
                    }
                    let take = (data.len() as u64).min(self.remaining) as usize;
                    data = &data[take..];
                    self.remaining -= take as u64;
                    if self.remaining == 0 {
                        self.state = State::DataCrlf;
                        self.crlf_left = 2;
                    }
                }
                State::DataCrlf => {
                    while self.crlf_left > 0 {
                        if data.is_empty() {
                            return false;
                        }
                        let want = if self.crlf_left == 2 { b'\r' } else { b'\n' };
                        if data[0] != want {
                            self.state = State::Done;
                            return true;
                        }
                        data = &data[1..];
                        self.crlf_left -= 1;
                    }
                    self.state = State::Size;
                }
                State::Trailers => {
                    let Some((line, consumed)) = take_line(&mut self.line, data) else {
                        if self.line.len() > LINE_CAP {
                            self.state = State::Done;
                            return true;
                        }
                        return false;
                    };
                    data = &data[consumed..];
                    // An empty line ends the trailer section.
                    if line.is_empty() {
                        self.state = State::Done;
                        return true;
                    }
                }
            }
        }
    }

    /// Whether the terminal chunk has been seen.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }
}

/// Takes the next CRLF-terminated line, joining it with the carried
/// prefix. Returns the line without its CRLF and how many bytes of
/// `data` it consumed.
///
/// A CRLF can straddle two reads, so the carry's trailing CR is
/// completed against the first byte of `data` before searching.
fn take_line(carry: &mut Vec<u8>, data: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut joined = 0usize;
    if carry.last() == Some(&b'\r') && data.first() == Some(&b'\n') {
        carry.push(b'\n');
        joined = 1;
    }
    // A CRLF that the carry already holds (just completed from data's
    // first byte) counts too: the payload may end exactly at a read
    // boundary.
    if let Some(n) = find_crlf(carry) {
        let line: Vec<u8> = carry.drain(..n + 2).collect();
        return Some((line[..n].to_vec(), joined));
    }
    match find_crlf(&data[joined..]) {
        Some(n) => {
            let mut line = std::mem::take(carry);
            line.extend_from_slice(&data[..joined + n]);
            Some((line, joined + n + 2))
        }
        None => {
            carry.extend_from_slice(data);
            None
        }
    }
}

/// Offset of the next CRLF, if the slice contains one.
fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

/// Parses a chunk-size line: hex digits, optional `;extension`.
fn parse_chunk_size(line: &[u8]) -> Option<u64> {
    let hex = match line.iter().position(|b| *b == b';') {
        Some(at) => &line[..at],
        None => line,
    };
    let hex = trim_ascii(hex);
    if hex.is_empty() {
        return None;
    }
    // Reject anything that is not a hex digit: `usize::from_str_radix`
    // would too, but the explicit check keeps the intent obvious.
    if !hex.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut size: u64 = 0;
    for b in hex {
        let digit = match b {
            b'0'..=b'9' => u64::from(b - b'0'),
            b'a'..=b'f' => u64::from(b - b'a') + 10,
            b'A'..=b'F' => u64::from(b - b'A') + 10,
            _ => return None,
        };
        size = size.saturating_mul(16).saturating_add(digit);
    }
    Some(size)
}

fn trim_ascii(mut b: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = b {
        if first.is_ascii_whitespace() {
            b = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = b {
        if last.is_ascii_whitespace() {
            b = rest;
        } else {
            break;
        }
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(chunks: &[&[u8]]) -> bool {
        let mut s = ChunkedScanner::new();
        let mut done = false;
        for c in chunks {
            if s.feed(c) {
                done = true;
            }
        }
        done
    }

    #[test]
    fn single_chunk() {
        assert!(scan(&[b"5\r\nhello\r\n0\r\n\r\n"]));
    }

    #[test]
    fn multiple_chunks() {
        assert!(scan(&[b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"]));
    }

    /// The sequence the old content scan looked for, inside chunk data.
    #[test]
    fn terminal_sequence_inside_payload_is_not_the_end() {
        let body = b"before-0\r\n\r\n-after";
        let mut resp = Vec::new();
        resp.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
        resp.extend_from_slice(body);
        resp.extend_from_slice(b"\r\n0\r\n\r\n");
        assert!(scan(&[&resp]), "real terminal must still be found");
    }

    /// Same, but the lookalike lands in an earlier read than the real
    /// terminal — the case that truncated bodies in production.
    #[test]
    fn terminal_sequence_in_an_earlier_read_is_not_the_end() {
        let body = b"before-0\r\n\r\n-after";
        let mut head = format!("{:x}\r\n", body.len()).into_bytes();
        head.extend_from_slice(b"before-0\r\n\r\n");
        let mut tail = Vec::new();
        tail.extend_from_slice(b"-after\r\n0\r\n\r\n");
        let mut s = ChunkedScanner::new();
        assert!(!s.feed(&head), "ended at the lookalike");
        assert!(s.feed(&tail), "missed the real terminal");
    }

    #[test]
    fn terminal_split_between_reads() {
        assert!(scan(&[b"5\r\nhello\r\n0\r\n", b"\r\n"]));
    }

    #[test]
    fn terminal_split_byte_by_byte() {
        let resp = b"5\r\nhello\r\n0\r\n\r\n";
        let pieces: Vec<&[u8]> = resp.chunks(1).collect();
        assert!(scan(&pieces));
    }

    /// Every possible single split point must find the terminal.
    #[test]
    fn every_single_split_point_finds_the_terminal() {
        let resp: &[u8] = b"5\r\nhello\r\n3\r\n bye\r\n0\r\n\r\n";
        for at in 0..=resp.len() {
            let (a, b) = resp.split_at(at);
            let mut s = ChunkedScanner::new();
            let done = s.feed(a) || s.feed(b);
            assert!(done, "missed terminal when split at {at}");
        }
    }

    /// Same, for a payload full of lookalikes.
    #[test]
    fn every_split_point_with_lookalike_payload() {
        let body = b"a0\r\n\r\nb0\r\n\r\nc";
        let mut resp = format!("{:x}\r\n", body.len()).into_bytes();
        resp.extend_from_slice(body);
        resp.extend_from_slice(b"\r\n0\r\n\r\n");
        for at in 0..=resp.len() {
            let (a, b) = resp.split_at(at);
            let mut s = ChunkedScanner::new();
            assert!(s.feed(a) || s.feed(b), "missed terminal at {at}");
        }
    }

    /// Byte-at-a-time delivery of a response with lookalikes.
    #[test]
    fn byte_at_a_time_with_lookalikes() {
        let body = b"a0\r\n\r\nb0\r\n\r\nc";
        let mut resp = format!("{:x}\r\n", body.len()).into_bytes();
        resp.extend_from_slice(body);
        resp.extend_from_slice(b"\r\n0\r\n\r\n");
        let mut s = ChunkedScanner::new();
        let mut done = false;
        for b in resp {
            if s.feed(&[b]) {
                done = true;
            }
        }
        assert!(done, "byte-at-a-time delivery lost the terminal");
    }

    #[test]
    fn trailers_are_consumed_to_the_blank_line() {
        assert!(scan(&[b"1\r\nx\r\n0\r\nX-Trailer: v\r\n\r\n"]));
    }

    #[test]
    fn trailer_split_across_reads() {
        assert!(scan(&[b"1\r\nx\r\n0\r\nX-Trailer: ", b"v\r\n", b"\r\n"]));
    }

    #[test]
    fn chunk_extensions_are_ignored() {
        assert!(scan(&[b"5;name=value\r\nhello\r\n0\r\n\r\n"]));
    }

    #[test]
    fn chunk_size_across_reads() {
        assert!(scan(&[b"1", b"0\r\n", b"0123456789\r\n", b"0\r\n\r\n"]));
    }

    #[test]
    fn uppercase_and_wide_hex_sizes() {
        assert!(scan(&[b"A\r\n0123456789\r\n0\r\n\r\n"]));
    }

    /// CRLF between payload and the next size line, split across reads.
    #[test]
    fn data_crlf_split_between_cr_and_lf() {
        assert!(scan(&[b"1\r\nx\r", b"\n0\r\n\r\n"]));
    }

    #[test]
    fn malformed_size_ends_the_scan() {
        let mut s = ChunkedScanner::new();
        assert!(s.feed(b"zz\r\nhello\r\n"), "garbage size accepted");
        assert!(s.is_done());
    }

    #[test]
    fn nothing_after_the_terminal_is_consumed() {
        let mut s = ChunkedScanner::new();
        assert!(s.feed(b"1\r\nx\r\n0\r\n\r\n"));
        // Bytes past the terminal belong to the next response.
        assert!(s.is_done());
        assert!(s.feed(b"GET /next HTTP/1.1\r\n"));
        assert!(s.is_done());
    }

    #[test]
    fn fresh_scanner_is_not_done() {
        assert!(!ChunkedScanner::new().is_done());
    }

    #[test]
    fn empty_input_is_not_done() {
        let mut s = ChunkedScanner::new();
        assert!(!s.feed(b""));
        assert!(!s.is_done());
    }
}
