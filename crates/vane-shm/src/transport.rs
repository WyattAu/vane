//! The bidirectional transport: ring pair + slot arena pair over shared
//! memory.
//!
//! Files created under the transport base:
//! `{base}/req.ring|.free|.blob`  request direction
//! `{base}/resp.ring|.free|.blob` response direction
//!
//! The `.blob` file is a **slot arena**: `SLOTS` fixed slots of
//! `slot_size` bytes each. Producers pop a free slot index from the
//! `.free` ring, copy the payload in, and publish a descriptor
//! `{id, offset, len}` through the `.ring`. Consumers copy/read the
//! payload and push the index back — out-of-order release is trivially
//! correct, there is no cursor arithmetic, and backpressure falls out of
//! the free ring being empty.
//!
//! The vane sidecar (server) **creates** the transport; clients attach.

use std::path::{Path, PathBuf};

use memmap2;
use shm_rings::SpmcRingBuffer;

use crate::descriptor::MsgDesc;

/// Errors from transport setup and use.
#[derive(Debug, thiserror::Error)]
pub enum ShmError {
    /// shm-rings failure.
    #[error("shm ring: {0}")]
    Ring(#[from] shm_rings::ShmRingError),
    /// Arena/mmap failure.
    #[error("shm arena: {0}")]
    Arena(String),
    /// Payload exceeds the slot size.
    #[error("payload {0} bytes exceeds slot size {1}")]
    TooLarge(usize, usize),
    /// Timed out waiting for the peer.
    #[error("operation timed out")]
    Timeout,
    /// A required file does not exist (server not up).
    #[error("transport file missing: {0}")]
    Missing(String),
}

/// Default slot size (256 KiB — sidecar req/resp payloads are small).
pub const DEFAULT_SLOT_SIZE: u32 = 256 * 1024;
/// Default slots per direction.
pub const DEFAULT_SLOTS: u32 = 16;
/// Data region starts after the file header.
const HEADER: u64 = 64;
/// Blob file magic (bytes 0..8).
const BLOB_MAGIC: u64 = 0x76_61_6e_65_42_4c_4f_42; // "vaneBLOB"
/// Blob header: magic @0, slot_size @8, slots @16.
const OFF_MAGIC: usize = 0;
const OFF_SLOT_SIZE: usize = 8;
const OFF_SLOTS: usize = 16;

/// One direction: descriptor ring + slot arena + free-index ring.
struct Direction {
    /// Descriptor ring (produced by this direction's sender).
    ring: SpmcRingBuffer<MsgDesc>,
    /// Free slot indices (produced by this direction's receiver).
    free: SpmcRingBuffer<u32>,
    /// Arena mapping (never resized after creation).
    map: memmap2::MmapMut,
    slot_size: u32,
    #[allow(dead_code)] // geometry mirror (diagnostics / future ABI)
    slots: u32,
    /// This side produces descriptors?
    produce: bool,
}

// SAFETY: mapping access is governed by the slot protocol (one writer per
// slot between publish and release); atomics in rings carry the ordering.
// SAFETY: Direction owns its mapping; cross-thread sharing follows the
// slot protocol (one writer per slot between publish and release).
unsafe impl Send for Direction {}
// SAFETY: &self methods only read the mapping or atomic ring state.
unsafe impl Sync for Direction {}

/// A freed slot: just the index.
type SlotIndex = u32;

impl Direction {
    fn paths(prefix: &Path) -> (PathBuf, PathBuf, PathBuf) {
        (
            PathBuf::from(format!("{}.ring", prefix.display())),
            PathBuf::from(format!("{}.free", prefix.display())),
            PathBuf::from(format!("{}.blob", prefix.display())),
        )
    }

    /// Creates one direction. `produce`: this side sends descriptors.
    fn create(prefix: &Path, slot_size: u32, slots: u32, produce: bool) -> Result<Self, ShmError> {
        let (ring_path, free_path, blob_path) = Self::paths(prefix);
        let ring = SpmcRingBuffer::<MsgDesc>::create_new_with_readers(&ring_path, 1024, 1)?;
        let mut free =
            SpmcRingBuffer::<SlotIndex>::create_new_with_readers(&free_path, slots as usize, 1)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&blob_path)
            .map_err(|e| ShmError::Arena(format!("open {blob_path:?}: {e}")))?;
        file.set_len(HEADER + u64::from(slot_size) * u64::from(slots))
            .map_err(|e| ShmError::Arena(e.to_string()))?;
        // SAFETY: file-backed mapping of the size set above.
        let mut map = unsafe {
            memmap2::MmapMut::map_mut(&file).map_err(|e| ShmError::Arena(e.to_string()))?
        };
        {
            // Initialize the creator-owned geometry header.
            let base = map.as_mut_ptr();
            // SAFETY: mapping >= HEADER bytes; fields written once before
            // any peer maps the file (create_new ordering).
            unsafe {
                base.add(OFF_MAGIC)
                    .cast::<u64>()
                    .write_unaligned(BLOB_MAGIC);
                base.add(OFF_SLOT_SIZE)
                    .cast::<u64>()
                    .write_unaligned(u64::from(slot_size));
                base.add(OFF_SLOTS)
                    .cast::<u64>()
                    .write_unaligned(u64::from(slots));
            }
        }
        // Seed the free ring with every slot index.
        for i in 0..slots {
            free.try_push(&i);
        }
        Ok(Self {
            ring,
            free,
            map,
            slot_size,
            slots,
            produce,
        })
    }

    /// Opens an existing direction (the other side).
    fn open(prefix: &Path, produce: bool) -> Result<Self, ShmError> {
        let (ring_path, free_path, blob_path) = Self::paths(prefix);
        for p in [&ring_path, &free_path, &blob_path] {
            if !p.exists() {
                return Err(ShmError::Missing(p.display().to_string()));
            }
        }
        let ring = SpmcRingBuffer::<MsgDesc>::open_existing(&ring_path)?;
        let free = SpmcRingBuffer::<SlotIndex>::open_existing(&free_path)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&blob_path)
            .map_err(|e| ShmError::Arena(e.to_string()))?;
        // SAFETY: file-backed mapping of the creator-sized blob.
        let _map = unsafe {
            memmap2::MmapMut::map_mut(&file).map_err(|e| ShmError::Arena(e.to_string()))?
        };
        let _blob_len = file
            .metadata()
            .map_err(|e| ShmError::Arena(e.to_string()))?
            .len();
        // SAFETY: file-backed mapping of the creator-sized blob.
        let map = unsafe {
            memmap2::MmapMut::map_mut(&file).map_err(|e| ShmError::Arena(e.to_string()))?
        };
        // Discover the geometry from the blob header (creator-owned).
        let (slot_size, slots) = {
            let base = map.as_ptr();
            // SAFETY: header fields are within the mapping and were
            // published by the creator before our ring-header Acquire
            // (open_existing ordering).
            unsafe {
                let magic = base.add(OFF_MAGIC).cast::<u64>().read_unaligned();
                if magic != BLOB_MAGIC {
                    return Err(ShmError::Arena(format!(
                        "{blob_path:?}: bad blob magic 0x{magic:x}"
                    )));
                }
                let ss = base.add(OFF_SLOT_SIZE).cast::<u64>().read_unaligned();
                let sc = base.add(OFF_SLOTS).cast::<u64>().read_unaligned();
                (
                    u32::try_from(ss).map_err(|e| ShmError::Arena(e.to_string()))?,
                    u32::try_from(sc).map_err(|e| ShmError::Arena(e.to_string()))?,
                )
            }
        };
        Ok(Self {
            ring,
            free,
            map,
            slot_size,
            slots,
            produce,
        })
    }

    fn slot_ptr(&self, index: u32) -> *mut u8 {
        // The mapping address is stable; writes follow the slot protocol
        // (pop → write → publish), never through this shared borrow.
        let base = self.map.as_ptr() as *mut u8;
        // SAFETY: offset HEADER + index*slot_size lies within the mapping
        // (index < slots, protocol invariant).
        unsafe { base.add((HEADER + u64::from(index) * u64::from(self.slot_size)) as usize) }
    }

    /// Sends one payload (blocks on a free slot up to `timeout`).
    fn send(
        &mut self,
        id: u64,
        payload: &[u8],
        timeout: std::time::Duration,
    ) -> Result<(), ShmError> {
        if !self.produce {
            return Err(ShmError::Arena("this side does not produce".into()));
        }
        if payload.len() > self.slot_size as usize {
            return Err(ShmError::TooLarge(payload.len(), self.slot_size as usize));
        }
        let deadline = std::time::Instant::now() + timeout;
        let index = loop {
            match self.free.try_pop(0) {
                Ok(Some(i)) => break i,
                _ => {
                    if std::time::Instant::now() > deadline {
                        return Err(ShmError::Timeout);
                    }
                    std::hint::spin_loop();
                }
            }
        };
        // SAFETY: slot exclusively owned between pop and descriptor publish;
        // len <= slot_size (checked above).
        unsafe {
            std::ptr::copy_nonoverlapping(payload.as_ptr(), self.slot_ptr(index), payload.len());
        }
        let desc = MsgDesc::new(
            id,
            u64::from(index) * u64::from(self.slot_size),
            payload.len() as u32,
            now_ns(),
        );
        // Push (retry briefly; ring is 1024 deep — full means the consumer
        // is thousands of messages behind, treat as timeout).
        let deadline = std::time::Instant::now() + timeout;
        while !self.ring.try_push(&desc) {
            if std::time::Instant::now() > deadline {
                // Return the slot to avoid a leak.
                let _ = self.free.try_push(&index);
                return Err(ShmError::Timeout);
            }
            std::hint::spin_loop();
        }
        Ok(())
    }

    /// Pops one descriptor (spins until deadline); `None` on timeout.
    fn recv(&self, timeout: std::time::Duration) -> Result<Option<MsgDesc>, ShmError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.ring.try_pop(0) {
                Ok(Some(desc)) => return Ok(Some(desc)),
                _ => {
                    if std::time::Instant::now() > deadline {
                        return Ok(None);
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Borrows a popped descriptor's payload (zero-copy read).
    ///
    /// # Safety
    /// The view must not outlive [`Self::release`] for this descriptor.
    unsafe fn view(&self, desc: &MsgDesc) -> &[u8] {
        let index = (desc.offset / u64::from(self.slot_size)) as u32;
        let base = self.slot_ptr(index);
        // SAFETY: len <= slot_size (protocol invariant) and the slot is
        // frozen between publish and release.
        // SAFETY: view lifetime ends at `release` (caller contract).
        unsafe { std::slice::from_raw_parts(base, desc.len as usize) }
    }

    /// Releases a consumed slot back to the producer.
    fn release(&mut self, desc: &MsgDesc) {
        let index = (desc.offset / u64::from(self.slot_size)) as u32;
        let _ = self.free.try_push(&index);
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Transport configuration.
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    /// Base path (a directory; ring/arena files live inside).
    pub base: PathBuf,
    /// Slot size in bytes (payload ceiling per message).
    pub slot_size: u32,
    /// Slots per direction.
    pub slots: u32,
}

impl SidecarConfig {
    /// Default config under `/dev/shm/<name>`.
    #[must_use]
    pub fn dev_shm(name: &str) -> Self {
        Self {
            base: PathBuf::from("/dev/shm").join(name),
            slot_size: DEFAULT_SLOT_SIZE,
            slots: DEFAULT_SLOTS,
        }
    }

    /// Test config in a temp dir.
    #[must_use]
    pub fn small(base: PathBuf) -> Self {
        Self {
            base,
            slot_size: 64 * 1024,
            slots: 4,
        }
    }
}

/// Client end: sends requests, receives responses.
pub struct SidecarClient {
    req: Direction,
    resp: Direction,
    next_id: u64,
}

impl SidecarClient {
    /// Attaches as the client (server must have created the transport).
    ///
    /// # Errors
    /// Missing files or shm failure.
    pub fn open(config: &SidecarConfig) -> Result<Self, ShmError> {
        let req = Direction::open(&config.base.join("req"), true)?;
        let resp = Direction::open(&config.base.join("resp"), false)?;
        Ok(Self {
            req,
            resp,
            next_id: 1,
        })
    }

    /// Sends a request; returns the message id.
    ///
    /// # Errors
    /// Transport failure / timeout.
    pub fn send(&mut self, payload: &[u8], timeout: std::time::Duration) -> Result<u64, ShmError> {
        let id = self.next_id;
        self.next_id += 1;
        self.req.send(id, payload, timeout)?;
        Ok(id)
    }

    /// Receives the next response (None on timeout).
    ///
    /// # Errors
    /// Transport failure.
    pub fn recv(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<(u64, Vec<u8>)>, ShmError> {
        let Some(desc) = self.resp.recv(timeout)? else {
            return Ok(None);
        };
        let id = desc.id;
        // SAFETY: copied before the slot is released.
        let data = unsafe { self.resp.view(&desc) }.to_vec();
        self.resp.release(&desc);
        Ok(Some((id, data)))
    }
}

/// Server end: receives requests, sends responses; owns the transport.
pub struct SidecarServer {
    req: Direction,
    resp: Direction,
}

impl SidecarServer {
    /// Creates the transport.
    ///
    /// # Errors
    /// shm failure.
    pub fn open(config: &SidecarConfig) -> Result<Self, ShmError> {
        std::fs::create_dir_all(&config.base).map_err(|e| ShmError::Arena(e.to_string()))?;
        let req = Direction::create(
            &config.base.join("req"),
            config.slot_size,
            config.slots,
            false,
        )?;
        let resp = Direction::create(
            &config.base.join("resp"),
            config.slot_size,
            config.slots,
            true,
        )?;
        Ok(Self { req, resp })
    }

    /// Pops one request (None on timeout).
    ///
    /// # Errors
    /// Transport failure.
    pub fn recv(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<(u64, Vec<u8>)>, ShmError> {
        let Some(desc) = self.req.recv(timeout)? else {
            return Ok(None);
        };
        let id = desc.id;
        // SAFETY: copied before the slot is released.
        let data = unsafe { self.req.view(&desc) }.to_vec();
        self.req.release(&desc);
        Ok(Some((id, data)))
    }

    /// Replies to a request id.
    ///
    /// # Errors
    /// Transport failure / timeout.
    pub fn reply(
        &mut self,
        id: u64,
        payload: &[u8],
        timeout: std::time::Duration,
    ) -> Result<(), ShmError> {
        self.resp.send(id, payload, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tmp_config(name: &str) -> (tempfile::TempDir, SidecarConfig) {
        let dir = tempfile::tempdir().expect("dir");
        let cfg = SidecarConfig::small(dir.path().join(name));
        (dir, cfg)
    }

    #[test]
    fn roundtrip() {
        let (_dir, cfg) = tmp_config("roundtrip");
        let mut server = SidecarServer::open(&cfg).expect("server");
        let mut client = SidecarClient::open(&cfg).expect("client");

        let id = client
            .send(b"hello shm", Duration::from_secs(1))
            .expect("send");
        let (rid, req) = server
            .recv(Duration::from_secs(1))
            .expect("recv")
            .expect("req");
        assert_eq!(id, rid);
        assert_eq!(req, b"hello shm");
        server
            .reply(id, b"pong", Duration::from_secs(1))
            .expect("reply");
        let (rid, resp) = client
            .recv(Duration::from_secs(1))
            .expect("recv")
            .expect("resp");
        assert_eq!(rid, id);
        assert_eq!(resp, b"pong");
    }

    #[test]
    fn backpressure_then_drain() {
        let (_dir, cfg) = tmp_config("reclaim");
        let mut server = SidecarServer::open(&cfg).expect("server");
        let mut client = SidecarClient::open(&cfg).expect("client");
        let big = vec![0xABu8; 60 * 1024];
        // Fill the 4 slots.
        for _ in 0..4 {
            client.send(&big, Duration::from_millis(200)).expect("send");
        }
        // 5th must time out (no slots).
        assert!(matches!(
            client.send(&big, Duration::from_millis(100)),
            Err(ShmError::Timeout)
        ));
        // Drain releases slots; sends flow again.
        for _ in 0..4 {
            server
                .recv(Duration::from_secs(1))
                .expect("recv")
                .expect("req");
        }
        client
            .send(&big, Duration::from_millis(200))
            .expect("send after drain");
    }

    #[test]
    fn client_before_server_is_missing() {
        let (_dir, cfg) = tmp_config("missing");
        assert!(matches!(
            SidecarClient::open(&cfg),
            Err(ShmError::Missing(_))
        ));
    }
}
