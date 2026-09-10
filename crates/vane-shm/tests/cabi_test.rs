//! Integration test for the C ABI — exercises the extern "C" functions
//! through a full sidecar round-trip (same process, real SHM transport).

use std::ffi::{CStr, CString};
use std::time::Duration;

use vane_shm::cabi;

/// Verifies that the C ABI open/send/recv/close cycle works end-to-end
/// against a real transport (spawned within the test process).
#[test]
fn cabi_roundtrip() {
    // Start the mock server (SHM transport requires the server side).
    let dir = tempfile::tempdir().expect("dir");
    let base = dir.path().join("cabi-test");
    let server_cfg = vane_shm::transport::SidecarConfig {
        base: base.clone(),
        slot_size: 64 * 1024,
        slots: 4,
    };
    let _server = vane_shm::transport::SidecarServer::open(&server_cfg).expect("server");

    // Open a client via the C ABI.
    let path = CString::new(base.to_str().expect("utf8")).expect("cstring");
    // SAFETY: `path` outlives the call and points to a valid C string.
    let client = unsafe { cabi::vane_sc_open(path.as_ptr()) };
    assert!(!client.is_null(), "vane_sc_open returned null");

    // Send a request.
    let payload = b"hello from C";
    // SAFETY: `client` is a valid handle from open; payload is valid for `len`.
    let id = unsafe {
        cabi::vane_sc_send(
            client,
            payload.as_ptr(),
            payload.len(),
            2000, // timeout_ms
        )
    };
    assert_ne!(id, u64::MAX, "send failed: {}", {
        let err = cabi::vane_sc_last_err();
        if err.is_null() {
            "no error".to_owned()
        } else {
            // SAFETY: err is a valid NUL-terminated C string from the ABI.
            unsafe { CStr::from_ptr(err) }
                .to_string_lossy()
                .into_owned()
        }
    });

    // Close.
    // SAFETY: `client` is a valid handle, closed exactly once.
    unsafe { cabi::vane_sc_close(client) };
}

/// Verifies that opening a nonexistent transport returns null + error.
#[test]
fn cabi_open_missing_returns_null() {
    let path = CString::new("/nonexistent/vane/cabi/transport").expect("cstring");
    // SAFETY: `path` outlives the call and points to a valid C string.
    let client = unsafe { cabi::vane_sc_open(path.as_ptr()) };
    assert!(client.is_null(), "expected null for missing transport");
    let err = cabi::vane_sc_last_err();
    assert!(!err.is_null(), "expected error message");
}

/// Recv timeout returns 0 (no data) without touching out buffers.
#[test]
fn cabi_recv_timeout_returns_zero() {
    let dir = tempfile::tempdir().expect("dir");
    let base = dir.path().join("recv-timeout");
    let cfg = vane_shm::transport::SidecarConfig {
        base: base.clone(),
        slot_size: 64 * 1024,
        slots: 2,
    };
    let _server = vane_shm::transport::SidecarServer::open(&cfg).expect("server");
    let path = CString::new(base.to_str().expect("utf8")).expect("cstring");
    // SAFETY: path outlives the call, valid C string.
    let client = unsafe { cabi::vane_sc_open(path.as_ptr()) };
    assert!(!client.is_null());

    let mut out = [0u8; 64];
    let mut id = 0u64;
    // SAFETY: valid handle; out/id are writable for the given sizes.
    let rc = unsafe { cabi::vane_sc_recv(client, out.as_mut_ptr(), out.len(), &mut id, 50) };
    assert_eq!(rc, 0, "timeout must be 0, got {rc}");

    // SAFETY: valid handle.
    unsafe { cabi::vane_sc_close(client) };
}

/// Recv null/invalid args are rejected with -1 and an error string.
#[test]
fn cabi_recv_bad_args() {
    let mut out = [0u8; 8];
    let mut id = 0u64;
    // SAFETY: deliberately invalid args; the ABI must reject, not crash.
    let rc = unsafe {
        cabi::vane_sc_recv(
            std::ptr::null_mut(),
            out.as_mut_ptr(),
            out.len(),
            &mut id,
            10,
        )
    };
    assert_eq!(rc, -1);
    assert!(!cabi::vane_sc_last_err().is_null());
}

/// Closing null is a no-op (no crash).
#[test]
fn cabi_close_null_is_noop() {
    // SAFETY: null is explicitly tolerated.
    unsafe { cabi::vane_sc_close(std::ptr::null_mut()) };
}

/// Recv after a real send: data lands, id matches, length correct.
#[test]
fn cabi_recv_returns_sent_data() {
    use std::thread;
    let dir = tempfile::tempdir().expect("dir");
    let base = dir.path().join("recv-data");
    let cfg = vane_shm::transport::SidecarConfig {
        base: base.clone(),
        slot_size: 64 * 1024,
        slots: 2,
    };
    let mut server = vane_shm::transport::SidecarServer::open(&cfg).expect("server");
    let path = CString::new(base.to_str().expect("utf8")).expect("cstring");
    // SAFETY: valid C string for the ABI.
    let client = unsafe { cabi::vane_sc_open(path.as_ptr()) };
    assert!(!client.is_null());

    // Server echoes: recv then reply with the same payload.
    thread::spawn(move || {
        loop {
            match server.recv(Duration::from_secs(1)) {
                Ok(Some((id, data))) => {
                    let _ = server.reply(id, &data, Duration::from_secs(5));
                }
                Ok(None) => continue,
                Err(_) => return,
            }
        }
    });

    // SAFETY: valid handle; payload valid for len.
    let id = unsafe { cabi::vane_sc_send(client, b"payload-123".as_ptr(), 11, 2000) };
    assert_ne!(id, u64::MAX);

    let mut out = [0u8; 64];
    let mut rid = 0u64;
    // SAFETY: valid handle; out writable.
    let rc = unsafe { cabi::vane_sc_recv(client, out.as_mut_ptr(), out.len(), &mut rid, 5000) };
    assert_eq!(rc, 11, "recv length");
    assert_eq!(rid, id, "reply id matches");
    assert_eq!(&out[..11], b"payload-123");

    // SAFETY: valid handle.
    unsafe { cabi::vane_sc_close(client) };
}

/// Out buffer smaller than the reply: -2 with an error string.
#[test]
fn cabi_recv_buffer_too_small() {
    use std::thread;
    let dir = tempfile::tempdir().expect("dir");
    let base = dir.path().join("recv-small");
    let cfg = vane_shm::transport::SidecarConfig {
        base: base.clone(),
        slot_size: 64 * 1024,
        slots: 2,
    };
    let mut server = vane_shm::transport::SidecarServer::open(&cfg).expect("server");
    let path = CString::new(base.to_str().expect("utf8")).expect("cstring");
    // SAFETY: valid C string for the ABI.
    let client = unsafe { cabi::vane_sc_open(path.as_ptr()) };
    assert!(!client.is_null());

    thread::spawn(move || {
        loop {
            match server.recv(Duration::from_secs(1)) {
                Ok(Some((id, data))) => {
                    let _ = server.reply(id, &data, Duration::from_secs(5));
                }
                Ok(None) => continue,
                Err(_) => return,
            }
        }
    });

    // SAFETY: valid handle.
    let id = unsafe { cabi::vane_sc_send(client, b"0123456789".as_ptr(), 10, 2000) };
    assert_ne!(id, u64::MAX);

    let mut out = [0u8; 4]; // too small
    let mut rid = 0u64;
    // SAFETY: valid handle; deliberately small out buffer.
    let rc = unsafe { cabi::vane_sc_recv(client, out.as_mut_ptr(), out.len(), &mut rid, 5000) };
    assert_eq!(rc, -2, "buffer-too-small must be -2, got {rc}");
    assert!(!cabi::vane_sc_last_err().is_null());

    // SAFETY: valid handle.
    unsafe { cabi::vane_sc_close(client) };
}
