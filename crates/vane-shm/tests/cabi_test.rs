//! Integration test for the C ABI — exercises the extern "C" functions
//! through a full sidecar round-trip (same process, real SHM transport).

use std::ffi::{CStr, CString};

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
