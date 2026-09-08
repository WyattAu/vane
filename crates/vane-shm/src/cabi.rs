//! C ABI for non-Rust sidecar SDKs (`vane_sidecar.h` mirrors these).
//!
//! All functions are thread-compatible (one client per handle, external
//! locking for sharing). Errors return negative values; `vane_sc_last_err`
//! retrieves a static message.

use std::ffi::{CStr, CString, c_char, c_int, c_ulonglong};
use std::sync::Mutex;
use std::time::Duration;

use crate::transport::{SidecarClient, SidecarConfig};

/// Opaque client handle.
pub struct VaneScClient {
    inner: Mutex<SidecarClient>,
}

thread_local! {
    static LAST_ERR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

fn set_err(msg: &str) {
    LAST_ERR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

/// Opens (or creates) a client transport at `base_path`.
///
/// # Safety
/// `base_path` must be a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vane_sc_open(base_path: *const c_char) -> *mut VaneScClient {
    if base_path.is_null() {
        set_err("null path");
        return std::ptr::null_mut();
    }
    // SAFETY: caller guarantees a valid NUL-terminated string.
    let path = unsafe { CStr::from_ptr(base_path) }
        .to_string_lossy()
        .into_owned();
    let config = SidecarConfig {
        base: std::path::PathBuf::from(path),
        ..SidecarConfig::dev_shm("default")
    };
    match SidecarClient::open(&config) {
        Ok(client) => Box::into_raw(Box::new(VaneScClient {
            inner: Mutex::new(client),
        })),
        Err(e) => {
            set_err(&e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Sends a request; returns the message id or -1.
///
/// # Safety
/// `payload` must be valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vane_sc_send(
    client: *mut VaneScClient,
    payload: *const u8,
    len: usize,
    timeout_ms: u32,
) -> c_ulonglong {
    if client.is_null() || (payload.is_null() && len > 0) {
        set_err("bad args");
        return c_ulonglong::MAX;
    }
    // SAFETY: caller guarantees `payload` is valid for `len` bytes.
    let slice: &[u8] = if len == 0 {
        &[][..]
    } else {
        // SAFETY: see contract above.
        unsafe { std::slice::from_raw_parts(payload, len) }
    };
    // SAFETY: caller guarantees a live handle from vane_sc_open.
    let handle: &VaneScClient = unsafe { &*client };
    match handle.inner.lock() {
        Ok(mut c) => match c.send(slice, Duration::from_millis(u64::from(timeout_ms))) {
            Ok(id) => id,
            Err(e) => {
                set_err(&e.to_string());
                c_ulonglong::MAX
            }
        },
        Err(_) => {
            set_err("poisoned");
            c_ulonglong::MAX
        }
    }
}

/// Receives the next response. Returns payload length, copying into
/// `out`/`out_len`; `0` on timeout; `usize::MAX` on error.
///
/// # Safety
/// `out` must be valid for `out_len` bytes; `id_out` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vane_sc_recv(
    client: *mut VaneScClient,
    out: *mut u8,
    out_len: usize,
    id_out: *mut u64,
    timeout_ms: u32,
) -> c_int {
    if client.is_null() || out.is_null() || id_out.is_null() {
        set_err("bad args");
        return -1;
    }
    // SAFETY: caller guarantees a live handle from vane_sc_open.
    let handle: &VaneScClient = unsafe { &*client };
    let Ok(mut c) = handle.inner.lock() else {
        set_err("poisoned");
        return -1;
    };
    match c.recv(Duration::from_millis(u64::from(timeout_ms))) {
        Ok(Some((id, data))) => {
            if data.len() > out_len {
                set_err("buffer too small");
                return -2;
            }
            // SAFETY: out valid for out_len >= data.len() bytes (checked).
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), out, data.len()) };
            // SAFETY: caller guarantees id_out is writable.
            unsafe { id_out.write(id) };
            data.len() as c_int
        }
        Ok(None) => 0,
        Err(e) => {
            set_err(&e.to_string());
            -1
        }
    }
}

/// Closes a client handle.
///
/// # Safety
/// `client` must have come from `vane_sc_open` and not be used after.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vane_sc_close(client: *mut VaneScClient) {
    if !client.is_null() {
        // SAFETY: pointer came from Box::into_raw in vane_sc_open.
        drop(unsafe { Box::from_raw(client) });
    }
}

/// Last error message (thread-local, valid until the next call).
#[unsafe(no_mangle)]
pub extern "C" fn vane_sc_last_err() -> *const c_char {
    LAST_ERR.with(|e| {
        let b = e.borrow();
        b.as_ref().map_or(std::ptr::null(), |c| c.as_ptr())
    })
}
