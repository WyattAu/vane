//! # vane-plugins
//!
//! WebAssembly plugin sandbox (`PL-01`).
//!
//! Plugins implement a tiny ABI (guest exports):
//!
//! ```wat
//! (memory (export "memory") 1)
//! (func (export "on_request") (param i32 i32 i32) (result i32))
//! ;;   ^ ptr to path, path len, ptr to scratch — returns 0 continue,
//! ;;    or 100..599 to short-circuit with that status
//! (func (export "alloc") (param i32) (result i32)) ;; bump allocator
//! ```
//!
//! Host→guest data passes through guest linear memory slices (zero-copy
//! into the Wasm heap — no serialization). The guest writes an optional
//! response body via `alloc` + a status return.
//!
//! ## ABI v2 — header access
//!
//! Guests may optionally import host functions from the `vane` namespace
//! to read request headers (no serialization — names/values are copied
//! into guest memory at pointers the guest chooses):
//!
//! ```wat
//! (import "vane" "header_count" (func (result i32)))
//! (import "vane" "header_name"  (func (param i32 i32) (result i32)))
//! ;;   ^ idx, dest ptr → bytes written, or -1 on OOB
//! (import "vane" "header_value" (func (param i32 i32) (result i32)))
//! ```
//!
//! Modules that import nothing keep working (ABI v1). Host functions
//! are always linked; unused imports cost nothing.
//!
//! The `wasm` feature pulls in `wasmtime`; the sidecar build keeps it off
//! to honor the 12 MB RSS budget.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use std::path::Path;
use std::sync::Arc;

/// Plugin errors.
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    /// The `wasm` feature is not enabled.
    #[error("vane built without the `wasm` feature")]
    FeatureDisabled,
    /// Module load/compile failure.
    #[error("plugin compile: {0}")]
    Compile(String),
    /// ABI mismatch (missing exports).
    #[error("plugin abi: {0}")]
    Abi(String),
    /// Guest trap.
    #[error("plugin trap: {0}")]
    Trap(String),
}

/// Guest verdict on a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestVerdict {
    /// Continue the pipeline.
    Continue,
    /// Short-circuit with this HTTP status.
    Reject(u16),
}

/// Compiled, instantiation-ready plugin module (clone-cheap handle).
pub struct PluginModule {
    #[allow(dead_code)] // read only under the `wasm` feature
    inner: Arc<Inner>,
}

#[cfg(feature = "wasm")]
struct Inner {
    module: wasmtime::Module,
    engine: wasmtime::Engine,
}

#[cfg(not(feature = "wasm"))]
struct Inner {}

impl PluginModule {
    /// Compiles a `.wasm` plugin.
    ///
    /// # Errors
    /// Feature disabled or compilation failure.
    pub fn compile(_path: &Path) -> Result<Self, PluginError> {
        #[cfg(feature = "wasm")]
        {
            let engine = wasmtime::Engine::default();
            let wasm =
                std::fs::read(_path).map_err(|e| PluginError::Compile(format!("read: {e}")))?;
            // Binary (`\0asm` magic) or WAT text (dev convenience — the
            // wasmtime `wat` feature is enabled by default).
            let module = if wasm.starts_with(b"\0asm") {
                wasmtime::Module::from_binary(&engine, &wasm)
            } else {
                wasmtime::Module::new(&engine, &wasm)
            }
            .map_err(|e| PluginError::Compile(e.to_string()))?;
            Ok(Self {
                inner: Arc::new(Inner { module, engine }),
            })
        }
        #[cfg(not(feature = "wasm"))]
        Err(PluginError::FeatureDisabled)
    }

    /// Creates a per-worker instance store for the plugin.
    ///
    /// # Errors
    /// Instantiation or ABI failure.
    pub fn instantiate(&self) -> Result<PluginInstance, PluginError> {
        #[cfg(feature = "wasm")]
        {
            let mut store = wasmtime::Store::new(&self.inner.engine, HostState::default());
            let mut linker: wasmtime::Linker<HostState> = wasmtime::Linker::new(&self.inner.engine);
            link_host_functions(&mut linker)
                .map_err(|e| PluginError::Abi(format!("host link: {e}")))?;
            let instance = linker
                .instantiate(&mut store, &self.inner.module)
                .map_err(|e| PluginError::Abi(e.to_string()))?;
            let memory = instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| PluginError::Abi("missing `memory` export".into()))?;
            let on_request = instance
                .get_typed_func::<(i32, i32, i32), i32>(&mut store, "on_request")
                .map_err(|e| PluginError::Abi(format!("missing `on_request`: {e}")))?;
            let alloc = instance
                .get_typed_func::<i32, i32>(&mut store, "alloc")
                .ok();
            Ok(PluginInstance {
                store,
                memory,
                on_request,
                alloc,
            })
        }
        #[cfg(not(feature = "wasm"))]
        Err(PluginError::FeatureDisabled)
    }
}

/// Per-request state visible to host functions (ABI v2 header access).
#[cfg(feature = "wasm")]
#[derive(Default)]
struct HostState {
    headers: Vec<(String, String)>,
}

/// Links the `vane` namespace host functions (ABI v2). Modules that
/// don't import them are unaffected.
#[cfg(feature = "wasm")]
fn link_host_functions(linker: &mut wasmtime::Linker<HostState>) -> Result<(), wasmtime::Error> {
    linker.func_wrap(
        "vane",
        "header_count",
        |caller: wasmtime::Caller<'_, HostState>| -> i32 { caller.data().headers.len() as i32 },
    )?;
    linker.func_wrap(
        "vane",
        "header_name",
        |caller: wasmtime::Caller<'_, HostState>, idx: i32, dest: i32| -> i32 {
            let Some(field) = caller
                .data()
                .headers
                .get(idx.max(0) as usize)
                .map(|h| h.0.clone())
            else {
                return -1;
            };
            write_guest(caller, dest, field.as_bytes())
        },
    )?;
    linker.func_wrap(
        "vane",
        "header_value",
        |caller: wasmtime::Caller<'_, HostState>, idx: i32, dest: i32| -> i32 {
            let Some(field) = caller
                .data()
                .headers
                .get(idx.max(0) as usize)
                .map(|h| h.1.clone())
            else {
                return -1;
            };
            write_guest(caller, dest, field.as_bytes())
        },
    )?;
    Ok(())
}

/// Copies `bytes` into guest memory at `dest`; returns the length or -1
/// if the memory export is missing or the write traps.
#[cfg(feature = "wasm")]
fn write_guest(mut caller: wasmtime::Caller<'_, HostState>, dest: i32, bytes: &[u8]) -> i32 {
    let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
        return -1;
    };
    if mem.write(&mut caller, dest.max(0) as usize, bytes).is_err() {
        return -1;
    }
    bytes.len() as i32
}

/// A live plugin instance bound to one worker (Wasm instances are not
/// `Sync`; one per worker thread keeps the hot path dispatch-free).
pub struct PluginInstance {
    #[cfg(feature = "wasm")]
    store: wasmtime::Store<HostState>,
    #[cfg(feature = "wasm")]
    memory: wasmtime::Memory,
    #[cfg(feature = "wasm")]
    on_request: wasmtime::TypedFunc<(i32, i32, i32), i32>,
    #[cfg(feature = "wasm")]
    alloc: Option<wasmtime::TypedFunc<i32, i32>>,
}

impl PluginInstance {
    /// Runs the guest's `on_request(path)` with no headers (ABI v1).
    ///
    /// The path is copied into guest memory via the guest's `alloc` (kept
    /// tiny — only the path crosses; body transformations come in later
    /// ABI revisions).
    ///
    /// # Errors
    /// ABI or trap failure.
    pub fn on_request(&mut self, path: &str) -> Result<GuestVerdict, PluginError> {
        self.on_request_with_headers(path, &[])
    }

    /// Runs the guest's `on_request(path)` with request headers exposed
    /// through the ABI v2 `vane.header_*` host functions.
    ///
    /// # Errors
    /// ABI or trap failure.
    pub fn on_request_with_headers(
        &mut self,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<GuestVerdict, PluginError> {
        #[cfg(feature = "wasm")]
        {
            // Publish headers for the host functions before the guest runs.
            *self.store.data_mut() = HostState {
                headers: headers.to_vec(),
            };
            // Allocate scratch in guest memory for the path.
            let Some(alloc) = self.alloc.as_ref() else {
                return Err(PluginError::Abi("missing `alloc` export".into()));
            };
            let ptr = alloc
                .call(&mut self.store, path.len() as i32)
                .map_err(|e| PluginError::Trap(e.to_string()))?;
            if !path.is_empty() {
                self.memory
                    .write(&mut self.store, ptr as usize, path.as_bytes())
                    .map_err(|e| PluginError::Trap(e.to_string()))?;
            }
            let rc = self
                .on_request
                .call(&mut self.store, (ptr, path.len() as i32, 0))
                .map_err(|e| PluginError::Trap(e.to_string()))?;
            Ok(match rc {
                0 => GuestVerdict::Continue,
                code if (100..600).contains(&code) => GuestVerdict::Reject(code as u16),
                other => {
                    return Err(PluginError::Abi(format!(
                        "on_request returned invalid status {other}"
                    )));
                }
            })
        }
        #[cfg(not(feature = "wasm"))]
        {
            let _ = (path, headers);
            Err(PluginError::FeatureDisabled)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_gate_reports_disabled() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("nope.wasm");
        std::fs::write(&path, b"\0asm").expect("write");
        match PluginModule::compile(&path) {
            Err(PluginError::FeatureDisabled) => {} // expected without feature
            Err(PluginError::Compile(_)) => {}      // expected with feature (truncated module)
            Err(other) => panic!("unexpected: {other}"),
            Ok(_) => panic!("garbage module compiled"),
        }
    }

    /// ABI v2 end-to-end: a guest that reads headers through the
    /// `vane.header_*` host functions and rejects unless
    /// `x-vane-auth: secret` is present.
    #[cfg(feature = "wasm")]
    #[test]
    fn abi_v2_header_access_roundtrip() {
        const GUEST: &str = r#"
(module
  (import "vane" "header_count" (func $count (result i32)))
  (import "vane" "header_name" (func $name (param i32 i32) (result i32)))
  (import "vane" "header_value" (func $value (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  ;; scratch: name at 2048, value at 3072
  (func (export "alloc") (param i32) (result i32) i32.const 1024)
  (func (export "on_request") (param i32 i32 i32) (result i32)
    (if (i32.ne (call $count) (i32.const 2))
      (then (return (i32.const 500))))
    ;; name(0) must be 11 bytes ("x-vane-auth")
    (if (i32.ne (call $name (i32.const 0) (i32.const 4096)) (i32.const 11))
      (then (return (i32.const 500))))
    ;; content check: first 4 bytes are "x-va" (LE 0x61762d78)
    (if (i32.ne (i32.load (i32.const 4096)) (i32.const 0x61762d78))
      (then (return (i32.const 401))))
    ;; value(0) must be 6 bytes ("secret")
    (if (i32.ne (call $value (i32.const 0) (i32.const 4096)) (i32.const 6))
      (then (return (i32.const 500))))
    ;; content check: first 4 bytes are "secr" (LE 0x72636573)
    (if (i32.ne (i32.load (i32.const 4096)) (i32.const 0x72636573))
      (then (return (i32.const 401))))
    ;; OOB index must return -1
    (if (i32.ne (call $name (i32.const 99) (i32.const 4096)) (i32.const -1))
      (then (return (i32.const 500))))
    (i32.const 0))
)
"#;
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("auth.wasm");
        std::fs::write(&path, GUEST).expect("write wat");

        let module = PluginModule::compile(&path).expect("compile");
        let mut inst = module.instantiate().expect("instantiate");

        // Wrong headers → 401 from the guest.
        let verdict = inst
            .on_request_with_headers(
                "/protected",
                &[
                    ("x-vane-auth".into(), "wrong1".into()),
                    ("accept".into(), "*/*".into()),
                ],
            )
            .expect("call");
        assert_eq!(verdict, GuestVerdict::Reject(401));

        // Correct headers → continue.
        let verdict = inst
            .on_request_with_headers(
                "/protected",
                &[
                    ("x-vane-auth".into(), "secret".into()),
                    ("accept".into(), "*/*".into()),
                ],
            )
            .expect("call");
        assert_eq!(verdict, GuestVerdict::Continue);
    }

    /// Invalid status return surfaces as an ABI error.
    #[cfg(feature = "wasm")]
    #[test]
    fn abi_invalid_status_is_error() {
        const GUEST: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "on_request") (param i32 i32 i32) (result i32)
    (i32.const 999))
)
"#;
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("bad.wasm");
        std::fs::write(&path, GUEST).expect("write wat");
        let module = PluginModule::compile(&path).expect("compile");
        let mut inst = module.instantiate().expect("instantiate");
        match inst.on_request("/x") {
            Err(PluginError::Abi(msg)) => assert!(msg.contains("999"), "{msg}"),
            other => panic!("expected Abi error, got {other:?}"),
        }
    }

    /// ABI v1 compatibility: a guest with no `vane` imports still runs.
    #[cfg(feature = "wasm")]
    #[test]
    fn abi_v1_guest_still_works() {
        const GUEST: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "on_request") (param i32 i32 i32) (result i32)
    (i32.const 418))
)
"#;
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("v1.wasm");
        std::fs::write(&path, GUEST).expect("write wat");

        let module = PluginModule::compile(&path).expect("compile");
        let mut inst = module.instantiate().expect("instantiate");
        let verdict = inst.on_request("/v1").expect("call");
        assert_eq!(verdict, GuestVerdict::Reject(418));
    }
}
