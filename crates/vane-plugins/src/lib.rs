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
            let module = wasmtime::Module::from_binary(&engine, &wasm)
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
            let mut store = wasmtime::Store::new(&self.inner.engine, ());
            let linker: wasmtime::Linker<()> = wasmtime::Linker::new(&self.inner.engine);
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

/// A live plugin instance bound to one worker (Wasm instances are not
/// `Sync`; one per worker thread keeps the hot path dispatch-free).
pub struct PluginInstance {
    #[cfg(feature = "wasm")]
    store: wasmtime::Store<()>,
    #[cfg(feature = "wasm")]
    memory: wasmtime::Memory,
    #[cfg(feature = "wasm")]
    on_request: wasmtime::TypedFunc<(i32, i32, i32), i32>,
    #[cfg(feature = "wasm")]
    alloc: Option<wasmtime::TypedFunc<i32, i32>>,
}

impl PluginInstance {
    /// Runs the guest's `on_request(path)`.
    ///
    /// The path is copied into guest memory via the guest's `alloc` (kept
    /// tiny — only the path crosses; headers/body transformations come in
    /// later ABI revisions).
    ///
    /// # Errors
    /// ABI or trap failure.
    pub fn on_request(&mut self, path: &str) -> Result<GuestVerdict, PluginError> {
        #[cfg(feature = "wasm")]
        {
            // Allocate scratch in guest memory for the path.
            let Some(alloc) = self.alloc.as_ref() else {
                return Err(PluginError::Abi("missing `alloc` export".into()));
            };
            let ptr = alloc
                .call(&mut self.store, path.len() as i32)
                .map_err(|e| PluginError::Trap(e.to_string()))?;
            if path.len() > 0 {
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
            let _ = path;
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
}
