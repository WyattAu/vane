//! TLS certificate hot reload: watches cert/key paths and swaps the shared
//! `rustls::ServerConfig` slot without touching live workers.
//!
//! Each listener with TLS gets a watcher task. On any change to either
//! file (notify event or 30s poll fallback), the new material is loaded;
//! on success the shared slot's inner Arc is replaced. Existing
//! connections keep their old `ServerConnection`; new connections pick up
//! the new certificate. Failures keep the previous config (last-writer-
//! wins only on valid loads).

use notify::Watcher as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// A swappable TLS configuration slot shared with workers.
pub type TlsSlot = Arc<std::sync::RwLock<Arc<rustls::ServerConfig>>>;

/// Loads cert + key and builds a server config with the given ALPN set.
///
/// # Errors
/// Material load failure (missing/unparsable files).
pub fn load_config(
    cert: &Path,
    key: &Path,
    alpn: &[Vec<u8>],
) -> Result<rustls::ServerConfig, String> {
    let mut cfg = vane_tls::server_config(cert, key).map_err(|e| e.to_string())?;
    cfg.alpn_protocols = alpn.to_vec();
    Ok(cfg)
}

/// Spawns the hot-reload watcher for one listener's TLS material.
///
/// `slot` is the same slot handed to the worker factory. Reload fires on
/// notify events and a 30 s poll fallback (covers NFS/editors that rename).
pub fn spawn_reloader(
    cert: PathBuf,
    key: PathBuf,
    alpn: Vec<Vec<u8>>,
    slot: TlsSlot,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        // inotify via the sync notify API on a blocking thread.
        {
            let tx = tx.clone();
            let cert = cert.clone();
            let key = key.clone();
            std::thread::spawn(move || {
                let (ntx, nrx) = std::sync::mpsc::channel();
                let mut watcher = match notify::recommended_watcher(ntx) {
                    Ok(w) => w,
                    Err(_) => return,
                };
                for p in [&cert, &key] {
                    let _ = watcher.watch(p, notify::RecursiveMode::NonRecursive);
                }
                for res in nrx {
                    if tx.blocking_send(()).is_err() {
                        return;
                    }
                    let _ = res;
                }
            });
        }

        // mtime fingerprint: skip reloads when nothing actually changed
        // (editors emit multiple events per save).
        let fingerprint = || -> Option<(std::time::SystemTime, std::time::SystemTime)> {
            Some((
                std::fs::metadata(&cert).ok()?.modified().ok()?,
                std::fs::metadata(&key).ok()?.modified().ok()?,
            ))
        };
        let mut last_fp = fingerprint();

        let mut poll = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = rx.recv() => {}
                _ = poll.tick() => {}
            }
            let fp = fingerprint();
            if fp == last_fp {
                continue;
            }
            match load_config(&cert, &key, &alpn) {
                Ok(cfg) => {
                    let mut w = slot.write().expect("tls slot");
                    *w = Arc::new(cfg);
                    last_fp = fp;
                    tracing::info!("tls: certificate reloaded");
                }
                Err(e) => {
                    tracing::warn!("tls: reload failed (keeping previous): {e}");
                }
            }
        }
    })
}
