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

/// Loads cert + key (and an optional client-auth CA) and builds a
/// server config with the given ALPN set.
///
/// # Errors
/// Material load failure (missing/unparsable files).
pub fn load_config(
    cert: &Path,
    key: &Path,
    alpn: &[Vec<u8>],
    client_ca: Option<&Path>,
) -> Result<rustls::ServerConfig, String> {
    let mut cfg = vane_tls::server_config_mtls(cert, key, client_ca).map_err(|e| e.to_string())?;
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
    client_ca: Option<PathBuf>,
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
            let watch_paths: Vec<PathBuf> = client_ca
                .iter()
                .chain(std::iter::once(&cert))
                .cloned()
                .chain(std::iter::once(key))
                .collect();
            std::thread::spawn(move || {
                let (ntx, nrx) = std::sync::mpsc::channel();
                let mut watcher = match notify::recommended_watcher(ntx) {
                    Ok(w) => w,
                    Err(_) => return,
                };
                for p in &watch_paths {
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
        let fingerprint = || -> Option<Vec<std::time::SystemTime>> {
            let mut times: Vec<std::time::SystemTime> = client_ca
                .iter()
                .filter_map(|p| std::fs::metadata(p).ok()?.modified().ok())
                .collect();
            times.push(std::fs::metadata(&cert).ok()?.modified().ok()?);
            times.push(std::fs::metadata(&key).ok()?.modified().ok()?);
            Some(times)
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
            match load_config(&cert, &key, &alpn, client_ca.as_deref()) {
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

#[cfg(test)]
mod reload_tests {
    use super::*;

    fn test_certpair(dir: &std::path::Path) -> (PathBuf, PathBuf) {
        let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        std::fs::write(&cert, certs.cert.pem()).expect("cert");
        std::fs::write(&key, certs.signing_key.serialize_pem()).expect("key");
        (cert, key)
    }

    #[test]
    fn load_config_reads_pair() {
        let dir = tempfile::tempdir().expect("dir");
        let (cert, key) = test_certpair(dir.path());
        let cfg = load_config(&cert, &key, &[b"h2".to_vec()], None).expect("load");
        assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn load_config_rejects_missing() {
        let dir = tempfile::tempdir().expect("dir");
        let missing = dir.path().join("nope.pem");
        assert!(load_config(&missing, &missing, &[], None).is_err());
    }

    #[tokio::test]
    async fn reloader_swaps_slot_on_change() {
        let dir = tempfile::tempdir().expect("dir");
        let (cert, key) = test_certpair(dir.path());
        let initial = load_config(&cert, &key, &[], None).expect("load");
        let slot: TlsSlot = Arc::new(std::sync::RwLock::new(Arc::new(initial)));
        let _handle = spawn_reloader(cert.clone(), key.clone(), None, vec![], Arc::clone(&slot));

        // Rewrite the cert with a fresh keypair: the watcher must swap.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (cert2, key2) = test_certpair(dir.path());
        // Atomic replace via rename (editor-style).
        std::fs::rename(&cert2, &cert).expect("rename cert");
        std::fs::rename(&key2, &key).expect("rename key");

        let before = Arc::as_ptr(&slot.read().expect("read"));
        let mut swapped = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            // A swap replaces the inner Arc: pointer inequality proves it.
            let current = slot.read().expect("read");
            if Arc::as_ptr(&current) != before {
                swapped = true;
                break;
            }
        }
        assert!(swapped, "slot was not swapped after cert change");
    }
}
