//! Decrypting wire-capture support for TLS h2 debugging.
//!
//! Two tools, both inert unless explicitly enabled:
//!
//! * [`spawn`] — a decrypting TCP proxy (activated with
//!   `VANE_WIREPROXY=1`): terminates the client's TLS leg (presenting
//!   the test server's own certificate), opens a second TLS leg to the
//!   real server, and pumps the PLAINTEXT h2 byte stream between them,
//!   recording both directions to `/tmp/opencode/wire/c2s.bin`
//!   (client→server) and `s2c.bin` (server→client).
//! * [`TeeStream`] (activated with `VANE_WIRETEE=1`) — a passive
//!   passthrough that wraps the client's TLS stream and tees the
//!   DECRYPTED read side (exactly the bytes the h2 parser consumes)
//!   to `/tmp/opencode/wire/client_view.bin`, without perturbing the
//!   connection path the way the proxy does.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

/// A running capture proxy.
pub struct WireProxy {
    /// The local address the proxy listens on.
    pub addr: SocketAddr,
}

/// Starts a capture proxy in front of `upstream` (the real TLS server).
/// `cert`/`key` are the PEM paths of the server certificate to present.
///
/// # Errors
/// Propagates bind / certificate-load failures.
pub async fn spawn(upstream: SocketAddr, cert: &Path, key: &Path) -> std::io::Result<WireProxy> {
    let _ = std::fs::remove_dir_all("/tmp/opencode/wire");
    std::fs::create_dir_all("/tmp/opencode/wire")?;

    let cert_der = CertificateDer::from_pem_file(cert).expect("wireproxy cert");
    let key_der = PrivateKeyDer::from_pem_file(key).expect("wireproxy key");

    let mut server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("wireproxy server cert");
    server_cfg.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = Arc::new(tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg)));

    let der = CertificateDer::from_pem_file(cert).expect("wireproxy root");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(der).expect("wireproxy root store");
    let mut client_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_cfg.alpn_protocols = vec![b"h2".to_vec()];
    let connector = Arc::new(tokio_rustls::TlsConnector::from(Arc::new(client_cfg)));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            let Ok((client_sock, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let connector = connector.clone();
            tokio::spawn(async move {
                let Ok(tls_client) = acceptor.accept(client_sock).await else {
                    eprintln!("WIRE client leg failed");
                    return;
                };
                let Ok(tls_server) = connector
                    .connect(
                        ServerName::try_from("localhost".to_owned()).expect("sni"),
                        tokio::net::TcpStream::connect(upstream)
                            .await
                            .expect("wireproxy dial"),
                    )
                    .await
                else {
                    eprintln!("WIRE server leg failed");
                    return;
                };
                let c2s = std::fs::File::create("/tmp/opencode/wire/c2s.bin").expect("c2s log");
                let s2c = std::fs::File::create("/tmp/opencode/wire/s2c.bin").expect("s2c log");
                let (cr, cw) = tokio::io::split(tls_client);
                let (sr, sw) = tokio::io::split(tls_server);
                let up = pump(cr, sw, c2s);
                let down = pump(sr, cw, s2c);
                tokio::select! {
                    _ = up => {}
                    _ = down => {}
                }
            });
        }
    });

    Ok(WireProxy { addr })
}

/// Tees every byte read from (and written to) an inner stream into a
/// log file. Wrapping the client's TLS stream captures the exact
/// plaintext the h2 parser consumes, in direct (non-proxy) mode.
pub struct TeeStream<T> {
    inner: T,
    log: std::fs::File,
}

impl<T> TeeStream<T> {
    /// Wraps `inner`, mirroring its read bytes into `log`.
    pub fn new(inner: T, log: std::fs::File) -> Self {
        Self { inner, log }
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for TeeStream<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                let filled = buf.filled();
                if filled.len() > before {
                    use std::io::Write as _;
                    let _ = self.log.write_all(&filled[before..]);
                    let _ = self.log.flush();
                }
                std::task::Poll::Ready(Ok(()))
            }
            p => p,
        }
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for TeeStream<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Copies `read` into `write`, appending every byte to `log`.
async fn pump<R, W>(mut read: R, mut write: W, mut log: std::fs::File) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use std::io::Write as _;
    let mut buf = [0u8; 16384];
    loop {
        let n = read.read(&mut buf).await?;
        if n == 0 {
            let _ = write.shutdown().await;
            return Ok(());
        }
        log.write_all(&buf[..n]).ok();
        log.flush().ok();
        write.write_all(&buf[..n]).await?;
        write.flush().await?;
    }
}
