//! SCM_RIGHTS fd passing over a Unix socket — the hot-upgrade artery.

use std::io::{self, IoSliceMut};
use std::os::fd::AsRawFd;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
use std::path::{Path, PathBuf};

/// Errors from fd passing.
#[derive(Debug, thiserror::Error)]
pub enum FdPassError {
    /// Socket setup/connect failure.
    #[error("handover socket: {0}")]
    Socket(String),
    /// Send/recv failure.
    #[error("fd transfer: {0}")]
    Io(#[from] io::Error),
}

/// Sends `fds` to the receiver listening at `path`.
///
/// # Errors
/// Connect or kernel transfer failure.
pub fn send_fds(path: &Path, fds: &[RawFd]) -> Result<(), FdPassError> {
    let stream = StdUnixStream::connect(path)
        .map_err(|e| FdPassError::Socket(format!("connect {path:?}: {e}")))?;
    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: std::ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };
    // One 1-byte data buffer (SCM_RIGHTS requires at least some data).
    let mut byte = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    msg.msg_iov = std::ptr::addr_of_mut!(iov);
    msg.msg_iovlen = 1;

    let cmsg_space = cmsg_space(fds.len());
    let mut cmsg_buf = vec![0u8; cmsg_space];
    msg.msg_control = cmsg_buf.as_mut_ptr().cast();
    msg.msg_controllen = cmsg_space as _;

    // SAFETY: cmsg buffer sized with CMSG_SPACE for fds.len() ints.
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(std::ptr::addr_of!(msg)) };
    // SAFETY: header exists (space reserved); fields valid for SCM_RIGHTS.
    unsafe {
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(fds) as u32) as _;
        std::ptr::copy_nonoverlapping(
            fds.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(cmsg).cast(),
            std::mem::size_of_val(fds),
        );
    }
    // SAFETY: msghdr fields and cmsg buffer are correctly sized above.
    let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if sent < 0 {
        return Err(io::Error::last_os_error().into());
    }
    drop(stream);
    Ok(())
}

/// Receives up to `max` fds. Returns `(fds, handshake_byte)`.
///
/// # Errors
/// Bind/accept or kernel transfer failure.
pub fn recv_fds(path: &Path, max: usize) -> Result<(Vec<RawFd>, u8), FdPassError> {
    let listener = StdUnixListener::bind(path)
        .map_err(|e| FdPassError::Socket(format!("bind {path:?}: {e}")))?;
    // TODO(milestone+): bound the accept with a deadline + stale-socket
    // cleanup; the launcher unlinks the path before old binary exits.
    let (stream, _) = listener
        .accept()
        .map_err(|e| FdPassError::Socket(e.to_string()))?;

    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: std::ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };
    let mut byte = [0u8; 1];
    let mut iov = [IoSliceMut::new(&mut byte)];
    msg.msg_iov = iov.as_mut_ptr().cast();
    msg.msg_iovlen = 1;

    let cmsg_space = cmsg_space(max);
    let mut cmsg_buf = vec![0u8; cmsg_space];
    msg.msg_control = cmsg_buf.as_mut_ptr().cast();
    msg.msg_controllen = cmsg_space as _;

    // SAFETY: msghdr fields and cmsg buffer are correctly sized above.
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error().into());
    }

    let mut fds = Vec::new();
    // SAFETY: iterate control messages within msg_controllen.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(std::ptr::addr_of!(msg));
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg);
                let len = (*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                let count = len / std::mem::size_of::<RawFd>();
                for i in 0..count {
                    let fd = data
                        .add(i * std::mem::size_of::<RawFd>())
                        .cast::<RawFd>()
                        .read();
                    fds.push(fd);
                }
            }
            cmsg = libc::CMSG_NXTHDR(std::ptr::addr_of!(msg), cmsg);
        }
    }
    Ok((fds, byte[0]))
}

fn cmsg_space(n: usize) -> usize {
    // SAFETY: macro over constant sizes; no dereference.
    unsafe { libc::CMSG_SPACE((std::mem::size_of::<RawFd>() * n) as u32) as usize }
}

// Re-export for stream wrapping at the call sites.
#[allow(unused_imports)]
use std::os::fd::FromRawFd as _FromRawFd;

/// Wraps a raw fd (from `recv_fds`) into a std TcpListener.
///
/// # Safety
/// `fd` must be a listening TCP socket transferred via SCM_RIGHTS.
pub unsafe fn tcp_listener_from_fd(fd: RawFd) -> std::net::TcpListener {
    // SAFETY: caller contract: exactly-once ownership of a listening socket.
    unsafe { std::net::TcpListener::from_raw_fd(fd) }
}

/// Default handover socket path.
#[must_use]
pub fn default_socket_path() -> PathBuf {
    std::env::var("VANE_HANDOVER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/vane-handover.sock"))
}
