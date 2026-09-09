//! ACME e2e against a dedicated pebble instance (`acme-pebble` feature).
//!
//! Starts its own pebble + challtestsrv containers (host network), drives
//! the real ACME client through a full issuance cycle, and cleans up.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use vane_control::acme::{AcmeConfig, AcmeManager};

/// Blocking cross-process test lock (flock on a temp file).
fn lock_serial() -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("vane-tests-serial.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .expect("open lock file");
    // SAFETY: flock on a regular file; released when the File drops.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(rc, 0, "flock");
    file
}

#[tokio::test]
async fn acme_issues_certificate_against_pebble() {
    let _lock = lock_serial();

    // Kill leaked vane children from prior failed runs.
    let _ = Command::new("pkill")
        .args(["-9", "-f", "target/debug/vane"])
        .output();

    // Clean slate + start dedicated pebble and challtestsrv (host network).
    let container = format!("vane-pebble-acme-{}", std::process::id());
    let chall = format!("vane-challs-acme-{}", std::process::id());
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .output();
    let _ = Command::new("docker").args(["rm", "-f", &chall]).output();

    let chall_up = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &chall,
            "--network",
            "host",
            "ghcr.io/letsencrypt/pebble-challtestsrv:latest",
            "-defaultIPv4",
            "",
            "-defaultIPv6",
            "",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("start challtestsrv");
    assert!(chall_up.success(), "challtestsrv failed to start");

    let pebble = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &container,
            "--network",
            "host",
            "ghcr.io/letsencrypt/pebble:latest",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("start pebble");
    assert!(pebble.success(), "pebble failed to start");

    // Readiness: wait for the ACME directory.
    let mut ready = false;
    for _ in 0..40 {
        if let Ok(out) = Command::new("curl")
            .args([
                "-sk",
                "--max-time",
                "2",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                "https://127.0.0.1:14000/dir",
            ])
            .output()
        {
            let code = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if code == "200" {
                ready = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(ready, "pebble never became ready");

    // Drive the ACME client.
    let storage = tempfile::tempdir().expect("dir");
    let mgr = Arc::new(AcmeManager::new(AcmeConfig {
        directory_url: "https://127.0.0.1:14000/dir".into(),
        emails: vec!["ci@example.com".into()],
        domains: vec!["localhost".into()],
        storage: storage.path().to_path_buf(),
        renew_at_fraction: 2.0 / 3.0,
        insecure_tls: true,
        challenge_answer_url: Some("http://127.0.0.1:8055/add-http01".into()),
    }));

    let worker = Arc::clone(&mgr);
    let result = tokio::spawn(async move { worker.obtain_certificate().await })
        .await
        .expect("issuance task");
    assert!(result.is_ok(), "acme issuance failed: {result:?}");

    let cert = std::fs::read_to_string(storage.path().join("cert.pem")).expect("cert.pem written");
    assert!(cert.contains("BEGIN CERTIFICATE"));

    // Cleanup.
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .output();
    let _ = Command::new("docker").args(["rm", "-f", &chall]).output();
}
