//! ACME e2e against a dedicated pebble instance (`acme-pebble` feature).
//!
//! Marked `#[ignore]` in the default run: requires Docker. Run with
//! `cargo test --features acme-pebble --test acme_pebble -- --ignored`.
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
#[ignore = "requires docker (pebble + challtestsrv containers)"]
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

    // Reserve ephemeral ports up-front (bind-then-drop) so concurrent
    // runs / foreign services on this shared host can't collide.
    let reserve = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        l.local_addr().expect("addr").port()
    };
    let mgmt_port = reserve(); // pebble ACME directory (default 14000)
    let http01_port = reserve(); // challtestsrv HTTP-01 (default 5002)
    let api_port = reserve(); // challtestsrv management (default 8055)

    // Pebble config: custom directory port + the HTTP-01 validation port
    // the VA dials (`httpPort`). Cert/key paths are the stock test certs
    // shipped inside the image.
    let pebble_cfg = format!(
        r#"{{"pebble":{{"listenAddress":"0.0.0.0:{mgmt_port}","certificate":"test/certs/localhost/cert.pem","privateKey":"test/certs/localhost/key.pem","httpPort":{http01_port}}}}}"#
    );
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let cfg_path = cfg_dir.path().join("pebble-config.json");
    std::fs::write(&cfg_path, pebble_cfg).expect("write pebble config");
    let cfg_mount = format!("{}:/test/pebble-config.json", cfg_path.display());

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
            "-http01",
            &format!(":{http01_port}"),
            "-management",
            &format!(":{api_port}"),
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
            "-v",
            &cfg_mount,
            "ghcr.io/letsencrypt/pebble:latest",
            "-config",
            "/test/pebble-config.json",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("start pebble");
    assert!(pebble.success(), "pebble failed to start");

    // Readiness: wait for the ACME directory.
    let dir_url = format!("https://127.0.0.1:{mgmt_port}/dir");
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
                &dir_url,
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
        directory_url: dir_url,
        emails: vec!["ci@example.com".into()],
        domains: vec!["localhost".into()],
        storage: storage.path().to_path_buf(),
        renew_at_fraction: 2.0 / 3.0,
        insecure_tls: true,
        challenge_answer_url: Some(format!("http://127.0.0.1:{api_port}/add-http01")),
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
