//! Server error paths: `run()` returns non-zero exit codes instead of
//! panicking on bad configs / unroutable listeners.

use vane::server::RunOptions;

async fn run_with(config: Option<&str>) -> i32 {
    vane::server::run(RunOptions::for_test(config)).await
}

#[tokio::test]
async fn missing_config_returns_one() {
    let code = run_with(Some("/nonexistent/vane/definitely-missing.toml")).await;
    assert_eq!(code, 1);
}

#[tokio::test]
async fn empty_config_returns_one() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("empty.toml");
    std::fs::write(&path, "# nothing\n").expect("write");
    let code = run_with(Some(path.to_str().expect("utf8"))).await;
    assert_eq!(code, 1, "no listeners must be rejected");
}

#[tokio::test]
async fn invalid_toml_returns_one() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is not = = toml [[[").expect("write");
    let code = run_with(Some(path.to_str().expect("utf8"))).await;
    assert_eq!(code, 1);
}

#[tokio::test]
async fn unroutable_listener_returns_one() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("unroutable.toml");
    // Multicast address cannot be bound.
    std::fs::write(
        &path,
        r#"
[[listeners]]
address = "224.0.0.1:1"

[runtime]
force_mio = true
"#,
    )
    .expect("write");
    let code = run_with(Some(path.to_str().expect("utf8"))).await;
    assert_eq!(code, 1, "bind failure must surface as exit code 1");
}
