//! vane — deterministic L4/L7 reverse proxy.
//!
//! CLI entry: `vane run`, `vane validate`, `vane hot-standby`.

use clap::{Parser, Subcommand};

/// vane — L4/L7 reverse proxy, edge gateway, micro sidecar.
#[derive(Debug, Parser)]
#[command(name = "vane", version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the proxy.
    Run {
        /// Config file (TOML); falls back to `VANE_CONFIG` / `vane.toml`.
        #[arg(short, long)]
        config: Option<String>,
        /// Receive listeners from a previous instance (hot upgrade).
        #[arg(long)]
        handover_from: Option<String>,
        /// On shutdown, hand listeners + routes to a standby instance
        /// running with --handover-from (hot upgrade, zero resets).
        #[arg(long)]
        handover_to: Option<String>,
        /// Force the mio engine (disable io_uring).
        #[arg(long)]
        force_mio: bool,
    },
    /// Validate a config file and exit.
    Validate {
        #[arg(short, long)]
        config: String,
    },
    /// Run the SHM sidecar server (bridges shared-memory calls to HTTP).
    Sidecar {
        #[arg(short, long)]
        config: Option<String>,
        /// Sidecar transport base path.
        #[arg(long, default_value = "/dev/shm/vane-sidecar")]
        base: String,
    },
}

fn main() {
    vane_tls::install_crypto_provider();
    // Telemetry (tracing subscriber, optional OTLP) is owned by the
    // server/sidecar entry points from their config — installing a fmt
    // subscriber here would shadow OTLP via try_init conflicts.
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Run {
            config,
            handover_from,
            handover_to,
            force_mio,
        } => cmd_run(config, handover_from, handover_to, force_mio),
        Cmd::Validate { config } => cmd_validate(&config),
        Cmd::Sidecar { config, base } => cmd_sidecar(config, base),
    };
    std::process::exit(code);
}

/// `vane run` body (separated for testability).
fn cmd_run(
    config: Option<String>,
    handover_from: Option<String>,
    handover_to: Option<String>,
    force_mio: bool,
) -> i32 {
    cmd_run_with_shutdown(config, handover_from, handover_to, force_mio, None)
}

/// `vane run` with an optional shutdown delay (tests bound the run).
fn cmd_run_with_shutdown(
    config: Option<String>,
    handover_from: Option<String>,
    handover_to: Option<String>,
    force_mio: bool,
    shutdown_after: Option<std::time::Duration>,
) -> i32 {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        vane::server::run(vane::server::RunOptions {
            config_path: config,
            handover_from,
            handover_to,
            shutdown_after,
            force_mio,
        })
        .await
    })
}

/// `vane validate` body: parse + semantic-validate, print verdict.
fn cmd_validate(config: &str) -> i32 {
    match std::fs::read_to_string(config)
        .map_err(|e| e.to_string())
        .and_then(|t| {
            vane_control::VaneConfig::parse_toml(&t)
                .and_then(|c| c.validate().map(|()| c))
                .map_err(|e| e.to_string())
        }) {
        Ok(_) => {
            println!("{config}: OK");
            0
        }
        Err(e) => {
            eprintln!("{config}: INVALID: {e}");
            1
        }
    }
}

/// `vane sidecar` body.
fn cmd_sidecar(config: Option<String>, base: String) -> i32 {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move { vane::sidecar::run(config, base).await })
}

#[cfg(test)]
mod cli_tests {
    use super::*;
    use clap::CommandFactory as _;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parse_run_with_config() {
        let cli = Cli::try_parse_from(["vane", "run", "-c", "/etc/vane.toml"]).expect("parse");
        match cli.cmd {
            Cmd::Run {
                config,
                handover_from,
                handover_to,
                force_mio,
            } => {
                assert_eq!(config.as_deref(), Some("/etc/vane.toml"));
                assert!(handover_from.is_none());
                assert!(handover_to.is_none());
                assert!(!force_mio);
            }
            other => panic!("wrong command: {other:?}"),
        }
    }

    #[test]
    fn parse_run_hot_upgrade_flags() {
        let cli = Cli::try_parse_from([
            "vane",
            "run",
            "--handover-from",
            "/tmp/hs.sock",
            "--handover-to",
            "/tmp/hs2.sock",
            "--force-mio",
        ])
        .expect("parse");
        match cli.cmd {
            Cmd::Run {
                handover_from,
                handover_to,
                force_mio,
                ..
            } => {
                assert_eq!(handover_from.as_deref(), Some("/tmp/hs.sock"));
                assert_eq!(handover_to.as_deref(), Some("/tmp/hs2.sock"));
                assert!(force_mio);
            }
            other => panic!("wrong command: {other:?}"),
        }
    }

    #[test]
    fn parse_validate_requires_config() {
        let err = Cli::try_parse_from(["vane", "validate"]).expect_err("missing --config");
        assert!(
            err.kind() == clap::error::ErrorKind::InvalidValue
                || err.kind() == clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn parse_sidecar_default_base() {
        let cli = Cli::try_parse_from(["vane", "sidecar"]).expect("parse");
        match cli.cmd {
            Cmd::Sidecar { base, .. } => {
                assert_eq!(base, "/dev/shm/vane-sidecar");
            }
            other => panic!("wrong command: {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_unknown_subcommand() {
        assert!(Cli::try_parse_from(["vane", "explode"]).is_err());
    }

    #[test]
    fn validate_good_config_exits_zero() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("good.toml");
        std::fs::write(
            &path,
            r#"
[[listeners]]
address = "127.0.0.1:0"

[clusters.up]
backends = ["127.0.0.1:9"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false
"#,
        )
        .expect("write");
        let code = super::cmd_validate(path.to_str().expect("utf8"));
        assert_eq!(code, 0);
    }

    #[test]
    fn validate_bad_config_exits_one() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not [[ valid toml").expect("write");
        let code = super::cmd_validate(path.to_str().expect("utf8"));
        assert_eq!(code, 1);
    }

    #[test]
    fn validate_missing_file_exits_one() {
        assert_eq!(super::cmd_validate("/nonexistent/vane/config.toml"), 1);
    }

    #[test]
    fn cmd_run_starts_and_shuts_down() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("run.toml");
        std::fs::write(
            &path,
            r#"
[[listeners]]
address = "127.0.0.1:0"

[clusters.up]
backends = ["127.0.0.1:9"]

[[routes]]
pattern = "/*rest"
cluster = "up"

[admin]
enabled = false

[runtime]
force_mio = true
"#,
        )
        .expect("write");
        let code = super::cmd_run_with_shutdown(
            Some(path.to_str().expect("utf8").to_owned()),
            None,
            None,
            true,
            Some(std::time::Duration::from_millis(200)),
        );
        assert_eq!(code, 0);
    }

    #[test]
    fn cmd_run_bad_config_exits_one() {
        let code = super::cmd_run_with_shutdown(
            Some("/nonexistent/vane.toml".into()),
            None,
            None,
            true,
            Some(std::time::Duration::from_millis(100)),
        );
        assert_eq!(code, 1);
    }
}

#[cfg(test)]
mod sidecar_cmd_tests {

    #[test]
    fn cmd_sidecar_no_backends_exits_one() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("sc.toml");
        // A cluster with no resolvable backends: the bridge cannot start.
        std::fs::write(
            &path,
            r#"
[clusters.empty]
backends = []

[sidecar]
enabled = false
base = "/nonexistent-vane-sidecar-test"
"#,
        )
        .expect("write");
        let code = super::cmd_sidecar(
            Some(path.to_str().expect("utf8").to_owned()),
            "/nonexistent-vane-sidecar-test".to_owned(),
        );
        assert_eq!(code, 1, "sidecar without backends must fail fast");
    }
}
