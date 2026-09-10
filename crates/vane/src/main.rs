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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Run {
            config,
            handover_from,
            handover_to,
            force_mio,
        } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(async move {
                vane::server::run(vane::server::RunOptions {
                    config_path: config,
                    handover_from,
                    handover_to,
                    shutdown_after: None,
                    force_mio,
                })
                .await
            })
        }
        Cmd::Validate { config } => {
            match std::fs::read_to_string(&config)
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
        Cmd::Sidecar { config, base } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(async move { vane::sidecar::run(config, base).await })
        }
    };
    std::process::exit(code);
}
