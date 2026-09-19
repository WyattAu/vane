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
    /// Watch Gateway API resources and drive a vane admin plane.
    GatewayOperator {
        /// Kubernetes API server base URL.
        #[arg(long, default_value = "https://kubernetes.default.svc")]
        api_server: String,
        /// Namespace to watch (v0.2: single namespace).
        #[arg(long, default_value = "default")]
        namespace: String,
        /// Path to the service-account token.
        #[arg(
            long,
            default_value = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )]
        token_path: String,
        /// vane admin plane base URL (receives xDS snapshots).
        #[arg(long, default_value = "http://127.0.0.1:7900")]
        admin: String,
        /// Poll interval seconds (poll mode).
        #[arg(long, default_value = "5")]
        poll_secs: u64,
        /// Watch mode: consume the API server's ?watch=true streams and
        /// push a snapshot on every change (replaces polling).
        #[arg(long, default_value_t = false)]
        watch: bool,
        /// Cover every namespace the RBAC grants (cluster-scoped
        /// list/watch paths) instead of one namespace.
        #[arg(long, default_value_t = false)]
        all_namespaces: bool,
    },
    /// Run an ADS (xDS) client against an Envoy-compatible management
    /// server and drive a vane admin plane with decoded snapshots.
    XdsClient {
        /// Management server address (`host:port`, h2c prior
        /// knowledge).
        #[arg(long)]
        management: String,
        /// Node id for the xDS handshake.
        #[arg(long, default_value = "vane-edge")]
        node_id: String,
        /// vane admin plane base URL (receives xDS snapshots).
        #[arg(long, default_value = "http://127.0.0.1:7900")]
        admin: String,
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
        Cmd::GatewayOperator {
            api_server,
            namespace,
            token_path,
            admin,
            poll_secs,
            watch,
            all_namespaces,
        } => cmd_gateway_operator(
            api_server,
            namespace,
            token_path,
            admin,
            poll_secs,
            watch,
            all_namespaces,
        ),
        Cmd::XdsClient {
            management,
            node_id,
            admin,
        } => cmd_xds_client(&management, &node_id, &admin),
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

/// `vane xds-client` body: delegates to the library loop (see
/// [`vane::xds_client::xds_client_loop`]).
fn cmd_xds_client(management: &str, node_id: &str, admin: &str) -> i32 {
    vane::xds_client::xds_client_loop(management, node_id, admin)
}

/// `vane gateway-operator` body: polls the K8s API for Gateway API
/// resources, compiles them, and POSTs xDS snapshots to the vane admin
/// plane. v0.2: poll-based, single namespace, SA-token auth.
fn cmd_gateway_operator(
    api_server: String,
    namespace: String,
    token_path: String,
    admin: String,
    poll_secs: u64,
    watch: bool,
    all_namespaces: bool,
) -> i32 {
    use std::io::BufRead as _;
    use std::sync::Mutex as StdMutex;
    use vane_control::gateway::GatewayState;
    use vane_control::gateway::compile;
    use vane_control::operator::{ResourceKind, WatchCache};

    let token = std::fs::read_to_string(&token_path)
        .map(|t| t.trim().to_owned())
        .unwrap_or_default();
    let client = reqwest::blocking::Client::builder()
        // The K8s API serves a cluster-specific CA; the SA token
        // authenticates and the operator pins nothing else.
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("http client");
    let scope = |path: &str| {
        if all_namespaces {
            format!("{api_server}{path}")
        } else {
            format!("{api_server}/namespaces/{namespace}{path}")
        }
    };
    let routes_url = scope(vane_control::operator::HTTPROUTES_PATH);
    let gateways_url = scope(vane_control::operator::GATEWAYS_PATH);
    let snapshot_url = format!("{admin}/xds/snapshot");

    if watch {
        eprintln!(
            "gateway-operator: watch mode ns={namespace}{} api={api_server} admin={admin}",
            if all_namespaces { " (all)" } else { "" },
        );
        let cache = std::sync::Arc::new(std::sync::Mutex::new(WatchCache::default()));
        let dirty = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();
        for (kind, url) in [
            (
                ResourceKind::HttpRoute,
                scope(vane_control::operator::HTTPROUTES_WATCH_PATH),
            ),
            (
                ResourceKind::Gateway,
                scope(vane_control::operator::GATEWAYS_WATCH_PATH),
            ),
        ] {
            let client = client.clone();
            let token = token.clone();
            let cache = std::sync::Arc::clone(&cache);
            let dirty = std::sync::Arc::clone(&dirty);
            handles.push(std::thread::spawn(move || {
                loop {
                    let resp = client
                        .get(&url)
                        .header("Authorization", format!("Bearer {token}"))
                        .send();
                    let Ok(resp) = resp else {
                        eprintln!("gateway-operator: watch connect failed; retrying");
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        continue;
                    };
                    if !resp.status().is_success() {
                        eprintln!(
                            "gateway-operator: watch rejected: {} (RBAC?); retrying",
                            resp.status()
                        );
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        continue;
                    }
                    let mut reader = std::io::BufReader::new(resp);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line) {
                            Ok(0) | Err(_) => break, // stream ended: reconnect
                            Ok(_) => {
                                let changed = match StdMutex::lock(&cache) {
                                    Ok(mut c) => c.apply(kind, line.trim()).unwrap_or(false),
                                    Err(_) => false,
                                };
                                if changed {
                                    dirty.store(true, std::sync::atomic::Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    eprintln!("gateway-operator: watch stream ended; reconnecting");
                }
            }));
        }
        // Snapshot poster: push on change, at most once per second.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if !dirty.swap(false, std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            let body = match StdMutex::lock(&cache) {
                Ok(c) => serde_json::to_string(&compile(&c.state())).map_err(|e| e.to_string()),
                Err(_) => Err("cache poisoned".to_string()),
            };
            match body {
                Ok(body) => match client
                    .post(&snapshot_url)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                {
                    Ok(r) if r.status().is_success() => {
                        eprintln!("gateway-operator: snapshot applied");
                    }
                    Ok(r) => {
                        eprintln!("gateway-operator: snapshot rejected: {}", r.status());
                    }
                    Err(e) => {
                        eprintln!("gateway-operator: admin unreachable: {e}");
                        dirty.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                _ => eprintln!("gateway-operator: compile failed"),
            }
        }
    }

    eprintln!("gateway-operator: ns={namespace} api={api_server} admin={admin} poll={poll_secs}s");
    loop {
        match (
            fetch_k8s_json(&client, &routes_url, &token),
            fetch_k8s_json(&client, &gateways_url, &token),
        ) {
            (Ok(routes_json), Ok(gateways_json)) => {
                let compiled = (|| -> Result<String, String> {
                    let state = GatewayState {
                        gateways: vane_control::operator::map_gateways(&gateways_json)?,
                        httproutes: vane_control::operator::map_httproutes(
                            &routes_json,
                            &namespace,
                        )?,
                    };
                    let snapshot = compile(&state)?;
                    serde_json::to_string(&snapshot).map_err(|e| e.to_string())
                })();
                match compiled {
                    Ok(body) => {
                        let resp = client
                            .post(&snapshot_url)
                            .header("content-type", "application/json")
                            .body(body)
                            .send();
                        match resp {
                            Ok(r) if r.status().is_success() => {
                                eprintln!("gateway-operator: snapshot applied");
                            }
                            Ok(r) => {
                                let status = r.status();
                                let text = r.text().unwrap_or_default();
                                eprintln!("gateway-operator: snapshot rejected: {status} {text}");
                            }
                            Err(e) => {
                                eprintln!("gateway-operator: admin unreachable: {e}");
                            }
                        }
                    }
                    Err(e) => eprintln!("gateway-operator: compile: {e}"),
                }
            }
            (Err(e), _) | (_, Err(e)) => {
                eprintln!("gateway-operator: fetch: {e}");
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(poll_secs));
    }
}

/// GETs a K8s API list endpoint with SA-token auth.
fn fetch_k8s_json(
    client: &reqwest::blocking::Client,
    url: &str,
    token: &str,
) -> Result<String, String> {
    let resp = client
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    let body = resp.text().map_err(|e| e.to_string())?;
    if !status.is_success() {
        let head: String = body.chars().take(200).collect();
        return Err(format!("{status}: {head}"));
    }
    Ok(body)
}

#[test]
fn xds_client_rejects_unresolvable_management() {
    // 999.999.999.999 fails `to_socket_addrs` without any network.
    let code = vane::xds_client::xds_client_loop("999.999.999.999:1", "n", "http://127.0.0.1:1");
    assert_eq!(code, 1);
}

#[test]
fn run_reports_config_load_failure() {
    let code = cmd_run(
        Some("/nonexistent/vane-definitely-missing.toml".into()),
        None,
        None,
        true,
    );
    assert_eq!(code, 1);
}
