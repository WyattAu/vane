//! Runtime wiring: bind, spawn workers, start the control plane, admin
//! server, and graceful shutdown.

use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::time::Duration;

use vane_control::config::ListenerMode;
use vane_control::{Reconciler, VaneConfig};
use vane_core::handler::Mode as CoreMode;
use vane_core::{WorkerCmd, WorkerConfig, spawn_worker};
use vane_observe::metrics::Registry;
use vane_observe::ring::EventRing;
use vane_router::Router;

use crate::proxy::HttpProxy;

/// Options for [`run`].
pub struct RunOptions {
    /// Config file path (falls back to `VANE_CONFIG` / `vane.toml`).
    pub config_path: Option<String>,
    /// Hot-upgrade: receive listeners from this handover socket.
    pub handover_from: Option<String>,
    /// Force the mio engine.
    pub force_mio: bool,
}

/// Loads config from the given path or the default locations.
///
/// # Errors
/// Missing file or invalid TOML.
pub fn load_config(path: Option<&str>) -> Result<VaneConfig, String> {
    let path = path
        .map(std::string::ToString::to_string)
        .or_else(|| std::env::var("VANE_CONFIG").ok())
        .unwrap_or_else(|| "vane.toml".to_owned());
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let cfg = VaneConfig::parse_toml(&text).map_err(|e| e.to_string())?;
    cfg.validate().map_err(|e| e.to_string())?;
    Ok(cfg)
}

/// Runs the proxy until shutdown. Returns the process exit code.
#[allow(clippy::too_many_lines)]
pub async fn run(opts: RunOptions) -> i32 {
    // Telemetry (control plane; the data plane logs through its rings).
    let telemetry = otelkit::TelemetryConfig::from_env().ok();
    let _otel_guard = telemetry
        .as_ref()
        .and_then(|c| otelkit::init(c.clone()).ok());

    let config = match load_config(opts.config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vane: {e}");
            return 1;
        }
    };
    if config.listeners.is_empty() {
        eprintln!("vane: no listeners configured");
        return 1;
    }

    let registry = Arc::new(Registry::new());
    let router = Arc::new(Router::new());
    let health = Arc::new(vane_control::HealthMap::new());

    // ---- Control plane (tokio) ------------------------------------------
    #[cfg_attr(
    not(any(feature = "file-provider", feature = "docker-provider", feature = "k8s")),
    allow(unused_variables)
)]
let (update_tx, update_rx) = tokio::sync::mpsc::channel(64);
    let mut reconciler = Reconciler::new(
        Arc::clone(&router),
        Arc::clone(&health),
        Arc::clone(&registry),
        update_rx,
    );

    // Static routes + health probe paths.
    reconciler.publish_static(&config);
    let mut checker = vane_control::HealthChecker::new(Arc::clone(&health), Duration::from_secs(5));
    for (name, cluster) in &config.clusters {
        if let Some(path) = &cluster.health_path {
            for b in &cluster.backends {
                if let Ok(addr) = b.parse::<std::net::SocketAddr>() {
                    checker.set_http_probe(addr, path.clone());
                } else if let Ok(addr) = format!("127.0.0.1:{b}").parse() {
                    checker.set_http_probe(addr, path.clone());
                }
            }
        }
        let _ = name;
    }
    let checker = Arc::new(checker);
    checker.spawn();
    let _reconcile_task = tokio::spawn(reconciler.run());

    // File provider.
    #[cfg(feature = "file-provider")]
    if let Some(dir) = &config.file_provider.directory {
        let provider = vane_control::FileProvider::new(dir.clone(), config.file_provider.poll_ms);
        match provider.spawn(Arc::clone(&health), update_tx.clone()) {
            Ok(_watcher) => tracing::info!("file provider watching {dir:?}"),
            Err(e) => tracing::warn!("file provider failed: {e}"),
        }
    }

    // Docker provider.
    #[cfg(feature = "docker-provider")]
    if config.docker.enabled {
        let provider =
            vane_control::DockerProvider::new(config.docker.socket.clone(), Duration::from_secs(2));
        provider.spawn(Arc::clone(&health), update_tx.clone());
    }

    // Kubernetes provider.
    #[cfg(feature = "k8s")]
    if config.kubernetes.enabled {
        if let Err(e) = vane_control::providers::k8s::K8sProvider::new(
            config.kubernetes.api_server.clone(),
            config.kubernetes.namespaces.clone(),
        ) {
            tracing::warn!("k8s provider: {e}");
        } else {
            let provider = vane_control::providers::k8s::K8sProvider::new(
                config.kubernetes.api_server.clone(),
                config.kubernetes.namespaces.clone(),
            )
            .expect("provider built above");
            provider.spawn(Arc::clone(&health), update_tx.clone());
        }
    }

    // ---- Bind listeners (or inherit via hot upgrade) --------------------
    let mut bound: Vec<StdTcpListener> = Vec::new();
    if let Some(sock) = &opts.handover_from {
        let state_path = std::path::PathBuf::from("/tmp/vane-handover-state.json");
        match vane_shm::handover::receive_listeners(
            std::path::Path::new(sock),
            &state_path,
            config.listeners.len(),
        ) {
            Ok(received) => {
                tracing::info!(
                    "hot upgrade: inherited {} listeners, generation {}",
                    received.listeners.len(),
                    received.state.generation
                );
                bound = received.listeners;
                // Seed the router from the archived state so routes serve
                // before providers reconcile.
                let records = received.state.routes;
                let health2 = Arc::clone(&health);
                router.update(|editor| {
                    for r in records {
                        let backends: Vec<vane_router::Backend> = r
                            .backends
                            .iter()
                            .filter_map(|b| b.parse::<std::net::SocketAddr>().ok())
                            .map(|addr| {
                                let mut be = vane_router::Backend::new(addr, 1);
                                be.attach_health(health2.flag(addr));
                                be
                            })
                            .collect();
                        let builder = vane_router::RouteBuilder {
                            host: r.host,
                            pattern: r.pattern,
                            methods: Vec::new(),
                            cluster: r.cluster,
                            strip_prefix: r.strip_prefix,
                            timeout_ms: None,
                            backends,
                            policy: vane_router::Policy::P2C,
                            priority: 20,
                        };
                        match builder.compile() {
                            Ok(entry) => editor.insert(entry),
                            Err(e) => tracing::warn!("handover route: {e}"),
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("vane: handover failed: {e}");
                return 1;
            }
        }
    } else {
        for l in &config.listeners {
            let addr = l
                .address
                .parse::<std::net::SocketAddr>()
                .expect("validated");
            match vane_core::tcp_listener(addr, true, config.runtime.backlog) {
                Ok(l) => bound.push(l),
                Err(e) => {
                    eprintln!("vane: bind {addr}: {e}");
                    return 1;
                }
            }
        }
    }

    // ---- Spawn workers (one per listener × core) ------------------------
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let workers_per_listener = config
        .listeners
        .first()
        .map_or(1, |l| if l.workers == 0 { cores } else { l.workers });
    let mut handles = Vec::new();
    for (li, listener) in bound.into_iter().enumerate() {
        let mode = match config
            .listeners
            .get(li)
            .map_or(ListenerMode::Http, |l| l.mode)
        {
            ListenerMode::Http => CoreMode::Http,
            ListenerMode::Tcp => CoreMode::L4,
        };
        for w in 0..workers_per_listener {
            // SO_REUSEPORT lets each worker own a duplicate bind; the
            // kernel load-balances accepts across cores.
            let listener = if w == 0 {
                listener.try_clone().expect("clone listener")
            } else {
                let addr = config.listeners[li].address.parse().expect("validated");
                match vane_core::tcp_listener(addr, true, config.runtime.backlog) {
                    Ok(l) => l,
                    Err(_) => listener.try_clone().expect("clone listener"),
                }
            };
            let worker_cfg = WorkerConfig {
                core: Some(w % cores),
                pool_slots: config.runtime.pool_slots,
                ring_entries: config.runtime.ring_entries,
                sqpoll: config.runtime.sqpoll,
                force_mio: opts.force_mio || config.runtime.force_mio,
                max_sessions: config.runtime.max_sessions,
                backlog: config.runtime.backlog,
            };
            let factory = WorkerFactory {
                mode,
                router: Arc::clone(&router),
                registry: Arc::clone(&registry),
                worker_id: li * workers_per_listener + w,
                events: Arc::new(EventRing::new()),
            };
            let events = Arc::new(EventRing::new());
            match spawn_worker(
                li * workers_per_listener + w,
                worker_cfg,
                listener,
                Arc::clone(&registry),
                events,
                &factory,
            ) {
                Ok(h) => handles.push(h),
                Err(e) => {
                    eprintln!("vane: worker spawn: {e}");
                    return 1;
                }
            }
        }
    }

    // ---- Admin server ----------------------------------------------------
    if config.admin.enabled {
        let admin_addr: std::net::SocketAddr = config.admin.address.parse().expect("validated");
        let admin_router =
            crate::admin::build_admin_router(Arc::clone(&router), Arc::clone(&registry));
        tokio::spawn(async move {
            if let Err(e) = crate::admin::serve(admin_addr, admin_router).await {
                tracing::error!("admin server: {e}");
            }
        });
    }

    // ---- Shutdown ---------------------------------------------------------
    // SIGTERM/SIGINT -> drain workers with a 30 s hard deadline.
    wait_for_signal().await;

    tracing::info!("shutting down: draining workers");
    let deadline_ms: u64 = 30_000;
    for h in &handles {
        let _ = h.cmd.send(WorkerCmd::Shutdown { deadline_ms });
    }
    for h in &mut handles {
        h.join();
    }
    tracing::info!("vane stopped");
    0
}

/// Resolves when SIGTERM or SIGINT arrives (control-plane tokio context).
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int_signal = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int_signal.recv() => {}
    }
}

/// Builds the per-worker proxy handler.
struct WorkerFactory {
    mode: CoreMode,
    router: Arc<Router>,
    registry: Arc<Registry>,
    worker_id: usize,
    events: Arc<EventRing<vane_observe::LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
}

impl vane_core::HandlerFactory for WorkerFactory {
    fn mode(&self) -> CoreMode {
        self.mode
    }

    fn build(&self, _ctx: &vane_core::WorkerCtx) -> Box<dyn vane_core::Handler> {
        Box::new(HttpProxy::new(
            crate::proxy::ProxyConfig {
                router: Arc::clone(&self.router),
                registry: Arc::clone(&self.registry),
                events: Arc::clone(&self.events),
                rate_limit_rps: None,
                connect_timeout_ms: 5_000,
                idle_timeout_ms: 75_000,
            },
            self.worker_id,
        ))
    }
}
