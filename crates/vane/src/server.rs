//! Runtime wiring: bind, spawn workers, start the control plane, admin
//! server, and graceful shutdown.

use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use std::collections::HashMap;
use vane_control::acme::AcmeManager;
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
    /// Hot-upgrade: on shutdown, send listeners + route state to this
    /// socket (the standby runs with `--handover-from`). Graceful plain
    /// drain if the peer is absent.
    pub handover_to: Option<String>,
    /// Test hook: trigger the shutdown path after this delay.
    pub shutdown_after: Option<Duration>,
    /// Force the mio engine.
    pub force_mio: bool,
}

/// Loads config from the given path or the default locations, then applies
/// `VANE_*` environment overrides (container-friendly layering):
///
/// - `VANE_LISTEN` — replaces the first listener's address
/// - `VANE_TLS_CERT` / `VANE_TLS_KEY` — TLS material for the first listener
/// - `VANE_ADMIN_ADDR` — admin bind address
/// - `VANE_CLUSTER_<NAME>` — comma-separated backends for cluster `<NAME>`
/// - `VANE_WORKERS` — workers per listener (0 = per core)
///
/// # Errors
/// Missing file or invalid TOML.
pub fn load_config(path: Option<&str>) -> Result<VaneConfig, String> {
    let path = path
        .map(std::string::ToString::to_string)
        .or_else(|| std::env::var("VANE_CONFIG").ok())
        .unwrap_or_else(|| "vane.toml".to_owned());
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let mut cfg = VaneConfig::parse_toml(&text).map_err(|e| e.to_string())?;

    if let Ok(addr) = std::env::var("VANE_LISTEN") {
        if let Some(l) = cfg.listeners.first_mut() {
            l.address = addr;
        } else {
            cfg.listeners.push(vane_control::ListenerConfig {
                address: addr,
                mode: vane_control::config::ListenerMode::Http,
                workers: 0,
                tls: None,
            });
        }
    }
    if let (Ok(cert), Ok(key)) = (
        std::env::var("VANE_TLS_CERT"),
        std::env::var("VANE_TLS_KEY"),
    ) {
        if let Some(l) = cfg.listeners.first_mut() {
            l.tls = Some(vane_control::ListenerTls {
                cert,
                key,
                alpn_h2: false,
            });
        }
    }
    if let Ok(addr) = std::env::var("VANE_ADMIN_ADDR") {
        cfg.admin.address = addr;
        cfg.admin.enabled = true;
    }
    for (name, cluster) in cfg.clusters.iter_mut() {
        let key = format!("VANE_CLUSTER_{}", name.to_uppercase());
        if let Ok(backends) = std::env::var(key) {
            cluster.backends = backends
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect();
        }
    }
    if let Ok(w) = std::env::var("VANE_WORKERS") {
        if let Ok(n) = w.parse::<usize>() {
            for l in &mut cfg.listeners {
                l.workers = n;
            }
        }
    }

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
        not(any(
            feature = "file-provider",
            feature = "docker-provider",
            feature = "k8s"
        )),
        allow(unused_variables)
    )]
    let (update_tx, update_rx) = tokio::sync::mpsc::channel(64);
    let mut reconciler = Reconciler::new(
        Arc::clone(&router),
        Arc::clone(&health),
        Arc::clone(&registry),
        update_rx,
    );

    // ---- ACME manager (certificate renewal + HTTP-01 answers) ----------
    let http01_tokens: Option<Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>> =
        if !config.acme.domains.is_empty() {
            let storage = config
                .acme
                .storage_dir
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from("/var/lib/vane/acme"));
            let acme_cfg = vane_control::acme::AcmeConfig {
                directory_url: config
                    .acme
                    .directory_url
                    .clone()
                    .unwrap_or_else(|| "https://acme-v02.api.letsencrypt.org/directory".into()),
                emails: config.acme.emails.clone(),
                domains: config
                    .acme
                    .domains
                    .iter()
                    .map(|d| d.domain.clone())
                    .collect(),
                storage: storage.clone(),
                renew_at_fraction: 2.0 / 3.0,
                insecure_tls: config.acme.insecure_tls,
                challenge_answer_url: None,
            };
            let mgr = Arc::new(AcmeManager::new(acme_cfg));
            mgr.clone().spawn_renewal();
            tracing::info!(
                "acme: managing {:?} (storage {})",
                config
                    .acme
                    .domains
                    .iter()
                    .map(|d| &d.domain)
                    .collect::<Vec<_>>(),
                storage.display()
            );
            Some(mgr.http01_tokens())
        } else {
            None
        };

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
    // Keep dup'd listener fds for the hot-upgrade sender (SCM_RIGHTS needs
    // an owned fd at shutdown; the workers get their own clones).
    let handover_listeners: Vec<StdTcpListener> = bound
        .iter()
        .map(|l| l.try_clone().expect("dup listener"))
        .collect();
    let inherited = opts.handover_from.is_some();
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let workers_per_listener = config
        .listeners
        .first()
        .map_or(1, |l| if l.workers == 0 { cores } else { l.workers });
    let mut handles = Vec::new();
    let mut tls_cfg_slots: Vec<Option<crate::tls_reload::TlsSlot>> = Vec::new();
    let mut event_rings: Vec<
        Arc<EventRing<vane_observe::LogEvent, { vane_observe::EVENT_RING_CAPACITY }>>,
    > = Vec::new();
    for (li, listener) in bound.into_iter().enumerate() {
        let mode = match config
            .listeners
            .get(li)
            .map_or(ListenerMode::Http, |l| l.mode)
        {
            ListenerMode::Http => CoreMode::Http,
            ListenerMode::Tcp => CoreMode::L4,
        };
        let want_h2 = cfg!(feature = "h2")
            && config
                .listeners
                .get(li)
                .and_then(|l| l.tls.as_ref())
                .is_some_and(|t| t.alpn_h2);
        // TLS termination material for this listener: shared, swappable
        // slot (hot reload replaces the inner Arc; workers read it once per
        // connection).
        let tls_cfg: Option<Arc<std::sync::RwLock<Arc<rustls::ServerConfig>>>> =
            match config.listeners.get(li).and_then(|l| l.tls.as_ref()) {
                Some(t) if mode == CoreMode::Http => {
                    match vane_tls::server_config(
                        std::path::Path::new(&t.cert),
                        std::path::Path::new(&t.key),
                    ) {
                        Ok(mut cfg) => {
                            if want_h2 {
                                cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
                            }
                            Some(Arc::new(std::sync::RwLock::new(Arc::new(cfg))))
                        }
                        Err(e) => {
                            eprintln!("vane: listener {}: tls: {e}", config.listeners[li].address);
                            return 1;
                        }
                    }
                }
                _ => None,
            };
        for w in 0..workers_per_listener {
            // SO_REUSEPORT lets each worker own a duplicate bind; the
            // kernel load-balances accepts across cores.
            let listener = if w == 0 {
                listener.try_clone().expect("clone listener")
            } else if inherited {
                // Inherited sockets cannot be re-bound: dup the fd so all
                // workers accept on the same listening socket.
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
            let events = Arc::new(EventRing::new());
            event_rings.push(Arc::clone(&events));
            tls_cfg_slots.push(tls_cfg.clone());
            let factory = WorkerFactory {
                mode,
                router: Arc::clone(&router),
                registry: Arc::clone(&registry),
                worker_id: li * workers_per_listener + w,
                events,
                tls: tls_cfg.clone(),
                runtime: config.runtime.clone(),
                plugins: config.plugins.iter().map(|p| p.path.clone()).collect(),
                http01_tokens: http01_tokens.clone(),
            };
            match spawn_worker(
                li * workers_per_listener + w,
                worker_cfg,
                listener,
                Arc::clone(&registry),
                Arc::clone(&factory.events),
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

    // ---- Access-log drain: workers push fixed-size events into their
    // rings; this task bridges them into `tracing` (never blocks workers).
    {
        let rings = event_rings.clone();
        tokio::spawn(async move {
            loop {
                let mut any = false;
                for (worker, ring) in rings.iter().enumerate() {
                    while let Some(ev) = ring.try_pop() {
                        any = true;
                        let msg = String::from_utf8_lossy(ev.msg()).into_owned();
                        match vane_observe::LogLevel::from_u8(ev.level) {
                            vane_observe::LogLevel::Error => {
                                tracing::error!(worker, "{}", msg);
                            }
                            vane_observe::LogLevel::Warn => {
                                tracing::warn!(worker, "{}", msg);
                            }
                            vane_observe::LogLevel::Debug => {
                                tracing::debug!(worker, "{}", msg);
                            }
                            vane_observe::LogLevel::Trace => {
                                tracing::trace!(worker, "{}", msg);
                            }
                            vane_observe::LogLevel::Info => {
                                tracing::info!(worker, "{}", msg);
                            }
                        }
                    }
                }
                if !any {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    // ---- Admin server ----------------------------------------------------
    if config.admin.enabled {
        let admin_addr: std::net::SocketAddr = config.admin.address.parse().expect("validated");
        let admin_router = crate::admin::build_admin_router(
            Arc::clone(&router),
            Arc::clone(&registry),
            Arc::clone(&health),
        );
        tokio::spawn(async move {
            if let Err(e) = crate::admin::serve(admin_addr, admin_router).await {
                tracing::error!("admin server: {e}");
            }
        });
    }

    // ---- HTTP/2 edge (feature `h2`) ---------------------------------------
    #[cfg(feature = "h2")]
    for (li, listener) in handover_listeners.iter().enumerate() {
        let want_h2 = config
            .listeners
            .get(li)
            .and_then(|l| l.tls.as_ref())
            .is_some_and(|t| t.alpn_h2);
        if !want_h2 {
            continue;
        }
        match vane_tls::server_config(
            std::path::Path::new(&config.listeners[li].tls.as_ref().expect("checked").cert),
            std::path::Path::new(&config.listeners[li].tls.as_ref().expect("checked").key),
        ) {
            Ok(cfg) => {
                let mut cfg = cfg;
                cfg.alpn_protocols = vec![b"h2".to_vec()];
                let dup = listener.try_clone().expect("dup for h2 edge");
                let edge = Arc::new(crate::h2_edge::H2Edge::new(
                    Arc::clone(&router),
                    Arc::clone(&registry),
                ));
                crate::h2_edge::spawn(dup, Arc::new(cfg), edge);
                tracing::info!("h2 edge listening (REUSEPORT) for listener {li}");
            }
            Err(e) => tracing::warn!("h2 edge tls: {e}"),
        }
    }

    // ---- TLS certificate hot reload --------------------------------------
    for (li, slot) in tls_cfg_slots.iter().enumerate() {
        if let (Some(t), Some(slot)) = (config.listeners.get(li).and_then(|l| l.tls.as_ref()), slot)
        {
            crate::tls_reload::spawn_reloader(
                PathBuf::from(&t.cert),
                PathBuf::from(&t.key),
                if cfg!(feature = "h2") && t.alpn_h2 {
                    vec![b"h2".to_vec()]
                } else {
                    vec![b"http/1.1".to_vec()]
                },
                Arc::clone(slot),
            );
        }
    }

    // ---- In-process SHM sidecar ------------------------------------------
    if config.sidecar.enabled {
        if let Err(e) = crate::sidecar::spawn_bridge(&config) {
            tracing::warn!("sidecar bridge: {e}");
        }
    }

    // ---- Shutdown ---------------------------------------------------------
    // SIGTERM/SIGINT (or the test hook) -> optional hot handover, then
    // drain workers with a 30 s hard deadline.
    let mut shutdown = Box::pin(wait_for_signal());
    match opts.shutdown_after {
        Some(d) => {
            let _ = tokio::time::timeout(d, shutdown.as_mut()).await;
        }
        None => shutdown.as_mut().await,
    }

    if let Some(sock) = &opts.handover_to {
        // The standby (started earlier with --handover-from) is bound to
        // this socket waiting to receive. Send listeners + route state so
        // it serves the instant we start draining — zero connection resets.
        let state = vane_shm::handover::HandoverState {
            generation: 0,
            routes: crate::proxy::flatten_routes(&router)
                .into_iter()
                .map(|r| vane_shm::handover::RouteRecord {
                    host: r.host,
                    pattern: r.pattern,
                    cluster: r.cluster,
                    backends: r.backends,
                    strip_prefix: r.strip_prefix,
                })
                .collect(),
            at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or_else(|_| "unknown".to_owned(), |d| format!("{}s", d.as_secs())),
        };
        let state_path = std::path::PathBuf::from("/tmp/vane-handover-state.json");
        match vane_shm::handover::send_listeners(
            std::path::Path::new(sock),
            &handover_listeners,
            &state,
            &state_path,
        ) {
            Ok(()) => tracing::info!("hot upgrade: handed over to standby"),
            Err(e) => tracing::warn!("hot upgrade handover failed (plain drain): {e}"),
        }
    }

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
    /// TLS termination slot for this worker's listener (`None` = plain).
    /// Shared + swappable: hot reload replaces the inner Arc.
    tls: Option<Arc<std::sync::RwLock<Arc<rustls::ServerConfig>>>>,
    /// Runtime knobs from config.
    runtime: vane_control::RuntimeConfig,
    /// Wasm plugin paths.
    plugins: Vec<String>,
    /// Shared HTTP-01 token map (Some when ACME is configured).
    http01_tokens: Option<Arc<std::sync::Mutex<HashMap<String, String>>>>,
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
                connect_timeout_ms: self.runtime.connect_timeout_ms,
                idle_timeout_ms: self.runtime.idle_timeout_ms,
                first_byte_timeout_ms: self.runtime.first_byte_timeout_ms,
                pool_per_backend: self.runtime.pool_per_backend,
                tls: self.tls.clone(),
                plugins: self.plugins.clone(),
                http01_tokens: self.http01_tokens.clone(),
            },
            self.worker_id,
        ))
    }
}
