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

impl RunOptions {
    /// Test/coverage helper: minimal options with just a config path.
    #[must_use]
    pub fn for_test(config_path: Option<&str>) -> Self {
        Self {
            config_path: config_path.map(str::to_owned),
            handover_from: None,
            handover_to: None,
            shutdown_after: None,
            force_mio: true,
        }
    }
}

/// Receives inherited listeners from the handover socket and seeds the
/// router from the archived route state. Returns the inherited listeners.
///
/// # Errors
/// Handover receive or state-archive failure.
pub fn receive_inherited(
    sock: &str,
    router: &Arc<Router>,
    health: &Arc<vane_control::HealthMap>,
    expected: usize,
) -> Result<Vec<StdTcpListener>, String> {
    let state_path = std::path::PathBuf::from("/tmp/vane-handover-state.json");
    let received =
        vane_shm::handover::receive_listeners(std::path::Path::new(sock), &state_path, expected)
            .map_err(|e| e.to_string())?;
    tracing::info!(
        "hot upgrade: inherited {} listeners, generation {}",
        received.listeners.len(),
        received.state.generation
    );
    let records = received.state.routes;
    let health2 = Arc::clone(health);
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
                upstream_h2: false,
                compression: false,
                outlier: None,
                policy: vane_router::Policy::P2C,
                priority: 20,
            };
            match builder.compile() {
                Ok(entry) => editor.insert(entry),
                Err(e) => tracing::warn!("handover route: {e}"),
            }
        }
    });
    Ok(received.listeners)
}

/// Sends this instance's listeners + live route table to the standby
/// (hot-upgrade `--handover-to` shutdown path).
///
/// # Errors
/// Handover send or state-archive failure.
pub fn send_handover(
    sock: &str,
    listeners: &[StdTcpListener],
    router: &Arc<Router>,
) -> Result<(), String> {
    let state = vane_shm::handover::HandoverState {
        generation: 0,
        routes: crate::proxy::flatten_routes(router)
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
    vane_shm::handover::send_listeners(std::path::Path::new(sock), listeners, &state, &state_path)
        .map_err(|e| e.to_string())
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
/// Initializes process telemetry from the file config with environment
/// overrides (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
/// `OTEL_SAMPLE_RATE`, `RUST_LOG`). Returns the guard (dropped on
/// shutdown); failures fall back to a plain stderr logger so a broken
/// telemetry setup can never take down the proxy.
#[must_use]
pub fn init_telemetry(cfg: &vane_control::TelemetryConfig) -> Option<otelkit::TelemetryGuard> {
    let mut merged = cfg.clone();
    if let Ok(ep) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        if !ep.is_empty() {
            merged.otlp_endpoint = Some(ep);
        }
    }
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME") {
        if !name.is_empty() {
            merged.service_name = name;
        }
    }
    if let Ok(rate) = std::env::var("OTEL_SAMPLE_RATE") {
        if let Ok(r) = rate.parse::<f32>() {
            merged.sample_rate = r.clamp(0.0, 1.0);
        }
    }
    if let Ok(level) = std::env::var("RUST_LOG") {
        if !level.is_empty() {
            merged.log_level = level;
        }
    }
    let otel_cfg = otelkit::TelemetryConfig {
        service_name: merged.service_name,
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        log_level: merged.log_level,
        log_format: otelkit::LogFormat::Text,
        otlp_endpoint: merged.otlp_endpoint,
        sentry_dsn: None,
        sample_rate: merged.sample_rate.clamp(0.0, 1.0),
    };
    match otelkit::init(otel_cfg) {
        Ok(guard) => Some(guard),
        Err(e) => {
            eprintln!("vane: telemetry init failed ({e}); continuing without export");
            None
        }
    }
}

/// Runs the proxy until shutdown. Returns the process exit code.
pub async fn run(opts: RunOptions) -> i32 {
    let config = match load_config(opts.config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vane: {e}");
            return 1;
        }
    };
    // Telemetry after config load (file config + env overrides). The
    // data plane logs through its rings; this is the control plane.
    let _otel_guard = init_telemetry(&config.telemetry);
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
        match receive_inherited(sock, &router, &health, config.listeners.len()) {
            Ok(received) => {
                bound = received;
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
    let mut access_logs: Vec<Arc<vane_observe::access::AccessLog>> = Vec::new();
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
                            // ALPN: h2 + http/1.1 when enabled — the
                            // engine path serves both (h2 via the
                            // native engine's translation shim).
                            if want_h2 {
                                cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
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
            let access = if config.access_log.enabled {
                Some(Arc::new(vane_observe::access::AccessLog::new()))
            } else {
                None
            };
            if let Some(a) = &access {
                access_logs.push(Arc::clone(a));
            }
            // Per-worker JWT validator (mtime-triggered JWKS reload).
            let jwt = config.jwt.as_ref().map(|j| {
                match vane_filters::jwt::JwtValidator::new(
                    j.jwks_path.as_deref().map(std::path::Path::new),
                    j.secret_path.as_deref().map(std::path::Path::new),
                    j.issuer.as_deref(),
                    j.audience.as_deref(),
                ) {
                    Ok(v) => Some(Arc::new(v)),
                    Err(e) => {
                        eprintln!("vane: jwt auth disabled: {e}");
                        None
                    }
                }
            });
            let jwt = jwt.flatten();
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
                access,
                jwt,
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

    // HTTP/2 (feature `h2`) is served on the engine data path via
    // ALPN negotiation (crates/vane/src/h2_server.rs) — the former
    // REUSEPORT tokio edge is retired.

    // ---- Access-log drain: render one JSON line per transaction into
    // the configured sink (stderr by default). Dropping on a full ring
    // is preferable to ever blocking a worker.
    if config.access_log.enabled {
        let logs = access_logs.clone();
        let path = config.access_log.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut file = path.and_then(|p| {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&p)
                    .map_err(|e| tracing::warn!("access log open {p}: {e}"))
                    .ok()
            });
            use std::io::Write as _;
            let mut out = String::with_capacity(512);
            loop {
                let mut any = false;
                for log in &logs {
                    while let Some(rec) = log.pop() {
                        any = true;
                        out.clear();
                        rec.render_json(&mut out);
                        out.push('\n');
                        if let Some(f) = file.as_mut() {
                            let _ = f.write_all(out.as_bytes());
                        } else {
                            let _ = std::io::stderr().write_all(out.as_bytes());
                        }
                    }
                }
                if !any {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        });
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
        if let Err(e) = send_handover(sock, &handover_listeners, &router) {
            tracing::warn!("hot upgrade handover failed (plain drain): {e}");
        }
    }

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
    /// Per-worker access log (`None` = disabled).
    access: Option<Arc<vane_observe::access::AccessLog>>,
    /// JWT bearer auth (`None` = disabled).
    jwt: Option<Arc<vane_filters::jwt::JwtValidator>>,
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
                access: self.access.clone(),
                jwt: self.jwt.clone(),
                l4_splice: self.mode == CoreMode::L4,
            },
            self.worker_id,
        ))
    }
}

#[cfg(test)]
mod config_override_tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn write_cfg(dir: &std::path::Path, name: &str, toml: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, toml).expect("write");
        path.to_str().expect("utf8").to_owned()
    }

    const BASE: &str = r#"
[[listeners]]
address = "127.0.0.1:1"

[clusters.up]
backends = ["127.0.0.1:2"]

[[routes]]
pattern = "/*rest"
cluster = "up"
"#;

    #[test]
    fn env_overrides_listener_admin_and_cluster() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: env mutation serialized by ENV_LOCK within this binary.
        unsafe {
            std::env::set_var("VANE_LISTEN", "127.0.0.1:19991");
            std::env::set_var("VANE_ADMIN_ADDR", "127.0.0.1:19992");
            std::env::set_var("VANE_CLUSTER_UP", "10.9.9.9:1, 10.9.9.10:2");
        }
        let dir = tempfile::tempdir().expect("dir");
        let path = write_cfg(dir.path(), "env.toml", BASE);
        let cfg = load_config(Some(&path)).expect("load");
        // SAFETY: same lock; restore to avoid cross-test leakage.
        unsafe {
            std::env::remove_var("VANE_LISTEN");
            std::env::remove_var("VANE_ADMIN_ADDR");
            std::env::remove_var("VANE_CLUSTER_UP");
        }
        assert_eq!(cfg.listeners[0].address, "127.0.0.1:19991");
        assert_eq!(cfg.admin.address, "127.0.0.1:19992");
        assert!(cfg.admin.enabled);
        assert_eq!(
            cfg.clusters["up"].backends,
            vec!["10.9.9.9:1".to_string(), "10.9.9.10:2".to_string()]
        );
    }

    #[test]
    fn env_listen_creates_listener_when_missing() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: serialized by ENV_LOCK.
        unsafe {
            std::env::set_var("VANE_LISTEN", "127.0.0.1:19993");
        }
        let dir = tempfile::tempdir().expect("dir");
        let path = write_cfg(
            dir.path(),
            "nolisten.toml",
            "[clusters.up]\nbackends = []\n",
        );
        let cfg = load_config(Some(&path)).expect("load");
        // SAFETY: restore.
        unsafe {
            std::env::remove_var("VANE_LISTEN");
        }
        assert!(cfg.listeners.iter().any(|l| l.address == "127.0.0.1:19993"));
    }

    #[test]
    fn load_config_missing_file_errors() {
        assert!(load_config(Some("/nonexistent/vane.toml")).is_err());
    }

    #[test]
    fn env_tls_override_attaches_to_first_listener() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let dir = tempfile::tempdir().expect("dir");
        let certs = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
        let cert = dir.path().join("c.pem");
        let key = dir.path().join("k.pem");
        std::fs::write(&cert, certs.cert.pem()).expect("cert");
        std::fs::write(&key, certs.signing_key.serialize_pem()).expect("key");

        // SAFETY: env mutation serialized by ENV_LOCK within this binary.
        unsafe {
            std::env::set_var("VANE_TLS_CERT", cert.to_str().expect("utf8"));
            std::env::set_var("VANE_TLS_KEY", key.to_str().expect("utf8"));
        }
        let path = write_cfg(dir.path(), "tls.toml", BASE);
        let cfg = load_config(Some(&path)).expect("load");
        // SAFETY: restore.
        unsafe {
            std::env::remove_var("VANE_TLS_CERT");
            std::env::remove_var("VANE_TLS_KEY");
        }
        let tls = cfg.listeners[0].tls.as_ref().expect("tls attached");
        assert_eq!(tls.cert, cert.to_str().expect("utf8"));
        assert!(!tls.alpn_h2);
    }

    #[test]
    fn flatten_routes_mirrors_table() {
        let router = Arc::new(Router::new());
        router.update(|editor| {
            editor.insert(vane_router::RouteEntry {
                host: Some("h.example".into()),
                pattern: "/a/*rest".into(),
                methods: vec!["GET".into()],
                cluster: "c".into(),
                strip_prefix: None,
                timeout_ms: None,
                backends: vec![vane_router::Backend::new(
                    "127.0.0.1:5".parse().expect("addr"),
                    1,
                )],
                upstream_h2: false,
                compression: false,
                outlier: None,
                policy: vane_router::Policy::P2C,
                gauges: Arc::new(vane_router::balancer::ConnGauges::new(1)),
                priority: 0,
            });
        });
        let records = crate::proxy::flatten_routes(&router);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].host.as_deref(), Some("h.example"));
        assert_eq!(records[0].pattern, "/a/*rest");
        assert_eq!(records[0].cluster, "c");
        assert_eq!(records[0].backends, vec!["127.0.0.1:5".to_string()]);
    }
}

#[cfg(test)]
mod handover_helper_tests {
    use super::*;

    /// receive_inherited + send_handover round-trip through a real
    /// handover socket with a live listener.
    #[test]
    fn send_then_receive_roundtrip() {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("hs.sock");
        vane_shm::handover::prepare_socket(&sock);

        let router = Arc::new(Router::new());
        router.update(|editor| {
            editor.insert(vane_router::RouteEntry {
                host: Some("roundtrip.test".into()),
                pattern: "/*rest".into(),
                methods: Vec::new(),
                cluster: "c".into(),
                strip_prefix: None,
                timeout_ms: None,
                backends: vec![vane_router::Backend::new(
                    "127.0.0.1:1".parse().expect("addr"),
                    1,
                )],
                upstream_h2: false,
                compression: false,
                outlier: None,
                policy: vane_router::Policy::P2C,
                gauges: Arc::new(vane_router::balancer::ConnGauges::new(1)),
                priority: 0,
            });
        });
        let health = Arc::new(vane_control::HealthMap::new());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        // listener moves into send_handover (fd ownership transfers).

        // Receiver thread: blocks until the send lands.
        let rx_sock = sock.clone();
        let rx_router = Arc::clone(&router);
        let rx_health = Arc::clone(&health);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let res = receive_inherited(rx_sock.to_str().expect("utf8"), &rx_router, &rx_health, 1);
            tx.send(res).expect("send");
        });

        send_handover(sock.to_str().expect("utf8"), &[listener], &router).expect("send_handover");

        let listeners = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("join")
            .expect("receive_inherited");
        assert_eq!(listeners.len(), 1);
        assert_eq!(listeners[0].local_addr().expect("local"), addr);

        // The router was seeded from the archived state.
        let table = router.load();
        let matched = table.table().lookup(Some("roundtrip.test"), "/x");
        assert!(matched.is_some(), "archived route seeded");
    }

    /// send_handover with an absent receiver errors (socket missing).
    #[test]
    fn send_handover_missing_socket_errors() {
        let dir = tempfile::tempdir().expect("dir");
        let sock = dir.path().join("nopeer.sock");
        let router = Arc::new(Router::new());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        // No receiver bound at that path: connect fails.
        let res = send_handover(sock.to_str().expect("utf8"), &[listener], &router);
        assert!(res.is_err(), "absent socket must error");
    }
}
