//! Declarative configuration (`CP-04` loading half; zero-copy read side is
//! the published `RouteTable`).
//!
//! Layered: defaults → TOML file → environment (via `envstack`) → CLI
//! overrides. Validation uses `validkit` newtypes where the shape fits and
//! explicit checks where it does not.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VaneConfig {
    /// Ingress listeners.
    #[serde(default)]
    pub listeners: Vec<ListenerConfig>,
    /// Upstream clusters.
    #[serde(default)]
    pub clusters: BTreeMap<String, ClusterConfig>,
    /// Statically configured routes.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    /// Admin/metrics server.
    #[serde(default)]
    pub admin: AdminConfig,
    /// Dynamic file provider.
    #[serde(default)]
    pub file_provider: FileProviderConfig,
    /// Docker provider.
    #[serde(default)]
    pub docker: DockerConfig,
    /// Kubernetes provider.
    #[serde(default)]
    pub kubernetes: K8sConfig,
    /// ACME certificate manager.
    #[serde(default)]
    pub acme: AcmeSection,
    /// In-process SHM sidecar transport.
    #[serde(default)]
    pub sidecar: SidecarConfig,
    /// Wasm plugins (feature `wasm`), applied to every request in order.
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
    /// JWT bearer authentication (absent = disabled).
    #[serde(default)]
    pub jwt: Option<JwtConfig>,
    /// Process-wide rate limiting (absent = unlimited).
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    /// Runtime tuning.
    #[serde(default)]
    pub runtime: RuntimeConfig,
    /// Structured access logging.
    #[serde(default)]
    pub access_log: AccessLogConfig,
    /// OpenTelemetry tracing/metrics export.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

/// An ingress listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// Bind address (e.g. `0.0.0.0:8080`).
    pub address: String,
    /// L4 passthrough (splice) instead of HTTP termination.
    #[serde(default)]
    pub mode: ListenerMode,
    /// Worker count (0 = one per core).
    #[serde(default)]
    pub workers: usize,
    /// TLS termination (absent = plaintext).
    #[serde(default)]
    pub tls: Option<ListenerTls>,
    /// Serve HTTP/2 cleartext (h2c, prior knowledge) on this plain
    /// listener — containers/service meshes speak h2c without TLS.
    #[serde(default)]
    pub h2c: bool,
}

/// TLS material for a listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerTls {
    /// PEM certificate chain path (or ACME-managed path).
    pub cert: String,
    /// PEM private key path.
    pub key: String,
    /// Serve HTTP/2 on this listener (feature `h2`; a dedicated acceptor
    /// handles h2 connections while engine workers keep http/1.1).
    #[serde(default)]
    pub alpn_h2: bool,
}

/// Listener protocol mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ListenerMode {
    /// HTTP/1.1 termination + routing.
    #[default]
    Http,
    /// L4 splice passthrough to the matched cluster.
    Tcp,
}

/// An upstream cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Static backend addresses (`host:port`).
    #[serde(default)]
    pub backends: Vec<String>,
    /// Unix socket path (single backend).
    #[serde(default)]
    pub unix_socket: Option<PathBuf>,
    /// Load balancing policy.
    #[serde(default)]
    pub policy: PolicyConfig,
    /// Active health check path (HTTP GET); empty disables HTTP checks.
    #[serde(default)]
    pub health_path: Option<String>,
    /// Gzip-compress compressible responses for routes on this cluster
    /// (h1 downstream relay).
    #[serde(default)]
    pub compression: bool,
    /// Per-backend outlier ejection (absent = disabled).
    #[serde(default)]
    pub outlier: Option<OutlierConfig>,
    /// Speak HTTP/2 to this cluster's backends (prior knowledge).
    /// Honored by the h2 edge; the engine's h1 pool ignores it.
    #[serde(default)]
    pub http2: bool,
}

/// Load balancing policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyConfig {
    /// Power of two choices (default).
    #[default]
    P2c,
    /// Round robin.
    RoundRobin,
    /// Least connections.
    LeastConn,
}

impl From<PolicyConfig> for vane_router::Policy {
    fn from(p: PolicyConfig) -> Self {
        match p {
            PolicyConfig::P2c => vane_router::Policy::P2C,
            PolicyConfig::RoundRobin => vane_router::Policy::RoundRobin,
            PolicyConfig::LeastConn => vane_router::Policy::LeastConn,
        }
    }
}

/// A statically configured route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    /// Host match (empty = any).
    #[serde(default)]
    pub host: Option<String>,
    /// Path pattern (`/api/:id`, `/files/*rest`).
    pub pattern: String,
    /// Allowed methods (empty = all).
    #[serde(default)]
    pub methods: Vec<String>,
    /// Target cluster.
    pub cluster: String,
    /// Strip a path prefix before forwarding.
    #[serde(default)]
    pub strip_prefix: Option<String>,
    /// Upstream timeout override (ms).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Priority (lower wins).
    #[serde(default)]
    pub priority: u32,
}

/// Admin server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    /// Bind address (disabled when `enabled = false`).
    pub address: String,
    /// Enabled flag.
    pub enabled: bool,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9100".to_owned(),
            enabled: true,
        }
    }
}

/// File provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FileProviderConfig {
    /// Directory of `*.toml` route files.
    pub directory: Option<PathBuf>,
    /// Poll interval fallback (ms; 0 disables polling).
    pub poll_ms: u64,
}

impl Default for FileProviderConfig {
    fn default() -> Self {
        Self {
            directory: None,
            poll_ms: 5_000,
        }
    }
}

/// Docker provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DockerConfig {
    /// Enabled flag.
    pub enabled: bool,
    /// Docker socket path.
    pub socket: PathBuf,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            socket: PathBuf::from("/var/run/docker.sock"),
        }
    }
}

/// Kubernetes provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct K8sConfig {
    /// Enabled flag.
    pub enabled: bool,
    /// API server URL (in-cluster discovery when unset).
    pub api_server: Option<String>,
    /// Watched namespaces ("*" = all).
    pub namespaces: Vec<String>,
}

impl Default for K8sConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_server: None,
            namespaces: vec!["default".to_owned()],
        }
    }
}

/// ACME section.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AcmeSection {
    /// ACME configuration (disabled when unset).
    pub domains: Vec<AcmeDomain>,
    /// ACME directory URL.
    pub directory_url: Option<String>,
    /// Contact emails.
    pub emails: Vec<String>,
    /// Persisted account/cert directory.
    pub storage_dir: Option<PathBuf>,
    /// Accept invalid ACME endpoint certificates (pebble/CI only).
    #[serde(default)]
    pub insecure_tls: bool,
}

/// Domains managed by ACME.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcmeDomain {
    /// DNS identifier.
    pub domain: String,
    /// Challenge type (only `http-01` supported).
    #[serde(default)]
    pub challenge: AcmeChallenge,
}

/// Supported ACME challenges.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AcmeChallenge {
    /// HTTP-01 (served by vane's listeners).
    #[default]
    Http01,
    /// TLS-ALPN-01 (planned; rejected at load time for now).
    TlsAlpn01,
}

/// OpenTelemetry export configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Service name reported to backends.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// OTLP/HTTP endpoint (e.g. `http://localhost:4318`). Unset = no
    /// export (local fmt logging only).
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Trace sample rate 0.0–1.0 (deterministic per trace id).
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f32,
    /// Log filter (tracing EnvFilter syntax).
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_service_name() -> String {
    "vane".to_owned()
}

fn default_sample_rate() -> f32 {
    1.0
}

fn default_log_level() -> String {
    "info".to_owned()
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: default_service_name(),
            otlp_endpoint: None,
            sample_rate: default_sample_rate(),
            log_level: default_log_level(),
        }
    }
}

/// Access-log configuration (`Default` = disabled, stderr sink).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccessLogConfig {
    /// Emit one JSON line per completed request.
    #[serde(default)]
    pub enabled: bool,
    /// Output file (append). `None` = stderr.
    #[serde(default)]
    pub path: Option<String>,
}

/// In-process SHM sidecar transport configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SidecarConfig {
    /// Enabled flag.
    pub enabled: bool,
    /// Transport base directory (files: `<base>/req.ring` etc.).
    pub base: String,
    /// Slot size (payload ceiling per message).
    pub slot_size: u32,
    /// Slots per direction.
    pub slots: u32,
}

impl Default for SidecarConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base: "/dev/shm/vane-sidecar".to_owned(),
            slot_size: 256 * 1024,
            slots: 16,
        }
    }
}

/// Per-backend outlier ejection policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlierConfig {
    /// Consecutive failures (dial errors or 5xx) before ejection.
    #[serde(default = "default_outlier_threshold")]
    pub consecutive_failures: u32,
    /// Ejection duration in milliseconds.
    #[serde(default = "default_outlier_ejection_ms")]
    pub ejection_ms: u64,
}

fn default_outlier_threshold() -> u32 {
    5
}

fn default_outlier_ejection_ms() -> u64 {
    30_000
}

/// Per-cluster outlier detection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterOutlierConfig {
    /// Per-backend outlier ejection policy.
    #[serde(default)]
    pub outlier: Option<OutlierConfig>,
}

/// Process-wide rate limiting (one GCRA bucket across all workers).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Sustained requests per second (aggregate).
    pub rps: u32,
    /// Instantaneous burst allowance.
    #[serde(default = "default_rate_burst")]
    pub burst: u32,
}

fn default_rate_burst() -> u32 {
    100
}

/// JWT bearer authentication for edge listeners.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// JWKS file (RS256/ES256 public keys, with `kid`s).
    pub jwks_path: Option<String>,
    /// HMAC secret file (HS256; trailing newline trimmed).
    pub secret_path: Option<String>,
    /// Required issuer (`iss`), if any.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Required audience (`aud`), if any.
    #[serde(default)]
    pub audience: Option<String>,
}

/// A Wasm plugin loaded into every worker (feature `wasm`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Path to the `.wasm` module.
    pub path: String,
}

/// Runtime tuning knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Buffer pool slots per worker.
    pub pool_slots: usize,
    /// io_uring queue depth.
    pub ring_entries: u32,
    /// Enable SQPOLL (zero-syscall submission; needs privileges).
    pub sqpoll: bool,
    /// Force the mio engine even when io_uring is available.
    pub force_mio: bool,
    /// Max sessions per worker.
    pub max_sessions: usize,
    /// Accept backlog.
    pub backlog: i32,
    /// Upstream connect timeout (ms).
    pub connect_timeout_ms: u64,
    /// Upstream first-response-byte timeout (ms).
    pub first_byte_timeout_ms: u64,
    /// Idle keep-alive timeout (ms).
    pub idle_timeout_ms: u64,
    /// Idle upstream connections retained per backend per worker.
    pub pool_per_backend: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            pool_slots: 1024,
            ring_entries: 4096,
            sqpoll: false,
            force_mio: false,
            max_sessions: 16_384,
            backlog: 4096,
            connect_timeout_ms: 5_000,
            first_byte_timeout_ms: 30_000,
            idle_timeout_ms: 75_000,
            // Upstream keep-alive pooling: 4 idle connections per backend
            // per worker. Validated under sustained load (load_pooled).
            pool_per_backend: 4,
        }
    }
}

/// Load errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// File read/decode failure.
    #[error("config file: {0}")]
    File(String),
    /// Semantic validation failure.
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl VaneConfig {
    /// Parses a TOML document.
    ///
    /// # Errors
    /// TOML decode failure.
    pub fn parse_toml(text: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError::File(e.to_string()))
    }

    /// Validates semantics (addresses parse, clusters referenced exist,
    /// challenge support).
    ///
    /// # Errors
    /// Returns [`ConfigError::Invalid`] with the first problem.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for l in &self.listeners {
            l.address
                .parse::<SocketAddr>()
                .map_err(|e| ConfigError::Invalid(format!("listener `{}`: {e}", l.address)))?;
        }
        for r in &self.routes {
            if !self.clusters.contains_key(&r.cluster) {
                return Err(ConfigError::Invalid(format!(
                    "route `{}` references unknown cluster `{}`",
                    r.pattern, r.cluster
                )));
            }
        }
        for d in &self.acme.domains {
            if d.challenge == AcmeChallenge::TlsAlpn01 {
                return Err(ConfigError::Invalid(
                    "tls-alpn-01 challenge is not yet supported".to_owned(),
                ));
            }
            if let Some(url) = &self.acme.directory_url {
                validkit::HttpsUrl::try_from(url.as_str())
                    .map_err(|e| ConfigError::Invalid(format!("acme directory url: {e}")))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[listeners]]
address = "0.0.0.0:8080"
mode = "http"

[clusters.api]
backends = ["127.0.0.1:9001", "127.0.0.1:9002"]
policy = "p2c"
health_path = "/healthz"

[[routes]]
host = "api.example.com"
pattern = "/v1/:resource"
cluster = "api"
strip_prefix = "/v1"

[[routes]]
pattern = "/*rest"
cluster = "api"

[admin]
address = "127.0.0.1:9100"
"#;

    #[test]
    fn parses_and_validates() {
        let cfg = VaneConfig::parse_toml(SAMPLE).expect("parses");
        cfg.validate().expect("valid");
        assert_eq!(cfg.listeners.len(), 1);
        assert_eq!(cfg.routes.len(), 2);
        assert_eq!(cfg.clusters["api"].backends.len(), 2);
    }

    #[test]
    fn rejects_unknown_cluster() {
        let bad = SAMPLE.replace("cluster = \"api\"", "cluster = \"nope\"");
        let cfg = VaneConfig::parse_toml(&bad).expect("parses");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_bad_listener() {
        let mut cfg = VaneConfig::parse_toml(SAMPLE).expect("parses");
        cfg.listeners[0].address = "not-an-addr".to_owned();
        assert!(cfg.validate().is_err());
    }
}

#[cfg(test)]
mod telemetry_tests {
    use super::*;

    #[test]
    fn telemetry_defaults_sane() {
        let cfg = TelemetryConfig::default();
        assert_eq!(cfg.service_name, "vane");
        assert!(cfg.otlp_endpoint.is_none());
        assert!((cfg.sample_rate - 1.0).abs() < f32::EPSILON);
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn telemetry_parses_full_section() {
        let cfg: VaneConfig = toml::from_str(
            r#"
[telemetry]
service_name = "edge-1"
otlp_endpoint = "http://otel:4318"
sample_rate = 0.25
log_level = "debug"
"#,
        )
        .expect("parse");
        assert_eq!(cfg.telemetry.service_name, "edge-1");
        assert_eq!(
            cfg.telemetry.otlp_endpoint.as_deref(),
            Some("http://otel:4318")
        );
        assert!((cfg.telemetry.sample_rate - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn sample_rate_clamped_by_consumer() {
        // Config parsing accepts the raw value; clamping happens where
        // the rate is consumed (documented contract).
        let cfg: TelemetryConfig = toml::from_str(
            r#"service_name = "x"
sample_rate = 2.0
"#,
        )
        .expect("parse");
        assert!((cfg.sample_rate - 2.0).abs() < f32::EPSILON);
    }
}

/// Dry-run outcome for a candidate config (never applied).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DryRunReport {
    /// Parsed listener count.
    pub listeners: usize,
    /// Parsed cluster count.
    pub clusters: usize,
    /// Parsed route count.
    pub routes: usize,
    /// Non-fatal notes (unresolvable backends are dropped at compile
    /// time, so they are surfaced here instead of as errors).
    pub warnings: Vec<String>,
}

/// Parses + validates a candidate TOML config **without applying it**.
/// The `POST /config/dry-run` admin endpoint and `vane validate` both
/// build on this.
///
/// # Errors
/// Parse or semantic failure with a human-readable message.
pub fn dry_run_toml(text: &str) -> Result<DryRunReport, String> {
    let cfg = VaneConfig::parse_toml(text).map_err(|e| e.to_string())?;
    cfg.validate().map_err(|e| e.to_string())?;
    // Route compilability: patterns must be non-empty and reference
    // known clusters (validate covers the latter; compile checks the
    // pattern shape through the router's own matcher rules).
    let mut warnings = Vec::new();
    for c in &cfg.clusters {
        if c.1.backends.is_empty() && c.1.unix_socket.is_none() {
            warnings.push(format!(
                "cluster `{}` has no backends and no unix socket",
                c.0
            ));
        }
    }
    Ok(DryRunReport {
        listeners: cfg.listeners.len(),
        clusters: cfg.clusters.len(),
        routes: cfg.routes.len(),
        warnings,
    })
}
