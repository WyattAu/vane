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
    /// Runtime tuning.
    #[serde(default)]
    pub runtime: RuntimeConfig,
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
