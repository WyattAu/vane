//! The [`Router`]: EBR-published generations of compiled routes.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crossbeam_epoch::{Atomic, Guard, Owned};

use crate::balancer::{Backend, Balancer, ConnGauges, Policy};
use crate::trie::{Matched, PathTrie};

/// Compiled route: what the data plane needs per match, nothing more.
#[derive(Debug, Clone)]
pub struct RouteEntry {
    /// Host this route binds to (`None` = catch-all host).
    pub host: Option<String>,
    /// Path pattern as configured (diagnostics).
    pub pattern: String,
    /// HTTP methods allowed (empty = any). Uppercase tokens.
    pub methods: Vec<String>,
    /// Upstream cluster name.
    pub cluster: String,
    /// Prefix to strip from the matched path before forwarding.
    pub strip_prefix: Option<String>,
    /// Request timeout override (ms).
    pub timeout_ms: Option<u64>,
    /// Backends (compiled from the cluster at publish time).
    pub backends: Vec<Backend>,
    /// Speak HTTP/2 to this cluster's backends (prior knowledge). The
    /// engine's zero-copy h1 pool ignores this; the h2 edge honors it.
    pub upstream_h2: bool,
    /// Gzip-compress compressible responses on this route.
    pub compression: bool,
    /// Per-backend outlier ejection (absent = disabled).
    pub outlier: Option<Arc<crate::outlier::OutlierSet>>,
    /// LB policy for the cluster.
    pub policy: Policy,
    /// Shared gauges for the backend list.
    pub gauges: Arc<ConnGauges>,
    /// Route priority (lower wins on pattern conflicts).
    pub priority: u32,
}

impl RouteEntry {
    /// Builds a worker-local balancer over this route's backends.
    #[must_use]
    pub fn balancer(&self, seed: u64) -> Balancer {
        Balancer::new(
            self.backends.clone(),
            Arc::clone(&self.gauges),
            self.policy,
            seed,
        )
    }
}

/// One host's trie with a default fallback.
#[derive(Clone)]
struct HostRoutes {
    trie: PathTrie<Arc<RouteEntry>>,
}

/// An immutable generation of the routing table.
pub struct RouteTable {
    /// Exact-host tries.
    hosts: std::collections::BTreeMap<String, HostRoutes>,
    /// Fallback trie for unmatched hosts.
    default_trie: PathTrie<Arc<RouteEntry>>,
    /// Insertion-ordered entries (diagnostics / handover snapshots).
    entries: Vec<Arc<RouteEntry>>,
}

/// Flat route record for snapshots (`vane` hot upgrade / admin dump).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RouteSnapshotRecord {
    /// Host match.
    pub host: Option<String>,
    /// Path pattern.
    pub pattern: String,
    /// Cluster name.
    pub cluster: String,
    /// Backend addresses.
    pub backends: Vec<String>,
    /// Strip prefix.
    pub strip_prefix: Option<String>,
}

impl RouteTable {
    /// Empty table.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            hosts: std::collections::BTreeMap::new(),
            default_trie: PathTrie::new(),
            entries: Vec::new(),
        }
    }

    /// Number of registered routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Flat snapshot of all routes (admin dump / hot handover).
    #[must_use]
    pub fn snapshot_records(&self) -> Vec<RouteSnapshotRecord> {
        self.entries
            .iter()
            .map(|e| RouteSnapshotRecord {
                host: e.host.clone(),
                pattern: e.pattern.clone(),
                cluster: e.cluster.clone(),
                backends: e.backends.iter().map(|b| b.addr.to_string()).collect(),
                strip_prefix: e.strip_prefix.clone(),
            })
            .collect()
    }

    /// Resolves a host's trie: exact match, else the default trie.
    fn trie_for<'a>(&'a self, host: Option<&str>) -> &'a PathTrie<Arc<RouteEntry>> {
        if let Some(h) = host {
            if let Some(hr) = self.hosts.get(h) {
                return &hr.trie;
            }
        }
        &self.default_trie
    }

    /// Looks a route up (host header + request path).
    #[must_use]
    pub fn lookup(&self, host: Option<&str>, path: &str) -> Option<Matched<'_, Arc<RouteEntry>>> {
        self.trie_for(host).lookup(path)
    }
}

/// Errors from the builder.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    /// Route pattern or host was rejected.
    #[error("invalid route: {0}")]
    Invalid(String),
}

/// Declarative route input (config-plane shape).
#[derive(Debug, Clone)]
pub struct RouteBuilder {
    /// Host match (`None` = any host).
    pub host: Option<String>,
    /// Path pattern (`/api/:id`, `/files/*rest`, `/health`).
    pub pattern: String,
    /// Allowed methods (empty = all).
    pub methods: Vec<String>,
    /// Upstream cluster.
    pub cluster: String,
    /// Prefix strip.
    pub strip_prefix: Option<String>,
    /// Timeout override ms.
    pub timeout_ms: Option<u64>,
    /// Backends of the cluster (materialized by the caller, health flags
    /// shared with the health checker).
    pub backends: Vec<crate::balancer::Backend>,
    /// LB policy.
    pub policy: Policy,
    /// HTTP/2 upstream (prior knowledge).
    pub upstream_h2: bool,
    /// Gzip-compress compressible responses on this route.
    pub compression: bool,
    /// Per-backend outlier ejection (absent = disabled).
    pub outlier: Option<Arc<crate::outlier::OutlierSet>>,
    /// Priority.
    pub priority: u32,
}

impl RouteBuilder {
    /// Compiles into a [`RouteEntry`].
    ///
    /// # Errors
    /// Invalid pattern (empty) or zero backends.
    pub fn compile(self) -> Result<RouteEntry, RouterError> {
        if self.pattern.is_empty() {
            return Err(RouterError::Invalid("empty pattern".into()));
        }
        if self.backends.is_empty() {
            return Err(RouterError::Invalid(format!(
                "cluster `{}` has no backends",
                self.cluster
            )));
        }
        let gauges = Arc::new(ConnGauges::new(self.backends.len()));
        let backends = self.backends;
        Ok(RouteEntry {
            host: self.host,
            pattern: self.pattern,
            methods: self.methods.iter().map(|m| m.to_uppercase()).collect(),
            cluster: self.cluster,
            strip_prefix: self.strip_prefix,
            timeout_ms: self.timeout_ms,
            backends,
            upstream_h2: self.upstream_h2,
            compression: self.compression,
            outlier: self.outlier,
            policy: self.policy,
            gauges,
            priority: self.priority,
        })
    }
}

/// EBR-published routing table: the lock-free worker read path (`CP-02`).
pub struct Router {
    current: Atomic<RouteTable>,
}

impl Router {
    /// New router with an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current: Atomic::new(RouteTable::empty()),
        }
    }

    /// Loads the active table snapshot (epoch-pinned; ~ns).
    ///
    /// The returned `TableGuard` keeps the generation alive for the duration
    /// of the request without blocking publishers.
    #[must_use]
    pub fn load(&self) -> TableGuard {
        let guard = crossbeam_epoch::pin();
        let table = self.current.load(Ordering::Acquire, &guard);
        // SAFETY: the guard pins the epoch; the generation is alive.
        let ptr = std::ptr::NonNull::from(unsafe { table.deref() });
        TableGuard { guard, ptr }
    }

    /// Publishes a new generation built by mutating a copy of the current
    /// one. Readers in flight keep their snapshot; the old generation is
    /// reclaimed when their epochs retire.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut TableEditor),
    {
        let guard = crossbeam_epoch::pin();
        let old_shared = self.current.load(Ordering::Acquire, &guard);
        // SAFETY: same pin guarantees the old generation is alive.
        let old_ref = unsafe { old_shared.deref() };
        // Editor starts from the live generation (structural sharing keeps
        // this cheap).
        let mut editor = TableEditor {
            hosts: old_ref.hosts.clone(),
            default_trie: shared_trie_clone(&old_ref.default_trie),
            entries: old_ref.entries.clone(),
        };
        f(&mut editor);
        let new_table = Box::new(RouteTable {
            hosts: editor.hosts,
            default_trie: editor.default_trie,
            entries: editor.entries,
        });
        let new_shared = Owned::new(*new_table).into_shared(&guard);
        // Release store: readers' Acquire load sees the fully built table.
        self.current.store(new_shared, Ordering::Release);
        // SAFETY: old generation published via this atomic; retired once
        // all pinned readers drain.
        unsafe {
            guard.defer_destroy(old_shared);
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a pinned table generation. The held epoch pin keeps the
/// generation alive without blocking publishers; dropping it lets the old
/// generation retire.
pub struct TableGuard {
    // `guard` declared first so the pinned epoch outlives `ptr` uses.
    #[allow(dead_code)] // pins the epoch; never read directly
    guard: Guard,
    ptr: std::ptr::NonNull<RouteTable>,
}

impl TableGuard {
    /// The live table.
    #[must_use]
    pub fn table(&self) -> &RouteTable {
        // SAFETY: the epoch is pinned by `guard`; the generation cannot be
        // reclaimed while this value lives.
        unsafe { self.ptr.as_ref() }
    }
}

/// Mutable view used inside [`Router::update`].
pub struct TableEditor {
    hosts: std::collections::BTreeMap<String, HostRoutes>,
    default_trie: PathTrie<Arc<RouteEntry>>,
    entries: Vec<Arc<RouteEntry>>,
}

impl TableEditor {
    /// Inserts a compiled route.
    pub fn insert(&mut self, entry: RouteEntry) {
        let arc = Arc::new(entry);
        let pattern = arc.pattern.clone();
        let host = arc.host.clone();
        if let Some(h) = host {
            let hr = self.hosts.entry(h).or_insert_with(|| HostRoutes {
                trie: PathTrie::new(),
            });
            hr.trie = hr.trie.insert(&pattern, Arc::clone(&arc));
        } else {
            self.default_trie = self.default_trie.insert(&pattern, Arc::clone(&arc));
        }
        self.entries.push(arc);
    }
}

/// Clones a trie — Arc structural sharing makes this O(1).
fn shared_trie_clone<T>(t: &PathTrie<T>) -> PathTrie<T> {
    t.clone()
}
