//! # vane-control
//!
//! The control plane's brain: configuration, dynamic discovery providers,
//! ACME certificate management, active health checks, and reconciliation —
//! everything that is *not* on the per-request hot path.
//!
//! Providers run as async tasks on the control runtime and push
//! [`ProviderUpdate`]s into a channel; the [`Reconciler`] merges them into
//! a new [`RouteTable`](vane_router::RouteTable) generation and publishes
//! it lock-free through [`Router`](vane_router::Router) (`CP-02`, <1 ms).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod acme;
pub mod config;
pub mod health;
pub mod providers;
pub mod reconcile;
pub mod routes;

pub use acme::{AcmeConfig, AcmeManager};
pub use config::{
    ClusterConfig, ListenerConfig, ListenerTls, RouteConfig, RuntimeConfig, VaneConfig,
};
pub use health::{HealthChecker, HealthMap};
pub use providers::ProviderUpdate;
#[cfg(feature = "docker-provider")]
pub use providers::docker::DockerProvider;
#[cfg(feature = "file-provider")]
pub use providers::file::FileProvider;
pub use reconcile::Reconciler;
