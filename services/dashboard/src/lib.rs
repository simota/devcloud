//! Rust reimplementation of the devcloud dashboard (Phase 2 foundation).
//!
//! The dashboard is the single user-facing HTTP entry point (`:8025`): it serves
//! the embedded React SPA and a set of `/api/<svc>/*` JSON endpoints. Unlike the
//! legacy dashboard (which holds in-process pointers to every service), this Rust
//! dashboard runs as its own subprocess and reaches each service over the
//! network — via the Phase-1 read-only `/_introspect/` endpoints, the mutation
//! `/_control/` endpoints, and (for services like SQS) the provider's own
//! protocol. Per-service base addresses arrive as env vars set by the legacy daemon
//! seam (`internal/app/dashboard_rust.rs`).
//!
//! Module map:
//!   - [`config`]   service base addresses + display metadata from env.
//!   - [`http`]     hand-rolled HTTP/1.1 server + router (mirrors the other crates).
//!   - [`forward`]  hand-rolled HTTP/1.1 client used to proxy to service endpoints.
//!   - [`assets`]   embedded SPA serving (cache/redirect/index-fallback parity).
//!   - [`services`] the `/api/services` registry.
//!   - [`sqs`]      the SQS `/api/sqs/*` handler — the reusable per-service template.

pub mod assets;
pub mod bigquery;
pub mod config;
pub mod dynamodb;
pub mod events;
pub mod forward;
pub mod gcs;
pub mod http;
pub mod mail;
pub mod pubsub;
pub mod redis;
pub mod redshift;
pub mod s3;
pub mod services;
pub mod sqs;

pub use config::Config;
