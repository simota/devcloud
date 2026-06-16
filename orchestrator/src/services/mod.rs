//! In-process service tasks. Each module exposes
//! `run(cfg, shutdown) -> Result<(), String>`, constructing its engine from the
//! shared `Config` and serving until shutdown — no subprocesses, no
//! `DEVCLOUD_<SVC>_ENGINE` seams.
//!
//! Wired incrementally (Rust migration roadmap Phase 1): mail first, then the
//! remaining services + managed-postgres + event relay + dashboard.

pub mod applicationautoscaling;
pub mod bigquery;
pub mod dashboard;
pub mod dynamodb;
pub mod gcs;
pub mod mail;
pub mod managed_postgres;
pub mod pubsub;
pub mod redis;
pub mod redshift;
pub mod s3;
pub mod sqs;
pub mod util;
