//! devcloud BigQuery service — Rust reimplementation (strangler-fig increment #9).
//!
//! Parity target: `internal/services/bigquery`. Modules land incrementally;
//! see `README.md` for the migration order and parity discipline.
//!
//! Part 1 (foundation): the resource model (`model`), legacy-compatible JSON
//! encoding (`wire_json`), on-disk persistence (`storage`), the response
//! envelope/error format (`responses`), shared request validation
//! (`validation`), and the dataset CRUD handlers (`dataset_handlers`).
//!
//! Part 2 (SQL engine): query parsing (`sql_parser`), expression/predicate
//! evaluation and aggregates (`sql_eval`), and query execution over stored
//! table data including query-job creation (`query_engine`).
//!
//! Part 3 (resource + query-job handlers): table CRUD (`table_handlers`),
//! routine CRUD (`routine_handlers`), insertAll/list/`/queries`
//! (`tabledata_handlers`), and the job endpoints for QUERY jobs
//! (`job_handlers`: insert/get/list/cancel/delete + getQueryResults).
//!
//! Part 4 (final): copy/load/extract job execution over the shared S3
//! `FileBucketStore` (`job_load_extract`), IAM policy stubs (`iam_handlers`),
//! the routing/auth/multipart layer (`routes`), the tokio HTTP wire layer
//! (`http`), dashboard snapshots (`dashboard`), and the daemon-seam binary
//! (`main.rs`, env-configured, `DEVCLOUD_EVENT` stdout bridge).

use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

static EVENT_SINK: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Installs a process-wide in-process sink for dashboard event JSON objects.
/// Called once by the single-binary orchestrator at startup. Each emitted event
/// is sent as the JSON object string `{"type":..,"service":..,"payload":..}`.
pub fn set_event_sink(tx: UnboundedSender<String>) {
    let _ = EVENT_SINK.set(tx);
}

/// Returns the installed event sink, if any.
pub(crate) fn event_sink() -> Option<&'static UnboundedSender<String>> {
    EVENT_SINK.get()
}

pub mod dashboard;
pub mod dataset_handlers;
pub mod http;
pub mod iam_handlers;
pub mod introspect;
pub mod job_handlers;
pub mod job_load_extract;
pub mod model;
pub mod query_engine;
pub mod responses;
pub mod routes;
pub mod routine_handlers;
pub mod server;
pub mod sql_eval;
pub mod sql_parser;
pub mod storage;
pub mod table_handlers;
pub mod tabledata_handlers;
pub mod validation;
pub mod wire_json;
