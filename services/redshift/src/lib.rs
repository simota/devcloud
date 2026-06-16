//! devcloud Redshift service — Rust reimplementation (strangler-fig increment #10).
//!
//! Parity target: `internal/services/redshift`. Part 1 ports the SQL
//! foundation: the PG value/type system, statement parsing, predicate
//! evaluation, the in-memory SQL engine, the catalog emulation
//! (information_schema / pg_catalog / SVV / STL / STV), the dashboard catalog
//! snapshot, and the `SqlBackend` trait boundary with its closure-backed
//! memory implementation. Part 2 ports the Redshift→PostgreSQL SQL translator
//! (`internal/services/redshift/translator`) and wires it into `execute_sql`.
//! Part 3 ports the pgwire protocol server (codec, simple + extended query
//! protocols), SQL batch execution, statement history, and state.json
//! persistence. Later parts add the Data API + control plane, COPY/UNLOAD,
//! and the Postgres backend.
//! See `README.md` for the migration order and parity discipline.

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

pub mod backend;
pub mod backend_memory;
pub mod backend_postgres;
pub mod catalog;
pub mod cluster;
pub mod copy_unload;
pub mod dataapi;
pub mod engine;
pub mod errors;
pub mod http;
pub mod http_api;
pub mod model;
pub mod pg_types;
pub mod pgwire;
pub mod pgwire_codec;
pub mod pgwire_extended;
pub mod server;
pub mod snapshot;
pub mod sql_parse;
pub mod sql_predicates;
pub mod storage;
pub mod translator;

pub use backend::{SqlBackend, SqlTransaction};
pub use backend_memory::MemoryBackend;
pub use cluster::{ClusterEndpoint, ClusterSnapshot, ClusterSnapshotMetadata, Tag};
pub use engine::QueryResult;
pub use errors::SqlError;
pub use http_api::HttpResponse;
pub use pg_types::PgField;
pub use pgwire_extended::ExtendedQuerySession;
pub use server::{Config, Server};
pub use snapshot::{
    CatalogSnapshot, DashboardSqlError, QueryResultSnapshot, ServiceSnapshot, StatementSnapshot,
    TableDetailSnapshot,
};
pub use translator::{Passthrough, RedshiftToPostgres, RedshiftTranslator};
