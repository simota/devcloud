//! BigQuery service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_bigquery` server the standalone
//! `devcloud-bigquery` binary did, but in-process. When S3 or GCS is also
//! enabled the object store is wired in so load/extract jobs can read/write
//! `gs://` and `s3://` URIs without leaving the process.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_bigquery::server::{Config, Server};
use devcloud_s3::store::FileBucketStore;

use crate::config::Config as OrchestratorConfig;

fn default_string(a: &str, b: &str) -> String {
    if !a.is_empty() {
        a.to_string()
    } else {
        b.to_string()
    }
}

/// Runs the BigQuery JSON API server until it errors or `shutdown` resolves.
///
/// Storage layout: dataset/table data under `<storage>/bigquery`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.bigquery_port);
    let root = Path::new(&cfg.storage.path);
    let storage = root.join("bigquery");

    let project = default_string(&cfg.services.bigquery.project, &cfg.auth.bigquery.project);
    let mut server = Server::new(Config {
        addr: addr.clone(),
        project,
        location: cfg.services.bigquery.location.clone(),
        auth_mode: cfg.auth.bigquery.mode.clone(),
        bearer_token: cfg.auth.bigquery.bearer_token.clone(),
        storage_path: storage.to_string_lossy().into_owned(),
        max_rows_per_table: cfg.services.bigquery.max_rows_per_table,
        max_request_bytes: cfg.services.bigquery.max_request_bytes,
        max_result_rows: cfg.services.bigquery.query.max_result_rows as i64,
        default_legacy_sql: cfg.services.bigquery.query.default_use_legacy_sql,
    })
    .enable_events();

    if cfg.services.s3.enabled || cfg.services.gcs.enabled {
        let object_store_path = root.join("s3/buckets");
        server = server.with_object_store(FileBucketStore::new(object_store_path));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("bigquery: bind {addr}: {e}"))?;
    let shared = Arc::new(server);

    tokio::select! {
        res = devcloud_bigquery::http::serve(listener, shared, shutdown) => {
            res.map_err(|e| format!("bigquery: server error: {e}"))
        }
    }
}
