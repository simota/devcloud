//! `devcloud-bigquery` binary: serves the BigQuery v2 JSON API on the loopback
//! address the legacy daemon would have used, persisting to the same storage root
//! and reading/writing `gs://` job URIs through the shared S3 bucket store.
//!
//! Configuration comes from environment variables (set by
//! `internal/app/bigquery_rust.rs`):
//!
//!   DEVCLOUD_BIGQUERY_ADDR                listen address (host:port)
//!   DEVCLOUD_BIGQUERY_PROJECT             default project
//!   DEVCLOUD_BIGQUERY_LOCATION            default location
//!   DEVCLOUD_BIGQUERY_AUTH_MODE           relaxed | oauth-relaxed | bearer-dev | strict
//!   DEVCLOUD_BIGQUERY_BEARER_TOKEN        expected bearer token
//!   DEVCLOUD_BIGQUERY_STORAGE             BigQuery storage root
//!   DEVCLOUD_BIGQUERY_OBJECT_STORE        shared S3 bucket storage root (optional;
//!                                         load/extract jobs fail without it, like
//!                                         legacy nil ObjectStore)
//!   DEVCLOUD_BIGQUERY_MAX_ROWS_PER_TABLE  per-table row cap (0 = default)
//!   DEVCLOUD_BIGQUERY_MAX_REQUEST_BYTES   request size cap (0 = default)
//!   DEVCLOUD_BIGQUERY_MAX_RESULT_ROWS     query result row cap (0 = default)
//!   DEVCLOUD_BIGQUERY_DEFAULT_LEGACY_SQL  "true" to default useLegacySql
//!
//! Successful mutations print `DEVCLOUD_EVENT {json}` lines on stdout for the
//! daemon's dashboard event bridge.

use std::sync::Arc;

use devcloud_bigquery::server::{Config, Server};
use devcloud_s3::store::FileBucketStore;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn env_i64(name: &str) -> i64 {
    env(name).trim().parse().unwrap_or(0)
}

fn main() {
    let addr = env("DEVCLOUD_BIGQUERY_ADDR");
    if addr.is_empty() {
        eprintln!("devcloud-bigquery: DEVCLOUD_BIGQUERY_ADDR is required");
        std::process::exit(2);
    }
    let storage = env("DEVCLOUD_BIGQUERY_STORAGE");
    if storage.is_empty() {
        eprintln!("devcloud-bigquery: DEVCLOUD_BIGQUERY_STORAGE is required");
        std::process::exit(2);
    }
    let mut server = Server::new(Config {
        addr: addr.clone(),
        project: env("DEVCLOUD_BIGQUERY_PROJECT"),
        location: env("DEVCLOUD_BIGQUERY_LOCATION"),
        auth_mode: env("DEVCLOUD_BIGQUERY_AUTH_MODE"),
        bearer_token: env("DEVCLOUD_BIGQUERY_BEARER_TOKEN"),
        storage_path: storage,
        max_rows_per_table: env_i64("DEVCLOUD_BIGQUERY_MAX_ROWS_PER_TABLE"),
        max_request_bytes: env_i64("DEVCLOUD_BIGQUERY_MAX_REQUEST_BYTES"),
        max_result_rows: env_i64("DEVCLOUD_BIGQUERY_MAX_RESULT_ROWS"),
        default_legacy_sql: env("DEVCLOUD_BIGQUERY_DEFAULT_LEGACY_SQL").trim() == "true",
    })
    .enable_events();
    let object_store = env("DEVCLOUD_BIGQUERY_OBJECT_STORE");
    if !object_store.is_empty() {
        server = server.with_object_store(FileBucketStore::new(object_store));
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("devcloud-bigquery: bind {addr}: {e}");
                std::process::exit(1);
            }
        };
        let shared = Arc::new(server);
        if let Err(e) = devcloud_bigquery::http::serve(listener, shared, shutdown_signal()).await {
            eprintln!("devcloud-bigquery: serve error: {e}");
            std::process::exit(1);
        }
    });
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
