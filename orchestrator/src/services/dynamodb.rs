//! DynamoDB service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_dynamodb` server the standalone
//! `devcloud-dynamodb` binary did, but in-process.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use devcloud_dynamodb::server::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the DynamoDB HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: state under `<storage>/dynamodb`, matching the legacy daemon's
/// `internal/services/dynamodb` on-disk format.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.dynamodb_port);
    let storage = Path::new(&cfg.storage.path).join("dynamodb");

    let config = Config {
        addr: addr.clone(),
        region: cfg.services.dynamodb.region.clone(),
        auth_mode: cfg.auth.dynamodb.mode.clone(),
        access_key_id: cfg.auth.dynamodb.access_key_id.clone(),
        secret_access_key: cfg.auth.dynamodb.secret_access_key.clone(),
        storage_path: storage.to_string_lossy().into_owned(),
        max_item_bytes: cfg.services.dynamodb.max_item_bytes,
        max_tables: cfg.services.dynamodb.max_tables as i64,
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        return Err(format!("dynamodb: failed to load state: {err}"));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("dynamodb: bind {addr}: {e}"))?;
    let shared = Arc::new(Mutex::new(server));
    let auth_mode = cfg.auth.dynamodb.mode.clone();

    tokio::select! {
        res = devcloud_dynamodb::http::serve(listener, shared, auth_mode, shutdown) => {
            res.map_err(|e| format!("dynamodb: server error: {e}"))
        }
    }
}
