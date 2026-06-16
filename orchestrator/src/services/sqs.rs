//! SQS service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_sqs` server the standalone `devcloud-sqs`
//! binary did, but in-process.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use devcloud_sqs::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the SQS HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: queue state under `<storage>/sqs`, matching the legacy daemon's
/// `internal/services/sqs` on-disk format.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.sqs_port);
    let storage = Path::new(&cfg.storage.path).join("sqs");

    let config = Config {
        addr: addr.clone(),
        region: cfg.services.sqs.region.clone(),
        account_id: cfg.auth.sqs.account_id.clone(),
        queue_url_host: cfg.services.sqs.queue_url_host.clone(),
        auth_mode: cfg.auth.sqs.mode.clone(),
        access_key_id: cfg.auth.sqs.access_key_id.clone(),
        secret_access_key: cfg.auth.sqs.secret_access_key.clone(),
        storage_path: storage.to_string_lossy().into_owned(),
        max_queues: cfg.services.sqs.max_queues as i64,
        max_message_bytes: cfg.services.sqs.max_message_bytes,
        max_receive_batch_size: cfg.services.sqs.max_receive_batch_size as i64,
        default_visibility_timeout_seconds: cfg.services.sqs.default_visibility_timeout_seconds
            as i64,
        default_delay_seconds: cfg.services.sqs.default_delay_seconds as i64,
        default_message_retention_seconds: cfg.services.sqs.default_message_retention_seconds
            as i64,
        default_receive_wait_time_seconds: cfg.services.sqs.default_receive_wait_time_seconds
            as i64,
    };

    let server = Server::new(config);
    if let Some(err) = server.load_err() {
        return Err(format!("sqs: failed to load state: {err}"));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("sqs: bind {addr}: {e}"))?;
    let shared = Arc::new(Mutex::new(server));

    tokio::select! {
        res = devcloud_sqs::http::serve(listener, shared, shutdown) => {
            res.map_err(|e| format!("sqs: server error: {e}"))
        }
    }
}
