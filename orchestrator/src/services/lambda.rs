//! Lambda service, run in-process as a supervisor task.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_lambda::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the Lambda HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: state + packages under `<storage>/lambda`. When S3 is
/// enabled, `Code.S3Bucket`/`S3Key` read from the shared `<storage>/s3/buckets`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.lambda_port);
    let root = Path::new(&cfg.storage.path);

    let config = Config {
        addr: addr.clone(),
        region: cfg.services.lambda.region.clone(),
        account_id: cfg.auth.lambda.account_id.clone(),
        auth_mode: cfg.auth.lambda.mode.clone(),
        access_key_id: cfg.auth.lambda.access_key_id.clone(),
        secret_access_key: cfg.auth.lambda.secret_access_key.clone(),
        storage_path: root.join("lambda").to_string_lossy().into_owned(),
        endpoint: format!("http://{addr}"),
        object_store_root: cfg.services.s3.enabled.then(|| root.join("s3/buckets")),
        interpreters: Default::default(),
    };

    let server = Arc::new(Server::new(config));
    if let Some(err) = server.load_err() {
        return Err(format!("lambda: failed to load state: {err}"));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("lambda: bind {addr}: {e}"))?;
    devcloud_lambda::http::serve(listener, server, shutdown)
        .await
        .map_err(|e| format!("lambda: server error: {e}"))
}
