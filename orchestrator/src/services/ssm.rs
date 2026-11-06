//! SSM Parameter Store, run in-process as a supervisor task.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_ssm::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the SSM HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: state under `<storage>/ssm`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.ssm_port);
    let storage = Path::new(&cfg.storage.path).join("ssm");

    let server = Arc::new(Server::new(Config {
        addr: addr.clone(),
        region: cfg.services.ssm.region.clone(),
        account_id: cfg.auth.ssm.account_id.clone(),
        auth_mode: cfg.auth.ssm.mode.clone(),
        access_key_id: cfg.auth.ssm.access_key_id.clone(),
        secret_access_key: cfg.auth.ssm.secret_access_key.clone(),
        storage_path: storage.to_string_lossy().into_owned(),
    }));
    if let Some(err) = server.load_err() {
        return Err(format!("ssm: failed to load state: {err}"));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("ssm: bind {addr}: {e}"))?;
    devcloud_ssm::http::serve(listener, server, shutdown)
        .await
        .map_err(|e| format!("ssm: server error: {e}"))
}
