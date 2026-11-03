//! STS, run in-process as a supervisor task. Stateless: no storage directory.

use std::future::Future;
use std::sync::Arc;

use devcloud_sts::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the STS HTTP server until it errors or `shutdown` resolves.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.sts_port);
    let server = Arc::new(Server::new(Config {
        addr: addr.clone(),
        region: cfg.services.sts.region.clone(),
        account_id: cfg.auth.sts.account_id.clone(),
        auth_mode: cfg.auth.sts.mode.clone(),
        access_key_id: cfg.auth.sts.access_key_id.clone(),
        secret_access_key: cfg.auth.sts.secret_access_key.clone(),
    }));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("sts: bind {addr}: {e}"))?;
    devcloud_sts::http::serve(listener, server, shutdown)
        .await
        .map_err(|e| format!("sts: server error: {e}"))
}
