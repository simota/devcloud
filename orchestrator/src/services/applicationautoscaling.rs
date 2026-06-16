//! Application Auto Scaling service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_applicationautoscaling` server the standalone
//! binary did, but in-process.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_applicationautoscaling::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the Application Auto Scaling HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: state under `<storage>/applicationautoscaling`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.app_auto_scaling_port);
    let storage = Path::new(&cfg.storage.path).join("applicationautoscaling");

    let config = Config {
        addr: addr.clone(),
        region: cfg.services.app_auto_scaling.region.clone(),
        account_id: cfg.auth.app_auto_scaling.account_id.clone(),
        auth_mode: cfg.auth.app_auto_scaling.mode.clone(),
        access_key_id: cfg.auth.app_auto_scaling.access_key_id.clone(),
        secret_access_key: cfg.auth.app_auto_scaling.secret_access_key.clone(),
        storage_path: storage.to_string_lossy().into_owned(),
    };

    let server = Arc::new(Server::new(config));
    if let Some(err) = server.load_err() {
        return Err(format!(
            "applicationautoscaling: failed to load state: {err}"
        ));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("applicationautoscaling: bind {addr}: {e}"))?;

    tokio::select! {
        res = devcloud_applicationautoscaling::http::serve(listener, server, shutdown) => {
            res.map_err(|e| format!("applicationautoscaling: server error: {e}"))
        }
    }
}
