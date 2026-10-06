//! Cloud Run service, run in-process as a supervisor task.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_cloudrun::{Config, Server};

use crate::config::Config as OrchestratorConfig;

/// Runs the Cloud Run control plane + data-plane proxy until it errors or
/// `shutdown` resolves; service instances are stopped on the way out.
///
/// Storage layout: state under `<storage>/cloudrun`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.cloud_run_port);
    let sv = &cfg.services.cloud_run;

    let config = Config {
        addr: addr.clone(),
        project: sv.project.clone(),
        region: sv.region.clone(),
        auth_mode: cfg.auth.cloud_run.mode.clone(),
        bearer_token: cfg.auth.cloud_run.bearer_token.clone(),
        storage_path: Path::new(&cfg.storage.path)
            .join("cloudrun")
            .to_string_lossy()
            .into_owned(),
        docker_bin: sv.docker.then(|| "docker".to_string()),
    };

    let server = Arc::new(Server::new(config));
    if let Some(err) = server.load_err() {
        return Err(format!("cloudrun: failed to load state: {err}"));
    }

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("cloudrun: bind {addr}: {e}"))?;
    devcloud_cloudrun::http::serve(listener, server, shutdown)
        .await
        .map_err(|e| format!("cloudrun: server error: {e}"))
}
