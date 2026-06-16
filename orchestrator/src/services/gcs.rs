//! GCS service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_gcs` server the standalone `devcloud-gcs`
//! binary did, but in-process. GCS shares the S3 bucket store root so that
//! objects written via one protocol are readable via the other.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use devcloud_gcs::http::{Config, Server};
use devcloud_s3::store::FileBucketStore;

use crate::config::Config as OrchestratorConfig;

fn default_string(a: &str, b: &str) -> String {
    if !a.is_empty() {
        a.to_string()
    } else {
        b.to_string()
    }
}

/// Runs the GCS JSON API server until it errors or `shutdown` resolves.
///
/// Storage layout: shared bucket data under `<storage>/s3/buckets` (same root
/// as the S3 service), upload session state under `<storage>/gcs/upload_sessions`.
pub async fn run(
    cfg: &OrchestratorConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.gcs_port);
    let root = Path::new(&cfg.storage.path);
    let storage = root.join("s3/buckets");
    let upload_session_path = root.join("gcs/upload_sessions");

    let project = default_string(&cfg.services.gcs.project, &cfg.auth.gcs.project);
    let server = Server::new(
        Config {
            project,
            location: cfg.services.gcs.location.clone(),
            auth_mode: cfg.auth.gcs.mode.clone(),
            bearer_token: cfg.auth.gcs.bearer_token.clone(),
            upload_session_path: upload_session_path.to_string_lossy().into_owned(),
        },
        FileBucketStore::new(storage),
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("gcs: bind {addr}: {e}"))?;
    let shared = Arc::new(Mutex::new(server));

    tokio::select! {
        res = devcloud_gcs::http::serve(listener, shared, shutdown) => {
            res.map_err(|e| format!("gcs: server error: {e}"))
        }
    }
}
