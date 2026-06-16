//! S3 service, run in-process as a supervisor task.
//!
//! Constructs the same `devcloud_s3` server the standalone `devcloud-s3`
//! binary did, but in-process — replacing env-var plumbing with direct Config
//! field access.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use devcloud_s3::http::AuthConfig;
use devcloud_s3::store::FileBucketStore;

use crate::config::Config;

/// Runs the S3 HTTP server until it errors or `shutdown` resolves.
///
/// Storage layout: bucket data under `<storage>/s3/buckets`, matching the legacy
/// daemon's `internal/services/s3` on-disk format.
pub async fn run(cfg: &Config, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.server.s3_port);
    let storage = Path::new(&cfg.storage.path).join("s3/buckets");
    let store = Arc::new(Mutex::new(FileBucketStore::new(storage)));
    let auth = AuthConfig {
        auth_mode: cfg.auth.s3.mode.clone(),
        access_key_id: cfg.auth.s3.access_key_id.clone(),
        secret_access_key: cfg.auth.s3.secret_access_key.clone(),
        region: cfg.services.s3.region.clone(),
    };

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("s3: bind {addr}: {e}"))?;

    tokio::select! {
        res = devcloud_s3::http::serve_with_auth(listener, store, auth, shutdown) => {
            res.map_err(|e| format!("s3: server error: {e}"))
        }
    }
}
