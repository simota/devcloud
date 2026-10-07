//! Lambda service, run in-process as a supervisor task.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use devcloud_lambda::runtime::FunctionCredentials;
use devcloud_lambda::{Config, Server};

use crate::config::{Config as OrchestratorConfig, LambdaServiceConfig};

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
        interpreters: devcloud_lambda::runtime::Interpreters {
            docker: cfg.services.lambda.docker.then(|| "docker".to_string()),
            ..Default::default()
        },
        function_credentials: function_credentials(&cfg.services.lambda)?,
        opt_dir: (!cfg.services.lambda.opt_dir.is_empty())
            .then(|| PathBuf::from(&cfg.services.lambda.opt_dir)),
        idle_timeout: cfg
            .services
            .lambda
            .idle_timeout_seconds
            .map(|s| std::time::Duration::from_secs(s.into())),
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

/// Credentials for handlers, from `services.lambda.function*`. A key id
/// without a secret (or the reverse) is a configuration error.
fn function_credentials(l: &LambdaServiceConfig) -> Result<Option<FunctionCredentials>, String> {
    match (
        l.function_access_key_id.is_empty(),
        l.function_secret_access_key.is_empty(),
    ) {
        (true, true) => Ok(None),
        (false, false) => Ok(Some(FunctionCredentials {
            access_key_id: l.function_access_key_id.clone(),
            secret_access_key: l.function_secret_access_key.clone(),
            session_token: l.function_session_token.clone(),
        })),
        _ => Err("lambda: set both services.lambda.functionAccessKeyId and functionSecretAccessKey, or neither".to_string()),
    }
}
