//! Lambda service, run in-process as a supervisor task.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use devcloud_lambda::aws_client::AwsEndpoint;
use devcloud_lambda::event_poller::EventSources;
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
        url_auth_mode: cfg.services.lambda.url_auth_mode.clone(),
        log_invocations: cfg.services.lambda.log_invocations,
        access_key_id: cfg.auth.lambda.access_key_id.clone(),
        secret_access_key: cfg.auth.lambda.secret_access_key.clone(),
        storage_path: root.join("lambda").to_string_lossy().into_owned(),
        endpoint: format!("http://{addr}"),
        object_store_root: cfg.services.s3.enabled.then(|| root.join("s3/buckets")),
        interpreters: devcloud_lambda::runtime::Interpreters {
            docker: cfg.services.lambda.docker.then(|| "docker".to_string()),
            docker_network: Some(cfg.services.lambda.docker_network.clone())
                .filter(|n| !n.is_empty()),
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
        event_sources: event_sources(cfg),
    };

    let server = Arc::new(Server::new(config));
    if let Some(err) = server.load_err() {
        return Err(format!("lambda: failed to load state: {err}"));
    }
    server.remove_orphaned_containers().await;

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("lambda: bind {addr}: {e}"))?;
    devcloud_lambda::http::serve(listener, server, shutdown)
        .await
        .map_err(|e| format!("lambda: server error: {e}"))
}

/// The local SQS and DynamoDB services that event source mappings poll,
/// each only when enabled, signed with that service's configured
/// credentials (so a `strict` SQS or DynamoDB accepts the poller).
fn event_sources(cfg: &OrchestratorConfig) -> EventSources {
    let endpoint = |port: i32, region: &str, key: &str, secret: &str| AwsEndpoint {
        url: format!("http://127.0.0.1:{port}"),
        region: region.to_string(),
        access_key_id: key.to_string(),
        secret_access_key: secret.to_string(),
    };
    EventSources {
        sqs: cfg.services.sqs.enabled.then(|| {
            endpoint(
                cfg.server.sqs_port,
                &cfg.services.sqs.region,
                &cfg.auth.sqs.access_key_id,
                &cfg.auth.sqs.secret_access_key,
            )
        }),
        dynamodb: cfg.services.dynamodb.enabled.then(|| {
            endpoint(
                cfg.server.dynamodb_port,
                &cfg.services.dynamodb.region,
                &cfg.auth.dynamodb.access_key_id,
                &cfg.auth.dynamodb.secret_access_key,
            )
        }),
        poll_interval: None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_sources_follow_enabled_sqs_and_dynamodb() {
        let mut cfg = OrchestratorConfig::default();
        let on = event_sources(&cfg);
        let sqs = on.sqs.expect("sqs enabled by default");
        assert_eq!(sqs.url, format!("http://127.0.0.1:{}", cfg.server.sqs_port));
        assert_eq!(sqs.access_key_id, cfg.auth.sqs.access_key_id);
        assert_eq!(sqs.region, cfg.services.sqs.region);
        let ddb = on.dynamodb.expect("dynamodb enabled by default");
        assert_eq!(
            ddb.url,
            format!("http://127.0.0.1:{}", cfg.server.dynamodb_port)
        );
        assert_eq!(ddb.access_key_id, cfg.auth.dynamodb.access_key_id);

        cfg.services.sqs.enabled = false;
        cfg.services.dynamodb.enabled = false;
        let off = event_sources(&cfg);
        assert!(off.sqs.is_none());
        assert!(off.dynamodb.is_none());
    }
}
