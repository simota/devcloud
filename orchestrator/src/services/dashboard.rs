//! Dashboard: the single user-facing HTTP entry (`:18025`). Serves the embedded
//! React SPA + `/api/*` forwarding to each service's network endpoint + the
//! events WS proxy to the relay. Mirrors the legacy daemon's `dashboard.Config`
//! wiring (`daemon.rs`) and the `dashboard_rust.rs` env contract, but builds the
//! Config directly in-process. Always starts (independent of `services.*.enabled`).

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use devcloud_dashboard::{http, Config};
use tokio::net::TcpListener;

use crate::config::Config as AppConfig;
use crate::services::util::{default_string, scoped_data_dir};

fn http_ep(port: i32) -> String {
    format!("http://127.0.0.1:{port}")
}

fn enabled_ep(enabled: bool, port: i32) -> String {
    if enabled {
        http_ep(port)
    } else {
        String::new()
    }
}

fn join(path: &str, sub: &str) -> String {
    Path::new(path).join(sub).to_string_lossy().into_owned()
}

/// Mirrors legacy `redisEndpointForDisplay`: display-only metadata.
fn redis_display_endpoint(cfg: &AppConfig) -> String {
    let fallback = format!("redis://127.0.0.1:{}", cfg.server.redis_port);
    let mode = cfg.services.redis.mode.trim().to_lowercase();
    let is_external = mode == "external"
        || (mode.is_empty() && !cfg.services.redis.external_url.trim().is_empty());
    if !is_external {
        return fallback;
    }
    let raw = cfg.services.redis.external_url.trim();
    if raw.is_empty() {
        fallback
    } else {
        raw.to_string()
    }
}

fn build_config(cfg: &AppConfig) -> Config {
    let s = &cfg.storage.path;
    Config {
        addr: format!("127.0.0.1:{}", cfg.server.dashboard_port),
        event_relay_endpoint: format!("ws://127.0.0.1:{}", cfg.server.event_relay_port),

        sqs_base: http_ep(cfg.server.sqs_port),
        sqs_region: cfg.services.sqs.region.clone(),
        sqs_auth_mode: cfg.auth.sqs.mode.clone(),
        sqs_storage_path: join(s, "sqs"),

        mail_base: http_ep(cfg.server.mail_http_port),
        mail_endpoint: format!("smtp://127.0.0.1:{}", cfg.server.smtp_port),
        mail_storage_path: join(s, "mail"),
        mail_disabled: !cfg.services.mail.enabled,

        s3_base: http_ep(cfg.server.s3_port),
        s3_endpoint: http_ep(cfg.server.s3_port),
        s3_storage_path: join(s, "s3"),

        gcs_base: http_ep(cfg.server.gcs_port),
        gcs_endpoint: http_ep(cfg.server.gcs_port),
        gcs_storage_path: join(s, "gcs"),

        dynamodb_base: http_ep(cfg.server.dynamodb_port),
        dynamodb_endpoint: http_ep(cfg.server.dynamodb_port),
        dynamodb_storage_path: join(s, "dynamodb"),

        bigquery_base: http_ep(cfg.server.bigquery_port),
        bigquery_endpoint: http_ep(cfg.server.bigquery_port),
        bigquery_storage_path: join(s, "bigquery"),

        redshift_base: http_ep(cfg.server.redshift_api_port),
        redshift_sql_endpoint: format!("127.0.0.1:{}", cfg.server.redshift_port),
        redshift_endpoint: http_ep(cfg.server.redshift_api_port),
        redshift_storage_path: scoped_data_dir(s, &cfg.services.redshift.data_dir, "redshift"),

        redis_base: http_ep(cfg.server.redis_http_port),
        redis_endpoint: redis_display_endpoint(cfg),
        redis_storage_path: scoped_data_dir(s, &cfg.services.redis.data_dir, "redis"),
        redis_enabled: cfg.services.redis.enabled,

        pubsub_base: http_ep(cfg.server.pubsub_rest_port),
        pubsub_endpoint: http_ep(cfg.server.pubsub_rest_port),
        pubsub_storage_path: default_string(&cfg.services.pubsub.data_dir, &join(s, "pubsub")),

        app_auto_scaling_base: http_ep(cfg.server.app_auto_scaling_port),
        app_auto_scaling_endpoint: http_ep(cfg.server.app_auto_scaling_port),
        app_auto_scaling_storage_path: join(s, "applicationautoscaling"),
        app_auto_scaling_region: cfg.services.app_auto_scaling.region.clone(),

        // Empty base == disabled: the registry/status handlers report the
        // service off instead of forwarding to a port nothing listens on.
        lambda_base: enabled_ep(cfg.services.lambda.enabled, cfg.server.lambda_port),
        lambda_endpoint: http_ep(cfg.server.lambda_port),
        lambda_storage_path: join(s, "lambda"),
        lambda_region: cfg.services.lambda.region.clone(),
        lambda_auth_mode: cfg.auth.lambda.mode.clone(),
        lambda_access_key_id: cfg.auth.lambda.access_key_id.clone(),
        lambda_secret_access_key: cfg.auth.lambda.secret_access_key.clone(),

        cloudrun_base: enabled_ep(cfg.services.cloud_run.enabled, cfg.server.cloud_run_port),
        cloudrun_endpoint: http_ep(cfg.server.cloud_run_port),
        cloudrun_storage_path: join(s, "cloudrun"),
        cloudrun_project: cfg.services.cloud_run.project.clone(),
        cloudrun_region: cfg.services.cloud_run.region.clone(),
        cloudrun_auth_mode: cfg.auth.cloud_run.mode.clone(),
        cloudrun_bearer_token: cfg.auth.cloud_run.bearer_token.clone(),
    }
}

pub async fn run(cfg: &AppConfig, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let config = build_config(cfg);
    let addr = config.addr.clone();
    let config = Arc::new(config);
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("dashboard: bind {addr}: {e}"))?;
    http::serve(listener, config, shutdown)
        .await
        .map_err(|e| format!("dashboard: serve error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_config_uses_distinct_s3_and_gcs_storage_paths() {
        let cfg = AppConfig::default();
        let dashboard = build_config(&cfg);

        assert_eq!(dashboard.s3_storage_path, ".devcloud/data/s3");
        assert_eq!(dashboard.gcs_storage_path, ".devcloud/data/gcs");
    }

    #[test]
    fn disabled_lambda_and_cloudrun_have_no_forward_base() {
        let mut cfg = AppConfig::default();
        let on = build_config(&cfg);
        assert_eq!(on.lambda_base, "http://127.0.0.1:19010");
        assert_eq!(on.cloudrun_base, "http://127.0.0.1:18095");
        cfg.services.lambda.enabled = false;
        cfg.services.cloud_run.enabled = false;
        let off = build_config(&cfg);
        assert!(off.lambda_base.is_empty());
        assert!(off.cloudrun_base.is_empty());
        assert_eq!(
            off.lambda_endpoint, "http://127.0.0.1:19010",
            "display endpoint stays"
        );
    }
}
