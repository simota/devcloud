//! Dashboard configuration, sourced entirely from environment variables set by
//! the legacy daemon seam (`internal/app/dashboard_rust.rs`).
//!
//! Mirrors the legacy `dashboard.Config` struct (`internal/dashboard/server.rs`): the
//! legacy dashboard receives in-process pointers, but the Rust dashboard receives the
//! same information as **network addresses** plus the display metadata the
//! `/api/services` registry needs. Every endpoint is a full base URL
//! (`http://127.0.0.1:19324`) the forwarding client can hit directly.

/// Reads an env var, returning an empty string when unset. Empty means
/// "not configured" — the registry reports such a service as `disabled`.
fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

fn env_or(key: &str, fallback: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => fallback.to_string(),
    }
}

/// Per-service network + display configuration. The `*_base` fields are the base
/// URLs the forwarding client targets; the display fields feed `/api/services`.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Listen address for the dashboard itself, e.g. `127.0.0.1:18025`.
    pub addr: String,
    /// WebSocket base for the daemon event relay, e.g. `ws://127.0.0.1:18027`.
    pub event_relay_endpoint: String,

    // SQS — the template service for Phase 2.
    /// Base URL of the SQS service HTTP listener (provider protocol +
    /// `/_introspect/`), e.g. `http://127.0.0.1:19324`. Empty == disabled.
    pub sqs_base: String,
    pub sqs_region: String,
    pub sqs_auth_mode: String,
    pub sqs_storage_path: String,

    // Remaining services carry only display metadata in the foundation
    // increment; their `/api/<svc>/*` handlers land in later increments using
    // the SQS template. The base URL still drives the registry status.
    pub mail_base: String,
    pub mail_endpoint: String,
    pub mail_storage_path: String,
    pub mail_disabled: bool,
    pub s3_base: String,
    pub s3_endpoint: String,
    pub s3_storage_path: String,
    pub gcs_base: String,
    pub gcs_endpoint: String,
    pub gcs_storage_path: String,
    pub dynamodb_base: String,
    pub dynamodb_endpoint: String,
    pub dynamodb_storage_path: String,
    pub bigquery_base: String,
    pub bigquery_endpoint: String,
    pub bigquery_storage_path: String,
    pub redshift_base: String,
    pub redshift_sql_endpoint: String,
    pub redshift_endpoint: String,
    pub redshift_storage_path: String,
    pub redis_base: String,
    pub redis_endpoint: String,
    pub redis_storage_path: String,
    pub redis_enabled: bool,
    pub pubsub_base: String,
    pub pubsub_endpoint: String,
    pub pubsub_storage_path: String,
}

impl Config {
    /// Builds the config from the environment. The daemon seam sets every
    /// `DEVCLOUD_DASHBOARD_*` variable; unset service bases mean the service is
    /// disabled (reported as such by the registry).
    pub fn from_env() -> Self {
        Config {
            addr: env("DEVCLOUD_DASHBOARD_ADDR"),
            event_relay_endpoint: env("DEVCLOUD_DASHBOARD_EVENT_RELAY"),

            sqs_base: env("DEVCLOUD_DASHBOARD_SQS_BASE"),
            sqs_region: env_or("DEVCLOUD_DASHBOARD_SQS_REGION", "us-east-1"),
            sqs_auth_mode: env_or("DEVCLOUD_DASHBOARD_SQS_AUTH_MODE", "relaxed"),
            sqs_storage_path: env_or("DEVCLOUD_DASHBOARD_SQS_STORAGE", ".devcloud/data/sqs"),

            mail_base: env("DEVCLOUD_DASHBOARD_MAIL_BASE"),
            mail_endpoint: env_or("DEVCLOUD_DASHBOARD_MAIL_ENDPOINT", "smtp://127.0.0.1:11025"),
            mail_storage_path: env_or("DEVCLOUD_DASHBOARD_MAIL_STORAGE", ".devcloud/data/mail"),
            mail_disabled: env("DEVCLOUD_DASHBOARD_MAIL_DISABLED") == "1",
            s3_base: env("DEVCLOUD_DASHBOARD_S3_BASE"),
            s3_endpoint: env_or("DEVCLOUD_DASHBOARD_S3_ENDPOINT", "http://127.0.0.1:14566"),
            s3_storage_path: env_or("DEVCLOUD_DASHBOARD_S3_STORAGE", ".devcloud/data/s3"),
            gcs_base: env("DEVCLOUD_DASHBOARD_GCS_BASE"),
            gcs_endpoint: env_or("DEVCLOUD_DASHBOARD_GCS_ENDPOINT", "http://127.0.0.1:14443"),
            gcs_storage_path: env_or("DEVCLOUD_DASHBOARD_GCS_STORAGE", ".devcloud/data/gcs"),
            dynamodb_base: env("DEVCLOUD_DASHBOARD_DYNAMODB_BASE"),
            dynamodb_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_DYNAMODB_ENDPOINT",
                "http://127.0.0.1:18000",
            ),
            dynamodb_storage_path: env_or(
                "DEVCLOUD_DASHBOARD_DYNAMODB_STORAGE",
                ".devcloud/data/dynamodb",
            ),
            bigquery_base: env("DEVCLOUD_DASHBOARD_BIGQUERY_BASE"),
            bigquery_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_BIGQUERY_ENDPOINT",
                "http://127.0.0.1:19050",
            ),
            bigquery_storage_path: env_or(
                "DEVCLOUD_DASHBOARD_BIGQUERY_STORAGE",
                ".devcloud/data/bigquery",
            ),
            redshift_base: env("DEVCLOUD_DASHBOARD_REDSHIFT_BASE"),
            redshift_sql_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_REDSHIFT_SQL_ENDPOINT",
                "127.0.0.1:15439",
            ),
            redshift_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_REDSHIFT_ENDPOINT",
                "http://127.0.0.1:19099",
            ),
            redshift_storage_path: env_or(
                "DEVCLOUD_DASHBOARD_REDSHIFT_STORAGE",
                ".devcloud/data/redshift",
            ),
            redis_base: env("DEVCLOUD_DASHBOARD_REDIS_BASE"),
            redis_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_REDIS_ENDPOINT",
                "redis://127.0.0.1:16379",
            ),
            redis_storage_path: env_or("DEVCLOUD_DASHBOARD_REDIS_STORAGE", ".devcloud/data/redis"),
            redis_enabled: env("DEVCLOUD_DASHBOARD_REDIS_ENABLED") == "1",
            pubsub_base: env("DEVCLOUD_DASHBOARD_PUBSUB_BASE"),
            pubsub_endpoint: env_or(
                "DEVCLOUD_DASHBOARD_PUBSUB_ENDPOINT",
                "http://127.0.0.1:18086",
            ),
            pubsub_storage_path: env_or(
                "DEVCLOUD_DASHBOARD_PUBSUB_STORAGE",
                ".devcloud/data/pubsub",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvRestore {
        key: &'static str,
        value: Option<String>,
    }

    impl EnvRestore {
        fn unset(key: &'static str) -> Self {
            let value = std::env::var(key).ok();
            std::env::remove_var(key);
            EnvRestore { key, value }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            match &self.value {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn from_env_defaults_s3_and_gcs_to_distinct_storage_paths() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _s3 = EnvRestore::unset("DEVCLOUD_DASHBOARD_S3_STORAGE");
        let _gcs = EnvRestore::unset("DEVCLOUD_DASHBOARD_GCS_STORAGE");

        let cfg = Config::from_env();

        assert_eq!(cfg.s3_storage_path, ".devcloud/data/s3");
        assert_eq!(cfg.gcs_storage_path, ".devcloud/data/gcs");
    }
}
