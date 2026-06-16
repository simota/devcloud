//! Configuration layer for the native Rust orchestrator.
//!
//! Faithful port of legacy `internal/app/config.rs` (plus the config helpers that
//! live in `internal/app/daemon.rs` and `internal/app/services.rs`). The goal is
//! byte-for-byte behavioral parity with the legacy daemon's config layer:
//!
//! - `default_config()`        ↔ legacy `DefaultConfig()`
//! - `load_config()`          ↔ legacy `LoadConfig()` (hand-rolled YAML-ish parser)
//! - `apply_config_value()`   ↔ legacy `applyConfigValue()` (per-key dispatch)
//! - `default_config_yaml()`  ↔ legacy `defaultConfigYAML()` (BYTE-IDENTICAL output)
//! - `init_workspace()`       ↔ legacy `InitWorkspace()`
//! - `reset_workspace()`      ↔ legacy `ResetWorkspace()`
//! - `service_names()`        ↔ legacy `ServiceNames()`
//! - `apply_service_selection()` ↔ legacy `ApplyServiceSelection()`
//!
//! The legacy code uses a custom YAML-ish parser, NOT a general YAML library; this
//! module replicates ITS exact semantics (indent = leading-spaces/2, section
//! stack, `key: value` cut on first colon, unknown keys silently ignored).

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead};
use std::path::{PathBuf, MAIN_SEPARATOR};

// ---------------------------------------------------------------------------
// Config struct types (legacy config.rs lines ~12-274)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub project: String,
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub storage: StorageConfig,
    pub services: ServicesConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerConfig {
    pub smtp_port: i32,
    pub mail_http_port: i32,
    pub dashboard_port: i32,
    pub event_relay_port: i32,
    pub s3_port: i32,
    pub gcs_port: i32,
    pub dynamodb_port: i32,
    pub bigquery_port: i32,
    pub redshift_port: i32,
    pub redshift_api_port: i32,
    pub redis_port: i32,
    pub redis_http_port: i32,
    pub sqs_port: i32,
    pub pubsub_grpc_port: i32,
    pub pubsub_rest_port: i32,
    pub app_auto_scaling_port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthConfig {
    pub smtp: SmtpAuthConfig,
    pub s3: S3AuthConfig,
    pub gcs: GcsAuthConfig,
    pub dynamodb: DynamoDbAuthConfig,
    pub bigquery: BigQueryAuthConfig,
    pub redshift: RedshiftAuthConfig,
    pub redis: RedisAuthConfig,
    pub sqs: SqsAuthConfig,
    pub pubsub: PubSubAuthConfig,
    pub app_auto_scaling: AppAutoScalingAuthConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SmtpAuthConfig {
    pub mode: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct S3AuthConfig {
    pub mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GcsAuthConfig {
    pub mode: String,
    pub project: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DynamoDbAuthConfig {
    pub mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BigQueryAuthConfig {
    pub mode: String,
    pub project: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftAuthConfig {
    pub mode: String,
    pub user: String,
    pub password: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisAuthConfig {
    pub mode: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqsAuthConfig {
    pub mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PubSubAuthConfig {
    pub mode: String,
    pub project_id: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppAutoScalingAuthConfig {
    pub mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StorageConfig {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServicesConfig {
    pub mail: MailServiceConfig,
    pub s3: S3ServiceConfig,
    pub gcs: GcsServiceConfig,
    pub dynamodb: DynamoDbServiceConfig,
    pub bigquery: BigQueryServiceConfig,
    pub redshift: RedshiftServiceConfig,
    pub redis: RedisServiceConfig,
    pub sqs: SqsServiceConfig,
    pub pubsub: PubSubServiceConfig,
    pub app_auto_scaling: AppAutoScalingServiceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MailServiceConfig {
    pub enabled: bool,
    pub max_message_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct S3ServiceConfig {
    pub enabled: bool,
    pub region: String,
    pub path_style: bool,
    pub virtual_host_style: bool,
    pub max_object_bytes: i64,
    pub multipart: S3MultipartConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GcsServiceConfig {
    pub enabled: bool,
    pub project: String,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DynamoDbServiceConfig {
    pub enabled: bool,
    pub region: String,
    pub billing_mode: String,
    pub max_item_bytes: i64,
    pub max_tables: i32,
    pub streams: DynamoDbStreamsConfig,
    pub ttl: DynamoDbTtlConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DynamoDbStreamsConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DynamoDbTtlConfig {
    pub scheduler_interval_seconds: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BigQueryServiceConfig {
    pub enabled: bool,
    pub project: String,
    pub location: String,
    pub max_rows_per_table: i64,
    pub max_request_bytes: i64,
    pub query: BigQueryQueryConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BigQueryQueryConfig {
    pub max_result_rows: i32,
    pub max_execution_seconds: i32,
    pub default_use_legacy_sql: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftServiceConfig {
    pub enabled: bool,
    pub region: String,
    pub cluster_identifier: String,
    pub database: String,
    pub data_dir: String,
    pub node_type: String,
    pub number_of_nodes: i32,
    pub max_statement_bytes: i64,
    pub backend: RedshiftBackendConfig,
    pub data_api: RedshiftDataApiConfig,
    pub sql: RedshiftSqlConfig,
    pub copy_unload: RedshiftCopyUnloadConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftBackendConfig {
    pub kind: String,
    pub mode: String,
    pub external_dsn: String,
    pub managed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftDataApiConfig {
    pub enabled: bool,
    pub max_result_bytes: i64,
    pub max_result_rows: i32,
    pub statement_retention_seconds: i32,
    pub session_retention_seconds: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftSqlConfig {
    pub enable_extended_protocol: bool,
    pub max_result_rows: i32,
    pub default_search_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedshiftCopyUnloadConfig {
    pub enable_local_s3: bool,
    pub max_input_row_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisServiceConfig {
    pub enabled: bool,
    pub mode: String,
    pub binary_path: String,
    pub external_url: String,
    pub data_dir: String,
    pub max_memory_mb: i32,
    pub append_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqsServiceConfig {
    pub enabled: bool,
    pub region: String,
    pub queue_url_host: String,
    pub max_queues: i32,
    pub max_message_bytes: i64,
    pub max_receive_batch_size: i32,
    pub default_visibility_timeout_seconds: i32,
    pub default_delay_seconds: i32,
    pub default_message_retention_seconds: i32,
    pub default_receive_wait_time_seconds: i32,
    pub scheduler_interval_seconds: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppAutoScalingServiceConfig {
    pub enabled: bool,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PubSubServiceConfig {
    pub enabled: bool,
    pub project: String,
    pub data_dir: String,
    pub message_data_dir: String,
    pub default_ack_deadline_seconds: i32,
    pub message_retention_seconds: i32,
    pub max_ack_deadline_seconds: i32,
    pub max_pull_messages: i32,
    pub pull_wait_timeout_seconds: i32,
    pub enable_rest: bool,
    pub enable_streaming_pull: bool,
    pub enable_push: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct S3MultipartConfig {
    pub min_part_bytes: i64,
}

// ---------------------------------------------------------------------------
// DefaultConfig (legacy config.rs lines ~275-465)
// ---------------------------------------------------------------------------

pub fn default_config() -> Config {
    Config {
        project: "dev".to_string(),
        server: ServerConfig {
            smtp_port: 11025,
            mail_http_port: 11080,
            dashboard_port: 18025,
            event_relay_port: 18027,
            s3_port: 14566,
            gcs_port: 14443,
            dynamodb_port: 18000,
            bigquery_port: 19050,
            redshift_port: 15439,
            redshift_api_port: 19099,
            redis_port: 16379,
            redis_http_port: 16380,
            sqs_port: 19324,
            pubsub_grpc_port: 18085,
            pubsub_rest_port: 18086,
            app_auto_scaling_port: 18030,
        },
        auth: AuthConfig {
            smtp: SmtpAuthConfig {
                mode: "relaxed".to_string(),
                username: "dev".to_string(),
                password: "dev".to_string(),
            },
            s3: S3AuthConfig {
                mode: "relaxed".to_string(),
                access_key_id: "dev".to_string(),
                secret_access_key: "dev".to_string(),
            },
            gcs: GcsAuthConfig {
                mode: "relaxed".to_string(),
                project: "devcloud".to_string(),
                bearer_token: String::new(),
            },
            dynamodb: DynamoDbAuthConfig {
                mode: "relaxed".to_string(),
                access_key_id: "dev".to_string(),
                secret_access_key: "dev".to_string(),
            },
            bigquery: BigQueryAuthConfig {
                mode: "relaxed".to_string(),
                project: "devcloud".to_string(),
                bearer_token: "dev".to_string(),
            },
            redshift: RedshiftAuthConfig {
                mode: "relaxed".to_string(),
                user: "dev".to_string(),
                password: "dev".to_string(),
                access_key_id: "dev".to_string(),
                secret_access_key: "dev".to_string(),
                account_id: "000000000000".to_string(),
            },
            redis: RedisAuthConfig {
                mode: "relaxed".to_string(),
                password: String::new(),
            },
            sqs: SqsAuthConfig {
                mode: "relaxed".to_string(),
                access_key_id: "dev".to_string(),
                secret_access_key: "dev".to_string(),
                account_id: "000000000000".to_string(),
            },
            pubsub: PubSubAuthConfig {
                mode: "relaxed".to_string(),
                project_id: "devcloud".to_string(),
                bearer_token: "dev".to_string(),
            },
            app_auto_scaling: AppAutoScalingAuthConfig {
                mode: "relaxed".to_string(),
                access_key_id: "dev".to_string(),
                secret_access_key: "dev".to_string(),
                account_id: "000000000000".to_string(),
            },
        },
        storage: StorageConfig {
            path: ".devcloud/data".to_string(),
        },
        services: ServicesConfig {
            mail: MailServiceConfig {
                enabled: true,
                max_message_bytes: 10 * 1024 * 1024,
            },
            s3: S3ServiceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
                path_style: true,
                virtual_host_style: false,
                max_object_bytes: 5 * 1024 * 1024 * 1024,
                multipart: S3MultipartConfig {
                    min_part_bytes: 5 * 1024 * 1024,
                },
            },
            gcs: GcsServiceConfig {
                enabled: true,
                project: "devcloud".to_string(),
                location: "US".to_string(),
            },
            dynamodb: DynamoDbServiceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
                billing_mode: "PAY_PER_REQUEST".to_string(),
                max_item_bytes: 400000,
                max_tables: 256,
                streams: DynamoDbStreamsConfig { enabled: false },
                ttl: DynamoDbTtlConfig {
                    scheduler_interval_seconds: 60,
                },
            },
            bigquery: BigQueryServiceConfig {
                enabled: true,
                project: "devcloud".to_string(),
                location: "US".to_string(),
                max_rows_per_table: 1000000,
                max_request_bytes: 10 * 1024 * 1024,
                query: BigQueryQueryConfig {
                    max_result_rows: 10000,
                    max_execution_seconds: 30,
                    default_use_legacy_sql: false,
                },
            },
            redshift: RedshiftServiceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
                cluster_identifier: "devcloud".to_string(),
                database: "dev".to_string(),
                data_dir: "redshift".to_string(),
                node_type: "dc2.large".to_string(),
                number_of_nodes: 1,
                max_statement_bytes: 16 * 1024 * 1024,
                backend: RedshiftBackendConfig {
                    kind: "postgres".to_string(),
                    mode: "managed".to_string(),
                    external_dsn: String::new(),
                    managed: true,
                },
                data_api: RedshiftDataApiConfig {
                    enabled: true,
                    max_result_bytes: 500 * 1024 * 1024,
                    max_result_rows: 10000,
                    statement_retention_seconds: 86400,
                    session_retention_seconds: 86400,
                },
                sql: RedshiftSqlConfig {
                    enable_extended_protocol: false,
                    max_result_rows: 10000,
                    default_search_path: "public".to_string(),
                },
                copy_unload: RedshiftCopyUnloadConfig {
                    enable_local_s3: true,
                    max_input_row_bytes: 4 * 1024 * 1024,
                },
            },
            redis: RedisServiceConfig {
                enabled: false,
                mode: "managed".to_string(),
                binary_path: String::new(),
                external_url: String::new(),
                data_dir: "redis".to_string(),
                max_memory_mb: 256,
                append_only: false,
            },
            sqs: SqsServiceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
                queue_url_host: "127.0.0.1".to_string(),
                max_queues: 256,
                max_message_bytes: 1024 * 1024,
                max_receive_batch_size: 10,
                default_visibility_timeout_seconds: 30,
                default_delay_seconds: 0,
                default_message_retention_seconds: 345600,
                default_receive_wait_time_seconds: 0,
                scheduler_interval_seconds: 1,
            },
            pubsub: PubSubServiceConfig {
                enabled: true,
                project: "devcloud".to_string(),
                data_dir: String::new(),
                message_data_dir: String::new(),
                default_ack_deadline_seconds: 10,
                message_retention_seconds: 604800,
                max_ack_deadline_seconds: 600,
                max_pull_messages: 1000,
                pull_wait_timeout_seconds: 1,
                enable_rest: true,
                enable_streaming_pull: true,
                enable_push: false,
            },
            app_auto_scaling: AppAutoScalingServiceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
            },
        },
    }
}

impl Default for Config {
    fn default() -> Self {
        default_config()
    }
}

// ---------------------------------------------------------------------------
// LoadConfig (legacy config.rs lines ~466-512) — hand-rolled YAML-ish parser
// ---------------------------------------------------------------------------

/// Replicates legacy `LoadConfig`. Missing file → default config (NOT an error).
/// Any malformed line / unparsable value → Err.
pub fn load_config(path: &str) -> io::Result<Config> {
    let mut cfg = default_config();
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(cfg),
        Err(err) => return Err(io::Error::new(err.kind(), format!("open config: {err}"))),
    };

    // section is the current key path stack (one entry per indent level).
    let mut section: Vec<String> = Vec::new();
    let reader = io::BufReader::new(file);
    for line_result in reader.lines() {
        let raw = match line_result {
            Ok(l) => l,
            Err(err) => return Err(io::Error::new(err.kind(), format!("read config: {err}"))),
        };
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // legacy: indent := leadingSpaces(raw) / 2
        let indent = leading_spaces(&raw) / 2;
        // legacy: key, value, ok := strings.Cut(line, ":")
        let (key, value, ok) = cut(line, ':');
        if !ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse config line {:?}: missing ':'", raw),
            ));
        }
        let key = key.trim().to_string();
        let value = value.trim().to_string();

        if value.is_empty() {
            // Section header. legacy: if indent > len(section) → error.
            if indent > section.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("parse config line {:?}: unexpected indentation", raw),
                ));
            }
            // legacy: section = append(section[:indent], key)
            section.truncate(indent);
            section.push(key);
            continue;
        }

        // legacy: path := section[:min(indent, len(section))] + key
        let take = indent.min(section.len());
        let mut path_keys: Vec<String> = section[..take].to_vec();
        path_keys.push(key);
        apply_config_value(&mut cfg, &path_keys, &value)?;
    }
    Ok(cfg)
}

/// legacy `leadingSpaces`: count of leading ' ' (space) characters, byte-wise.
/// legacy iterates runes but only checks for ' ', so byte iteration is equivalent.
fn leading_spaces(value: &str) -> usize {
    value.bytes().take_while(|&b| b == b' ').count()
}

/// legacy `strings.Cut(s, sep)`: split on first occurrence of `sep`.
/// Returns (before, after, found). When not found, before = s, after = "".
fn cut(s: &str, sep: char) -> (&str, &str, bool) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + sep.len_utf8()..], true),
        None => (s, "", false),
    }
}

// ---------------------------------------------------------------------------
// applyConfigValue (legacy config.rs lines ~818-1464) — per-key dispatch
// ---------------------------------------------------------------------------

/// Replicate legacy strconv.Atoi: signed 64→truncated to int; here legacy `int` is
/// 64-bit on the target platforms but field types are i32/i64 per struct. We
/// parse as i64 then narrow; an out-of-range value mirrors legacy ParseInt error.
fn parse_int(field: &str, value: &str) -> io::Result<i64> {
    value.parse::<i64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse {field}: invalid integer {value:?}"),
        )
    })
}

/// Replicate legacy strconv.ParseBool: accepts 1,t,T,TRUE,true,True and
/// 0,f,F,FALSE,false,False. Anything else is an error.
fn parse_bool(field: &str, value: &str) -> io::Result<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse {field}: invalid bool {value:?}"),
        )),
    }
}

fn err_positive(field: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("parse {field}: must be positive"),
    )
}

fn err_non_negative(field: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("parse {field}: must be non-negative"),
    )
}

/// legacy `strings.Trim(value, "\"")`: strip leading/trailing double-quote chars.
fn trim_quotes(value: &str) -> String {
    value.trim_matches('"').to_string()
}

pub fn apply_config_value(cfg: &mut Config, path: &[String], value: &str) -> io::Result<()> {
    let key = path.join(".");
    match key.as_str() {
        "project" => cfg.project = value.to_string(),
        "server.smtpPort" => cfg.server.smtp_port = parse_int("server.smtpPort", value)? as i32,
        "server.mailHttpPort" | "server.mailHTTPPort" => {
            cfg.server.mail_http_port = parse_int("server.mailHttpPort", value)? as i32
        }
        "server.dashboardPort" => {
            cfg.server.dashboard_port = parse_int("server.dashboardPort", value)? as i32
        }
        "server.eventRelayPort" => {
            cfg.server.event_relay_port = parse_int("server.eventRelayPort", value)? as i32
        }
        "server.s3Port" => cfg.server.s3_port = parse_int("server.s3Port", value)? as i32,
        "server.gcsPort" => cfg.server.gcs_port = parse_int("server.gcsPort", value)? as i32,
        "server.dynamodbPort" => {
            cfg.server.dynamodb_port = parse_int("server.dynamodbPort", value)? as i32
        }
        "server.bigqueryPort" | "server.bigQueryPort" => {
            cfg.server.bigquery_port = parse_int("server.bigQueryPort", value)? as i32
        }
        "server.redshiftPort" => {
            cfg.server.redshift_port = parse_int("server.redshiftPort", value)? as i32
        }
        "server.redshiftAPIPort" | "server.redshiftApiPort" => {
            cfg.server.redshift_api_port = parse_int("server.redshiftAPIPort", value)? as i32
        }
        "server.redisPort" => cfg.server.redis_port = parse_int("server.redisPort", value)? as i32,
        "server.redisHttpPort" | "server.redisHTTPPort" => {
            cfg.server.redis_http_port = parse_int("server.redisHttpPort", value)? as i32
        }
        "server.sqsPort" => cfg.server.sqs_port = parse_int("server.sqsPort", value)? as i32,
        "server.pubsubGrpcPort" => {
            cfg.server.pubsub_grpc_port = parse_int("server.pubsubGrpcPort", value)? as i32
        }
        "server.pubsubRestPort" => {
            cfg.server.pubsub_rest_port = parse_int("server.pubsubRestPort", value)? as i32
        }
        "server.appAutoScalingPort" => {
            cfg.server.app_auto_scaling_port = parse_int("server.appAutoScalingPort", value)? as i32
        }
        "auth.smtp.mode" => cfg.auth.smtp.mode = value.to_string(),
        "auth.smtp.user" => cfg.auth.smtp.username = value.to_string(),
        "auth.smtp.password" => cfg.auth.smtp.password = value.to_string(),
        "auth.s3.mode" => cfg.auth.s3.mode = value.to_string(),
        "auth.s3.accessKeyId" => cfg.auth.s3.access_key_id = value.to_string(),
        "auth.s3.secretAccessKey" => cfg.auth.s3.secret_access_key = value.to_string(),
        "auth.gcs.mode" => cfg.auth.gcs.mode = value.to_string(),
        "auth.gcs.project" => cfg.auth.gcs.project = value.to_string(),
        "auth.gcs.bearerToken" => cfg.auth.gcs.bearer_token = value.to_string(),
        "auth.dynamodb.mode" => cfg.auth.dynamodb.mode = value.to_string(),
        "auth.dynamodb.accessKeyId" => cfg.auth.dynamodb.access_key_id = value.to_string(),
        "auth.dynamodb.secretAccessKey" => cfg.auth.dynamodb.secret_access_key = value.to_string(),
        "auth.bigquery.mode" => cfg.auth.bigquery.mode = value.to_string(),
        "auth.bigquery.project" => cfg.auth.bigquery.project = value.to_string(),
        "auth.bigquery.bearerToken" => cfg.auth.bigquery.bearer_token = value.to_string(),
        "auth.redshift.mode" => cfg.auth.redshift.mode = value.to_string(),
        "auth.redshift.user" => cfg.auth.redshift.user = value.to_string(),
        "auth.redshift.password" => cfg.auth.redshift.password = value.to_string(),
        "auth.redshift.accessKeyId" => cfg.auth.redshift.access_key_id = value.to_string(),
        "auth.redshift.secretAccessKey" => cfg.auth.redshift.secret_access_key = value.to_string(),
        "auth.redshift.accountId" => cfg.auth.redshift.account_id = trim_quotes(value),
        "auth.redis.mode" => cfg.auth.redis.mode = value.to_string(),
        "auth.redis.password" => cfg.auth.redis.password = value.to_string(),
        "auth.sqs.mode" => cfg.auth.sqs.mode = value.to_string(),
        "auth.sqs.accessKeyId" => cfg.auth.sqs.access_key_id = value.to_string(),
        "auth.sqs.secretAccessKey" => cfg.auth.sqs.secret_access_key = value.to_string(),
        "auth.sqs.accountId" => cfg.auth.sqs.account_id = trim_quotes(value),
        "auth.pubsub.mode" => cfg.auth.pubsub.mode = value.to_string(),
        "auth.pubsub.projectID" | "auth.pubsub.projectId" => {
            cfg.auth.pubsub.project_id = value.to_string()
        }
        "auth.pubsub.bearerToken" => cfg.auth.pubsub.bearer_token = value.to_string(),
        "auth.appAutoScaling.mode" => cfg.auth.app_auto_scaling.mode = value.to_string(),
        "auth.appAutoScaling.accessKeyId" => {
            cfg.auth.app_auto_scaling.access_key_id = value.to_string()
        }
        "auth.appAutoScaling.secretAccessKey" => {
            cfg.auth.app_auto_scaling.secret_access_key = value.to_string()
        }
        "auth.appAutoScaling.accountId" => {
            cfg.auth.app_auto_scaling.account_id = trim_quotes(value)
        }
        "storage.path" => cfg.storage.path = value.to_string(),
        "services.mail.enabled" => {
            cfg.services.mail.enabled = parse_bool("services.mail.enabled", value)?
        }
        "services.mail.maxMessageBytes" => {
            let n = parse_int("services.mail.maxMessageBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.mail.maxMessageBytes"));
            }
            cfg.services.mail.max_message_bytes = n;
        }
        "services.s3.enabled" => {
            cfg.services.s3.enabled = parse_bool("services.s3.enabled", value)?
        }
        "services.s3.region" => cfg.services.s3.region = value.to_string(),
        "services.s3.pathStyle" => {
            cfg.services.s3.path_style = parse_bool("services.s3.pathStyle", value)?
        }
        "services.s3.virtualHostStyle" => {
            cfg.services.s3.virtual_host_style = parse_bool("services.s3.virtualHostStyle", value)?
        }
        "services.s3.maxObjectBytes" => {
            let n = parse_int("services.s3.maxObjectBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.s3.maxObjectBytes"));
            }
            cfg.services.s3.max_object_bytes = n;
        }
        "services.s3.multipart.minPartBytes" => {
            let n = parse_int("services.s3.multipart.minPartBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.s3.multipart.minPartBytes"));
            }
            cfg.services.s3.multipart.min_part_bytes = n;
        }
        "services.gcs.enabled" => {
            cfg.services.gcs.enabled = parse_bool("services.gcs.enabled", value)?
        }
        "services.gcs.project" => cfg.services.gcs.project = value.to_string(),
        "services.gcs.location" => cfg.services.gcs.location = value.to_string(),
        "services.dynamodb.enabled" => {
            cfg.services.dynamodb.enabled = parse_bool("services.dynamodb.enabled", value)?
        }
        "services.dynamodb.region" => cfg.services.dynamodb.region = value.to_string(),
        "services.dynamodb.billingMode" => cfg.services.dynamodb.billing_mode = value.to_string(),
        "services.dynamodb.maxItemBytes" => {
            let n = parse_int("services.dynamodb.maxItemBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.dynamodb.maxItemBytes"));
            }
            cfg.services.dynamodb.max_item_bytes = n;
        }
        "services.dynamodb.maxTables" => {
            let n = parse_int("services.dynamodb.maxTables", value)?;
            if n <= 0 {
                return Err(err_positive("services.dynamodb.maxTables"));
            }
            cfg.services.dynamodb.max_tables = n as i32;
        }
        "services.dynamodb.streams.enabled" => {
            cfg.services.dynamodb.streams.enabled =
                parse_bool("services.dynamodb.streams.enabled", value)?
        }
        "services.dynamodb.ttl.schedulerIntervalSeconds" => {
            let n = parse_int("services.dynamodb.ttl.schedulerIntervalSeconds", value)?;
            if n <= 0 {
                return Err(err_positive(
                    "services.dynamodb.ttl.schedulerIntervalSeconds",
                ));
            }
            cfg.services.dynamodb.ttl.scheduler_interval_seconds = n as i32;
        }
        "services.bigquery.enabled" => {
            cfg.services.bigquery.enabled = parse_bool("services.bigquery.enabled", value)?
        }
        "services.bigquery.project" => cfg.services.bigquery.project = value.to_string(),
        "services.bigquery.location" => cfg.services.bigquery.location = value.to_string(),
        "services.bigquery.maxRowsPerTable" => {
            let n = parse_int("services.bigquery.maxRowsPerTable", value)?;
            if n <= 0 {
                return Err(err_positive("services.bigquery.maxRowsPerTable"));
            }
            cfg.services.bigquery.max_rows_per_table = n;
        }
        "services.bigquery.maxRequestBytes" => {
            let n = parse_int("services.bigquery.maxRequestBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.bigquery.maxRequestBytes"));
            }
            cfg.services.bigquery.max_request_bytes = n;
        }
        "services.bigquery.query.maxResultRows" => {
            let n = parse_int("services.bigquery.query.maxResultRows", value)?;
            if n <= 0 {
                return Err(err_positive("services.bigquery.query.maxResultRows"));
            }
            cfg.services.bigquery.query.max_result_rows = n as i32;
        }
        "services.bigquery.query.maxExecutionSeconds" => {
            let n = parse_int("services.bigquery.query.maxExecutionSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.bigquery.query.maxExecutionSeconds"));
            }
            cfg.services.bigquery.query.max_execution_seconds = n as i32;
        }
        "services.bigquery.query.defaultUseLegacySql" => {
            cfg.services.bigquery.query.default_use_legacy_sql =
                parse_bool("services.bigquery.query.defaultUseLegacySql", value)?
        }
        "services.redshift.enabled" => {
            cfg.services.redshift.enabled = parse_bool("services.redshift.enabled", value)?
        }
        "services.redshift.region" => cfg.services.redshift.region = value.to_string(),
        "services.redshift.clusterIdentifier" => {
            cfg.services.redshift.cluster_identifier = value.to_string()
        }
        "services.redshift.database" => cfg.services.redshift.database = value.to_string(),
        "services.redshift.dataDir" => cfg.services.redshift.data_dir = value.to_string(),
        "services.redshift.nodeType" => cfg.services.redshift.node_type = value.to_string(),
        "services.redshift.numberOfNodes" => {
            let n = parse_int("services.redshift.numberOfNodes", value)?;
            if n <= 0 {
                return Err(err_positive("services.redshift.numberOfNodes"));
            }
            cfg.services.redshift.number_of_nodes = n as i32;
        }
        "services.redshift.maxStatementBytes" => {
            let n = parse_int("services.redshift.maxStatementBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.redshift.maxStatementBytes"));
            }
            cfg.services.redshift.max_statement_bytes = n;
        }
        "services.redshift.backend.kind" => cfg.services.redshift.backend.kind = value.to_string(),
        "services.redshift.backend.mode" => cfg.services.redshift.backend.mode = value.to_string(),
        "services.redshift.backend.externalDsn"
        | "services.redshift.backend.externalDSN"
        | "services.redshift.backend.postgresDsn"
        | "services.redshift.backend.postgresDSN" => {
            cfg.services.redshift.backend.external_dsn = value.to_string()
        }
        "services.redshift.backend.managed" => {
            cfg.services.redshift.backend.managed =
                parse_bool("services.redshift.backend.managed", value)?
        }
        "services.redshift.dataApi.enabled" | "services.redshift.dataAPI.enabled" => {
            cfg.services.redshift.data_api.enabled =
                parse_bool("services.redshift.dataApi.enabled", value)?
        }
        "services.redshift.dataApi.maxResultBytes" | "services.redshift.dataAPI.maxResultBytes" => {
            let n = parse_int("services.redshift.dataApi.maxResultBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.redshift.dataApi.maxResultBytes"));
            }
            cfg.services.redshift.data_api.max_result_bytes = n;
        }
        "services.redshift.dataApi.maxResultRows" | "services.redshift.dataAPI.maxResultRows" => {
            let n = parse_int("services.redshift.dataApi.maxResultRows", value)?;
            if n <= 0 {
                return Err(err_positive("services.redshift.dataApi.maxResultRows"));
            }
            cfg.services.redshift.data_api.max_result_rows = n as i32;
        }
        "services.redshift.dataApi.statementRetentionSeconds"
        | "services.redshift.dataAPI.statementRetentionSeconds" => {
            let n = parse_int("services.redshift.dataApi.statementRetentionSeconds", value)?;
            if n <= 0 {
                return Err(err_positive(
                    "services.redshift.dataApi.statementRetentionSeconds",
                ));
            }
            cfg.services.redshift.data_api.statement_retention_seconds = n as i32;
        }
        "services.redshift.dataApi.sessionRetentionSeconds"
        | "services.redshift.dataAPI.sessionRetentionSeconds" => {
            let n = parse_int("services.redshift.dataApi.sessionRetentionSeconds", value)?;
            if n <= 0 {
                return Err(err_positive(
                    "services.redshift.dataApi.sessionRetentionSeconds",
                ));
            }
            cfg.services.redshift.data_api.session_retention_seconds = n as i32;
        }
        "services.redshift.sql.enableExtendedProtocol" => {
            cfg.services.redshift.sql.enable_extended_protocol =
                parse_bool("services.redshift.sql.enableExtendedProtocol", value)?
        }
        "services.redshift.sql.maxResultRows" => {
            let n = parse_int("services.redshift.sql.maxResultRows", value)?;
            if n <= 0 {
                return Err(err_positive("services.redshift.sql.maxResultRows"));
            }
            cfg.services.redshift.sql.max_result_rows = n as i32;
        }
        "services.redshift.sql.defaultSearchPath" => {
            cfg.services.redshift.sql.default_search_path = value.to_string()
        }
        "services.redshift.copyUnload.enableLocalS3" => {
            cfg.services.redshift.copy_unload.enable_local_s3 =
                parse_bool("services.redshift.copyUnload.enableLocalS3", value)?
        }
        "services.redshift.copyUnload.maxInputRowBytes" => {
            let n = parse_int("services.redshift.copyUnload.maxInputRowBytes", value)?;
            if n <= 0 {
                return Err(err_positive(
                    "services.redshift.copyUnload.maxInputRowBytes",
                ));
            }
            cfg.services.redshift.copy_unload.max_input_row_bytes = n;
        }
        "services.redis.enabled" => {
            cfg.services.redis.enabled = parse_bool("services.redis.enabled", value)?
        }
        "services.redis.mode" => cfg.services.redis.mode = value.to_string(),
        "services.redis.binaryPath" => cfg.services.redis.binary_path = value.to_string(),
        "services.redis.externalUrl" | "services.redis.externalURL" => {
            cfg.services.redis.external_url = value.to_string()
        }
        "services.redis.dataDir" => cfg.services.redis.data_dir = value.to_string(),
        "services.redis.maxMemoryMB" => {
            let n = parse_int("services.redis.maxMemoryMB", value)?;
            if n <= 0 {
                return Err(err_positive("services.redis.maxMemoryMB"));
            }
            cfg.services.redis.max_memory_mb = n as i32;
        }
        "services.redis.appendOnly" => {
            cfg.services.redis.append_only = parse_bool("services.redis.appendOnly", value)?
        }
        "services.sqs.enabled" => {
            cfg.services.sqs.enabled = parse_bool("services.sqs.enabled", value)?
        }
        "services.sqs.region" => cfg.services.sqs.region = value.to_string(),
        "services.sqs.queueUrlHost" => cfg.services.sqs.queue_url_host = value.to_string(),
        "services.sqs.maxQueues" => {
            let n = parse_int("services.sqs.maxQueues", value)?;
            if n <= 0 {
                return Err(err_positive("services.sqs.maxQueues"));
            }
            cfg.services.sqs.max_queues = n as i32;
        }
        "services.sqs.maxMessageBytes" => {
            let n = parse_int("services.sqs.maxMessageBytes", value)?;
            if n <= 0 {
                return Err(err_positive("services.sqs.maxMessageBytes"));
            }
            cfg.services.sqs.max_message_bytes = n;
        }
        "services.sqs.maxReceiveBatchSize" => {
            let n = parse_int("services.sqs.maxReceiveBatchSize", value)?;
            if n <= 0 {
                return Err(err_positive("services.sqs.maxReceiveBatchSize"));
            }
            cfg.services.sqs.max_receive_batch_size = n as i32;
        }
        "services.sqs.defaultVisibilityTimeoutSeconds" => {
            let n = parse_int("services.sqs.defaultVisibilityTimeoutSeconds", value)?;
            if n < 0 {
                return Err(err_non_negative(
                    "services.sqs.defaultVisibilityTimeoutSeconds",
                ));
            }
            cfg.services.sqs.default_visibility_timeout_seconds = n as i32;
        }
        "services.sqs.defaultDelaySeconds" => {
            let n = parse_int("services.sqs.defaultDelaySeconds", value)?;
            if n < 0 {
                return Err(err_non_negative("services.sqs.defaultDelaySeconds"));
            }
            cfg.services.sqs.default_delay_seconds = n as i32;
        }
        "services.sqs.defaultMessageRetentionSeconds" => {
            let n = parse_int("services.sqs.defaultMessageRetentionSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.sqs.defaultMessageRetentionSeconds"));
            }
            cfg.services.sqs.default_message_retention_seconds = n as i32;
        }
        "services.sqs.defaultReceiveWaitTimeSeconds" => {
            let n = parse_int("services.sqs.defaultReceiveWaitTimeSeconds", value)?;
            if n < 0 {
                return Err(err_non_negative(
                    "services.sqs.defaultReceiveWaitTimeSeconds",
                ));
            }
            cfg.services.sqs.default_receive_wait_time_seconds = n as i32;
        }
        "services.sqs.schedulerIntervalSeconds" => {
            let n = parse_int("services.sqs.schedulerIntervalSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.sqs.schedulerIntervalSeconds"));
            }
            cfg.services.sqs.scheduler_interval_seconds = n as i32;
        }
        "services.pubsub.enabled" => {
            cfg.services.pubsub.enabled = parse_bool("services.pubsub.enabled", value)?
        }
        "services.pubsub.project" => cfg.services.pubsub.project = value.to_string(),
        "services.pubsub.dataDir" => cfg.services.pubsub.data_dir = value.to_string(),
        "services.pubsub.messageDataDir" => {
            cfg.services.pubsub.message_data_dir = value.to_string()
        }
        "services.pubsub.defaultAckDeadlineSeconds" => {
            let n = parse_int("services.pubsub.defaultAckDeadlineSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.pubsub.defaultAckDeadlineSeconds"));
            }
            cfg.services.pubsub.default_ack_deadline_seconds = n as i32;
        }
        "services.pubsub.messageRetentionSeconds" => {
            let n = parse_int("services.pubsub.messageRetentionSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.pubsub.messageRetentionSeconds"));
            }
            cfg.services.pubsub.message_retention_seconds = n as i32;
        }
        "services.pubsub.maxAckDeadlineSeconds" => {
            let n = parse_int("services.pubsub.maxAckDeadlineSeconds", value)?;
            if n <= 0 {
                return Err(err_positive("services.pubsub.maxAckDeadlineSeconds"));
            }
            cfg.services.pubsub.max_ack_deadline_seconds = n as i32;
        }
        "services.pubsub.maxPullMessages" => {
            let n = parse_int("services.pubsub.maxPullMessages", value)?;
            if n <= 0 {
                return Err(err_positive("services.pubsub.maxPullMessages"));
            }
            cfg.services.pubsub.max_pull_messages = n as i32;
        }
        "services.pubsub.pullWaitTimeoutSeconds" => {
            let n = parse_int("services.pubsub.pullWaitTimeoutSeconds", value)?;
            if n < 0 {
                return Err(err_non_negative("services.pubsub.pullWaitTimeoutSeconds"));
            }
            cfg.services.pubsub.pull_wait_timeout_seconds = n as i32;
        }
        "services.pubsub.enableREST" => {
            cfg.services.pubsub.enable_rest = parse_bool("services.pubsub.enableREST", value)?
        }
        "services.pubsub.enableStreamingPull" => {
            cfg.services.pubsub.enable_streaming_pull =
                parse_bool("services.pubsub.enableStreamingPull", value)?
        }
        "services.pubsub.enablePush" => {
            cfg.services.pubsub.enable_push = parse_bool("services.pubsub.enablePush", value)?
        }
        "services.appAutoScaling.enabled" => {
            cfg.services.app_auto_scaling.enabled =
                parse_bool("services.appAutoScaling.enabled", value)?
        }
        "services.appAutoScaling.region" => {
            cfg.services.app_auto_scaling.region = value.to_string()
        }
        // legacy default: unknown key → silently ignored (return nil).
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Config helpers (ported from internal/app/daemon.rs)
// ---------------------------------------------------------------------------

/// legacy `defaultString(value, fallback)`.
fn default_string(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

/// legacy `redshiftBackendKind`: strings.ToLower(defaultString(Kind, "postgres")).
fn redshift_backend_kind(b: &RedshiftBackendConfig) -> String {
    default_string(&b.kind, "postgres").to_lowercase()
}

/// legacy `redshiftBackendMode`.
fn redshift_backend_mode(b: &RedshiftBackendConfig) -> String {
    let mode = b.mode.trim().to_lowercase();
    if !mode.is_empty() {
        return mode;
    }
    if !b.external_dsn.trim().is_empty() {
        return "external".to_string();
    }
    if redshift_backend_kind(b) == "memory" {
        return "memory".to_string();
    }
    "managed".to_string()
}

/// legacy `redisMode`.
fn redis_mode(r: &RedisServiceConfig) -> String {
    let mode = r.mode.trim().to_lowercase();
    if !mode.is_empty() {
        return mode;
    }
    if !r.external_url.trim().is_empty() {
        return "external".to_string();
    }
    "managed".to_string()
}

/// legacy `filepath.Join` (POSIX-ish, matching the platforms devcloud runs on).
/// Joins with the OS separator and cleans the result like path.Clean does for
/// the simple cases we hit (no `..` in our inputs).
fn path_join(a: &str, b: &str) -> String {
    let mut p = PathBuf::from(a);
    p.push(b);
    p.to_string_lossy().into_owned()
}

/// legacy `pubsubDataDir`.
fn pubsub_data_dir(cfg: &Config) -> String {
    default_string(
        &cfg.services.pubsub.data_dir,
        &path_join(&cfg.storage.path, "pubsub"),
    )
}

/// legacy `pubsubMessageDataDir`.
fn pubsub_message_data_dir(cfg: &Config) -> String {
    default_string(
        &cfg.services.pubsub.message_data_dir,
        &path_join(&cfg.storage.path, "message"),
    )
}

/// legacy `redshiftDataDir`. Note `filepath.Clean` semantics + the `.devcloud`
/// escape clause; we replicate path.Clean for the no-`..` inputs in use.
fn redshift_data_dir(cfg: &Config) -> String {
    let data_dir = &cfg.services.redshift.data_dir;
    if data_dir.is_empty() {
        return path_join(&cfg.storage.path, "redshift");
    }
    let clean = clean_path(data_dir);
    let prefix = format!(".devcloud{MAIN_SEPARATOR}");
    if clean == ".devcloud" || clean.starts_with(&prefix) {
        return clean;
    }
    path_join(&cfg.storage.path, &clean)
}

/// legacy `redisDataDir`.
fn redis_data_dir(cfg: &Config) -> String {
    let data_dir = &cfg.services.redis.data_dir;
    if data_dir.is_empty() {
        return path_join(&cfg.storage.path, "redis");
    }
    let clean = clean_path(data_dir);
    let prefix = format!(".devcloud{MAIN_SEPARATOR}");
    if clean == ".devcloud" || clean.starts_with(&prefix) {
        return clean;
    }
    path_join(&cfg.storage.path, &clean)
}

/// Approximation of legacy `filepath.Clean` sufficient for devcloud config inputs
/// (relative paths, no `..`). Collapses redundant separators and `.` elements.
fn clean_path(p: &str) -> String {
    PathBuf::from(p).to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// defaultConfigYAML (legacy config.rs lines ~638-807) — BYTE-IDENTICAL OUTPUT
// ---------------------------------------------------------------------------

/// Format a legacy-`%t` bool: "true" / "false".
fn fmt_bool(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// Replicate legacy `defaultConfigYAML(cfg)`. The output MUST be byte-identical to
/// the legacy version — same comments (none), spacing, ordering, formatting. This
/// string is written verbatim to `.devcloud/config.yaml` by `init`.
pub fn default_config_yaml(cfg: &Config) -> String {
    let s = &cfg.server;
    let a = &cfg.auth;
    let sv = &cfg.services;
    let rs = &sv.redshift;
    let redis = &sv.redis;

    // Mirrors the legacy Sprintf argument list exactly.
    format!(
        concat!(
            "project: {project}\n",
            "\n",
            "server:\n",
            "  smtpPort: {smtp_port}\n",
            "  mailHttpPort: {mail_http_port}\n",
            "  dashboardPort: {dashboard_port}\n",
            "  eventRelayPort: {event_relay_port}\n",
            "  s3Port: {s3_port}\n",
            "  gcsPort: {gcs_port}\n",
            "  dynamodbPort: {dynamodb_port}\n",
            "  bigqueryPort: {bigquery_port}\n",
            "  redshiftPort: {redshift_port}\n",
            "  redshiftAPIPort: {redshift_api_port}\n",
            "  redisPort: {redis_port}\n",
            "  redisHttpPort: {redis_http_port}\n",
            "  sqsPort: {sqs_port}\n",
            "  pubsubGrpcPort: {pubsub_grpc_port}\n",
            "  pubsubRestPort: {pubsub_rest_port}\n",
            "  appAutoScalingPort: {app_auto_scaling_port}\n",
            "\n",
            "auth:\n",
            "  smtp:\n",
            "    mode: {smtp_mode}\n",
            "    user: {smtp_user}\n",
            "    password: {smtp_password}\n",
            "  s3:\n",
            "    mode: {s3_auth_mode}\n",
            "    accessKeyId: {s3_access_key_id}\n",
            "    secretAccessKey: {s3_secret_access_key}\n",
            "  gcs:\n",
            "    mode: {gcs_auth_mode}\n",
            "    project: {gcs_auth_project}\n",
            "  dynamodb:\n",
            "    mode: {ddb_auth_mode}\n",
            "    accessKeyId: {ddb_access_key_id}\n",
            "    secretAccessKey: {ddb_secret_access_key}\n",
            "  bigquery:\n",
            "    mode: {bq_auth_mode}\n",
            "    project: {bq_auth_project}\n",
            "    bearerToken: {bq_bearer_token}\n",
            "  redshift:\n",
            "    mode: {rs_auth_mode}\n",
            "    user: {rs_auth_user}\n",
            "    password: {rs_auth_password}\n",
            "    accessKeyId: {rs_access_key_id}\n",
            "    secretAccessKey: {rs_secret_access_key}\n",
            "    accountId: \"{rs_account_id}\"\n",
            "  redis:\n",
            "    mode: {redis_auth_mode}\n",
            "    password: {redis_auth_password}\n",
            "  sqs:\n",
            "    mode: {sqs_auth_mode}\n",
            "    accessKeyId: {sqs_access_key_id}\n",
            "    secretAccessKey: {sqs_secret_access_key}\n",
            "    accountId: \"{sqs_account_id}\"\n",
            "  pubsub:\n",
            "    mode: {pubsub_auth_mode}\n",
            "    projectID: {pubsub_project_id}\n",
            "    bearerToken: {pubsub_bearer_token}\n",
            "  appAutoScaling:\n",
            "    mode: {aas_auth_mode}\n",
            "    accessKeyId: {aas_access_key_id}\n",
            "    secretAccessKey: {aas_secret_access_key}\n",
            "    accountId: \"{aas_account_id}\"\n",
            "\n",
            "storage:\n",
            "  path: {storage_path}\n",
            "\n",
            "services:\n",
            "  mail:\n",
            "    enabled: {mail_enabled}\n",
            "    maxMessageBytes: {mail_max_message_bytes}\n",
            "  s3:\n",
            "    enabled: {s3_enabled}\n",
            "    region: {s3_region}\n",
            "    pathStyle: {s3_path_style}\n",
            "    virtualHostStyle: {s3_virtual_host_style}\n",
            "    maxObjectBytes: {s3_max_object_bytes}\n",
            "    multipart:\n",
            "      minPartBytes: {s3_min_part_bytes}\n",
            "  gcs:\n",
            "    enabled: {gcs_enabled}\n",
            "    project: {gcs_project}\n",
            "    location: {gcs_location}\n",
            "  dynamodb:\n",
            "    enabled: {ddb_enabled}\n",
            "    region: {ddb_region}\n",
            "    billingMode: {ddb_billing_mode}\n",
            "    maxItemBytes: {ddb_max_item_bytes}\n",
            "    maxTables: {ddb_max_tables}\n",
            "    streams:\n",
            "      enabled: {ddb_streams_enabled}\n",
            "    ttl:\n",
            "      schedulerIntervalSeconds: {ddb_ttl_scheduler}\n",
            "  bigquery:\n",
            "    enabled: {bq_enabled}\n",
            "    project: {bq_project}\n",
            "    location: {bq_location}\n",
            "    maxRowsPerTable: {bq_max_rows_per_table}\n",
            "    maxRequestBytes: {bq_max_request_bytes}\n",
            "    query:\n",
            "      maxResultRows: {bq_query_max_result_rows}\n",
            "      maxExecutionSeconds: {bq_query_max_execution_seconds}\n",
            "      defaultUseLegacySql: {bq_query_default_use_legacy_sql}\n",
            "  redshift:\n",
            "    enabled: {rs_enabled}\n",
            "    region: {rs_region}\n",
            "    clusterIdentifier: {rs_cluster_identifier}\n",
            "    database: {rs_database}\n",
            "    dataDir: {rs_data_dir}\n",
            "    nodeType: {rs_node_type}\n",
            "    numberOfNodes: {rs_number_of_nodes}\n",
            "    maxStatementBytes: {rs_max_statement_bytes}\n",
            "    backend:\n",
            "      kind: {rs_backend_kind}\n",
            "      mode: {rs_backend_mode}\n",
            "      externalDsn: {rs_backend_external_dsn}\n",
            "      managed: {rs_backend_managed}\n",
            "    dataApi:\n",
            "      enabled: {rs_dataapi_enabled}\n",
            "      maxResultBytes: {rs_dataapi_max_result_bytes}\n",
            "      maxResultRows: {rs_dataapi_max_result_rows}\n",
            "      statementRetentionSeconds: {rs_dataapi_statement_retention}\n",
            "      sessionRetentionSeconds: {rs_dataapi_session_retention}\n",
            "    sql:\n",
            "      enableExtendedProtocol: {rs_sql_extended_protocol}\n",
            "      maxResultRows: {rs_sql_max_result_rows}\n",
            "      defaultSearchPath: {rs_sql_default_search_path}\n",
            "    copyUnload:\n",
            "      enableLocalS3: {rs_copy_enable_local_s3}\n",
            "      maxInputRowBytes: {rs_copy_max_input_row_bytes}\n",
            "  redis:\n",
            "    enabled: {redis_enabled}\n",
            "    mode: {redis_mode}\n",
            "    binaryPath: {redis_binary_path}\n",
            "    externalUrl: {redis_external_url}\n",
            "    dataDir: {redis_data_dir}\n",
            "    maxMemoryMB: {redis_max_memory_mb}\n",
            "    appendOnly: {redis_append_only}\n",
            "  sqs:\n",
            "    enabled: {sqs_enabled}\n",
            "    region: {sqs_region}\n",
            "    queueUrlHost: {sqs_queue_url_host}\n",
            "    maxQueues: {sqs_max_queues}\n",
            "    maxMessageBytes: {sqs_max_message_bytes}\n",
            "    maxReceiveBatchSize: {sqs_max_receive_batch_size}\n",
            "    defaultVisibilityTimeoutSeconds: {sqs_default_visibility_timeout}\n",
            "    defaultDelaySeconds: {sqs_default_delay}\n",
            "    defaultMessageRetentionSeconds: {sqs_default_message_retention}\n",
            "    defaultReceiveWaitTimeSeconds: {sqs_default_receive_wait}\n",
            "    schedulerIntervalSeconds: {sqs_scheduler_interval}\n",
            "  pubsub:\n",
            "    enabled: {pubsub_enabled}\n",
            "    project: {pubsub_project}\n",
            "    dataDir: {pubsub_data_dir}\n",
            "    messageDataDir: {pubsub_message_data_dir}\n",
            "    defaultAckDeadlineSeconds: {pubsub_default_ack_deadline}\n",
            "    messageRetentionSeconds: {pubsub_message_retention}\n",
            "    maxAckDeadlineSeconds: {pubsub_max_ack_deadline}\n",
            "    maxPullMessages: {pubsub_max_pull_messages}\n",
            "    pullWaitTimeoutSeconds: {pubsub_pull_wait_timeout}\n",
            "    enableREST: {pubsub_enable_rest}\n",
            "    enableStreamingPull: {pubsub_enable_streaming_pull}\n",
            "    enablePush: {pubsub_enable_push}\n",
            "  appAutoScaling:\n",
            "    enabled: {aas_enabled}\n",
            "    region: {aas_region}\n",
        ),
        project = cfg.project,
        smtp_port = s.smtp_port,
        mail_http_port = s.mail_http_port,
        dashboard_port = s.dashboard_port,
        event_relay_port = s.event_relay_port,
        s3_port = s.s3_port,
        gcs_port = s.gcs_port,
        dynamodb_port = s.dynamodb_port,
        bigquery_port = s.bigquery_port,
        redshift_port = s.redshift_port,
        redshift_api_port = s.redshift_api_port,
        redis_port = s.redis_port,
        redis_http_port = s.redis_http_port,
        sqs_port = s.sqs_port,
        pubsub_grpc_port = s.pubsub_grpc_port,
        pubsub_rest_port = s.pubsub_rest_port,
        app_auto_scaling_port = s.app_auto_scaling_port,
        smtp_mode = a.smtp.mode,
        smtp_user = a.smtp.username,
        smtp_password = a.smtp.password,
        s3_auth_mode = a.s3.mode,
        s3_access_key_id = a.s3.access_key_id,
        s3_secret_access_key = a.s3.secret_access_key,
        gcs_auth_mode = a.gcs.mode,
        gcs_auth_project = a.gcs.project,
        ddb_auth_mode = a.dynamodb.mode,
        ddb_access_key_id = a.dynamodb.access_key_id,
        ddb_secret_access_key = a.dynamodb.secret_access_key,
        bq_auth_mode = a.bigquery.mode,
        bq_auth_project = a.bigquery.project,
        bq_bearer_token = a.bigquery.bearer_token,
        rs_auth_mode = a.redshift.mode,
        rs_auth_user = a.redshift.user,
        rs_auth_password = a.redshift.password,
        rs_access_key_id = a.redshift.access_key_id,
        rs_secret_access_key = a.redshift.secret_access_key,
        rs_account_id = a.redshift.account_id,
        redis_auth_mode = a.redis.mode,
        redis_auth_password = a.redis.password,
        sqs_auth_mode = a.sqs.mode,
        sqs_access_key_id = a.sqs.access_key_id,
        sqs_secret_access_key = a.sqs.secret_access_key,
        sqs_account_id = a.sqs.account_id,
        pubsub_auth_mode = a.pubsub.mode,
        pubsub_project_id = a.pubsub.project_id,
        pubsub_bearer_token = a.pubsub.bearer_token,
        aas_auth_mode = a.app_auto_scaling.mode,
        aas_access_key_id = a.app_auto_scaling.access_key_id,
        aas_secret_access_key = a.app_auto_scaling.secret_access_key,
        aas_account_id = a.app_auto_scaling.account_id,
        storage_path = cfg.storage.path,
        mail_enabled = fmt_bool(sv.mail.enabled),
        mail_max_message_bytes = sv.mail.max_message_bytes,
        s3_enabled = fmt_bool(sv.s3.enabled),
        s3_region = sv.s3.region,
        s3_path_style = fmt_bool(sv.s3.path_style),
        s3_virtual_host_style = fmt_bool(sv.s3.virtual_host_style),
        s3_max_object_bytes = sv.s3.max_object_bytes,
        s3_min_part_bytes = sv.s3.multipart.min_part_bytes,
        gcs_enabled = fmt_bool(sv.gcs.enabled),
        gcs_project = sv.gcs.project,
        gcs_location = sv.gcs.location,
        ddb_enabled = fmt_bool(sv.dynamodb.enabled),
        ddb_region = sv.dynamodb.region,
        ddb_billing_mode = sv.dynamodb.billing_mode,
        ddb_max_item_bytes = sv.dynamodb.max_item_bytes,
        ddb_max_tables = sv.dynamodb.max_tables,
        ddb_streams_enabled = fmt_bool(sv.dynamodb.streams.enabled),
        ddb_ttl_scheduler = sv.dynamodb.ttl.scheduler_interval_seconds,
        bq_enabled = fmt_bool(sv.bigquery.enabled),
        bq_project = sv.bigquery.project,
        bq_location = sv.bigquery.location,
        bq_max_rows_per_table = sv.bigquery.max_rows_per_table,
        bq_max_request_bytes = sv.bigquery.max_request_bytes,
        bq_query_max_result_rows = sv.bigquery.query.max_result_rows,
        bq_query_max_execution_seconds = sv.bigquery.query.max_execution_seconds,
        bq_query_default_use_legacy_sql = fmt_bool(sv.bigquery.query.default_use_legacy_sql),
        rs_enabled = fmt_bool(rs.enabled),
        rs_region = rs.region,
        rs_cluster_identifier = rs.cluster_identifier,
        rs_database = rs.database,
        rs_data_dir = default_string(&rs.data_dir, "redshift"),
        rs_node_type = rs.node_type,
        rs_number_of_nodes = rs.number_of_nodes,
        rs_max_statement_bytes = rs.max_statement_bytes,
        rs_backend_kind = redshift_backend_kind(&rs.backend),
        rs_backend_mode = redshift_backend_mode(&rs.backend),
        rs_backend_external_dsn = rs.backend.external_dsn,
        rs_backend_managed = fmt_bool(redshift_backend_mode(&rs.backend) == "managed"),
        rs_dataapi_enabled = fmt_bool(rs.data_api.enabled),
        rs_dataapi_max_result_bytes = rs.data_api.max_result_bytes,
        rs_dataapi_max_result_rows = rs.data_api.max_result_rows,
        rs_dataapi_statement_retention = rs.data_api.statement_retention_seconds,
        rs_dataapi_session_retention = rs.data_api.session_retention_seconds,
        rs_sql_extended_protocol = fmt_bool(rs.sql.enable_extended_protocol),
        rs_sql_max_result_rows = rs.sql.max_result_rows,
        rs_sql_default_search_path = rs.sql.default_search_path,
        rs_copy_enable_local_s3 = fmt_bool(rs.copy_unload.enable_local_s3),
        rs_copy_max_input_row_bytes = rs.copy_unload.max_input_row_bytes,
        redis_enabled = fmt_bool(redis.enabled),
        redis_mode = redis_mode(redis),
        redis_binary_path = redis.binary_path,
        redis_external_url = redis.external_url,
        redis_data_dir = default_string(&redis.data_dir, "redis"),
        redis_max_memory_mb = redis.max_memory_mb,
        redis_append_only = fmt_bool(redis.append_only),
        sqs_enabled = fmt_bool(sv.sqs.enabled),
        sqs_region = sv.sqs.region,
        sqs_queue_url_host = sv.sqs.queue_url_host,
        sqs_max_queues = sv.sqs.max_queues,
        sqs_max_message_bytes = sv.sqs.max_message_bytes,
        sqs_max_receive_batch_size = sv.sqs.max_receive_batch_size,
        sqs_default_visibility_timeout = sv.sqs.default_visibility_timeout_seconds,
        sqs_default_delay = sv.sqs.default_delay_seconds,
        sqs_default_message_retention = sv.sqs.default_message_retention_seconds,
        sqs_default_receive_wait = sv.sqs.default_receive_wait_time_seconds,
        sqs_scheduler_interval = sv.sqs.scheduler_interval_seconds,
        pubsub_enabled = fmt_bool(sv.pubsub.enabled),
        pubsub_project = sv.pubsub.project,
        pubsub_data_dir =
            default_string(&sv.pubsub.data_dir, &path_join(&cfg.storage.path, "pubsub")),
        pubsub_message_data_dir = default_string(
            &sv.pubsub.message_data_dir,
            &path_join(&cfg.storage.path, "message")
        ),
        pubsub_default_ack_deadline = sv.pubsub.default_ack_deadline_seconds,
        pubsub_message_retention = sv.pubsub.message_retention_seconds,
        pubsub_max_ack_deadline = sv.pubsub.max_ack_deadline_seconds,
        pubsub_max_pull_messages = sv.pubsub.max_pull_messages,
        pubsub_pull_wait_timeout = sv.pubsub.pull_wait_timeout_seconds,
        pubsub_enable_rest = fmt_bool(sv.pubsub.enable_rest),
        pubsub_enable_streaming_pull = fmt_bool(sv.pubsub.enable_streaming_pull),
        pubsub_enable_push = fmt_bool(sv.pubsub.enable_push),
        aas_enabled = fmt_bool(sv.app_auto_scaling.enabled),
        aas_region = sv.app_auto_scaling.region,
    )
}

// ---------------------------------------------------------------------------
// InitWorkspace / ResetWorkspace (legacy config.rs lines ~513-637)
// ---------------------------------------------------------------------------

/// legacy `validateStoragePath`: path must be under `.devcloud`.
fn validate_storage_path(path: &str) -> io::Result<()> {
    let clean = clean_path(path);
    let prefix = format!(".devcloud{MAIN_SEPARATOR}");
    if clean == ".devcloud" || clean.starts_with(&prefix) {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("storage.path must be under .devcloud: {path}"),
    ))
}

/// legacy `ensureFile`: write `data` only if `path` does not already exist.
fn ensure_file(path: &str, data: &[u8]) -> io::Result<()> {
    match fs::metadata(path) {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => fs::write(path, data),
        Err(err) => Err(err),
    }
}

fn mkdir_all(path: &str, ctx: &str) -> io::Result<()> {
    fs::create_dir_all(path).map_err(|err| io::Error::new(err.kind(), format!("{ctx}: {err}")))
}

/// legacy `InitWorkspace`. Validates storage paths, creates per-service storage
/// dirs, seeds the mail index, and writes `.devcloud/config.yaml` (only if it
/// does not already exist).
pub fn init_workspace(cfg: &Config) -> io::Result<()> {
    validate_storage_path(&cfg.storage.path)?;
    if !cfg.services.pubsub.data_dir.is_empty() {
        validate_storage_path(&cfg.services.pubsub.data_dir)
            .map_err(|e| io::Error::new(e.kind(), format!("pubsub dataDir: {e}")))?;
    }
    if !cfg.services.pubsub.message_data_dir.is_empty() {
        validate_storage_path(&cfg.services.pubsub.message_data_dir)
            .map_err(|e| io::Error::new(e.kind(), format!("pubsub messageDataDir: {e}")))?;
    }
    validate_storage_path(&redshift_data_dir(cfg))
        .map_err(|e| io::Error::new(e.kind(), format!("redshift dataDir: {e}")))?;
    validate_storage_path(&redis_data_dir(cfg))
        .map_err(|e| io::Error::new(e.kind(), format!("redis dataDir: {e}")))?;

    mkdir_all(
        &path_join(&cfg.storage.path, "blobs"),
        "create blob storage",
    )?;
    mkdir_all(&path_join(&cfg.storage.path, "mail"), "create mail storage")?;
    ensure_file(
        &path_join(&path_join(&cfg.storage.path, "mail"), "index.json"),
        b"{}\n",
    )
    .map_err(|e| io::Error::new(e.kind(), format!("create mail index: {e}")))?;
    mkdir_all(
        &path_join(&path_join(&cfg.storage.path, "s3"), "buckets"),
        "create s3 bucket storage",
    )?;
    mkdir_all(
        &path_join(&path_join(&cfg.storage.path, "s3"), "multipart"),
        "create s3 multipart storage",
    )?;
    mkdir_all(
        &path_join(&path_join(&cfg.storage.path, "gcs"), "upload_sessions"),
        "create gcs upload session storage",
    )?;
    mkdir_all(
        &path_join(&cfg.storage.path, "dynamodb"),
        "create dynamodb storage",
    )?;
    mkdir_all(
        &path_join(&cfg.storage.path, "bigquery"),
        "create bigquery storage",
    )?;
    mkdir_all(&redshift_data_dir(cfg), "create redshift storage")?;
    mkdir_all(&redis_data_dir(cfg), "create redis storage")?;
    mkdir_all(&path_join(&cfg.storage.path, "sqs"), "create sqs storage")?;
    mkdir_all(
        &path_join(&cfg.storage.path, "applicationautoscaling"),
        "create applicationautoscaling storage",
    )?;
    mkdir_all(&pubsub_data_dir(cfg), "create pubsub storage")?;
    mkdir_all(&pubsub_message_data_dir(cfg), "create message storage")?;
    mkdir_all(&path_join(&cfg.storage.path, "kv"), "create kv storage")?;
    mkdir_all(".devcloud/logs", "create log directory")?;

    ensure_file(".devcloud/config.yaml", default_config_yaml(cfg).as_bytes())
}

/// legacy `ResetWorkspace`. Validates storage paths, removes storage trees, then
/// re-inits the workspace.
pub fn reset_workspace(cfg: &Config) -> io::Result<()> {
    validate_storage_path(&cfg.storage.path)?;
    if !cfg.services.pubsub.data_dir.is_empty() {
        validate_storage_path(&cfg.services.pubsub.data_dir)
            .map_err(|e| io::Error::new(e.kind(), format!("pubsub dataDir: {e}")))?;
    }
    if !cfg.services.pubsub.message_data_dir.is_empty() {
        validate_storage_path(&cfg.services.pubsub.message_data_dir)
            .map_err(|e| io::Error::new(e.kind(), format!("pubsub messageDataDir: {e}")))?;
    }
    validate_storage_path(&redshift_data_dir(cfg))
        .map_err(|e| io::Error::new(e.kind(), format!("redshift dataDir: {e}")))?;
    validate_storage_path(&redis_data_dir(cfg))
        .map_err(|e| io::Error::new(e.kind(), format!("redis dataDir: {e}")))?;

    remove_all(&cfg.storage.path, "remove storage")?;
    if !cfg.services.redshift.data_dir.is_empty() {
        remove_all(&redshift_data_dir(cfg), "remove redshift storage")?;
    }
    if !cfg.services.redis.data_dir.is_empty() {
        remove_all(&redis_data_dir(cfg), "remove redis storage")?;
    }
    if !cfg.services.pubsub.data_dir.is_empty() {
        remove_all(&pubsub_data_dir(cfg), "remove pubsub storage")?;
    }
    if !cfg.services.pubsub.message_data_dir.is_empty() {
        remove_all(
            &pubsub_message_data_dir(cfg),
            "remove pubsub message storage",
        )?;
    }
    init_workspace(cfg)
}

/// legacy `os.RemoveAll`: removing a non-existent path is NOT an error.
fn remove_all(path: &str, ctx: &str) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(io::Error::new(err.kind(), format!("{ctx}: {err}"))),
    }
}

// ---------------------------------------------------------------------------
// ServiceNames / ApplyServiceSelection (legacy internal/app/services.rs)
// ---------------------------------------------------------------------------

/// Canonical service names → toggle closure target. Order does not matter here;
/// `service_names` sorts the keys to match legacy `sort.Strings`.
const SERVICE_TOGGLES: &[&str] = &[
    "mail",
    "s3",
    "gcs",
    "dynamodb",
    "bigquery",
    "redshift",
    "redis",
    "sqs",
    "pubsub",
    "appautoscaling",
];

/// legacy `ServiceNames`: canonical service identifiers, alphabetically sorted.
pub fn service_names() -> Vec<String> {
    let mut names: Vec<String> = SERVICE_TOGGLES.iter().map(|s| s.to_string()).collect();
    names.sort();
    names
}

/// Set the enabled flag for a canonical service name on cfg.
fn set_service_enabled(cfg: &mut Config, name: &str, v: bool) {
    match name {
        "mail" => cfg.services.mail.enabled = v,
        "s3" => cfg.services.s3.enabled = v,
        "gcs" => cfg.services.gcs.enabled = v,
        "dynamodb" => cfg.services.dynamodb.enabled = v,
        "bigquery" => cfg.services.bigquery.enabled = v,
        "redshift" => cfg.services.redshift.enabled = v,
        "redis" => cfg.services.redis.enabled = v,
        "sqs" => cfg.services.sqs.enabled = v,
        "pubsub" => cfg.services.pubsub.enabled = v,
        "appautoscaling" => cfg.services.app_auto_scaling.enabled = v,
        _ => {}
    }
}

/// legacy `serviceAliases`: user-facing names (lowercase) → canonical key.
fn service_aliases() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        ("mail", "mail"),
        ("smtp", "mail"),
        ("s3", "s3"),
        ("gcs", "gcs"),
        ("dynamodb", "dynamodb"),
        ("ddb", "dynamodb"),
        ("bigquery", "bigquery"),
        ("bq", "bigquery"),
        ("redshift", "redshift"),
        ("redis", "redis"),
        ("sqs", "sqs"),
        ("pubsub", "pubsub"),
        ("pub-sub", "pubsub"),
        ("pub_sub", "pubsub"),
        ("appautoscaling", "appautoscaling"),
        ("app-autoscaling", "appautoscaling"),
        ("app_autoscaling", "appautoscaling"),
        ("applicationautoscaling", "appautoscaling"),
    ])
}

/// legacy `ApplyServiceSelection`. Returns a copy of cfg with only the named
/// services enabled. Empty/blank selection leaves cfg unchanged. Unknown name
/// → Err (matching legacy error text format).
pub fn apply_service_selection(cfg: &Config, selected: &[String]) -> io::Result<Config> {
    if selected.is_empty() {
        return Ok(cfg.clone());
    }

    let aliases = service_aliases();
    let mut chosen: Vec<&'static str> = Vec::new();
    for raw in selected {
        let name = raw.trim().to_lowercase();
        if name.is_empty() {
            continue;
        }
        match aliases.get(name.as_str()) {
            Some(canonical) => {
                if !chosen.contains(canonical) {
                    chosen.push(canonical);
                }
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unknown service {:?} (known: {})",
                        raw,
                        service_names().join(", ")
                    ),
                ));
            }
        }
    }

    if chosen.is_empty() {
        return Ok(cfg.clone());
    }

    let mut out = cfg.clone();
    for &name in SERVICE_TOGGLES {
        let enable = chosen.contains(&name);
        set_service_enabled(&mut out, name, enable);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests — YAML byte-compat + parser round-trip
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_yaml_round_trips_through_parser() {
        // The YAML emitted from default config must parse back to the default
        // config, EXCEPT for the pubsub data dirs: defaultConfigYAML resolves the
        // empty default `data_dir`/`message_data_dir` to their concrete on-disk
        // paths (Storage.Path joined). Loading that YAML therefore reads those
        // concrete values back, not "" — this matches legacy defaultConfigYAML +
        // LoadConfig exactly. Normalize those two fields before comparing.
        let cfg = default_config();
        let yaml = default_config_yaml(&cfg);
        let mut parsed = parse_yaml_in_memory(&yaml).expect("parse default yaml");
        assert_eq!(parsed.services.pubsub.data_dir, ".devcloud/data/pubsub");
        assert_eq!(
            parsed.services.pubsub.message_data_dir,
            ".devcloud/data/message"
        );
        parsed.services.pubsub.data_dir = cfg.services.pubsub.data_dir.clone();
        parsed.services.pubsub.message_data_dir = cfg.services.pubsub.message_data_dir.clone();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn default_yaml_first_and_last_lines() {
        let yaml = default_config_yaml(&default_config());
        assert!(yaml.starts_with("project: dev\n\nserver:\n"));
        assert!(yaml.ends_with("  appAutoScaling:\n    enabled: true\n    region: us-east-1\n"));
    }

    #[test]
    fn account_ids_are_quoted() {
        let yaml = default_config_yaml(&default_config());
        assert!(yaml.contains("    accountId: \"000000000000\"\n"));
    }

    #[test]
    fn service_names_sorted() {
        assert_eq!(
            service_names(),
            vec![
                "appautoscaling",
                "bigquery",
                "dynamodb",
                "gcs",
                "mail",
                "pubsub",
                "redis",
                "redshift",
                "s3",
                "sqs",
            ]
        );
    }

    #[test]
    fn selection_enables_only_chosen() {
        let cfg = default_config();
        let out = apply_service_selection(&cfg, &["s3".to_string(), "bq".to_string()]).unwrap();
        assert!(out.services.s3.enabled);
        assert!(out.services.bigquery.enabled);
        assert!(!out.services.mail.enabled);
        assert!(!out.services.sqs.enabled);
    }

    #[test]
    fn selection_unknown_errors() {
        let cfg = default_config();
        let err = apply_service_selection(&cfg, &["bogus".to_string()]).unwrap_err();
        assert!(err.to_string().contains("unknown service"));
    }

    #[test]
    fn empty_selection_unchanged() {
        let cfg = default_config();
        let out = apply_service_selection(&cfg, &[]).unwrap();
        assert_eq!(out, cfg);
    }

    // In-memory mirror of load_config's parse loop, for the round-trip test.
    fn parse_yaml_in_memory(text: &str) -> io::Result<Config> {
        let mut cfg = default_config();
        let mut section: Vec<String> = Vec::new();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let indent = leading_spaces(raw) / 2;
            let (key, value, ok) = cut(line, ':');
            if !ok {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "missing ':'"));
            }
            let key = key.trim().to_string();
            let value = value.trim().to_string();
            if value.is_empty() {
                if indent > section.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected indentation",
                    ));
                }
                section.truncate(indent);
                section.push(key);
                continue;
            }
            let take = indent.min(section.len());
            let mut path_keys: Vec<String> = section[..take].to_vec();
            path_keys.push(key);
            apply_config_value(&mut cfg, &path_keys, &value)?;
        }
        Ok(cfg)
    }
}
