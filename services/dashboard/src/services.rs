//! The `/api/services` registry, replicating legacy `internal/dashboard/services.rs`.
//!
//! The legacy dashboard derives each service's status from an in-process `!= nil`
//! pointer check. Out-of-process, the Rust dashboard derives it from whether the
//! service's network base address is **configured** (non-empty) — the daemon
//! seam sets a base only for enabled services, so "configured" is the
//! out-of-process equivalent of "running". The JSON shape (field names + order +
//! the service list order) is identical to the legacy response.

use serde::Serialize;

use crate::config::Config;
use crate::http::{Request, Response};

/// One service entry. Field names/`omitempty` match legacy `DashboardService`.
#[derive(Serialize)]
struct DashboardService {
    id: &'static str,
    name: &'static str,
    path: &'static str,
    status: &'static str,
    #[serde(skip_serializing_if = "str::is_empty")]
    endpoint: String,
    #[serde(rename = "storagePath", skip_serializing_if = "str::is_empty")]
    storage_path: String,
    description: &'static str,
}

#[derive(Serialize)]
struct ServicesResponse {
    services: Vec<DashboardService>,
}

/// "running" when the service is reachable/configured, else "disabled" — the
/// out-of-process analogue of legacy `objectServiceStatus(s.x != nil)`.
fn status(configured: bool) -> &'static str {
    if configured {
        "running"
    } else {
        "disabled"
    }
}

/// Mail status: disabled when the daemon flagged it off, else running (it has no
/// snapshot pointer in legacy either — `mailServiceStatus`).
fn mail_status(disabled: bool) -> &'static str {
    if disabled {
        "disabled"
    } else {
        "running"
    }
}

pub fn handle(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    Response::json(
        200,
        &ServicesResponse {
            services: build(config),
        },
    )
}

fn build(c: &Config) -> Vec<DashboardService> {
    vec![
        DashboardService {
            id: "mail",
            name: "Mail",
            path: "/dashboard/mail",
            status: mail_status(c.mail_disabled),
            endpoint: c.mail_endpoint.clone(),
            storage_path: c.mail_storage_path.clone(),
            description: "Inspect messages received by the local SMTP server.",
        },
        DashboardService {
            id: "s3",
            name: "S3",
            path: "/dashboard/s3",
            status: status(!c.s3_base.is_empty()),
            endpoint: c.s3_endpoint.clone(),
            storage_path: c.s3_storage_path.clone(),
            description: "Browse buckets, objects, metadata, and local S3 activity.",
        },
        DashboardService {
            id: "gcs",
            name: "GCS",
            path: "/dashboard/gcs",
            status: status(!c.gcs_base.is_empty()),
            endpoint: c.gcs_endpoint.clone(),
            storage_path: c.gcs_storage_path.clone(),
            description: "Browse buckets, objects, metadata, and local GCS activity.",
        },
        DashboardService {
            id: "dynamodb",
            name: "DynamoDB",
            path: "/dashboard/dynamodb",
            status: status(!c.dynamodb_base.is_empty()),
            endpoint: c.dynamodb_endpoint.clone(),
            storage_path: c.dynamodb_storage_path.clone(),
            description: "Inspect local DynamoDB tables, indexes, and item counts.",
        },
        DashboardService {
            id: "bigquery",
            name: "BigQuery",
            path: "/dashboard/bigquery",
            status: status(!c.bigquery_base.is_empty()),
            endpoint: c.bigquery_endpoint.clone(),
            storage_path: c.bigquery_storage_path.clone(),
            description: "Inspect local BigQuery projects, datasets, tables, rows, and jobs.",
        },
        DashboardService {
            id: "redshift",
            name: "Redshift",
            path: "/dashboard/redshift",
            status: status(!c.redshift_base.is_empty()),
            endpoint: c.redshift_endpoint.clone(),
            storage_path: c.redshift_storage_path.clone(),
            description:
                "Inspect local Redshift clusters, catalog metadata, and statement history.",
        },
        DashboardService {
            id: "redis",
            name: "Redis",
            path: "/dashboard/redis",
            status: status(c.redis_enabled && !c.redis_base.is_empty()),
            endpoint: c.redis_endpoint.clone(),
            storage_path: c.redis_storage_path.clone(),
            description: "Inspect local Redis keys, TTLs, and command results.",
        },
        DashboardService {
            id: "sqs",
            name: "SQS",
            path: "/dashboard/sqs",
            status: status(!c.sqs_base.is_empty()),
            endpoint: c.sqs_endpoint_display(),
            storage_path: c.sqs_storage_path.clone(),
            description: "Inspect local SQS queues, messages, leases, and attributes.",
        },
        DashboardService {
            id: "pubsub",
            name: "Pub/Sub",
            path: "/dashboard/pubsub",
            status: status(!c.pubsub_base.is_empty()),
            endpoint: c.pubsub_endpoint.clone(),
            storage_path: c.pubsub_storage_path.clone(),
            description: "Inspect local Pub/Sub topics, subscriptions, backlog, and leases.",
        },
    ]
}

impl Config {
    /// The SQS registry endpoint mirrors the legacy default of the SQS HTTP base.
    fn sqs_endpoint_display(&self) -> String {
        if self.sqs_base.is_empty() {
            "http://127.0.0.1:19324".to_string()
        } else {
            self.sqs_base.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> Request {
        Request {
            method: "GET".to_string(),
            path: "/api/services".to_string(),
            raw_path: "/api/services".to_string(),
            query: String::new(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        }
    }

    #[test]
    fn registry_lists_nine_services_in_order() {
        let cfg = Config::default();
        let resp = handle(&cfg, &req());
        assert_eq!(resp.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        let services = v["services"].as_array().unwrap();
        let ids: Vec<&str> = services.iter().map(|s| s["id"].as_str().unwrap()).collect();
        assert_eq!(
            ids,
            ["mail", "s3", "gcs", "dynamodb", "bigquery", "redshift", "redis", "sqs", "pubsub"]
        );
    }

    #[test]
    fn sqs_running_when_base_configured() {
        let mut cfg = Config::default();
        cfg.sqs_base = "http://127.0.0.1:19324".to_string();
        let resp = handle(&cfg, &req());
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        let sqs = v["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == "sqs")
            .unwrap();
        assert_eq!(sqs["status"], "running");
    }

    #[test]
    fn sqs_disabled_when_base_empty() {
        let cfg = Config::default();
        let resp = handle(&cfg, &req());
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        let sqs = v["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == "sqs")
            .unwrap();
        assert_eq!(sqs["status"], "disabled");
    }

    #[test]
    fn rejects_non_get() {
        let cfg = Config::default();
        let mut r = req();
        r.method = "POST".to_string();
        let resp = handle(&cfg, &r);
        assert_eq!(resp.status, 405);
    }
}
