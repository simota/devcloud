//! GCS Pub/Sub notification configurations (`notificationConfigs`) and local
//! notification event recording (plus optional loopback Pub/Sub REST delivery).

use std::collections::BTreeMap;

use devcloud_s3::base64;
use devcloud_s3::model::Object;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::http::{
    emit_dashboard_event, json_error, method_not_allowed, object_generation_string, url_decode,
    url_encode, Request, Response, Server,
};
use crate::state::{
    append_notification_event, read_notification_events, read_notifications, write_notifications,
    NotificationConfig, NotificationEvent,
};

const VALID_EVENT_TYPES: &[&str] = &[
    "OBJECT_FINALIZE",
    "OBJECT_METADATA_UPDATE",
    "OBJECT_DELETE",
    "OBJECT_ARCHIVE",
];

#[derive(Debug, Serialize)]
struct NotificationResource {
    kind: String,
    id: String,
    #[serde(rename = "selfLink")]
    self_link: String,
    topic: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    event_types: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    custom_attributes: BTreeMap<String, String>,
    payload_format: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    object_name_prefix: String,
    etag: String,
}

#[derive(Debug, Serialize)]
struct NotificationsListResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<NotificationResource>,
}

#[derive(Debug, Default, Deserialize)]
struct CreateNotificationRequest {
    #[serde(default)]
    topic: String,
    #[serde(default)]
    event_types: Vec<String>,
    #[serde(default)]
    custom_attributes: BTreeMap<String, String>,
    #[serde(default)]
    payload_format: String,
    #[serde(default)]
    object_name_prefix: String,
}

fn notification_resource(bucket: &str, cfg: &NotificationConfig) -> NotificationResource {
    NotificationResource {
        kind: "storage#notification".to_string(),
        id: cfg.id.clone(),
        self_link: format!(
            "/storage/v1/b/{}/notificationConfigs/{}",
            url_encode(bucket),
            url_encode(&cfg.id)
        ),
        topic: cfg.topic.clone(),
        event_types: cfg.event_types.clone(),
        custom_attributes: cfg.custom_attributes.clone(),
        payload_format: cfg.payload_format.clone(),
        object_name_prefix: cfg.object_name_prefix.clone(),
        etag: cfg.etag.clone(),
    }
}

fn normalize_topic(raw: &str) -> Result<String, &'static str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("topic is required");
    }
    let path = trimmed
        .strip_prefix("//pubsub.googleapis.com/")
        .unwrap_or(trimmed);
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() == 4
        && parts[0] == "projects"
        && !parts[1].is_empty()
        && parts[2] == "topics"
        && !parts[3].is_empty()
    {
        return Ok(format!("//pubsub.googleapis.com/{path}"));
    }
    Err("topic must match projects/{project}/topics/{topic} or //pubsub.googleapis.com/projects/{project}/topics/{topic}")
}

pub fn handle_notification_configs(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    id_escaped: Option<&str>,
) -> Response {
    match server.store().get_bucket(bucket) {
        Ok(Some(_)) => {}
        Ok(None) => return json_error(404, "notFound", "bucket not found"),
        Err(_) => return json_error(400, "invalid", "invalid bucket name"),
    }
    let mut store = match read_notifications(server.store(), bucket) {
        Ok(s) => s,
        Err(_) => return json_error(500, "backendError", "internal error"),
    };

    match id_escaped {
        None => match req.method.as_str() {
            "GET" => {
                let items = store
                    .items
                    .iter()
                    .map(|cfg| notification_resource(bucket, cfg))
                    .collect();
                Response::json_ok(&NotificationsListResponse {
                    kind: "storage#notifications".to_string(),
                    items,
                })
            }
            "POST" => {
                let parsed: CreateNotificationRequest = match serde_json::from_slice(&req.body) {
                    Ok(v) => v,
                    Err(_) => return json_error(400, "invalid", "invalid json request"),
                };
                let topic = match normalize_topic(&parsed.topic) {
                    Ok(t) => t,
                    Err(msg) => return json_error(400, "invalid", msg),
                };
                let payload_format = parsed.payload_format.trim();
                if payload_format != "JSON_API_V1" && payload_format != "NONE" {
                    return json_error(
                        400,
                        "invalid",
                        "payload_format must be JSON_API_V1 or NONE",
                    );
                }
                for et in &parsed.event_types {
                    if !VALID_EVENT_TYPES.contains(&et.as_str()) {
                        return json_error(
                            400,
                            "invalid",
                            &format!("invalid notification event_type {et:?}"),
                        );
                    }
                }
                let id = store.next_id.to_string();
                store.next_id = store.next_id.saturating_add(1);
                let cfg = NotificationConfig {
                    id: id.clone(),
                    topic,
                    event_types: parsed.event_types,
                    custom_attributes: parsed.custom_attributes,
                    payload_format: payload_format.to_string(),
                    object_name_prefix: parsed.object_name_prefix,
                    etag: id,
                };
                store.items.push(cfg.clone());
                if write_notifications(server.store(), bucket, &store).is_err() {
                    return json_error(500, "backendError", "internal error");
                }
                Response::json_ok(&notification_resource(bucket, &cfg))
            }
            _ => method_not_allowed("GET, POST"),
        },
        Some(raw_id) => {
            let id = url_decode(raw_id);
            if id.is_empty() {
                return json_error(400, "invalid", "invalid notification id");
            }
            match req.method.as_str() {
                "GET" => match store.items.iter().find(|c| c.id == id) {
                    Some(cfg) => Response::json_ok(&notification_resource(bucket, cfg)),
                    None => json_error(404, "notFound", "notification config not found"),
                },
                "DELETE" => {
                    let before = store.items.len();
                    store.items.retain(|c| c.id != id);
                    if store.items.len() == before {
                        return json_error(404, "notFound", "notification config not found");
                    }
                    if write_notifications(server.store(), bucket, &store).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    Response::empty_status(204)
                }
                _ => method_not_allowed("GET, DELETE"),
            }
        }
    }
}

/// Evaluates bucket notification configs for an object lifecycle event,
/// appends matching events to `<bucket>/gcs/notification_events.json`,
/// emits a metadata-only dashboard event, and optionally forwards to a
/// configured local loopback Pub/Sub REST endpoint.
pub fn emit_object_notification(server: &Server, event_type: &str, object: &Object) {
    let Ok(configs) = read_notifications(server.store(), &object.bucket) else {
        return;
    };
    if configs.items.is_empty() {
        return;
    }
    let event_time = server.now();
    let generation = object_generation_string(object);
    let obj_resource_value =
        serde_json::to_value(crate::http::build_object_resource(server.store(), object)).ok();

    for cfg in &configs.items {
        if !cfg.event_types.is_empty() && !cfg.event_types.iter().any(|et| et == event_type) {
            continue;
        }
        if !cfg.object_name_prefix.is_empty() && !object.key.starts_with(&cfg.object_name_prefix) {
            continue;
        }
        let mut attributes = cfg.custom_attributes.clone();
        attributes.insert("bucketId".to_string(), object.bucket.clone());
        attributes.insert("eventTime".to_string(), event_time.clone());
        attributes.insert("eventType".to_string(), event_type.to_string());
        attributes.insert(
            "notificationConfig".to_string(),
            format!(
                "projects/_/buckets/{}/notificationConfigs/{}",
                object.bucket, cfg.id
            ),
        );
        attributes.insert("objectGeneration".to_string(), generation.clone());
        attributes.insert("objectId".to_string(), object.key.clone());
        attributes.insert("payloadFormat".to_string(), cfg.payload_format.clone());

        let payload = if cfg.payload_format == "JSON_API_V1" {
            obj_resource_value.clone()
        } else {
            None
        };

        let record = NotificationEvent {
            notification_id: cfg.id.clone(),
            topic: cfg.topic.clone(),
            event_type: event_type.to_string(),
            bucket_id: object.bucket.clone(),
            object_id: object.key.clone(),
            object_generation: generation.clone(),
            payload_format: cfg.payload_format.clone(),
            event_time: event_time.clone(),
            attributes: attributes.clone(),
            payload: payload.clone(),
        };
        let _ = append_notification_event(server.store(), &object.bucket, record);
        emit_dashboard_event(
            "gcs.notification.emitted",
            serde_json::json!({
                "bucket": object.bucket,
                "key": object.key,
                "eventType": event_type,
                "topic": cfg.topic,
                "notificationId": cfg.id,
            }),
        );
        if let Some(addr) = server.pubsub_rest_addr() {
            forward_to_local_pubsub(addr, &cfg.topic, &attributes, payload.as_ref());
        }
    }
}

pub fn list_notification_events(server: &Server, bucket: &str) -> Vec<NotificationEvent> {
    read_notification_events(server.store(), bucket).unwrap_or_default()
}

fn is_loopback_addr(addr: &str) -> bool {
    addr.starts_with("127.0.0.1:") || addr.starts_with("localhost:") || addr.starts_with("[::1]:")
}

/// Builds the HTTP/1.1 Pub/Sub REST v1 `:publish` request for a GCS
/// notification message (loopback-only forwarding helper).
pub fn build_pubsub_publish_request(
    addr: &str,
    topic: &str,
    attributes: &BTreeMap<String, String>,
    payload: Option<&serde_json::Value>,
) -> String {
    let topic_path = topic
        .strip_prefix("//pubsub.googleapis.com/")
        .unwrap_or(topic);
    let data_bytes = payload
        .and_then(|v| serde_json::to_vec(v).ok())
        .unwrap_or_default();
    let body = serde_json::json!({
        "messages": [{
            "data": base64::std_encode(&data_bytes),
            "attributes": attributes,
        }]
    })
    .to_string();
    format!(
        "POST /v1/{topic_path}:publish HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

/// Best-effort publish to a local loopback devcloud Pub/Sub REST server.
fn forward_to_local_pubsub(
    addr: &str,
    topic: &str,
    attributes: &BTreeMap<String, String>,
    payload: Option<&serde_json::Value>,
) {
    if !is_loopback_addr(addr) {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let req = build_pubsub_publish_request(addr, topic, attributes, payload);
    let addr = addr.to_string();
    handle.spawn(async move {
        let Ok(mut stream) = TcpStream::connect(&addr).await else {
            return;
        };
        let _ = stream.write_all(req.as_bytes()).await;
        let _ = stream.flush().await;
    });
}
