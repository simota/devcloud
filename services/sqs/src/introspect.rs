//! Read-only introspection API for SQS, mirroring
//! `internal/services/sqs/{introspect,dashboard}.rs`.
//!
//! CONVENTION (reused verbatim across every service):
//!   * All introspection routes live under the `/_introspect/` prefix and are
//!     intercepted at the top of the HTTP handler, BEFORE the provider-protocol
//!     (AWS Query/JSON action) dispatch.
//!   * Methods are GET-only and read-only. No mutation endpoints here.
//!   * Each response body is the exact JSON encoding of the same snapshot the
//!     dashboard already serializes in-process (no message body content beyond
//!     what `dashboard.rs` already exposes).
//!   * A missing resource returns 404; an unsupported method returns 405.
//!
//! Responses are rendered through the same `JsonOutcome` the JSON action path
//! uses, so the socket layer emits the bytes `writeJSON` / `writeJSONError`
//! would (`application/x-amz-json-1.0`, `x-amzn-RequestId: devcloud-sqs`).
//!
//! Shapes are `#[derive(Serialize)]` structs (NOT `serde_json::Value`/`json!`,
//! which sort object keys): serde emits STRUCT fields in declaration order,
//! matching legacy `encoding/json`, while the nested `BTreeMap`s sort their keys
//! the same way legacy sorts map keys. Each `#[serde(rename)]` + `skip_serializing_if`
//! reproduces the corresponding legacy json tag (`omitempty`).

use std::collections::BTreeMap;

use serde::Serialize;

use crate::policy::parse_redrive_policy;
use crate::server::QueueState;
use crate::time_fmt::before;
use crate::{MessageAttributeValue, MessageState, Server};

pub const INTROSPECT_PREFIX: &str = "/_introspect/";

/// A rendered introspection response. The body is serialized to bytes HERE (not
/// carried as a `serde_json::Value`) so the snapshot structs keep their field
/// declaration order: `serde_json::to_value` would route through
/// `Value::Object` which — with `preserve_order` off — sorts keys, diverging
/// from legacy `encoding/json` struct field order.
pub struct IntrospectOutcome {
    pub status: u16,
    pub body: Vec<u8>,
    /// `true` only for the GET-only 405, which carries `Allow: GET`.
    pub allow_get: bool,
}

/// Reports whether the request targets the introspection API.
pub fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

/// Serves the read-only introspection endpoints:
///
///   GET /_introspect/queues               -> Snapshot()
///   GET /_introspect/queues/{name}        -> QueueDetailSnapshot(name)
///   GET /_introspect/queues/{name}/dlq    -> DeadLetterSnapshot(name)
///
/// Mirrors `handleIntrospect`: non-GET → 405 (Allow: GET), unknown subpath → 404.
pub fn handle_introspect(server: &Server, method: &str, path: &str) -> IntrospectOutcome {
    if method != "GET" {
        return error_outcome(
            405,
            "InvalidAction",
            "introspection endpoints are read-only",
        );
    }

    let rest = path.strip_prefix(INTROSPECT_PREFIX).unwrap_or("");
    if rest == "queues" {
        return ok_struct(&snapshot(server));
    }
    if let Some(after) = rest.strip_prefix("queues/") {
        let segments: Vec<&str> = after.split('/').collect();
        let name = segments[0];
        if !name.is_empty() {
            match segments.len() {
                1 => {
                    return match queue_detail_snapshot(server, name) {
                        Some(detail) => ok_struct(&detail),
                        None => error_outcome(
                            404,
                            "AWS.SimpleQueueService.NonExistentQueue",
                            "queue does not exist",
                        ),
                    }
                }
                2 if segments[1] == "dlq" => {
                    return match dead_letter_snapshot(server, name) {
                        Some(dlq) => ok_struct(&dlq),
                        None => error_outcome(
                            404,
                            "AWS.SimpleQueueService.NonExistentQueue",
                            "queue does not exist",
                        ),
                    }
                }
                _ => {}
            }
        }
    }

    error_outcome(404, "InvalidAddress", "introspection endpoint not found")
}

/// Renders a serializable snapshot as a 200 OK outcome, serializing the struct
/// directly to bytes to preserve field declaration order.
fn ok_struct<T: Serialize>(value: &T) -> IntrospectOutcome {
    IntrospectOutcome {
        status: 200,
        body: serde_json::to_vec(value).unwrap_or_default(),
        allow_get: false,
    }
}

/// Mirrors `writeJSONError`: `{"__type": code, "message": msg}`. The map's two
/// keys sort the same in legacy and serde (`__type` < `message`). The 405 carries
/// `Allow: GET`, matching `handleIntrospect`.
fn error_outcome(status: u16, code: &str, message: &str) -> IntrospectOutcome {
    let body = BTreeMap::from([("__type", code), ("message", message)]);
    IntrospectOutcome {
        status,
        body: serde_json::to_vec(&body).unwrap_or_default(),
        allow_get: status == 405,
    }
}

// --- snapshot DTOs (mirror dashboard.rs struct declaration order + json tags) ---

#[derive(Serialize)]
struct Snapshot {
    status: &'static str,
    running: bool,
    region: String,
    queues: Vec<QueueSnapshot>,
}

#[derive(Serialize)]
struct QueueSnapshot {
    name: String,
    url: String,
    arn: String,
    attributes: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tags: BTreeMap<String, String>,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "visibleMessages")]
    visible_messages: i64,
    #[serde(rename = "notVisibleMessages")]
    not_visible_messages: i64,
    #[serde(rename = "delayedMessages")]
    delayed_messages: i64,
    #[serde(rename = "totalRetainedMessages")]
    total_retained_messages: i64,
}

#[derive(Serialize)]
struct QueueDetailSnapshot {
    queue: QueueSnapshot,
    messages: Vec<MessageSnapshot>,
    leases: Vec<LeaseSnapshot>,
}

#[derive(Serialize)]
struct DeadLetterSnapshot {
    #[serde(rename = "deadLetterQueue", skip_serializing_if = "Option::is_none")]
    dead_letter_queue: Option<QueueSnapshot>,
    #[serde(rename = "deadLetterSourceQueues")]
    dead_letter_source_queues: Vec<QueueSnapshot>,
}

#[derive(Serialize)]
struct MessageSnapshot {
    #[serde(rename = "messageId")]
    message_id: String,
    body: String,
    #[serde(rename = "md5OfMessageBody")]
    md5_of_message_body: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<String, MessageAttributeValue>,
    #[serde(
        rename = "systemAttributes",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    system_attributes: BTreeMap<String, MessageAttributeValue>,
    #[serde(rename = "sentAt")]
    sent_at: String,
    #[serde(rename = "availableAt")]
    available_at: String,
    // legacy: `InvisibleUntil time.Time json:"invisibleUntil,omitempty"`. `omitempty`
    // does NOT drop a (non-pointer) struct value, so the zero time string is
    // always emitted — normalized below to the legacy zero time.
    #[serde(rename = "invisibleUntil")]
    invisible_until: String,
    #[serde(rename = "receiveCount")]
    receive_count: i64,
    // legacy: `FirstReceiveAt *time.Time json:"firstReceiveAt,omitempty"` — a nil
    // pointer when the source time is zero, so legacy drops it.
    #[serde(rename = "firstReceiveAt", skip_serializing_if = "Option::is_none")]
    first_receive_at: Option<String>,
    state: &'static str,
    #[serde(rename = "messageGroupId", skip_serializing_if = "String::is_empty")]
    message_group_id: String,
    #[serde(rename = "deduplicationId", skip_serializing_if = "String::is_empty")]
    deduplication_id: String,
    #[serde(rename = "sequenceNumber", skip_serializing_if = "String::is_empty")]
    sequence_number: String,
}

#[derive(Serialize)]
struct LeaseSnapshot {
    #[serde(rename = "messageId")]
    message_id: String,
    #[serde(rename = "visibleAfter")]
    visible_after: String,
    #[serde(rename = "receiveCount")]
    receive_count: i64,
    #[serde(rename = "receiptHandlePresent")]
    receipt_handle_present: bool,
}

// --- snapshot builders (mirror dashboard.rs) ---

/// Mirrors `Server.Snapshot`. Queues iterate in sorted order (`BTreeMap`
/// matches legacy `sort.Strings(names)`); counts are computed read-only.
fn snapshot(server: &Server) -> Snapshot {
    let now = crate::time_fmt::now_rfc3339();
    let queues = server
        .queues
        .values()
        .map(|q| queue_snapshot(q, &now))
        .collect();
    Snapshot {
        status: "running",
        running: true,
        region: default_str(&server.config().region, "us-east-1").to_string(),
        queues,
    }
}

/// Mirrors `queueSnapshotLocked`.
fn queue_snapshot(queue: &QueueState, now: &str) -> QueueSnapshot {
    let mut visible = 0i64;
    let mut not_visible = 0i64;
    let mut delayed = 0i64;
    let mut total = 0i64;
    for message in &queue.messages {
        if message.deleted {
            continue;
        }
        total += 1;
        if before(now, &message.available_at) {
            delayed += 1;
        } else if before(now, &message.invisible_until) {
            not_visible += 1;
        } else {
            visible += 1;
        }
    }
    QueueSnapshot {
        name: queue.name.clone(),
        url: queue.url.clone(),
        arn: queue.arn.clone(),
        attributes: queue.attributes.clone(),
        tags: queue.tags.clone(),
        created_at: queue.created_at.clone(),
        visible_messages: visible,
        not_visible_messages: not_visible,
        delayed_messages: delayed,
        total_retained_messages: total,
    }
}

/// Mirrors `Server.QueueDetailSnapshot`. Returns `None` when the queue is absent.
fn queue_detail_snapshot(server: &Server, name: &str) -> Option<QueueDetailSnapshot> {
    let queue = server.queues.get(name)?;
    let now = crate::time_fmt::now_rfc3339();

    let mut messages = Vec::new();
    let mut leases = Vec::new();
    for message in &queue.messages {
        if message.deleted {
            continue;
        }
        messages.push(message_snapshot(message, &now));
        if !message.receipt_handle.is_empty() && before(&now, &message.invisible_until) {
            leases.push(LeaseSnapshot {
                message_id: message.id.clone(),
                visible_after: message.invisible_until.clone(),
                receive_count: message.receive_count,
                receipt_handle_present: true,
            });
        }
    }

    Some(QueueDetailSnapshot {
        queue: queue_snapshot(queue, &now),
        messages,
        leases,
    })
}

/// Mirrors `Server.DeadLetterSnapshot`.
fn dead_letter_snapshot(server: &Server, name: &str) -> Option<DeadLetterSnapshot> {
    let queue = server.queues.get(name)?;
    let now = crate::time_fmt::now_rfc3339();

    let mut dead_letter_queue = None;
    if let Some(policy) = redrive_policy_from_queue(queue) {
        if let Some(dlq) = server
            .queues
            .values()
            .find(|q| q.arn == policy.dead_letter_target_arn)
        {
            dead_letter_queue = Some(queue_snapshot(dlq, &now));
        }
    }

    // BTreeMap iteration is already sorted by name, matching legacy
    // sort.Strings(sourceNames).
    let dead_letter_source_queues = server
        .queues
        .values()
        .filter(|source| {
            redrive_policy_from_queue(source)
                .map(|p| p.dead_letter_target_arn == queue.arn)
                .unwrap_or(false)
        })
        .map(|source| queue_snapshot(source, &now))
        .collect();

    Some(DeadLetterSnapshot {
        dead_letter_queue,
        dead_letter_source_queues,
    })
}

/// Mirrors `messageSnapshotLocked`.
fn message_snapshot(message: &MessageState, now: &str) -> MessageSnapshot {
    MessageSnapshot {
        message_id: message.id.clone(),
        body: message.body.clone(),
        md5_of_message_body: message.body_md5.clone(),
        attributes: message.attributes.clone(),
        system_attributes: message.system_attributes.clone(),
        sent_at: zero_time_normalized(&message.sent_at),
        available_at: zero_time_normalized(&message.available_at),
        invisible_until: zero_time_normalized(&message.invisible_until),
        receive_count: message.receive_count,
        first_receive_at: if is_zero_time(&message.first_receive_at) {
            None
        } else {
            Some(message.first_receive_at.clone())
        },
        state: message_state_name(message, now),
        message_group_id: message.message_group_id.clone(),
        deduplication_id: message.deduplication_id.clone(),
        sequence_number: message.sequence_number.clone(),
    }
}

/// Mirrors `messageStateName`.
fn message_state_name(message: &MessageState, now: &str) -> &'static str {
    if message.deleted {
        "deleted"
    } else if before(now, &message.available_at) {
        "delayed"
    } else if before(now, &message.invisible_until) {
        "in_flight"
    } else {
        "visible"
    }
}

// --- local helpers ---

/// Mirrors `redrivePolicyFromQueue` (`queue_attributes.rs`): returns the policy
/// only when valid (maxReceiveCount >= 1, non-empty target ARN).
fn redrive_policy_from_queue(queue: &QueueState) -> Option<crate::policy::RedrivePolicy> {
    let raw = queue.attributes.get("RedrivePolicy")?;
    if raw.is_empty() {
        return None;
    }
    let policy = parse_redrive_policy(raw).ok()?;
    if policy.max_receive_count < 1 || policy.dead_letter_target_arn.is_empty() {
        return None;
    }
    Some(policy)
}

fn default_str<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() {
        fallback
    } else {
        value
    }
}

/// `true` when the timestamp is the legacy zero time or an unset (empty) string.
fn is_zero_time(s: &str) -> bool {
    s.is_empty() || s == crate::model::ZERO_TIME
}

/// Normalizes an in-memory timestamp to its legacy `time.Time` JSON form: a zero or
/// unset value serializes as the legacy zero time `0001-01-01T00:00:00Z` (an
/// in-memory `String::default()` is `""`, which legacy never emits for a non-pointer
/// `time.Time` field).
fn zero_time_normalized(s: &str) -> String {
    if s.is_empty() {
        crate::model::ZERO_TIME.to_string()
    } else {
        s.to_string()
    }
}
