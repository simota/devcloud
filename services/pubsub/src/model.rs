//! Pub/Sub resource model — the persisted, byte-compatible shapes.
//!
//! Field declaration order and `omitempty` mirror the legacy structs in
//! `internal/services/pubsub/types.rs`, because `resources.json` / `pubsub.json`
//! and the REST response bodies are reproduced byte-for-byte. Free-form
//! sub-objects (schema settings, push config, dead-letter/retry policy, …) are
//! `serde_json::Value` (sorted-key objects), matching legacy `map[string]any`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

fn is_false(b: &bool) -> bool {
    !*b
}
fn is_zero(n: &i64) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Topic {
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(
        rename = "createdAt",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub created_at: String,
    #[serde(
        rename = "updatedAt",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub updated_at: String,
    #[serde(
        rename = "messageRetentionDuration",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub message_retention_duration: String,
    #[serde(
        rename = "schemaSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_settings: Option<Value>,
    #[serde(
        rename = "kmsKeyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub kms_key_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Subscription {
    pub name: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub detached: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(
        rename = "createdAt",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub created_at: String,
    #[serde(
        rename = "updatedAt",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub updated_at: String,
    #[serde(
        rename = "ackDeadlineSeconds",
        default,
        skip_serializing_if = "is_zero"
    )]
    pub ack_deadline_seconds: i64,
    #[serde(
        rename = "enableMessageOrdering",
        default,
        skip_serializing_if = "is_false"
    )]
    pub enable_message_ordering: bool,
    #[serde(
        rename = "enableExactlyOnceDelivery",
        default,
        skip_serializing_if = "is_false"
    )]
    pub enable_exactly_once_delivery: bool,
    #[serde(
        rename = "retainAckedMessages",
        default,
        skip_serializing_if = "is_false"
    )]
    pub retain_acked_messages: bool,
    #[serde(
        rename = "messageRetentionDuration",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub message_retention_duration: String,
    #[serde(
        rename = "expirationPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration_policy: Option<Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub filter: String,
    #[serde(
        rename = "deadLetterPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub dead_letter_policy: Option<Value>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<Value>,
    #[serde(
        rename = "pushConfig",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub push_config: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Snapshot {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topic: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subscription: String,
    #[serde(
        rename = "expireTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub expire_time: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<DeliveryRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaRevision {
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub type_: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub definition: String,
    #[serde(
        rename = "revisionId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub revision_id: String,
    #[serde(
        rename = "revisionCreateTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub revision_create_time: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Schema {
    pub name: String,
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub type_: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub definition: String,
    #[serde(
        rename = "revisionId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub revision_id: String,
    #[serde(
        rename = "revisionCreateTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub revision_create_time: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revisions: Vec<SchemaRevision>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PubsubMessage {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "publishTime")]
    pub publish_time: String,
    #[serde(
        rename = "orderingKey",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub ordering_key: String,
}

/// A per-subscription delivery record. legacy has **no** JSON tags on
/// `deliveryRecord`, so the field names are PascalCase and times are RFC3339
/// strings (legacy marshals `time.Time` as RFC3339). The legacy zero time marshals as
/// `0001-01-01T00:00:00Z`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    #[serde(rename = "MessageID")]
    pub message_id: String,
    #[serde(rename = "AckID")]
    pub ack_id: String,
    #[serde(rename = "LeaseDeadline")]
    pub lease_deadline: String,
    #[serde(rename = "NextDeliveryTime")]
    pub next_delivery_time: String,
    #[serde(rename = "DeliveryAttempt")]
    pub delivery_attempt: i64,
    #[serde(rename = "Acked")]
    pub acked: bool,
}

impl Default for DeliveryRecord {
    fn default() -> Self {
        DeliveryRecord {
            message_id: String::new(),
            ack_id: String::new(),
            lease_deadline: ZERO_TIME.to_string(),
            next_delivery_time: ZERO_TIME.to_string(),
            delivery_attempt: 0,
            acked: false,
        }
    }
}

/// legacy zero `time.Time` RFC3339 rendering.
pub const ZERO_TIME: &str = "0001-01-01T00:00:00Z";
