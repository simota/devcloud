//! Mirrors the in-memory + persisted state types from
//! `internal/services/sqs/{types,persistence}.go`.
//!
//! Serialization is pinned to Go's `encoding/json`:
//!   * `messageState`, `deduplicationState`, `moveTaskState` have NO Go json
//!     tags, so they serialize with their exact Go field names (PascalCase) and
//!     every field is always present.
//!   * `time.Time` fields serialize as RFC 3339; the Go zero time is the literal
//!     `0001-01-01T00:00:00Z`, which we model as a `String` default.
//!   * `messageAttributeValue` keeps its Go json tags (omitempty on the value
//!     fields).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Go's zero `time.Time` in RFC 3339 — emitted for unset message timestamps.
pub const ZERO_TIME: &str = "0001-01-01T00:00:00Z";

fn zero_time() -> String {
    ZERO_TIME.to_string()
}

/// Mirrors `messageAttributeValue` (keeps Go json tags + omitempty).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttributeValue {
    #[serde(rename = "DataType", default)]
    pub data_type: String,
    #[serde(
        rename = "StringValue",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub string_value: String,
    #[serde(
        rename = "BinaryValue",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_value: String,
    #[serde(
        rename = "StringListValues",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub string_list_values: Vec<String>,
    #[serde(
        rename = "BinaryListValues",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub binary_list_values: Vec<String>,
}

/// Mirrors `messageState`. No Go json tags → exact field names, all always
/// present. Times are RFC 3339 strings (zero = `0001-01-01T00:00:00Z`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageState {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Body", default)]
    pub body: String,
    #[serde(rename = "BodyMD5", default)]
    pub body_md5: String,
    #[serde(rename = "Attributes", default)]
    pub attributes: BTreeMap<String, MessageAttributeValue>,
    #[serde(rename = "SystemAttributes", default)]
    pub system_attributes: BTreeMap<String, MessageAttributeValue>,
    #[serde(rename = "SentAt", default = "zero_time")]
    pub sent_at: String,
    #[serde(rename = "AvailableAt", default = "zero_time")]
    pub available_at: String,
    #[serde(rename = "InvisibleUntil", default = "zero_time")]
    pub invisible_until: String,
    #[serde(rename = "ReceiveCount", default)]
    pub receive_count: i64,
    #[serde(rename = "FirstReceiveAt", default = "zero_time")]
    pub first_receive_at: String,
    #[serde(rename = "ReceiptHandle", default)]
    pub receipt_handle: String,
    #[serde(rename = "Deleted", default)]
    pub deleted: bool,
    #[serde(rename = "MessageGroupID", default)]
    pub message_group_id: String,
    #[serde(rename = "DeduplicationID", default)]
    pub deduplication_id: String,
    #[serde(rename = "SequenceNumber", default)]
    pub sequence_number: String,
    #[serde(rename = "DeadLetterSourceARN", default)]
    pub dead_letter_source_arn: String,
}

/// Mirrors `deduplicationState`. No Go json tags → exact field names.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicationState {
    #[serde(rename = "ExpiresAt", default = "zero_time")]
    pub expires_at: String,
    #[serde(rename = "Message", default, skip_serializing_if = "Option::is_none")]
    pub message: Option<MessageState>,
}

/// Mirrors `moveTaskState`. No Go json tags → exact field names.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveTaskState {
    #[serde(rename = "TaskHandle", default)]
    pub task_handle: String,
    #[serde(rename = "SourceARN", default)]
    pub source_arn: String,
    #[serde(rename = "DestinationARN", default)]
    pub destination_arn: String,
    #[serde(rename = "Status", default)]
    pub status: String,
    #[serde(rename = "StartedAt", default = "zero_time")]
    pub started_at: String,
    #[serde(rename = "ApproximateNumberOfMessagesMoved", default)]
    pub approximate_number_of_messages_moved: i64,
}
