//! API error type and the JSON error-body shape.
//!
//! Mirrors `writeError` in `internal/services/dynamodb/routes.rs`: the HTTP body
//! is `{"__type":"com.amazonaws.dynamodb.v20120810#<Name>","message":"<msg>"}`
//! and the `X-Amzn-Errortype` response header carries `<Name>`. The compact
//! legacy-style JSON encoding (HTML escaping + trailing newline) is applied by the
//! HTTP layer via `wire_json`.

use serde_json::{Map, Value};

const TYPE_PREFIX: &str = "com.amazonaws.dynamodb.v20120810#";

/// A DynamoDB API error: an HTTP status, the AWS error name (e.g.
/// `ValidationException`), a human-readable message, and optional extra body
/// fields (e.g. the `Item` on a ConditionalCheckFailedException with ALL_OLD).
/// Handler logic returns these; the dispatch layer renders them to the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiError {
    pub status: u16,
    pub name: String,
    pub message: String,
    /// Extra top-level body fields merged with `__type`/`message`. legacy builds the
    /// condition-failure body as a `map[string]any`, so the final JSON is fully
    /// key-sorted.
    pub extra: Map<String, Value>,
}

impl ApiError {
    pub fn new(status: u16, name: &str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            name: name.to_string(),
            message: message.into(),
            extra: Map::new(),
        }
    }

    /// 400 ConditionalCheckFailedException, optionally carrying the prior item
    /// (when `ReturnValuesOnConditionCheckFailure=ALL_OLD` and the item existed).
    /// Mirrors `writeConditionCheckFailed`.
    pub fn condition_check_failed(
        message: impl Into<String>,
        return_values: &str,
        old_item: Option<&crate::model::Item>,
    ) -> Self {
        let mut err = ApiError::new(400, "ConditionalCheckFailedException", message);
        if return_values == "ALL_OLD" {
            if let Some(item) = old_item {
                err.extra.insert(
                    "Item".to_string(),
                    serde_json::to_value(item).unwrap_or(Value::Null),
                );
            }
        }
        err
    }

    /// 400 ValidationException — the most common request-shape rejection.
    pub fn validation(message: impl Into<String>) -> Self {
        ApiError::new(400, "ValidationException", message)
    }

    /// 400 ResourceNotFoundException.
    pub fn not_found(message: impl Into<String>) -> Self {
        ApiError::new(400, "ResourceNotFoundException", message)
    }

    /// 400 ResourceInUseException.
    pub fn in_use(message: impl Into<String>) -> Self {
        ApiError::new(400, "ResourceInUseException", message)
    }

    /// 500 InternalServerError.
    pub fn internal(message: impl Into<String>) -> Self {
        ApiError::new(500, "InternalServerError", message)
    }

    /// The JSON error body legacy emits: `__type` + `message` plus any `extra`
    /// fields, as a `serde_json::Value` (sorted-key object, matching legacy
    /// `map[string]any`).
    pub fn body(&self) -> Value {
        let mut map = self.extra.clone();
        map.insert(
            "__type".to_string(),
            Value::String(format!("{TYPE_PREFIX}{}", self.name)),
        );
        map.insert("message".to_string(), Value::String(self.message.clone()));
        Value::Object(map)
    }

    /// The encoded legacy-wire error body (compact, HTML-escaped, trailing newline).
    pub fn body_bytes(&self) -> Vec<u8> {
        crate::wire_json::to_vec(&self.body())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_body_matches_legacy_shape() {
        let err = ApiError::validation("table name is required");
        assert_eq!(
            err.body_bytes(),
            b"{\"__type\":\"com.amazonaws.dynamodb.v20120810#ValidationException\",\"message\":\"table name is required\"}\n".to_vec()
        );
    }

    #[test]
    fn condition_failure_can_carry_item() {
        let mut item = crate::model::Item::new();
        item.insert("pk".to_string(), serde_json::json!({"S": "a"}));
        let err =
            ApiError::condition_check_failed("condition check failed", "ALL_OLD", Some(&item));
        let body = String::from_utf8(err.body_bytes()).unwrap();
        // Fully key-sorted: Item, __type, message (uppercase 'I' < '_' < 'm').
        assert!(body.starts_with("{\"Item\":{\"pk\":{\"S\":\"a\"}},\"__type\":"));
    }
}
