//! API error type and the JSON error-body shape.
//!
//! Mirrors `writeError` in `internal/services/dynamodb/routes.go`: the HTTP body
//! is `{"__type":"com.amazonaws.dynamodb.v20120810#<Name>","message":"<msg>"}`
//! and the `X-Amzn-Errortype` response header carries `<Name>`. The compact
//! Go-style JSON encoding (HTML escaping + trailing newline) is applied by the
//! HTTP layer via `go_json`.

use serde::Serialize;

const TYPE_PREFIX: &str = "com.amazonaws.dynamodb.v20120810#";

/// A DynamoDB API error: an HTTP status, the AWS error name (e.g.
/// `ValidationException`), and a human-readable message. Handler logic returns
/// these; the dispatch layer renders them to the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiError {
    pub status: u16,
    pub name: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status: u16, name: &str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            name: name.to_string(),
            message: message.into(),
        }
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

    /// The JSON error body Go emits.
    pub fn body(&self) -> ErrorBody {
        ErrorBody {
            type_: format!("{TYPE_PREFIX}{}", self.name),
            message: self.message.clone(),
        }
    }
}

/// Serialized error body. Go writes the keys in this order (`__type` then
/// `message`) via a `map[string]string`, which Go marshals **sorted** — and
/// `__type` < `message`, so declaration order here matches.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    #[serde(rename = "__type")]
    pub type_: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_body_matches_go_shape() {
        let err = ApiError::validation("table name is required");
        let body = crate::go_json::to_vec(&err.body());
        assert_eq!(
            body,
            b"{\"__type\":\"com.amazonaws.dynamodb.v20120810#ValidationException\",\"message\":\"table name is required\"}\n".to_vec()
        );
    }
}
