//! REST API error type and the JSON error-body shape.
//!
//! Mirrors `writeError` in `internal/services/pubsub/responses.go`: the body is
//! `{"error":{"code":<status>,"message":...,"status":<CODE>}}` (a sorted-key
//! `map[string]any`, so `code` < `message` < `status`). The HTTP status carries
//! the same numeric code.

use serde_json::{json, Value};

/// A Pub/Sub REST error: HTTP status, the gRPC-style status code string (e.g.
/// `INVALID_ARGUMENT`), and a human-readable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code: code.to_string(),
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        ApiError::new(400, "INVALID_ARGUMENT", message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        ApiError::new(404, "NOT_FOUND", message)
    }
    pub fn already_exists(message: impl Into<String>) -> Self {
        ApiError::new(409, "ALREADY_EXISTS", message)
    }
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        ApiError::new(400, "FAILED_PRECONDITION", message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        ApiError::new(500, "INTERNAL", message)
    }

    /// The JSON error body Go emits (sorted-key object).
    pub fn body(&self) -> Value {
        json!({
            "error": {
                "code": self.status,
                "message": self.message,
                "status": self.code,
            }
        })
    }

    /// The encoded Go-wire error body (compact, HTML-escaped, trailing newline).
    pub fn body_bytes(&self) -> Vec<u8> {
        crate::go_json::to_vec(&self.body())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_is_sorted_key_object() {
        let err = ApiError::invalid_argument("bad");
        assert_eq!(
            String::from_utf8(err.body_bytes()).unwrap(),
            "{\"error\":{\"code\":400,\"message\":\"bad\",\"status\":\"INVALID_ARGUMENT\"}}\n"
        );
    }
}
