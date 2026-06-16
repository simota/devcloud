//! Response envelope and shared helpers — port of
//! `internal/services/bigquery/responses.rs`.

use std::path::Path;

use serde::Serialize;

use crate::model::{ErrorBody, ErrorItem, ErrorResponse, IamPolicy};
use crate::wire_json;

/// A rendered handler response: HTTP status + legacy-encoded body (empty for 204),
/// plus the optional headers the legacy handlers set (`Location` on dataset/table
/// creation, `Allow` on 405). The HTTP layer (part 4) adds `Server` /
/// `WWW-Authenticate` and writes this to the wire.
#[derive(Debug)]
pub struct ApiResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub location: Option<String>,
    pub allow: Option<String>,
    /// Set on auth failures (legacy: `WWW-Authenticate: Bearer realm="devcloud-bigquery"`).
    pub www_authenticate: bool,
}

impl ApiResponse {
    /// legacy `writeJSON`: `Content-Type: application/json; charset=utf-8` +
    /// `json.NewEncoder(w).Encode(value)`.
    pub fn json<T: Serialize>(status: u16, value: &T) -> Self {
        ApiResponse {
            status,
            body: wire_json::to_vec(value),
            location: None,
            allow: None,
            www_authenticate: false,
        }
    }

    /// legacy `writeError`: the BigQuery error envelope.
    pub fn error(status: u16, reason: &str, message: &str) -> Self {
        ApiResponse::json(
            status,
            &ErrorResponse {
                error: ErrorBody {
                    code: i64::from(status),
                    message: message.to_string(),
                    errors: vec![ErrorItem {
                        domain: "global".to_string(),
                        reason: reason.to_string(),
                        message: message.to_string(),
                    }],
                    status: status_text(status),
                },
            },
        )
    }

    /// legacy `methodNotAllowed`: `Allow` header + 405 error envelope.
    pub fn method_not_allowed(allow: &str) -> Self {
        let mut response = ApiResponse::error(405, "methodNotAllowed", "method not allowed");
        response.allow = Some(allow.to_string());
        response
    }

    /// Bare `w.WriteHeader(http.StatusNoContent)`.
    pub fn no_content() -> Self {
        ApiResponse {
            status: 204,
            body: Vec::new(),
            location: None,
            allow: None,
            www_authenticate: false,
        }
    }

    pub fn with_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_www_authenticate(mut self) -> Self {
        self.www_authenticate = true;
        self
    }

    pub fn body_str(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap_or("")
    }
}

/// legacy `hasChildren`.
pub(crate) fn has_children(path: &Path) -> bool {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_some(),
        Err(_) => false,
    }
}

/// legacy `datasetETag`: `"\"<UnixNano>\""`.
pub(crate) fn dataset_etag(unix_nanos: i64) -> String {
    format!("\"{unix_nanos}\"")
}

/// legacy `unixMillisString` for the same instant (`UnixMilli`).
pub(crate) fn unix_millis_string(unix_nanos: i64) -> String {
    (unix_nanos / 1_000_000).to_string()
}

/// legacy `defaultString`.
pub(crate) fn default_string(value: String, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value
    }
}

/// legacy `defaultIAMPolicy`: version 1, etag for `time.Unix(0, 0)` (`"0"`),
/// empty bindings.
pub(crate) fn default_iam_policy() -> IamPolicy {
    IamPolicy {
        version: 1,
        etag: dataset_etag(0),
        bindings: Vec::new(),
    }
}

/// legacy `normalizeIAMPolicy`. `now_unix_nanos` feeds the replacement etag when
/// the stored one is empty (legacy calls `time.Now()` inline).
pub(crate) fn normalize_iam_policy(mut policy: IamPolicy, now_unix_nanos: i64) -> IamPolicy {
    if policy.version == 0 {
        policy.version = 1;
    }
    if policy.etag.is_empty() {
        policy.etag = dataset_etag(now_unix_nanos);
    }
    policy
}

/// legacy `statusText`: explicit overrides, otherwise `http.StatusText` upper-cased
/// with spaces replaced by underscores (unknown codes → "").
pub(crate) fn status_text(status: u16) -> String {
    match status {
        400 => "BAD_REQUEST",
        401 => "UNAUTHENTICATED",
        404 => "NOT_FOUND",
        409 => "ALREADY_EXISTS",
        405 => "METHOD_NOT_ALLOWED",
        other => {
            return legacy_http_status_text(other)
                .to_uppercase()
                .replace(' ', "_")
        }
    }
    .to_string()
}

/// `net/http.StatusText` for the codes this service can produce.
fn legacy_http_status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        403 => "Forbidden",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Request Entity Too Large",
        414 => "Request URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Requested Range Not Satisfiable",
        417 => "Expectation Failed",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_text_matches_legacy_overrides_and_fallback() {
        assert_eq!(status_text(400), "BAD_REQUEST");
        assert_eq!(status_text(401), "UNAUTHENTICATED");
        assert_eq!(status_text(404), "NOT_FOUND");
        assert_eq!(status_text(405), "METHOD_NOT_ALLOWED");
        assert_eq!(status_text(409), "ALREADY_EXISTS");
        assert_eq!(status_text(500), "INTERNAL_SERVER_ERROR");
    }

    #[test]
    fn default_iam_policy_etag_is_zero_nanos() {
        let policy = default_iam_policy();
        assert_eq!(policy.version, 1);
        assert_eq!(policy.etag, "\"0\"");
        assert!(policy.bindings.is_empty());
    }
}
