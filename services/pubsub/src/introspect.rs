//! Read-only introspection API, ported from `internal/services/pubsub/introspect.rs`.
//!
//! CONVENTION (reused verbatim across every service):
//!   - All introspection routes live under the `/_introspect/` prefix and are
//!     intercepted at the top of the REST router, AFTER auth + readiness checks,
//!     BEFORE the `/v1/` REST routing.
//!   - Methods are GET-only and read-only. No mutation endpoints here.
//!   - Each response body is the exact JSON encoding of the same snapshot struct
//!     the dashboard browses (no bespoke DTOs); message payloads are never
//!     included — only ids and delivery bookkeeping.
//!   - A missing resource returns 404; an unsupported method returns 405.
//!   - Mounted on the REST HTTP server ONLY — never the gRPC server.
//!
//! Routes:
//!   GET /_introspect/snapshot       -> Server::snapshot()
//!   GET /_introspect/messages/{id}  -> Server::message_snapshot(id)

use crate::errors::ApiError;
use crate::server::{RestResponse, Server};

pub const INTROSPECT_PREFIX: &str = "/_introspect/";

/// Reports whether the request targets the introspection API.
pub fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

/// Serves the read-only introspection endpoints. `method` is the request method;
/// `path` is the raw request path (already known to start with the prefix).
pub fn handle_introspect(server: &mut Server, method: &str, path: &str) -> RestResponse {
    if method != "GET" {
        let mut resp = render_error(&ApiError::new(
            405,
            "METHOD_NOT_ALLOWED",
            "introspection endpoints are read-only",
        ));
        resp.allow = Some("GET".to_string());
        return resp;
    }

    let rest = path.strip_prefix(INTROSPECT_PREFIX).unwrap_or("");
    if rest == "snapshot" {
        return RestResponse::ok_struct(&server.snapshot());
    }
    if let Some(message_id) = rest.strip_prefix("messages/") {
        // Reject empty ids and sub-paths like /_introspect/messages/id/extra.
        if !message_id.is_empty() && !message_id.contains('/') {
            return match server.message_snapshot(message_id) {
                Some(snapshot) => RestResponse::ok_struct(&snapshot),
                None => render_error(&ApiError::not_found("message does not exist")),
            };
        }
    }

    render_error(&ApiError::not_found("introspection endpoint not found"))
}

fn render_error(err: &ApiError) -> RestResponse {
    RestResponse {
        status: err.status,
        body: err.body_bytes(),
        allow: None,
        www_authenticate: false,
    }
}
