//! Mail dashboard handler — ports `internal/dashboard/mail_handlers.rs`.
//!
//! The legacy dashboard routes are `/api/messages` and `/api/messages/{id}` (NOT
//! `/api/mail/*`); they are preserved byte-for-byte.
//!
//!   READS  -> the mail service's `/_introspect/` API (mail/http.rs):
//!             GET {mail_base}/_introspect/messages?limit=N   (ListMessagesResult)
//!             GET {mail_base}/_introspect/messages/{id}        (Message)
//!             GET {mail_base}/_introspect/messages/{id}/raw    (message/rfc822)
//!
//!   MUTATIONS -> the mail service's `/_control/` API:
//!             DELETE {mail_base}/_control/messages       (DeleteAll, emits mail.cleared)
//!             DELETE {mail_base}/_control/messages/{id}  (Delete, emits mail.deleted)
//!
//! The legacy dashboard read List with a fixed `limit=100` and relayed the raw
//! message bytes verbatim; both reads re-relay the introspection body so the
//! `/api/messages` shapes stay identical.

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `/api/messages` — GET lists (limit=100), DELETE clears all.
pub async fn handle_messages(config: &Config, req: &Request) -> Response {
    if config.mail_base.is_empty() {
        return Response::text_error(503, "mail service is disabled");
    }
    match req.method.as_str() {
        "GET" => match introspect(config, "/_introspect/messages?limit=100").await {
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        "DELETE" => match control(config, "DELETE", "/_control/messages", Vec::new()).await {
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        _ => Response::method_not_allowed("GET, DELETE"),
    }
}

/// `/api/messages/{id}` and `/api/messages/{id}/raw` — mirrors legacy `handleMessage`.
pub async fn handle_message(config: &Config, req: &Request) -> Response {
    if config.mail_base.is_empty() {
        return Response::text_error(503, "mail service is disabled");
    }
    let (id, raw) = match parse_message_path(&req.raw_path) {
        Some(v) => v,
        None => return Response::text_error(404, "404 page not found"),
    };

    match req.method.as_str() {
        "GET" => {
            let path = if raw {
                format!("/_introspect/messages/{}/raw", encode_segment(&id))
            } else {
                format!("/_introspect/messages/{}", encode_segment(&id))
            };
            match introspect(config, &path).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        "DELETE" => {
            if raw {
                // legacy returns 405 "GET" for DELETE on the /raw sub-path.
                return Response::method_not_allowed("GET");
            }
            let path = format!("/_control/messages/{}", encode_segment(&id));
            match control(config, "DELETE", &path, Vec::new()).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        _ => {
            if raw {
                Response::method_not_allowed("GET")
            } else {
                Response::method_not_allowed("GET, DELETE")
            }
        }
    }
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.mail_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
}

async fn control(
    config: &Config,
    method: &str,
    path: &str,
    body: Vec<u8>,
) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.mail_base,
        method,
        path,
        headers: Vec::new(),
        body,
    })
    .await
}

/// Relays a downstream response verbatim (status + body + content-type).
fn relay(resp: ForwardResponse) -> Response {
    let content_type = {
        let ct = resp.header("content-type");
        if ct.is_empty() {
            "application/json".to_string()
        } else {
            ct.to_string()
        }
    };
    Response::new(resp.status, &content_type, resp.body)
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "mail service is unreachable"),
        ForwardError::BadBase => Response::text_error(500, "mail service address is misconfigured"),
        ForwardError::BadResponse => {
            Response::text_error(502, "mail service returned an invalid response")
        }
    }
}

/// Splits `/api/messages/{id}` or `/api/messages/{id}/raw` into (id, raw),
/// mirroring legacy `parseMessagePath`. Returns `None` when not a valid message path.
fn parse_message_path(escaped_path: &str) -> Option<(String, bool)> {
    let suffix = escaped_path.strip_prefix("/api/messages/")?;
    let trimmed = suffix.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.split_once('/') {
        Some((id, "raw")) => {
            let decoded = path_segment_decode(id)?;
            if decoded.is_empty() {
                return None;
            }
            Some((decoded, true))
        }
        Some(_) => None,
        None => {
            let decoded = path_segment_decode(trimmed)?;
            if decoded.is_empty() {
                return None;
            }
            Some((decoded, false))
        }
    }
}

/// Percent-encodes a single path segment for an outbound introspection URL.
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_message_path_plain_id() {
        assert_eq!(
            parse_message_path("/api/messages/abc"),
            Some(("abc".into(), false))
        );
    }

    #[test]
    fn parse_message_path_raw() {
        assert_eq!(
            parse_message_path("/api/messages/abc/raw"),
            Some(("abc".into(), true))
        );
    }

    #[test]
    fn parse_message_path_rejects_other_suffix() {
        assert_eq!(parse_message_path("/api/messages/abc/other"), None);
    }

    #[test]
    fn parse_message_path_empty() {
        assert_eq!(parse_message_path("/api/messages/"), None);
    }

    #[test]
    fn parse_message_path_decodes() {
        assert_eq!(
            parse_message_path("/api/messages/a%2Bb"),
            Some(("a+b".into(), false))
        );
    }
}
