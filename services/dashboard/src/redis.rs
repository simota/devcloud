//! Redis dashboard handler — ports `internal/dashboard/redis_handlers.rs`.
//!
//!   READS  -> the redis service's `/_introspect/` API (redis/http.rs):
//!             GET {redis_base}/_introspect/status
//!             GET {redis_base}/_introspect/keys?cursor=&match=&count=
//!             GET {redis_base}/_introspect/keys/{key}
//!
//!   MUTATIONS -> the redis service's `/_control/` API:
//!             POST   {redis_base}/_control/select-db        {db}
//!             DELETE {redis_base}/_control/keys?confirm=FLUSHDB
//!             DELETE {redis_base}/_control/keys/{key}
//!             POST   {redis_base}/_control/keys/{key}/expire {ttlSeconds}
//!             POST   {redis_base}/_control/exec             {command,args}
//!
//! The redis service's `/_introspect/` + `/_control/` response bodies are already
//! byte-identical to the legacy dashboard's (see the "match …redis_handlers.rs
//! exactly" note in redis/http.rs), so every read/mutation relays verbatim.
//!
//! The one envelope the introspection API does not carry is the dashboard
//! `status` body's `storagePath` field; we merge it in (and synthesize the
//! `disabled` body) to match `handleRedisStatus` byte-for-byte.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/redis/status` — reads `/_introspect/status` and merges `storagePath`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let storage_path = config.redis_storage_path.clone();
    let address = config
        .redis_endpoint
        .strip_prefix("redis://")
        .unwrap_or(&config.redis_endpoint)
        .to_string();

    if config.redis_base.is_empty() {
        return Response::json(
            200,
            &serde_json::json!({
                "service": "redis",
                "status": "disabled",
                "running": false,
                "mode": "managed",
                "address": address,
                "serverVersion": "",
                "connectedClients": 0,
                "usedMemoryHuman": "",
                "currentDB": 0,
                "databaseCount": 0,
                "currentDBKeys": 0,
                "storagePath": storage_path,
            }),
        );
    }

    match introspect(config, "/_introspect/status").await {
        Ok(resp) if resp.status == 200 => {
            let mut body: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => {
                    return Response::text_error(502, "redis introspection returned invalid json")
                }
            };
            if let Value::Object(ref mut map) = body {
                map.insert("storagePath".to_string(), Value::String(storage_path));
            }
            Response::json(200, &body)
        }
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `/api/redis/keys` — GET scans, DELETE flushes the current DB.
pub async fn handle_keys(config: &Config, req: &Request) -> Response {
    if config.redis_base.is_empty() {
        return Response::text_error(503, "redis service is disabled");
    }
    match req.method.as_str() {
        "GET" => {
            let path = if req.query.is_empty() {
                "/_introspect/keys".to_string()
            } else {
                format!("/_introspect/keys?{}", req.query)
            };
            match introspect(config, &path).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        "DELETE" => {
            let path = if req.query.is_empty() {
                "/_control/keys".to_string()
            } else {
                format!("/_control/keys?{}", req.query)
            };
            match control(config, "DELETE", &path, Vec::new()).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        _ => Response::method_not_allowed("GET, DELETE"),
    }
}

/// `/api/redis/keys/{key}` and `/api/redis/keys/{key}/expire`.
pub async fn handle_key(config: &Config, req: &Request) -> Response {
    if config.redis_base.is_empty() {
        return Response::text_error(503, "redis service is disabled");
    }
    let (key, action) = match parse_key_path(&req.raw_path) {
        Some(v) => v,
        None => return Response::text_error(400, "invalid redis key path"),
    };
    let encoded = encode_segment(&key);

    match (action.as_str(), req.method.as_str()) {
        ("", "GET") => match introspect(config, &format!("/_introspect/keys/{encoded}")).await {
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        ("", "DELETE") => {
            match control(
                config,
                "DELETE",
                &format!("/_control/keys/{encoded}"),
                Vec::new(),
            )
            .await
            {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        ("expire", "POST") => {
            let path = if req.query.is_empty() {
                format!("/_control/keys/{encoded}/expire")
            } else {
                format!("/_control/keys/{encoded}/expire?{}", req.query)
            };
            match control(config, "POST", &path, req.body.clone()).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        _ => Response::method_not_allowed("GET, DELETE, POST"),
    }
}

/// `POST /api/redis/command` — forwards to `/_control/exec`.
pub async fn handle_command(config: &Config, req: &Request) -> Response {
    if config.redis_base.is_empty() {
        return Response::text_error(503, "redis service is disabled");
    }
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    match control(config, "POST", "/_control/exec", req.body.clone()).await {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `POST /api/redis/select-db` — forwards to `/_control/select-db`.
pub async fn handle_select_db(config: &Config, req: &Request) -> Response {
    if config.redis_base.is_empty() {
        return Response::text_error(503, "redis service is disabled");
    }
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    match control(config, "POST", "/_control/select-db", req.body.clone()).await {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.redis_base,
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
        base: &config.redis_base,
        method,
        path,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body,
    })
    .await
}

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
        ForwardError::Unreachable(_) => Response::text_error(502, "redis service is unreachable"),
        ForwardError::BadBase => {
            Response::text_error(500, "redis service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "redis service returned an invalid response")
        }
    }
}

/// Splits `/api/redis/keys/{key}` or `/api/redis/keys/{key}/expire` into
/// (key, action), mirroring legacy `redisKeyPath`. Returns `None` for an invalid key.
fn parse_key_path(escaped_path: &str) -> Option<(String, String)> {
    let suffix = escaped_path.strip_prefix("/api/redis/keys/")?;
    if suffix.is_empty() {
        return None;
    }
    let (key_part, action) = if let Some(stripped) = suffix.strip_suffix("/expire") {
        (stripped.to_string(), "expire".to_string())
    } else {
        (suffix.trim_end_matches('/').to_string(), String::new())
    };
    let key = path_segment_decode(&key_part)?;
    if key.is_empty() {
        return None;
    }
    Some((key, action))
}

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
    fn parse_key_path_plain() {
        assert_eq!(
            parse_key_path("/api/redis/keys/foo"),
            Some(("foo".into(), String::new()))
        );
    }

    #[test]
    fn parse_key_path_expire() {
        assert_eq!(
            parse_key_path("/api/redis/keys/foo/expire"),
            Some(("foo".into(), "expire".into()))
        );
    }

    #[test]
    fn parse_key_path_empty() {
        assert_eq!(parse_key_path("/api/redis/keys/"), None);
    }

    #[test]
    fn parse_key_path_decodes() {
        assert_eq!(
            parse_key_path("/api/redis/keys/a%3Ab"),
            Some(("a:b".into(), String::new()))
        );
    }
}
