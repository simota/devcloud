//! Lambda function URLs: the stored configuration, request validation, and
//! the data plane that turns an HTTP request into a payload format 2.0 event
//! and the handler's result back into an HTTP response.
//!
//! A URL is served on the Lambda listener itself, like Cloud Run services:
//! `Host: <url-id>.lambda-url.<region>.localhost[:port]` (the `FunctionUrl`
//! devcloud hands out) or the path form `/_url/<url-id>/...` for clients that
//! cannot set the Host header. The handler runs through the same invoke path
//! as the `Invoke` API (synchronous `RequestResponse`), so it is recorded and
//! executed exactly like any other invocation.
//!
//! `AuthType: AWS_IAM` refuses unsigned requests in every auth mode; strict
//! mode verifies the SigV4 signature (service `lambda`) against the configured
//! credentials, the other modes check only that it is well formed. `NONE`
//! needs no signature: devcloud does not evaluate resource-based policies, so
//! no `AddPermission` is needed for public access.

use std::collections::{BTreeMap, HashMap};

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::http::{decode_query_component, Request};
use crate::server::Reply;
use crate::sigv4::{verify_signature, Credentials, SignedRequest};

const HOST_MARKER: &str = ".lambda-url.";
const HOST_SUFFIX: &str = ".localhost";
const PATH_PREFIX: &str = "/_url/";
/// Stable path form: `/urls/<function name>/...` survives the URL being
/// recreated and needs no `*.localhost` name resolution.
const NAME_PATH_PREFIX: &str = "/urls/";

/// Which function URL a request addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UrlTarget {
    /// By URL id (host name or `/_url/<id>/`).
    Id(String),
    /// By function name (`/urls/<name>/`).
    Function(String),
}

/// The URL id devcloud gives a function's URL: derived from the account,
/// region, and function name, so it stays the same when the URL (or the
/// function, or the whole data volume) is recreated.
pub(crate) fn url_id_for(account_id: &str, region: &str, function: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(&Sha256::digest(format!("{account_id}:{region}:{function}").as_bytes())[..16])
}

/// A function's URL configuration. Field names are the persisted
/// `state.json` keys.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct UrlConfig {
    pub url_id: String,
    pub auth_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<Cors>,
    pub invoke_mode: String,
    pub creation_time: String,
    pub last_modified_time: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Cors {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_credentials: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_headers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_methods: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_origins: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose_headers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<i64>,
}

/// The settable fields of a Create/UpdateFunctionUrlConfig request; `None`
/// means "not in the request".
pub(crate) struct UrlSettings {
    pub auth_type: Option<String>,
    pub cors: Option<Cors>,
    pub invoke_mode: Option<String>,
}

fn validation(message: &str) -> Reply {
    Reply::error(400, "ValidationException", message)
}

/// Parses and validates the request body of Create/UpdateFunctionUrlConfig.
pub(crate) fn parse_settings(req: &Value) -> Result<UrlSettings, Reply> {
    let auth_type = match req.get("AuthType") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "NONE" || s == "AWS_IAM" => Some(s.clone()),
        Some(other) => {
            let shown = other
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| other.to_string());
            return Err(validation(&format!(
                "1 validation error detected: Value '{shown}' at 'authType' failed to satisfy constraint: Member must satisfy enum value set: [AWS_IAM, NONE]"
            )));
        }
    };
    let invoke_mode = match req.get("InvokeMode") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "BUFFERED" || s == "RESPONSE_STREAM" => Some(s.clone()),
        Some(other) => {
            let shown = other
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| other.to_string());
            return Err(validation(&format!(
                "1 validation error detected: Value '{shown}' at 'invokeMode' failed to satisfy constraint: Member must satisfy enum value set: [BUFFERED, RESPONSE_STREAM]"
            )));
        }
    };
    let cors = match req.get("Cors") {
        None | Some(Value::Null) => None,
        Some(v) => Some(parse_cors(v)?),
    };
    Ok(UrlSettings {
        auth_type,
        cors,
        invoke_mode,
    })
}

fn parse_cors(v: &Value) -> Result<Cors, Reply> {
    let obj = v
        .as_object()
        .ok_or_else(|| validation("Cors must be an object"))?;
    let list = |key: &str| -> Result<Option<Vec<String>>, Reply> {
        match obj.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Array(items)) => items
                .iter()
                .map(|i| match i.as_str() {
                    // The values are echoed into response headers verbatim.
                    Some(s) if s.chars().any(char::is_control) => Err(validation(&format!(
                        "Cors.{key} values must not contain control characters"
                    ))),
                    Some(s) => Ok(s.to_string()),
                    None => Err(validation(&format!("Cors.{key} must be a list of strings"))),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some),
            Some(_) => Err(validation(&format!("Cors.{key} must be a list of strings"))),
        }
    };
    let allow_credentials = match obj.get("AllowCredentials") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(_) => return Err(validation("Cors.AllowCredentials must be a boolean")),
    };
    let max_age = match obj.get("MaxAge") {
        None | Some(Value::Null) => None,
        Some(v) => match v.as_i64() {
            Some(n) if (0..=86400).contains(&n) => Some(n),
            _ => {
                return Err(validation(&format!(
                    "1 validation error detected: Value '{v}' at 'cors.maxAge' failed to satisfy constraint: Member must have value between 0 and 86400"
                )))
            }
        },
    };
    Ok(Cors {
        allow_credentials,
        allow_headers: list("AllowHeaders")?,
        allow_methods: list("AllowMethods")?,
        allow_origins: list("AllowOrigins")?,
        expose_headers: list("ExposeHeaders")?,
        max_age,
    })
}

/// The URL clients use: `<scheme>://<url-id>.lambda-url.<region>.localhost[:port]/`,
/// with the scheme and port of devcloud's public Lambda endpoint.
pub(crate) fn function_url(endpoint: &str, url_id: &str, region: &str) -> String {
    let (scheme, rest) = endpoint.split_once("://").unwrap_or(("http", endpoint));
    let authority = rest.split('/').next().unwrap_or("");
    let port = match authority.rsplit_once(':') {
        // `[::1]` alone has colons but no port.
        Some((host, port)) if !host.ends_with(':') && port.bytes().all(|b| b.is_ascii_digit()) => {
            format!(":{port}")
        }
        _ => String::new(),
    };
    format!("{scheme}://{url_id}{HOST_MARKER}{region}{HOST_SUFFIX}{port}/")
}

/// The URL id a request is addressed to and the function-relative raw path,
/// when the request targets a function URL rather than the Lambda API.
pub(crate) fn route(req: &Request) -> Option<(UrlTarget, String)> {
    let host = req
        .headers
        .get("host")
        .map(|h| h.to_ascii_lowercase())
        .unwrap_or_default();
    let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(&host);
    if let Some(prefix) = host.strip_suffix(HOST_SUFFIX) {
        if let Some((id, _region)) = prefix.split_once(HOST_MARKER) {
            if is_url_id(id) {
                let path = if req.raw_path.is_empty() {
                    "/".to_string()
                } else {
                    req.raw_path.clone()
                };
                return Some((UrlTarget::Id(id.to_string()), path));
            }
        }
    }
    let split = |rest: &str| match rest.find('/') {
        Some(i) => (rest[..i].to_string(), rest[i..].to_string()),
        None => (rest.to_string(), "/".to_string()),
    };
    if let Some(rest) = req.raw_path.strip_prefix(NAME_PATH_PREFIX) {
        let (name, path) = split(rest);
        return Some((UrlTarget::Function(name), path));
    }
    let (id, path) = split(req.raw_path.strip_prefix(PATH_PREFIX)?);
    // A malformed id is still a URL request (answered 403 like AWS), never a
    // Lambda API call.
    Some((UrlTarget::Id(id.to_ascii_lowercase()), path))
}

fn is_url_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Authenticates a request to an `AWS_IAM` URL and returns the caller's
/// access key. Unsigned requests are refused in every auth mode; strict mode
/// verifies the signature against the configured credentials, the other
/// modes only check that it is a well-formed SigV4 signature for `lambda`.
pub(crate) fn verify_iam(req: &Request, cfg: &crate::server::Config) -> Result<String, Reply> {
    let authorization = req
        .headers
        .get("authorization")
        .map(String::as_str)
        .unwrap_or("");
    let header_fn = |name: &str| -> Option<String> { req.headers.get(name).cloned() };
    let get = |name: &str| req.headers.get(name).map(String::as_str).unwrap_or("");
    let signed = SignedRequest {
        method: &req.method,
        path: &req.raw_path,
        query: &req.query,
        host: get("host"),
        authorization,
        amz_date: get("x-amz-date"),
        content_sha256: get("x-amz-content-sha256"),
        header: &header_fn,
        body: &req.body,
    };
    // The URL auth mode, when set, overrides the API's: signatures can be
    // verified on URLs while the API stays relaxed.
    let mode = if cfg.url_auth_mode.is_empty() {
        &cfg.auth_mode
    } else {
        &cfg.url_auth_mode
    };
    let strict = mode.eq_ignore_ascii_case("strict");
    let creds = Credentials {
        auth_mode: if strict { "strict" } else { "signed-relaxed" },
        access_key_id: &cfg.access_key_id,
        secret_access_key: &cfg.secret_access_key,
        region: &cfg.region,
    };
    verify_signature(&signed, &creds).map_err(|_| forbidden())?;
    Ok(caller_access_key(authorization))
}

pub(crate) fn forbidden() -> Reply {
    message_reply(403, "Forbidden")
}

fn message_reply(status: u16, message: &str) -> Reply {
    Reply::json(status, &json!({ "Message": message }))
}

/// The caller's access key from a SigV4 `Authorization` header.
pub(crate) fn caller_access_key(authorization: &str) -> String {
    authorization
        .split("Credential=")
        .nth(1)
        .and_then(|c| c.split('/').next())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Everything about the request the event needs besides the request itself.
pub(crate) struct EventContext<'a> {
    pub url_id: &'a str,
    pub raw_path: &'a str,
    pub account_id: &'a str,
    pub request_id: &'a str,
    /// `Some(access key)` for an `AWS_IAM` URL.
    pub iam_access_key: Option<&'a str>,
    pub domain_name: String,
}

/// Builds the API Gateway payload format 2.0 event Lambda sends for a URL.
pub(crate) fn build_event(req: &Request, ctx: &EventContext) -> Value {
    let mut headers = Map::new();
    let mut cookies = Vec::new();
    let mut sorted: Vec<(&String, &String)> = req.headers.iter().collect();
    sorted.sort();
    for (k, v) in sorted {
        if k == "cookie" {
            cookies.extend(
                v.split(';')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .map(|c| Value::String(c.to_string())),
            );
        } else {
            headers.insert(k.clone(), Value::String(v.clone()));
        }
    }

    let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pair in req.query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        query
            .entry(decode_query_component(k))
            .or_default()
            .push(decode_query_component(v));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let user_agent = req.headers.get("user-agent").cloned().unwrap_or_default();
    let mut request_context = json!({
        "accountId": if ctx.iam_access_key.is_some() { ctx.account_id } else { "anonymous" },
        "apiId": ctx.url_id,
        "domainName": ctx.domain_name,
        "domainPrefix": ctx.url_id,
        "http": {
            "method": req.method,
            "path": ctx.raw_path,
            "protocol": "HTTP/1.1",
            "sourceIp": "127.0.0.1",
            "userAgent": user_agent,
        },
        "requestId": ctx.request_id,
        "routeKey": "$default",
        "stage": "$default",
        "time": clf_time(now.as_secs() as i64),
        "timeEpoch": now.as_millis() as u64,
    });
    if let Some(access_key) = ctx.iam_access_key {
        request_context["authorizer"] = json!({
            "iam": {
                "accessKey": access_key,
                "accountId": ctx.account_id,
                "callerId": access_key,
                "cognitoIdentity": null,
                "principalOrgId": null,
                "userArn": format!("arn:aws:iam::{}:user/devcloud", ctx.account_id),
                "userId": access_key,
            }
        });
    }

    let mut event = json!({
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": ctx.raw_path,
        "rawQueryString": req.query,
        "headers": headers,
        "requestContext": request_context,
        "isBase64Encoded": false,
    });
    if !cookies.is_empty() {
        event["cookies"] = Value::Array(cookies);
    }
    if !query.is_empty() {
        let params: Map<String, Value> = query
            .into_iter()
            .map(|(k, v)| (k, Value::String(v.join(","))))
            .collect();
        event["queryStringParameters"] = Value::Object(params);
    }
    if !req.body.is_empty() {
        let content_type = req
            .headers
            .get("content-type")
            .map(String::as_str)
            .unwrap_or("");
        match std::str::from_utf8(&req.body) {
            Ok(text) if is_text(content_type) => event["body"] = Value::String(text.to_string()),
            _ => {
                event["body"] =
                    Value::String(base64::engine::general_purpose::STANDARD.encode(&req.body));
                event["isBase64Encoded"] = Value::Bool(true);
            }
        }
    }
    event
}

/// Content types Lambda passes as plain text rather than base64.
fn is_text(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    ct.starts_with("text/")
        || ct.ends_with("+json")
        || ct.ends_with("+xml")
        || matches!(
            ct.as_str(),
            "application/json"
                | "application/xml"
                | "application/javascript"
                | "application/x-www-form-urlencoded"
                | "application/graphql"
                | "application/yaml"
        )
}

/// `06/Oct/2026:12:34:56 +0000`, the payload 2.0 `requestContext.time`.
fn clf_time(secs: i64) -> String {
    let rfc = crate::time_fmt::rfc3339_from_unix(secs, 0);
    // `YYYY-MM-DDTHH:MM:SSZ`
    let (date, time) = rfc
        .trim_end_matches('Z')
        .split_once('T')
        .unwrap_or(("", ""));
    let mut parts = date.split('-');
    let (y, m, d) = (
        parts.next().unwrap_or(""),
        parts.next().unwrap_or("01"),
        parts.next().unwrap_or(""),
    );
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month = m
        .parse::<usize>()
        .ok()
        .and_then(|i| MONTHS.get(i.wrapping_sub(1)))
        .unwrap_or(&"Jan");
    format!("{d}/{month}/{y}:{time} +0000")
}

/// Turns the `Invoke` reply for a URL request into the HTTP response.
pub(crate) fn map_response(invoke: Reply) -> Reply {
    if invoke.status == 413 {
        return message_reply(413, "Request Too Long");
    }
    let function_error = invoke
        .headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Function-Error"));
    if invoke.status != 200 || function_error {
        return message_reply(502, "Internal Server Error");
    }
    let result: Value = match serde_json::from_slice(&invoke.body) {
        Ok(v) => v,
        Err(_) => return message_reply(502, "Internal Server Error"),
    };
    let Some(obj) = result.as_object().filter(|o| o.contains_key("statusCode")) else {
        // Any other JSON value is the body of a 200 JSON response.
        return Reply {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: invoke.body,
            content_type: "",
        };
    };
    let status = match obj.get("statusCode").and_then(Value::as_u64) {
        Some(s) if (100..=599).contains(&s) => s as u16,
        _ => return message_reply(502, "Internal Server Error"),
    };
    let mut headers = Vec::new();
    let mut has_content_type = false;
    let mut add = |k: &str, v: &Value| {
        let value = match v {
            Value::String(s) => s.clone(),
            Value::Null => return,
            other => other.to_string(),
        };
        // Header names/values reach the raw response head: refuse anything
        // that could split it.
        if k.is_empty() || k.contains([':', '\r', '\n', ' ']) || value.contains(['\r', '\n']) {
            return;
        }
        // devcloud frames the body itself (always with Content-Length).
        if ["content-length", "transfer-encoding", "connection"]
            .iter()
            .any(|h| k.eq_ignore_ascii_case(h))
        {
            return;
        }
        if k.eq_ignore_ascii_case("content-type") {
            has_content_type = true;
        }
        headers.push((k.to_string(), value));
    };
    if let Some(h) = obj.get("headers").and_then(Value::as_object) {
        for (k, v) in h {
            add(k, v);
        }
    }
    if let Some(h) = obj.get("multiValueHeaders").and_then(Value::as_object) {
        for (k, vs) in h {
            for v in vs.as_array().into_iter().flatten() {
                add(k, v);
            }
        }
    }
    if let Some(cookies) = obj.get("cookies").and_then(Value::as_array) {
        for c in cookies {
            add("Set-Cookie", c);
        }
    }
    let body = match obj.get("body") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => {
            if obj.get("isBase64Encoded") == Some(&Value::Bool(true)) {
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(b) => b,
                    Err(_) => return message_reply(502, "Internal Server Error"),
                }
            } else {
                s.clone().into_bytes()
            }
        }
        Some(other) => other.to_string().into_bytes(),
    };
    if !has_content_type && !body.is_empty() {
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    Reply {
        status,
        headers,
        body,
        // The response's own Content-Type travels in `headers`.
        content_type: "",
    }
}

fn origin_allowed(cors: &Cors, origin: &str) -> Option<String> {
    // The origin may be echoed into a response header: never one that could
    // split it (the request reader only splits head lines at CRLF).
    if origin.chars().any(char::is_control) {
        return None;
    }
    let origins = cors.allow_origins.as_deref().unwrap_or_default();
    if origins.iter().any(|o| o == "*") {
        // Credentials cannot be combined with a wildcard: echo the origin.
        return Some(if cors.allow_credentials == Some(true) {
            origin.to_string()
        } else {
            "*".to_string()
        });
    }
    origins
        .iter()
        .any(|o| o.eq_ignore_ascii_case(origin))
        .then(|| origin.to_string())
}

/// Answers a CORS preflight for a URL with a `Cors` configuration (Lambda
/// handles these itself; the function never sees them).
pub(crate) fn preflight(cors: &Cors, headers: &HashMap<String, String>) -> Reply {
    let mut reply = Reply::empty(200);
    let origin = headers.get("origin").map(String::as_str).unwrap_or("");
    let Some(allow_origin) = origin_allowed(cors, origin) else {
        return reply;
    };
    let method = headers
        .get("access-control-request-method")
        .map(String::as_str)
        .unwrap_or("");
    let methods = cors.allow_methods.as_deref().unwrap_or_default();
    if !methods
        .iter()
        .any(|m| m == "*" || m.eq_ignore_ascii_case(method))
    {
        return reply;
    }
    // With credentials, browsers take `*` literally (a method or header
    // named "*"), so a wildcard is answered with what was asked for. Both
    // values come from the request: only plain HTTP tokens are echoed.
    let credentials = cors.allow_credentials == Some(true);
    let wildcard = |list: &[String]| list.iter().any(|v| v == "*");
    let allow_methods = if credentials && wildcard(methods) {
        if !is_token_list(method) {
            return reply;
        }
        method.to_string()
    } else {
        methods.join(",")
    };
    let requested_headers = headers
        .get("access-control-request-headers")
        .map(|h| h.trim())
        .unwrap_or("");
    let allow_headers = match cors.allow_headers.as_deref().filter(|h| !h.is_empty()) {
        Some(h) if credentials && wildcard(h) => (!requested_headers.is_empty()
            && is_token_list(requested_headers))
        .then(|| requested_headers.to_string()),
        Some(h) => Some(h.join(",")),
        None => None,
    };
    reply
        .headers
        .push(("Access-Control-Allow-Origin".into(), allow_origin));
    reply
        .headers
        .push(("Access-Control-Allow-Methods".into(), allow_methods));
    if let Some(h) = allow_headers {
        reply
            .headers
            .push(("Access-Control-Allow-Headers".into(), h));
    }
    if credentials {
        reply
            .headers
            .push(("Access-Control-Allow-Credentials".into(), "true".into()));
    }
    if let Some(age) = cors.max_age {
        reply
            .headers
            .push(("Access-Control-Max-Age".into(), age.to_string()));
    }
    reply.headers.push((
        "Vary".into(),
        "Origin, Access-Control-Request-Method, Access-Control-Request-Headers".into(),
    ));
    reply
}

/// A comma-separated list of HTTP tokens (method or header names), safe to
/// echo into a response header.
fn is_token_list(value: &str) -> bool {
    !value.is_empty()
        && value.split(',').all(|item| {
            let item = item.trim();
            !item.is_empty()
                && item
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
        })
}

/// Applies a `Cors` configuration to a URL response: the configured headers
/// replace any `Access-Control-*` headers the function returned.
pub(crate) fn apply_cors(mut reply: Reply, cors: &Cors, origin: &str) -> Reply {
    reply
        .headers
        .retain(|(k, _)| !k.to_ascii_lowercase().starts_with("access-control-"));
    if origin.is_empty() {
        return reply;
    }
    let Some(allow_origin) = origin_allowed(cors, origin) else {
        return reply;
    };
    reply
        .headers
        .push(("Access-Control-Allow-Origin".into(), allow_origin));
    let credentials = cors.allow_credentials == Some(true);
    if credentials {
        reply
            .headers
            .push(("Access-Control-Allow-Credentials".into(), "true".into()));
    }
    if let Some(h) = cors.expose_headers.as_ref().filter(|h| !h.is_empty()) {
        // With credentials `*` is literal: expose the response's own headers.
        let expose = if credentials && h.iter().any(|v| v == "*") {
            let mut names: Vec<String> = reply.headers.iter().map(|(k, _)| k.clone()).collect();
            names.dedup();
            names.join(",")
        } else {
            h.join(",")
        };
        if !expose.is_empty() {
            reply
                .headers
                .push(("Access-Control-Expose-Headers".into(), expose));
        }
    }
    reply.headers.push(("Vary".into(), "Origin".into()));
    reply
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(path: &str, host: &str) -> Request {
        Request {
            method: "GET".into(),
            raw_path: path.into(),
            query: String::new(),
            headers: HashMap::from([("host".to_string(), host.to_string())]),
            body: Vec::new(),
        }
    }

    const ID: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn routes_host_and_path_forms() {
        let host = format!("{ID}.lambda-url.us-east-1.localhost:19010");
        assert_eq!(
            route(&req("/a/b", &host)),
            Some((UrlTarget::Id(ID.to_string()), "/a/b".to_string()))
        );
        assert_eq!(
            route(&req(&format!("/_url/{ID}/x%20y"), "127.0.0.1:19010")),
            Some((UrlTarget::Id(ID.to_string()), "/x%20y".to_string()))
        );
        assert_eq!(
            route(&req(&format!("/_url/{ID}"), "127.0.0.1")),
            Some((UrlTarget::Id(ID.to_string()), "/".to_string()))
        );
        assert_eq!(
            route(&req("/urls/my-fn/a/b", "127.0.0.1")),
            Some((UrlTarget::Function("my-fn".to_string()), "/a/b".to_string()))
        );
        assert_eq!(
            route(&req("/urls/my-fn", "127.0.0.1")),
            Some((UrlTarget::Function("my-fn".to_string()), "/".to_string()))
        );
        assert_eq!(
            route(&req("/2015-03-31/functions", "127.0.0.1:19010")),
            None
        );
        assert_eq!(
            route(&req("/", "short.lambda-url.us-east-1.localhost")),
            None
        );
    }

    #[test]
    fn url_ids_are_stable_per_account_region_and_function() {
        let id = url_id_for("000000000000", "us-east-1", "fn");
        assert_eq!(id, url_id_for("000000000000", "us-east-1", "fn"));
        assert!(is_url_id(&id), "{id}");
        assert_ne!(id, url_id_for("000000000000", "us-east-1", "fn2"));
        assert_ne!(id, url_id_for("000000000000", "eu-west-1", "fn"));
    }

    #[test]
    fn function_url_uses_the_endpoint_scheme_and_port() {
        assert_eq!(
            function_url("http://127.0.0.1:19010", ID, "us-east-1"),
            format!("http://{ID}.lambda-url.us-east-1.localhost:19010/")
        );
        assert_eq!(
            function_url("https://lambda.example", ID, "eu-west-1"),
            format!("https://{ID}.lambda-url.eu-west-1.localhost/")
        );
    }

    #[test]
    fn clf_time_format() {
        assert_eq!(clf_time(1_700_000_000), "14/Nov/2023:22:13:20 +0000");
    }

    #[test]
    fn caller_access_key_parses_the_credential_scope() {
        assert_eq!(
            caller_access_key("AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=ab"),
            "AKID"
        );
        assert_eq!(caller_access_key(""), "");
    }

    #[test]
    fn response_headers_cannot_split_the_response() {
        let invoke = Reply::json(
            200,
            &json!({"statusCode": 200, "headers": {"x-ok": "1", "x-bad": "a\r\nInjected: 1", "bad name": "v"}}),
        );
        let out = map_response(invoke);
        assert_eq!(out.headers, vec![("x-ok".to_string(), "1".to_string())]);
    }
}
