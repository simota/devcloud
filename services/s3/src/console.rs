//! Web console for the standalone image: an embedded single-page app plus a small
//! JSON API on its own port. Reads use the introspection API and mutations go
//! through the S3 provider protocol in-process, never directly to storage.
//!
//! Browser-origin protection: every mutation is PUT or DELETE and `OPTIONS` is
//! refused, so a cross-site page cannot get past the CORS preflight; the Host
//! allowlist stops DNS rebinding. Strict auth mode adds HTTP Basic, using the
//! configured access key and secret.

use std::sync::{Arc, Mutex};

use include_dir::{include_dir, Dir};
use tokio::net::{TcpListener, TcpStream};

use crate::http::{
    constant_time_eq, is_transient_accept_error, percent_decode, read_request, route, tag_text_in,
    write_response, AuthConfig, Request, Response,
};
use crate::percent::aws_percent_encode;
use crate::store::FileBucketStore;
use crate::validation::valid_bucket_name;

static UI: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets/console");

const SPA_CSP: &str =
    "default-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const API_CSP: &str =
    "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox";

#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub auth: AuthConfig,
    /// Extra Host names beyond loopback, IPs, and single-label names; `*` allows any.
    pub allowed_hosts: Vec<String>,
}

/// Runs the console accept loop until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    store: Arc<Mutex<FileBucketStore>>,
    config: ConsoleConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    let config = Arc::new(config);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(e) if is_transient_accept_error(&e) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, store, config).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    store: Arc<Mutex<FileBucketStore>>,
    config: Arc<ConsoleConfig>,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = {
        let store = store.lock().unwrap();
        dispatch(&store, &request, &config)
    };
    write_response(&mut stream, response).await
}

pub fn dispatch(store: &FileBucketStore, req: &Request, config: &ConsoleConfig) -> Response {
    let mut response = dispatch_unsecured(store, req, config);
    let policy = if req.path.starts_with("/api/") {
        API_CSP
    } else {
        SPA_CSP
    };
    response
        .headers
        .insert("Content-Security-Policy".to_string(), policy.to_string());
    response
        .headers
        .insert("X-Content-Type-Options".to_string(), "nosniff".to_string());
    response
}

fn dispatch_unsecured(store: &FileBucketStore, req: &Request, config: &ConsoleConfig) -> Response {
    if !host_allowed(req.header("host"), &config.allowed_hosts) {
        return json_error(
            403,
            "Host denied. Add the hostname to DEVCLOUD_S3_CONSOLE_ALLOWED_HOSTS.",
        );
    }
    if !authorized(req, &config.auth) {
        let mut response = json_error(401, "Unauthorized");
        response.headers.insert(
            "WWW-Authenticate".to_string(),
            "Basic realm=\"devcloud-s3\"".to_string(),
        );
        return response;
    }
    match req.path.strip_prefix("/api/") {
        Some(path) => handle_api(store, req, config, path),
        None => serve_asset(req),
    }
}

fn authorized(req: &Request, auth: &AuthConfig) -> bool {
    if !auth.auth_mode.eq_ignore_ascii_case("strict") {
        return true;
    }
    let credentials = req
        .header("authorization")
        .strip_prefix("Basic ")
        .and_then(crate::base64::std_decode)
        .and_then(|decoded| String::from_utf8(decoded).ok());
    let Some((user, password)) = credentials.as_deref().and_then(|c| c.split_once(':')) else {
        return false;
    };
    let user_ok = constant_time_eq(user.as_bytes(), auth.access_key_id.as_bytes());
    let password_ok = constant_time_eq(password.as_bytes(), auth.secret_access_key.as_bytes());
    user_ok & password_ok
}

fn handle_api(
    store: &FileBucketStore,
    req: &Request,
    config: &ConsoleConfig,
    path: &str,
) -> Response {
    if path == "info" {
        return match req.method.as_str() {
            "GET" => json_response(
                200,
                &serde_json::json!({
                    "authMode": if config.auth.auth_mode.eq_ignore_ascii_case("strict") {
                        "strict"
                    } else {
                        "relaxed"
                    },
                    "region": if config.auth.region.is_empty() {
                        "us-east-1"
                    } else {
                        config.auth.region.as_str()
                    },
                }),
            ),
            _ => method_not_allowed("GET"),
        };
    }
    if path == "buckets" {
        return match req.method.as_str() {
            "GET" => introspect(store, "/_introspect/buckets"),
            _ => method_not_allowed("GET"),
        };
    }
    let Some(rest) = path.strip_prefix("buckets/") else {
        return json_error(404, "not found");
    };
    let (raw_bucket, suffix) = rest.split_once('/').unwrap_or((rest, ""));
    let bucket = percent_decode(raw_bucket);
    if !valid_bucket_name(&bucket) {
        return json_error(400, "Bucket names must be valid S3 bucket names.");
    }
    if suffix.is_empty() && !rest.ends_with('/') {
        return match req.method.as_str() {
            "PUT" | "DELETE" => provider(store, &req.method, &format!("/{bucket}"), req),
            _ => method_not_allowed("PUT, DELETE"),
        };
    }
    if suffix == "objects" {
        if req.method != "GET" {
            return method_not_allowed("GET");
        }
        let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
        return introspect(
            store,
            &format!(
                "/_introspect/buckets/{bucket}/objects?prefix={}",
                aws_percent_encode(prefix, "")
            ),
        );
    }
    if suffix != "object" {
        return json_error(404, "not found");
    }
    // The key travels in the query: browsers resolve `.` and `..` path segments
    // (even as `%2e`), which would retarget the request at a different object.
    let Some(key) = req.query.get("key").filter(|k| !k.is_empty()) else {
        return json_error(400, "The key parameter is required.");
    };
    let target = format!("/{bucket}/{}", aws_percent_encode(key, "/"));
    match req.method.as_str() {
        "GET" => download(store, &target, key, req),
        "PUT" | "DELETE" => provider(store, &req.method, &target, req),
        _ => method_not_allowed("GET, PUT, DELETE"),
    }
}

fn introspect(store: &FileBucketStore, target: &str) -> Response {
    crate::introspect::handle_introspect(store, &Request::new("GET", target, Vec::new()))
}

/// Forwards to the S3 router with relaxed auth: the console has already
/// authorized the caller, and it holds no SigV4 signature to present.
fn provider(store: &FileBucketStore, method: &str, target: &str, req: &Request) -> Response {
    let mut forwarded = Request::new(method, target, req.body.clone());
    if !req.header("content-type").is_empty() {
        forwarded.headers.insert(
            "content-type".to_string(),
            req.header("content-type").to_string(),
        );
    }
    json_errors(route(store, &forwarded))
}

fn download(store: &FileBucketStore, target: &str, key: &str, req: &Request) -> Response {
    let mut response = provider(store, "GET", target, req);
    if response.status == 200 {
        // Always an attachment: a stored HTML object must never render on the
        // console's origin.
        let filename = key.rsplit('/').next().unwrap_or(key);
        response.headers.insert(
            "Content-Disposition".to_string(),
            format!(
                "attachment; filename*=UTF-8''{}",
                aws_percent_encode(filename, "")
            ),
        );
    }
    response
}

fn json_errors(response: Response) -> Response {
    if response.status < 400 {
        return response;
    }
    let body = String::from_utf8_lossy(&response.body);
    let code = tag_text_in(&body, "Code").unwrap_or_default();
    let message = tag_text_in(&body, "Message").unwrap_or_else(|| "request failed".to_string());
    json_response(
        response.status,
        &serde_json::json!({ "error": message, "code": code }),
    )
}

fn serve_asset(req: &Request) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    let name = if req.path == "/" {
        "index.html"
    } else {
        req.path.trim_start_matches('/')
    };
    let Some(file) = UI.get_file(name) else {
        return json_error(404, "not found");
    };
    let content_type = match name.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        _ => "application/octet-stream",
    };
    let mut response = Response::with_body(200, file.contents().to_vec());
    response
        .headers
        .insert("Content-Type".to_string(), content_type.to_string());
    response
}

fn method_not_allowed(allow: &str) -> Response {
    let mut response = json_error(405, "method not allowed");
    response
        .headers
        .insert("Allow".to_string(), allow.to_string());
    response
}

fn json_response(status: u16, value: &serde_json::Value) -> Response {
    let mut response = Response::with_body(status, value.to_string().into_bytes());
    response
        .headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    response
}

fn json_error(status: u16, message: &str) -> Response {
    json_response(status, &serde_json::json!({ "error": message }))
}

/// Same policy as the mailbox console: loopback, IP literals, `.localhost`,
/// `host.docker.internal`, and single-label names pass; a dotted public-looking
/// name must be listed, which defeats DNS rebinding.
pub fn host_allowed(value: &str, extra: &[String]) -> bool {
    if value.is_empty()
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
        || value.contains(['/', '\\', '@', '?', '#'])
    {
        return false;
    }
    let host = if let Some(rest) = value.strip_prefix('[') {
        let Some((ip, suffix)) = rest.split_once(']') else {
            return false;
        };
        if ip.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        if !suffix.is_empty() && !suffix.strip_prefix(':').is_some_and(valid_port) {
            return false;
        }
        ip
    } else if value.parse::<std::net::IpAddr>().is_ok() {
        value
    } else if let Some((host, port)) = value.rsplit_once(':') {
        if host.contains(':') || !valid_port(port) {
            return false;
        }
        host
    } else {
        value
    };
    if host.is_empty()
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
    {
        return false;
    }
    let host = host.to_ascii_lowercase();
    extra
        .iter()
        .any(|s| s == "*" || s.eq_ignore_ascii_case(&host))
        || host.parse::<std::net::IpAddr>().is_ok()
        || host == "localhost"
        || host.ends_with(".localhost")
        || host == "host.docker.internal"
        || !host.contains('.')
}

fn valid_port(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) && value.parse::<u16>().is_ok()
}
