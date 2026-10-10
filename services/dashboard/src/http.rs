//! Minimal HTTP/1.1 socket server for the dashboard, mirroring the hand-rolled
//! pattern in `services/sqs/src/http.rs` (plain tokio reader/writer, tiny
//! dependency surface — no web framework).
//!
//! The router dispatches:
//!   - `/`                                -> the plain service index HTML
//!   - `/dashboard`, `/dashboard/...`     -> embedded SPA (cache/redirect/fallback)
//!   - `/mail`,`/s3`,`/gcs`,`/dynamodb`,`/bigquery`,`/redis` -> 301 to /dashboard/<svc>
//!   - `/api/services`, `/api/dashboard/services` -> the registry
//!   - `/api/sqs/...`                     -> the SQS handler (forwards to the service)
//!
//! Each handler is async because the `/api/*` handlers forward over the network.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::config::Config;
use crate::{
    applicationautoscaling, assets, bigquery, cloudrun, dynamodb, events, gcs, lambda, mail,
    pubsub, redis, redshift, s3, services, sqs,
};
pub use devcloud_hostguard::trusted_host;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// A parsed inbound HTTP request.
pub struct Request {
    pub method: String,
    /// Decoded path component (no query string), e.g. `/api/sqs/queues`.
    pub path: String,
    /// Raw escaped path exactly as received (used for percent-encoded segments,
    /// mirroring legacy `r.URL.EscapedPath()` usage in the dashboard handlers).
    pub raw_path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }
}

/// A rendered HTTP response a handler returns. Status + headers + body bytes.
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Response {
            status,
            headers: vec![("Content-Type".to_string(), content_type.to_string())],
            body,
        }
    }

    /// A JSON response. `value` is any serde-serializable value; serialization
    /// failure degrades to a 500 with an empty body (the handlers never produce
    /// unserializable values, so this is a defensive floor).
    pub fn json<T: serde::Serialize>(status: u16, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(body) => Response::new(status, "application/json", body),
            Err(_) => Response::new(500, "application/json", b"{}".to_vec()),
        }
    }

    /// A `text/plain` error body matching legacy `http.Error` (which appends a
    /// trailing newline).
    pub fn text_error(status: u16, message: &str) -> Self {
        Response::new(
            status,
            "text/plain; charset=utf-8",
            format!("{message}\n").into_bytes(),
        )
    }

    pub fn redirect(status: u16, location: &str) -> Self {
        Response {
            status,
            headers: vec![("Location".to_string(), location.to_string())],
            body: Vec::new(),
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn method_not_allowed(allow: &str) -> Self {
        Response::text_error(405, "method not allowed").header("Allow", allow)
    }
}

/// Runs the accept loop until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    config: Arc<Config>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    // Out of descriptors, or a peer gone before the accept:
                    // back off and keep serving instead of stopping (which
                    // would take every devcloud service down with it).
                    Err(e) => {
                        // Only transient errors; a broken listener still stops.
                        let transient = matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::Interrupted
                                | std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                        ) || matches!(e.raw_os_error(), Some(12 | 23 | 24)); // ENOMEM, ENFILE, EMFILE
                        if !transient {
                            return Err(e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, config).await;
                });
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, config: Arc<Config>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    // `/api/events` is a WebSocket upgrade, not a request/response route: hijack
    // the stream and hand it to the events proxy (it never returns a Response).
    if events::is_events_upgrade(&request) {
        // WebSockets are not subject to CORS, and the events carry mail
        // metadata and resource activity: the same Host guard as `route`,
        // plus the Origin check every cross-site page would otherwise pass.
        let host = request.header("host");
        let origin = request.header("origin");
        let allowed = trusted_host(host)
            && (trusted_origin(origin, request.header("sec-fetch-site"))
                || same_origin(origin, host));
        if !allowed {
            let refused =
                Response::text_error(403, "cross-site WebSocket connections are not allowed");
            return write_response(&mut stream, refused).await;
        }
        events::handle(stream, &request, Arc::clone(&config)).await;
        return Ok(());
    }
    let response = route(&config, &request).await;
    write_response(&mut stream, response).await
}

/// Routes a request to its handler. Public so integration tests can drive the
/// router directly without a socket.
pub async fn route(config: &Config, req: &Request) -> Response {
    let path = req.path.as_str();
    if !trusted_host(req.header("host")) {
        return Response::text_error(
            403,
            "untrusted Host header (DNS rebinding guard); add the name to DEVCLOUD_ALLOWED_HOSTS to allow it",
        );
    }
    // CSRF guard for every API mutation: a cross-site page can send a
    // "simple" POST (text/plain or form body) without any CORS preflight.
    if path.starts_with("/api/")
        && !matches!(req.method.as_str(), "GET" | "HEAD" | "OPTIONS")
        && !trusted_origin(req.header("origin"), req.header("sec-fetch-site"))
        && !same_origin(req.header("origin"), req.header("host"))
    {
        return Response::text_error(403, "cross-origin requests are not allowed");
    }

    if path == "/" {
        return index_response(req);
    }
    if path == "/dashboard" || path.starts_with("/dashboard/") {
        return assets::serve(req);
    }
    match path {
        "/mail" => return legacy_redirect(req, "/dashboard/mail"),
        "/s3" => return legacy_redirect(req, "/dashboard/s3"),
        "/gcs" => return legacy_redirect(req, "/dashboard/gcs"),
        "/dynamodb" => return legacy_redirect(req, "/dashboard/dynamodb"),
        "/bigquery" => return legacy_redirect(req, "/dashboard/bigquery"),
        "/redis" => return legacy_redirect(req, "/dashboard/redis"),
        "/api/services" | "/api/dashboard/services" => return services::handle(config, req),
        _ => {}
    }
    if path == "/api/sqs/status" {
        return sqs::handle_status(config, req).await;
    }
    if path == "/api/sqs/queues" {
        return sqs::handle_queues(config, req).await;
    }
    if path.starts_with("/api/sqs/queues/") {
        return sqs::handle_queue(config, req).await;
    }

    // Mail — legacy `/api/messages` paths (NOT `/api/mail/*`).
    if path == "/api/messages" {
        return mail::handle_messages(config, req).await;
    }
    if path.starts_with("/api/messages/") {
        return mail::handle_message(config, req).await;
    }

    // S3.
    if path == "/api/s3/status" {
        return s3::handle_status(config, req).await;
    }
    if path == "/api/s3/buckets" {
        return s3::handle_buckets(config, req).await;
    }
    if path.starts_with("/api/s3/buckets/") {
        return s3::handle_bucket(config, req).await;
    }

    // GCS.
    if path == "/api/gcs/status" {
        return gcs::handle_status(config, req).await;
    }
    if path == "/api/gcs/buckets" {
        return gcs::handle_buckets(config, req).await;
    }
    if path.starts_with("/api/gcs/buckets/") {
        return gcs::handle_bucket(config, req).await;
    }
    if path == "/api/gcs/uploads" || path == "/api/gcs/upload-sessions" {
        return gcs::handle_upload_sessions(config, req).await;
    }
    if path.starts_with("/api/gcs/uploads/") {
        return gcs::handle_upload_session(config, req).await;
    }

    // DynamoDB.
    if path == "/api/dynamodb/status" {
        return dynamodb::handle_status(config, req).await;
    }
    if path == "/api/dynamodb/tables" {
        return dynamodb::handle_tables(config, req).await;
    }
    if path.starts_with("/api/dynamodb/tables/") {
        return dynamodb::handle_table(config, req).await;
    }

    // BigQuery.
    if path == "/api/bigquery/status" {
        return bigquery::handle_status(config, req).await;
    }
    if path == "/api/bigquery/projects" {
        return bigquery::handle_projects(config, req).await;
    }
    if path.starts_with("/api/bigquery/projects/") {
        return bigquery::handle_project_resource(config, req).await;
    }

    // Redshift.
    if path == "/api/redshift/status" {
        return redshift::handle_status(config, req).await;
    }
    if path == "/api/redshift/clusters" {
        return redshift::handle_clusters(config, req).await;
    }
    if path == "/api/redshift/catalog" {
        return redshift::handle_catalog(config, req).await;
    }
    if path == "/api/redshift/statements" {
        return redshift::handle_statements(config, req).await;
    }
    if path.starts_with("/api/redshift/tables/") {
        return redshift::handle_table(config, req).await;
    }
    if path == "/api/redshift/query" {
        return redshift::handle_query(config, req).await;
    }

    // Redis.
    if path == "/api/redis/status" {
        return redis::handle_status(config, req).await;
    }
    if path == "/api/redis/keys" {
        return redis::handle_keys(config, req).await;
    }
    if path.starts_with("/api/redis/keys/") {
        return redis::handle_key(config, req).await;
    }
    if path == "/api/redis/command" {
        return redis::handle_command(config, req).await;
    }
    if path == "/api/redis/select-db" {
        return redis::handle_select_db(config, req).await;
    }

    // Pub/Sub.
    if path == "/api/pubsub/health" || path == "/api/pubsub/status" {
        return pubsub::handle_status(config, req).await;
    }
    if path == "/api/pubsub/projects" {
        return pubsub::handle_projects(config, req).await;
    }
    if path == "/api/pubsub/topics" {
        return pubsub::handle_topics(config, req).await;
    }
    if path.starts_with("/api/pubsub/topics/") {
        return pubsub::handle_topic(config, req).await;
    }
    if path == "/api/pubsub/subscriptions" {
        return pubsub::handle_subscriptions(config, req).await;
    }
    if path.starts_with("/api/pubsub/subscriptions/") {
        return pubsub::handle_subscription(config, req).await;
    }
    if path.starts_with("/api/pubsub/messages/") {
        return pubsub::handle_message(config, req).await;
    }

    // Application Auto Scaling.
    if path == "/api/applicationautoscaling/status" {
        return applicationautoscaling::handle_status(config, req).await;
    }
    if path == "/api/applicationautoscaling/scalable-targets" {
        return applicationautoscaling::handle_scalable_targets(config, req).await;
    }
    if path == "/api/applicationautoscaling/scaling-policies" {
        return applicationautoscaling::handle_scaling_policies(config, req).await;
    }
    if path == "/api/applicationautoscaling/scheduled-actions" {
        return applicationautoscaling::handle_scheduled_actions(config, req).await;
    }

    // Lambda.
    if path == "/api/lambda/status" {
        return lambda::handle_status(config, req).await;
    }
    if path == "/api/lambda/functions" {
        return lambda::handle_functions(config, req).await;
    }
    if path == "/api/lambda/invocations" {
        return lambda::handle_invocations(config, req).await;
    }
    if path.starts_with("/api/lambda/functions/") && path.ends_with("/invoke") {
        return lambda::handle_invoke(config, req).await;
    }

    // Cloud Run.
    if path == "/api/cloudrun/status" {
        return cloudrun::handle_status(config, req).await;
    }
    if path == "/api/cloudrun/services" {
        return cloudrun::handle_services(config, req).await;
    }
    if path == "/api/cloudrun/instances" {
        return cloudrun::handle_instances(config, req).await;
    }
    if path.starts_with("/api/cloudrun/services/") {
        return cloudrun::handle_service(config, req).await;
    }

    Response::text_error(404, "404 page not found")
}

fn index_response(req: &Request) -> Response {
    if req.method != "GET" && req.method != "HEAD" {
        return Response::method_not_allowed("GET, HEAD");
    }
    Response::new(
        200,
        "text/html; charset=utf-8",
        assets::SERVICE_INDEX_HTML.as_bytes().to_vec(),
    )
}

fn legacy_redirect(req: &Request, target: &str) -> Response {
    if req.method != "GET" && req.method != "HEAD" {
        return Response::method_not_allowed("GET, HEAD");
    }
    Response::redirect(301, target)
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Ok(None);
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("");
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let path = percent_decode(&raw_path);

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Ok(None);
    }

    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);

    Ok(Some(Request {
        method,
        path,
        raw_path,
        query,
        headers,
        body,
    }))
}

async fn write_response(stream: &mut TcpStream, resp: Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    head.push_str("Server: devcloud-dashboard\r\n");
    let mut have_content_type = false;
    for (name, value) in &resp.headers {
        if name.eq_ignore_ascii_case("content-type") {
            have_content_type = true;
        }
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if !have_content_type && !resp.body.is_empty() {
        head.push_str("Content-Type: application/octet-stream\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    if !resp.body.is_empty() {
        stream.write_all(&resp.body).await?;
    }
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Decodes `%XX` escapes in a path. Used to normalize the request path for
/// routing; percent-encoded path segments (e.g. URL-encoded queue names) are
/// re-derived from `raw_path` by the handlers via [`path_segment_decode`].
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Decodes a single percent-encoded path segment, rejecting traversal and
/// embedded separators — mirrors legacy `dashboardPathParts`
/// (`internal/dashboard/helpers.rs`). Returns `None` for an invalid segment.
pub fn path_segment_decode(raw: &str) -> Option<String> {
    let decoded = percent_decode(raw);
    if decoded == "." || decoded == ".." || decoded.contains('/') || decoded.contains('\\') {
        return None;
    }
    Some(decoded)
}

/// CSRF guard for state-changing / code-executing requests. AWS SDKs, CLIs and
/// other non-browser clients send no `Origin`; a browser always does on a
/// cross-origin POST — including "simple" `text/plain` ones that CORS never
/// blocks from being sent. Only loopback origins (local front-ends, the
/// devcloud dashboard) are trusted; a DNS-rebinding page keeps its own
/// non-loopback origin and is refused too.
pub fn trusted_origin(origin: &str, sec_fetch_site: &str) -> bool {
    if origin.is_empty() {
        return !sec_fetch_site.eq_ignore_ascii_case("cross-site");
    }
    let Some((scheme, rest)) = origin.split_once("://") else {
        return false; // includes the opaque `null` origin
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let host = if let Some(v6) = rest.strip_prefix('[') {
        match v6.split_once(']') {
            Some((h, _)) => h.to_string(),
            None => return false,
        }
    } else {
        rest.rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(rest)
            .to_ascii_lowercase()
    };
    host == "localhost" || host == "127.0.0.1" || host == "::1" || host.ends_with(".localhost")
}

/// The SPA opened through a non-loopback address (a LAN IP, a compose
/// service name): its `Origin` names the very host it is served from, which
/// the Host guard has already accepted.
fn same_origin(origin: &str, host: &str) -> bool {
    let Some((scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    (scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
        && !host.is_empty()
        && authority.eq_ignore_ascii_case(host.trim())
        && trusted_host(host)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn api_mutations_and_rebinding_hosts_are_refused() {
        let cfg = Config::default();
        let with = |method: &str, path: &str, headers: &[(&str, &str)]| {
            let mut r = req(method, path);
            for (k, v) in headers {
                r.headers.insert(k.to_string(), v.to_string());
            }
            r
        };
        // A cross-site "simple" POST never needs a preflight.
        let csrf = with(
            "POST",
            "/api/dynamodb/tables",
            &[
                ("origin", "https://evil.example.com"),
                ("sec-fetch-site", "cross-site"),
            ],
        );
        assert_eq!(route(&cfg, &csrf).await.status, 403);
        let no_origin_cross_site = with(
            "POST",
            "/api/s3/buckets",
            &[("sec-fetch-site", "cross-site")],
        );
        assert_eq!(route(&cfg, &no_origin_cross_site).await.status, 403);
        // Reads stay open to any origin; the SPA itself is same-origin.
        let read = with(
            "GET",
            "/api/services",
            &[("origin", "https://evil.example.com")],
        );
        assert_ne!(route(&cfg, &read).await.status, 403);
        // A rebinding page keeps its own Host, on reads too.
        let rebound = with(
            "GET",
            "/api/services",
            &[("host", "evil.example.com:18025")],
        );
        assert_eq!(route(&cfg, &rebound).await.status, 403);
        let local = with("GET", "/api/services", &[("host", "127.0.0.1:18025")]);
        assert_eq!(route(&cfg, &local).await.status, 200);
        // The SPA opened from a LAN address posts with its own origin.
        assert!(same_origin(
            "http://192.168.1.20:18025",
            "192.168.1.20:18025"
        ));
        assert!(!same_origin(
            "http://192.168.1.20:18025",
            "192.168.1.21:18025"
        ));
        assert!(!same_origin("http://evil.example.com", "evil.example.com"));
        assert!(!same_origin("null", "devcloud:18025"));
    }

    fn req(method: &str, path: &str) -> Request {
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_path: path.to_string(),
            query: String::new(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn index_serves_html() {
        let cfg = Config::default();
        let resp = route(&cfg, &req("GET", "/")).await;
        assert_eq!(resp.status, 200);
        assert!(content_type(&resp).starts_with("text/html"));
    }

    #[tokio::test]
    async fn legacy_paths_redirect_301() {
        let cfg = Config::default();
        for (path, target) in [
            ("/mail", "/dashboard/mail"),
            ("/s3", "/dashboard/s3"),
            ("/gcs", "/dashboard/gcs"),
            ("/dynamodb", "/dashboard/dynamodb"),
            ("/bigquery", "/dashboard/bigquery"),
            ("/redis", "/dashboard/redis"),
        ] {
            let resp = route(&cfg, &req("GET", path)).await;
            assert_eq!(resp.status, 301, "{path}");
            assert_eq!(location(&resp), target);
        }
    }

    #[tokio::test]
    async fn dashboard_root_serves_index_no_cache() {
        let cfg = Config::default();
        let resp = route(&cfg, &req("GET", "/dashboard/")).await;
        assert_eq!(resp.status, 200);
        assert_eq!(cache_control(&resp), "no-cache");
        assert!(content_type(&resp).starts_with("text/html"));
    }

    #[tokio::test]
    async fn dashboard_client_route_falls_back_to_index() {
        let cfg = Config::default();
        let resp = route(&cfg, &req("GET", "/dashboard/sqs")).await;
        assert_eq!(resp.status, 200);
        assert_eq!(cache_control(&resp), "no-cache");
        assert!(content_type(&resp).starts_with("text/html"));
    }

    #[tokio::test]
    async fn dashboard_bare_redirects_to_slash() {
        let cfg = Config::default();
        let resp = route(&cfg, &req("GET", "/dashboard")).await;
        assert_eq!(resp.status, 301);
        assert_eq!(location(&resp), "/dashboard/");
    }

    #[tokio::test]
    async fn unknown_asset_under_assets_is_404() {
        let cfg = Config::default();
        let resp = route(&cfg, &req("GET", "/dashboard/assets/does-not-exist.js")).await;
        assert_eq!(resp.status, 404);
    }

    fn header<'a>(resp: &'a Response, name: &str) -> &'a str {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }
    fn content_type(resp: &Response) -> &str {
        header(resp, "Content-Type")
    }
    fn cache_control(resp: &Response) -> &str {
        header(resp, "Cache-Control")
    }
    fn location(resp: &Response) -> &str {
        header(resp, "Location")
    }
}
