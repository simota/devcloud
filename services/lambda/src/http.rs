//! HTTP/1.1 front-end for the Lambda REST API (restJson1).
//!
//! Hand-rolled on plain tokio like the other AWS-protocol crates: one request
//! per connection (`Connection: close`), SigV4 verified per auth mode, then the
//! versioned path (`/2015-03-31/functions/...`, `/2017-03-31/tags/...`) is
//! routed to the [`Server`]. Path segments are percent-decoded individually so
//! ARNs (`arn%3Aaws%3Alambda%3A...`) resolve like plain names.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::server::{Reply, Server};
use crate::sigv4::{verify_signature, Credentials, SignedRequest};

const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Base64 inflates a 50 MiB package to ~67 MiB of JSON.
const MAX_BODY_BYTES: usize = 72 * 1024 * 1024;

/// A parsed HTTP request (raw path kept for SigV4, lowercase header keys).
pub struct Request {
    pub method: String,
    pub raw_path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    // Tracked, not detached: on shutdown every in-flight request is aborted
    // and awaited, so `runtime::Cleanup` kills its handler process group.
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    // Out of descriptors, or a peer gone before the accept:
                    // back off and keep serving instead of stopping (which
                    // would take every devcloud service down with it).
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                while connections.try_join_next().is_some() {}
                let server = Arc::clone(&server);
                connections.spawn(async move {
                    let _ = handle_conn(stream, server).await;
                });
            }
        }
    };
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    server.shutdown().await;
    result
}

async fn handle_conn(mut stream: TcpStream, server: Arc<Server>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(Ok(req))) => req,
        // An unreadable body is answered, never processed as a shorter one.
        Ok(Some(Err(reply))) => return write_reply(&mut stream, "POST", reply).await,
        _ => return Ok(()),
    };
    let reply = process(&server, &request).await;
    write_reply(&mut stream, &request.method, reply).await
}

/// Records one request header line. A repeated header keeps every value,
/// combined as HTTP allows: `Cookie` lines with `; `, others with `,` (also
/// how SigV4 canonicalises them and how Lambda function URL events carry
/// them). A repeated `Content-Length` thereby stops parsing as one number.
fn add_header(headers: &mut HashMap<String, String>, name: &str, value: &str) {
    let name = name.trim().to_ascii_lowercase();
    let value = value.trim();
    match headers.get_mut(&name) {
        Some(existing) => {
            existing.push_str(if name == "cookie" { "; " } else { "," });
            existing.push_str(value);
        }
        None => {
            headers.insert(name, value.to_string());
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Result<Request, Reply>>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
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
    let mut parts = lines.next().unwrap_or("").split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/");
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            add_header(&mut headers, k, v);
        }
    }
    let leftover = buf[header_end + 4..].to_vec();
    let body = match crate::body::read_body(stream, leftover, &headers, MAX_BODY_BYTES).await? {
        Ok(b) => b,
        Err(e) => return Ok(Some(Err(body_error_reply(e)))),
    };
    Ok(Some(Ok(Request {
        method,
        raw_path,
        query,
        headers,
        body,
    })))
}

async fn write_reply<W: AsyncWrite + Unpin>(
    stream: &mut W,
    method: &str,
    reply: Reply,
) -> std::io::Result<()> {
    let mut head = format!("HTTP/1.1 {} {}\r\n", reply.status, reason(reply.status));
    head.push_str("Server: devcloud-lambda\r\n");
    // An empty `content_type` means the reply carries its own Content-Type
    // header (function URL responses).
    if !reply.body.is_empty() && !reply.content_type.is_empty() {
        head.push_str(&format!("Content-Type: {}\r\n", reply.content_type));
    }
    for (k, v) in &reply.headers {
        // Last line of defence against header injection: a line break in a
        // name or value would end the header early.
        if k.contains(['\r', '\n', '\0', ':']) || v.contains(['\r', '\n', '\0']) {
            continue;
        }
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    // A 204 carries no body and must not declare one (RFC 9110 §8.6).
    if reply.status != 204 {
        head.push_str(&format!("Content-Length: {}\r\n", reply.body.len()));
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    if method != "HEAD" && reply.status != 204 {
        stream.write_all(&reply.body).await?;
    }
    stream.flush().await
}

/// Full request pipeline (gate → SigV4 → route). Public for in-process tests.
pub async fn process(server: &Arc<Server>, req: &Request) -> Reply {
    if !trusted_host(req.header("host")) {
        return untrusted_host();
    }
    if server.load_err().is_some() {
        return Reply::error(500, "ServiceException", "failed to load lambda state");
    }
    // Function URLs are meant to be called from anywhere, browsers included:
    // they bypass the API's CSRF guard and SigV4 check and apply the URL's
    // own AuthType instead.
    if let Some((target, path)) = crate::function_url::route(req) {
        return server.serve_function_url(req, &target, &path).await;
    }
    let segments: Vec<String> = match req
        .raw_path
        .trim_start_matches('/')
        .split('/')
        .map(percent_decode)
        .collect::<Option<Vec<_>>>()
    {
        Some(s) => s,
        None => return Reply::error(400, "InvalidRequestContentException", "malformed path"),
    };
    let seg: Vec<&str> = segments.iter().map(String::as_str).collect();
    let query = parse_query(&req.query);

    // devcloud-only surface: unsigned and read-only, like the presigned URL
    // AWS hands out as `Code.Location`.
    match (req.method.as_str(), seg.as_slice()) {
        ("GET", ["_devcloud", "functions", name, "code.zip"]) => {
            let (name, sha) = (name.to_string(), query.get("CodeSha256").cloned());
            return blocking(server, move |s| s.code_package(&name, sha.as_deref())).await;
        }
        _ => {}
    }

    if !matches!(req.method.as_str(), "GET" | "HEAD")
        && !trusted_origin(req.header("origin"), req.header("sec-fetch-site"))
    {
        return Reply::error(
            403,
            "AccessDeniedException",
            "cross-origin requests to the devcloud Lambda API are not allowed",
        );
    }

    if let Err(reply) = check_signature(server, req) {
        return reply;
    }

    // Invocation log tails are handler output: signed like the API (the
    // dashboard signs its requests in strict mode).
    if let ("GET", ["_introspect", "invocations"]) = (req.method.as_str(), seg.as_slice()) {
        return server.introspect_invocations();
    }

    let m = req.method.as_str();
    match seg.as_slice() {
        ["2015-03-31", "functions"] | ["2015-03-31", "functions", ""] => match m {
            "GET" => server.list_functions(&query),
            "POST" => {
                let body = req.body.clone();
                blocking(server, move |s| s.create_function(&body)).await
            }
            _ => method_not_allowed(),
        },
        ["2015-03-31", "functions", id] => match m {
            "GET" => server.get_function(id, &query),
            "DELETE" => server.delete_function(id, &query),
            _ => method_not_allowed(),
        },
        ["2015-03-31", "functions", id, "configuration"] => match m {
            "GET" => server.get_function_configuration(id, &query),
            "PUT" => server.update_function_configuration(id, &req.body),
            _ => method_not_allowed(),
        },
        ["2015-03-31", "functions", id, "code"] => match m {
            "PUT" => {
                let (id, body) = (id.to_string(), req.body.clone());
                blocking(server, move |s| s.update_function_code(&id, &body)).await
            }
            _ => method_not_allowed(),
        },
        ["2015-03-31", "functions", id, "invocations"] => match m {
            "POST" => {
                server
                    .invoke(
                        id,
                        &query,
                        req.header("x-amz-invocation-type"),
                        req.header("x-amz-log-type"),
                        &req.body,
                    )
                    .await
            }
            _ => method_not_allowed(),
        },
        ["2021-10-31", "functions", id, "url"] => match m {
            "POST" => server.create_function_url_config(id, &query, &req.body),
            "GET" => server.get_function_url_config(id, &query),
            "PUT" => server.update_function_url_config(id, &query, &req.body),
            "DELETE" => server.delete_function_url_config(id, &query),
            _ => method_not_allowed(),
        },
        ["2021-10-31", "functions", id, "urls"] => match m {
            "GET" => server.list_function_url_configs(id),
            _ => method_not_allowed(),
        },
        ["2017-03-31", "tags", arn] => match m {
            "GET" => server.list_tags(arn),
            "POST" => server.tag_resource(arn, &req.body),
            "DELETE" => {
                let keys = query_all(&req.query, "tagKeys");
                server.untag_resource(arn, &keys)
            }
            _ => method_not_allowed(),
        },
        ["2016-08-19", "account-settings"] | ["2016-08-19", "account-settings", ""] => match m {
            "GET" => server.account_settings(),
            _ => method_not_allowed(),
        },
        _ => Reply::error(
            404,
            "UnknownOperationException",
            &format!("devcloud lambda does not implement {m} {}", req.raw_path),
        ),
    }
}

/// Runs a handler that unzips, extracts or reads whole packages (and holds
/// the function's lock meanwhile) off the async workers, so it cannot stall
/// other connections or running invocations. Shutdown waits for it.
async fn blocking(
    server: &Arc<Server>,
    handler: impl FnOnce(&Server) -> Reply + Send + 'static,
) -> Reply {
    server.run_blocking(handler).await
}

/// SigV4 verification per auth mode. Kept synchronous so the borrowed header
/// resolver never lives across an `.await`.
fn check_signature(server: &Server, req: &Request) -> Result<(), Reply> {
    let header_fn = |name: &str| -> Option<String> { req.headers.get(name).cloned() };
    let signed = SignedRequest {
        method: &req.method,
        path: &req.raw_path,
        query: &req.query,
        host: req.header("host"),
        authorization: req.header("authorization"),
        amz_date: req.header("x-amz-date"),
        content_sha256: req.header("x-amz-content-sha256"),
        header: &header_fn,
        body: &req.body,
    };
    let cfg = server.config();
    let creds = Credentials {
        auth_mode: &cfg.auth_mode,
        access_key_id: &cfg.access_key_id,
        secret_access_key: &cfg.secret_access_key,
        region: &cfg.region,
    };
    verify_signature(&signed, &creds).map_err(|e| Reply::error(e.status, e.name, e.name))
}

/// CSRF guard for state-changing / code-executing requests. AWS SDKs, CLIs and
/// other non-browser clients send no `Origin`; a browser always does on a
/// cross-origin POST — including "simple" `text/plain` ones that CORS never
/// blocks from being sent. Only loopback origins (local front-ends, the
/// devcloud dashboard) are trusted; a DNS-rebinding page keeps its own
/// non-loopback origin and is refused too.
fn trusted_origin(origin: &str, sec_fetch_site: &str) -> bool {
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

fn untrusted_host() -> Reply {
    Reply::error(
        403,
        "AccessDeniedException",
        "untrusted Host header (DNS rebinding guard); add the name to DEVCLOUD_ALLOWED_HOSTS to allow it",
    )
}

/// DNS-rebinding guard. A page on `http://evil.example` whose name is
/// re-pointed at 127.0.0.1 makes same-origin requests (no `Origin`, so the
/// CSRF guard cannot tell), but its `Host` still names the attacker's domain.
/// Trusted: no `Host` (HTTP/1.0, in-process callers), IP literals, single-label
/// names (`localhost`, docker-compose service names), and names under
/// suffixes no public DNS answers for (`.localhost`, `.internal` as in
/// `host.docker.internal`, `.local`, `.test`, `.example`, `.invalid`,
/// `.home.arpa`). `DEVCLOUD_ALLOWED_HOSTS` adds names (comma-separated; a
/// leading `.` allows a suffix, `*` allows any host).
pub fn trusted_host(host: &str) -> bool {
    let host = host.trim();
    if host.is_empty() {
        return true;
    }
    let name = if let Some(v6) = host.strip_prefix('[') {
        return v6
            .split_once(']')
            .is_some_and(|(ip, _)| ip.parse::<std::net::Ipv6Addr>().is_ok());
    } else {
        match host.rsplit_once(':') {
            Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
            _ => host,
        }
    };
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    if name.parse::<std::net::Ipv4Addr>().is_ok() || !name.contains('.') {
        return !name.is_empty();
    }
    const PRIVATE_SUFFIXES: [&str; 7] = [
        ".localhost",
        ".internal",
        ".local",
        ".test",
        ".example",
        ".invalid",
        ".home.arpa",
    ];
    if PRIVATE_SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return true;
    }
    allowed_hosts(
        &std::env::var("DEVCLOUD_ALLOWED_HOSTS").unwrap_or_default(),
        &name,
    )
}

/// Whether `name` (lowercase, no port) is in a `DEVCLOUD_ALLOWED_HOSTS` list.
fn allowed_hosts(list: &str, name: &str) -> bool {
    list.split(',')
        .map(|entry| entry.trim().trim_end_matches('.').to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            entry == "*"
                || entry == name
                || (entry.starts_with('.') && (name.ends_with(&entry) || name == &entry[1..]))
        })
}

fn body_error_reply(e: crate::body::BodyError) -> Reply {
    use crate::body::BodyError;
    match e {
        BodyError::TooLarge => {
            Reply::error(413, "RequestTooLargeException", "Request body is too large")
        }
        BodyError::UnsupportedEncoding(te) => Reply::error(
            501,
            "InvalidRequestContentException",
            &format!("Unsupported Transfer-Encoding: {te}"),
        ),
        BodyError::Malformed(why) => Reply::error(400, "InvalidRequestContentException", why),
        BodyError::Truncated => Reply::error(
            400,
            "InvalidRequestContentException",
            "request body ended early",
        ),
    }
}

fn method_not_allowed() -> Reply {
    Reply::error(405, "MethodNotAllowedException", "method not allowed")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        _ => "Status",
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Percent-decodes one path segment or query component (`+` stays literal in
/// paths; callers decode `+` for queries).
fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex_val(*bytes.get(i + 1)?)?;
            let lo = hex_val(*bytes.get(i + 2)?)?;
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

pub(crate) fn decode_query_component(s: &str) -> String {
    percent_decode(&s.replace('+', " ")).unwrap_or_else(|| s.to_string())
}

fn parse_query(q: &str) -> BTreeMap<String, String> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode_query_component(k), decode_query_component(v)),
            None => (decode_query_component(p), String::new()),
        })
        .collect()
}

fn query_all(q: &str, key: &str) -> Vec<String> {
    q.split('&')
        .filter_map(|p| p.split_once('='))
        .filter(|(k, _)| decode_query_component(k) == key)
        .map(|(_, v)| decode_query_component(v))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_guard_refuses_rebinding_names() {
        for ok in [
            "",
            "127.0.0.1:18025",
            "localhost",
            "localhost:9000",
            "[::1]:8080",
            "192.168.1.20:18025",
            "devcloud:18025",
            "host.docker.internal:18025",
            "abc.lambda-url.us-east-1.localhost:19010",
            "mymac.local",
            "devcloud.test",
            "LOCALHOST.",
        ] {
            assert!(trusted_host(ok), "{ok}");
        }
        for bad in [
            "evil.example.com",
            "evil.example.com:18025",
            "127.0.0.1.nip.io",
            "localhost.evil.com",
            "[not-an-ip]",
            ":80",
        ] {
            assert!(!trusted_host(bad), "{bad}");
        }
        assert!(allowed_hosts(
            "devcloud.corp.dev, .lan.example.org",
            "devcloud.corp.dev"
        ));
        assert!(allowed_hosts(".lan.example.org", "box.lan.example.org"));
        assert!(allowed_hosts(".lan.example.org", "lan.example.org"));
        assert!(!allowed_hosts(".lan.example.org", "evillan.example.org"));
        assert!(allowed_hosts("*", "anything.com"));
        assert!(!allowed_hosts("", "anything.com"));
    }

    #[test]
    fn repeated_headers_keep_every_value() {
        let mut h = HashMap::new();
        for (k, v) in [
            ("X-Tag", " a"),
            ("x-tag", "b"),
            ("Cookie", "s=1"),
            ("cookie", "t=2"),
        ] {
            add_header(&mut h, k, v);
        }
        assert_eq!(h["x-tag"], "a,b");
        assert_eq!(h["cookie"], "s=1; t=2");
    }

    #[tokio::test]
    async fn no_content_replies_declare_no_body() {
        let mut out = Vec::new();
        let mut reply = Reply::empty(204);
        reply.body = b"ignored".to_vec();
        write_reply(&mut out, "DELETE", reply).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("HTTP/1.1 204 "), "{text}");
        assert!(
            !text.to_ascii_lowercase().contains("content-length"),
            "{text}"
        );
        assert!(text.ends_with("\r\n\r\n"), "{text}");

        let mut out = Vec::new();
        write_reply(&mut out, "GET", Reply::empty(200))
            .await
            .unwrap();
        assert!(String::from_utf8(out)
            .unwrap()
            .contains("Content-Length: 0\r\n"));
    }

    #[test]
    fn decodes_arn_segments() {
        assert_eq!(
            percent_decode("arn%3Aaws%3Alambda%3Aus-east-1%3A0%3Afunction%3Af").unwrap(),
            "arn:aws:lambda:us-east-1:0:function:f"
        );
        assert!(percent_decode("bad%zz").is_none());
    }

    #[test]
    fn origin_guard() {
        assert!(trusted_origin("", ""));
        assert!(trusted_origin("", "same-origin"));
        assert!(!trusted_origin("", "cross-site"));
        assert!(trusted_origin("http://127.0.0.1:18025", "same-origin"));
        assert!(trusted_origin("http://localhost:3000", "cross-site"));
        assert!(trusted_origin("http://[::1]:8080", ""));
        assert!(trusted_origin(
            "http://web.us-central1.p.run.localhost:18095",
            ""
        ));
        assert!(!trusted_origin("https://evil.example", "cross-site"));
        assert!(!trusted_origin("http://127.0.0.1.evil.example", ""));
        assert!(!trusted_origin("null", ""));
        assert!(!trusted_origin("file://", ""));
    }

    #[test]
    fn query_helpers() {
        let q = parse_query("Qualifier=%24LATEST&MaxItems=2");
        assert_eq!(q["Qualifier"], "$LATEST");
        assert_eq!(q["MaxItems"], "2");
        assert_eq!(
            query_all("tagKeys=a&tagKeys=b&x=1", "tagKeys"),
            vec!["a", "b"]
        );
    }
}
