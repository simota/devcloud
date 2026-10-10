//! HTTP/1.1 front-end: Cloud Run Admin API v2 plus the service data plane.
//!
//! One listener serves both. A request whose `Host` is
//! `<service>.<location>.<project>.run.localhost[:port]`, or whose path starts
//! with `/_run/<project>/<location>/<service>/`, is reverse-proxied to that
//! service's local instance (started on demand). Everything else is the
//! control plane (`/v2/projects/...`) or devcloud introspection
//! (`/_introspect/...`), answered one request per connection.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::instances::StartError;
use crate::server::{Reply, Server};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const RUN_HOST_SUFFIX: &str = ".run.localhost";

/// A parsed control-plane request.
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

/// Parsed request head plus whatever body bytes arrived with it.
struct Head {
    method: String,
    target: String,
    version: String,
    /// Header lines in arrival order (original casing) for faithful proxying.
    raw_headers: Vec<(String, String)>,
    headers: HashMap<String, String>,
    leftover: Vec<u8>,
}

pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    // Tracked so shutdown can cancel and await them: a delete that is mid-
    // teardown is dropped deterministically (its instance guard kills the
    // group) instead of being cut off whenever the runtime goes away.
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
                            break Err(e);
                        }
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
    // Never leave service processes behind when devcloud stops.
    server.instances.stop_all().await;
    result
}

async fn handle_conn(mut stream: TcpStream, server: Arc<Server>) -> std::io::Result<()> {
    let Some(head) = read_head(&mut stream).await? else {
        return Ok(());
    };
    let (path, query) = split_target(&head.target);

    // Both planes: a rebinding page must not drive the Admin API nor read
    // the services it serves.
    if !trusted_host(head.headers.get("host").map(String::as_str).unwrap_or("")) {
        return write_reply(
            &mut stream,
            Reply::error(
                403,
                "PERMISSION_DENIED",
                "untrusted Host header (DNS rebinding guard); add the name to DEVCLOUD_ALLOWED_HOSTS to allow it",
            ),
        )
        .await;
    }

    if let Some(route) = data_plane_route(&head) {
        return proxy(stream, &server, head, route).await;
    }

    // Content-Length or chunked; an unreadable body is answered, never
    // processed as a shorter one (an empty PATCH would be a silent no-op).
    let body =
        match crate::body::read_body(&mut stream, head.leftover, &head.headers, MAX_BODY_BYTES)
            .await?
        {
            Ok(b) => b,
            Err(e) => {
                use crate::body::BodyError;
                let reply = match e {
                    BodyError::TooLarge => {
                        Reply::error(413, "INVALID_ARGUMENT", "request body too large")
                    }
                    BodyError::UnsupportedEncoding(te) => Reply::error(
                        501,
                        "UNIMPLEMENTED",
                        &format!("Unsupported Transfer-Encoding: {te}"),
                    ),
                    BodyError::Malformed(why) => Reply::error(400, "INVALID_ARGUMENT", why),
                    BodyError::Truncated => {
                        Reply::error(400, "INVALID_ARGUMENT", "request body ended early")
                    }
                };
                return write_reply(&mut stream, reply).await;
            }
        };
    let req = Request {
        method: head.method,
        path,
        query,
        headers: head.headers,
        body,
    };
    let reply = process(&server, &req).await;
    write_reply(&mut stream, reply).await
}

async fn read_head(stream: &mut TcpStream) -> std::io::Result<Option<Head>> {
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
    let text = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = text.split("\r\n");
    let mut parts = lines.next().unwrap_or("").split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();
    let mut raw_headers = Vec::new();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_string(), v.trim().to_string());
            headers.insert(k.to_ascii_lowercase(), v.clone());
            raw_headers.push((k, v));
        }
    }
    Ok(Some(Head {
        method,
        target,
        version,
        raw_headers,
        headers,
        leftover: buf[header_end + 4..].to_vec(),
    }))
}

fn split_target(target: &str) -> (String, String) {
    let (raw, query) = match target.split_once('?') {
        Some((p, q)) => (p, q.to_string()),
        None => (target, String::new()),
    };
    (
        percent_decode(raw).unwrap_or_else(|| raw.to_string()),
        query,
    )
}

/// Where a data-plane request goes and the path it should carry upstream.
struct Route {
    project: String,
    location: String,
    service_id: String,
    upstream_target: String,
    /// `/_run/<project>/<location>/<service>` for path-routed requests: the
    /// service does not know it, so redirects into it must get it back.
    path_prefix: Option<String>,
    /// Path-routed root without the trailing `/`: answered with a redirect so
    /// relative links resolve inside the service.
    needs_trailing_slash: bool,
}

fn data_plane_route(head: &Head) -> Option<Route> {
    // The target may end up in a `Location` header (trailing-slash redirect):
    // a bare LF or other control byte must never get that far.
    if head.target.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return None;
    }
    let host = head.headers.get("host").map(String::as_str).unwrap_or("");
    let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    if let Some(labels) = host.to_ascii_lowercase().strip_suffix(RUN_HOST_SUFFIX) {
        let parts: Vec<&str> = labels.split('.').collect();
        if let [service_id, location, project] = parts.as_slice() {
            return Some(Route {
                project: project.to_string(),
                location: location.to_string(),
                service_id: service_id.to_string(),
                upstream_target: head.target.clone(),
                path_prefix: None,
                needs_trailing_slash: false,
            });
        }
        return None;
    }
    // Work on the raw target: the prefix boundary is found in undecoded
    // bytes and only the three routing names are decoded, so `we%62` routes
    // to `web` while the forwarded path and query stay byte-for-byte intact.
    let (raw_path, raw_query) = match head.target.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (head.target.as_str(), None),
    };
    let rest = raw_path.strip_prefix("/_run/")?;
    let mut parts = rest.splitn(4, '/');
    // Routing names are resource ids: letters, digits, and `-._:` only. This
    // rejects `/`, CR/LF and anything else that would need re-encoding when
    // the names are written back into a URL (redirects, rewritten Location).
    let mut name = || -> Option<String> {
        let decoded = percent_decode(parts.next()?)?;
        let valid = !decoded.is_empty()
            && decoded
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b':'));
        valid.then_some(decoded)
    };
    let (project, location, service_id) = (name()?, name()?, name()?);
    let remainder = parts.next();
    let needs_trailing_slash = remainder.is_none();
    let mut upstream_target = format!("/{}", remainder.unwrap_or(""));
    if let Some(q) = raw_query {
        upstream_target.push('?');
        upstream_target.push_str(q);
    }
    let path_prefix = Some(format!("/_run/{project}/{location}/{service_id}"));
    Some(Route {
        project,
        location,
        service_id,
        upstream_target,
        path_prefix,
        needs_trailing_slash,
    })
}

async fn proxy(
    mut client: TcpStream,
    server: &Server,
    head: Head,
    route: Route,
) -> std::io::Result<()> {
    let service_name = format!(
        "projects/{}/locations/{}/services/{}",
        route.project, route.location, route.service_id
    );
    if route.needs_trailing_slash {
        // `/_run/p/l/web` → `/_run/p/l/web/`: without the slash a relative
        // `login` or `app.js` would resolve next to the service, not in it.
        // 308 keeps the method and body (a 301 turns a followed POST into GET).
        let query = head
            .target
            .split_once('?')
            .map(|(_, q)| format!("?{q}"))
            .unwrap_or_default();
        let location = format!("{}/{query}", route.path_prefix.as_deref().unwrap_or(""));
        let resp = format!(
            "HTTP/1.1 308 Permanent Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        return client.write_all(resp.as_bytes()).await;
    }
    let Some(spec) = server.launch_spec(&route.project, &route.location, &route.service_id) else {
        return write_plain(
            &mut client,
            404,
            &format!("Cloud Run service {service_name} not found\n"),
        )
        .await;
    };
    if !server.allows_public_invoke(&service_name)
        && !server.authorized(
            head.headers
                .get("authorization")
                .map(String::as_str)
                .unwrap_or(""),
        )
    {
        return write_plain(
            &mut client,
            403,
            "Error: Forbidden\nYour client does not have permission to get URL from this server.\n",
        )
        .await;
    }
    drop(spec);
    let current = || server.launch_spec(&route.project, &route.location, &route.service_id);
    let port = match server.instances.ensure(&service_name, current).await {
        Ok(p) => p,
        Err(StartError::Gone) => {
            return write_plain(
                &mut client,
                404,
                &format!("Cloud Run service {service_name} not found\n"),
            )
            .await
        }
        Err(StartError::NotRunnable(msg)) => {
            return write_plain(&mut client, 503, &format!("{msg}\n")).await
        }
        Err(e) => {
            return write_plain(&mut client, 503, &format!("Service Unavailable: {e}\n")).await
        }
    };
    let mut upstream = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(s) => s,
        Err(e) => {
            return write_plain(&mut client, 502, &format!("upstream connect failed: {e}\n")).await
        }
    };

    let mut out = format!(
        "{} {} {}\r\n",
        head.method, route.upstream_target, head.version
    );
    let upgrade = is_upgrade(&head);
    // Hop-by-hop headers stop here, including any the client listed in
    // `Connection`; the forwarding headers are the platform's to set.
    // Every `Connection` field counts, not just the last one parsed.
    let listed: Vec<String> = head
        .raw_headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, v)| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        // Framing and routing headers are never the client's to drop: the
        // body is relayed as sent.
        .filter(|t| {
            !matches!(
                t.as_str(),
                "upgrade" | "host" | "content-length" | "transfer-encoding"
            )
        })
        .collect();
    let mut forwarded_for = Vec::new();
    for (k, v) in &head.raw_headers {
        let lower = k.to_ascii_lowercase();
        let hop_by_hop = matches!(
            lower.as_str(),
            "connection" | "keep-alive" | "proxy-connection" | "te" | "proxy-authorization"
        ) || (lower == "upgrade" && !upgrade)
            || listed.contains(&lower);
        if hop_by_hop || lower == "x-forwarded-proto" {
            continue;
        }
        if lower == "x-forwarded-for" {
            forwarded_for.push(v.trim().to_string());
            continue;
        }
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("X-Forwarded-Proto: http\r\n");
    // Like Cloud Run's front end: the client's own chain, then its address,
    // as one header.
    forwarded_for.push("127.0.0.1".to_string());
    out.push_str(&format!(
        "X-Forwarded-For: {}\r\n",
        forwarded_for.join(", ")
    ));
    // An upgrade (WebSocket) handshake needs `Connection: Upgrade` next to
    // `Upgrade:`; plain requests are one-shot.
    if upgrade {
        out.push_str("Connection: Upgrade\r\n\r\n");
    } else {
        out.push_str("Connection: close\r\n\r\n");
    }
    upstream.write_all(out.as_bytes()).await?;
    upstream.write_all(&head.leftover).await?;
    emit_request(&service_name, &head.method);
    let Some(prefix) = &route.path_prefix else {
        let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        return Ok(());
    };
    // Path routing rewrites the response head, but the request body must keep
    // flowing meanwhile: an app that reads the whole body before answering
    // would otherwise deadlock against us waiting for its response.
    let host = head.headers.get("host").cloned().unwrap_or_default();
    let (mut client_rd, mut client_wr) = client.split();
    let (mut upstream_rd, mut upstream_wr) = upstream.split();
    let request = async {
        let _ = tokio::io::copy(&mut client_rd, &mut upstream_wr).await;
        let _ = upstream_wr.shutdown().await;
    };
    let response = async {
        relay_response_head(&mut upstream_rd, &mut client_wr, prefix, &host).await?;
        tokio::io::copy(&mut upstream_rd, &mut client_wr).await?;
        client_wr.shutdown().await
    };
    tokio::pin!(request, response);
    tokio::select! {
        // Response complete: the exchange is over (the request side may still
        // be parked on a client that keeps the connection open).
        _ = &mut response => {}
        // Client done sending: keep relaying the response to the end.
        _ = &mut request => {
            let _ = response.await;
        }
    }
    Ok(())
}

fn is_upgrade(head: &Head) -> bool {
    head.headers.contains_key("upgrade")
        && head
            .raw_headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("connection"))
            .flat_map(|(_, v)| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
}

/// Copies the upstream response head to the client, re-prefixing redirects
/// that point back into the service. Anything that is not a parsable head
/// (oversized, early EOF) is passed through untouched.
async fn relay_response_head<R, W>(
    upstream: &mut R,
    client: &mut W,
    prefix: &str,
    host: &str,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    // Interim responses (100 Continue, 103 Early Hints) precede the final
    // one: pass them through and rewrite the head that actually decides
    // where the client goes. 101 switches protocols — nothing follows to
    // rewrite.
    loop {
        let end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break Some(pos);
            }
            if buf.len() > MAX_HEADER_BYTES {
                break None;
            }
            let n = upstream.read(&mut tmp).await?;
            if n == 0 {
                break None;
            }
            buf.extend_from_slice(&tmp[..n]);
        };
        let Some(end) = end else {
            return client.write_all(&buf).await;
        };
        let head = String::from_utf8_lossy(&buf[..end]).into_owned();
        let status: u16 = head
            .split(' ')
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        if (100..200).contains(&status) && status != 101 {
            client.write_all(&buf[..end + 4]).await?;
            buf.drain(..end + 4);
            continue;
        }
        let out = if status == 101 {
            head
        } else {
            rewrite_head_locations(&head, prefix, host)
        };
        client.write_all(out.as_bytes()).await?;
        return client.write_all(&buf[end..]).await;
    }
}

fn rewrite_head_locations(head: &str, prefix: &str, host: &str) -> String {
    let mut out = String::with_capacity(head.len() + prefix.len());
    for (i, line) in head.split("\r\n").enumerate() {
        if i > 0 {
            out.push_str("\r\n");
        }
        match line.split_once(':') {
            Some((name, value))
                if name.trim().eq_ignore_ascii_case("location")
                    || name.trim().eq_ignore_ascii_case("content-location") =>
            {
                match rewrite_location(value.trim(), prefix, host) {
                    Some(v) => out.push_str(&format!("{}: {v}", name.trim())),
                    None => out.push_str(line),
                }
            }
            _ => out.push_str(line),
        }
    }
    out
}

/// `/docs/` → `<prefix>/docs/`; `http://<this host>/docs/` likewise. Other
/// origins, scheme-relative and relative references are left alone.
fn rewrite_location(value: &str, prefix: &str, host: &str) -> Option<String> {
    if value.starts_with('/') && !value.starts_with("//") {
        return Some(format!("{prefix}{value}"));
    }
    let (scheme, rest) = value.split_once("://")?;
    // The authority ends at the first `/`, `?` or `#`; an omitted path is `/`
    // (`http://h?q=1` means `http://h/?q=1`).
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let path = if tail.starts_with('/') {
        tail.to_string()
    } else {
        format!("/{tail}")
    };
    (!host.is_empty() && authority.eq_ignore_ascii_case(host))
        .then(|| format!("{scheme}://{authority}{prefix}{path}"))
}

fn emit_request(service_name: &str, method: &str) {
    let event = json!({
        "type": "cloudrun.service.request",
        "service": "cloudrun",
        "payload": { "service": service_name, "method": method },
    });
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(event.to_string());
    }
}

async fn write_plain(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reason(status),
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

async fn write_reply(stream: &mut TcpStream, reply: Reply) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nServer: devcloud-cloudrun\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.status,
        reason(reply.status),
        reply.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&reply.body).await?;
    stream.flush().await
}

/// Control-plane pipeline (auth → route). Public for in-process tests.
pub async fn process(server: &Arc<Server>, req: &Request) -> Reply {
    if server.load_err().is_some() {
        return Reply::error(500, "INTERNAL", "failed to load cloud run state");
    }
    let query = parse_query(&req.query);
    let m = req.method.as_str();

    if let Some(rest) = req.path.strip_prefix("/_introspect/") {
        // Service resources carry env values and logs carry app output: in
        // strict mode they need the same token as the Admin API.
        if !server.authorized(req.header("authorization")) {
            return Reply::error(
                401,
                "UNAUTHENTICATED",
                "Request had invalid authentication credentials. Expected OAuth 2 access token.",
            );
        }
        if m != "GET" {
            return Reply::error(405, "INVALID_ARGUMENT", "method not allowed");
        }
        let seg: Vec<&str> = rest.split('/').collect();
        return match seg.as_slice() {
            ["services"] => Reply::json(200, &json!({ "services": server.all_services() })),
            ["instances"] => {
                Reply::json(200, &json!({ "instances": server.instances.list().await }))
            }
            ["logs", project, location, service_id] => {
                let name = format!("projects/{project}/locations/{location}/services/{service_id}");
                Reply::json(
                    200,
                    &json!({ "service": name, "lines": server.instances.logs(&name) }),
                )
            }
            _ => Reply::error(404, "NOT_FOUND", "unknown introspection path"),
        };
    }

    if !matches!(m, "GET" | "HEAD")
        && !trusted_origin(req.header("origin"), req.header("sec-fetch-site"))
    {
        return Reply::error(
            403,
            "PERMISSION_DENIED",
            "cross-origin requests to the devcloud Cloud Run Admin API are not allowed",
        );
    }

    if !server.authorized(req.header("authorization")) {
        return Reply::error(
            401,
            "UNAUTHENTICATED",
            "Request had invalid authentication credentials. Expected OAuth 2 access token.",
        );
    }

    let Some(rest) = req.path.strip_prefix("/v2/") else {
        return Reply::error(
            404,
            "NOT_FOUND",
            &format!("devcloud cloud run does not implement {m} {}", req.path),
        );
    };
    let seg: Vec<&str> = rest.trim_end_matches('/').split('/').collect();
    match seg.as_slice() {
        ["projects", p, "locations"] if m == "GET" => server.list_locations(p),
        ["projects", p, "locations", l, "services"] => match m {
            "GET" => server.list_services(p, l, &query),
            "POST" => server.create_service(p, l, &query, &req.body),
            _ => not_allowed(),
        },
        ["projects", p, "locations", l, "services", s] => {
            let (service_id, verb) = match s.split_once(':') {
                Some((id, verb)) => (id, Some(verb)),
                None => (*s, None),
            };
            let name = format!("projects/{p}/locations/{l}/services/{service_id}");
            match (m, verb) {
                ("GET", None) => server.get_service(&name),
                ("PATCH", None) => server.update_service(p, l, service_id, &query, &req.body),
                ("DELETE", None) => server.delete_service(p, l, service_id, &query).await,
                ("GET", Some("getIamPolicy")) | ("POST", Some("getIamPolicy")) => {
                    server.get_iam_policy(&name)
                }
                ("POST", Some("setIamPolicy")) => server.set_iam_policy(&name, &req.body),
                ("POST", Some("testIamPermissions")) => {
                    server.test_iam_permissions(&name, &req.body)
                }
                _ => not_allowed(),
            }
        }
        ["projects", p, "locations", l, "services", s, "revisions"] if m == "GET" => {
            server.list_revisions(&format!("projects/{p}/locations/{l}/services/{s}"), &query)
        }
        ["projects", p, "locations", l, "services", s, "revisions", r] => {
            let service = format!("projects/{p}/locations/{l}/services/{s}");
            let revision = format!("{service}/revisions/{r}");
            match m {
                "GET" => server.get_revision(&revision),
                "DELETE" => server.delete_revision(&service, &revision, &query),
                _ => not_allowed(),
            }
        }
        ["projects", p, "locations", l, "operations"] if m == "GET" => {
            server.list_operations(&format!("projects/{p}/locations/{l}"))
        }
        ["projects", p, "locations", l, "operations", o] => {
            let (id, verb) = match o.split_once(':') {
                Some((id, verb)) => (id, Some(verb)),
                None => (*o, None),
            };
            let name = format!("projects/{p}/locations/{l}/operations/{id}");
            match (m, verb) {
                ("GET", None) | ("POST", Some("wait")) => server.get_operation(&name),
                ("DELETE", None) => server.delete_operation(&name),
                _ => not_allowed(),
            }
        }
        _ => Reply::error(
            404,
            "NOT_FOUND",
            &format!("devcloud cloud run does not implement {m} {}", req.path),
        ),
    }
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

fn not_allowed() -> Reply {
    Reply::error(405, "INVALID_ARGUMENT", "method not allowed")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
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

fn parse_query(q: &str) -> BTreeMap<String, String> {
    let decode = |s: &str| percent_decode(&s.replace('+', " ")).unwrap_or_else(|| s.to_string());
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
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

    fn head(target: &str, host: &str) -> Head {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), host.to_string());
        Head {
            method: "GET".into(),
            target: target.into(),
            version: "HTTP/1.1".into(),
            raw_headers: vec![("Host".into(), host.into())],
            headers,
            leftover: Vec::new(),
        }
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
    fn routes_by_host() {
        let h = head("/hello?x=1", "web.us-central1.my-proj.run.localhost:18095");
        let r = data_plane_route(&h).unwrap();
        assert_eq!(
            (
                r.project.as_str(),
                r.location.as_str(),
                r.service_id.as_str()
            ),
            ("my-proj", "us-central1", "web")
        );
        assert_eq!(r.upstream_target, "/hello?x=1");
    }

    #[test]
    fn routes_by_path_prefix() {
        let h = head("/_run/p/us-central1/web/api/v1?q=2", "127.0.0.1:18095");
        let r = data_plane_route(&h).unwrap();
        assert_eq!(r.service_id, "web");
        assert_eq!(r.upstream_target, "/api/v1?q=2");
        let root = head("/_run/p/us-central1/web?q=1", "127.0.0.1");
        assert!(data_plane_route(&root).unwrap().needs_trailing_slash);
        let slashed = data_plane_route(&head("/_run/p/us-central1/web/?q=1", "127.0.0.1")).unwrap();
        assert!(!slashed.needs_trailing_slash);
        assert_eq!(slashed.upstream_target, "/?q=1");
    }

    #[test]
    fn encoded_routing_names_keep_the_forwarded_path_intact() {
        let h = head(
            "/_run/p/us-central1/we%62/hello%20there?x=1",
            "127.0.0.1:18095",
        );
        let r = data_plane_route(&h).unwrap();
        assert_eq!(r.service_id, "web");
        assert_eq!(r.upstream_target, "/hello%20there?x=1");
        let trailing = head("/_run/p/us-central1/web/", "127.0.0.1");
        assert_eq!(data_plane_route(&trailing).unwrap().upstream_target, "/");
        let slash_in_name = head("/_run/p/us-central1/a%2Fb/x", "127.0.0.1");
        assert!(data_plane_route(&slash_in_name).is_none());
    }

    #[test]
    fn redirects_into_the_service_keep_the_route_prefix() {
        let p = "/_run/demo/us-central1/web";
        assert_eq!(
            rewrite_location("/docs/", p, "127.0.0.1:18095").unwrap(),
            "/_run/demo/us-central1/web/docs/"
        );
        assert_eq!(
            rewrite_location(
                "http://127.0.0.1:18095/login?next=%2F",
                p,
                "127.0.0.1:18095"
            )
            .unwrap(),
            "http://127.0.0.1:18095/_run/demo/us-central1/web/login?next=%2F"
        );
        assert_eq!(
            rewrite_location("http://127.0.0.1:18095?q=1", p, "127.0.0.1:18095").unwrap(),
            "http://127.0.0.1:18095/_run/demo/us-central1/web/?q=1"
        );
        assert_eq!(
            rewrite_location("http://127.0.0.1:18095#top", p, "127.0.0.1:18095").unwrap(),
            "http://127.0.0.1:18095/_run/demo/us-central1/web/#top"
        );
        assert_eq!(
            rewrite_location("http://127.0.0.1:18095", p, "127.0.0.1:18095").unwrap(),
            "http://127.0.0.1:18095/_run/demo/us-central1/web/"
        );
        assert_eq!(
            rewrite_location("http://other.example?q=1", p, "127.0.0.1:18095"),
            None
        );
        assert_eq!(
            rewrite_location("https://accounts.example/x", p, "127.0.0.1:18095"),
            None
        );
        assert_eq!(
            rewrite_location("//cdn.example/x", p, "127.0.0.1:18095"),
            None
        );
        assert_eq!(rewrite_location("next/page", p, "127.0.0.1:18095"), None);
        assert!(
            data_plane_route(&head("/x", "web.us-central1.p.run.localhost"))
                .unwrap()
                .path_prefix
                .is_none()
        );
    }

    #[test]
    fn upgrade_requests_are_detected() {
        let mut h = head("/ws", "127.0.0.1");
        assert!(!is_upgrade(&h));
        h.headers.insert("upgrade".into(), "websocket".into());
        h.headers
            .insert("connection".into(), "keep-alive, Upgrade".into());
        h.raw_headers
            .push(("Connection".into(), "keep-alive, Upgrade".into()));
        assert!(is_upgrade(&h));
        // The token may sit in any of several Connection fields.
        let mut split = head("/ws", "127.0.0.1");
        split.headers.insert("upgrade".into(), "websocket".into());
        split
            .raw_headers
            .push(("Connection".into(), "Upgrade".into()));
        split
            .raw_headers
            .push(("connection".into(), "keep-alive".into()));
        split
            .headers
            .insert("connection".into(), "keep-alive".into());
        assert!(is_upgrade(&split));
    }

    #[test]
    fn routing_names_with_control_or_url_syntax_bytes_are_rejected() {
        for target in [
            "/_run/p%0D%0AX-Injected%3A%20yes/us-central1/web",
            "/_run/p/us-central1%0A/web",
            "/_run/p/us-central1/we%3Fb/x",
            "/_run/p/us-central1/we%20b/x",
            "/_run/p/us-central1/web?a\nX-Injected: yes",
        ] {
            assert!(
                data_plane_route(&head(target, "127.0.0.1")).is_none(),
                "{target:?}"
            );
        }
        assert!(data_plane_route(&head(
            "/_run/example.com:proj/us-central1/web/",
            "127.0.0.1"
        ))
        .is_some());
    }

    #[tokio::test]
    async fn interim_responses_pass_through_and_the_final_head_is_rewritten() {
        let upstream_bytes = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </app.css>; rel=preload\r\n\r\nHTTP/1.1 302 Found\r\nLocation: /login\r\nContent-Length: 0\r\n\r\n";
        let mut upstream = &upstream_bytes[..];
        let mut client = Vec::new();
        relay_response_head(
            &mut upstream,
            &mut client,
            "/_run/p/l/web",
            "127.0.0.1:18095",
        )
        .await
        .unwrap();
        let out = String::from_utf8(client).unwrap();
        assert!(
            out.starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n"),
            "{out}"
        );
        assert!(
            out.contains("HTTP/1.1 302 Found\r\nLocation: /_run/p/l/web/login\r\n"),
            "{out}"
        );

        let switching = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\nframe";
        let mut upstream = &switching[..];
        let mut client = Vec::new();
        relay_response_head(&mut upstream, &mut client, "/_run/p/l/web", "h")
            .await
            .unwrap();
        assert_eq!(
            client,
            switching.to_vec(),
            "101 and the tunnel bytes pass through untouched"
        );
    }

    #[test]
    fn control_plane_is_not_routed() {
        let h = head("/v2/projects/p/locations/l/services", "127.0.0.1:18095");
        assert!(data_plane_route(&h).is_none());
    }
}
