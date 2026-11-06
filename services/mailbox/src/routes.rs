use std::io::Write;
use std::net::IpAddr;
use std::sync::Arc;

use devcloud_mail::http::{check_basic_auth, HttpAuth};
use devcloud_mail::{Message, Service};
use tokio::sync::{broadcast, Semaphore};

use crate::http::{
    ascii_content_disposition, content_disposition, safe_content_type, Request, Response,
    ResponseBuffer, MAX_RESPONSE_BYTES,
};
use crate::{mailhog, mime, static_files, ui_api};

pub const API_CSP: &str = "sandbox; default-src 'none'";
pub const SPA_CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'";
pub const HTML_CSP: &str = "sandbox allow-popups allow-popups-to-escape-sandbox; default-src 'none'; img-src data: http: https:; style-src 'unsafe-inline'; frame-ancestors 'self'; form-action 'none'";

pub struct App {
    pub service: Arc<Service>,
    pub auth: HttpAuth,
    pub hostname: String,
    pub allowed_hosts: Vec<String>,
    pub events: broadcast::Sender<Arc<str>>,
    pub sse_slots: Arc<Semaphore>,
}

impl App {
    pub fn new(
        service: Arc<Service>,
        auth: HttpAuth,
        hostname: String,
        allowed_hosts: Vec<String>,
    ) -> Self {
        let (events, _) = broadcast::channel(16);
        Self {
            service,
            auth,
            hostname,
            allowed_hosts,
            events,
            sse_slots: Arc::new(Semaphore::new(64)),
        }
    }

    /// Serialize once, then share the same compact payload with all subscribers.
    pub fn publish(&self, id: &str) {
        if self.events.receiver_count() == 0 {
            return;
        }
        if let Ok(Some(message)) = self.service.get(id) {
            let raw = read_raw(&self.service, &message);
            if raw.len() > MAX_RESPONSE_BYTES {
                return;
            }
            let projected = mailhog::project(&message, &raw, &self.hostname);
            let response = Response::json(&projected);
            if response.status == 200 {
                // Delete may have raced with projection. Do not announce an
                // already tombstoned message to a reconnecting UI.
                if matches!(self.service.get(id), Ok(Some(_))) {
                    if let Ok(json) = String::from_utf8(response.body) {
                        let _ = self.events.send(Arc::from(json));
                    }
                }
            }
        }
    }
}

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
    } else if value.parse::<IpAddr>().is_ok() {
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
        || host.parse::<IpAddr>().is_ok()
        || host == "localhost"
        || host.ends_with(".localhost")
        || host == "host.docker.internal"
        || !host.contains('.')
}

fn valid_port(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) && value.parse::<u16>().is_ok()
}

pub fn secure(response: Response, path: &str) -> Response {
    let html =
        response.status == 200 && path.starts_with("/api/mailbox/") && path.ends_with("/html");
    let policy = if html {
        HTML_CSP
    } else if path.starts_with("/api/") {
        API_CSP
    } else {
        SPA_CSP
    };
    let response = response
        .with_header("X-Content-Type-Options", "nosniff")
        .with_header("Content-Security-Policy", policy);
    if html {
        response.with_header("Referrer-Policy", "no-referrer")
    } else {
        response
    }
}

pub fn dispatch(app: &App, req: &Request) -> Response {
    secure(dispatch_inner(app, req), &req.path)
}

fn dispatch_inner(app: &App, req: &Request) -> Response {
    if !host_allowed(req.header("host"), &app.allowed_hosts) {
        let host: String = req
            .header("host")
            .chars()
            .filter(|c| !c.is_control())
            .take(255)
            .collect();
        return Response::text(
            403,
            &format!("Host denied: {host}\nSet DEVCLOUD_MAILBOX_ALLOWED_HOSTS=<comma list> to allow your hostnames (without ports), or DEVCLOUD_MAILBOX_ALLOWED_HOSTS=* to disable the Host allowlist check.\n"),
        );
    }
    if !check_basic_auth(&app.auth, req.header("authorization")) {
        return Response::text(401, "Unauthorized")
            .with_header("WWW-Authenticate", "Basic realm=\"devcloud-mailbox\"");
    }
    if req.path.starts_with("/api/") && !static_files::valid_path(&req.path) {
        return not_found();
    }
    if req.method == "OPTIONS" {
        return method_not_allowed("GET, HEAD, DELETE");
    }
    let method = if req.method == "HEAD" {
        "GET"
    } else {
        &req.method
    };
    match req.path.as_str() {
        "/api/v2/messages" if method == "GET" => return compat_list(app, req, false, false),
        "/api/v2/search" if method == "GET" => return compat_list(app, req, true, false),
        "/api/v1/messages" => {
            return match method {
                "GET" => compat_list(app, req, false, true),
                "DELETE" => match app.service.delete_all() {
                    Ok(()) => empty(),
                    Err(_) => failure(),
                },
                _ => method_not_allowed("GET, HEAD, DELETE"),
            }
        }
        "/api/mailbox/messages" if method == "GET" => return ui_list(app, req),
        "/api/v1/events" if method == "GET" => {
            let mut response = Response::new(200, "text/event-stream", Vec::new())
                .with_header("Cache-Control", "no-cache");
            response.sse = true;
            return response;
        }
        "/api/v2/messages" | "/api/v2/search" | "/api/mailbox/messages" | "/api/v1/events" => {
            return method_not_allowed("GET, HEAD")
        }
        _ => {}
    }
    if let Some(rest) = req.path.strip_prefix("/api/v1/messages/") {
        return compat_message(app, method, rest);
    }
    if let Some(rest) = req.path.strip_prefix("/api/mailbox/messages/") {
        return ui_message(app, method, rest);
    }
    if req.path.starts_with("/api/") {
        return not_found();
    }
    if method != "GET" {
        return method_not_allowed("GET, HEAD");
    }
    static_files::serve(&req.path)
}

fn empty() -> Response {
    Response::new(200, "text/json", Vec::new())
}
fn not_found() -> Response {
    Response::text(404, "Not found")
}
fn failure() -> Response {
    Response::text(500, "Mail storage unavailable")
}
fn method_not_allowed(allow: &str) -> Response {
    Response::text(405, "Method not allowed").with_header("Allow", allow)
}

fn read_raw(service: &Service, message: &Message) -> Vec<u8> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        service.get_raw(&message.id)
    }))
    .ok()
    .and_then(Result::ok)
    .flatten()
    .unwrap_or_default()
}

fn parsed(message: &Message, raw: &[u8]) -> mime::ParsedMail {
    let mut parsed = mime::parse(mailhog::raw_data(raw));
    if raw.is_empty() {
        mime::warn(&mut parsed.warnings, "Source blob empty or unavailable");
    }
    if !message.parse_error.is_empty() {
        mime::warn(
            &mut parsed.warnings,
            "Stored message has a parse error; source is still available",
        );
    }
    parsed
}

fn compat_list(app: &App, req: &Request, search: bool, v1: bool) -> Response {
    if search
        && (!matches!(req.param("kind"), "from" | "to" | "containing")
            || req.param("query").is_empty())
    {
        return Response::new(400, "application/json", Vec::new());
    }
    let Ok(messages) = app.service.list_all() else {
        return failure();
    };
    let (start, limit) = if v1 {
        (0, 1000)
    } else {
        mailhog::paging(req.param("start"), req.param("limit"), 250)
    };
    // Reserve room for paging metadata, then prepend the final emitted count.
    let mut output = ResponseBuffer::new(MAX_RESPONSE_BYTES - 128);
    let mut total = if search { 0 } else { messages.len() };
    let mut count = 0;
    for (position, entry) in messages.into_iter().enumerate() {
        if !search && (position < start || position - start >= limit) {
            continue;
        }
        let message = match app.service.get(&entry.id) {
            Ok(Some(message)) => message,
            Ok(None) => continue,
            Err(_) => return failure(),
        };
        let raw = read_raw(&app.service, &message);
        if raw.len() > MAX_RESPONSE_BYTES {
            return response_limit();
        }
        let projected = mailhog::project(&message, &raw, &app.hostname);
        if search {
            if !mailhog::matches(&projected, req.param("kind"), req.param("query")) {
                continue;
            }
            let position = total;
            total += 1;
            if position < start || position - start >= limit {
                continue;
            }
        }
        if count > 0 && output.write_all(b",").is_err() {
            return response_limit();
        }
        if serde_json::to_writer(&mut output, &projected).is_err() {
            return response_limit();
        }
        count += 1;
    }
    if search && start > total {
        total = 0;
    }
    let prefix = if v1 {
        "[".into()
    } else {
        format!("{{\"total\":{total},\"count\":{count},\"start\":{start},\"items\":[")
    };
    let mut output = output.into_inner();
    if output.try_reserve_exact(prefix.len() + 2).is_err() {
        return response_limit();
    }
    output.splice(..0, prefix.bytes());
    output.extend_from_slice(if v1 { b"]" } else { b"]}" });
    Response::new(
        200,
        if search {
            "application/json"
        } else {
            "text/json"
        },
        output,
    )
}

fn compat_message(app: &App, method: &str, rest: &str) -> Response {
    let mut segments = rest.split('/');
    let id = segments.next().unwrap_or("");
    let suffix: Vec<_> = segments.collect();
    if method != "GET" && !(method == "DELETE" && suffix.is_empty()) {
        return method_not_allowed("GET, HEAD, DELETE");
    }
    let message = match app.service.get(id) {
        Ok(Some(message)) => message,
        Ok(None) => return not_found(),
        Err(_) => return failure(),
    };
    if method == "DELETE" {
        return match app.service.delete(&message.id) {
            Ok(()) => empty(),
            Err(_) => failure(),
        };
    }
    let raw = read_raw(&app.service, &message);
    if raw.len() > MAX_RESPONSE_BYTES {
        return response_limit();
    }
    let projected = mailhog::project(&message, &raw, &app.hostname);
    match suffix.as_slice() {
        [] => Response::json(&projected).with_header("Content-Type", "text/json"),
        ["download"] => match mailhog::download(&projected) {
            Ok(bytes) => Response::new(200, "message/rfc822", bytes).with_header(
                "Content-Disposition",
                &ascii_content_disposition(&format!("{}.eml", message.id)),
            ),
            Err(_) => response_limit(),
        },
        ["mime", "part", index, "download"] => {
            let part = index.parse::<usize>().ok().and_then(|i| {
                projected
                    .mime
                    .as_ref()
                    .and_then(|m| m.parts.as_ref())
                    .and_then(|p| p.get(i))
            });
            let Some(part) = part else {
                return not_found();
            };
            let transfer = mailhog::header_value(part, "Content-Transfer-Encoding");
            if part.body.len() > MAX_RESPONSE_BYTES {
                return response_limit();
            }
            let bytes = if transfer.eq_ignore_ascii_case("base64") {
                mime::decode_base64(part.body.as_bytes(), &mut Vec::new())
            } else {
                part.body.as_bytes().to_vec()
            };
            let ctype = mailhog::header_value(part, "Content-Type");
            let ctype = if ctype.is_empty() {
                "text/plain; charset=utf-8".into()
            } else {
                safe_content_type(ctype)
            };
            let mut response = Response::new(200, &ctype, bytes);
            for (key, values) in &part.headers {
                if ![
                    "Content-Transfer-Encoding",
                    "Content-ID",
                    "Content-Description",
                    "MIME-Version",
                ]
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
                {
                    continue;
                }
                for value in values {
                    response.headers.push((key.clone(), value.clone()));
                }
            }
            let (_, params) = mime::media_type(mailhog::header_value(part, "Content-Disposition"));
            let filename = mime::parameter(&params, "filename", &mut Vec::new())
                .unwrap_or_else(|| format!("{}-part-{index}", message.id));
            response.with_header("Content-Disposition", &content_disposition(&filename))
        }
        _ => not_found(),
    }
}

fn ui_list(app: &App, req: &Request) -> Response {
    let Ok(entries) = app.service.list_all() else {
        return failure();
    };
    let (start, limit) = mailhog::paging(req.param("start"), req.param("limit"), 100);
    let query = req.param("q");
    let mut total = if query.is_empty() { entries.len() } else { 0 };
    let mut items = Vec::new();
    for (position, entry) in entries.into_iter().enumerate() {
        if query.is_empty() && (position < start || position - start >= limit) {
            continue;
        }
        let message = match app.service.get(&entry.id) {
            Ok(Some(message)) => message,
            Ok(None) => continue,
            Err(_) => return failure(),
        };
        let raw = read_raw(&app.service, &message);
        let mut parsed = parsed(&message, &raw);
        let summary = ui_api::summary(&message, &raw, &mut parsed);
        if !query.is_empty() {
            if !ui_api::matches(&summary, &ui_api::body_text(&parsed), query) {
                continue;
            }
            let position = total;
            total += 1;
            if position < start || position - start >= limit {
                continue;
            }
        }
        items.push(summary);
    }
    Response::json(&serde_json::json!({ "total": total, "start": start, "items": items }))
}

fn response_limit() -> Response {
    Response::text(500, "Response size limit exceeded")
}

fn ui_message(app: &App, method: &str, rest: &str) -> Response {
    if method != "GET" {
        return method_not_allowed("GET, HEAD");
    }
    let (id, suffix) = rest.split_once('/').unwrap_or((rest, ""));
    let message = match app.service.get(id) {
        Ok(Some(message)) => message,
        Ok(None) => return not_found(),
        Err(_) => return failure(),
    };
    let raw = read_raw(&app.service, &message);
    if suffix == "raw" {
        return Response::new(200, "text/plain; charset=utf-8", raw);
    }
    let mut parsed = parsed(&message, &raw);
    match suffix {
        "" => Response::json(&ui_api::detail(&message, &raw, &mut parsed)),
        "html" => match ui_api::html(&parsed) {
            Some(html) => Response::new(200, "text/html; charset=utf-8", html.into_bytes()),
            None => not_found(),
        },
        _ => {
            let attachment = suffix
                .strip_prefix("attachments/")
                .and_then(|i| i.parse::<usize>().ok())
                .and_then(|i| parsed.attachments.get(i));
            match attachment {
                Some(a) => Response::new(200, &safe_content_type(&a.content_type), a.bytes.clone())
                    .with_header("Content-Disposition", &content_disposition(&a.filename)),
                None => not_found(),
            }
        }
    }
}
