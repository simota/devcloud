//! Routing, auth, and multipart upload decoding — port of
//! `internal/services/bigquery/routes.rs` (`handle`, `authorize`, the path
//! splitters) plus `insertUploadJob` from `job_handlers.rs`.
//!
//! [`handle`] is the transport-independent equivalent of legacy
//! `server.routes().ServeHTTP`: the hand-rolled HTTP layer (`http`) builds a
//! [`Request`] from the wire and writes the returned [`ApiResponse`] back.

use crate::model::{
    JobInsertRequest, ProjectListItem, ProjectReference, ProjectsListResponse,
    ServiceAccountResponse,
};
use crate::responses::ApiResponse;
use crate::server::Server;
use crate::validation::{validate_resource_id, Query};

/// The already-parsed request surface the legacy handlers consume.
#[derive(Debug, Default)]
pub struct Request {
    pub method: String,
    /// The escaped path (legacy `r.URL.EscapedPath()`).
    pub path: String,
    pub query: Query,
    /// `Authorization` header value.
    pub authorization: String,
    /// `Content-Type` header value (multipart uploads).
    pub content_type: String,
    pub body: Vec<u8>,
}

impl Request {
    /// Builds a request from a method + request target (path?query), the way
    /// the tests drive `httptest.NewRequest`.
    pub fn new(method: &str, target: &str, body: &[u8]) -> Request {
        let (path, raw_query) = match target.split_once('?') {
            Some((path, raw_query)) => (path, raw_query),
            None => (target, ""),
        };
        Request {
            method: method.to_string(),
            path: path.to_string(),
            query: Query::parse(raw_query),
            authorization: String::new(),
            content_type: String::new(),
            body: body.to_vec(),
        }
    }
}

/// legacy `handle` (minus the `Server` response header, which the HTTP layer
/// adds).
pub fn handle(server: &Server, req: &Request) -> ApiResponse {
    if !authorize(server, req) {
        return ApiResponse::error(401, "authError", "invalid authentication credentials")
            .with_www_authenticate();
    }
    if crate::introspect::is_introspect_path(&req.path) {
        return crate::introspect::handle_introspect(server, req);
    }
    if req.path.starts_with("/upload/bigquery/v2/projects/") {
        return handle_upload_project_resource(server, req);
    }
    // legacy compares the decoded `r.URL.Path` here.
    if path_unescape(&req.path).as_deref() == Some("/bigquery/v2/projects") {
        return handle_projects(server, req);
    }
    if req.path.starts_with("/bigquery/v2/projects/") {
        return handle_project_resource(server, req);
    }
    ApiResponse::error(404, "notFound", "not found")
}

/// legacy `authorize`.
fn authorize(server: &Server, req: &Request) -> bool {
    let mode = server.config().auth_mode.trim().to_lowercase();
    match mode.as_str() {
        "" | "off" | "relaxed" => true,
        "oauth-relaxed" => !bearer_token_from_request(req).is_empty(),
        "bearer-dev" | "strict" => {
            let token = bearer_token_from_request(req);
            let expected = server.config().bearer_token.trim();
            if token.is_empty() || expected.is_empty() {
                return false;
            }
            constant_time_eq(token.as_bytes(), expected.as_bytes())
        }
        _ => false,
    }
}

/// legacy `subtle.ConstantTimeCompare`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// legacy `bearerTokenFromRequest`.
fn bearer_token_from_request(req: &Request) -> String {
    let auth = req.authorization.trim();
    match auth.split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("Bearer") => token.trim().to_string(),
        _ => String::new(),
    }
}

/// legacy `handleUploadProjectResource`.
fn handle_upload_project_resource(server: &Server, req: &Request) -> ApiResponse {
    let Some(parts) = resource_parts(&req.path, "/upload/bigquery/v2/projects/") else {
        return ApiResponse::error(400, "invalid", "invalid resource path");
    };
    if parts.len() == 2 && parts[1] == "jobs" && !parts[0].is_empty() {
        if req.method != "POST" {
            return ApiResponse::method_not_allowed("POST");
        }
        if let Err(message) = validate_resource_id(&parts[0], "project") {
            return ApiResponse::error(400, "invalid", &message);
        }
        return insert_upload_job(server, req, &parts[0]);
    }
    ApiResponse::error(404, "notFound", "not found")
}

/// legacy `insertUploadJob` (job_handlers.rs).
fn insert_upload_job(server: &Server, req: &Request, project_id: &str) -> ApiResponse {
    if req.query.get("uploadType") != "multipart" {
        return ApiResponse::error(400, "invalid", "uploadType=multipart is required");
    }
    let Some((request, media)) = decode_multipart_job_insert_request(server, req) else {
        return ApiResponse::error(400, "invalid", "invalid multipart upload request");
    };
    if !request.job_reference.project_id.is_empty()
        && request.job_reference.project_id != project_id
    {
        return ApiResponse::error(
            400,
            "invalid",
            "jobReference.projectId must match request project",
        );
    }
    if request
        .configuration
        .load
        .destination_table
        .table_id
        .is_empty()
    {
        return ApiResponse::error(
            400,
            "invalid",
            "configuration.load.destinationTable is required",
        );
    }
    match server.create_upload_load_job(
        project_id,
        &request.job_reference,
        request.configuration.load,
        &media,
    ) {
        Err(message) => ApiResponse::error(400, "invalid", &message),
        Ok(job) => ApiResponse::json(200, &job.job),
    }
}

/// legacy `decodeMultipartJobInsertRequest`: metadata part (JSON) + media part.
fn decode_multipart_job_insert_request(
    server: &Server,
    req: &Request,
) -> Option<(JobInsertRequest, Vec<u8>)> {
    let (media_type, boundary) = parse_media_type(&req.content_type)?;
    if !media_type.starts_with("multipart/") || boundary.is_empty() {
        return None;
    }
    // legacy wraps the body in `http.MaxBytesReader(maxRequestBytes)`.
    if req.body.len() as i64 > server.max_request_bytes() {
        return None;
    }
    let parts = multipart_parts(&req.body, &boundary)?;
    if parts.len() < 2 {
        return None;
    }
    let request: JobInsertRequest = serde_json::from_slice(&parts[0]).ok()?;
    Some((request, parts[1].clone()))
}

/// The subset of legacy `mime.ParseMediaType` this path needs: the lowercased
/// media type plus the `boundary` parameter (optionally quoted).
fn parse_media_type(content_type: &str) -> Option<(String, String)> {
    let mut segments = content_type.split(';');
    let media_type = segments.next()?.trim().to_lowercase();
    if media_type.is_empty() {
        return None;
    }
    let mut boundary = String::new();
    for segment in segments {
        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };
        if !key.trim().eq_ignore_ascii_case("boundary") {
            continue;
        }
        let value = value.trim();
        boundary = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value)
            .to_string();
    }
    Some((media_type, boundary))
}

/// Minimal `mime/multipart` reader: returns the part bodies in order. Any
/// malformation yields `None` (the legacy caller collapses every reader error
/// into "invalid multipart upload request").
fn multipart_parts(body: &[u8], boundary: &str) -> Option<Vec<Vec<u8>>> {
    let delimiter = format!("--{boundary}");
    let delimiter = delimiter.as_bytes();
    let start = find_subslice(body, delimiter)?;
    let mut cursor = start + delimiter.len();
    let mut parts = Vec::new();
    loop {
        if body[cursor..].starts_with(b"--") {
            return Some(parts); // closing delimiter
        }
        // Skip the rest of the boundary line.
        let line_end = find_subslice(&body[cursor..], b"\n")?;
        cursor += line_end + 1;
        // Part headers run until a blank line.
        let headers_end = find_subslice(&body[cursor..], b"\r\n\r\n")
            .map(|i| (i, 4))
            .or_else(|| find_subslice(&body[cursor..], b"\n\n").map(|i| (i, 2)))?;
        cursor += headers_end.0 + headers_end.1;
        // Content runs until the next boundary line.
        let next = find_subslice(&body[cursor..], delimiter)?;
        let mut content_end = cursor + next;
        // Drop the CRLF (or LF) that precedes the boundary.
        if content_end >= 2 && &body[content_end - 2..content_end] == b"\r\n" {
            content_end -= 2;
        } else if content_end >= 1 && body[content_end - 1] == b'\n' {
            content_end -= 1;
        }
        parts.push(body[cursor..content_end].to_vec());
        cursor += next + delimiter.len();
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// legacy `handleProjects`.
fn handle_projects(server: &Server, req: &Request) -> ApiResponse {
    if req.method != "GET" {
        return ApiResponse::method_not_allowed("GET");
    }
    let project_id = server.project_id().to_string();
    ApiResponse::json(
        200,
        &ProjectsListResponse {
            kind: "bigquery#projectList".to_string(),
            projects: vec![ProjectListItem {
                kind: "bigquery#project".to_string(),
                id: project_id.clone(),
                numeric_id: "0".to_string(),
                project_ref: ProjectReference {
                    project_id: project_id.clone(),
                },
                friendly_name: project_id,
            }],
            total_items: 1,
        },
    )
}

/// legacy `handleProjectResource`.
fn handle_project_resource(server: &Server, req: &Request) -> ApiResponse {
    let Some(parts) = resource_parts(&req.path, "/bigquery/v2/projects/") else {
        return ApiResponse::error(400, "invalid", "invalid resource path");
    };
    if parts.len() == 2 && parts[1] == "serviceAccount" && !parts[0].is_empty() {
        if req.method != "GET" {
            return ApiResponse::method_not_allowed("GET");
        }
        if let Err(message) = validate_resource_id(&parts[0], "project") {
            return ApiResponse::error(400, "invalid", &message);
        }
        return ApiResponse::json(
            200,
            &ServiceAccountResponse {
                kind: "bigquery#getServiceAccountResponse".to_string(),
                email: format!("devcloud-bigquery@{}.iam.gserviceaccount.com", parts[0]),
            },
        );
    }
    if parts.len() >= 2 && parts[1] == "queries" && !parts[0].is_empty() {
        return handle_queries(server, req, &parts);
    }
    if parts.len() >= 2 && parts[1] == "jobs" && !parts[0].is_empty() {
        return handle_jobs(server, req, &parts);
    }
    if parts.len() >= 2 && parts[1] == "datasets" && !parts[0].is_empty() {
        return handle_datasets(server, req, &parts);
    }
    ApiResponse::error(404, "notFound", "not found")
}

/// legacy `handleQueries`.
fn handle_queries(server: &Server, req: &Request, parts: &[String]) -> ApiResponse {
    let project_id = parts[0].as_str();
    if let Err(message) = validate_resource_id(project_id, "project") {
        return ApiResponse::error(400, "invalid", &message);
    }
    match parts.len() {
        2 => {
            if req.method != "POST" {
                return ApiResponse::method_not_allowed("POST");
            }
            server.query_rows(project_id, &req.body)
        }
        3 => {
            if req.method != "GET" {
                return ApiResponse::method_not_allowed("GET");
            }
            server.get_query_results(project_id, &parts[2], &req.query)
        }
        _ => ApiResponse::error(404, "notFound", "not found"),
    }
}

/// legacy `handleJobs`.
fn handle_jobs(server: &Server, req: &Request, parts: &[String]) -> ApiResponse {
    let project_id = parts[0].as_str();
    if let Err(message) = validate_resource_id(project_id, "project") {
        return ApiResponse::error(400, "invalid", &message);
    }
    match parts.len() {
        2 => match req.method.as_str() {
            "GET" => server.list_jobs(project_id, &req.query),
            "POST" => server.insert_job(project_id, &req.body),
            _ => ApiResponse::method_not_allowed("GET, POST"),
        },
        3 => match req.method.as_str() {
            "GET" => server.get_job(project_id, &parts[2]),
            "DELETE" => server.delete_job_metadata(project_id, &parts[2]),
            _ => ApiResponse::method_not_allowed("GET, DELETE"),
        },
        4 => match parts[3].as_str() {
            "cancel" => {
                if req.method != "POST" {
                    return ApiResponse::method_not_allowed("POST");
                }
                server.cancel_job(project_id, &parts[2])
            }
            "getQueryResults" => {
                if req.method != "GET" {
                    return ApiResponse::method_not_allowed("GET");
                }
                server.get_query_results(project_id, &parts[2], &req.query)
            }
            "delete" => {
                if req.method != "DELETE" {
                    return ApiResponse::method_not_allowed("DELETE");
                }
                server.delete_job_metadata(project_id, &parts[2])
            }
            _ => ApiResponse::error(404, "notFound", "not found"),
        },
        _ => ApiResponse::error(404, "notFound", "not found"),
    }
}

/// legacy `handleDatasets`.
fn handle_datasets(server: &Server, req: &Request, parts: &[String]) -> ApiResponse {
    let project_id = parts[0].as_str();
    if let Err(message) = validate_resource_id(project_id, "project") {
        return ApiResponse::error(400, "invalid", &message);
    }

    match parts.len() {
        2 => match req.method.as_str() {
            "GET" => server.list_datasets(project_id, &req.query),
            "POST" => server.create_dataset(project_id, &req.body),
            _ => ApiResponse::method_not_allowed("GET, POST"),
        },
        3 => {
            let (dataset_id, action) = split_resource_action(&parts[2]);
            if let Err(message) = validate_resource_id(dataset_id, "dataset") {
                return ApiResponse::error(400, "invalid", &message);
            }
            if !action.is_empty() {
                return server.handle_dataset_iam_policy(
                    &req.method,
                    project_id,
                    dataset_id,
                    action,
                    &req.body,
                );
            }
            match req.method.as_str() {
                "GET" => server.get_dataset(project_id, dataset_id),
                "PATCH" => server.patch_dataset(project_id, dataset_id, false, &req.body),
                "PUT" => server.patch_dataset(project_id, dataset_id, true, &req.body),
                "DELETE" => server.delete_dataset(project_id, dataset_id, &req.query),
                _ => ApiResponse::method_not_allowed("GET, PATCH, PUT, DELETE"),
            }
        }
        4 => {
            let dataset_id = parts[2].as_str();
            if let Err(message) = validate_resource_id(dataset_id, "dataset") {
                return ApiResponse::error(400, "invalid", &message);
            }
            match parts[3].as_str() {
                "tables" => match req.method.as_str() {
                    "GET" => server.list_tables(project_id, dataset_id, &req.query),
                    "POST" => server.create_table(project_id, dataset_id, &req.body),
                    _ => ApiResponse::method_not_allowed("GET, POST"),
                },
                "routines" => match req.method.as_str() {
                    "GET" => server.list_routines(project_id, dataset_id, &req.query),
                    "POST" => server.create_routine(project_id, dataset_id, &req.body),
                    _ => ApiResponse::method_not_allowed("GET, POST"),
                },
                _ => ApiResponse::error(404, "notFound", "not found"),
            }
        }
        5 => {
            let dataset_id = parts[2].as_str();
            if let Err(message) = validate_resource_id(dataset_id, "dataset") {
                return ApiResponse::error(400, "invalid", &message);
            }
            match parts[3].as_str() {
                "tables" => {
                    let (table_id, action) = split_resource_action(&parts[4]);
                    if let Err(message) = validate_resource_id(table_id, "table") {
                        return ApiResponse::error(400, "invalid", &message);
                    }
                    if !action.is_empty() {
                        return server.handle_table_iam_policy(
                            &req.method,
                            project_id,
                            dataset_id,
                            table_id,
                            action,
                            &req.body,
                        );
                    }
                    match req.method.as_str() {
                        "GET" => server.get_table(project_id, dataset_id, table_id),
                        "PATCH" => {
                            server.patch_table(project_id, dataset_id, table_id, false, &req.body)
                        }
                        "PUT" => {
                            server.patch_table(project_id, dataset_id, table_id, true, &req.body)
                        }
                        "DELETE" => server.delete_table(project_id, dataset_id, table_id),
                        _ => ApiResponse::method_not_allowed("GET, PATCH, PUT, DELETE"),
                    }
                }
                "routines" => {
                    let routine_id = parts[4].as_str();
                    if let Err(message) = validate_resource_id(routine_id, "routine") {
                        return ApiResponse::error(400, "invalid", &message);
                    }
                    match req.method.as_str() {
                        "GET" => server.get_routine(project_id, dataset_id, routine_id),
                        "PATCH" => server
                            .patch_routine(project_id, dataset_id, routine_id, false, &req.body),
                        "PUT" => server
                            .patch_routine(project_id, dataset_id, routine_id, true, &req.body),
                        "DELETE" => server.delete_routine(project_id, dataset_id, routine_id),
                        _ => ApiResponse::method_not_allowed("GET, PATCH, PUT, DELETE"),
                    }
                }
                _ => ApiResponse::error(404, "notFound", "not found"),
            }
        }
        6 => {
            if parts[3] != "tables" {
                return ApiResponse::error(404, "notFound", "not found");
            }
            let dataset_id = parts[2].as_str();
            let table_id = parts[4].as_str();
            if let Err(message) = validate_resource_id(dataset_id, "dataset") {
                return ApiResponse::error(400, "invalid", &message);
            }
            if let Err(message) = validate_resource_id(table_id, "table") {
                return ApiResponse::error(400, "invalid", &message);
            }
            match parts[5].as_str() {
                "insertAll" => {
                    if req.method != "POST" {
                        return ApiResponse::method_not_allowed("POST");
                    }
                    server.insert_rows(project_id, dataset_id, table_id, &req.body)
                }
                "data" => {
                    if req.method != "GET" {
                        return ApiResponse::method_not_allowed("GET");
                    }
                    server.list_rows(project_id, dataset_id, table_id, &req.query)
                }
                _ => ApiResponse::error(404, "notFound", "not found"),
            }
        }
        _ => ApiResponse::error(404, "notFound", "not found"),
    }
}

/// legacy `projectResourceParts` / `uploadProjectResourceParts`: trim the prefix,
/// split on `/`, path-unescape each segment, drop empties. An unescape error
/// surfaces as "invalid resource path".
fn resource_parts(escaped_path: &str, prefix: &str) -> Option<Vec<String>> {
    let suffix = escaped_path.strip_prefix(prefix).unwrap_or(escaped_path);
    let mut parts = Vec::new();
    for raw in suffix.trim_matches('/').split('/') {
        let part = path_unescape(raw)?;
        if !part.is_empty() {
            parts.push(part);
        }
    }
    Some(parts)
}

/// legacy `splitResourceAction`.
fn split_resource_action(part: &str) -> (&str, &str) {
    match part.split_once(':') {
        Some((resource_id, action)) => (resource_id, action),
        None => (part, ""),
    }
}

/// legacy `url.PathUnescape`: percent-decoding only (`+` is left alone); invalid
/// escapes are an error.
fn path_unescape(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex_digit(*bytes.get(i + 1)?)?;
            let lo = hex_digit(*bytes.get(i + 2)?)?;
            out.push(hi * 16 + lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_digit(b: u8) -> Option<u8> {
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

    #[test]
    fn split_resource_action_matches_legacy() {
        assert_eq!(split_resource_action("analytics"), ("analytics", ""));
        assert_eq!(
            split_resource_action("analytics:getIamPolicy"),
            ("analytics", "getIamPolicy")
        );
        assert_eq!(split_resource_action(":x"), ("", "x"));
    }

    #[test]
    fn path_unescape_matches_legacy() {
        assert_eq!(path_unescape("plain").as_deref(), Some("plain"));
        assert_eq!(path_unescape("a%20b").as_deref(), Some("a b"));
        assert_eq!(path_unescape("a+b").as_deref(), Some("a+b"));
        assert_eq!(path_unescape("bad%zz"), None);
        assert_eq!(path_unescape("bad%2"), None);
    }

    #[test]
    fn multipart_parts_reads_metadata_and_media() {
        let body = b"--BOUND\r\nContent-Type: application/json\r\n\r\n{\"a\":1}\r\n--BOUND\r\nContent-Type: application/octet-stream\r\n\r\nrow-bytes\r\n--BOUND--\r\n";
        let parts = multipart_parts(body, "BOUND").expect("parse multipart");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], b"{\"a\":1}");
        assert_eq!(parts[1], b"row-bytes");
        assert!(multipart_parts(b"no delimiters here", "BOUND").is_none());
    }

    #[test]
    fn parse_media_type_extracts_boundary() {
        let (media_type, boundary) =
            parse_media_type("multipart/related; boundary=abc123").expect("parse");
        assert_eq!(media_type, "multipart/related");
        assert_eq!(boundary, "abc123");
        let (_, quoted) =
            parse_media_type("Multipart/Related; boundary=\"q-u_o.ted\"").expect("parse");
        assert_eq!(quoted, "q-u_o.ted");
    }
}
