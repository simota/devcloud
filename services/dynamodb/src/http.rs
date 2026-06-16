//! AWS JSON 1.0 HTTP server and operation dispatch.
//!
//! Mirrors `internal/services/dynamodb/{routes,server}.rs`: a single `POST /`
//! endpoint dispatched by `X-Amz-Target` (`DynamoDB_20120810.<Op>`). Before each
//! operation the server expires TTL items, then decodes the JSON body into the
//! typed request, runs the op, and renders the result (or error) with legacy
//! `writeJSON`/`writeError` headers. A hand-rolled HTTP/1.1 reader/writer on
//! plain tokio keeps the dependency surface tiny (same pattern as the
//! applicationautoscaling/sqs crates).

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::errors::ApiError;
use crate::server::Server;
use crate::sigv4::{Credentials, SignedRequest};

const TARGET_PREFIX: &str = "DynamoDB_20120810.";
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// The outcome of an operation: HTTP status, the optional `X-Amzn-Errortype`
/// header (set on errors), and the already-encoded body bytes.
pub struct Outcome {
    pub status: u16,
    pub error_type: Option<String>,
    pub body: Vec<u8>,
}

impl Outcome {
    fn ok(body: Vec<u8>) -> Self {
        Outcome {
            status: 200,
            error_type: None,
            body,
        }
    }
    fn from_error(err: ApiError) -> Self {
        Outcome {
            status: err.status,
            error_type: Some(err.name.clone()),
            body: err.body_bytes(),
        }
    }
    /// Builds a bare error outcome (used by the request layer for malformed
    /// requests).
    pub fn error(status: u16, name: &str, message: &str) -> Self {
        Outcome::from_error(ApiError::new(status, name, message))
    }
}

impl Server {
    /// Dispatches a decoded request: verifies the signature, expires TTL items,
    /// then runs the operation named by `target` against `body`.
    pub fn dispatch(
        &mut self,
        target: &str,
        body: &[u8],
        creds_mode: &str,
        signed: Option<&SignedRequest>,
        now_unix: i64,
    ) -> Outcome {
        // SigV4 (no-op in relaxed mode).
        if let Some(req) = signed {
            let creds = Credentials {
                auth_mode: creds_mode,
                region: self.sigv4_region(),
                access_key_id: self.sigv4_access_key(),
                secret_access_key: self.sigv4_secret(),
            };
            if let Err(sig) = crate::sigv4::verify_signature(&creds, req) {
                return Outcome::error(sig.status, sig.code, sig.code);
            }
        }

        let Some(op) = target.strip_prefix(TARGET_PREFIX) else {
            return Outcome::error(400, "UnknownOperationException", "unknown operation");
        };

        // Expire TTL items before serving (mirrors the legacy middleware).
        if let Err(err) = self.expire_ttl_items(now_unix) {
            return Outcome::from_error(err);
        }

        macro_rules! run {
            ($req:ty, $call:expr) => {{
                match decode::<$req>(body) {
                    Ok(req) => to_outcome($call(self, &req)),
                    Err(out) => out,
                }
            }};
        }

        match op {
            "ListTables" => run!(crate::requests::ListTablesRequest, |s: &mut Server, r| s
                .list_tables(r)),
            "CreateTable" => run!(crate::requests::CreateTableRequest, |s: &mut Server, r| s
                .create_table(r)),
            "DescribeTable" => run!(
                crate::requests::TableNameRequest,
                |s: &mut Server, r: &crate::requests::TableNameRequest| s
                    .describe_table(&r.table_name)
            ),
            "DeleteTable" => run!(
                crate::requests::TableNameRequest,
                |s: &mut Server, r: &crate::requests::TableNameRequest| s
                    .delete_table(&r.table_name)
            ),
            "UpdateTable" => run!(crate::requests::UpdateTableRequest, |s: &mut Server, r| s
                .update_table(r)),
            "DescribeLimits" => Outcome::ok(self.describe_limits()),
            "DescribeEndpoints" => Outcome::ok(self.describe_endpoints()),
            "PutItem" => run!(crate::requests::PutItemRequest, |s: &mut Server, r| s
                .put_item(r)),
            "GetItem" => run!(crate::requests::GetItemRequest, |s: &mut Server, r| s
                .get_item(r)),
            "DeleteItem" => run!(crate::requests::DeleteItemRequest, |s: &mut Server, r| s
                .delete_item(r)),
            "UpdateItem" => run!(crate::requests::UpdateItemRequest, |s: &mut Server, r| s
                .update_item(r)),
            "Query" => run!(crate::requests::QueryRequest, |s: &mut Server, r| s
                .query(r)),
            "Scan" => run!(crate::requests::ScanRequest, |s: &mut Server, r| s.scan(r)),
            "BatchGetItem" => run!(crate::requests::BatchGetItemRequest, |s: &mut Server, r| s
                .batch_get_item(r)),
            "BatchWriteItem" => run!(
                crate::requests::BatchWriteItemRequest,
                |s: &mut Server, r| s.batch_write_item(r)
            ),
            "ExecuteStatement" => run!(
                crate::requests::ExecuteStatementRequest,
                |s: &mut Server, r| s.execute_statement(r)
            ),
            "BatchExecuteStatement" => {
                run!(
                    crate::requests::BatchExecuteStatementRequest,
                    |s: &mut Server, r| s.batch_execute_statement(r)
                )
            }
            "ExecuteTransaction" => run!(
                crate::requests::ExecuteTransactionRequest,
                |s: &mut Server, r| s.execute_transaction(r)
            ),
            "TransactGetItems" => run!(
                crate::requests::TransactGetItemsRequest,
                |s: &mut Server, r| s.transact_get_items(r)
            ),
            "TransactWriteItems" => run!(
                crate::requests::TransactWriteItemsRequest,
                |s: &mut Server, r| s.transact_write_items(r)
            ),
            "ListStreams" => run!(crate::requests::ListStreamsRequest, |s: &mut Server, r| s
                .list_streams(r)),
            "DescribeStream" => run!(
                crate::requests::DescribeStreamRequest,
                |s: &mut Server, r| s.describe_stream(r)
            ),
            "GetShardIterator" => run!(
                crate::requests::GetShardIteratorRequest,
                |s: &mut Server, r| s.get_shard_iterator(r)
            ),
            "GetRecords" => run!(crate::requests::GetRecordsRequest, |s: &mut Server, r| s
                .get_records(r)),
            "DescribeTimeToLive" => run!(
                crate::requests::TableNameRequest,
                |s: &mut Server, r: &crate::requests::TableNameRequest| s
                    .describe_time_to_live(&r.table_name)
            ),
            "UpdateTimeToLive" => run!(
                crate::requests::UpdateTimeToLiveRequest,
                |s: &mut Server, r| s.update_time_to_live(r)
            ),
            "DescribeContinuousBackups" => run!(
                crate::requests::TableNameRequest,
                |s: &mut Server, r: &crate::requests::TableNameRequest| s
                    .describe_continuous_backups(&r.table_name)
            ),
            "UpdateContinuousBackups" => run!(
                crate::requests::UpdateContinuousBackupsRequest,
                |s: &mut Server, r| s.update_continuous_backups(r)
            ),
            "CreateBackup" => run!(crate::requests::CreateBackupRequest, |s: &mut Server, r| s
                .create_backup(r)),
            "DescribeBackup" => run!(crate::requests::BackupArnRequest, |s: &mut Server, r| s
                .describe_backup(r)),
            "ListBackups" => run!(crate::requests::ListBackupsRequest, |s: &mut Server, r| s
                .list_backups(r)),
            "DeleteBackup" => run!(crate::requests::BackupArnRequest, |s: &mut Server, r| s
                .delete_backup(r)),
            "RestoreTableFromBackup" => run!(
                crate::requests::RestoreTableFromBackupRequest,
                |s: &mut Server, r| s.restore_table_from_backup(r)
            ),
            "TagResource" => run!(crate::requests::TagResourceRequest, |s: &mut Server, r| s
                .tag_resource(r)),
            "ListTagsOfResource" => run!(
                crate::requests::ListTagsOfResourceRequest,
                |s: &mut Server, r| s.list_tags_of_resource(r)
            ),
            "UntagResource" => run!(
                crate::requests::UntagResourceRequest,
                |s: &mut Server, r| s.untag_resource(r)
            ),
            "PutResourcePolicy" => run!(
                crate::requests::PutResourcePolicyRequest,
                |s: &mut Server, r| s.put_resource_policy(r)
            ),
            "GetResourcePolicy" => {
                run!(crate::requests::ResourceArnRequest, |s: &mut Server, r| s
                    .get_resource_policy(r))
            }
            "DeleteResourcePolicy" => {
                run!(crate::requests::ResourceArnRequest, |s: &mut Server, r| s
                    .delete_resource_policy(r))
            }
            _ => Outcome::error(400, "UnknownOperationException", "unknown operation"),
        }
    }
}

fn to_outcome(result: Result<Vec<u8>, ApiError>) -> Outcome {
    match result {
        Ok(body) => Outcome::ok(body),
        Err(err) => Outcome::from_error(err),
    }
}

/// Decodes the JSON request body into `T`, returning a SerializationException
/// outcome on failure (mirroring `decodeRequest`).
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, Outcome> {
    // An empty body decodes to the type's default (legacy decodes `{}` to zero value).
    let bytes: &[u8] = if body.is_empty() { b"{}" } else { body };
    serde_json::from_slice::<T>(bytes)
        .map_err(|_| Outcome::error(400, "SerializationException", "invalid json request"))
}

// --- socket server ---------------------------------------------------------

struct Request {
    method: String,
    path: String,
    query: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    host: String,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

/// Runs the accept loop until `shutdown` resolves. The `Server` is shared behind
/// a `Mutex` (its operations take `&mut self`).
pub async fn serve(
    listener: TcpListener,
    server: std::sync::Arc<Mutex<Server>>,
    auth_mode: String,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = std::sync::Arc::clone(&server);
                let mode = auth_mode.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, server, mode).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    server: std::sync::Arc<Mutex<Server>>,
    auth_mode: String,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let outcome = process(&server, &request, &auth_mode);
    write_response(&mut stream, outcome).await
}

fn process(server: &Mutex<Server>, req: &Request, auth_mode: &str) -> Outcome {
    // The read-only introspection API is intercepted BEFORE the provider-protocol
    // method/content-type checks (mirrors `handle()` in legacy routes.rs, which
    // matches `isIntrospectPath` first). SigV4 still runs (no-op in relaxed mode),
    // using the request's real path/query for the canonical request.
    if crate::introspect::is_introspect_path(&req.path) {
        let signed = SignedRequest {
            method: &req.method,
            path: &req.path,
            query: &req.query,
            host: &req.host,
            headers: &req.headers,
            body: &req.body,
        };
        let guard = server.lock().unwrap();
        if let Some(sig) = verify_sigv4(&guard, auth_mode, &signed) {
            return sig;
        }
        return guard.handle_introspect(&req.method, &req.path, &req.query);
    }

    if req.method != "POST" {
        return Outcome::error(405, "ValidationException", "method not allowed");
    }
    let content_type = req.header("content-type");
    if !content_type.is_empty() && !content_type.starts_with("application/x-amz-json-1.0") {
        return Outcome::error(400, "ValidationException", "unsupported content type");
    }
    let target = req.header("x-amz-target").to_string();
    // Build the signed-request view for SigV4 (used only in strict modes).
    let signed = SignedRequest {
        method: &req.method,
        path: "/",
        query: "",
        host: &req.host,
        headers: &req.headers,
        body: &req.body,
    };
    let now = crate::time_util::now_unix();
    let mut guard = server.lock().unwrap();
    guard.dispatch(&target, &req.body, auth_mode, Some(&signed), now)
}

/// Runs the SigV4 verifier for the introspection path, returning an error
/// outcome on failure (or `None` to proceed). Relaxed mode short-circuits inside
/// `verify_signature`.
fn verify_sigv4(server: &Server, auth_mode: &str, signed: &SignedRequest) -> Option<Outcome> {
    let creds = Credentials {
        auth_mode,
        region: server.sigv4_region(),
        access_key_id: server.sigv4_access_key(),
        secret_access_key: server.sigv4_secret(),
    };
    crate::sigv4::verify_signature(&creds, signed)
        .err()
        .map(|sig| Outcome::error(sig.status, sig.code, sig.code))
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
    let mut request_parts = request_line.split(' ');
    let method = request_parts.next().unwrap_or("").to_string();
    let target = request_parts.next().unwrap_or("/");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let host = headers.get("host").cloned().unwrap_or_default();

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
        query,
        headers,
        body,
        host,
    }))
}

async fn write_response(stream: &mut TcpStream, outcome: Outcome) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        outcome.status,
        reason_phrase(outcome.status)
    );
    head.push_str("Server: devcloud-dynamodb\r\n");
    head.push_str("Content-Type: application/x-amz-json-1.0\r\n");
    if let Some(error_type) = &outcome.error_type {
        head.push_str(&format!("X-Amzn-Errortype: {error_type}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n", outcome.body.len()));
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&outcome.body).await?;
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}
