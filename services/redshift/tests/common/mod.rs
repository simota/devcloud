//! Shared helpers for the parity test suites (legacy: server_test.rs helpers).
#![allow(dead_code)]

use std::collections::BTreeMap;

use devcloud_redshift::engine::QueryResult;
use devcloud_redshift::http_api::HttpResponse;
use devcloud_redshift::pgwire_codec::{
    put_cstring, read_message_payload, write_message, PG_PROTOCOL_VERSION,
};
use devcloud_redshift::snapshot::TableColumnSnapshot;
use devcloud_redshift::Server;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Mirrors `redshiftDataAPIRequest`: a `RedshiftData.<op>` POST.
pub fn data_api_request(server: &Server, operation: &str, body: &str) -> HttpResponse {
    let mut headers = BTreeMap::new();
    headers.insert(
        "content-type".to_string(),
        "application/x-amz-json-1.1".to_string(),
    );
    headers.insert(
        "x-amz-target".to_string(),
        format!("RedshiftData.{operation}"),
    );
    server.dispatch_http("POST", "/", "", &headers, body.as_bytes())
}

/// Mirrors `redshiftServerlessRequest`: a `RedshiftServerless.<op>` POST.
pub fn serverless_request(server: &Server, operation: &str, body: &str) -> HttpResponse {
    let mut headers = BTreeMap::new();
    headers.insert(
        "content-type".to_string(),
        "application/x-amz-json-1.1".to_string(),
    );
    headers.insert(
        "x-amz-target".to_string(),
        format!("RedshiftServerless.{operation}"),
    );
    server.dispatch_http("POST", "/", "", &headers, body.as_bytes())
}

/// Mirrors a form-encoded AWS Query POST to `/` (the control plane).
pub fn query_request(server: &Server, form_body: &str) -> HttpResponse {
    let mut headers = BTreeMap::new();
    headers.insert(
        "content-type".to_string(),
        "application/x-www-form-urlencoded".to_string(),
    );
    server.dispatch_http("POST", "/", "", &headers, form_body.as_bytes())
}

/// Mirrors `writeTestStartup` (params as a slice — legacy iterates a map in
/// random order; the order is not part of the protocol contract).
pub async fn write_test_startup<S: AsyncWrite + Unpin>(
    conn: &mut S,
    params: &[(&str, &str)],
) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&PG_PROTOCOL_VERSION.to_be_bytes());
    for (key, value) in params {
        put_cstring(&mut body, key);
        put_cstring(&mut body, value);
    }
    body.push(0);
    write_test_typed_message(conn, 0, &body).await
}

/// Mirrors `writeTestTypedMessage`.
pub async fn write_test_typed_message<S: AsyncWrite + Unpin>(
    conn: &mut S,
    message_type: u8,
    body: &[u8],
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    write_message(&mut buf, message_type, body).expect("frame test message");
    conn.write_all(&buf).await?;
    conn.flush().await
}

/// Mirrors `readTestMessage`.
pub async fn read_test_message<S: AsyncRead + Unpin>(conn: &mut S) -> (u8, Vec<u8>) {
    let mut message_type = [0u8; 1];
    conn.read_exact(&mut message_type)
        .await
        .expect("read message type");
    let mut length_bytes = [0u8; 4];
    conn.read_exact(&mut length_bytes)
        .await
        .expect("read message length");
    let length = u32::from_be_bytes(length_bytes) as usize;
    assert!(length >= 4, "invalid PostgreSQL message length");
    let mut payload = vec![0u8; length - 4];
    conn.read_exact(&mut payload)
        .await
        .expect("read message payload");
    (message_type[0], payload)
}

/// Mirrors `waitForReady`.
pub async fn wait_for_ready<S: AsyncRead + Unpin>(conn: &mut S) {
    loop {
        let (message_type, _) = read_test_message(conn).await;
        if message_type == b'Z' {
            return;
        }
    }
}

/// Mirrors `readTestBufferMessageTypes`.
pub fn read_test_buffer_message_types(buffer: &[u8]) -> Vec<u8> {
    let mut reader = buffer;
    let mut message_types = Vec::new();
    while !reader.is_empty() {
        let message_type = reader[0];
        reader = &reader[1..];
        read_message_payload(&mut reader).expect("read buffer message payload");
        message_types.push(message_type);
    }
    message_types
}

/// Mirrors legacy `bytes.Contains`.
pub fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// Mirrors legacy `resultContainsRow`: true when any row contains `values` as a
/// contiguous subsequence.
pub fn result_contains_row(result: &QueryResult, values: &[&str]) -> bool {
    for row in &result.rows {
        if row.len() < values.len() {
            continue;
        }
        for start in 0..=row.len() - values.len() {
            if values
                .iter()
                .enumerate()
                .all(|(i, value)| row[start + i] == *value)
            {
                return true;
            }
        }
    }
    false
}

/// Mirrors legacy `columnSnapshotHas`.
pub fn column_snapshot_has(
    columns: &[TableColumnSnapshot],
    name: &str,
    encoding: &str,
    default_value: &str,
    identity: bool,
) -> bool {
    columns.iter().any(|column| {
        column.name == name
            && column.encoding == encoding
            && column.default_value == default_value
            && column.identity == identity
    })
}
