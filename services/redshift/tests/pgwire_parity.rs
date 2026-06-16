//! 1:1 port of `internal/services/redshift/pgwire_test.rs`.
//!
//! Connection-level tests replace legacy `net.Pipe` + goroutine with
//! `tokio::io::duplex` + `tokio::spawn`; handler-level tests drive the
//! extended-protocol session over byte buffers exactly as legacy does.

mod common;

use std::sync::Arc;

use common::{
    contains_bytes, read_test_buffer_message_types, read_test_message, wait_for_ready,
    write_test_startup, write_test_typed_message,
};
use devcloud_redshift::pg_types::PG_TYPE_INT4_OID;
use devcloud_redshift::pgwire_codec::{put_cstring, put_i16, put_i32, PG_AUTH_CLEARTEXT};
use devcloud_redshift::{Config, ExtendedQuerySession, Server};

fn execute_all(server: &Server, statements: &[&str]) {
    for statement in statements {
        server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute setup {statement:?}: {err}"));
    }
}

fn parse_payload(name: &str, statement: &str, parameter_oids: &[i32]) -> Vec<u8> {
    let mut parse = Vec::new();
    put_cstring(&mut parse, name);
    put_cstring(&mut parse, statement);
    put_i16(&mut parse, parameter_oids.len() as i16);
    for oid in parameter_oids {
        put_i32(&mut parse, *oid);
    }
    parse
}

/// Mirrors `bindPayload`.
fn bind_payload(portal_name: &str, statement_name: &str) -> Vec<u8> {
    let mut bind = Vec::new();
    put_cstring(&mut bind, portal_name);
    put_cstring(&mut bind, statement_name);
    put_i16(&mut bind, 0);
    put_i16(&mut bind, 0);
    put_i16(&mut bind, 0);
    bind
}

/// Mirrors `executePayload`.
fn execute_payload(portal_name: &str, max_rows: i32) -> Vec<u8> {
    let mut execute = Vec::new();
    put_cstring(&mut execute, portal_name);
    put_i32(&mut execute, max_rows);
    execute
}

/// Mirrors `bindPayloadWithResultFormats`.
fn bind_payload_with_result_formats(
    portal_name: &str,
    statement_name: &str,
    formats: &[i16],
) -> Vec<u8> {
    let mut bind = Vec::new();
    put_cstring(&mut bind, portal_name);
    put_cstring(&mut bind, statement_name);
    put_i16(&mut bind, 0);
    put_i16(&mut bind, 0);
    put_i16(&mut bind, formats.len() as i16);
    for format in formats {
        put_i16(&mut bind, *format);
    }
    bind
}

/// Mirrors `bindPayloadWithTextParams`.
fn bind_payload_with_text_params(
    portal_name: &str,
    statement_name: &str,
    values: &[&str],
) -> Vec<u8> {
    let mut bind = Vec::new();
    put_cstring(&mut bind, portal_name);
    put_cstring(&mut bind, statement_name);
    put_i16(&mut bind, 0);
    put_i16(&mut bind, values.len() as i16);
    for value in values {
        put_i32(&mut bind, value.len() as i32);
        bind.extend_from_slice(value.as_bytes());
    }
    put_i16(&mut bind, 0);
    bind
}

fn describe_or_close_payload(target: u8, name: &str) -> Vec<u8> {
    let mut payload = vec![target];
    put_cstring(&mut payload, name);
    payload
}

#[tokio::test]
async fn pgwire_select_one_with_password_auth() {
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".to_string(),
        password: "dev".to_string(),
        ..Config::default()
    }));
    let (mut client, server_conn) = tokio::io::duplex(1 << 16);
    let conn_server = Arc::clone(&server);
    tokio::spawn(async move { conn_server.handle_sql_conn(server_conn).await });

    write_test_startup(
        &mut client,
        &[
            ("user", "dev"),
            ("database", "dev"),
            ("client_encoding", "UTF8"),
        ],
    )
    .await
    .expect("write startup");

    let (message_type, payload) = read_test_message(&mut client).await;
    assert!(
        message_type == b'R'
            && u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                == PG_AUTH_CLEARTEXT as u32,
        "auth request = {message_type:?} {payload:?}"
    );
    write_test_typed_message(&mut client, b'p', b"dev\x00")
        .await
        .expect("write password");
    wait_for_ready(&mut client).await;

    write_test_typed_message(&mut client, b'Q', b"select 1;\x00")
        .await
        .expect("write query");

    let mut saw_row = false;
    loop {
        let (message_type, payload) = read_test_message(&mut client).await;
        match message_type {
            b'D' => {
                assert!(
                    contains_bytes(&payload, b"1"),
                    "data row payload = {payload:?}"
                );
                saw_row = true;
            }
            b'Z' => {
                assert!(saw_row, "ReadyForQuery arrived before DataRow");
                let _ = write_test_typed_message(&mut client, b'X', &[]).await;
                return;
            }
            _ => {}
        }
    }
}

#[test]
fn pgwire_minimal_extended_query_select_one() {
    let server = Server::new(Config::default());
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(&mut wire, &parse_payload("stmt1", "select 1", &[]));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(&mut wire, &bind_payload("portal1", "stmt1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind responses"
    );

    wire.clear();
    session.handle_describe(
        &server,
        &mut wire,
        &describe_or_close_payload(b'P', "portal1"),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'T'],
        "describe responses"
    );

    wire.clear();
    session.handle_execute(&server, &mut wire, &execute_payload("portal1", 0));
    assert!(
        contains_bytes(&wire, b"SELECT 1"),
        "execute response missing command tag: {wire:?}"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'D', b'C'],
        "execute responses"
    );
}

#[test]
fn pgwire_extended_protocol_describe_prepared_statement_and_sync_recovery() {
    let server = Server::new(Config::default());
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(&mut wire, &parse_payload("stmt1", "select 1", &[]));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_describe(
        &server,
        &mut wire,
        &describe_or_close_payload(b'S', "stmt1"),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b't', b'T'],
        "describe prepared statement responses"
    );

    wire.clear();
    session.handle_bind(&mut wire, b"broken");
    assert!(
        session.failed,
        "protocol error did not mark extended session failed"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'E'],
        "protocol error responses"
    );

    session.handle_sync();
    wire.clear();
    session.handle_bind(&mut wire, &bind_payload("portal1", "stmt1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind after sync recovery responses"
    );
}

#[test]
fn pgwire_extended_protocol_bind_text_parameters_without_logging_values() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id int, payload varchar(64))",
            "insert into public.events(id, payload) values (777, 'alpha')",
        ],
    );
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(
        &mut wire,
        &parse_payload(
            "stmt1",
            "select payload from public.events where id = $1",
            &[PG_TYPE_INT4_OID],
        ),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(
        &mut wire,
        &bind_payload_with_text_params("portal1", "stmt1", &["777"]),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind responses"
    );

    wire.clear();
    session.handle_execute(&server, &mut wire, &execute_payload("portal1", 0));
    assert!(
        contains_bytes(&wire, b"alpha"),
        "execute response missing selected row: {wire:?}"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'D', b'C'],
        "execute responses"
    );

    let statements = server.statement_snapshots();
    assert_eq!(statements.len(), 1, "statement history count");
    assert_eq!(
        statements[0].query_preview, "select payload from public.events where id = $1",
        "statement history logged executable SQL with bind values: {:?}",
        statements[0]
    );
}

#[test]
fn pgwire_extended_protocol_describe_portal_with_text_parameters() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id int, payload varchar(64))",
            "insert into public.events(id, payload) values (42, 'portal-describe')",
        ],
    );
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(
        &mut wire,
        &parse_payload(
            "stmt1",
            "select payload from public.events where id = $1",
            &[PG_TYPE_INT4_OID],
        ),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(
        &mut wire,
        &bind_payload_with_text_params("portal1", "stmt1", &["42"]),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind responses"
    );

    wire.clear();
    session.handle_describe(
        &server,
        &mut wire,
        &describe_or_close_payload(b'P', "portal1"),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'T'],
        "describe portal responses"
    );
}

#[test]
fn pgwire_extended_protocol_rejects_binary_result_formats() {
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(&mut wire, &parse_payload("stmt1", "select 1", &[]));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(
        &mut wire,
        &bind_payload_with_result_formats("portal1", "stmt1", &[1]),
    );
    assert!(
        session.failed,
        "binary result format did not mark extended session failed"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'E'],
        "bind responses"
    );
    assert!(
        !contains_bytes(&wire, b"select 1"),
        "binary result format error leaked SQL text: {:?}",
        String::from_utf8_lossy(&wire)
    );
}

#[test]
fn pgwire_extended_protocol_execute_honors_max_rows_and_resumes_portal() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id int, payload varchar(64))",
            "insert into public.events(id, payload) values (1, 'one')",
            "insert into public.events(id, payload) values (2, 'two')",
        ],
    );
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(
        &mut wire,
        &parse_payload("stmt1", "select id, payload from public.events", &[]),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(&mut wire, &bind_payload("portal1", "stmt1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind responses"
    );

    wire.clear();
    session.handle_execute(&server, &mut wire, &execute_payload("portal1", 1));
    assert!(
        !contains_bytes(&wire, b"two"),
        "first execute returned more than maxRows: {wire:?}"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'D', b's'],
        "first execute responses"
    );

    wire.clear();
    session.handle_execute(&server, &mut wire, &execute_payload("portal1", 0));
    assert!(
        contains_bytes(&wire, b"two"),
        "second execute did not resume portal: {wire:?}"
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'D', b'C'],
        "second execute responses"
    );

    let statements = server.statement_snapshots();
    assert_eq!(statements.len(), 1, "statement history count");
}

#[test]
fn pgwire_extended_protocol_close_statement_and_portal() {
    let server = Server::new(Config::default());
    let mut session = ExtendedQuerySession::new();

    let mut wire = Vec::new();
    session.handle_parse(&mut wire, &parse_payload("stmt1", "select 1", &[]));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'1'],
        "parse responses"
    );

    wire.clear();
    session.handle_bind(&mut wire, &bind_payload("portal1", "stmt1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'2'],
        "bind responses"
    );

    wire.clear();
    session.handle_close(&mut wire, &describe_or_close_payload(b'P', "portal1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'3'],
        "close portal responses"
    );

    wire.clear();
    session.handle_describe(
        &server,
        &mut wire,
        &describe_or_close_payload(b'P', "portal1"),
    );
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'E'],
        "describe closed portal responses"
    );

    session.handle_sync();
    wire.clear();
    session.handle_close(&mut wire, &describe_or_close_payload(b'S', "stmt1"));
    assert_eq!(
        read_test_buffer_message_types(&wire),
        vec![b'3'],
        "close statement responses"
    );
}

#[tokio::test]
async fn pgwire_runs_multiple_sql_core_statements() {
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".to_string(),
        password: "dev".to_string(),
        ..Config::default()
    }));
    let (mut client, server_conn) = tokio::io::duplex(1 << 16);
    let conn_server = Arc::clone(&server);
    tokio::spawn(async move { conn_server.handle_sql_conn(server_conn).await });

    write_test_startup(&mut client, &[("user", "dev"), ("database", "dev")])
        .await
        .expect("write startup");
    read_test_message(&mut client).await;
    write_test_typed_message(&mut client, b'p', b"dev\x00")
        .await
        .expect("write password");
    wait_for_ready(&mut client).await;

    let sql = [
        "create schema if not exists loop",
        "create table loop.events(id integer encode raw, payload varchar(64)) distkey(id)",
        "insert into loop.events values (1, 'created')",
        "select id, payload from loop.events where id = 1",
    ]
    .join(";\n")
        + ";\x00";
    write_test_typed_message(&mut client, b'Q', sql.as_bytes())
        .await
        .expect("write query");

    let mut saw_created_payload = false;
    loop {
        let (message_type, payload) = read_test_message(&mut client).await;
        match message_type {
            b'D' => {
                if contains_bytes(&payload, b"created") {
                    saw_created_payload = true;
                }
            }
            b'Z' => {
                assert!(
                    saw_created_payload,
                    "ReadyForQuery arrived before selected row"
                );
                let _ = write_test_typed_message(&mut client, b'X', &[]).await;
                return;
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn pgwire_rejects_bad_password_without_leaking_value() {
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".to_string(),
        password: "dev".to_string(),
        ..Config::default()
    }));
    let (mut client, server_conn) = tokio::io::duplex(1 << 16);
    let conn_server = Arc::clone(&server);
    tokio::spawn(async move { conn_server.handle_sql_conn(server_conn).await });

    write_test_startup(&mut client, &[("user", "dev")])
        .await
        .expect("write startup");
    read_test_message(&mut client).await;
    write_test_typed_message(&mut client, b'p', b"wrong-secret\x00")
        .await
        .expect("write password");

    let (message_type, payload) = read_test_message(&mut client).await;
    assert_eq!(message_type, b'E', "message type, want ErrorResponse");
    assert!(
        !contains_bytes(&payload, b"wrong-secret"),
        "error leaked password: {:?}",
        String::from_utf8_lossy(&payload)
    );
}
