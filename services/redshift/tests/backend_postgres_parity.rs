//! 1:1 port of `internal/services/redshift/backend/postgres/postgres_test.rs`
//! and `internal/services/redshift/postgres_backend_test.rs`.
//!
//! legacy drives the backend through a fake `database/sql` driver that returns
//! canned rows without a live PostgreSQL. The Rust backend is a hand-rolled
//! wire client, so the faithful analog is a fake *wire server*: a tiny
//! in-process TCP server that speaks the PostgreSQL backend protocol and
//! replies with the same canned RowDescription / DataRow / CommandComplete the
//! legacy fake driver returns. No live PostgreSQL is required — exactly like the legacy
//! tests, which never start one.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use devcloud_redshift::backend::SqlBackend;
use devcloud_redshift::backend_postgres::{
    command_tag, postgres_type_oid, wrap_error, Backend, Config,
};
use devcloud_redshift::pgwire_codec::{
    put_cstring, put_i16, put_i32, write_command_complete, write_data_row, write_message,
    write_row_description,
};

#[derive(Default)]
struct Counters {
    begins: AtomicUsize,
    commits: AtomicUsize,
    rollbacks: AtomicUsize,
}

struct FakeServer {
    dsn: String,
    counters: Arc<Counters>,
}

/// Spawns a fake PostgreSQL wire server. Each connection runs the
/// startup/auth/ReadyForQuery handshake and then answers simple queries with
/// canned result sets keyed off the query text (mirroring `fakeRowsForQuery`).
fn start_fake_server(fail_queries: bool) -> FakeServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake pg server");
    let addr = listener.local_addr().expect("local addr");
    let counters = Arc::new(Counters::default());
    let counters_thread = Arc::clone(&counters);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let counters = Arc::clone(&counters_thread);
            thread::spawn(move || {
                let _ = handle_conn(stream, counters, fail_queries);
            });
        }
    });
    FakeServer {
        dsn: format!(
            "postgres://dev:secret@127.0.0.1:{}/dev?sslmode=disable",
            addr.port()
        ),
        counters,
    }
}

fn handle_conn(
    mut stream: TcpStream,
    counters: Arc<Counters>,
    fail_queries: bool,
) -> std::io::Result<()> {
    // Startup message: untagged i32 length + body. Consume and ignore it.
    let _ = read_untagged(&mut stream)?;
    let mut out = Vec::new();
    // AuthenticationOk (R, code 0) + ReadyForQuery (Z, 'I').
    write_message(&mut out, b'R', &0i32.to_be_bytes())?;
    write_message(&mut out, b'Z', b"I")?;
    stream.write_all(&out)?;

    loop {
        let mut tag = [0u8; 1];
        if stream.read_exact(&mut tag).is_err() {
            return Ok(());
        }
        let payload = read_tagged_payload(&mut stream)?;
        match tag[0] {
            b'X' => return Ok(()), // Terminate
            b'Q' => {
                let query = cstr(&payload);
                let mut response = Vec::new();
                respond_to_query(&mut response, &query, &counters, fail_queries)?;
                stream.write_all(&response)?;
            }
            _ => {}
        }
    }
}

fn respond_to_query(
    out: &mut Vec<u8>,
    query: &str,
    counters: &Counters,
    fail_queries: bool,
) -> std::io::Result<()> {
    let normalized = query.to_lowercase();
    let trimmed = normalized.trim_start();
    match trimmed {
        "begin" => {
            counters.begins.fetch_add(1, Ordering::SeqCst);
            write_command_complete(out, "BEGIN")?;
            return ready(out);
        }
        "commit" => {
            counters.commits.fetch_add(1, Ordering::SeqCst);
            write_command_complete(out, "COMMIT")?;
            return ready(out);
        }
        "rollback" => {
            counters.rollbacks.fetch_add(1, Ordering::SeqCst);
            write_command_complete(out, "ROLLBACK")?;
            return ready(out);
        }
        _ => {}
    }

    if fail_queries {
        write_error(out, "42601", "syntax error")?;
        return ready(out);
    }

    if normalized.contains("information_schema.columns") {
        write_row_description(
            out,
            &[
                field("table_schema", 1043, -1),
                field("table_name", 1043, -1),
                field("table_type", 1043, -1),
                field("column_name", 1043, -1),
                field("data_type", 1043, -1),
            ],
        )?;
        write_data_row(
            out,
            &strs(&["public", "events", "BASE TABLE", "id", "integer"]),
        )?;
        write_data_row(
            out,
            &strs(&[
                "public",
                "events",
                "BASE TABLE",
                "payload",
                "character varying",
            ]),
        )?;
        write_command_complete(out, "SELECT 2")?;
        return ready(out);
    }

    if normalized.contains("mixed types") || normalized.contains("mixed") {
        // bool, numeric, timestamp, varchar, unknown(text); NULL last column.
        write_row_description(
            out,
            &[
                field("ok", 16, -1),
                field("amount", 1700, -1),
                field("created_at", 1114, -1),
                field("payload", 1043, 64),
                field("missing", 25, -1),
            ],
        )?;
        write_data_row_nullable(
            out,
            &[
                Some("true"),
                Some("12.34"),
                Some("2026-05-04 15:28:19"),
                Some("hello"),
                None,
            ],
        )?;
        write_command_complete(out, "SELECT 1")?;
        return ready(out);
    }

    if trimmed.starts_with("insert") {
        write_command_complete(out, "INSERT 0 1")?;
        return ready(out);
    }

    // Default: a single int4 "answer" = 42 (mirrors fakeRowsForQuery default).
    write_row_description(out, &[field("answer", 23, -1)])?;
    write_data_row(out, &strs(&["42"]))?;
    write_command_complete(out, "SELECT 1")?;
    ready(out)
}

fn ready(out: &mut Vec<u8>) -> std::io::Result<()> {
    write_message(out, b'Z', b"I")
}

fn write_error(out: &mut Vec<u8>, code: &str, message: &str) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.push(b'S');
    put_cstring(&mut body, "ERROR");
    body.push(b'C');
    put_cstring(&mut body, code);
    body.push(b'M');
    put_cstring(&mut body, message);
    body.push(0);
    write_message(out, b'E', &body)
}

fn write_data_row_nullable(out: &mut Vec<u8>, values: &[Option<&str>]) -> std::io::Result<()> {
    let mut body = Vec::new();
    put_i16(&mut body, values.len() as i16);
    for value in values {
        match value {
            None => put_i32(&mut body, -1),
            Some(text) => {
                put_i32(&mut body, text.len() as i32);
                body.extend_from_slice(text.as_bytes());
            }
        }
    }
    write_message(out, b'D', &body)
}

fn field(name: &str, type_oid: i32, type_size: i16) -> devcloud_redshift::pg_types::PgField {
    devcloud_redshift::pg_types::PgField {
        name: name.to_string(),
        type_oid,
        type_size,
    }
}

fn strs(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

fn read_untagged(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; len.saturating_sub(4)];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn read_tagged_payload(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; len.saturating_sub(4)];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn cstr(payload: &[u8]) -> String {
    let end = payload
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..end]).into_owned()
}

// ---- postgres_backend_test.rs ----

/// Mirrors `TestPostgresBackendExecMapsRowsAndFields`.
#[test]
fn postgres_backend_exec_maps_rows_and_fields() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let result = backend.exec("select 42 as answer").expect("exec");
    assert_eq!(result.tag, "SELECT 1");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "42");
    assert_eq!(result.fields.len(), 1);
    assert_eq!(result.fields[0].name, "answer");
    assert_eq!(result.fields[0].type_oid, 23);
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendTransactionCommitAndRollback`.
#[test]
fn postgres_backend_transaction_commit_and_rollback() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let mut tx = backend.begin().expect("begin");
    tx.exec("select 42 as answer").expect("tx exec");
    tx.commit().expect("commit");

    let mut tx = backend.begin().expect("second begin");
    tx.rollback().expect("rollback");

    assert_eq!(server.counters.begins.load(Ordering::SeqCst), 2);
    assert_eq!(server.counters.commits.load(Ordering::SeqCst), 1);
    assert_eq!(server.counters.rollbacks.load(Ordering::SeqCst), 1);
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendCatalogSnapshotMapsInformationSchema`.
#[test]
fn postgres_backend_catalog_snapshot_maps_information_schema() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let catalog = backend.catalog().expect("catalog");
    let table = catalog
        .find_table("public", "events")
        .expect("catalog missing public.events");
    assert_eq!(table.kind, "base table");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[1].data_type, "character varying");
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendErrorDoesNotLeakDSN`.
#[test]
fn postgres_backend_error_does_not_leak_dsn() {
    let server = start_fake_server(true);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let err = backend.exec("select fail").expect_err("exec should fail");
    let text = err.to_string();
    assert!(!text.contains("secret"), "error leaked DSN: {text}");
    assert!(!text.contains(&server.dsn), "error leaked DSN: {text}");
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendMapsMixedTypesAndNulls`.
#[test]
fn postgres_backend_maps_mixed_types_and_nulls() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let result = backend.exec("select mixed types").expect("exec");
    assert_eq!(result.tag, "SELECT 1");
    assert_eq!(result.fields.len(), 5);
    let want_oids = [16, 1700, 1114, 1043, 25];
    for (i, want) in want_oids.iter().enumerate() {
        assert_eq!(result.fields[i].type_oid, *want, "field {i} oid");
    }
    assert_eq!(result.rows.len(), 1);
    let want_row = ["true", "12.34", "2026-05-04 15:28:19", "hello", ""];
    for (i, want) in want_row.iter().enumerate() {
        assert_eq!(result.rows[0][i], *want, "row[{i}]");
    }
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendCommandTagForNonSelect`.
#[test]
fn postgres_backend_command_tag_for_non_select() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");

    let result = backend.exec("insert into events values (1)").expect("exec");
    assert_eq!(result.tag, "INSERT");
    assert_eq!(result.fields.len(), 0);
    assert_eq!(result.rows.len(), 0);
    let _ = backend.close();
}

/// Mirrors `TestPostgresBackendNilAndClosedStatesAreSafe` (the live-server part:
/// double Close is safe). The Rust `Backend` is constructed only via `open`, so
/// a nil-backend analog is not representable; the legacy nil cases are covered by
/// `postgres_backend_requires_external_dsn` (open errors) below.
#[test]
fn postgres_backend_double_close_is_safe() {
    let server = start_fake_server(false);
    let backend = Backend::open(Config {
        dsn: server.dsn.clone(),
        ..Config::default()
    })
    .expect("open");
    backend.close().expect("close");
    backend.close().expect("second close");
}

/// Mirrors `TestPostgresBackendRequiresExternalDSN`.
#[test]
fn postgres_backend_requires_external_dsn() {
    let err = match Backend::open(Config::default()) {
        Ok(_) => panic!("open should fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("external dsn"),
        "open error = {err}"
    );
}

// ---- postgres_test.rs: TestHelpers ----

/// Mirrors `TestHelpers` (commandTag / postgresTypeOID / wrapError).
#[test]
fn helpers() {
    assert_eq!(command_tag("  select 1", 3), "SELECT 3");
    assert_eq!(command_tag("update events set id = 2", 0), "UPDATE");
    assert_eq!(command_tag("  ", 0), "");

    let cases: &[(&str, i32)] = &[
        ("boolean", 16),
        ("smallint", 21),
        ("int", 23),
        ("bigint", 20),
        ("real", 700),
        ("double precision", 701),
        ("decimal", 1700),
        ("date", 1082),
        ("timestamp with time zone", 1184),
        ("char", 1042),
        ("unknown", 25),
    ];
    for (name, want) in cases {
        assert_eq!(postgres_type_oid(name), *want, "postgres_type_oid({name})");
    }

    let wrapped = wrap_error("exec", "boom");
    assert!(wrapped
        .to_string()
        .contains("postgres redshift backend exec"));
    assert_eq!(wrapped.to_string(), "postgres redshift backend exec: boom");
    assert_eq!(
        wrap_error("", "boom").to_string(),
        "postgres redshift backend: boom"
    );
}

// Mirrors `TestOpenAndBackendErrors` Open-with-missing-store path: connecting to
// an unreachable DSN fails with a wrapped "ping" error (not a leak).
#[test]
fn open_unreachable_dsn_reports_ping_error() {
    // Port 1 on loopback is reserved/unbound in test environments.
    let err = match Backend::open(Config {
        dsn: "postgres://dev:secret@127.0.0.1:1/dev?sslmode=disable".to_string(),
        ..Config::default()
    }) {
        Ok(_) => panic!("open should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("ping"), "open error = {err}");
    assert!(
        !err.to_string().contains("secret"),
        "open leaked DSN: {err}"
    );
}

// ---- SCRAM-SHA-256 SASL round-trip ----
//
// The daemon-managed PostgreSQL (`internal/app/managed_postgres.rs`) is started
// with `--auth-host=scram-sha-256`, so live TCP connections negotiate SASL. The
// fake server above answers AuthenticationOk directly; this one performs the
// full SCRAM-SHA-256 server side so the client's exchange is exercised
// in-process (RFC 5802 + RFC 7677, no channel binding). Password is "secret"
// (matching the DSN), salt/iterations are canned.

fn hmac256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(input: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input);
    h.finalize().to_vec()
}

fn pbkdf2(pw: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
    let mut block = salt.to_vec();
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac256(pw, &block);
    let mut out = u.clone();
    for _ in 1..iters {
        u = hmac256(pw, &u);
        for (o, b) in out.iter_mut().zip(u.iter()) {
            *o ^= b;
        }
    }
    out
}

fn start_scram_server(password: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind scram server");
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            thread::spawn(move || {
                let _ = handle_scram_conn(stream, password);
            });
        }
    });
    format!(
        "postgres://dev:{password}@127.0.0.1:{}/dev?sslmode=disable",
        addr.port()
    )
}

fn handle_scram_conn(mut stream: TcpStream, password: &str) -> std::io::Result<()> {
    use devcloud_s3::base64;
    // Startup message.
    let _ = read_untagged(&mut stream)?;

    // AuthenticationSASL (R, code 10): one mechanism, terminated by empty entry.
    let mut sasl = Vec::new();
    sasl.extend_from_slice(&10i32.to_be_bytes());
    put_cstring(&mut sasl, "SCRAM-SHA-256");
    sasl.push(0);
    let mut out = Vec::new();
    write_message(&mut out, b'R', &sasl)?;
    stream.write_all(&out)?;

    // SASLInitialResponse ('p'): mechanism (NUL) + i32 len + client-first.
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag)?;
    assert_eq!(tag[0], b'p');
    let payload = read_tagged_payload(&mut stream)?;
    let mut pos = 0;
    let _mech = {
        let s = cstr(&payload[pos..]);
        pos += s.len() + 1;
        s
    };
    let cf_len = u32::from_be_bytes([
        payload[pos],
        payload[pos + 1],
        payload[pos + 2],
        payload[pos + 3],
    ]) as usize;
    pos += 4;
    let client_first = String::from_utf8(payload[pos..pos + cf_len].to_vec()).unwrap();
    // gs2 header "n,," then "n=,r=<nonce>"; bare = everything after "n,,".
    let client_first_bare = client_first.strip_prefix("n,,").unwrap().to_string();
    let client_nonce = client_first_bare
        .split(',')
        .find_map(|a| a.strip_prefix("r="))
        .unwrap()
        .to_string();

    // server-first: r=<client+server nonce>, s=<salt>, i=<iters>.
    let salt = b"devcloud-scram-salt-001";
    let iters = 4096u32;
    let server_nonce = format!("{client_nonce}SERVERPART01234567");
    let server_first = format!("r={server_nonce},s={},i={iters}", base64::std_encode(salt));
    let mut sc = Vec::new();
    sc.extend_from_slice(&11i32.to_be_bytes()); // AuthenticationSASLContinue
    sc.extend_from_slice(server_first.as_bytes());
    let mut out = Vec::new();
    write_message(&mut out, b'R', &sc)?;
    stream.write_all(&out)?;

    // SASLResponse ('p'): client-final c=biws,r=<nonce>,p=<proof>.
    stream.read_exact(&mut tag)?;
    assert_eq!(tag[0], b'p');
    let payload = read_tagged_payload(&mut stream)?;
    let client_final = String::from_utf8(payload).unwrap();
    let proof_b64 = client_final
        .split(',')
        .find_map(|a| a.strip_prefix("p="))
        .unwrap();
    let cf_without_proof = format!("c=biws,r={server_nonce}");

    // Recompute and verify the client's proof, then send the server signature.
    let salted = pbkdf2(password.as_bytes(), salt, iters);
    let client_key = hmac256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let auth_message = format!("{client_first_bare},{server_first},{cf_without_proof}");
    let client_sig = hmac256(&stored_key, auth_message.as_bytes());
    let expected_proof: Vec<u8> = client_key
        .iter()
        .zip(client_sig.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    assert_eq!(
        base64::std_decode(proof_b64).unwrap(),
        expected_proof,
        "client proof mismatch"
    );
    let server_key = hmac256(&salted, b"Server Key");
    let server_sig = hmac256(&server_key, auth_message.as_bytes());

    // AuthenticationSASLFinal (R, code 12): v=<base64 server signature>.
    let mut sf = Vec::new();
    sf.extend_from_slice(&12i32.to_be_bytes());
    sf.extend_from_slice(format!("v={}", base64::std_encode(&server_sig)).as_bytes());
    let mut out = Vec::new();
    write_message(&mut out, b'R', &sf)?;
    // AuthenticationOk + ReadyForQuery.
    write_message(&mut out, b'R', &0i32.to_be_bytes())?;
    write_message(&mut out, b'Z', b"I")?;
    stream.write_all(&out)?;

    // Answer a single query so the test can assert a working connection.
    loop {
        if stream.read_exact(&mut tag).is_err() {
            return Ok(());
        }
        let payload = read_tagged_payload(&mut stream)?;
        match tag[0] {
            b'X' => return Ok(()),
            b'Q' => {
                let _ = cstr(&payload);
                let mut resp = Vec::new();
                write_row_description(&mut resp, &[field("answer", 23, 4)])?;
                write_data_row(&mut resp, &strs(&["42"]))?;
                write_command_complete(&mut resp, "SELECT 1")?;
                write_message(&mut resp, b'Z', b"I")?;
                stream.write_all(&resp)?;
            }
            _ => {}
        }
    }
}

/// The hand-rolled client completes a SCRAM-SHA-256 SASL handshake (the auth
/// mode the daemon-managed PostgreSQL uses for TCP) and can then run a query.
#[test]
fn postgres_backend_scram_sha256_handshake() {
    let dsn = start_scram_server("secret");
    let backend = Backend::open(Config {
        dsn,
        ..Config::default()
    })
    .expect("scram handshake should succeed");
    let result = backend.exec("select 1").expect("query after scram auth");
    assert_eq!(result.rows, vec![vec!["42".to_string()]]);
    backend.close().unwrap();
}
