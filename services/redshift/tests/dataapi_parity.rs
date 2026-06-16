//! 1:1 port of `internal/services/redshift/dataapi_test.rs` and
//! `state_test.rs` (Data API statement lifecycle, pagination, redaction, and
//! state.json persistence of catalog rows + cluster metadata + statement
//! history/results).

mod common;

use std::time::SystemTime;

use common::{data_api_request, query_request, result_contains_row};
use devcloud_redshift::engine::QueryResult;
use devcloud_redshift::pg_types::{PgField, PG_TYPE_INT4_OID};
use devcloud_redshift::server::StatementRecord;
use devcloud_redshift::{Config, Server};

fn cfg() -> Config {
    Config::default()
}

fn execute_all(server: &Server, statements: &[&str]) {
    for statement in statements {
        server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute setup {statement:?}: {err}"));
    }
}

/// Extracts the `Id` field from an ExecuteStatement-style JSON response.
fn response_id(body: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(body).expect("decode response");
    value["Id"].as_str().unwrap_or_default().to_string()
}

#[test]
fn execute_statement_supports_create_table_as_select() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    execute_all(
        &server,
        &[
            "create table public.events(id integer, payload varchar(64))",
            "insert into public.events values (1, 'created')",
        ],
    );

    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"Database":"dev","DbUser":"dev","Sql":"create table public.created_events as select id, payload from public.events where id = 1"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);

    let result = server
        .execute_sql("select id, payload from public.created_events")
        .expect("select CTAS table");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string(), "created".to_string()]]
    );
}

#[test]
fn execute_describe_get_result_and_idempotency() {
    let server = devcloud_server();

    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1","ClientToken":"token-1"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);
    assert!(!id.is_empty());

    let retry = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1","ClientToken":"token-1"}"#,
    );
    assert_eq!(response_id(&retry.body), id, "idempotent Id");

    let describe = data_api_request(&server, "DescribeStatement", &format!(r#"{{"Id":"{id}"}}"#));
    assert_eq!(describe.status, 200, "body = {}", describe.body);
    let value: serde_json::Value = serde_json::from_str(&describe.body).unwrap();
    assert_eq!(value["Status"], "FINISHED");
    assert_eq!(value["ResultRows"], 1);
    assert_eq!(value["HasResultSet"], true);

    let result = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert_eq!(result.status, 200, "body = {}", result.body);
    for want in ["ColumnMetadata", "Records", "longValue"] {
        assert!(
            result.body.contains(want),
            "GetStatementResult missing {want:?}: {}",
            result.body
        );
    }
}

#[test]
fn result_fields_preserve_zero_false_and_double_types() {
    let server = devcloud_server();
    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 0 as zero_value, false as active, 1.5 as score"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    let result = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert_eq!(result.status, 200, "body = {}", result.body);
    for want in [
        r#""longValue":0"#,
        r#""booleanValue":false"#,
        r#""doubleValue":1.5"#,
        r#""typeName":"int4""#,
        r#""typeName":"bool""#,
        r#""typeName":"float8""#,
    ] {
        assert!(
            result.body.contains(want),
            "result missing {want:?}: {}",
            result.body
        );
    }
}

#[test]
fn get_statement_result_v2_returns_csv_records() {
    let server = devcloud_server();
    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1 as id, 'hello, csv' as payload","ResultFormat":"CSV"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    let result = data_api_request(
        &server,
        "GetStatementResultV2",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert_eq!(result.status, 200, "body = {}", result.body);
    for want in [
        r#""ResultFormat":"CSV""#,
        r#""CSVRecords":"1,\"hello, csv\"""#,
        r#""TotalNumRows":1"#,
    ] {
        assert!(
            result.body.contains(want),
            "v2 missing {want:?}: {}",
            result.body
        );
    }
}

#[test]
fn get_statement_result_v2_requires_csv_result_format() {
    let server = Server::new(cfg());
    let exec = data_api_request(&server, "ExecuteStatement", r#"{"Sql":"select 1"}"#);
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    let result = data_api_request(
        &server,
        "GetStatementResultV2",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert_eq!(result.status, 400, "body = {}", result.body);
    assert!(result.body.contains("ResultFormat CSV"));
}

#[test]
fn execute_statement_tracks_session_metadata() {
    let server = devcloud_server();
    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1","SessionKeepAliveSeconds":60}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let value: serde_json::Value = serde_json::from_str(&exec.body).unwrap();
    let id = value["Id"].as_str().unwrap_or_default().to_string();
    let session_id = value["SessionId"].as_str().unwrap_or_default().to_string();
    assert!(
        !id.is_empty() && !session_id.is_empty(),
        "execute response = {}",
        exec.body
    );

    let describe = data_api_request(&server, "DescribeStatement", &format!(r#"{{"Id":"{id}"}}"#));
    let describe_value: serde_json::Value = serde_json::from_str(&describe.body).unwrap();
    assert_eq!(describe_value["SessionId"], session_id);

    let batch = data_api_request(
        &server,
        "BatchExecuteStatement",
        &format!(
            r#"{{"Sqls":["select 1"],"SessionId":"{session_id}","SessionKeepAliveSeconds":120}}"#
        ),
    );
    assert_eq!(batch.status, 200, "body = {}", batch.body);
    let batch_value: serde_json::Value = serde_json::from_str(&batch.body).unwrap();
    assert_eq!(batch_value["SessionId"], session_id);

    let statements = server.statement_snapshots();
    assert!(
        statements.iter().any(|s| s.session_id == session_id),
        "statement snapshots missing session {session_id}"
    );
}

#[test]
fn execute_statement_rejects_invalid_session_keep_alive() {
    let server = Server::new(cfg());
    let rec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"Sql":"select 1","SessionKeepAliveSeconds":-1}"#,
    );
    assert_eq!(rec.status, 400, "body = {}", rec.body);
    assert!(rec.body.contains("SessionKeepAliveSeconds"));
}

#[test]
fn rejects_oversize_statements_without_persisting_sql() {
    let server = Server::new(Config {
        max_statement_bytes: 8,
        ..cfg()
    });
    let rec = data_api_request(&server, "ExecuteStatement", r#"{"Sql":"select 123456789"}"#);
    assert_eq!(rec.status, 400, "body = {}", rec.body);
    assert!(rec.body.contains("maxStatementBytes"));
    assert_eq!(
        server.statement_snapshots().len(),
        0,
        "oversize execute persisted history"
    );

    let batch = data_api_request(
        &server,
        "BatchExecuteStatement",
        r#"{"Sqls":["select 1","select 123456789"]}"#,
    );
    assert_eq!(batch.status, 400, "body = {}", batch.body);
    assert!(batch.body.contains("maxStatementBytes"));
    assert_eq!(
        server.statement_snapshots().len(),
        0,
        "oversize batch persisted history"
    );
}

#[test]
fn get_statement_result_paginates_rows() {
    let server = devcloud_server();
    execute_all(
        &server,
        &[
            "create table public.page_events(id integer, payload varchar(64))",
            "insert into public.page_events values (1, 'one')",
            "insert into public.page_events values (2, 'two')",
            "insert into public.page_events values (3, 'three')",
        ],
    );

    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select id, payload from public.page_events order by id"}"#,
    );
    let id = response_id(&exec.body);

    let first = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}","MaxResults":2}}"#),
    );
    let first_value: serde_json::Value = serde_json::from_str(&first.body).unwrap();
    assert_eq!(first_value["NextToken"], "2");
    assert_eq!(first_value["TotalNumRows"], 3);
    assert_eq!(first_value["Records"].as_array().unwrap().len(), 2);
    assert_eq!(first_value["Records"][0][1]["stringValue"], "one");

    let next = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}","MaxResults":2,"NextToken":"2"}}"#),
    );
    let next_value: serde_json::Value = serde_json::from_str(&next.body).unwrap();
    assert!(next_value.get("NextToken").is_none());
    assert_eq!(next_value["Records"].as_array().unwrap().len(), 1);
    assert_eq!(next_value["Records"][0][1]["stringValue"], "three");

    let invalid = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}","NextToken":"not-a-token"}}"#),
    );
    assert_eq!(invalid.status, 400, "body = {}", invalid.body);
    assert!(invalid.body.contains("NextToken is invalid"));
}

#[test]
fn batch_execute_statement_runs_statements_and_is_idempotent() {
    let server = devcloud_server();
    let exec = data_api_request(
        &server,
        "BatchExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sqls":["create schema if not exists batch","create table batch.events(id integer, payload varchar(64))","insert into batch.events values (1, 'created')","select id, payload from batch.events where id = 1"],"ClientToken":"batch-token-1"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);
    assert!(!id.is_empty());

    let retry = data_api_request(
        &server,
        "BatchExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sqls":["select 1"],"ClientToken":"batch-token-1"}"#,
    );
    assert_eq!(response_id(&retry.body), id, "idempotent Id");

    let describe = data_api_request(&server, "DescribeStatement", &format!(r#"{{"Id":"{id}"}}"#));
    assert!(
        describe.body.contains(r#""Status":"FINISHED""#),
        "describe = {}",
        describe.body
    );
    let result = data_api_request(
        &server,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert!(result.body.contains("created"), "result = {}", result.body);
}

#[test]
fn batch_execute_statement_rolls_back_on_failure() {
    let server = Server::new(cfg());
    let exec = data_api_request(
        &server,
        "BatchExecuteStatement",
        r#"{"Sqls":["create schema if not exists batch_fail","create table batch_fail.events(id integer)","insert into batch_fail.events values (1, 'extra')"]}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    let describe = data_api_request(&server, "DescribeStatement", &format!(r#"{{"Id":"{id}"}}"#));
    assert!(
        describe.body.contains(r#""Status":"FAILED""#),
        "describe = {}",
        describe.body
    );
    assert!(
        server
            .execute_sql("select * from batch_fail.events")
            .is_err(),
        "batch failure left table behind"
    );
}

#[test]
fn cancel_statement_and_list_status_filter() {
    let server = devcloud_server();
    let created_at = SystemTime::now();
    server.seed_statement(StatementRecord {
        id: "running".to_string(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        db_user: "dev".to_string(),
        session_id: String::new(),
        query_string: "select 1".to_string(),
        result_format: String::new(),
        created_at,
        updated_at: created_at,
        status: "STARTED".to_string(),
        error: String::new(),
        has_result_set: false,
        result: QueryResult::default(),
    });
    server.seed_statement(StatementRecord {
        id: "finished".to_string(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        db_user: "dev".to_string(),
        session_id: String::new(),
        query_string: "select 2".to_string(),
        result_format: String::new(),
        created_at,
        updated_at: created_at,
        status: "FINISHED".to_string(),
        error: String::new(),
        has_result_set: true,
        result: QueryResult {
            fields: vec![PgField {
                name: "?column?".to_string(),
                type_oid: PG_TYPE_INT4_OID,
                type_size: 4,
            }],
            rows: vec![vec!["2".to_string()]],
            tag: "SELECT 1".to_string(),
        },
    });

    let cancel = data_api_request(&server, "CancelStatement", r#"{"Id":"running"}"#);
    assert_eq!(cancel.status, 200, "body = {}", cancel.body);
    assert!(cancel.body.contains(r#""Status":true"#));

    let describe = data_api_request(&server, "DescribeStatement", r#"{"Id":"running"}"#);
    assert!(
        describe.body.contains(r#""Status":"ABORTED""#),
        "describe = {}",
        describe.body
    );

    let finished_cancel = data_api_request(&server, "CancelStatement", r#"{"Id":"finished"}"#);
    assert!(
        finished_cancel.body.contains(r#""Status":false"#),
        "body = {}",
        finished_cancel.body
    );

    let list = data_api_request(&server, "ListStatements", r#"{"Status":"ABORTED"}"#);
    assert_eq!(list.status, 200, "body = {}", list.body);
    assert!(list.body.contains(r#""Id":"running""#));
    assert!(!list.body.contains(r#""Id":"finished""#));
}

#[test]
fn metadata_lists_use_catalog() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        ..cfg()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists loop",
            "create table loop.events(id integer, payload varchar(64))",
        ],
    );

    let databases = data_api_request(&server, "ListDatabases", r#"{"Database":"dev"}"#);
    assert!(
        databases.body.contains(r#""dev""#),
        "databases = {}",
        databases.body
    );

    let schemas = data_api_request(&server, "ListSchemas", r#"{"Database":"dev"}"#);
    assert!(
        schemas.body.contains(r#""loop""#),
        "schemas = {}",
        schemas.body
    );

    let tables = data_api_request(
        &server,
        "ListTables",
        r#"{"Database":"dev","Schema":"loop"}"#,
    );
    assert!(
        tables.body.contains(r#""events""#),
        "tables = {}",
        tables.body
    );

    let describe = data_api_request(
        &server,
        "DescribeTable",
        r#"{"Database":"dev","Schema":"loop","Table":"events"}"#,
    );
    assert!(
        describe.body.contains(r#""ColumnList""#),
        "describe = {}",
        describe.body
    );
}

#[test]
fn metadata_lists_support_pattern_filters_and_pagination() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        ..cfg()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists alpha",
            "create schema if not exists loop",
            "create table alpha.metrics(id integer)",
            "create table loop.events(id integer, payload varchar(64))",
        ],
    );

    let first = data_api_request(
        &server,
        "ListSchemas",
        r#"{"Database":"dev","SchemaPattern":"%","MaxResults":1}"#,
    );
    assert!(
        first.body.contains(r#""NextToken":"1""#),
        "first schemas = {}",
        first.body
    );

    let next = data_api_request(
        &server,
        "ListSchemas",
        r#"{"Database":"dev","SchemaPattern":"%","MaxResults":1,"NextToken":"1"}"#,
    );
    assert!(
        next.body.contains(r#""loop""#),
        "next schemas = {}",
        next.body
    );

    let tables = data_api_request(
        &server,
        "ListTables",
        r#"{"Database":"dev","SchemaPattern":"lo%","TablePattern":"ev%"}"#,
    );
    assert!(
        tables.body.contains(r#""events""#) && !tables.body.contains(r#""metrics""#),
        "tables = {}",
        tables.body
    );

    let describe = data_api_request(
        &server,
        "DescribeTable",
        r#"{"Database":"dev","Schema":"loop","Table":"events","MaxResults":1}"#,
    );
    assert!(
        describe.body.contains(r#""NextToken":"1""#),
        "describe = {}",
        describe.body
    );
    assert!(describe.body.contains(r#""TableName":"events""#));

    let invalid = data_api_request(
        &server,
        "ListTables",
        r#"{"Database":"dev","NextToken":"not-a-token"}"#,
    );
    assert_eq!(invalid.status, 400, "body = {}", invalid.body);
    assert!(invalid.body.contains("NextToken is invalid"));
}

#[test]
fn statement_metadata_redacts_sensitive_sql() {
    let server = devcloud_server();
    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    for (operation, payload) in [
        ("DescribeStatement", format!(r#"{{"Id":"{id}"}}"#)),
        ("ListStatements", "{}".to_string()),
    ] {
        let rec = data_api_request(&server, operation, &payload);
        assert_eq!(rec.status, 200, "{operation} body = {}", rec.body);
        assert!(
            rec.body.contains(r#""QueryString":"[redacted]""#),
            "{operation} not redacted: {}",
            rec.body
        );
        assert!(!rec.body.contains("secret-role"));
        assert!(!rec.body.contains("s3://bucket/events.csv"));
    }
}

// --- state_test.rs ---------------------------------------------------------

#[test]
fn state_persists_catalog_rows_and_cluster_metadata() {
    let dir = temp_dir("catalog");
    let storage_path = dir.to_string_lossy().to_string();
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        storage_path: storage_path.clone(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists loop",
            "create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
            "insert into loop.events values (1, 'created')",
        ],
    );
    let create = query_request(
        &server,
        "Action=CreateCluster&ClusterIdentifier=analytics&DBName=warehouse&MasterUsername=analyst",
    );
    assert_eq!(create.status, 200, "body = {}", create.body);

    let reloaded = Server::new(Config {
        sql_addr: "127.0.0.1:25439".to_string(),
        storage_path,
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let result = reloaded
        .execute_sql("select id, payload from loop.events where id = 1")
        .expect("select after reload");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string(), "created".to_string()]]
    );

    let table_info = reloaded
        .execute_sql("select * from svv_table_info")
        .expect("svv_table_info after reload");
    assert!(
        result_contains_row(&table_info, &["loop", "events", "key", "id", "id", "1"]),
        "table metadata after reload = {:?}",
        table_info.rows
    );

    let snapshot = reloaded.service_snapshot();
    assert_eq!(snapshot.clusters.len(), 2, "clusters after reload");
    for cluster in &snapshot.clusters {
        assert_eq!(
            cluster.endpoint.port, 25439,
            "endpoint not normalized: {cluster:?}"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn state_persists_data_api_statement_history_and_results() {
    let dir = temp_dir("dataapi");
    let storage_path = dir.to_string_lossy().to_string();
    let server = Server::new(Config {
        storage_path: storage_path.clone(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });

    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1 as id","ClientToken":"persist-token"}"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);
    let id = response_id(&exec.body);

    let reloaded = Server::new(Config {
        storage_path,
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let retry = data_api_request(
        &reloaded,
        "ExecuteStatement",
        r#"{"ClusterIdentifier":"devcloud","Database":"dev","DbUser":"dev","Sql":"select 1 as id","ClientToken":"persist-token"}"#,
    );
    assert_eq!(retry.status, 200, "body = {}", retry.body);
    assert_eq!(response_id(&retry.body), id, "reloaded idempotent Id");

    let list = data_api_request(&reloaded, "ListStatements", "{}");
    assert_eq!(list.status, 200, "body = {}", list.body);
    assert!(list.body.contains(r#""Status":"FINISHED""#));
    assert!(list.body.contains("select 1 as id"));

    let result = data_api_request(
        &reloaded,
        "GetStatementResult",
        &format!(r#"{{"Id":"{id}"}}"#),
    );
    assert_eq!(result.status, 200, "body = {}", result.body);
    assert!(
        result.body.contains(r#""longValue":1"#),
        "result = {}",
        result.body
    );

    std::fs::remove_dir_all(&dir).ok();
}

fn devcloud_server() -> Server {
    Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    })
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-redshift-dataapi-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp storage dir");
    dir
}
