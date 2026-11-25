//! `strict` auth mode: owner/principal/anonymous callers and dataset/table
//! IAM enforcement. Relaxed behavior is covered by the parity test files.

use devcloud_bigquery::responses::ApiResponse;
use devcloud_bigquery::routes::{handle, Request};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::strict_auth::parse_principals;

const OWNER: &str = "owner-token";
const ALICE: &str = "alice-token";
const BOB: &str = "bob-token";
const DS: &str = "/bigquery/v2/projects/p/datasets/analytics";

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-strict-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn server(mode: &str) -> Server {
    let mut server = Server::new(Config {
        project: "p".to_string(),
        auth_mode: mode.to_string(),
        bearer_token: OWNER.to_string(),
        storage_path: tempdir().to_string_lossy().into_owned(),
        ..Default::default()
    });
    for (token, member) in parse_principals(&format!(
        "{ALICE}=user:alice@example.com,{BOB}=serviceAccount:bob@other.test"
    )) {
        server.add_principal(token, member);
    }
    server
}

fn call(server: &Server, method: &str, target: &str, body: &str, token: &str) -> ApiResponse {
    let mut req = Request::new(method, target, body.as_bytes());
    if !token.is_empty() {
        req.authorization = format!("Bearer {token}");
    }
    handle(server, &req)
}

fn status(server: &Server, method: &str, target: &str, body: &str, token: &str) -> u16 {
    call(server, method, target, body, token).status
}

/// Dataset `analytics` with table `people` (one row), created by the owner.
fn seed(server: &Server) {
    let datasets = "/bigquery/v2/projects/p/datasets";
    let body = r#"{"datasetReference":{"datasetId":"analytics"}}"#;
    assert_eq!(status(server, "POST", datasets, body, OWNER), 200);
    let table = r#"{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING"}]}}"#;
    assert_eq!(
        status(server, "POST", &format!("{DS}/tables"), table, OWNER),
        200
    );
    let rows = r#"{"rows":[{"json":{"id":"1"}}]}"#;
    let insert = format!("{DS}/tables/people/insertAll");
    assert_eq!(status(server, "POST", &insert, rows, OWNER), 200);
}

fn set_policy(server: &Server, resource: &str, bindings: &str) {
    let body = format!(r#"{{"policy":{{"bindings":{bindings}}}}}"#);
    let r = call(
        server,
        "POST",
        &format!("{resource}:setIamPolicy"),
        &body,
        OWNER,
    );
    assert_eq!(r.status, 200, "{}", r.body_str());
}

#[test]
fn strict_requires_a_known_bearer_token() {
    let s = server("strict");
    let anon = call(&s, "GET", "/bigquery/v2/projects", "", "");
    assert_eq!(anon.status, 401);
    assert_eq!(status(&s, "GET", "/bigquery/v2/projects", "", "nope"), 401);
    assert_eq!(status(&s, "GET", "/bigquery/v2/projects", "", OWNER), 200);
}

#[test]
fn dataset_policy_grants_principals_dataset_and_table_access() {
    let s = server("strict");
    seed(&s);
    set_policy(
        &s,
        DS,
        r#"[{"role":"roles/bigquery.dataViewer","members":["user:alice@example.com"]}]"#,
    );
    let data = format!("{DS}/tables/people/data");
    let rows = call(&s, "GET", &data, "", ALICE);
    assert_eq!(rows.status, 200, "{}", rows.body_str());
    assert!(rows.body_str().contains("\"1\""));
    assert_eq!(status(&s, "GET", DS, "", ALICE), 200);
    assert_eq!(status(&s, "GET", &format!("{DS}/tables"), "", ALICE), 200);

    // A viewer cannot write or administer.
    let insert = format!("{DS}/tables/people/insertAll");
    let denied = call(
        &s,
        "POST",
        &insert,
        r#"{"rows":[{"json":{"id":"2"}}]}"#,
        ALICE,
    );
    assert_eq!(denied.status, 403);
    assert!(denied
        .body_str()
        .contains("Permission bigquery.tables.updateData denied for user:alice@example.com"));
    assert_eq!(
        status(&s, "DELETE", &format!("{DS}/tables/people"), "", ALICE),
        403
    );
    assert_eq!(
        status(
            &s,
            "POST",
            &format!("{DS}:setIamPolicy"),
            r#"{"policy":{}}"#,
            ALICE
        ),
        403
    );
    // Project-level calls (jobs, dataset list) are owner-only.
    let jobs = "/bigquery/v2/projects/p/jobs";
    assert_eq!(status(&s, "GET", jobs, "", ALICE), 403);
    assert_eq!(
        status(&s, "GET", "/bigquery/v2/projects/p/datasets", "", ALICE),
        403
    );
    // testIamPermissions needs no permission.
    let test = format!("{DS}:testIamPermissions");
    assert_eq!(
        status(&s, "POST", &test, r#"{"permissions":[]}"#, ALICE),
        200
    );

    // Routine and model calls follow the same dataset policy.
    assert_eq!(status(&s, "GET", &format!("{DS}/routines"), "", ALICE), 200);
    assert_eq!(status(&s, "GET", &format!("{DS}/models"), "", ALICE), 200);
    let routine = r#"{"routineReference":{"routineId":"f"},"routineType":"SCALAR_FUNCTION","language":"SQL","definitionBody":"1"}"#;
    assert_eq!(
        status(&s, "POST", &format!("{DS}/routines"), routine, ALICE),
        403
    );
    assert_eq!(status(&s, "GET", &format!("{DS}/models"), "", BOB), 403);

    // Bob has no binding; anonymous callers get 401.
    assert_eq!(status(&s, "GET", &data, "", BOB), 403);
    assert_eq!(status(&s, "GET", &data, "", ""), 401);
}

#[test]
fn table_policy_is_evaluated_together_with_the_dataset_policy() {
    let s = server("strict");
    seed(&s);
    let table = format!("{DS}/tables/people");
    set_policy(
        &s,
        &table,
        r#"[{"role":"roles/bigquery.dataEditor","members":["domain:other.test"]},
            {"role":"roles/bigquery.dataViewer","members":["allUsers"]},
            {"role":"roles/bigquery.admin","members":["user:alice@example.com"],
             "condition":{"title":"never","expression":"false"}}]"#,
    );
    let insert = format!("{table}/insertAll");
    let rows = r#"{"rows":[{"json":{"id":"2"}}]}"#;
    // domain: covers serviceAccount:bob@other.test.
    assert_eq!(status(&s, "POST", &insert, rows, BOB), 200);
    // allUsers can read the table, even anonymously, but not the dataset.
    assert_eq!(status(&s, "GET", &format!("{table}/data"), "", ""), 200);
    assert_eq!(status(&s, "GET", DS, "", ""), 401);
    // Alice's only write grant is conditional, so it does not apply.
    assert_eq!(status(&s, "POST", &insert, rows, ALICE), 403);
    // Conditions round-trip through getIamPolicy.
    let policy = call(&s, "POST", &format!("{table}:getIamPolicy"), "{}", OWNER);
    assert!(policy.body_str().contains("\"condition\""));
}

#[test]
fn non_strict_modes_ignore_principals_and_policies() {
    let s = server("relaxed");
    seed(&s);
    let data = format!("{DS}/tables/people/data");
    assert_eq!(status(&s, "GET", &data, "", ""), 200);
    assert_eq!(status(&s, "GET", &data, "", ALICE), 200);

    let s = server("bearer-dev");
    seed(&s);
    assert_eq!(status(&s, "GET", &data, "", OWNER), 200);
    assert_eq!(status(&s, "GET", &data, "", ALICE), 401);
}

const PROJECT_IAM: &str = "/v1/projects/p";

#[test]
fn project_policy_is_inherited_by_datasets_and_tables() {
    let s = server("strict");
    seed(&s);
    let data = format!("{DS}/tables/people/data");
    assert_eq!(status(&s, "GET", &data, "", ALICE), 403);
    set_policy(
        &s,
        PROJECT_IAM,
        r#"[{"role":"roles/bigquery.dataViewer","members":["user:alice@example.com"]}]"#,
    );
    assert_eq!(status(&s, "GET", &data, "", ALICE), 200);
    assert_eq!(status(&s, "GET", DS, "", ALICE), 200);
    let insert = format!("{DS}/tables/people/insertAll");
    let rows = r#"{"rows":[{"json":{"id":"2"}}]}"#;
    assert_eq!(status(&s, "POST", &insert, rows, ALICE), 403);
    // Queries stay owner-only even with project grants.
    assert_eq!(
        status(
            &s,
            "POST",
            "/bigquery/v2/projects/p/queries",
            r#"{"query":"SELECT 1"}"#,
            ALICE
        ),
        403
    );

    // Project IAM itself needs roles/owner on the project.
    let get = format!("{PROJECT_IAM}:getIamPolicy");
    assert_eq!(status(&s, "POST", &get, "{}", ALICE), 403);
    assert_eq!(status(&s, "POST", &get, "{}", ""), 401);
    let policy = call(&s, "POST", &get, "{}", OWNER);
    assert_eq!(policy.status, 200);
    assert!(policy.body_str().contains("roles/bigquery.dataViewer"));
    set_policy(
        &s,
        PROJECT_IAM,
        r#"[{"role":"roles/owner","members":["serviceAccount:bob@other.test"]}]"#,
    );
    assert_eq!(status(&s, "POST", &get, "{}", BOB), 200);
    assert_eq!(status(&s, "POST", &insert, rows, BOB), 200);
    assert_eq!(
        status(
            &s,
            "POST",
            &format!("{PROJECT_IAM}:setIamPolicy"),
            r#"{"policy":{}}"#,
            BOB
        ),
        200
    );
    assert_eq!(status(&s, "GET", &data, "", ALICE), 403);
    assert_eq!(
        status(
            &s,
            "POST",
            "/v1/projects/bad%2Fid:getIamPolicy",
            "{}",
            OWNER
        ),
        400
    );
}

#[test]
fn principals_with_dataset_create_own_the_datasets_they_create() {
    let s = server("strict");
    let datasets = "/bigquery/v2/projects/p/datasets";
    let body = r#"{"datasetReference":{"datasetId":"mine"}}"#;
    assert_eq!(status(&s, "POST", datasets, body, ALICE), 403);
    set_policy(
        &s,
        PROJECT_IAM,
        r#"[{"role":"roles/bigquery.user","members":["user:alice@example.com"]},
            {"role":"roles/viewer","members":["serviceAccount:bob@other.test"]}]"#,
    );
    let created = call(&s, "POST", datasets, body, ALICE);
    assert_eq!(created.status, 200, "{}", created.body_str());
    let created: serde_json::Value = serde_json::from_str(created.body_str()).unwrap();
    assert_eq!(
        created["access"],
        serde_json::json!([
            {"role": "WRITER", "specialGroup": "projectWriters"},
            {"role": "OWNER", "specialGroup": "projectOwners"},
            {"role": "READER", "specialGroup": "projectReaders"},
            {"role": "OWNER", "userByEmail": "alice@example.com"}
        ])
    );
    let mine = format!("{datasets}/mine");
    let table =
        r#"{"tableReference":{"tableId":"t"},"schema":{"fields":[{"name":"id","type":"STRING"}]}}"#;
    assert_eq!(
        status(&s, "POST", &format!("{mine}/tables"), table, ALICE),
        200
    );
    // projectReaders covers bob's project roles/viewer.
    assert_eq!(
        status(&s, "GET", &format!("{mine}/tables/t/data"), "", BOB),
        200
    );
    assert_eq!(
        status(&s, "POST", &format!("{mine}/tables"), table, BOB),
        403
    );
    // The owner token creating a dataset adds no access entries.
    let owner = call(
        &s,
        "POST",
        datasets,
        r#"{"datasetReference":{"datasetId":"o"}}"#,
        OWNER,
    );
    assert!(!owner.body_str().contains("access"));
    assert_eq!(
        status(
            &s,
            "DELETE",
            &format!("{mine}?deleteContents=true"),
            "",
            ALICE
        ),
        204
    );
}

#[test]
fn dataset_access_entries_authorize_principals() {
    let s = server("strict");
    seed(&s);
    let data = format!("{DS}/tables/people/data");
    let insert = format!("{DS}/tables/people/insertAll");
    let rows = r#"{"rows":[{"json":{"id":"2"}}]}"#;
    let patch = |access: &str| {
        let r = call(&s, "PATCH", DS, &format!(r#"{{"access":{access}}}"#), OWNER);
        assert_eq!(r.status, 200, "{}", r.body_str());
    };
    patch(r#"[{"role":"READER","userByEmail":"alice@example.com"}]"#);
    assert_eq!(status(&s, "GET", &data, "", ALICE), 200);
    assert_eq!(status(&s, "GET", DS, "", ALICE), 200);
    assert_eq!(status(&s, "POST", &insert, rows, ALICE), 403);
    assert_eq!(status(&s, "GET", &data, "", BOB), 403);
    // userByEmail also matches service accounts; WRITER can insert.
    patch(
        r#"[{"role":"WRITER","userByEmail":"bob@other.test"},{"role":"roles/bigquery.dataViewer","domain":"example.com"}]"#,
    );
    assert_eq!(status(&s, "POST", &insert, rows, BOB), 200);
    assert_eq!(status(&s, "GET", &data, "", ALICE), 200);
    assert_eq!(status(&s, "DELETE", DS, "", BOB), 403);
    patch(
        r#"[{"role":"OWNER","iamMember":"user:alice@example.com"},{"role":"READER","specialGroup":"allAuthenticatedUsers"}]"#,
    );
    assert_eq!(status(&s, "GET", &data, "", BOB), 200);
    assert_eq!(status(&s, "GET", &data, "", ""), 401);
    assert_eq!(
        status(
            &s,
            "POST",
            &format!("{DS}/tables/people:setIamPolicy"),
            r#"{"policy":{}}"#,
            ALICE
        ),
        200
    );
    // Authorized views grant callers nothing.
    patch(r#"[{"view":{"projectId":"p","datasetId":"analytics","tableId":"v"}}]"#);
    assert_eq!(status(&s, "GET", &data, "", ALICE), 403);
}
