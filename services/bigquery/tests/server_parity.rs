//! 1:1 port of `internal/services/bigquery/server_test.rs`: projects list,
//! serviceAccount, the bearer-dev/strict auth modes, and the BigQuery error
//! envelope — all driven through the routing layer (`routes::handle`), the
//! equivalent of legacy `server.routes().ServeHTTP`.

use devcloud_bigquery::model::{ErrorResponse, ProjectsListResponse, ServiceAccountResponse};
use devcloud_bigquery::routes::{handle, Request};
use devcloud_bigquery::server::{Config, Server};

// legacy: TestProjectsListUsesBigQueryShape
#[test]
fn projects_list_uses_bigquery_shape() {
    let server = Server::new(Config {
        project: "local-project".to_string(),
        auth_mode: "relaxed".to_string(),
        ..Default::default()
    });
    let rec = handle(&server, &Request::new("GET", "/bigquery/v2/projects", b""));
    assert_eq!(rec.status, 200, "status = {}, want 200", rec.status);
    let response: ProjectsListResponse =
        serde_json::from_slice(&rec.body).expect("decode response");
    assert!(
        response.kind == "bigquery#projectList" && response.total_items == 1,
        "response metadata = {response:?}"
    );
    assert!(
        response.projects.len() == 1
            && response.projects[0].project_ref.project_id == "local-project",
        "projects = {:?}",
        response.projects
    );
}

// legacy: TestProjectServiceAccountUsesBigQueryShapeAndValidatesProjectID
#[test]
fn project_service_account_uses_bigquery_shape_and_validates_project_id() {
    let server = Server::new(Config {
        project: "local-project".to_string(),
        auth_mode: "relaxed".to_string(),
        ..Default::default()
    });

    let rec = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/serviceAccount",
            b"",
        ),
    );
    assert_eq!(rec.status, 200, "status = {}, want 200", rec.status);
    let response: ServiceAccountResponse =
        serde_json::from_slice(&rec.body).expect("decode response");
    assert!(
        response.kind == "bigquery#getServiceAccountResponse"
            && response.email == "devcloud-bigquery@local-project.iam.gserviceaccount.com",
        "service account response = {response:?}"
    );

    let invalid = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/bad.project/serviceAccount",
            b"",
        ),
    );
    assert_eq!(
        invalid.status,
        400,
        "invalid status = {}, body = {}",
        invalid.status,
        invalid.body_str()
    );
    assert!(
        !invalid.body_str().contains("devcloud-bigquery@bad.project"),
        "invalid project error leaked synthesized email: {}",
        invalid.body_str()
    );
}

// legacy: TestBearerDevModeRequiresMatchingBearerToken
#[test]
fn bearer_dev_mode_requires_matching_bearer_token() {
    let server = Server::new(Config {
        project: "local-project".to_string(),
        auth_mode: "bearer-dev".to_string(),
        bearer_token: "expected".to_string(),
        ..Default::default()
    });

    let unauthorized = handle(&server, &Request::new("GET", "/bigquery/v2/projects", b""));
    assert_eq!(
        unauthorized.status, 401,
        "unauthorized status = {}, want 401",
        unauthorized.status
    );
    assert!(
        unauthorized.www_authenticate,
        "401 must carry WWW-Authenticate"
    );
    assert!(
        !unauthorized.body_str().contains("expected"),
        "error response leaked bearer token: {}",
        unauthorized.body_str()
    );

    let mut req = Request::new("GET", "/bigquery/v2/projects", b"");
    req.authorization = "Bearer expected".to_string();
    let authorized = handle(&server, &req);
    assert_eq!(
        authorized.status, 200,
        "authorized status = {}, want 200",
        authorized.status
    );
}

// legacy: TestStrictModeRequiresMatchingBearerToken
#[test]
fn strict_mode_requires_matching_bearer_token() {
    let server = Server::new(Config {
        project: "local-project".to_string(),
        auth_mode: "strict".to_string(),
        bearer_token: "expected".to_string(),
        ..Default::default()
    });

    let mut wrong_req = Request::new("GET", "/bigquery/v2/projects", b"");
    wrong_req.authorization = "Bearer wrong".to_string();
    let wrong_token = handle(&server, &wrong_req);
    assert_eq!(
        wrong_token.status, 401,
        "wrong token status = {}, want 401",
        wrong_token.status
    );
    assert!(
        !wrong_token.body_str().contains("expected") && !wrong_token.body_str().contains("wrong"),
        "error response leaked bearer token: {}",
        wrong_token.body_str()
    );
}

// legacy: TestNotFoundUsesBigQueryErrorShape
#[test]
fn not_found_uses_bigquery_error_shape() {
    let server = Server::new(Config {
        project: "local-project".to_string(),
        ..Default::default()
    });
    let rec = handle(&server, &Request::new("GET", "/missing", b""));
    assert_eq!(rec.status, 404, "status = {}, want 404", rec.status);
    let response: ErrorResponse = serde_json::from_slice(&rec.body).expect("decode error");
    assert!(
        response.error.code == 404 && response.error.status == "NOT_FOUND",
        "error = {:?}",
        response.error
    );
    assert!(
        response.error.errors.len() == 1 && response.error.errors[0].reason == "notFound",
        "error details = {:?}",
        response.error.errors
    );
}
