//! The dashboard must keep working when Lambda / Cloud Run verify credentials:
//! forwarding is SigV4-signed (Lambda) or carries the bearer token (Cloud Run).
//! Exercised against the real services in strict mode, not a mock.

use std::collections::HashMap;
use std::sync::Arc;

use devcloud_dashboard::config::Config;
use devcloud_dashboard::http::{route, Request};

fn req(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Request {
    Request {
        method: method.to_string(),
        path: path.to_string(),
        raw_path: path.to_string(),
        query: String::new(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        body: body.to_vec(),
    }
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("devcloud-dash-strict-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn strict_lambda(dir: &std::path::Path) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(devcloud_lambda::Server::new(devcloud_lambda::Config {
        addr: addr.clone(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "devsecret".into(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    }));
    tokio::spawn(devcloud_lambda::http::serve(
        listener,
        server,
        std::future::pending(),
    ));
    format!("http://{addr}")
}

fn lambda_cfg(base: String) -> Config {
    Config {
        lambda_base: base,
        lambda_region: "us-east-1".into(),
        lambda_auth_mode: "strict".into(),
        lambda_access_key_id: "dev".into(),
        lambda_secret_access_key: "devsecret".into(),
        ..Config::default()
    }
}

#[tokio::test]
async fn lambda_forwarding_is_signed_for_strict_mode() {
    let dir = temp_dir("lambda");
    let base = strict_lambda(&dir).await;

    let resp = route(
        &lambda_cfg(base.clone()),
        &req("GET", "/api/lambda/functions", &[], b""),
    )
    .await;
    assert_eq!(resp.status, 200, "{}", String::from_utf8_lossy(&resp.body));

    // Unsigned forwarding (relaxed dashboard config) is what used to 403.
    let unsigned = Config {
        lambda_auth_mode: "relaxed".into(),
        ..lambda_cfg(base.clone())
    };
    let resp = route(&unsigned, &req("GET", "/api/lambda/functions", &[], b"")).await;
    assert_eq!(resp.status, 403);

    // Signed invoke reaches the function lookup (404: no such function).
    let resp = route(
        &lambda_cfg(base),
        &req("POST", "/api/lambda/functions/missing/invoke", &[], b"{}"),
    )
    .await;
    assert_eq!(resp.status, 404, "{}", String::from_utf8_lossy(&resp.body));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn dashboard_invoke_rejects_cross_site_origins() {
    let dir = temp_dir("csrf");
    let base = strict_lambda(&dir).await;
    let cfg = lambda_cfg(base);
    let evil = route(
        &cfg,
        &req(
            "POST",
            "/api/lambda/functions/f/invoke",
            &[("Origin", "https://evil.example")],
            b"{}",
        ),
    )
    .await;
    assert_eq!(evil.status, 403);
    let own = route(
        &cfg,
        &req(
            "POST",
            "/api/lambda/functions/f/invoke",
            &[("Origin", "http://127.0.0.1:18025")],
            b"{}",
        ),
    )
    .await;
    assert_eq!(
        own.status, 404,
        "the dashboard's own origin is allowed through"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cloudrun_forwarding_sends_bearer_for_strict_mode() {
    let dir = temp_dir("cloudrun");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(devcloud_cloudrun::Server::new(devcloud_cloudrun::Config {
        addr: addr.clone(),
        project: "p".into(),
        region: "us-central1".into(),
        auth_mode: "strict".into(),
        bearer_token: "secret".into(),
        storage_path: dir.to_string_lossy().into_owned(),
        docker_bin: None,
    }));
    tokio::spawn(devcloud_cloudrun::http::serve(
        listener,
        server,
        std::future::pending(),
    ));

    // Create a service through the Admin API with the token.
    let body = br#"{"template":{"containers":[{"image":"x"}]}}"#;
    let created =
        devcloud_dashboard::forward::forward(devcloud_dashboard::forward::ForwardRequest {
            base: &format!("http://{addr}"),
            method: "POST",
            path: "/v2/projects/p/locations/us-central1/services?serviceId=web",
            headers: vec![("Authorization".into(), "Bearer secret".into())],
            body: body.to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(created.status, 200);

    let cfg = Config {
        cloudrun_base: format!("http://{addr}"),
        cloudrun_auth_mode: "strict".into(),
        cloudrun_bearer_token: "secret".into(),
        ..Config::default()
    };
    let resp = route(
        &cfg,
        &req(
            "GET",
            "/api/cloudrun/services/p/us-central1/web/revisions",
            &[],
            b"",
        ),
    )
    .await;
    assert_eq!(resp.status, 200, "{}", String::from_utf8_lossy(&resp.body));
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["revisions"].as_array().unwrap().len(), 1);

    let no_token = Config {
        cloudrun_bearer_token: String::new(),
        ..cfg
    };
    let resp = route(
        &no_token,
        &req(
            "GET",
            "/api/cloudrun/services/p/us-central1/web/revisions",
            &[],
            b"",
        ),
    )
    .await;
    assert_eq!(resp.status, 401);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cloudrun_revisions_view_follows_every_page() {
    let dir = temp_dir("revpages");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(devcloud_cloudrun::Server::new(devcloud_cloudrun::Config {
        addr: addr.clone(),
        project: "p".into(),
        region: "us-central1".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    }));
    tokio::spawn(devcloud_cloudrun::http::serve(
        listener,
        server,
        std::future::pending(),
    ));
    let base = format!("http://{addr}");
    let send = |method: &'static str, path: String, body: String| {
        let base = base.clone();
        async move {
            devcloud_dashboard::forward::forward(devcloud_dashboard::forward::ForwardRequest {
                base: &base,
                method,
                path: &path,
                headers: Vec::new(),
                body: body.into_bytes(),
            })
            .await
            .unwrap()
            .status
        }
    };
    let template = |i: usize| {
        format!(
            r#"{{"template":{{"containers":[{{"image":"x","env":[{{"name":"N","value":"{i}"}}]}}]}}}}"#
        )
    };
    let svc = "/v2/projects/p/locations/us-central1/services";
    assert_eq!(
        send("POST", format!("{svc}?serviceId=web"), template(0)).await,
        200
    );
    for i in 1..=100 {
        assert_eq!(send("PATCH", format!("{svc}/web"), template(i)).await, 200);
    }

    let cfg = Config {
        cloudrun_base: base.clone(),
        ..Config::default()
    };
    let resp = route(
        &cfg,
        &req(
            "GET",
            "/api/cloudrun/services/p/us-central1/web/revisions",
            &[],
            b"",
        ),
    )
    .await;
    assert_eq!(resp.status, 200);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["revisions"].as_array().unwrap().len(), 101);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn applicationautoscaling_forwarding_is_signed_for_strict_mode() {
    let dir = temp_dir("aas");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(devcloud_applicationautoscaling::Server::new(
        devcloud_applicationautoscaling::Config {
            addr: addr.clone(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            auth_mode: "strict".into(),
            access_key_id: "dev".into(),
            secret_access_key: "devsecret".into(),
            storage_path: dir.to_string_lossy().into_owned(),
        },
    ));
    tokio::spawn(devcloud_applicationautoscaling::http::serve(
        listener,
        server,
        std::future::pending(),
    ));

    let cfg = Config {
        app_auto_scaling_base: format!("http://{addr}"),
        app_auto_scaling_region: "us-east-1".into(),
        app_auto_scaling_auth_mode: "strict".into(),
        app_auto_scaling_access_key_id: "dev".into(),
        app_auto_scaling_secret_access_key: "devsecret".into(),
        ..Config::default()
    };

    let reg = route(
        &cfg,
        &req(
            "POST",
            "/api/applicationautoscaling/scalable-targets",
            &[],
            br#"{"input":{"ResourceId":"table/strict-t","ScalableDimension":"dynamodb:table:ReadCapacityUnits","MinCapacity":1,"MaxCapacity":5}}"#,
        ),
    )
    .await;
    assert_eq!(reg.status, 200, "{}", String::from_utf8_lossy(&reg.body));

    let list = route(
        &cfg,
        &req(
            "GET",
            "/api/applicationautoscaling/scalable-targets",
            &[],
            b"",
        ),
    )
    .await;
    assert_eq!(list.status, 200, "{}", String::from_utf8_lossy(&list.body));
    let v: serde_json::Value = serde_json::from_slice(&list.body).unwrap();
    assert_eq!(v["scalableTargets"].as_array().unwrap().len(), 1);

    let unsigned = Config {
        app_auto_scaling_auth_mode: "relaxed".into(),
        ..cfg
    };
    let denied = route(
        &unsigned,
        &req(
            "GET",
            "/api/applicationautoscaling/scalable-targets",
            &[],
            b"",
        ),
    )
    .await;
    assert_eq!(denied.status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cloudrun_mutations_forward_bearer_in_strict_mode() {
    let dir = temp_dir("cloudrun-mutate");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = Arc::new(devcloud_cloudrun::Server::new(devcloud_cloudrun::Config {
        addr: addr.clone(),
        project: "p".into(),
        region: "us-central1".into(),
        auth_mode: "strict".into(),
        bearer_token: "secret".into(),
        storage_path: dir.to_string_lossy().into_owned(),
        docker_bin: None,
    }));
    tokio::spawn(devcloud_cloudrun::http::serve(
        listener,
        server,
        std::future::pending(),
    ));

    let cfg = Config {
        cloudrun_base: format!("http://{addr}"),
        cloudrun_project: "p".into(),
        cloudrun_region: "us-central1".into(),
        cloudrun_auth_mode: "strict".into(),
        cloudrun_bearer_token: "secret".into(),
        ..Config::default()
    };

    let created = route(
        &cfg,
        &req(
            "POST",
            "/api/cloudrun/services/create",
            &[],
            br#"{"serviceId":"api-svc","service":{"template":{"containers":[{"image":"us-docker.pkg.dev/cloudrun/container/hello"}]}}}"#,
        ),
    )
    .await;
    assert_eq!(
        created.status,
        200,
        "{}",
        String::from_utf8_lossy(&created.body)
    );

    let updated = route(
        &cfg,
        &req(
            "POST",
            "/api/cloudrun/services/p/us-central1/api-svc/update",
            &[],
            br#"{"service":{"template":{"containers":[{"image":"us-docker.pkg.dev/cloudrun/container/hello","env":[{"name":"V","value":"2"}]}]}}}"#,
        ),
    )
    .await;
    assert_eq!(
        updated.status,
        200,
        "{}",
        String::from_utf8_lossy(&updated.body)
    );

    let deleted = route(
        &cfg,
        &req(
            "DELETE",
            "/api/cloudrun/services/p/us-central1/api-svc",
            &[],
            br#"{"confirmation":"api-svc"}"#,
        ),
    )
    .await;
    assert_eq!(
        deleted.status,
        200,
        "{}",
        String::from_utf8_lossy(&deleted.body)
    );
    let _ = std::fs::remove_dir_all(dir);
}
