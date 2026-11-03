//! SSM Parameter Store behavior through the full request pipeline.

use devcloud_ssm::http::serve_for_test;
use devcloud_ssm::{Config, Server};
use serde_json::{json, Value};

fn server(storage: &str) -> Server {
    Server::new(Config {
        region: "ap-northeast-1".to_string(),
        account_id: "123456789012".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: storage.to_string(),
        ..Default::default()
    })
}

fn call(s: &Server, op: &str, body: Value) -> (u16, Option<String>, Value) {
    let target = format!("AmazonSSM.{op}");
    let resp = serve_for_test(
        s,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            ("X-Amz-Target", &target),
        ],
        body.to_string().as_bytes(),
    );
    (
        resp.status,
        resp.error_type,
        serde_json::from_slice(&resp.body).unwrap(),
    )
}

fn temp_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-ssm-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    dir.to_string_lossy().into_owned()
}

#[test]
fn put_then_get_string() {
    let s = server("");
    let (status, _, body) = call(
        &s,
        "PutParameter",
        json!({"Name": "/csa/dwh/user", "Value": "loader", "Type": "String"}),
    );
    assert_eq!(status, 200);
    assert_eq!(body, json!({"Version": 1, "Tier": "Standard"}));

    let (status, _, body) = call(&s, "GetParameter", json!({"Name": "/csa/dwh/user"}));
    assert_eq!(status, 200);
    let p = &body["Parameter"];
    assert_eq!(p["Name"], "/csa/dwh/user");
    assert_eq!(p["Type"], "String");
    assert_eq!(p["Value"], "loader");
    assert_eq!(p["Version"], 1);
    assert_eq!(p["DataType"], "text");
    assert_eq!(
        p["ARN"],
        "arn:aws:ssm:ap-northeast-1:123456789012:parameter/csa/dwh/user"
    );
    assert!(p["LastModifiedDate"].as_f64().unwrap() > 1_700_000_000.0);
}

#[test]
fn secure_string_requires_with_decryption() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "/csa/dwh/password", "Value": "s3cret", "Type": "SecureString"}),
    );

    let (_, _, plain) = call(
        &s,
        "GetParameter",
        json!({"Name": "/csa/dwh/password", "WithDecryption": true}),
    );
    assert_eq!(plain["Parameter"]["Value"], "s3cret");
    assert_eq!(plain["Parameter"]["Type"], "SecureString");

    let (_, _, opaque) = call(&s, "GetParameter", json!({"Name": "/csa/dwh/password"}));
    let value = opaque["Parameter"]["Value"].as_str().unwrap();
    assert_ne!(value, "s3cret");
    assert!(!value.contains("s3cret"));
}

#[test]
fn overwrite_rules_and_versioning() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "a", "Type": "String"}),
    );

    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "b", "Type": "String"}),
    );
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("ParameterAlreadyExists"))
    );

    // Type may be omitted on overwrite: the existing type is kept.
    let (status, _, body) = call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "b", "Overwrite": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["Version"], 2);

    let (_, _, body) = call(&s, "GetParameter", json!({"Name": "role"}));
    assert_eq!(body["Parameter"]["Value"], "b");
    assert_eq!(body["Parameter"]["Type"], "String");

    let (status, _, body) = call(&s, "GetParameter", json!({"Name": "role:2"}));
    assert_eq!(status, 200);
    assert_eq!(body["Parameter"]["Selector"], ":2");
    let (status, err, _) = call(&s, "GetParameter", json!({"Name": "role:1"}));
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("ParameterVersionNotFound"))
    );
}

#[test]
fn validation_errors() {
    let s = server("");
    let cases = [
        json!({"Name": "", "Value": "v", "Type": "String"}),
        json!({"Name": "a/b", "Value": "v", "Type": "String"}),
        json!({"Name": "/aws/x", "Value": "v", "Type": "String"}),
        json!({"Name": "SSM-x", "Value": "v", "Type": "String"}),
        json!({"Name": "bad name", "Value": "v", "Type": "String"}),
        json!({"Name": "x", "Value": "v", "Type": "Number"}),
        json!({"Name": "x", "Value": "", "Type": "String"}),
        json!({"Name": "x", "Type": "String"}),
        json!({"Name": "x", "Value": "v"}),
        json!({"Name": "x", "Value": "v".repeat(4097), "Type": "String"}),
    ];
    for body in cases {
        let (status, err, _) = call(&s, "PutParameter", body.clone());
        assert_eq!(
            (status, err.as_deref()),
            (400, Some("ValidationException")),
            "{body}"
        );
    }
    let (status, _, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "x", "Value": "v".repeat(4097), "Type": "String", "Tier": "Advanced"}),
    );
    assert_eq!(status, 200);
}

#[test]
fn missing_parameter_and_unknown_operation() {
    let s = server("");
    let (status, err, body) = call(&s, "GetParameter", json!({"Name": "/nope"}));
    assert_eq!((status, err.as_deref()), (400, Some("ParameterNotFound")));
    assert_eq!(body["__type"], "ParameterNotFound");
    let (status, err, _) = call(&s, "DeleteParameter", json!({"Name": "/nope"}));
    assert_eq!((status, err.as_deref()), (400, Some("ParameterNotFound")));
    let (status, err, _) = call(&s, "GetParametersByPath", json!({"Path": "/"}));
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("UnknownOperationException"))
    );
}

#[test]
fn delete_removes_parameter() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "gone", "Value": "v", "Type": "String"}),
    );
    let (status, _, body) = call(&s, "DeleteParameter", json!({"Name": "gone"}));
    assert_eq!((status, body), (200, json!({})));
    let (status, _, _) = call(&s, "GetParameter", json!({"Name": "gone"}));
    assert_eq!(status, 400);
}

#[test]
fn describe_lists_metadata_without_values_and_paginates() {
    let s = server("");
    for i in 0..3 {
        call(
            &s,
            "PutParameter",
            json!({"Name": format!("/p/{i}"), "Value": format!("secret-{i}"), "Type": "SecureString", "Description": "d"}),
        );
    }
    let (status, _, page1) = call(&s, "DescribeParameters", json!({"MaxResults": 2}));
    assert_eq!(status, 200);
    let names: Vec<_> = page1["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["Name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["/p/0", "/p/1"]);
    assert!(!page1.to_string().contains("secret-"));
    assert_eq!(page1["Parameters"][0]["KeyId"], "alias/aws/ssm");
    assert_eq!(page1["Parameters"][0]["Description"], "d");

    let token = page1["NextToken"].as_str().unwrap();
    let (_, _, page2) = call(
        &s,
        "DescribeParameters",
        json!({"MaxResults": 2, "NextToken": token}),
    );
    assert_eq!(page2["Parameters"].as_array().unwrap().len(), 1);
    assert!(page2.get("NextToken").is_none());

    let (status, err, _) = call(
        &s,
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Name", "Values": ["/p/0"]}]}),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
}

#[test]
fn state_persists_across_restart() {
    let dir = temp_dir("persist");
    {
        let s = server(&dir);
        call(
            &s,
            "PutParameter",
            json!({"Name": "/k", "Value": "v1", "Type": "SecureString"}),
        );
    }
    let s = server(&dir);
    assert!(s.load_err().is_none());
    let (status, _, body) = call(
        &s,
        "GetParameter",
        json!({"Name": "/k", "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["Parameter"]["Value"], "v1");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn rejects_wrong_method_path_and_content_type() {
    let s = server("");
    let r = serve_for_test(&s, "GET", "/", &[], b"");
    assert_eq!(r.status, 405);
    let r = serve_for_test(&s, "POST", "/x", &[], b"");
    assert_eq!(r.status, 404);
    let r = serve_for_test(
        &s,
        "POST",
        "/",
        &[
            ("Content-Type", "text/plain"),
            ("X-Amz-Target", "AmazonSSM.GetParameter"),
        ],
        b"{}",
    );
    assert_eq!(r.status, 400);
}

#[test]
fn strict_mode_requires_signature() {
    let s = Server::new(Config {
        auth_mode: "strict".to_string(),
        ..Default::default()
    });
    let r = serve_for_test(
        &s,
        "POST",
        "/",
        &[("X-Amz-Target", "AmazonSSM.GetParameter")],
        br#"{"Name":"x"}"#,
    );
    assert_eq!(r.status, 403);
}
