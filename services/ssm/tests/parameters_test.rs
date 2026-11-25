//! SSM Parameter Store behavior through the full request pipeline.

use devcloud_ssm::http::serve_for_test;
use devcloud_ssm::{Config, PolicyEvent, PolicyEventKind, Server};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot};

fn config(storage: &str) -> Config {
    Config {
        region: "ap-northeast-1".to_string(),
        account_id: "123456789012".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: storage.to_string(),
        ..Default::default()
    }
}

fn server(storage: &str) -> Server {
    Server::new(config(storage))
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

    // Previous version 1 is retained in version history and can be retrieved by selector.
    let (status, _, body) = call(&s, "GetParameter", json!({"Name": "role:1"}));
    assert_eq!(status, 200);
    assert_eq!(body["Parameter"]["Value"], "a");
    assert_eq!(body["Parameter"]["Version"], 1);
    assert_eq!(body["Parameter"]["Selector"], ":1");

    // Non-existent version returns ParameterVersionNotFound.
    let (status, err, _) = call(&s, "GetParameter", json!({"Name": "role:99"}));
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
    let (status, err, _) = call(&s, "NoSuchOperation", json!({}));
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
fn get_parameters_batch_and_delete_parameters() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "/app/host", "Value": "db.internal", "Type": "String"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/app/host", "Value": "db2.internal", "Overwrite": true}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/app/secret", "Value": "topsecret", "Type": "SecureString"}),
    );
    call(
        &s,
        "LabelParameterVersion",
        json!({"Name": "/app/host", "ParameterVersion": 1, "Labels": ["v1-release"]}),
    );

    // Without decryption: SecureString is masked, version and label selectors work, missing goes to InvalidParameters.
    let (status, _, body) = call(
        &s,
        "GetParameters",
        json!({
            "Names": [
                "/app/host",
                "/app/host:1",
                "/app/host:v1-release",
                "/app/secret",
                "/app/missing",
                "/app/host:99",
                "/app/host:no-such-label"
            ],
            "WithDecryption": false
        }),
    );
    assert_eq!(status, 200);
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 4);
    assert_eq!(params[0]["Name"], "/app/host");
    assert_eq!(params[0]["Value"], "db2.internal");
    assert_eq!(params[0]["Version"], 2);
    assert!(params[0].get("Selector").is_none());

    assert_eq!(params[1]["Name"], "/app/host");
    assert_eq!(params[1]["Value"], "db.internal");
    assert_eq!(params[1]["Version"], 1);
    assert_eq!(params[1]["Selector"], ":1");

    assert_eq!(params[2]["Name"], "/app/host");
    assert_eq!(params[2]["Value"], "db.internal");
    assert_eq!(params[2]["Version"], 1);
    assert_eq!(params[2]["Selector"], ":v1-release");

    assert_eq!(params[3]["Name"], "/app/secret");
    let secret_val = params[3]["Value"].as_str().unwrap();
    assert_ne!(secret_val, "topsecret");
    assert!(!secret_val.contains("topsecret"));

    let invalid = body["InvalidParameters"].as_array().unwrap();
    assert_eq!(
        invalid,
        &[
            json!("/app/missing"),
            json!("/app/host:99"),
            json!("/app/host:no-such-label")
        ]
    );

    // WithDecryption: true returns plaintext for SecureString.
    let (status, _, body) = call(
        &s,
        "GetParameters",
        json!({"Names": ["/app/secret"], "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["Parameters"][0]["Value"], "topsecret");

    // Validation: empty or >10 names.
    let (status, err, _) = call(&s, "GetParameters", json!({"Names": []}));
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
    let too_many: Vec<String> = (0..11).map(|i| format!("/p/{i}")).collect();
    let (status, err, _) = call(&s, "GetParameters", json!({"Names": too_many}));
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));

    // DeleteParameters deletes existing and reports missing.
    let (status, _, body) = call(
        &s,
        "DeleteParameters",
        json!({"Names": ["/app/host", "/app/missing"]}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["DeletedParameters"], json!(["/app/host"]));
    assert_eq!(body["InvalidParameters"], json!(["/app/missing"]));
}

#[test]
fn get_parameters_by_path_hierarchy_filters_and_pagination() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "/svc/a", "Value": "va", "Type": "String"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/svc/b", "Value": "vb-secret", "Type": "SecureString", "KeyId": "alias/custom-key"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/svc/sub/c", "Value": "vc", "Type": "StringList"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/svc-other/d", "Value": "vd", "Type": "String"}),
    );

    // Non-recursive under /svc returns only direct children (/svc/a and /svc/b), not /svc/sub/c or /svc-other/d.
    let (status, _, body) = call(&s, "GetParametersByPath", json!({"Path": "/svc"}));
    assert_eq!(status, 200);
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0]["Name"], "/svc/a");
    assert_eq!(params[0]["Value"], "va");
    assert_eq!(params[1]["Name"], "/svc/b");
    assert!(!params[1]["Value"].as_str().unwrap().contains("vb-secret"));

    // Recursive with WithDecryption and pagination.
    let (status, _, page1) = call(
        &s,
        "GetParametersByPath",
        json!({"Path": "/svc/", "Recursive": true, "WithDecryption": true, "MaxResults": 2}),
    );
    assert_eq!(status, 200);
    let p1 = page1["Parameters"].as_array().unwrap();
    assert_eq!(p1.len(), 2);
    assert_eq!(p1[0]["Name"], "/svc/a");
    assert_eq!(p1[1]["Name"], "/svc/b");
    assert_eq!(p1[1]["Value"], "vb-secret");
    let token = page1["NextToken"].as_str().unwrap();

    let (status, _, page2) = call(
        &s,
        "GetParametersByPath",
        json!({"Path": "/svc", "Recursive": true, "MaxResults": 2, "NextToken": token}),
    );
    assert_eq!(status, 200);
    let p2 = page2["Parameters"].as_array().unwrap();
    assert_eq!(p2.len(), 1);
    assert_eq!(p2[0]["Name"], "/svc/sub/c");
    assert!(page2.get("NextToken").is_none());

    // ParameterFilters by Type and KeyId.
    let (status, _, filtered) = call(
        &s,
        "GetParametersByPath",
        json!({
            "Path": "/svc",
            "Recursive": true,
            "WithDecryption": true,
            "ParameterFilters": [
                {"Key": "Type", "Option": "Equals", "Values": ["SecureString"]},
                {"Key": "KeyId", "Option": "BeginsWith", "Values": ["alias/custom"]}
            ]
        }),
    );
    assert_eq!(status, 200);
    let fp = filtered["Parameters"].as_array().unwrap();
    assert_eq!(fp.len(), 1);
    assert_eq!(fp[0]["Name"], "/svc/b");
    assert_eq!(fp[0]["Value"], "vb-secret");

    // ParameterFilters by Label selects the labeled version.
    call(
        &s,
        "PutParameter",
        json!({"Name": "/svc/a", "Value": "va-v2", "Overwrite": true}),
    );
    call(
        &s,
        "LabelParameterVersion",
        json!({"Name": "/svc/a", "ParameterVersion": 1, "Labels": ["stable"]}),
    );
    let (status, _, by_label) = call(
        &s,
        "GetParametersByPath",
        json!({
            "Path": "/svc",
            "ParameterFilters": [{"Key": "Label", "Values": ["stable"]}]
        }),
    );
    assert_eq!(status, 200);
    let lp = by_label["Parameters"].as_array().unwrap();
    assert_eq!(lp.len(), 1);
    assert_eq!(lp[0]["Name"], "/svc/a");
    assert_eq!(lp[0]["Version"], 1);
    assert_eq!(lp[0]["Value"], "va");
    assert_eq!(lp[0]["Selector"], ":stable");

    // Invalid Path or MaxResults.
    let (status, err, _) = call(&s, "GetParametersByPath", json!({"Path": "relative"}));
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
    let (status, err, _) = call(
        &s,
        "GetParametersByPath",
        json!({"Path": "/svc", "MaxResults": 11}),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
}

#[test]
fn labels_and_version_history() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "/cfg/secret", "Value": "sec-1", "Type": "SecureString", "KeyId": "arn:aws:kms:ap-northeast-1:123456789012:key/my-key"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/cfg/secret", "Value": "sec-2", "Overwrite": true, "Description": "second"}),
    );
    call(
        &s,
        "PutParameter",
        json!({"Name": "/cfg/secret", "Value": "sec-3", "Overwrite": true, "Description": "third"}),
    );

    // Attach label "prod" to version 1 and verify invalid labels are reported.
    let (status, _, lbl_resp) = call(
        &s,
        "LabelParameterVersion",
        json!({
            "Name": "/cfg/secret",
            "ParameterVersion": 1,
            "Labels": ["prod", "current", "aws-invalid", "123numeric"]
        }),
    );
    assert_eq!(status, 200);
    assert_eq!(lbl_resp["ParameterVersion"], 1);
    assert_eq!(
        lbl_resp["InvalidLabels"],
        json!(["aws-invalid", "123numeric"])
    );

    // GetParameter by label selector `:prod` returns version 1.
    let (status, _, get_v1) = call(
        &s,
        "GetParameter",
        json!({"Name": "/cfg/secret:prod", "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(get_v1["Parameter"]["Version"], 1);
    assert_eq!(get_v1["Parameter"]["Value"], "sec-1");
    assert_eq!(get_v1["Parameter"]["Selector"], ":prod");

    // Moving label "prod" to latest version (omitting ParameterVersion defaults to latest = 3).
    let (status, _, move_resp) = call(
        &s,
        "LabelParameterVersion",
        json!({"Name": "/cfg/secret", "Labels": ["prod"]}),
    );
    assert_eq!(status, 200);
    assert_eq!(move_resp["ParameterVersion"], 3);

    let (status, _, get_v3) = call(
        &s,
        "GetParameter",
        json!({"Name": "/cfg/secret:prod", "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(get_v3["Parameter"]["Version"], 3);
    assert_eq!(get_v3["Parameter"]["Value"], "sec-3");

    // UnlabelParameterVersion removes "current" from version 1 and reports unknown label in InvalidLabels.
    let (status, _, unlbl_resp) = call(
        &s,
        "UnlabelParameterVersion",
        json!({
            "Name": "/cfg/secret",
            "ParameterVersion": 1,
            "Labels": ["current", "not-attached"]
        }),
    );
    assert_eq!(status, 200);
    assert_eq!(unlbl_resp["RemovedLabels"], json!(["current"]));
    assert_eq!(unlbl_resp["InvalidLabels"], json!(["not-attached"]));

    // After unlabeling, `:current` returns ParameterVersionNotFound.
    let (status, err, _) = call(&s, "GetParameter", json!({"Name": "/cfg/secret:current"}));
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("ParameterVersionNotFound"))
    );

    // GetParameterHistory without decryption masks SecureString values, and paginates.
    let (status, _, hist1) = call(
        &s,
        "GetParameterHistory",
        json!({"Name": "/cfg/secret", "MaxResults": 2}),
    );
    assert_eq!(status, 200);
    let h1 = hist1["Parameters"].as_array().unwrap();
    assert_eq!(h1.len(), 2);
    assert_eq!(h1[0]["Version"], 1);
    assert_eq!(h1[0]["Labels"], json!([]));
    assert_eq!(
        h1[0]["KeyId"],
        "arn:aws:kms:ap-northeast-1:123456789012:key/my-key"
    );
    assert!(!h1[0]["Value"].as_str().unwrap().contains("sec-1"));
    assert_eq!(h1[1]["Version"], 2);
    assert_eq!(h1[1]["Description"], "second");

    let token = hist1["NextToken"].as_str().unwrap();
    let (status, _, hist2) = call(
        &s,
        "GetParameterHistory",
        json!({"Name": "/cfg/secret", "WithDecryption": true, "MaxResults": 2, "NextToken": token}),
    );
    assert_eq!(status, 200);
    let h2 = hist2["Parameters"].as_array().unwrap();
    assert_eq!(h2.len(), 1);
    assert_eq!(h2[0]["Version"], 3);
    assert_eq!(h2[0]["Value"], "sec-3");
    assert_eq!(h2[0]["Labels"], json!(["prod"]));
    assert!(hist2.get("NextToken").is_none());
}

#[test]
fn tags_lifecycle_and_validation() {
    let s = server("");
    // Creating parameter with initial tags.
    let (status, _, _) = call(
        &s,
        "PutParameter",
        json!({
            "Name": "/tagged/param",
            "Value": "v1",
            "Type": "String",
            "Tags": [
                {"Key": "env", "Value": "dev"},
                {"Key": "team", "Value": "core"}
            ]
        }),
    );
    assert_eq!(status, 200);

    // Overwrite + Tags in PutParameter is rejected.
    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({
            "Name": "/tagged/param",
            "Value": "v2",
            "Overwrite": true,
            "Tags": [{"Key": "env", "Value": "prod"}]
        }),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));

    // Overwrite without Tags preserves existing tags.
    let (status, _, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "/tagged/param", "Value": "v2", "Overwrite": true}),
    );
    assert_eq!(status, 200);

    let (status, _, listed) = call(
        &s,
        "ListTagsForResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/tagged/param"}),
    );
    assert_eq!(status, 200);
    assert_eq!(
        listed["TagList"],
        json!([
            {"Key": "env", "Value": "dev"},
            {"Key": "team", "Value": "core"}
        ])
    );

    // AddTagsToResource updates existing key and appends new key.
    let (status, _, _) = call(
        &s,
        "AddTagsToResource",
        json!({
            "ResourceType": "Parameter",
            "ResourceId": "/tagged/param",
            "Tags": [
                {"Key": "env", "Value": "prod"},
                {"Key": "cost-center", "Value": "100"}
            ]
        }),
    );
    assert_eq!(status, 200);

    // RemoveTagsFromResource removes specified tag keys.
    let (status, _, _) = call(
        &s,
        "RemoveTagsFromResource",
        json!({
            "ResourceType": "Parameter",
            "ResourceId": "/tagged/param",
            "TagKeys": ["team"]
        }),
    );
    assert_eq!(status, 200);

    let (status, _, listed) = call(
        &s,
        "ListTagsForResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/tagged/param"}),
    );
    assert_eq!(status, 200);
    assert_eq!(
        listed["TagList"],
        json!([
            {"Key": "env", "Value": "prod"},
            {"Key": "cost-center", "Value": "100"}
        ])
    );

    // Missing resource returns InvalidResourceId.
    let (status, err, _) = call(
        &s,
        "ListTagsForResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/no/such/param"}),
    );
    assert_eq!((status, err.as_deref()), (400, Some("InvalidResourceId")));
}

#[test]
fn policies_and_kms_key_id() {
    let s = server("");
    let policies_json = json!([
        {
            "Type": "Expiration",
            "Version": "1.0",
            "Attributes": {"Timestamp": "2026-12-01T00:00:00.000Z"}
        },
        {
            "Type": "ExpirationNotification",
            "Version": "1.0",
            "Attributes": {"Before": "15", "Unit": "Days"}
        },
        {
            "Type": "NoChangeNotification",
            "Version": "1.0",
            "Attributes": {"Before": "30", "Unit": "Days"}
        }
    ])
    .to_string();

    // Policies with explicit Standard tier are rejected.
    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({
            "Name": "/policy/param",
            "Value": "secret-val",
            "Type": "SecureString",
            "Tier": "Standard",
            "Policies": policies_json
        }),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));

    // Policies promote unspecified/Intelligent-Tiering or accept Advanced tier.
    let (status, _, put_body) = call(
        &s,
        "PutParameter",
        json!({
            "Name": "/policy/param",
            "Value": "secret-val",
            "Type": "SecureString",
            "KeyId": "alias/my-ssm-key",
            "Policies": policies_json
        }),
    );
    assert_eq!(status, 200);
    assert_eq!(put_body["Tier"], "Advanced");

    // DescribeParameters returns Policies metadata and KeyId without exposing Value.
    let (status, _, desc) = call(&s, "DescribeParameters", json!({}));
    assert_eq!(status, 200);
    let p = &desc["Parameters"][0];
    assert_eq!(p["Name"], "/policy/param");
    assert_eq!(p["Tier"], "Advanced");
    assert_eq!(p["KeyId"], "alias/my-ssm-key");
    assert!(p.get("Value").is_none());
    let policies = p["Policies"].as_array().unwrap();
    assert_eq!(policies.len(), 3);
    assert_eq!(policies[0]["PolicyType"], "Expiration");
    assert_eq!(policies[0]["PolicyStatus"], "Finished");
    assert_eq!(policies[1]["PolicyType"], "ExpirationNotification");
    assert_eq!(policies[2]["PolicyType"], "NoChangeNotification");

    // KeyId on non-SecureString is rejected.
    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({
            "Name": "/bad/keyid",
            "Value": "plain",
            "Type": "String",
            "KeyId": "alias/my-ssm-key"
        }),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
}

#[derive(Clone, Default)]
struct PolicyClock(Arc<AtomicU64>);

impl PolicyClock {
    fn server(&self, storage: &str) -> Arc<Server> {
        let clock = self.clone();
        Arc::new(Server::with_clock(config(storage), move || {
            UNIX_EPOCH + Duration::from_secs(1_798_761_600 + clock.0.load(Ordering::SeqCst))
        }))
    }

    async fn tick_at(&self, seconds_since_2027: u64) {
        self.0.store(seconds_since_2027, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
    }
}

async fn start_policy_server(
    server: Arc<Server>,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async move {
        devcloud_ssm::http::serve(listener, server, async {
            let _ = shutdown.await;
        })
        .await
        .unwrap();
    });
    (stop, task)
}

fn put_policies(server: &Server, name: &str, policies: Value) {
    let (status, _, _) = call(
        server,
        "PutParameter",
        json!({"Name": name, "Type": "SecureString", "Value": "test-value", "Policies": policies.to_string()}),
    );
    assert_eq!(status, 200);
}

fn expiration(timestamp: &str) -> Value {
    json!({"Type": "Expiration", "Version": "1.0", "Attributes": {"Timestamp": timestamp}})
}

fn notification(kind: &str, before: &str, unit: &str) -> Value {
    json!({"Type": kind, "Version": "1.0", "Attributes": {"Before": before, "Unit": unit}})
}

#[tokio::test(start_paused = true)]
async fn expiration_deletes_parameter_and_history_at_deadline_and_persists() {
    let clock = PolicyClock::default();
    let dir = temp_dir("expiration");
    let s = clock.server(&dir);
    put_policies(
        &s,
        "/policy/expired",
        json!([expiration("2026-12-31T23:59:59Z")]),
    );
    put_policies(
        &s,
        "/policy/active",
        json!([
            expiration("2027-01-02T00:00:00Z"),
            expiration("2027-01-01T09:00:02+09:00"),
            notification("ExpirationNotification", "1", "Hours"),
        ]),
    );
    assert_eq!(
        call(
            &s,
            "PutParameter",
            json!({"Name": "/policy/active", "Value": "updated", "Overwrite": true})
        )
        .0,
        200
    );
    let (stop, task) = start_policy_server(s.clone()).await;

    clock.tick_at(1).await;
    assert_eq!(
        call(&s, "GetParameter", json!({"Name": "/policy/expired"}))
            .1
            .as_deref(),
        Some("ParameterNotFound")
    );
    assert_eq!(
        call(&s, "GetParameter", json!({"Name": "/policy/active:1"})).0,
        200
    );
    clock.tick_at(2).await;
    for server in [&s, &clock.server(&dir)] {
        for (op, name) in [
            ("GetParameter", "/policy/active:1"),
            ("GetParameterHistory", "/policy/active"),
        ] {
            assert_eq!(
                call(server, op, json!({"Name": name})).1.as_deref(),
                Some("ParameterNotFound")
            );
        }
        assert_eq!(
            call(server, "DescribeParameters", json!({})).2["Parameters"],
            json!([])
        );
    }
    stop.send(()).unwrap();
    task.await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn notifications_fire_once_and_no_change_rearms_after_overwrite() {
    let clock = PolicyClock::default();
    let dir = temp_dir("policy-notifications");
    let s = clock.server(&dir);
    put_policies(
        &s,
        "watched",
        json!([
            expiration("2027-01-03T00:00:00Z"),
            notification("ExpirationNotification", "1", "Days"),
            notification("NoChangeNotification", "24", "Hours"),
        ]),
    );
    put_policies(
        &s,
        "without-expiration",
        json!([notification("ExpirationNotification", "1", "Days")]),
    );
    let mut events = s.subscribe_policy_events();
    let (stop, task) = start_policy_server(s.clone()).await;

    clock.tick_at(86_399).await;
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    clock.tick_at(86_400).await;
    for kind in [
        PolicyEventKind::ExpirationNotification,
        PolicyEventKind::NoChangeNotification,
    ] {
        assert_eq!(
            events.try_recv().unwrap(),
            PolicyEvent {
                kind,
                parameter_name: "watched".to_string(),
                parameter_version: 1,
            }
        );
    }
    clock.tick_at(86_401).await;
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    stop.send(()).unwrap();
    task.await.unwrap();

    let s = clock.server(&dir);
    let mut events = s.subscribe_policy_events();
    let (stop, task) = start_policy_server(s.clone()).await;
    clock.tick_at(86_402).await;
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );

    put_policies(
        &s,
        "rotation-only",
        json!([notification("NoChangeNotification", "1", "Hours")]),
    );
    assert_eq!(
        call(
            &s,
            "PutParameter",
            json!({"Name": "watched", "Value": "test-value", "Overwrite": true})
        )
        .0,
        200
    );
    clock.tick_at(90_002).await;
    assert_eq!(
        events.try_recv().unwrap(),
        PolicyEvent {
            kind: PolicyEventKind::NoChangeNotification,
            parameter_name: "rotation-only".to_string(),
            parameter_version: 1,
        }
    );
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    assert_eq!(
        call(
            &s,
            "PutParameter",
            json!({
                "Name": "rotation-only", "Value": "test-value", "Overwrite": true
            })
        )
        .0,
        200
    );
    clock.tick_at(93_601).await;
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    clock.tick_at(93_602).await;
    assert_eq!(
        events.try_recv().unwrap(),
        PolicyEvent {
            kind: PolicyEventKind::NoChangeNotification,
            parameter_name: "rotation-only".to_string(),
            parameter_version: 2,
        }
    );
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    stop.send(()).unwrap();
    task.await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn expiration_rejects_invalid_rfc3339_timestamp() {
    let s = server("");
    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "invalid", "Type": "String", "Value": "value", "Policies": json!([expiration("2027-02-30T00:00:00Z")]).to_string()}),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
}

#[tokio::test(start_paused = true)]
async fn policy_evaluation_retries_without_changes_or_events_after_persist_failure() {
    let clock = PolicyClock::default();
    let dir = temp_dir("policy-retry");
    let s = clock.server(&dir);
    put_policies(&s, "expires", json!([expiration("2027-01-01T01:00:00Z")]));
    put_policies(
        &s,
        "unmodified",
        json!([notification("NoChangeNotification", "1", "Hours")]),
    );
    let blocked_write = std::path::Path::new(&dir).join("state.json.tmp");
    std::fs::create_dir(&blocked_write).unwrap();
    let mut events = s.subscribe_policy_events();
    let (stop, task) = start_policy_server(s.clone()).await;

    clock.tick_at(3_600).await;
    assert_eq!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    );
    assert_eq!(call(&s, "GetParameter", json!({"Name": "expires"})).0, 200);
    std::fs::remove_dir(blocked_write).unwrap();

    clock.tick_at(3_601).await;
    assert_eq!(
        events.try_recv().unwrap(),
        PolicyEvent {
            kind: PolicyEventKind::NoChangeNotification,
            parameter_name: "unmodified".to_string(),
            parameter_version: 1,
        }
    );
    assert_eq!(
        call(&s, "GetParameter", json!({"Name": "expires"}))
            .1
            .as_deref(),
        Some("ParameterNotFound")
    );
    stop.send(()).unwrap();
    task.await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn state_persists_across_restart() {
    let dir = temp_dir("persist");
    {
        let s = server(&dir);
        call(
            &s,
            "PutParameter",
            json!({
                "Name": "/k",
                "Value": "v1",
                "Type": "SecureString",
                "KeyId": "alias/custom",
                "Tags": [{"Key": "env", "Value": "test"}]
            }),
        );
        call(
            &s,
            "PutParameter",
            json!({"Name": "/k", "Value": "v2", "Overwrite": true}),
        );
        call(
            &s,
            "LabelParameterVersion",
            json!({"Name": "/k", "ParameterVersion": 1, "Labels": ["initial"]}),
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
    assert_eq!(body["Parameter"]["Value"], "v2");
    assert_eq!(body["Parameter"]["Version"], 2);

    let (status, _, old) = call(
        &s,
        "GetParameter",
        json!({"Name": "/k:initial", "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(old["Parameter"]["Value"], "v1");
    assert_eq!(old["Parameter"]["Version"], 1);

    let (status, _, tags) = call(
        &s,
        "ListTagsForResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/k"}),
    );
    assert_eq!(status, 200);
    assert_eq!(tags["TagList"], json!([{"Key": "env", "Value": "test"}]));

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

fn sign_ssm_streaming(
    target: &str,
    payload_hash: &str,
    chunks: &[&[u8]],
    trailer: Option<&str>,
    tamper_chunk_sig: bool,
    override_body: Option<&[u8]>,
) -> (Vec<(String, String)>, Vec<u8>) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    fn sha(b: &[u8]) -> String {
        hex::encode(Sha256::digest(b))
    }
    fn hm(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    let host = "127.0.0.1:18031";
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let region = "us-east-1";
    let scope = format!("{date_stamp}/{region}/ssm/aws4_request");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-target";
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n"
    );
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha(canonical_request.as_bytes())
    );
    let k_date = hm(b"AWS4dev", date_stamp.as_bytes());
    let k_region = hm(&k_date, region.as_bytes());
    let k_service = hm(&k_region, b"ssm");
    let signing_key = hm(&k_service, b"aws4_request");
    let seed = hex::encode(hm(&signing_key, string_to_sign.as_bytes()));

    let decoded_len: usize = chunks.iter().map(|c| c.len()).sum();
    let body = match override_body {
        Some(raw) => raw.to_vec(),
        None => {
            let empty_hash = sha(b"");
            let mut prev = seed.clone();
            let mut out = Vec::new();
            for (idx, chunk) in chunks.iter().copied().chain([&b""[..]]).enumerate() {
                let to_sign = format!(
                    "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev}\n{empty_hash}\n{}",
                    sha(chunk)
                );
                let mut sig = hex::encode(hm(&signing_key, to_sign.as_bytes()));
                prev = sig.clone();
                if tamper_chunk_sig && idx == 0 {
                    let last = sig.pop().unwrap();
                    sig.push(if last == '0' { '1' } else { '0' });
                }
                out.extend(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).into_bytes());
                out.extend_from_slice(chunk);
                out.extend_from_slice(b"\r\n");
            }
            if let Some(trailer) = trailer {
                out.truncate(out.len() - 2);
                let to_sign = format!(
                    "AWS4-HMAC-SHA256-TRAILER\n{amz_date}\n{scope}\n{prev}\n{}",
                    sha(format!("{trailer}\n").as_bytes())
                );
                let sig = hex::encode(hm(&signing_key, to_sign.as_bytes()));
                out.extend(
                    format!("{trailer}\r\nx-amz-trailer-signature:{sig}\r\n\r\n").into_bytes(),
                );
            }
            out
        }
    };
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders={signed_headers}, Signature={seed}"
    );
    let headers = vec![
        ("Host".to_string(), host.to_string()),
        (
            "Content-Type".to_string(),
            "application/x-amz-json-1.1".to_string(),
        ),
        ("Content-Encoding".to_string(), "aws-chunked".to_string()),
        ("X-Amz-Target".to_string(), target.to_string()),
        ("X-Amz-Date".to_string(), amz_date.to_string()),
        ("X-Amz-Content-Sha256".to_string(), payload_hash.to_string()),
        (
            "X-Amz-Decoded-Content-Length".to_string(),
            decoded_len.to_string(),
        ),
        ("Authorization".to_string(), auth),
    ];
    (headers, body)
}

#[test]
fn streaming_payloads_execute_put_and_get_parameter_end_to_end() {
    use devcloud_ssm::sigv4::{
        STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER,
    };

    let s = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        auth_mode: "strict".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        ..Default::default()
    });

    // 1. Strict mode multi-chunk STREAMING-AWS4-HMAC-SHA256-PAYLOAD PutParameter.
    let put_body = br#"{"Name":"/stream/key","Value":"decoded-value","Type":"String"}"#;
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.PutParameter",
        STREAMING_SIGNED,
        &[&put_body[..25], &put_body[25..]],
        None,
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let put = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));

    // 2. Strict mode STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER GetParameter reads back value.
    let get_body = br#"{"Name":"/stream/key"}"#;
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED_TRAILER,
        &[get_body],
        Some("x-amz-checksum-crc32:AAAA"),
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let get = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(get.status, 200, "{}", String::from_utf8_lossy(&get.body));
    let parsed: Value = serde_json::from_slice(&get.body).unwrap();
    assert_eq!(parsed["Parameter"]["Value"], "decoded-value");

    // 3. Tampered chunk signature rejected with 403.
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED,
        &[get_body],
        None,
        true,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let tampered = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(tampered.status, 403);

    // 4. Malformed framing rejected with 400.
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED,
        &[get_body],
        None,
        false,
        Some(b"not-chunked"),
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let malformed = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(malformed.status, 400);

    // 5. Relaxed & signed-relaxed modes decode aws-chunked payload.
    for mode in ["relaxed", "signed-relaxed"] {
        let ms = Server::new(Config {
            region: "us-east-1".to_string(),
            auth_mode: mode.to_string(),
            ..Default::default()
        });
        let payload = br#"{"Name":"/relaxed/key","Value":"v","Type":"String"}"#;
        let chunked = format!(
            "{:x}\r\n{}\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n",
            payload.len(),
            std::str::from_utf8(payload).unwrap()
        );
        let sig = "a".repeat(64);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential=any/20260430/us-east-1/ssm/aws4_request, SignedHeaders=host;x-amz-date, Signature={sig}"
        );
        let mut hdrs = vec![
            ("Content-Type", "application/x-amz-json-1.1"),
            ("Content-Encoding", "aws-chunked"),
            ("X-Amz-Target", "AmazonSSM.PutParameter"),
            ("X-Amz-Date", "20260430T100000Z"),
            ("X-Amz-Content-Sha256", STREAMING_UNSIGNED_TRAILER),
        ];
        if mode == "signed-relaxed" {
            hdrs.push(("Authorization", &auth));
        }
        let put = serve_for_test(&ms, "POST", "/", &hdrs, chunked.as_bytes());
        assert_eq!(
            put.status,
            200,
            "{mode}: {}",
            String::from_utf8_lossy(&put.body)
        );
    }
}
