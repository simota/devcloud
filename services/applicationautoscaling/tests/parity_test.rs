//! 1:1 parity port of `internal/services/applicationautoscaling/server_test.rs`,
//! plus byte-exact golden-oracle assertions captured from the legacy server.

use devcloud_applicationautoscaling::http::serve_for_test;
use devcloud_applicationautoscaling::{Config, Server};

fn temp_dir(tag: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("devcloud-aas-test-{tag}-{nanos}-{n}"));
    std::fs::create_dir_all(&p).unwrap();
    p.to_string_lossy().into_owned()
}

fn new_server(tag: &str) -> Server {
    Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        storage_path: temp_dir(tag),
        ..Default::default()
    })
}

/// Mirrors the legacy test helper `doRequest`: POST with the AWS JSON content-type
/// and the `AnyScaleFrontendService.<action>` target. Returns (status, body).
fn do_request(server: &Server, action: &str, body: &str) -> (u16, String) {
    let target = format!("AnyScaleFrontendService.{action}");
    let resp = serve_for_test(
        server,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            ("X-Amz-Target", &target),
        ],
        body.as_bytes(),
    );
    (resp.status, String::from_utf8(resp.body).unwrap())
}

#[test]
fn happy_path() {
    let server = new_server("happy");

    // RegisterScalableTarget → "{}".
    let (status, body) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10,"RoleARN":"arn:aws:iam::000000000000:role/dev"}"#,
    );
    assert_eq!(status, 200, "register status; body={body}");
    assert_eq!(body.trim(), "{}");

    // DescribeScalableTargets → 1 result with the capacities we set.
    let (status, body) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let targets = v["ScalableTargets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["MinCapacity"], 1);
    assert_eq!(targets[0]["MaxCapacity"], 10);

    // PutScalingPolicy (TargetTrackingScaling).
    let (status, body) = do_request(
        &server,
        "PutScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","PolicyType":"TargetTrackingScaling","TargetTrackingScalingPolicyConfiguration":{"TargetValue":70.0}}"#,
    );
    assert_eq!(status, 200, "put policy; body={body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let arn = v["PolicyARN"].as_str().unwrap();
    assert!(
        arn.starts_with("arn:aws:autoscaling:us-east-1:000000000000:scalingPolicy:"),
        "arn={arn}"
    );
    assert!(
        arn.contains(":resource/dynamodb/table/Orders:policyName/WriteScaling"),
        "arn={arn}"
    );
    assert!(v["Alarms"].is_array(), "Alarms must be a non-null array");

    // DescribeScalingPolicies → 1 result.
    let (status, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let policies = v["ScalingPolicies"].as_array().unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0]["PolicyName"], "WriteScaling");

    // DescribeScalingActivities → always empty.
    let (status, body) = do_request(
        &server,
        "DescribeScalingActivities",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 200);
    assert!(body.contains(r#""ScalingActivities":[]"#), "body={body}");

    // DeregisterScalableTarget.
    let (status, _) = do_request(
        &server,
        "DeregisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 200);

    // Targets now empty.
    let (_, body) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert!(body.contains(r#""ScalableTargets":[]"#), "body={body}");

    // Cascading delete: policies also gone.
    let (_, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert!(body.contains(r#""ScalingPolicies":[]"#), "body={body}");
}

#[test]
fn rejects_non_dynamodb_namespace() {
    let server = new_server("ns");
    let resp = serve_for_test(
        &server,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            (
                "X-Amz-Target",
                "AnyScaleFrontendService.RegisterScalableTarget",
            ),
        ],
        br#"{"ServiceNamespace":"ec2","ResourceId":"asg/foo","ScalableDimension":"ec2:asg:DesiredCapacity","MinCapacity":1,"MaxCapacity":5}"#,
    );
    assert_eq!(resp.status, 400);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains("ValidationException"), "body={body}");
    assert!(
        body.contains("only dynamodb namespace is supported"),
        "body={body}"
    );
    assert_eq!(resp.error_type.as_deref(), Some("ValidationException"));
}

#[test]
fn rejects_unknown_target() {
    let server = new_server("unk");
    let (status, body) = do_request(&server, "WhoKnows", "{}");
    assert_eq!(status, 400);
    assert!(body.contains("UnknownOperationException"), "body={body}");
}

#[test]
fn rejects_bad_content_type() {
    let server = new_server("ct");
    let resp = serve_for_test(
        &server,
        "POST",
        "/",
        &[
            ("Content-Type", "text/plain"),
            (
                "X-Amz-Target",
                "AnyScaleFrontendService.DescribeScalableTargets",
            ),
        ],
        b"{}",
    );
    assert_eq!(resp.status, 400);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains("ValidationException"), "body={body}");
}

#[test]
fn deregister_missing_returns_object_not_found() {
    let server = new_server("missing");
    let (status, body) = do_request(
        &server,
        "DeregisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Missing","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 400);
    assert!(body.contains("ObjectNotFoundException"), "body={body}");
}

#[test]
fn method_not_allowed() {
    let server = new_server("method");
    let resp = serve_for_test(&server, "GET", "/", &[], b"");
    assert_eq!(resp.status, 405);
    assert_eq!(resp.allow.as_deref(), Some("POST"));
}

// --- Byte-exact golden oracle (captured from the legacy server) ---

#[test]
fn error_body_matches_legacy_byte_for_byte() {
    let server = new_server("oracle-err");
    let resp = serve_for_test(
        &server,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            (
                "X-Amz-Target",
                "AnyScaleFrontendService.RegisterScalableTarget",
            ),
        ],
        br#"{"ServiceNamespace":"ec2"}"#,
    );
    // legacy writeError marshals a map (sorted keys) → __type before message, then
    // json.Encoder appends '\n'. serde_json::Map (BTreeMap) sorts the same way.
    let want = concat!(
        r#"{"__type":"com.amazonaws.application-autoscaling.v20160206#ValidationException","message":"only dynamodb namespace is supported"}"#,
        "\n"
    );
    assert_eq!(String::from_utf8(resp.body).unwrap(), want);
}

#[test]
fn empty_success_body_matches_legacy() {
    let server = new_server("oracle-empty");
    let (status, body) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/X","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":2}"#,
    );
    assert_eq!(status, 200);
    assert_eq!(body, "{}\n", "legacy writes `{{}}` + newline");
}

#[test]
fn state_json_matches_legacy_byte_for_byte() {
    let dir = temp_dir("oracle-state");
    let server = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        storage_path: dir.clone(),
        ..Default::default()
    });
    do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10}"#,
    );
    let got = std::fs::read_to_string(format!("{dir}/state.json")).unwrap();
    // CreationTime is volatile; blank it for the comparison. Everything else must
    // match the legacy server byte-for-byte — note `"SuspendedState":{}` is present
    // (legacy omitempty does not drop struct values) and there is no trailing
    // newline (legacy uses json.Marshal + os.WriteFile, not an Encoder).
    let normalized = blank_creation_time(&got);
    let want = r#"{"ScalableTargets":{"dynamodb|table/Orders|dynamodb:table:WriteCapacityUnits":{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10,"SuspendedState":{},"CreationTime":"X"}},"ScalingPolicies":{},"ScheduledActions":{},"Tags":{}}"#;
    assert_eq!(normalized, want);
}

/// Replaces the value of every `"CreationTime":"..."` with `"X"`.
fn blank_creation_time(s: &str) -> String {
    let needle = "\"CreationTime\":\"";
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find(needle) {
        out.push_str(&rest[..pos + needle.len()]);
        let after = &rest[pos + needle.len()..];
        let end = after.find('"').unwrap_or(0);
        out.push('X');
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

// --- SigV4 strict mode (exercises the ported signer end-to-end) ---

#[test]
fn strict_mode_rejects_missing_authorization() {
    let server = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        auth_mode: "strict".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        storage_path: temp_dir("strict-noauth"),
        ..Default::default()
    });
    let resp = serve_for_test(
        &server,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            (
                "X-Amz-Target",
                "AnyScaleFrontendService.DescribeScalableTargets",
            ),
        ],
        br#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(resp.status, 403);
    assert_eq!(resp.error_type.as_deref(), Some("AccessDeniedException"));
}

#[test]
fn relaxed_mode_allows_unsigned() {
    // Default auth_mode is empty → relaxed; unsigned requests pass.
    let server = new_server("relaxed");
    let (status, _) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(status, 200);
}
