//! Regression tests for OMEN-4: every mutating handler in `server.rs` must roll
//! back its in-memory state when `persist()` fails, mirroring the pattern
//! established for sqs (`server_test.rs`) and pubsub (`topic_parity.rs` /
//! `subscription_parity.rs`). Storage-path corruption (replacing the storage
//! directory with a plain file) forces `persist()`'s `create_dir_all` to fail.

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
    let p = std::env::temp_dir().join(format!("devcloud-aas-rollback-{tag}-{nanos}-{n}"));
    std::fs::create_dir_all(&p).unwrap();
    p.to_string_lossy().into_owned()
}

fn new_server(tag: &str) -> (Server, String) {
    let dir = temp_dir(tag);
    let server = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        storage_path: dir.clone(),
        ..Default::default()
    });
    (server, dir)
}

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

/// Replaces the storage directory with a plain file, so `persist()`'s
/// `create_dir_all` fails on the next mutating call.
fn corrupt_storage(dir: &str) {
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::write(dir, b"not a directory").unwrap();
}

/// Restores the storage directory so subsequent calls persist normally again.
fn repair_storage(dir: &str) {
    std::fs::remove_file(dir).unwrap();
    std::fs::create_dir_all(dir).unwrap();
}

const TARGET: &str = r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#;

#[test]
fn register_scalable_target_rolls_back_update_when_persist_fails() {
    let (server, dir) = new_server("register-update");
    let (status, _) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10}"#,
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":5,"MaxCapacity":50}"#,
    );
    assert_eq!(status, 500);

    // Memory must still reflect the pre-failure capacities, not the failed update.
    let (_, body) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let targets = v["ScalableTargets"].as_array().unwrap();
    assert_eq!(targets.len(), 1, "body={body}");
    assert_eq!(targets[0]["MinCapacity"], 1);
    assert_eq!(targets[0]["MaxCapacity"], 10);

    repair_storage(&dir);
    let (status, _) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":5,"MaxCapacity":50}"#,
    );
    assert_eq!(status, 200);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn register_scalable_target_rolls_back_new_entry_when_persist_fails() {
    let (server, dir) = new_server("register-new");
    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10}"#,
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert!(body.contains(r#""ScalableTargets":[]"#), "body={body}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn deregister_scalable_target_rolls_back_cascade_when_persist_fails() {
    let (server, dir) = new_server("deregister");
    let (status, _) = do_request(
        &server,
        "RegisterScalableTarget",
        r#"{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","MinCapacity":1,"MaxCapacity":10}"#,
    );
    assert_eq!(status, 200);
    let (status, _) = do_request(
        &server,
        "PutScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","PolicyType":"TargetTrackingScaling","TargetTrackingScalingPolicyConfiguration":{"TargetValue":70.0}}"#,
    );
    assert_eq!(status, 200);
    let (status, _) = do_request(
        &server,
        "PutScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","Schedule":"at(2030-01-01T00:00:00)"}"#,
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(&server, "DeregisterScalableTarget", TARGET);
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScalableTargets",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ScalableTargets"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "scalable target must survive rollback; body={body}"
    );
    let (_, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ScalingPolicies"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "scaling policy must survive rollback; body={body}"
    );
    let (_, body) = do_request(
        &server,
        "DescribeScheduledActions",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ScheduledActions"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "scheduled action must survive rollback; body={body}"
    );

    // A subsequent successful deregister must still cascade-delete correctly.
    let (status, _) = do_request(&server, "DeregisterScalableTarget", TARGET);
    assert_eq!(status, 200);
    let (_, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert!(body.contains(r#""ScalingPolicies":[]"#), "body={body}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn put_scaling_policy_rolls_back_update_when_persist_fails() {
    let (server, dir) = new_server("policy-update");
    let (_, body) = do_request(
        &server,
        "PutScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","PolicyType":"StepScaling"}"#,
    );
    let original_arn = serde_json::from_str::<serde_json::Value>(&body).unwrap()["PolicyARN"]
        .as_str()
        .unwrap()
        .to_string();

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "PutScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","PolicyType":"TargetTrackingScaling","TargetTrackingScalingPolicyConfiguration":{"TargetValue":70.0}}"#,
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let policies = v["ScalingPolicies"].as_array().unwrap();
    assert_eq!(policies.len(), 1, "body={body}");
    assert_eq!(policies[0]["PolicyType"], "StepScaling");
    assert_eq!(policies[0]["PolicyARN"], original_arn);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn delete_scaling_policy_rolls_back_when_persist_fails() {
    let (server, dir) = new_server("policy-delete");
    let (status, _) = do_request(
        &server,
        "PutScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","PolicyType":"StepScaling"}"#,
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "DeleteScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScalingPolicies",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ScalingPolicies"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "body={body}"
    );

    let (status, _) = do_request(
        &server,
        "DeleteScalingPolicy",
        r#"{"PolicyName":"WriteScaling","ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 200);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn put_scheduled_action_rolls_back_update_when_persist_fails() {
    let (server, dir) = new_server("schedule-update");
    let (status, _) = do_request(
        &server,
        "PutScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","Schedule":"at(2030-01-01T00:00:00)"}"#,
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "PutScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","Schedule":"at(2031-06-01T00:00:00)"}"#,
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScheduledActions",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let actions = v["ScheduledActions"].as_array().unwrap();
    assert_eq!(actions.len(), 1, "body={body}");
    assert_eq!(actions[0]["Schedule"], "at(2030-01-01T00:00:00)");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn delete_scheduled_action_rolls_back_when_persist_fails() {
    let (server, dir) = new_server("schedule-delete");
    let (status, _) = do_request(
        &server,
        "PutScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits","Schedule":"at(2030-01-01T00:00:00)"}"#,
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "DeleteScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "DescribeScheduledActions",
        r#"{"ServiceNamespace":"dynamodb"}"#,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ScheduledActions"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "body={body}"
    );

    let (status, _) = do_request(
        &server,
        "DeleteScheduledAction",
        r#"{"ServiceNamespace":"dynamodb","ScheduledActionName":"ScaleUp","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}"#,
    );
    assert_eq!(status, 200);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tag_resource_rolls_back_when_persist_fails() {
    let (server, dir) = new_server("tag");
    let arn = "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/abc";
    let (status, _) = do_request(
        &server,
        "TagResource",
        &format!(r#"{{"ResourceARN":"{arn}","Tags":{{"team":"core"}}}}"#),
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "TagResource",
        &format!(r#"{{"ResourceARN":"{arn}","Tags":{{"env":"prod"}}}}"#),
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "ListTagsForResource",
        &format!(r#"{{"ResourceARN":"{arn}"}}"#),
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["Tags"],
        serde_json::json!({"team": "core"}),
        "body={body}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tag_resource_rolls_back_new_bucket_when_persist_fails() {
    let (server, dir) = new_server("tag-new");
    let arn = "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/new";
    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "TagResource",
        &format!(r#"{{"ResourceARN":"{arn}","Tags":{{"env":"prod"}}}}"#),
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "ListTagsForResource",
        &format!(r#"{{"ResourceARN":"{arn}"}}"#),
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["Tags"], serde_json::json!({}), "body={body}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn untag_resource_rolls_back_when_persist_fails() {
    let (server, dir) = new_server("untag");
    let arn = "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/abc";
    let (status, _) = do_request(
        &server,
        "TagResource",
        &format!(r#"{{"ResourceARN":"{arn}","Tags":{{"env":"prod","team":"core"}}}}"#),
    );
    assert_eq!(status, 200);

    corrupt_storage(&dir);
    let (status, _) = do_request(
        &server,
        "UntagResource",
        &format!(r#"{{"ResourceARN":"{arn}","TagKeys":["env"]}}"#),
    );
    assert_eq!(status, 500);

    repair_storage(&dir);
    let (_, body) = do_request(
        &server,
        "ListTagsForResource",
        &format!(r#"{{"ResourceARN":"{arn}"}}"#),
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["Tags"],
        serde_json::json!({"env": "prod", "team": "core"}),
        "body={body}"
    );

    let (status, _) = do_request(
        &server,
        "UntagResource",
        &format!(r#"{{"ResourceARN":"{arn}","TagKeys":["env"]}}"#),
    );
    assert_eq!(status, 200);
    let (_, body) = do_request(
        &server,
        "ListTagsForResource",
        &format!(r#"{{"ResourceARN":"{arn}"}}"#),
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["Tags"],
        serde_json::json!({"team": "core"}),
        "body={body}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
