//! Parity tests for the SQS queue-management core (queue lifecycle, attributes,
//! tags, policy), with values captured from the legacy server.

use std::collections::BTreeMap;

use devcloud_sqs::{
    normalized_permission_actions, parse_redrive_allow_policy, parse_redrive_policy,
    queue_name_from_url, Config, Server,
};

fn cfg() -> Config {
    Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        queue_url_host: "127.0.0.1:9324".to_string(),
        ..Default::default()
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn url_arn_and_defaults_match_legacy() {
    let s = Server::new(cfg());
    assert_eq!(
        s.queue_url("Orders"),
        "http://127.0.0.1:9324/000000000000/Orders"
    );
    assert_eq!(
        s.queue_arn("Orders"),
        "arn:aws:sqs:us-east-1:000000000000:Orders"
    );
    let def = s.default_queue_attributes();
    assert_eq!(def["DelaySeconds"], "0");
    assert_eq!(def["MaximumMessageSize"], "1048576");
    assert_eq!(def["MessageRetentionPeriod"], "345600");
    assert_eq!(def["ReceiveMessageWaitTimeSeconds"], "0");
    assert_eq!(def["VisibilityTimeout"], "30");
}

#[test]
fn queue_name_from_url_matches_legacy() {
    assert_eq!(
        queue_name_from_url("http://127.0.0.1:9324/000000000000/Orders"),
        "Orders"
    );
    assert_eq!(queue_name_from_url("http://127.0.0.1:9324/"), "");
    assert_eq!(
        queue_name_from_url("https://sqs.us-east-1.amazonaws.com/123/MyQ.fifo"),
        "MyQ.fifo"
    );
}

#[test]
fn create_list_get_delete_lifecycle() {
    let mut s = Server::new(cfg());
    let q = s
        .create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .expect("create");
    assert_eq!(q.url, "http://127.0.0.1:9324/000000000000/Orders");

    // Idempotent create with identical attributes returns the existing queue.
    assert!(s
        .create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .is_ok());

    assert_eq!(
        s.list_queue_urls(""),
        vec!["http://127.0.0.1:9324/000000000000/Orders".to_string()]
    );
    assert!(s.queue_by_name("Orders").is_some());
    assert!(s.delete_queue("http://127.0.0.1:9324/000000000000/Orders"));
    assert!(s.queue_by_name("Orders").is_none());
    assert!(!s.delete_queue("http://127.0.0.1:9324/000000000000/Orders"));
}

#[test]
fn fifo_suffix_rules() {
    let mut s = Server::new(cfg());
    // .fifo name without FifoQueue=true is rejected.
    assert!(s
        .create_queue("Orders.fifo", &BTreeMap::new(), &BTreeMap::new())
        .unwrap_err()
        .contains("must set FifoQueue to true"));
    // FifoQueue=true without .fifo suffix is rejected.
    assert!(s
        .create_queue("Orders", &map(&[("FifoQueue", "true")]), &BTreeMap::new())
        .unwrap_err()
        .contains("FIFO queues must use a .fifo suffix"));
    // Correct FIFO queue is accepted.
    assert!(s
        .create_queue(
            "Orders.fifo",
            &map(&[("FifoQueue", "true")]),
            &BTreeMap::new()
        )
        .is_ok());
}

#[test]
fn create_queue_validation() {
    let mut s = Server::new(cfg());
    assert!(s
        .create_queue("", &BTreeMap::new(), &BTreeMap::new())
        .unwrap_err()
        .contains("QueueName is required"));
    assert!(s
        .create_queue("bad name", &BTreeMap::new(), &BTreeMap::new())
        .unwrap_err()
        .contains("alphanumeric"));
    // Out-of-range attribute value.
    assert!(s
        .create_queue(
            "Q",
            &map(&[("VisibilityTimeout", "99999")]),
            &BTreeMap::new()
        )
        .unwrap_err()
        .contains("invalid attribute value for VisibilityTimeout"));
    // Non-settable attribute.
    assert!(s
        .create_queue("Q", &map(&[("QueueArn", "x")]), &BTreeMap::new())
        .unwrap_err()
        .contains("unknown queue attribute name"));
}

#[test]
fn computed_attributes_present() {
    let mut s = Server::new(cfg());
    s.create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    let attrs = s
        .get_queue_attributes("http://127.0.0.1:9324/000000000000/Orders", &[])
        .unwrap();
    assert_eq!(
        attrs["QueueArn"],
        "arn:aws:sqs:us-east-1:000000000000:Orders"
    );
    assert_eq!(attrs["ApproximateNumberOfMessages"], "0");
    assert!(attrs.contains_key("CreatedTimestamp"));
    assert!(attrs.contains_key("LastModifiedTimestamp"));
    // CreatedTimestamp is a positive UNIX time (now), not the legacy zero time.
    assert!(attrs["CreatedTimestamp"].parse::<i64>().unwrap() > 0);

    // Filtered read returns only requested + rejects unknown.
    let filtered = s
        .get_queue_attributes(
            "http://127.0.0.1:9324/000000000000/Orders",
            &["VisibilityTimeout".to_string()],
        )
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered["VisibilityTimeout"], "30");
    assert!(s
        .get_queue_attributes(
            "http://127.0.0.1:9324/000000000000/Orders",
            &["Bogus".to_string()]
        )
        .unwrap_err()
        .contains("unknown queue attribute name"));
    assert!(s
        .get_queue_attributes("http://127.0.0.1:9324/000000000000/Missing", &[])
        .unwrap_err()
        .contains("queue does not exist"));
}

#[test]
fn update_queue_attributes_rolls_back_memory_when_persist_fails() {
    let storage_path = std::env::temp_dir().join(format!(
        "devcloud-sqs-attrs-rollback-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&storage_path);
    let _ = std::fs::remove_dir_all(&storage_path);

    let mut c = cfg();
    c.storage_path = storage_path.to_string_lossy().into_owned();
    let mut s = Server::new(c);
    let url = "http://127.0.0.1:9324/000000000000/Orders";
    s.create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();

    std::fs::remove_dir_all(&storage_path).unwrap();
    std::fs::write(&storage_path, b"not a directory").unwrap();

    assert!(s
        .update_queue_attributes(url, &map(&[("VisibilityTimeout", "7")]))
        .is_err());
    assert_eq!(
        s.get_queue_attributes(url, &["VisibilityTimeout".to_string()])
            .unwrap()["VisibilityTimeout"],
        "30"
    );

    std::fs::remove_file(&storage_path).unwrap();
    std::fs::create_dir_all(&storage_path).unwrap();

    let queue = s
        .update_queue_attributes(url, &map(&[("VisibilityTimeout", "7")]))
        .unwrap();
    assert_eq!(queue.attributes["VisibilityTimeout"], "7");

    std::fs::remove_dir_all(&storage_path).unwrap();
}

#[test]
fn tags_lifecycle() {
    let mut s = Server::new(cfg());
    let url = "http://127.0.0.1:9324/000000000000/Orders";
    s.create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.tag_queue(url, &map(&[("env", "prod"), ("team", "core")]))
        .unwrap();
    assert_eq!(
        s.list_queue_tags(url).unwrap(),
        map(&[("env", "prod"), ("team", "core")])
    );
    s.untag_queue(url, &["env".to_string()]).unwrap();
    assert_eq!(s.list_queue_tags(url).unwrap(), map(&[("team", "core")]));
    assert!(s
        .tag_queue(url, &BTreeMap::new())
        .unwrap_err()
        .contains("Tags is required"));
}

#[test]
fn add_permission_policy_json_matches_legacy() {
    let mut s = Server::new(cfg());
    let url = "http://127.0.0.1:9324/000000000000/Orders";
    s.create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.add_permission(
        url,
        "L1",
        &["111".to_string(), "222".to_string()],
        &[
            "SendMessage".to_string(),
            "sqs:ReceiveMessage".to_string(),
            "*".to_string(),
        ],
    )
    .unwrap();
    let attrs = s
        .get_queue_attributes(url, &["Policy".to_string()])
        .unwrap();
    // Byte-exact match to the legacy json.Marshal(queuePolicy) output.
    let want = r#"{"Version":"2012-10-17","Id":"arn:aws:sqs:us-east-1:000000000000:Orders/SQSDefaultPolicy","Statement":[{"Sid":"L1","Effect":"Allow","Principal":{"AWS":["111","222"]},"Action":["SQS:SendMessage","sqs:ReceiveMessage","*"],"Resource":"arn:aws:sqs:us-east-1:000000000000:Orders"}]}"#;
    assert_eq!(attrs["Policy"], want);

    // RemovePermission with the only label drops the Policy attribute entirely.
    s.remove_permission(url, "L1").unwrap();
    let attrs = s.get_queue_attributes(url, &[]).unwrap();
    assert!(!attrs.contains_key("Policy"));
}

#[test]
fn normalized_permission_actions_matches_legacy() {
    let actions = vec![
        "SendMessage".to_string(),
        "sqs:Recv".to_string(),
        "*".to_string(),
        "".to_string(),
    ];
    assert_eq!(
        normalized_permission_actions(&actions),
        vec!["SQS:SendMessage", "sqs:Recv", "*"]
    );
}

#[test]
fn redrive_policy_parsing() {
    // number maxReceiveCount
    let p = parse_redrive_policy(r#"{"deadLetterTargetArn":"arn:x","maxReceiveCount":5}"#).unwrap();
    assert_eq!(p.dead_letter_target_arn, "arn:x");
    assert_eq!(p.max_receive_count, 5);
    // string maxReceiveCount
    let p =
        parse_redrive_policy(r#"{"deadLetterTargetArn":"arn:x","maxReceiveCount":"3"}"#).unwrap();
    assert_eq!(p.max_receive_count, 3);
    // missing target
    assert!(parse_redrive_policy(r#"{"maxReceiveCount":5}"#)
        .unwrap_err()
        .contains("deadLetterTargetArn is required"));
    // non-integer
    assert!(
        parse_redrive_policy(r#"{"deadLetterTargetArn":"arn:x","maxReceiveCount":1.5}"#)
            .unwrap_err()
            .contains("must be an integer")
    );
    // invalid json
    assert!(parse_redrive_policy("not json")
        .unwrap_err()
        .contains("valid JSON"));
}

#[test]
fn redrive_allow_policy_parsing() {
    let p = parse_redrive_allow_policy(r#"{"redrivePermission":"allowAll"}"#).unwrap();
    assert_eq!(p.permission, "allowAll");
    // default permission when absent
    let p = parse_redrive_allow_policy("{}").unwrap();
    assert_eq!(p.permission, "allowAll");
    // byQueue requires sourceQueueArns
    assert!(
        parse_redrive_allow_policy(r#"{"redrivePermission":"byQueue"}"#)
            .unwrap_err()
            .contains("sourceQueueArns is required")
    );
    let p = parse_redrive_allow_policy(
        r#"{"redrivePermission":"byQueue","sourceQueueArns":["arn:a","arn:b"]}"#,
    )
    .unwrap();
    assert_eq!(p.source_queue_arns, vec!["arn:a", "arn:b"]);
    // bad permission
    assert!(
        parse_redrive_allow_policy(r#"{"redrivePermission":"bogus"}"#)
            .unwrap_err()
            .contains("must be allowAll, denyAll, or byQueue")
    );
}

#[test]
fn redrive_policy_dlq_validation_via_set_attributes() {
    let mut s = Server::new(cfg());
    let src = "http://127.0.0.1:9324/000000000000/Source";
    s.create_queue("Source", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.create_queue("DLQ", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    // DLQ does not exist by that ARN.
    let bad = map(&[(
        "RedrivePolicy",
        r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:Nope","maxReceiveCount":3}"#,
    )]);
    assert!(s
        .update_queue_attributes(src, &bad)
        .unwrap_err()
        .contains("deadLetterTargetArn queue does not exist"));
    // Valid DLQ wiring succeeds.
    let good = map(&[(
        "RedrivePolicy",
        r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:DLQ","maxReceiveCount":3}"#,
    )]);
    assert!(s.update_queue_attributes(src, &good).is_ok());
}

#[test]
fn persisted_state_survives_reload() {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-sqs-srv-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut c = cfg();
    c.storage_path = dir.to_string_lossy().into_owned();
    {
        let mut s = Server::new(c.clone());
        s.create_queue(
            "Orders",
            &map(&[("VisibilityTimeout", "45")]),
            &map(&[("env", "prod")]),
        )
        .unwrap();
    }
    // Reload from disk.
    let s2 = Server::new(c);
    let q = s2.queue_by_name("Orders").expect("reloaded");
    assert_eq!(q.attributes["VisibilityTimeout"], "45");
    assert_eq!(q.tags["env"], "prod");
}
