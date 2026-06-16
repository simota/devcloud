//! Parity tests for the SQS dead-letter source listing + message-move-task
//! lifecycle (start / list / cancel), mirroring deadletter_move_tasks.rs.

use std::collections::BTreeMap;

use devcloud_sqs::{Config, ReceiveMessageRequest, SendMessageRequest, Server};

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

fn url(name: &str) -> String {
    format!("http://127.0.0.1:9324/000000000000/{name}")
}

fn arn(name: &str) -> String {
    format!("arn:aws:sqs:us-east-1:000000000000:{name}")
}

/// Builds a Source→DLQ wiring with one message redriven into the DLQ
/// (DeadLetterSourceARN recorded), so move-back can be exercised.
fn server_with_redriven_message() -> Server {
    let mut s = Server::new(cfg());
    s.create_queue("Src", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.create_queue("DLQ", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.update_queue_attributes(
        &url("Src"),
        &map(&[(
            "RedrivePolicy",
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:DLQ","maxReceiveCount":1}"#,
        )]),
    )
    .unwrap();
    s.send_message(&SendMessageRequest {
        queue_url: url("Src"),
        message_body: "poison".to_string(),
        ..Default::default()
    })
    .unwrap();
    // Receive once (count→1) then again triggers redrive (count 1 >= 1).
    let recv = |s: &mut Server| {
        s.receive_messages(&ReceiveMessageRequest {
            queue_url: url("Src"),
            visibility_timeout: Some(0),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap()
    };
    assert_eq!(recv(&mut s).len(), 1);
    assert_eq!(recv(&mut s).len(), 0, "redriven to DLQ");
    s
}

#[test]
fn list_dead_letter_source_queues() {
    let s = server_with_redriven_message();
    // DLQ lists Src as a source.
    assert_eq!(
        s.list_dead_letter_source_queue_urls(&url("DLQ")).unwrap(),
        vec![url("Src")]
    );
    // A queue with no sources returns empty.
    assert_eq!(
        s.list_dead_letter_source_queue_urls(&url("Src")).unwrap(),
        Vec::<String>::new()
    );
    // Unknown queue errors.
    assert!(s
        .list_dead_letter_source_queue_urls(&url("Nope"))
        .unwrap_err()
        .contains("queue does not exist"));
}

#[test]
fn start_move_task_with_explicit_destination() {
    let mut s = Server::new(cfg());
    s.create_queue("A", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.create_queue("B", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    for i in 0..3 {
        s.send_message(&SendMessageRequest {
            queue_url: url("A"),
            message_body: format!("m{i}"),
            ..Default::default()
        })
        .unwrap();
    }
    let task = s.start_message_move_task(&arn("A"), &arn("B")).unwrap();
    assert_eq!(task.status, "COMPLETED");
    assert_eq!(task.approximate_number_of_messages_moved, 3);
    assert!(task.task_handle.starts_with("mvt-"));

    // All three are now receivable from B; A is empty.
    let from_b = s
        .receive_messages(&ReceiveMessageRequest {
            queue_url: url("B"),
            max_number_of_messages: Some(10),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(from_b.len(), 3);
    let from_a = s
        .receive_messages(&ReceiveMessageRequest {
            queue_url: url("A"),
            max_number_of_messages: Some(10),
            visibility_timeout: Some(0),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(from_a.len(), 0);
}

#[test]
fn start_move_task_back_to_original_source() {
    let mut s = server_with_redriven_message();
    // No destination → each message returns to its recorded source (Src).
    let task = s.start_message_move_task(&arn("DLQ"), "").unwrap();
    assert_eq!(task.approximate_number_of_messages_moved, 1);

    let back = s
        .receive_messages(&ReceiveMessageRequest {
            queue_url: url("Src"),
            visibility_timeout: Some(30),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].body, "poison");
    // The dead-letter source ARN was cleared on move-back.
    assert_eq!(back[0].attributes["ApproximateReceiveCount"], "1");
}

#[test]
fn start_move_task_validation() {
    let mut s = Server::new(cfg());
    s.create_queue("A", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    assert!(s
        .start_message_move_task("", "")
        .unwrap_err()
        .contains("SourceArn is required"));
    assert!(s
        .start_message_move_task(&arn("Missing"), "")
        .unwrap_err()
        .contains("queue does not exist"));
    assert!(s
        .start_message_move_task(&arn("A"), &arn("MissingDest"))
        .unwrap_err()
        .contains("destination queue does not exist"));
}

#[test]
fn list_and_cancel_move_tasks() {
    let mut s = Server::new(cfg());
    s.create_queue("A", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.create_queue("B", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    let t1 = s.start_message_move_task(&arn("A"), &arn("B")).unwrap();

    let tasks = s.list_message_move_tasks(&arn("A"), 0).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_handle, t1.task_handle);
    assert_eq!(tasks[0].status, "COMPLETED");

    // Different source has no tasks.
    assert_eq!(s.list_message_move_tasks(&arn("B"), 0).unwrap().len(), 0);

    // Cancel returns the moved count and does not error.
    assert_eq!(s.cancel_message_move_task(&t1.task_handle).unwrap(), 0);

    // Validation.
    assert!(s
        .list_message_move_tasks("", 0)
        .unwrap_err()
        .contains("SourceArn is required"));
    assert!(s
        .list_message_move_tasks(&arn("Missing"), 0)
        .unwrap_err()
        .contains("queue does not exist"));
    assert!(s
        .cancel_message_move_task("")
        .unwrap_err()
        .contains("TaskHandle is required"));
    assert!(s
        .cancel_message_move_task("mvt-bogus")
        .unwrap_err()
        .contains("message move task does not exist"));
}

#[test]
fn move_tasks_survive_reload() {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-sqs-mvt-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut c = cfg();
    c.storage_path = dir.to_string_lossy().into_owned();
    let handle = {
        let mut s = Server::new(c.clone());
        s.create_queue("A", &BTreeMap::new(), &BTreeMap::new())
            .unwrap();
        s.create_queue("B", &BTreeMap::new(), &BTreeMap::new())
            .unwrap();
        s.start_message_move_task(&arn("A"), &arn("B"))
            .unwrap()
            .task_handle
    };
    // Reload: the move task is restored from state.json.
    let s2 = Server::new(c);
    let tasks = s2.list_message_move_tasks(&arn("A"), 0).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_handle, handle);
}
