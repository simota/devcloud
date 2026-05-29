//! Parity tests for the JSON-protocol (AWS JSON 1.0) HTTP dispatch. Drives the
//! `dispatch_json` core end-to-end and asserts response shapes captured from the
//! Go server (`writeJSON` / `writeJSONError`).

use devcloud_sqs::{Config, Server};
use serde_json::{json, Value};

fn cfg() -> Config {
    Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        queue_url_host: "127.0.0.1:9324".to_string(),
        ..Default::default()
    }
}

fn target(op: &str) -> String {
    format!("AmazonSQS.{op}")
}

/// Dispatch a JSON op, returning (status, error_type, body).
fn call(s: &mut Server, op: &str, body: Value) -> (u16, Option<String>, Value) {
    let raw = serde_json::to_vec(&body).unwrap();
    let out = s.dispatch_json(&target(op), &raw);
    (out.status, out.error_type, out.body)
}

const URL: &str = "http://127.0.0.1:9324/000000000000/Orders";

#[test]
fn unknown_target_and_operation() {
    let mut s = Server::new(cfg());
    // Missing AmazonSQS. prefix.
    let out = s.dispatch_json("Bogus.Op", b"{}");
    assert_eq!(out.status, 400);
    assert_eq!(out.error_type.as_deref(), Some("InvalidAction"));
    // Unknown operation under the right prefix.
    let (status, et, _) = call(&mut s, "Frobnicate", json!({}));
    assert_eq!(status, 400);
    assert_eq!(et.as_deref(), Some("InvalidAction"));
}

#[test]
fn create_get_list_delete_queue() {
    let mut s = Server::new(cfg());
    let (status, _, body) = call(&mut s, "CreateQueue", json!({"QueueName": "Orders"}));
    assert_eq!(status, 200);
    assert_eq!(body, json!({"QueueUrl": URL}));

    let (_, _, body) = call(&mut s, "GetQueueUrl", json!({"QueueName": "Orders"}));
    assert_eq!(body, json!({"QueueUrl": URL}));

    // GetQueueUrl for a missing queue → QueueDoesNotExist.
    let (status, et, _) = call(&mut s, "GetQueueUrl", json!({"QueueName": "Nope"}));
    assert_eq!(status, 400);
    assert_eq!(et.as_deref(), Some("QueueDoesNotExist"));

    let (_, _, body) = call(&mut s, "ListQueues", json!({}));
    assert_eq!(body, json!({"QueueUrls": [URL]}));

    let (_, _, body) = call(&mut s, "DeleteQueue", json!({"QueueUrl": URL}));
    assert_eq!(body, json!({}));
    // Deleting again → QueueDoesNotExist.
    let (status, et, _) = call(&mut s, "DeleteQueue", json!({"QueueUrl": URL}));
    assert_eq!(status, 400);
    assert_eq!(et.as_deref(), Some("QueueDoesNotExist"));
}

#[test]
fn get_set_attributes_and_tags() {
    let mut s = Server::new(cfg());
    call(&mut s, "CreateQueue", json!({"QueueName": "Orders"}));

    let (_, _, body) = call(
        &mut s,
        "GetQueueAttributes",
        json!({"QueueUrl": URL, "AttributeNames": ["VisibilityTimeout"]}),
    );
    assert_eq!(body, json!({"Attributes": {"VisibilityTimeout": "30"}}));

    let (status, _, body) = call(
        &mut s,
        "SetQueueAttributes",
        json!({"QueueUrl": URL, "Attributes": {"VisibilityTimeout": "45"}}),
    );
    assert_eq!(status, 200);
    assert_eq!(body, json!({}));

    call(
        &mut s,
        "TagQueue",
        json!({"QueueUrl": URL, "Tags": {"env": "prod"}}),
    );
    let (_, _, body) = call(&mut s, "ListQueueTags", json!({"QueueUrl": URL}));
    assert_eq!(body, json!({"Tags": {"env": "prod"}}));
    let (_, _, body) = call(
        &mut s,
        "UntagQueue",
        json!({"QueueUrl": URL, "TagKeys": ["env"]}),
    );
    assert_eq!(body, json!({}));
    let (_, _, body) = call(&mut s, "ListQueueTags", json!({"QueueUrl": URL}));
    assert_eq!(body, json!({"Tags": {}}));
}

#[test]
fn send_receive_delete_roundtrip() {
    let mut s = Server::new(cfg());
    call(&mut s, "CreateQueue", json!({"QueueName": "Orders"}));

    // SendMessage → {MessageId, MD5OfMessageBody} (md5 of "hello" from Go oracle).
    let (status, _, body) = call(
        &mut s,
        "SendMessage",
        json!({"QueueUrl": URL, "MessageBody": "hello"}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["MD5OfMessageBody"], "5d41402abc4b2a76b9719d911017c592");
    assert!(body["MessageId"].as_str().unwrap().starts_with("msg-"));
    // No SequenceNumber for a standard queue, no attribute MD5s.
    assert!(body.get("SequenceNumber").is_none());
    assert!(body.get("MD5OfMessageAttributes").is_none());

    // ReceiveMessage → {Messages: [{MessageId, ReceiptHandle, MD5OfMessageBody,
    // Body, Attributes:{...}}]}.
    let (_, _, body) = call(
        &mut s,
        "ReceiveMessage",
        json!({"QueueUrl": URL, "VisibilityTimeout": 30, "WaitTimeSeconds": 0}),
    );
    let msgs = body["Messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    let m = &msgs[0];
    assert_eq!(m["Body"], "hello");
    assert_eq!(m["MD5OfMessageBody"], "5d41402abc4b2a76b9719d911017c592");
    assert_eq!(m["Attributes"]["ApproximateReceiveCount"], "1");
    let handle = m["ReceiptHandle"].as_str().unwrap().to_string();

    let (status, _, body) = call(
        &mut s,
        "DeleteMessage",
        json!({"QueueUrl": URL, "ReceiptHandle": handle}),
    );
    assert_eq!(status, 200);
    assert_eq!(body, json!({}));

    // Empty queue → {"Messages": []}.
    let (_, _, body) = call(
        &mut s,
        "ReceiveMessage",
        json!({"QueueUrl": URL, "VisibilityTimeout": 0, "WaitTimeSeconds": 0}),
    );
    assert_eq!(body, json!({"Messages": []}));
}

#[test]
fn send_message_to_missing_queue_errors() {
    let mut s = Server::new(cfg());
    let (status, et, body) = call(
        &mut s,
        "SendMessage",
        json!({"QueueUrl": "http://127.0.0.1:9324/000000000000/Nope", "MessageBody": "x"}),
    );
    assert_eq!(status, 400);
    assert_eq!(et.as_deref(), Some("QueueDoesNotExist"));
    // Error body shape matches Go writeJSONError.
    assert_eq!(
        body,
        json!({"__type": "QueueDoesNotExist", "message": "queue does not exist"})
    );
}

#[test]
fn send_message_batch_shape() {
    let mut s = Server::new(cfg());
    call(&mut s, "CreateQueue", json!({"QueueName": "Orders"}));
    let (status, _, body) = call(
        &mut s,
        "SendMessageBatch",
        json!({
            "QueueUrl": URL,
            "Entries": [
                {"Id": "a", "MessageBody": "m1"},
                {"Id": "b", "MessageBody": ""}
            ]
        }),
    );
    assert_eq!(status, 200);
    // One success, one failure (empty body) — Successful/Failed always present.
    let successful = body["Successful"].as_array().unwrap();
    let failed = body["Failed"].as_array().unwrap();
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "a");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "b");
    assert_eq!(failed[0]["SenderFault"], true);
}

#[test]
fn fifo_send_returns_sequence_number() {
    let mut s = Server::new(cfg());
    call(
        &mut s,
        "CreateQueue",
        json!({"QueueName": "Orders.fifo", "Attributes": {"FifoQueue": "true"}}),
    );
    let furl = "http://127.0.0.1:9324/000000000000/Orders.fifo";
    let (status, _, body) = call(
        &mut s,
        "SendMessage",
        json!({
            "QueueUrl": furl,
            "MessageBody": "x",
            "MessageGroupId": "g1",
            "MessageDeduplicationId": "d1"
        }),
    );
    assert_eq!(status, 200);
    assert_eq!(body["SequenceNumber"], "1");
}

#[test]
fn lowercase_tags_field_accepted_on_create() {
    // Go accepts both `Tags` and lowercase `tags` on CreateQueue.
    let mut s = Server::new(cfg());
    call(
        &mut s,
        "CreateQueue",
        json!({"QueueName": "Orders", "tags": {"team": "core"}}),
    );
    let (_, _, body) = call(&mut s, "ListQueueTags", json!({"QueueUrl": URL}));
    assert_eq!(body, json!({"Tags": {"team": "core"}}));
}

#[test]
fn move_task_operations() {
    let mut s = Server::new(cfg());
    call(&mut s, "CreateQueue", json!({"QueueName": "A"}));
    call(&mut s, "CreateQueue", json!({"QueueName": "B"}));
    let src = "arn:aws:sqs:us-east-1:000000000000:A";
    let dst = "arn:aws:sqs:us-east-1:000000000000:B";

    let (_, _, body) = call(
        &mut s,
        "StartMessageMoveTask",
        json!({"SourceArn": src, "DestinationArn": dst}),
    );
    let handle = body["TaskHandle"].as_str().unwrap().to_string();
    assert!(handle.starts_with("mvt-"));

    let (_, _, body) = call(&mut s, "ListMessageMoveTasks", json!({"SourceArn": src}));
    let results = body["Results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["Status"], "COMPLETED");
    assert_eq!(results[0]["SourceArn"], src);
    // DestinationArn present (non-empty); moved count 0 (no messages).
    assert_eq!(results[0]["DestinationArn"], dst);
    assert_eq!(results[0]["ApproximateNumberOfMessagesMoved"], 0);

    let (_, _, body) = call(
        &mut s,
        "CancelMessageMoveTask",
        json!({"TaskHandle": handle}),
    );
    assert_eq!(body, json!({"ApproximateNumberOfMessagesMoved": 0}));
}
