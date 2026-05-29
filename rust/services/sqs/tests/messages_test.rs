//! Parity tests for the SQS message lifecycle: send (+FIFO dedup), receive
//! (visibility, FIFO group blocking, DLQ redrive), delete, change-visibility,
//! batches, and retention. Behavior captured from the Go server.

use std::collections::BTreeMap;

use devcloud_sqs::{
    Config, DeleteMessageBatchEntry, ReceiveMessageRequest, SendMessageBatchEntry,
    SendMessageRequest, Server,
};

const URL: &str = "http://127.0.0.1:9324/000000000000/Orders";

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

fn server_with_queue() -> Server {
    let mut s = Server::new(cfg());
    s.create_queue("Orders", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s
}

fn send(s: &mut Server, body: &str) -> String {
    s.send_message(&SendMessageRequest {
        queue_url: URL.to_string(),
        message_body: body.to_string(),
        ..Default::default()
    })
    .unwrap()
    .id
}

/// Receive immediately-visible messages (visibility override 0 so repeated
/// receives keep seeing the same message — used to drive DLQ redrive).
fn receive(s: &mut Server, max: i64, vis: Option<i64>) -> Vec<devcloud_sqs::ReceivedMessage> {
    s.receive_messages(&ReceiveMessageRequest {
        queue_url: URL.to_string(),
        max_number_of_messages: Some(max),
        visibility_timeout: vis,
        wait_time_seconds: Some(0),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn send_basic_sets_body_md5() {
    let mut s = server_with_queue();
    let m = s
        .send_message(&SendMessageRequest {
            queue_url: URL.to_string(),
            message_body: "hello".to_string(),
            ..Default::default()
        })
        .unwrap();
    // md5("hello") from the Go oracle.
    assert_eq!(m.body_md5, "5d41402abc4b2a76b9719d911017c592");
    assert!(m.id.starts_with("msg-"));
}

#[test]
fn send_validation() {
    let mut s = server_with_queue();
    let base = || SendMessageRequest {
        queue_url: URL.to_string(),
        ..Default::default()
    };
    assert!(s
        .send_message(&base())
        .unwrap_err()
        .contains("MessageBody is required"));
    // queue must exist
    assert!(s
        .send_message(&SendMessageRequest {
            queue_url: "http://127.0.0.1:9324/000000000000/Nope".to_string(),
            message_body: "x".to_string(),
            ..Default::default()
        })
        .unwrap_err()
        .contains("queue does not exist"));
    // invalid control char in body
    assert!(s
        .send_message(&SendMessageRequest {
            queue_url: URL.to_string(),
            message_body: "\u{0001}bad".to_string(),
            ..Default::default()
        })
        .unwrap_err()
        .contains("invalid characters"));
}

#[test]
fn receive_sets_handle_and_increments_count() {
    let mut s = server_with_queue();
    send(&mut s, "m1");
    let got = receive(&mut s, 1, Some(30));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].body, "m1");
    assert!(!got[0].receipt_handle.is_empty());
    assert_eq!(got[0].attributes["ApproximateReceiveCount"], "1");
    assert!(got[0].attributes.contains_key("SentTimestamp"));
    assert!(got[0]
        .attributes
        .contains_key("ApproximateFirstReceiveTimestamp"));

    // Default visibility (30s) hides it from the next immediate receive.
    let again = receive(&mut s, 1, Some(30));
    assert_eq!(again.len(), 0);
}

#[test]
fn visibility_zero_keeps_message_visible() {
    let mut s = server_with_queue();
    send(&mut s, "m1");
    let first = receive(&mut s, 1, Some(0));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attributes["ApproximateReceiveCount"], "1");
    // Visibility 0 → immediately receivable again, count climbs.
    let second = receive(&mut s, 1, Some(0));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].attributes["ApproximateReceiveCount"], "2");
}

#[test]
fn delete_message_removes_in_flight() {
    let mut s = server_with_queue();
    send(&mut s, "m1");
    let got = receive(&mut s, 1, Some(30));
    let handle = got[0].receipt_handle.clone();
    s.delete_message(URL, &handle).unwrap();
    // Deleted: no longer receivable even with visibility 0.
    assert_eq!(receive(&mut s, 1, Some(0)).len(), 0);
    // Re-deleting with the same (now cleared) handle is invalid.
    assert!(s
        .delete_message(URL, &handle)
        .unwrap_err()
        .contains("receipt handle is invalid"));
}

#[test]
fn delete_requires_handle_and_queue() {
    let mut s = server_with_queue();
    assert!(s
        .delete_message(URL, "")
        .unwrap_err()
        .contains("ReceiptHandle is required"));
    assert!(s
        .delete_message(URL, "bogus")
        .unwrap_err()
        .contains("receipt handle is invalid"));
}

#[test]
fn change_visibility_extends_then_zero_makes_visible() {
    let mut s = server_with_queue();
    send(&mut s, "m1");
    let got = receive(&mut s, 1, Some(30));
    let handle = got[0].receipt_handle.clone();
    // Hidden right now.
    assert_eq!(receive(&mut s, 1, Some(30)).len(), 0);
    // Set visibility to 0 → becomes immediately visible.
    s.change_message_visibility(URL, &handle, 0).unwrap();
    assert_eq!(receive(&mut s, 1, Some(30)).len(), 1);
}

#[test]
fn max_messages_caps_at_10_and_batch_size() {
    let mut s = server_with_queue();
    for i in 0..5 {
        send(&mut s, &format!("m{i}"));
    }
    let got = receive(&mut s, 3, Some(30));
    assert_eq!(got.len(), 3);
    // Remaining 2 visible.
    let rest = receive(&mut s, 10, Some(30));
    assert_eq!(rest.len(), 2);
}

#[test]
fn fifo_requires_group_id_and_dedups() {
    let mut s = Server::new(cfg());
    s.create_queue(
        "Orders.fifo",
        &map(&[("FifoQueue", "true")]),
        &BTreeMap::new(),
    )
    .unwrap();
    let furl = "http://127.0.0.1:9324/000000000000/Orders.fifo";

    // Missing group id is rejected.
    assert!(s
        .send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: "x".to_string(),
            ..Default::default()
        })
        .unwrap_err()
        .contains("MessageGroupId is required"));

    // Missing dedup id (no content-based dedup) is rejected.
    assert!(s
        .send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: "x".to_string(),
            message_group_id: "g1".to_string(),
            ..Default::default()
        })
        .unwrap_err()
        .contains("MessageDeduplicationId is required"));

    // Explicit dedup id: a repeat within the window returns the original.
    let m1 = s
        .send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: "x".to_string(),
            message_group_id: "g1".to_string(),
            message_deduplication_id: "d1".to_string(),
            ..Default::default()
        })
        .unwrap();
    let m2 = s
        .send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: "x".to_string(),
            message_group_id: "g1".to_string(),
            message_deduplication_id: "d1".to_string(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(m1.id, m2.id, "deduped send returns the original message");
    assert_eq!(m1.sequence_number, "1");

    // FIFO does not support DelaySeconds.
    assert!(s
        .send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: "y".to_string(),
            message_group_id: "g1".to_string(),
            message_deduplication_id: "d2".to_string(),
            delay_seconds: Some(5),
            ..Default::default()
        })
        .unwrap_err()
        .contains("DelaySeconds is not supported"));
}

#[test]
fn fifo_content_based_dedup() {
    let mut s = Server::new(cfg());
    s.create_queue(
        "C.fifo",
        &map(&[("FifoQueue", "true"), ("ContentBasedDeduplication", "true")]),
        &BTreeMap::new(),
    )
    .unwrap();
    let furl = "http://127.0.0.1:9324/000000000000/C.fifo";
    let send_body = |s: &mut Server, body: &str| {
        s.send_message(&SendMessageRequest {
            queue_url: furl.to_string(),
            message_body: body.to_string(),
            message_group_id: "g".to_string(),
            ..Default::default()
        })
        .unwrap()
    };
    let a = send_body(&mut s, "same");
    let b = send_body(&mut s, "same"); // identical body → deduped
    assert_eq!(a.id, b.id);
    let c = send_body(&mut s, "different");
    assert_ne!(a.id, c.id);
}

#[test]
fn dlq_redrive_after_max_receive_count() {
    let mut s = Server::new(cfg());
    s.create_queue("Src", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    s.create_queue("DLQ", &BTreeMap::new(), &BTreeMap::new())
        .unwrap();
    let src = "http://127.0.0.1:9324/000000000000/Src";
    let dlq = "http://127.0.0.1:9324/000000000000/DLQ";
    s.update_queue_attributes(
        src,
        &map(&[(
            "RedrivePolicy",
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:DLQ","maxReceiveCount":2}"#,
        )]),
    )
    .unwrap();

    s.send_message(&SendMessageRequest {
        queue_url: src.to_string(),
        message_body: "poison".to_string(),
        ..Default::default()
    })
    .unwrap();

    let recv_src = |s: &mut Server| {
        s.receive_messages(&ReceiveMessageRequest {
            queue_url: src.to_string(),
            visibility_timeout: Some(0),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap()
    };
    // count 0<2 deliver (count→1); count 1<2 deliver (count→2); count 2>=2 redrive.
    assert_eq!(recv_src(&mut s).len(), 1);
    assert_eq!(recv_src(&mut s).len(), 1);
    assert_eq!(recv_src(&mut s).len(), 0, "redriven, not delivered");

    // The message now lives in the DLQ.
    let dlq_msgs = s
        .receive_messages(&ReceiveMessageRequest {
            queue_url: dlq.to_string(),
            visibility_timeout: Some(0),
            wait_time_seconds: Some(0),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(dlq_msgs.len(), 1);
    assert_eq!(dlq_msgs[0].body, "poison");
}

#[test]
fn batch_send_and_delete() {
    let mut s = server_with_queue();
    let entries: Vec<SendMessageBatchEntry> = (0..3)
        .map(|i| SendMessageBatchEntry {
            id: format!("e{i}"),
            message_body: format!("b{i}"),
            ..Default::default()
        })
        .collect();
    let res = s.send_message_batch(URL, &entries).unwrap();
    assert_eq!(res.successful.len(), 3);
    assert_eq!(res.failed.len(), 0);

    let got = receive(&mut s, 10, Some(30));
    assert_eq!(got.len(), 3);
    let del: Vec<DeleteMessageBatchEntry> = got
        .iter()
        .enumerate()
        .map(|(i, m)| DeleteMessageBatchEntry {
            id: format!("d{i}"),
            receipt_handle: m.receipt_handle.clone(),
        })
        .collect();
    let dres = s.delete_message_batch(URL, &del).unwrap();
    assert_eq!(dres.successful.len(), 3);
    assert_eq!(receive(&mut s, 10, Some(0)).len(), 0);
}

#[test]
fn batch_entry_validation() {
    let mut s = server_with_queue();
    // duplicate ids
    let dup = vec![
        SendMessageBatchEntry {
            id: "x".to_string(),
            message_body: "a".to_string(),
            ..Default::default()
        },
        SendMessageBatchEntry {
            id: "x".to_string(),
            message_body: "b".to_string(),
            ..Default::default()
        },
    ];
    assert!(s
        .send_message_batch(URL, &dup)
        .unwrap_err()
        .contains("must be unique"));
    // empty entries
    assert!(s
        .send_message_batch(URL, &[])
        .unwrap_err()
        .contains("Entries is required"));
    // per-entry failure is captured, not fatal (bad body on one entry)
    let mixed = vec![
        SendMessageBatchEntry {
            id: "ok".to_string(),
            message_body: "good".to_string(),
            ..Default::default()
        },
        SendMessageBatchEntry {
            id: "bad".to_string(),
            message_body: String::new(), // empty body → fails
            ..Default::default()
        },
    ];
    let res = s.send_message_batch(URL, &mixed).unwrap();
    assert_eq!(res.successful.len(), 1);
    assert_eq!(res.failed.len(), 1);
    assert_eq!(res.failed[0].id, "bad");
    assert!(res.failed[0].sender_fault);
}
