//! Rust-native gRPC **conformance** test for the Pub/Sub gRPC wire surface.
//!
//! This is the Phase-3 (legacy-removal) successor to the legacy-hosted differential
//! harness `internal/services/pubsub/grpc_parity_test.rs`. That harness ran each
//! engine-agnostic RPC sequence against BOTH the in-process legacy server (bufconn,
//! "source of truth") AND the Rust `devcloud-pubsub` subprocess, applying the
//! SAME hardcoded behavioral assertions to each. When legacy is deleted that oracle
//! is gone — so the assertions are lifted here verbatim and run against the Rust
//! gRPC server only. The assertions ARE the spec: this is behavioral
//! conformance, not byte-golden comparison, and no legacy / subprocess / golden file
//! is involved.
//!
//! How the server boots in-process (no subprocess): each test constructs a
//! `Server` over a fresh temp storage dir, wraps it in `Arc<Mutex<…>>` (the same
//! shared-state shape `main.rs` uses), and serves the three tonic services on a
//! `127.0.0.1:0`-bound loopback port from a background tokio task. tonic clients
//! connect over a `Channel` to that addr and drive the RPC sequences.
//!
//! NOTE: actually RUNNING these tests binds a loopback TCP socket, which the
//! build sandbox blocks — `cargo test` will fail to RUN in-sandbox with a bind
//! error. Build-green (`cargo build --tests`) is the in-sandbox gate; the run is
//! performed sandbox-disabled by the orchestrator.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use devcloud_pubsub::grpc::PubSubGrpc;
use devcloud_pubsub::proto::pubsub::{
    publisher_client::PublisherClient, publisher_server::PublisherServer,
    schema_service_client::SchemaServiceClient, schema_service_server::SchemaServiceServer,
    subscriber_client::SubscriberClient, subscriber_server::SubscriberServer,
};
use devcloud_pubsub::proto::pubsub::{
    schema, AcknowledgeRequest, CreateSchemaRequest, CreateSnapshotRequest, DeleteSchemaRequest,
    DeleteSnapshotRequest, DeleteSubscriptionRequest, DeleteTopicRequest, GetSchemaRequest,
    GetSnapshotRequest, GetSubscriptionRequest, GetTopicRequest, PublishRequest, PubsubMessage,
    PullRequest, Schema, SeekRequest, StreamingPullRequest, StreamingPullResponse, Subscription,
    Topic,
};
use devcloud_pubsub::server::{Config, Server};

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::Code;

const PROJECT: &str = "devcloud";

/// Engine-agnostic config for a conformance scenario — the Rust counterpart of
/// the legacy harness's `parityConfig` (only the knobs the scenarios share).
#[derive(Clone, Default)]
struct EngineConfig {
    default_ack_deadline: i64,
    message_retention_seconds: i64,
    streaming_pull_disabled: bool,
}

/// A connected set of tonic clients plus the project the engine serves —
/// the Rust counterpart of the legacy harness's `engineConn`.
struct Engine {
    publisher: PublisherClient<Channel>,
    subscriber: SubscriberClient<Channel>,
    schemas: SchemaServiceClient<Channel>,
    project: String,
}

/// Boot the Rust gRPC server in-process on a loopback port and return connected
/// clients. The server task runs in the background and shuts down when the test
/// process exits (the tonic clients keep the channel alive for the test).
async fn dial(cfg: EngineConfig) -> Engine {
    let server = Server::new(Config {
        project: PROJECT.to_string(),
        storage_path: temp_dir(),
        message_storage_path: temp_dir(),
        default_ack_deadline_seconds: cfg.default_ack_deadline,
        message_retention_seconds: cfg.message_retention_seconds,
        streaming_pull_disabled: cfg.streaming_pull_disabled,
        ..Default::default()
    });

    // Bind an ephemeral loopback port; hand the listener to tonic so the bound
    // addr is known before any client connects (no readiness race).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr: SocketAddr = listener.local_addr().expect("local addr");

    let adapter = PubSubGrpc::new(Arc::new(Mutex::new(server)));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PublisherServer::new(adapter.clone()))
            .add_service(SubscriberServer::new(adapter.clone()))
            .add_service(SchemaServiceServer::new(adapter))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });

    let endpoint = format!("http://{addr}");
    let channel = connect(&endpoint).await;
    Engine {
        publisher: PublisherClient::new(channel.clone()),
        subscriber: SubscriberClient::new(channel.clone()),
        schemas: SchemaServiceClient::new(channel),
        project: PROJECT.to_string(),
    }
}

/// Connect a channel, retrying briefly while the background server task starts
/// accepting (the listener is already bound, so this is near-instant).
async fn connect(endpoint: &str) -> Channel {
    let mut last_err = None;
    for _ in 0..50 {
        match Channel::from_shared(endpoint.to_string())
            .expect("endpoint")
            .connect()
            .await
        {
            Ok(c) => return c,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("connect {endpoint}: {last_err:?}");
}

/// Unique temp dir per call (mirrors the legacy `t.TempDir()` per-engine isolation).
fn temp_dir() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-ps-grpc-conf-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir.to_string_lossy().to_string()
}

/// `topicName` — the legacy harness's resource-name helper.
fn topic_name(project: &str, id: &str) -> String {
    format!("projects/{project}/topics/{id}")
}

/// `paritySubName` — subscription resource name helper.
fn sub_name(project: &str, id: &str) -> String {
    format!("projects/{project}/subscriptions/{id}")
}

fn snapshot_name(project: &str, id: &str) -> String {
    format!("projects/{project}/snapshots/{id}")
}

fn schema_name(project: &str, id: &str) -> String {
    format!("projects/{project}/schemas/{id}")
}

// --- Scenarios -------------------------------------------------------------
// Each #[tokio::test] is the Rust port of one `t.Run(...)` subtest in
// grpc_parity_test.rs, with the legacy bufconn leg dropped. Default config matches
// the legacy `parityConfig{project: "devcloud", defaultAckDeadline: 30}`.

fn default_cfg() -> EngineConfig {
    EngineConfig {
        default_ack_deadline: 30,
        ..Default::default()
    }
}

#[tokio::test]
async fn topic_crud() {
    let mut e = dial(default_cfg()).await;
    let name = topic_name(&e.project, "crud-topic");

    let created = e
        .publisher
        .create_topic(Topic {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic")
        .into_inner();
    assert_eq!(created.name, name, "created topic name");

    let got = e
        .publisher
        .get_topic(GetTopicRequest {
            topic: name.clone(),
        })
        .await
        .expect("GetTopic")
        .into_inner();
    assert_eq!(got.name, name, "got topic name");

    // Duplicate create is AlreadyExists.
    let dup = e
        .publisher
        .create_topic(Topic {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect_err("duplicate CreateTopic");
    assert_eq!(dup.code(), Code::AlreadyExists, "duplicate CreateTopic");

    e.publisher
        .delete_topic(DeleteTopicRequest {
            topic: name.clone(),
        })
        .await
        .expect("DeleteTopic");

    let after = e
        .publisher
        .get_topic(GetTopicRequest { topic: name })
        .await
        .expect_err("GetTopic after delete");
    assert_eq!(after.code(), Code::NotFound, "GetTopic after delete");
}

#[tokio::test]
async fn subscription_crud() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "sub-crud-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let name = sub_name(&e.project, "sub-crud-sub");
    let created = e
        .subscriber
        .create_subscription(Subscription {
            name: name.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription")
        .into_inner();
    assert_eq!(created.topic, topic, "subscription topic");
    assert_eq!(created.ack_deadline_seconds, 30, "ack deadline");

    let got = e
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: name.clone(),
        })
        .await
        .expect("GetSubscription")
        .into_inner();
    assert_eq!(got.name, name, "got subscription name");

    let dup = e
        .subscriber
        .create_subscription(Subscription {
            name: name.clone(),
            topic: topic.clone(),
            ..Default::default()
        })
        .await
        .expect_err("duplicate CreateSubscription");
    assert_eq!(
        dup.code(),
        Code::AlreadyExists,
        "duplicate CreateSubscription"
    );

    e.subscriber
        .delete_subscription(DeleteSubscriptionRequest {
            subscription: name.clone(),
        })
        .await
        .expect("DeleteSubscription");

    let after = e
        .subscriber
        .get_subscription(GetSubscriptionRequest { subscription: name })
        .await
        .expect_err("GetSubscription after delete");
    assert_eq!(after.code(), Code::NotFound, "GetSubscription after delete");
}

#[tokio::test]
async fn publish_pull_ack() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "ppa-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "ppa-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    let publish = e
        .publisher
        .publish(PublishRequest {
            topic: topic.clone(),
            messages: vec![PubsubMessage {
                data: b"hello over grpc".to_vec(),
                attributes: [("source".to_string(), "parity".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish")
        .into_inner();
    assert_eq!(publish.message_ids.len(), 1, "message ids");
    assert!(!publish.message_ids[0].is_empty(), "message id non-empty");

    let pull = e
        .subscriber
        .pull(PullRequest {
            subscription: subscription.clone(),
            max_messages: 1,
            ..Default::default()
        })
        .await
        .expect("Pull")
        .into_inner();
    assert_eq!(pull.received_messages.len(), 1, "received");
    let received = &pull.received_messages[0];
    let msg = received.message.as_ref().expect("message present");
    assert_eq!(msg.data, b"hello over grpc", "data");
    assert_eq!(
        msg.attributes.get("source").map(String::as_str),
        Some("parity"),
        "attributes"
    );
    assert!(!received.ack_id.is_empty(), "ack id empty");

    e.subscriber
        .acknowledge(AcknowledgeRequest {
            subscription: subscription.clone(),
            ack_ids: vec![received.ack_id.clone()],
        })
        .await
        .expect("Acknowledge");

    let empty = e
        .subscriber
        .pull(PullRequest {
            subscription,
            max_messages: 1,
            return_immediately: true,
        })
        .await
        .expect("Pull after ack")
        .into_inner();
    assert_eq!(empty.received_messages.len(), 0, "received after ack");
}

#[tokio::test]
async fn snapshot() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "snap-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "snap-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    let snap = snapshot_name(&e.project, "snap-1");
    let created = e
        .subscriber
        .create_snapshot(CreateSnapshotRequest {
            name: snap.clone(),
            subscription: subscription.clone(),
            labels: Default::default(),
        })
        .await
        .expect("CreateSnapshot")
        .into_inner();
    assert_eq!(created.name, snap, "snapshot name");
    assert_eq!(created.topic, topic, "snapshot topic");

    let got = e
        .subscriber
        .get_snapshot(GetSnapshotRequest {
            snapshot: snap.clone(),
        })
        .await
        .expect("GetSnapshot")
        .into_inner();
    assert_eq!(got.name, snap, "got snapshot name");

    e.subscriber
        .seek(SeekRequest {
            subscription,
            target: Some(
                devcloud_pubsub::proto::pubsub::seek_request::Target::Snapshot(snap.clone()),
            ),
        })
        .await
        .expect("Seek to snapshot");

    e.subscriber
        .delete_snapshot(DeleteSnapshotRequest {
            snapshot: snap.clone(),
        })
        .await
        .expect("DeleteSnapshot");

    let after = e
        .subscriber
        .get_snapshot(GetSnapshotRequest { snapshot: snap })
        .await
        .expect_err("GetSnapshot after delete");
    assert_eq!(after.code(), Code::NotFound, "GetSnapshot after delete");
}

#[tokio::test]
async fn schema_crud() {
    let mut e = dial(default_cfg()).await;
    let name = schema_name(&e.project, "parity-schema");

    let created = e
        .schemas
        .create_schema(CreateSchemaRequest {
            parent: format!("projects/{}", e.project),
            schema_id: "parity-schema".to_string(),
            schema: Some(Schema {
                r#type: schema::Type::Avro as i32,
                definition:
                    r#"{"type":"record","name":"P","fields":[{"name":"id","type":"string"}]}"#
                        .to_string(),
                ..Default::default()
            }),
        })
        .await
        .expect("CreateSchema")
        .into_inner();
    assert_eq!(created.name, name, "schema name");
    assert_eq!(
        created.r#type,
        schema::Type::Avro as i32,
        "schema type AVRO"
    );

    let got = e
        .schemas
        .get_schema(GetSchemaRequest {
            name: name.clone(),
            view: 0,
        })
        .await
        .expect("GetSchema")
        .into_inner();
    assert_eq!(got.name, name, "got schema name");

    let dup = e
        .schemas
        .create_schema(CreateSchemaRequest {
            parent: format!("projects/{}", e.project),
            schema_id: "parity-schema".to_string(),
            schema: Some(Schema {
                r#type: schema::Type::Avro as i32,
                definition: created.definition.clone(),
                ..Default::default()
            }),
        })
        .await
        .expect_err("duplicate CreateSchema");
    assert_eq!(dup.code(), Code::AlreadyExists, "duplicate CreateSchema");

    e.schemas
        .delete_schema(DeleteSchemaRequest { name: name.clone() })
        .await
        .expect("DeleteSchema");

    let after = e
        .schemas
        .get_schema(GetSchemaRequest { name, view: 0 })
        .await
        .expect_err("GetSchema after delete");
    assert_eq!(after.code(), Code::NotFound, "GetSchema after delete");
}

#[tokio::test]
async fn streaming_pull_flow_control() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "stream-flow-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "stream-flow-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    e.publisher
        .publish(PublishRequest {
            topic,
            messages: vec![
                PubsubMessage {
                    data: b"first".to_vec(),
                    ..Default::default()
                },
                PubsubMessage {
                    data: b"second".to_vec(),
                    ..Default::default()
                },
            ],
        })
        .await
        .expect("Publish");

    // Drive the bidi stream from an mpsc channel so the test can interleave
    // sending acks with awaiting responses (mirrors the legacy stream.Send /
    // stream.Recv interleaving).
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<StreamingPullRequest>();
    tx.send(StreamingPullRequest {
        subscription: subscription.clone(),
        stream_ack_deadline_seconds: 30,
        max_outstanding_messages: 1,
        ..Default::default()
    })
    .expect("send initial");

    let mut resp_stream = e
        .subscriber
        .streaming_pull(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
        .await
        .expect("StreamingPull")
        .into_inner();

    let first = next_response(&mut resp_stream).await.expect("recv first");
    assert_eq!(first.received_messages.len(), 1, "first response count");
    assert_eq!(message_data(&first, 0), b"first", "first response data");
    let first_ack = first.received_messages[0].ack_id.clone();

    // With MaxOutstandingMessages=1 the second message must NOT arrive until the
    // first is acked: a bounded wait should time out.
    let early = tokio::time::timeout(Duration::from_millis(120), resp_stream.message()).await;
    assert!(early.is_err(), "received second before ack: {early:?}");

    // Ack the first; the second must now be delivered.
    tx.send(StreamingPullRequest {
        ack_ids: vec![first_ack],
        ..Default::default()
    })
    .expect("send ack");

    let second = tokio::time::timeout(Duration::from_secs(5), next_response(&mut resp_stream))
        .await
        .expect("second not delivered after ack")
        .expect("recv second after ack");
    assert_eq!(second.received_messages.len(), 1, "second response count");
    assert_eq!(message_data(&second, 0), b"second", "second response data");

    drop(tx); // CloseSend
}

#[tokio::test]
async fn streaming_pull_ack_over_stream_no_redelivery() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "stream-ack-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "stream-ack-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    e.publisher
        .publish(PublishRequest {
            topic,
            messages: vec![PubsubMessage {
                data: b"ack me over stream".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<StreamingPullRequest>();
    tx.send(StreamingPullRequest {
        subscription: subscription.clone(),
        stream_ack_deadline_seconds: 30,
        max_outstanding_messages: 1,
        ..Default::default()
    })
    .expect("send initial");

    let mut resp_stream = e
        .subscriber
        .streaming_pull(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
        .await
        .expect("StreamingPull")
        .into_inner();

    let resp = next_response(&mut resp_stream).await.expect("recv");
    assert_eq!(resp.received_messages.len(), 1, "received");
    let ack_id = resp.received_messages[0].ack_id.clone();

    tx.send(StreamingPullRequest {
        ack_ids: vec![ack_id],
        ..Default::default()
    })
    .expect("send ack");
    drop(tx); // CloseSend

    // After a stream ack a unary Pull must observe zero retained messages. Poll
    // because the ack is applied asynchronously on the stream's receive side.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let pull = e
            .subscriber
            .pull(PullRequest {
                subscription: subscription.clone(),
                max_messages: 1,
                return_immediately: true,
            })
            .await
            .expect("Pull after stream ack")
            .into_inner();
        if pull.received_messages.is_empty() {
            return; // acked: no redelivery.
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "message redelivered after stream ack: {} messages",
                pull.received_messages.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn streaming_pull_release_on_close() {
    let mut e = dial(default_cfg()).await;
    let topic = topic_name(&e.project, "stream-close-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "stream-close-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    e.publisher
        .publish(PublishRequest {
            topic,
            messages: vec![PubsubMessage {
                data: b"release on close".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<StreamingPullRequest>();
    tx.send(StreamingPullRequest {
        subscription: subscription.clone(),
        stream_ack_deadline_seconds: 30,
        max_outstanding_messages: 1,
        ..Default::default()
    })
    .expect("send initial");

    let mut resp_stream = e
        .subscriber
        .streaming_pull(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
        .await
        .expect("StreamingPull")
        .into_inner();

    let resp = next_response(&mut resp_stream).await.expect("recv");
    assert_eq!(resp.received_messages.len(), 1, "received");
    let stream_ack_id = resp.received_messages[0].ack_id.clone();
    drop(tx); // CloseSend without acking.

    // Closing the stream without acking must release the outstanding message for
    // redelivery with a NEW ack id.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let pull = e
            .subscriber
            .pull(PullRequest {
                subscription: subscription.clone(),
                max_messages: 1,
                return_immediately: true,
            })
            .await
            .expect("Pull after stream close")
            .into_inner();
        if pull.received_messages.len() == 1 {
            assert_ne!(
                pull.received_messages[0].ack_id, stream_ack_id,
                "redelivery reused ack id"
            );
            return; // released.
        }
        if tokio::time::Instant::now() > deadline {
            panic!("outstanding message not released after stream close");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn streaming_pull_disabled() {
    let mut e = dial(EngineConfig {
        streaming_pull_disabled: true,
        ..Default::default()
    })
    .await;

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<StreamingPullRequest>();
    tx.send(StreamingPullRequest {
        subscription: sub_name(&e.project, "disabled-stream"),
        stream_ack_deadline_seconds: 10,
        ..Default::default()
    })
    .expect("send initial streaming pull request");

    // The "streaming pull is disabled" Unimplemented status may surface either
    // when the stream is opened (Rust server) or on the first message read —
    // accept Unimplemented at either point.
    match e
        .subscriber
        .streaming_pull(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
        .await
    {
        Err(status) => {
            assert_eq!(
                status.code(),
                Code::Unimplemented,
                "StreamingPull disabled (stream open)"
            );
        }
        Ok(resp) => {
            let err = resp
                .into_inner()
                .message()
                .await
                .expect_err("StreamingPull disabled");
            assert_eq!(
                err.code(),
                Code::Unimplemented,
                "StreamingPull disabled (first message)"
            );
        }
    }
}

/// Retention expiry: with a 1s retention window, a published message aged past
/// the window must be expired before delivery — a unary Pull returns 0 messages.
/// Mirrors `TestPubSubGRPCParityRetentionExpiry` (Rust leg).
#[tokio::test]
async fn retention_expiry() {
    const RETENTION_SECONDS: i64 = 1;
    let mut e = dial(EngineConfig {
        default_ack_deadline: 600, // long lease: isolate retention from lease expiry
        message_retention_seconds: RETENTION_SECONDS,
        ..Default::default()
    })
    .await;

    let topic = topic_name(&e.project, "retention-topic");
    e.publisher
        .create_topic(Topic {
            name: topic.clone(),
            ..Default::default()
        })
        .await
        .expect("CreateTopic");

    let subscription = sub_name(&e.project, "retention-sub");
    e.subscriber
        .create_subscription(Subscription {
            name: subscription.clone(),
            topic: topic.clone(),
            ack_deadline_seconds: 600,
            ..Default::default()
        })
        .await
        .expect("CreateSubscription");

    e.publisher
        .publish(PublishRequest {
            topic,
            messages: vec![PubsubMessage {
                data: b"aged out".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .expect("Publish");

    // Let the message age past the 1s retention window (real wall-clock: the
    // cleanup runs off system time, same as the legacy subprocess leg).
    tokio::time::sleep(Duration::from_millis(
        (RETENTION_SECONDS as u64) * 1000 + 1200,
    ))
    .await;

    let pull = e
        .subscriber
        .pull(PullRequest {
            subscription,
            max_messages: 1,
            return_immediately: true,
        })
        .await
        .expect("Pull")
        .into_inner();
    assert_eq!(
        pull.received_messages.len(),
        0,
        "rust engine returned messages after retention expiry, want 0"
    );
}

// --- streaming helpers -----------------------------------------------------

/// Await the next StreamingPullResponse that actually carries messages.
/// The legacy test treats each `Recv()` as delivering messages; the Rust server may
/// surface empty keep-alive frames, so skip those to match the legacy semantics.
async fn next_response(
    stream: &mut tonic::Streaming<StreamingPullResponse>,
) -> Result<StreamingPullResponse, tonic::Status> {
    loop {
        match stream.message().await? {
            Some(resp) if !resp.received_messages.is_empty() => return Ok(resp),
            Some(_) => continue, // empty frame — keep waiting for messages.
            None => {
                return Err(tonic::Status::new(
                    Code::Unavailable,
                    "stream closed before delivering messages",
                ))
            }
        }
    }
}

fn message_data(resp: &StreamingPullResponse, idx: usize) -> &[u8] {
    resp.received_messages[idx]
        .message
        .as_ref()
        .expect("message present")
        .data
        .as_slice()
}
