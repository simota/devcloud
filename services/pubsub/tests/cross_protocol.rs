//! Cross-protocol state and persistence invariants. REST requests use the real
//! router; generated tonic clients exercise gRPC over an ephemeral TCP listener.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use devcloud_pubsub::grpc::PubSubGrpc;
use devcloud_pubsub::http::{route, Request};
use devcloud_pubsub::proto::pubsub::{
    publisher_client::PublisherClient, publisher_server::PublisherServer,
    subscriber_client::SubscriberClient, subscriber_server::SubscriberServer, AcknowledgeRequest,
    GetTopicRequest, PublishRequest, PubsubMessage, PullRequest, Subscription, Topic,
};
use devcloud_pubsub::server::{Config, Server};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

fn rest(shared: &Arc<Mutex<Server>>, method: &str, path: &str, body: Value) -> Value {
    let req = Request {
        method: method.into(),
        path: path.into(),
        query: BTreeMap::new(),
        headers: BTreeMap::new(),
        body: serde_json::to_vec(&body).unwrap(),
    };
    let response = route(&mut shared.lock().unwrap(), &req);
    assert_eq!(response.status, 200, "REST operation failed");
    serde_json::from_slice(&response.body).unwrap()
}

#[tokio::test]
async fn rest_and_grpc_share_resources_delivery_and_restart_state() {
    let root = std::env::temp_dir().join(format!("devcloud-ps-cross-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let config = Config {
        project: "audit".into(),
        storage_path: root.join("resources").to_string_lossy().into_owned(),
        message_storage_path: root.join("messages").to_string_lossy().into_owned(),
        ..Default::default()
    };
    let mut server = Server::new(config.clone());
    server.set_fixed_now("2026-09-15T00:00:00.123456789Z");
    let shared = Arc::new(Mutex::new(server));
    let adapter = PubSubGrpc::new(shared.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PublisherServer::new(adapter.clone()))
            .add_service(SubscriberServer::new(adapter))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    let mut publisher = PublisherClient::new(channel.clone());
    let mut subscriber = SubscriberClient::new(channel);
    let topic = "projects/audit/topics/rest-topic";
    let sub = "projects/audit/subscriptions/grpc-sub";

    rest(&shared, "PUT", &format!("/v1/{topic}"), json!({}));
    assert_eq!(
        publisher
            .get_topic(GetTopicRequest {
                topic: topic.into()
            })
            .await
            .unwrap()
            .into_inner()
            .name,
        topic
    );
    subscriber
        .create_subscription(Subscription {
            name: sub.into(),
            topic: topic.into(),
            ack_deadline_seconds: 30,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        rest(&shared, "GET", &format!("/v1/{sub}"), json!({}))["topic"],
        topic
    );
    rest(
        &shared,
        "POST",
        &format!("/v1/{topic}:publish"),
        json!({"messages":[{"data":"YXVkaXQ="}]}),
    );
    let pulled = subscriber
        .pull(PullRequest {
            subscription: sub.into(),
            max_messages: 1,
            return_immediately: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pulled.received_messages.len(), 1);
    assert_eq!(
        pulled.received_messages[0].message.as_ref().unwrap().data,
        b"audit"
    );
    rest(
        &shared,
        "POST",
        &format!("/v1/{sub}:acknowledge"),
        json!({"ackIds":[pulled.received_messages[0].ack_id]}),
    );

    let reverse_topic = "projects/audit/topics/grpc-topic";
    let reverse_sub = "projects/audit/subscriptions/rest-sub";
    publisher
        .create_topic(Topic {
            name: reverse_topic.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        rest(&shared, "GET", &format!("/v1/{reverse_topic}"), json!({}))["name"],
        reverse_topic
    );
    rest(
        &shared,
        "PUT",
        &format!("/v1/{reverse_sub}"),
        json!({"topic":reverse_topic,"ackDeadlineSeconds":30}),
    );
    publisher
        .publish(PublishRequest {
            topic: reverse_topic.into(),
            messages: vec![PubsubMessage {
                data: b"audit".to_vec(),
                ..Default::default()
            }],
        })
        .await
        .unwrap();
    let pulled = rest(
        &shared,
        "POST",
        &format!("/v1/{reverse_sub}:pull"),
        json!({"maxMessages":1}),
    );
    assert_eq!(pulled["receivedMessages"][0]["message"]["data"], "YXVkaXQ=");
    subscriber
        .acknowledge(AcknowledgeRequest {
            subscription: reverse_sub.into(),
            ack_ids: vec![pulled["receivedMessages"][0]["ackId"]
                .as_str()
                .unwrap()
                .into()],
        })
        .await
        .unwrap();

    drop(publisher);
    drop(subscriber);
    stop.send(()).unwrap();
    task.await.unwrap();
    drop(shared);
    let mut reopened = Server::new(config);
    reopened.set_fixed_now("2026-09-15T00:01:00Z");
    assert!(reopened.load_err().is_none());
    let shared = Arc::new(Mutex::new(reopened));
    for (topic, sub) in [(topic, sub), (reverse_topic, reverse_sub)] {
        assert_eq!(
            rest(&shared, "GET", &format!("/v1/{topic}"), json!({}))["name"],
            topic
        );
        assert_eq!(
            rest(&shared, "GET", &format!("/v1/{sub}"), json!({}))["topic"],
            topic
        );
        let empty = rest(
            &shared,
            "POST",
            &format!("/v1/{sub}:pull"),
            json!({"maxMessages":1}),
        );
        assert!(
            empty.get("receivedMessages").is_none(),
            "ack must survive restart"
        );
    }
    drop(shared);
    std::fs::remove_dir_all(root).unwrap();
}
