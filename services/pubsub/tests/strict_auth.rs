//! Topic/subscription IAM policy storage and `strict` auth enforcement over
//! REST and gRPC. Relaxed behavior is covered by the parity test files.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use devcloud_pubsub::grpc::PubSubGrpc;
use devcloud_pubsub::http::{route, Request};
use devcloud_pubsub::proto::pubsub::{
    publisher_client::PublisherClient, publisher_server::PublisherServer,
    subscriber_client::SubscriberClient, subscriber_server::SubscriberServer, GetTopicRequest,
    ListTopicsRequest, PublishRequest, PubsubMessage, PullRequest, StreamingPullRequest,
};
use devcloud_pubsub::server::{Config, Server};
use devcloud_pubsub::strict_auth::parse_principals;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

const OWNER: &str = "owner-token";
const PUBLISHER: &str = "publisher-token";
const READER: &str = "reader-token";
const TOPIC: &str = "/v1/projects/devcloud/topics/orders";
const SUB: &str = "/v1/projects/devcloud/subscriptions/orders-sub";

fn temp_dir() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-ps-strict-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir.to_string_lossy().into_owned()
}

fn server(mode: &str, storage: &str) -> Server {
    let mut s = Server::new(Config {
        project: "devcloud".into(),
        auth_mode: mode.into(),
        bearer_token: OWNER.into(),
        storage_path: storage.into(),
        message_storage_path: temp_dir(),
        default_ack_deadline_seconds: 10,
        max_ack_deadline_seconds: 600,
        max_pull_messages: 1000,
        message_retention_seconds: 604_800,
        ..Default::default()
    });
    assert_eq!(s.load_err(), None);
    for (token, member) in parse_principals(&format!(
        "{PUBLISHER}=serviceAccount:pub@devcloud.iam.gserviceaccount.com,{READER}=user:reader@example.com"
    )) {
        s.add_principal(token, member);
    }
    s
}

fn call(s: &mut Server, method: &str, path: &str, body: Value, token: &str) -> (u16, Value) {
    let mut headers = BTreeMap::new();
    if !token.is_empty() {
        headers.insert("authorization".to_string(), format!("Bearer {token}"));
    }
    let req = Request {
        method: method.into(),
        path: path.into(),
        query: BTreeMap::new(),
        headers,
        body: serde_json::to_vec(&body).unwrap(),
    };
    let resp = route(s, &req);
    let value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
    (resp.status, value)
}

fn publish_body() -> Value {
    json!({"messages":[{"data":"aGk="}]})
}

/// Owner creates `orders` + `orders-sub` and grants the publisher/reader roles.
fn seed(s: &mut Server) {
    assert_eq!(call(s, "PUT", TOPIC, json!({}), OWNER).0, 200);
    let sub = json!({"topic":"projects/devcloud/topics/orders"});
    assert_eq!(call(s, "PUT", SUB, sub, OWNER).0, 200);
    let topic_policy = json!({"policy":{"bindings":[
        {"role":"roles/pubsub.publisher","members":["serviceAccount:pub@devcloud.iam.gserviceaccount.com"]},
        {"role":"roles/pubsub.viewer","members":["allAuthenticatedUsers"]}
    ]}});
    let set = format!("{TOPIC}:setIamPolicy");
    assert_eq!(call(s, "POST", &set, topic_policy, OWNER).0, 200);
    let sub_policy = json!({"policy":{"bindings":[
        {"role":"roles/pubsub.subscriber","members":["user:reader@example.com"]},
        {"role":"roles/pubsub.admin","members":["serviceAccount:pub@devcloud.iam.gserviceaccount.com"],
         "condition":{"title":"never","expression":"false"}}
    ]}});
    let set = format!("{SUB}:setIamPolicy");
    assert_eq!(call(s, "POST", &set, sub_policy, OWNER).0, 200);
}

#[test]
fn iam_policies_are_stored_persisted_and_pruned_on_delete() {
    let storage = temp_dir();
    let mut s = server("relaxed", &storage);
    seed(&mut s);
    let get = format!("{TOPIC}:getIamPolicy");
    let (status, policy) = call(&mut s, "POST", &get, json!({}), "");
    assert_eq!(status, 200);
    assert_eq!(policy["bindings"][0]["role"], "roles/pubsub.publisher");

    // Survives a restart.
    let mut s = server("relaxed", &storage);
    assert_eq!(call(&mut s, "POST", &get, json!({}), "").1, policy);

    // Deleting and recreating the subscription starts with an empty policy.
    assert_eq!(call(&mut s, "DELETE", SUB, json!({}), "").0, 204);
    let sub = json!({"topic":"projects/devcloud/topics/orders"});
    assert_eq!(call(&mut s, "PUT", SUB, sub, "").0, 200);
    let sub_get = format!("{SUB}:getIamPolicy");
    let (_, sub_policy) = call(&mut s, "POST", &sub_get, json!({}), "");
    assert_eq!(sub_policy, json!({"bindings":[],"version":1}));
}

#[test]
fn strict_rest_enforces_topic_and_subscription_policies() {
    let mut s = server("strict", "");
    seed(&mut s);
    let publish = format!("{TOPIC}:publish");
    let pull = format!("{SUB}:pull");

    // The publisher can publish, the reader cannot.
    assert_eq!(
        call(&mut s, "POST", &publish, publish_body(), PUBLISHER).0,
        200
    );
    let (status, err) = call(&mut s, "POST", &publish, publish_body(), READER);
    assert_eq!(status, 403);
    assert_eq!(err["error"]["status"], "PERMISSION_DENIED");

    // The reader can pull; the publisher's conditional admin grant is ignored.
    let (status, pulled) = call(&mut s, "POST", &pull, json!({"maxMessages":10}), READER);
    assert_eq!(status, 200);
    assert_eq!(pulled["receivedMessages"].as_array().unwrap().len(), 1);
    assert_eq!(call(&mut s, "POST", &pull, json!({}), PUBLISHER).0, 403);
    let ack_id = pulled["receivedMessages"][0]["ackId"].clone();
    for action in ["acknowledge", "modifyAckDeadline", "seek"] {
        let path = format!("{SUB}:{action}");
        let body =
            json!({"ackIds":[ack_id], "ackDeadlineSeconds": 0, "time": "2026-01-01T00:00:00Z"});
        assert_eq!(
            call(&mut s, "POST", &path, body, PUBLISHER).0,
            403,
            "{action}"
        );
    }
    let ack = format!("{SUB}:acknowledge");
    assert_eq!(
        call(&mut s, "POST", &ack, json!({"ackIds":[ack_id]}), READER).0,
        200
    );

    // allAuthenticatedUsers viewer: both principals can get the topic.
    assert_eq!(call(&mut s, "GET", TOPIC, json!({}), READER).0, 200);
    assert_eq!(call(&mut s, "DELETE", TOPIC, json!({}), READER).0, 403);
    // Only the owner may change policies or make project-level calls.
    let set = format!("{TOPIC}:setIamPolicy");
    assert_eq!(
        call(&mut s, "POST", &set, json!({"policy":{}}), PUBLISHER).0,
        403
    );
    let topics = "/v1/projects/devcloud/topics";
    assert_eq!(call(&mut s, "GET", topics, json!({}), PUBLISHER).0, 403);
    assert_eq!(call(&mut s, "GET", topics, json!({}), OWNER).0, 200);

    // Anonymous or unknown tokens are unauthenticated.
    assert_eq!(call(&mut s, "GET", TOPIC, json!({}), "").0, 401);
    assert_eq!(
        call(&mut s, "POST", &publish, publish_body(), "nope").0,
        401
    );
}

#[test]
fn strict_rest_allows_anonymous_only_through_all_users() {
    let mut s = server("strict", "");
    seed(&mut s);
    let publish = format!("{TOPIC}:publish");
    assert_eq!(call(&mut s, "POST", &publish, publish_body(), "").0, 401);
    let open =
        json!({"policy":{"bindings":[{"role":"roles/pubsub.publisher","members":["allUsers"]}]}});
    let set = format!("{TOPIC}:setIamPolicy");
    assert_eq!(call(&mut s, "POST", &set, open, OWNER).0, 200);
    assert_eq!(call(&mut s, "POST", &publish, publish_body(), "").0, 200);
}

#[test]
fn non_strict_modes_ignore_policies() {
    let mut s = server("relaxed", "");
    seed(&mut s);
    let publish = format!("{TOPIC}:publish");
    assert_eq!(call(&mut s, "POST", &publish, publish_body(), "").0, 200);
    assert_eq!(
        call(&mut s, "POST", &publish, publish_body(), READER).0,
        200
    );

    let mut s = server("bearer-dev", "");
    seed(&mut s);
    assert_eq!(call(&mut s, "POST", &publish, publish_body(), OWNER).0, 200);
    assert_eq!(
        call(&mut s, "POST", &publish, publish_body(), PUBLISHER).0,
        401
    );
}

fn with_token<T>(message: T, token: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    if !token.is_empty() {
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    req
}

#[tokio::test]
async fn strict_grpc_enforces_the_same_policies() {
    let mut s = server("strict", "");
    seed(&mut s);
    let shared = Arc::new(Mutex::new(s));
    let adapter = PubSubGrpc::new(shared.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
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
    let topic = "projects/devcloud/topics/orders".to_string();
    let sub = "projects/devcloud/subscriptions/orders-sub".to_string();
    let publish = |token: &str| {
        with_token(
            PublishRequest {
                topic: topic.clone(),
                messages: vec![PubsubMessage {
                    data: b"hi".to_vec(),
                    ..Default::default()
                }],
            },
            token,
        )
    };

    assert!(publisher.publish(publish(PUBLISHER)).await.is_ok());
    let denied = publisher.publish(publish(READER)).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    let anon = publisher.publish(publish("")).await.unwrap_err();
    assert_eq!(anon.code(), tonic::Code::Unauthenticated);

    let pull = |token: &str| {
        with_token(
            PullRequest {
                subscription: sub.clone(),
                max_messages: 10,
                return_immediately: true,
                ..Default::default()
            },
            token,
        )
    };
    let pulled = subscriber.pull(pull(READER)).await.unwrap().into_inner();
    assert_eq!(pulled.received_messages.len(), 1);
    let denied = subscriber.pull(pull(PUBLISHER)).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    // StreamingPull checks the subscription named in the first request.
    let open = |token: &str| {
        let first = StreamingPullRequest {
            subscription: sub.clone(),
            stream_ack_deadline_seconds: 10,
            ..Default::default()
        };
        with_token(tokio_stream::iter(vec![first]), token)
    };
    let denied = subscriber
        .streaming_pull(open(PUBLISHER))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(subscriber.streaming_pull(open(READER)).await.is_ok());

    let get = with_token(
        GetTopicRequest {
            topic: topic.clone(),
        },
        READER,
    );
    assert!(publisher.get_topic(get).await.is_ok());
    let list = with_token(
        ListTopicsRequest {
            project: "projects/devcloud".into(),
            ..Default::default()
        },
        READER,
    );
    let denied = publisher.list_topics(list).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    let list = with_token(
        ListTopicsRequest {
            project: "projects/devcloud".into(),
            ..Default::default()
        },
        OWNER,
    );
    assert!(publisher.list_topics(list).await.is_ok());

    let _ = stop.send(());
    task.await.unwrap();
}
