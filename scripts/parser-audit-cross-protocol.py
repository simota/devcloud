"""Extend the existing loopback gRPC harness without changing production code."""
from pathlib import Path
import sys
root = Path(sys.argv[1] if len(sys.argv) > 1 else '.')
p = root / 'services/pubsub/tests/grpc_conformance.rs'
s = p.read_text()
old = 'struct Engine {\n    publisher:'
assert s.count(old) == 1
s = s.replace(old, 'struct Engine {\n    state: Arc<Mutex<Server>>,\n    publisher:', 1)
old = '    let adapter = PubSubGrpc::new(Arc::new(Mutex::new(server)));'
assert s.count(old) == 1
s = s.replace(old, '    let state = Arc::new(Mutex::new(server));\n    let adapter = PubSubGrpc::new(state.clone());', 1)
old = '    Engine {\n        publisher:'
assert s.count(old) == 1
s = s.replace(old, '    Engine {\n        state,\n        publisher:', 1)
s += r'''

/// Exercise the REST protocol router against the exact state used by the
/// loopback tonic server; never keep its mutex guard across an RPC await.
fn rest(e: &Engine, method: &str, path: &str, body: serde_json::Value) -> serde_json::Value {
    let request = devcloud_pubsub::http::Request {
        method: method.to_string(),
        path: path.to_string(),
        query: Default::default(),
        headers: Default::default(),
        body: serde_json::to_vec(&body).unwrap(),
    };
    let response = devcloud_pubsub::http::route(&mut e.state.lock().unwrap(), &request);
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

#[tokio::test]
async fn regression_rest_and_grpc_share_resources_deliveries_and_acknowledgements() {
    let mut e = dial(EngineConfig::default()).await;
    let topic = "projects/devcloud/topics/cross-rest-topic".to_string();
    let subscription = "projects/devcloud/subscriptions/cross-grpc-sub".to_string();
    rest(&e, "PUT", &format!("/v1/{topic}"), serde_json::json!({}));
    assert_eq!(
        e.publisher.get_topic(GetTopicRequest { topic: topic.clone() })
            .await.unwrap().into_inner().name,
        topic
    );
    e.subscriber.create_subscription(Subscription {
        name: subscription.clone(),
        topic: topic.clone(),
        ack_deadline_seconds: 600,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(
        rest(&e, "GET", &format!("/v1/{subscription}"), serde_json::json!({}))["topic"],
        topic
    );

    rest(&e, "POST", &format!("/v1/{topic}:publish"), serde_json::json!({
        "messages": [{"data": "aGk="}]
    }));
    let received = e.subscriber.pull(PullRequest {
        subscription: subscription.clone(),
        max_messages: 1,
        return_immediately: true,
    }).await.unwrap().into_inner().received_messages;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].message.as_ref().unwrap().data, b"hi");
    rest(&e, "POST", &format!("/v1/{subscription}:acknowledge"), serde_json::json!({
        "ackIds": [received[0].ack_id.clone()]
    }));
    assert!(e.subscriber.pull(PullRequest {
        subscription: subscription.clone(), max_messages: 1, return_immediately: true,
    }).await.unwrap().into_inner().received_messages.is_empty());

    e.publisher.publish(PublishRequest {
        topic: topic.clone(),
        messages: vec![PubsubMessage { data: b"from grpc".to_vec(), ..Default::default() }],
    }).await.unwrap();
    let received = rest(&e, "POST", &format!("/v1/{subscription}:pull"), serde_json::json!({
        "maxMessages": 1, "returnImmediately": true
    }));
    assert_eq!(received["receivedMessages"].as_array().unwrap().len(), 1);
    assert_eq!(received["receivedMessages"][0]["message"]["data"], "ZnJvbSBncnBj");
    let ack_id = received["receivedMessages"][0]["ackId"].as_str().unwrap().to_string();
    e.subscriber.acknowledge(AcknowledgeRequest {
        subscription: subscription.clone(), ack_ids: vec![ack_id],
    }).await.unwrap();
    assert_eq!(
        rest(&e, "POST", &format!("/v1/{subscription}:pull"), serde_json::json!({
            "maxMessages": 1, "returnImmediately": true
        })),
        serde_json::json!({})
    );

    let inverse_topic = "projects/devcloud/topics/cross-grpc-topic".to_string();
    let inverse_sub = "projects/devcloud/subscriptions/cross-rest-sub".to_string();
    e.publisher.create_topic(Topic { name: inverse_topic.clone(), ..Default::default() })
        .await.unwrap();
    assert_eq!(
        rest(&e, "GET", &format!("/v1/{inverse_topic}"), serde_json::json!({}))["name"],
        inverse_topic
    );
    rest(&e, "PUT", &format!("/v1/{inverse_sub}"), serde_json::json!({"topic": inverse_topic}));
    assert_eq!(
        e.subscriber.get_subscription(GetSubscriptionRequest { subscription: inverse_sub })
            .await.unwrap().into_inner().topic,
        inverse_topic
    );
}
'''
p.write_text(s)
