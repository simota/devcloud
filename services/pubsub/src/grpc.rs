//! Pub/Sub gRPC adapter (stage ②: all unary RPCs).
//!
//! Mirrors `internal/services/pubsub/{grpc,topic_grpc,subscription_grpc,
//! pull_grpc,snapshot_grpc,schema_grpc}.rs`. The tonic service impls convert
//! proto ↔ model and delegate to the `grpc_*` business methods on the shared
//! `Server` (the same in-memory state the REST handlers mutate). `StreamingPull`
//! (stage ③) is a full bidirectional stream mirroring
//! `streaming_pull_grpc.rs`.
//!
//! Error mapping: the `grpc_*` methods return `ApiError`, whose code string maps
//! to a gRPC `Code` (INVALID_ARGUMENT → InvalidArgument, NOT_FOUND → NotFound,
//! ALREADY_EXISTS → AlreadyExists, FAILED_PRECONDITION → FailedPrecondition,
//! INTERNAL → Internal), reproducing the legacy gRPC `codes.X` + message wording.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use serde_json::{Map, Value};
use tonic::{Request, Response, Status};

use crate::errors::ApiError;
use crate::model::{
    Schema as SchemaModel, Snapshot as SnapshotModel, Subscription as SubModel, Topic as TopicModel,
};
use crate::proto::pubsub as pb;
use crate::server::Server;

/// Shared state handle: the gRPC and REST tasks both hold this `Arc<Mutex<…>>`,
/// mirroring how the legacy gRPC adapter and REST handlers share one `*Server`.
pub type SharedServer = Arc<Mutex<Server>>;

/// The tonic adapter holding the shared state.
#[derive(Clone)]
pub struct PubSubGrpc {
    server: SharedServer,
}

impl PubSubGrpc {
    pub fn new(server: SharedServer) -> Self {
        PubSubGrpc { server }
    }
}

// --- error mapping ---------------------------------------------------------

fn to_status(err: ApiError) -> Status {
    let code = match err.code.as_str() {
        "INVALID_ARGUMENT" => tonic::Code::InvalidArgument,
        "NOT_FOUND" => tonic::Code::NotFound,
        "ALREADY_EXISTS" => tonic::Code::AlreadyExists,
        "FAILED_PRECONDITION" => tonic::Code::FailedPrecondition,
        "INTERNAL" => tonic::Code::Internal,
        _ => tonic::Code::Unknown,
    };
    Status::new(code, err.message)
}

// --- grpcProjectID ---------------------------------------------------------

/// `grpcProjectID` — parse `projects/<id>` into the bare id.
fn grpc_project_id(project: &str) -> Result<String, Status> {
    let project = project.trim();
    let parts: Vec<&str> = project.split('/').collect();
    if parts.len() == 2 && parts[0] == "projects" && crate::paths::valid_project_id(parts[1]) {
        Ok(parts[1].to_string())
    } else {
        Err(Status::invalid_argument("invalid project name"))
    }
}

// --- well-known type conversions ------------------------------------------

/// `protoDuration` — render a legacy duration string (e.g. "60s") to a proto
/// Duration. Invalid/empty → None.
fn duration_to_proto(raw: &str) -> Option<prost_types::Duration> {
    if raw.trim().is_empty() {
        return None;
    }
    let nanos = crate::duration::parse_go_duration(raw)?;
    let secs = (nanos / 1_000_000_000) as i64;
    let sub = (nanos % 1_000_000_000) as i32;
    Some(prost_types::Duration {
        seconds: secs,
        nanos: sub,
    })
}

/// `grpcDurationString` — proto Duration → legacy duration string ("<n>s"), using
/// whole seconds (matches legacy `fmt.Sprintf("%ds", seconds)`).
fn duration_from_proto(d: &Option<prost_types::Duration>) -> String {
    match d {
        Some(d) => format!("{}s", d.seconds),
        None => String::new(),
    }
}

/// Parse an RFC3339(Nano) string to a proto Timestamp.
fn timestamp_to_proto(raw: &str) -> Option<prost_types::Timestamp> {
    if raw.trim().is_empty() {
        return None;
    }
    let (secs, nanos) = crate::time_fmt::parse_rfc3339(raw)?;
    Some(prost_types::Timestamp {
        seconds: secs,
        nanos: nanos as i32,
    })
}

/// Render a proto Timestamp to RFC3339Nano (UTC), matching `timestamppb` → legacy
/// time formatting used when the gRPC client sends a time and the REST layer
/// would store it.
fn timestamp_to_rfc3339(ts: &prost_types::Timestamp) -> String {
    crate::time_fmt::rfc3339nano_from_unix(ts.seconds, ts.nanos.max(0) as u32)
}

fn copy_map(m: &std::collections::HashMap<String, String>) -> BTreeMap<String, String> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn to_proto_map(m: &BTreeMap<String, String>) -> std::collections::HashMap<String, String> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

// --- Topic <-> proto -------------------------------------------------------

fn topic_to_proto(t: &TopicModel) -> pb::Topic {
    pb::Topic {
        name: t.name.clone(),
        labels: to_proto_map(&t.labels),
        message_storage_policy: None,
        kms_key_name: t.kms_key_name.clone(),
        schema_settings: schema_settings_to_proto(t.schema_settings.as_ref()),
        satisfies_pzs: false,
        message_retention_duration: duration_to_proto(&t.message_retention_duration),
    }
}

fn topic_from_proto(t: &pb::Topic) -> TopicModel {
    TopicModel {
        name: t.name.clone(),
        labels: copy_map(&t.labels),
        created_at: String::new(),
        updated_at: String::new(),
        message_retention_duration: duration_from_proto(&t.message_retention_duration),
        schema_settings: schema_settings_from_proto(t.schema_settings.as_ref()),
        kms_key_name: t.kms_key_name.clone(),
    }
}

/// `grpcSchemaSettings` — proto SchemaSettings → sorted JSON map (omitting empties).
fn schema_settings_from_proto(s: Option<&pb::SchemaSettings>) -> Option<Value> {
    let s = s?;
    let mut map = Map::new();
    if !s.schema.trim().is_empty() {
        map.insert("schema".into(), Value::String(s.schema.trim().to_string()));
    }
    let enc = encoding_string(s.encoding);
    if !enc.is_empty() {
        map.insert("encoding".into(), Value::String(enc));
    }
    if !s.first_revision_id.trim().is_empty() {
        map.insert(
            "firstRevisionId".into(),
            Value::String(s.first_revision_id.trim().to_string()),
        );
    }
    if !s.last_revision_id.trim().is_empty() {
        map.insert(
            "lastRevisionId".into(),
            Value::String(s.last_revision_id.trim().to_string()),
        );
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// `protoSchemaSettings` — sorted JSON map → proto SchemaSettings.
fn schema_settings_to_proto(settings: Option<&Value>) -> Option<pb::SchemaSettings> {
    let obj = settings?.as_object()?;
    if obj.is_empty() {
        return None;
    }
    let schema = obj.get("schema").and_then(Value::as_str).unwrap_or("");
    let encoding = obj.get("encoding").and_then(Value::as_str).unwrap_or("");
    let first = obj
        .get("firstRevisionId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let last = obj
        .get("lastRevisionId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let enc = encoding_to_proto(encoding);
    if schema.is_empty()
        && enc == pb::Encoding::Unspecified as i32
        && first.is_empty()
        && last.is_empty()
    {
        return None;
    }
    Some(pb::SchemaSettings {
        schema: schema.to_string(),
        encoding: enc,
        first_revision_id: first.to_string(),
        last_revision_id: last.to_string(),
    })
}

fn encoding_string(enc: i32) -> String {
    match pb::Encoding::try_from(enc) {
        Ok(pb::Encoding::Json) => "JSON".to_string(),
        Ok(pb::Encoding::Binary) => "BINARY".to_string(),
        _ => String::new(),
    }
}

fn encoding_to_proto(enc: &str) -> i32 {
    match enc {
        "JSON" => pb::Encoding::Json as i32,
        "BINARY" => pb::Encoding::Binary as i32,
        _ => pb::Encoding::Unspecified as i32,
    }
}

// --- Subscription <-> proto ------------------------------------------------

fn sub_to_proto(s: &SubModel) -> pb::Subscription {
    pb::Subscription {
        name: s.name.clone(),
        topic: s.topic.clone(),
        push_config: push_config_to_proto(s.push_config.as_ref()),
        bigquery_config: None,
        cloud_storage_config: None,
        bigtable_config: None,
        ack_deadline_seconds: s.ack_deadline_seconds as i32,
        retain_acked_messages: s.retain_acked_messages,
        message_retention_duration: duration_to_proto(&s.message_retention_duration),
        labels: to_proto_map(&s.labels),
        enable_message_ordering: s.enable_message_ordering,
        expiration_policy: None,
        filter: s.filter.clone(),
        dead_letter_policy: dead_letter_to_proto(s.dead_letter_policy.as_ref()),
        retry_policy: retry_to_proto(s.retry_policy.as_ref()),
        detached: s.detached,
        enable_exactly_once_delivery: s.enable_exactly_once_delivery,
        topic_message_retention_duration: None,
    }
}

fn sub_from_proto(s: &pb::Subscription) -> SubModel {
    SubModel {
        name: s.name.clone(),
        topic: s.topic.clone(),
        detached: s.detached,
        labels: copy_map(&s.labels),
        created_at: String::new(),
        updated_at: String::new(),
        ack_deadline_seconds: s.ack_deadline_seconds as i64,
        enable_message_ordering: s.enable_message_ordering,
        enable_exactly_once_delivery: s.enable_exactly_once_delivery,
        retain_acked_messages: s.retain_acked_messages,
        message_retention_duration: duration_from_proto(&s.message_retention_duration),
        expiration_policy: None,
        filter: s.filter.clone(),
        dead_letter_policy: dead_letter_from_proto(s.dead_letter_policy.as_ref()),
        retry_policy: retry_from_proto(s.retry_policy.as_ref()),
        push_config: push_config_from_proto(s.push_config.as_ref()),
    }
}

/// `grpcDeadLetterPolicy`.
fn dead_letter_from_proto(p: Option<&pb::DeadLetterPolicy>) -> Option<Value> {
    let p = p?;
    let mut map = Map::new();
    map.insert(
        "deadLetterTopic".into(),
        Value::String(p.dead_letter_topic.clone()),
    );
    map.insert(
        "maxDeliveryAttempts".into(),
        Value::Number(serde_json::Number::from(p.max_delivery_attempts as i64)),
    );
    Some(Value::Object(map))
}

/// `protoDeadLetterPolicy`.
fn dead_letter_to_proto(p: Option<&Value>) -> Option<pb::DeadLetterPolicy> {
    let obj = p?.as_object()?;
    if obj.is_empty() {
        return None;
    }
    let max = obj.get("maxDeliveryAttempts").and_then(Value::as_i64)?;
    let topic = obj
        .get("deadLetterTopic")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(pb::DeadLetterPolicy {
        dead_letter_topic: topic,
        max_delivery_attempts: max as i32,
    })
}

/// `grpcRetryPolicy`.
fn retry_from_proto(p: Option<&pb::RetryPolicy>) -> Option<Value> {
    let p = p?;
    let mut map = Map::new();
    if let Some(min) = &p.minimum_backoff {
        map.insert(
            "minimumBackoff".into(),
            Value::String(format!("{}s", min.seconds)),
        );
    }
    if let Some(max) = &p.maximum_backoff {
        map.insert(
            "maximumBackoff".into(),
            Value::String(format!("{}s", max.seconds)),
        );
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// `protoRetryPolicy`.
fn retry_to_proto(p: Option<&Value>) -> Option<pb::RetryPolicy> {
    let obj = p?.as_object()?;
    if obj.is_empty() {
        return None;
    }
    let min = obj
        .get("minimumBackoff")
        .and_then(Value::as_str)
        .and_then(duration_to_proto);
    let max = obj
        .get("maximumBackoff")
        .and_then(Value::as_str)
        .and_then(duration_to_proto);
    if min.is_none() && max.is_none() {
        return None;
    }
    Some(pb::RetryPolicy {
        minimum_backoff: min,
        maximum_backoff: max,
    })
}

/// `grpcPushConfig`.
fn push_config_from_proto(c: Option<&pb::PushConfig>) -> Option<Value> {
    let c = c?;
    let mut map = Map::new();
    let endpoint = c.push_endpoint.trim();
    if !endpoint.is_empty() {
        map.insert("pushEndpoint".into(), Value::String(endpoint.to_string()));
    }
    if !c.attributes.is_empty() {
        let attrs: Map<String, Value> = c
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        map.insert("attributes".into(), Value::Object(attrs));
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// `protoPushConfig`.
fn push_config_to_proto(c: Option<&Value>) -> Option<pb::PushConfig> {
    let obj = c?.as_object()?;
    if obj.is_empty() {
        return None;
    }
    let endpoint = obj
        .get("pushEndpoint")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let attributes: std::collections::HashMap<String, String> = obj
        .get("attributes")
        .and_then(Value::as_object)
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    if endpoint.is_empty() && attributes.is_empty() {
        return None;
    }
    Some(pb::PushConfig {
        push_endpoint: endpoint,
        attributes,
        authentication_method: None,
        wrapper: None,
    })
}

// --- Snapshot / Schema / Message conversions -------------------------------

fn snapshot_to_proto(s: &SnapshotModel) -> pb::Snapshot {
    pb::Snapshot {
        name: s.name.clone(),
        topic: s.topic.clone(),
        expire_time: timestamp_to_proto(&s.expire_time),
        labels: to_proto_map(&s.labels),
    }
}

fn message_to_proto(m: &crate::model::PubsubMessage) -> pb::PubsubMessage {
    let data = crate::validation::decode_base64_bytes(&m.data).unwrap_or_default();
    pb::PubsubMessage {
        data,
        attributes: to_proto_map(&m.attributes),
        message_id: m.message_id.clone(),
        publish_time: timestamp_to_proto(&m.publish_time),
        ordering_key: m.ordering_key.clone(),
    }
}

fn schema_to_proto(s: &SchemaModel) -> pb::Schema {
    pb::Schema {
        name: s.name.clone(),
        r#type: schema_type_to_proto(&s.type_),
        definition: s.definition.clone(),
        revision_id: s.revision_id.clone(),
        revision_create_time: timestamp_to_proto(&s.revision_create_time),
    }
}

fn schema_from_proto(s: &pb::Schema) -> SchemaModel {
    SchemaModel {
        name: s.name.clone(),
        type_: schema_type_string(s.r#type),
        definition: s.definition.clone(),
        revision_id: s.revision_id.clone(),
        revision_create_time: String::new(),
        revisions: Vec::new(),
    }
}

fn schema_type_string(t: i32) -> String {
    match pb::schema::Type::try_from(t) {
        Ok(pb::schema::Type::ProtocolBuffer) => "PROTOCOL_BUFFER".to_string(),
        Ok(pb::schema::Type::Avro) => "AVRO".to_string(),
        Ok(pb::schema::Type::Unspecified) => String::new(),
        Err(_) => "INVALID".to_string(),
    }
}

fn schema_type_to_proto(t: &str) -> i32 {
    match t {
        "PROTOCOL_BUFFER" => pb::schema::Type::ProtocolBuffer as i32,
        "AVRO" => pb::schema::Type::Avro as i32,
        _ => pb::schema::Type::Unspecified as i32,
    }
}

fn schema_view_string(v: i32) -> String {
    match pb::SchemaView::try_from(v) {
        Ok(pb::SchemaView::Basic) => "BASIC".to_string(),
        Ok(pb::SchemaView::Full) => "FULL".to_string(),
        _ => String::new(),
    }
}

// --- Publisher impl --------------------------------------------------------

#[tonic::async_trait]
impl pb::publisher_server::Publisher for PubSubGrpc {
    async fn create_topic(
        &self,
        request: Request<pb::Topic>,
    ) -> Result<Response<pb::Topic>, Status> {
        let topic = topic_from_proto(&request.into_inner());
        let mut server = self.server.lock().unwrap();
        let created = server.grpc_create_topic(&topic).map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&created)))
    }

    async fn update_topic(
        &self,
        request: Request<pb::UpdateTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let req = request.into_inner();
        let Some(topic) = req.topic else {
            return Err(Status::invalid_argument("invalid topic name"));
        };
        let paths = req.update_mask.map(|m| m.paths).unwrap_or_default();
        let model = topic_from_proto(&topic);
        let mut server = self.server.lock().unwrap();
        let updated = server
            .grpc_update_topic(&model, &paths)
            .map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&updated)))
    }

    async fn get_topic(
        &self,
        request: Request<pb::GetTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let topic = request.into_inner().topic;
        let server = self.server.lock().unwrap();
        let found = server.grpc_get_topic(&topic).map_err(to_status)?;
        Ok(Response::new(topic_to_proto(&found)))
    }

    async fn list_topics(
        &self,
        request: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.project)?;
        let server = self.server.lock().unwrap();
        let (topics, next) = server
            .grpc_list_topics(&project, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicsResponse {
            topics: topics.iter().map(topic_to_proto).collect(),
            next_page_token: next,
        }))
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<pb::ListTopicSubscriptionsRequest>,
    ) -> Result<Response<pb::ListTopicSubscriptionsResponse>, Status> {
        let req = request.into_inner();
        let server = self.server.lock().unwrap();
        let (subs, next) = server
            .grpc_list_topic_subscriptions(&req.topic, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicSubscriptionsResponse {
            subscriptions: subs,
            next_page_token: next,
        }))
    }

    async fn list_topic_snapshots(
        &self,
        request: Request<pb::ListTopicSnapshotsRequest>,
    ) -> Result<Response<pb::ListTopicSnapshotsResponse>, Status> {
        let req = request.into_inner();
        let server = self.server.lock().unwrap();
        let (snaps, next) = server
            .grpc_list_topic_snapshots(&req.topic, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListTopicSnapshotsResponse {
            snapshots: snaps,
            next_page_token: next,
        }))
    }

    async fn publish(
        &self,
        request: Request<pb::PublishRequest>,
    ) -> Result<Response<pb::PublishResponse>, Status> {
        let req = request.into_inner();
        if req.messages.is_empty() {
            // Mirror legacy: empty messages → InvalidArgument (handled in business
            // method too, but the topic-name check must precede). Delegate fully.
        }
        let mut messages = Vec::with_capacity(req.messages.len());
        for m in &req.messages {
            let data = base64_std(&m.data);
            messages.push((data, copy_map(&m.attributes), m.ordering_key.clone()));
        }
        let mut server = self.server.lock().unwrap();
        let ids = server
            .grpc_publish(&req.topic, &messages)
            .map_err(to_status)?;
        Ok(Response::new(pb::PublishResponse { message_ids: ids }))
    }

    async fn delete_topic(
        &self,
        request: Request<pb::DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let topic = request.into_inner().topic;
        let mut server = self.server.lock().unwrap();
        server.grpc_delete_topic(&topic).map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn detach_subscription(
        &self,
        request: Request<pb::DetachSubscriptionRequest>,
    ) -> Result<Response<pb::DetachSubscriptionResponse>, Status> {
        let subscription = request.into_inner().subscription;
        let mut server = self.server.lock().unwrap();
        server
            .grpc_detach_subscription(&subscription)
            .map_err(to_status)?;
        Ok(Response::new(pb::DetachSubscriptionResponse {}))
    }
}

// --- Subscriber impl -------------------------------------------------------

#[tonic::async_trait]
impl pb::subscriber_server::Subscriber for PubSubGrpc {
    async fn create_subscription(
        &self,
        request: Request<pb::Subscription>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let model = sub_from_proto(&request.into_inner());
        let mut server = self.server.lock().unwrap();
        let created = server.grpc_create_subscription(model).map_err(to_status)?;
        Ok(Response::new(sub_to_proto(&created)))
    }

    async fn get_subscription(
        &self,
        request: Request<pb::GetSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let subscription = request.into_inner().subscription;
        let server = self.server.lock().unwrap();
        let found = server
            .grpc_get_subscription(&subscription)
            .map_err(to_status)?;
        Ok(Response::new(sub_to_proto(&found)))
    }

    async fn update_subscription(
        &self,
        request: Request<pb::UpdateSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        let req = request.into_inner();
        let Some(sub) = req.subscription else {
            return Err(Status::invalid_argument("invalid subscription name"));
        };
        let paths = req.update_mask.map(|m| m.paths).unwrap_or_default();
        let model = sub_from_proto(&sub);
        let mut server = self.server.lock().unwrap();
        let updated = server
            .grpc_update_subscription(&model, &paths)
            .map_err(to_status)?;
        Ok(Response::new(sub_to_proto(&updated)))
    }

    async fn list_subscriptions(
        &self,
        request: Request<pb::ListSubscriptionsRequest>,
    ) -> Result<Response<pb::ListSubscriptionsResponse>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.project)?;
        let server = self.server.lock().unwrap();
        let (subs, next) = server
            .grpc_list_subscriptions(&project, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSubscriptionsResponse {
            subscriptions: subs.iter().map(sub_to_proto).collect(),
            next_page_token: next,
        }))
    }

    async fn delete_subscription(
        &self,
        request: Request<pb::DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let subscription = request.into_inner().subscription;
        let mut server = self.server.lock().unwrap();
        server
            .grpc_delete_subscription(&subscription)
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<pb::ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let mut server = self.server.lock().unwrap();
        server
            .grpc_modify_ack_deadline(&req.subscription, &req.ack_ids, req.ack_deadline_seconds)
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn acknowledge(
        &self,
        request: Request<pb::AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let mut server = self.server.lock().unwrap();
        server
            .grpc_acknowledge(&req.subscription, &req.ack_ids)
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn pull(
        &self,
        request: Request<pb::PullRequest>,
    ) -> Result<Response<pb::PullResponse>, Status> {
        let req = request.into_inner();
        // Validate the subscription name up front (matches legacy ordering) and run
        // the bounded blocking wait when !return_immediately.
        if !crate::paths::valid_full_subscription_name(&req.subscription) {
            return Err(Status::invalid_argument("invalid subscription name"));
        }
        if !req.return_immediately {
            self.wait_for_pull(&req.subscription).await;
        }
        let mut server = self.server.lock().unwrap();
        let received = server
            .grpc_pull(&req.subscription, req.max_messages)
            .map_err(to_status)?;
        let messages = received
            .into_iter()
            .map(|(ack_id, message, attempt)| pb::ReceivedMessage {
                ack_id,
                message: Some(message_to_proto(&message)),
                delivery_attempt: attempt as i32,
            })
            .collect();
        Ok(Response::new(pb::PullResponse {
            received_messages: messages,
        }))
    }

    type StreamingPullStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = Result<pb::StreamingPullResponse, Status>,
                > + Send,
        >,
    >;

    async fn streaming_pull(
        &self,
        request: Request<tonic::Streaming<pb::StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        // Mirrors streaming_pull_grpc.rs:StreamingPull.
        {
            let server = self.server.lock().unwrap();
            if server.streaming_pull_disabled() {
                return Err(Status::unimplemented("streaming pull is disabled"));
            }
        }
        let mut incoming = request.into_inner();

        // First client message: EOF before any message ends the stream cleanly.
        let initial = match incoming.message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                return Ok(Response::new(Box::pin(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                )));
            }
            Err(status) => return Err(status),
        };
        if !crate::paths::valid_full_subscription_name(&initial.subscription) {
            return Err(Status::invalid_argument("invalid subscription name"));
        }
        let subscription = initial.subscription.clone();

        // Per-stream ack deadline + subscription validation (locked).
        let mut stream_ack_deadline = {
            let server = self.server.lock().unwrap();
            let deadline = server
                .grpc_stream_ack_deadline(initial.stream_ack_deadline_seconds)
                .map_err(to_status)?;
            server
                .grpc_validate_streaming_subscription(&subscription)
                .map_err(to_status)?;
            deadline
        };
        let max_outstanding_messages = initial.max_outstanding_messages;
        let max_outstanding_bytes = initial.max_outstanding_bytes;

        // Apply the initial request (acks/modacks carried on it), locked.
        {
            let mut outstanding: BTreeMap<String, i64> = BTreeMap::new();
            let mut server = self.server.lock().unwrap();
            apply_streaming_pull_request(
                &mut server,
                &subscription,
                &initial,
                true,
                &mut stream_ack_deadline,
                &mut outstanding,
            )
            .map_err(to_status)?;
            // `outstanding` from an initial request is always empty (acks only
            // remove entries), so it is rebuilt fresh in the driver task.
        }

        // Driver task: the same select loop as legacy (incoming requests vs a 10ms
        // delivery tick), streaming responses through an mpsc channel.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::StreamingPullResponse, Status>>(4);
        let server = self.server.clone();
        tokio::spawn(async move {
            let mut outstanding: BTreeMap<String, i64> = BTreeMap::new();
            let mut ticker = tokio::time::interval(StdDuration::from_millis(10));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // A unit of work emitted by a select arm; all `Server` lock access is
            // confined to the (synchronous) arm bodies — only sends `.await`,
            // never while holding the guard (the MutexGuard is not `Send`).
            loop {
                // `Some(Err)` → terminal error to send then stop; `Some(Ok(None))`
                // → stop quietly (EOF); `Some(Ok(Some(batch)))` → send batch;
                // `None` → nothing to send (no capacity / empty / applied ok).
                #[allow(clippy::type_complexity)]
                let action: Option<
                    Result<Option<pb::StreamingPullResponse>, Status>,
                > = tokio::select! {
                    msg = incoming.message() => {
                        match msg {
                            Ok(Some(request)) => {
                                let mut s = server.lock().unwrap();
                                match apply_streaming_pull_request(
                                    &mut s,
                                    &subscription,
                                    &request,
                                    false,
                                    &mut stream_ack_deadline,
                                    &mut outstanding,
                                ) {
                                    Ok(()) => None,
                                    Err(err) => Some(Err(to_status(err))),
                                }
                            }
                            Ok(None) => Some(Ok(None)),     // client CloseSend / EOF.
                            Err(status) => Some(Err(status)), // client cancel / transport error.
                        }
                    }
                    _ = ticker.tick() => {
                        let mut s = server.lock().unwrap();
                        s.grpc_prune_outstanding(&subscription, &mut outstanding);
                        if !streaming_has_capacity(&outstanding, max_outstanding_messages, max_outstanding_bytes) {
                            None
                        } else {
                            let remaining = streaming_remaining_capacity(&outstanding, max_outstanding_messages);
                            if remaining <= 0 {
                                None
                            } else {
                                let outstanding_bytes = streaming_outstanding_bytes(&outstanding);
                                match s.grpc_streaming_response(
                                    &subscription,
                                    remaining,
                                    stream_ack_deadline,
                                    max_outstanding_bytes,
                                    outstanding_bytes,
                                ) {
                                    Ok(received) if received.is_empty() => None,
                                    Ok(received) => {
                                        let mut messages = Vec::with_capacity(received.len());
                                        for (ack_id, message, attempt) in received {
                                            let size = streaming_received_size(&ack_id, &message);
                                            outstanding.insert(ack_id.clone(), size);
                                            messages.push(pb::ReceivedMessage {
                                                ack_id,
                                                message: Some(message_to_proto(&message)),
                                                delivery_attempt: attempt as i32,
                                            });
                                        }
                                        Some(Ok(Some(pb::StreamingPullResponse {
                                            received_messages: messages,
                                            acknowledge_confirmation: None,
                                            modify_ack_deadline_confirmation: None,
                                            subscription_properties: None,
                                        })))
                                    }
                                    Err(err) => Some(Err(to_status(err))),
                                }
                            }
                        }
                    }
                };
                match action {
                    None => continue,
                    Some(Ok(None)) => break,
                    Some(Ok(Some(batch))) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            break; // client dropped the stream.
                        }
                    }
                    Some(Err(status)) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
            // Teardown: release any still-outstanding unacked deliveries.
            {
                let mut s = server.lock().unwrap();
                s.grpc_release_outstanding(&subscription, &outstanding);
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn modify_push_config(
        &self,
        request: Request<pb::ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let push = push_config_from_proto(req.push_config.as_ref());
        let mut server = self.server.lock().unwrap();
        server
            .grpc_modify_push_config(&req.subscription, push.as_ref())
            .map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn get_snapshot(
        &self,
        request: Request<pb::GetSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let snapshot = request.into_inner().snapshot;
        let server = self.server.lock().unwrap();
        let found = server.grpc_get_snapshot(&snapshot).map_err(to_status)?;
        Ok(Response::new(snapshot_to_proto(&found)))
    }

    async fn list_snapshots(
        &self,
        request: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.project)?;
        let server = self.server.lock().unwrap();
        let (snaps, next) = server
            .grpc_list_snapshots(&project, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSnapshotsResponse {
            snapshots: snaps.iter().map(snapshot_to_proto).collect(),
            next_page_token: next,
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<pb::CreateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let req = request.into_inner();
        let labels = copy_map(&req.labels);
        let mut server = self.server.lock().unwrap();
        let created = server
            .grpc_create_snapshot(&req.name, &req.subscription, &labels)
            .map_err(to_status)?;
        Ok(Response::new(snapshot_to_proto(&created)))
    }

    async fn update_snapshot(
        &self,
        request: Request<pb::UpdateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        let req = request.into_inner();
        let Some(snapshot) = req.snapshot else {
            return Err(Status::invalid_argument("invalid snapshot name"));
        };
        let paths = req.update_mask.map(|m| m.paths).unwrap_or_default();
        let model = SnapshotModel {
            name: snapshot.name.clone(),
            topic: snapshot.topic.clone(),
            subscription: String::new(),
            expire_time: snapshot
                .expire_time
                .as_ref()
                .map(timestamp_to_rfc3339)
                .unwrap_or_default(),
            labels: copy_map(&snapshot.labels),
            deliveries: Vec::new(),
        };
        let mut server = self.server.lock().unwrap();
        let updated = server
            .grpc_update_snapshot(&model, &paths)
            .map_err(to_status)?;
        Ok(Response::new(snapshot_to_proto(&updated)))
    }

    async fn delete_snapshot(
        &self,
        request: Request<pb::DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        let snapshot = request.into_inner().snapshot;
        let mut server = self.server.lock().unwrap();
        server.grpc_delete_snapshot(&snapshot).map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn seek(
        &self,
        request: Request<pb::SeekRequest>,
    ) -> Result<Response<pb::SeekResponse>, Status> {
        let req = request.into_inner();
        // Validate name + target presence first (matches legacy ordering).
        if !crate::paths::valid_full_subscription_name(&req.subscription) {
            return Err(Status::invalid_argument("invalid subscription name"));
        }
        let Some(target) = req.target else {
            return Err(Status::invalid_argument("snapshot or time is required"));
        };
        let (snapshot, time_secs) = match target {
            pb::seek_request::Target::Time(ts) => {
                if ts.seconds == 0 && ts.nanos == 0 {
                    // proto3 default timestamp is the unix epoch, still a valid
                    // seek time in legacy (timestamp.IsValid()); seek by epoch.
                }
                (None, Some(ts.seconds))
            }
            pb::seek_request::Target::Snapshot(name) => (Some(name), None),
        };
        let mut server = self.server.lock().unwrap();
        server
            .grpc_seek(&req.subscription, snapshot.as_deref(), time_secs)
            .map_err(to_status)?;
        Ok(Response::new(pb::SeekResponse {}))
    }
}

impl PubSubGrpc {
    /// Bounded blocking wait mirroring `waitForPullAvailability` (1s default
    /// timeout, 10ms poll). The legacy server uses `config.PullWaitTimeout`; the Rust
    /// REST config does not carry it, so the parity-default 1s is used.
    async fn wait_for_pull(&self, subscription: &str) {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(1);
        loop {
            {
                let mut server = self.server.lock().unwrap();
                if server.grpc_pull_may_return(subscription) {
                    return;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }
}

// --- SchemaService impl ----------------------------------------------------

#[tonic::async_trait]
impl pb::schema_service_server::SchemaService for PubSubGrpc {
    async fn create_schema(
        &self,
        request: Request<pb::CreateSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.parent)?;
        if !crate::paths::valid_resource_id(&req.schema_id) {
            return Err(Status::invalid_argument("invalid schema name"));
        }
        let Some(schema) = req.schema else {
            return Err(Status::invalid_argument("schema is required"));
        };
        let model = schema_from_proto(&schema);
        let mut server = self.server.lock().unwrap();
        let created = server
            .grpc_create_schema(&project, &req.schema_id, &model)
            .map_err(to_status)?;
        Ok(Response::new(schema_to_proto(&created)))
    }

    async fn get_schema(
        &self,
        request: Request<pb::GetSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        let req = request.into_inner();
        let view = schema_view_string(req.view);
        let server = self.server.lock().unwrap();
        let found = server
            .grpc_get_schema(&req.name, &view)
            .map_err(to_status)?;
        Ok(Response::new(schema_to_proto(&found)))
    }

    async fn list_schemas(
        &self,
        request: Request<pb::ListSchemasRequest>,
    ) -> Result<Response<pb::ListSchemasResponse>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.parent)?;
        let view = schema_view_string(req.view);
        let server = self.server.lock().unwrap();
        let (schemas, next) = server
            .grpc_list_schemas(&project, &view, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSchemasResponse {
            schemas: schemas.iter().map(schema_to_proto).collect(),
            next_page_token: next,
        }))
    }

    async fn list_schema_revisions(
        &self,
        request: Request<pb::ListSchemaRevisionsRequest>,
    ) -> Result<Response<pb::ListSchemaRevisionsResponse>, Status> {
        let req = request.into_inner();
        let view = schema_view_string(req.view);
        let server = self.server.lock().unwrap();
        let (schemas, next) = server
            .grpc_list_schema_revisions(&req.name, &view, req.page_size, &req.page_token)
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSchemaRevisionsResponse {
            schemas: schemas.iter().map(schema_to_proto).collect(),
            next_page_token: next,
        }))
    }

    async fn commit_schema(
        &self,
        request: Request<pb::CommitSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        let req = request.into_inner();
        let Some(schema) = req.schema else {
            return Err(Status::invalid_argument("schema is required"));
        };
        let model = schema_from_proto(&schema);
        let mut server = self.server.lock().unwrap();
        let committed = server
            .grpc_commit_schema(&req.name, &model)
            .map_err(to_status)?;
        Ok(Response::new(schema_to_proto(&committed)))
    }

    async fn rollback_schema(
        &self,
        request: Request<pb::RollbackSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        let req = request.into_inner();
        let mut server = self.server.lock().unwrap();
        let rolled = server
            .grpc_rollback_schema(&req.name, &req.revision_id)
            .map_err(to_status)?;
        Ok(Response::new(schema_to_proto(&rolled)))
    }

    async fn delete_schema_revision(
        &self,
        request: Request<pb::DeleteSchemaRevisionRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        let req = request.into_inner();
        // `schemaRevisionRequestTarget`: a `name@revision` form overrides the
        // revision_id field.
        let (name, revision_id) = match req.name.split_once('@') {
            Some((n, r)) => (n.trim().to_string(), r.trim().to_string()),
            None => (
                req.name.trim().to_string(),
                req.revision_id.trim().to_string(),
            ),
        };
        if !crate::paths::valid_full_schema_name(&name) || revision_id.is_empty() {
            return Err(Status::invalid_argument("invalid schema revision name"));
        }
        let mut server = self.server.lock().unwrap();
        let updated = server
            .grpc_delete_schema_revision(&name, &revision_id)
            .map_err(to_status)?;
        Ok(Response::new(schema_to_proto(&updated)))
    }

    async fn delete_schema(
        &self,
        request: Request<pb::DeleteSchemaRequest>,
    ) -> Result<Response<()>, Status> {
        let name = request.into_inner().name;
        let mut server = self.server.lock().unwrap();
        server.grpc_delete_schema(&name).map_err(to_status)?;
        Ok(Response::new(()))
    }

    async fn validate_schema(
        &self,
        request: Request<pb::ValidateSchemaRequest>,
    ) -> Result<Response<pb::ValidateSchemaResponse>, Status> {
        let req = request.into_inner();
        if grpc_project_id(&req.parent).is_err() {
            return Err(Status::invalid_argument("invalid project name"));
        }
        let Some(schema) = req.schema else {
            return Err(Status::invalid_argument("schema is required"));
        };
        let model = schema_from_proto(&schema);
        let server = self.server.lock().unwrap();
        server.grpc_validate_schema(&model).map_err(to_status)?;
        Ok(Response::new(pb::ValidateSchemaResponse {}))
    }

    async fn validate_message(
        &self,
        request: Request<pb::ValidateMessageRequest>,
    ) -> Result<Response<pb::ValidateMessageResponse>, Status> {
        let req = request.into_inner();
        let project = grpc_project_id(&req.parent)?;
        let (name, inline) = match req.schema_spec {
            Some(pb::validate_message_request::SchemaSpec::Name(n)) => (n, None),
            Some(pb::validate_message_request::SchemaSpec::Schema(s)) => {
                (String::new(), Some(schema_from_proto(&s)))
            }
            None => (String::new(), None),
        };
        let encoding = encoding_string(req.encoding);
        let server = self.server.lock().unwrap();
        server
            .grpc_validate_message(&project, &name, inline.as_ref(), &req.message, &encoding)
            .map_err(to_status)?;
        Ok(Response::new(pb::ValidateMessageResponse {}))
    }
}

// --- StreamingPull helpers (mirror streaming_pull_grpc.rs) -----------------

/// `applyStreamingPullRequest` — validate + apply one client message's
/// flow-control update, acks, and modacks. `outstanding` is the ackID→size map
/// the driver maintains; acked/zero-modacked ids are removed from it.
fn apply_streaming_pull_request(
    server: &mut Server,
    subscription: &str,
    request: &pb::StreamingPullRequest,
    initial: bool,
    stream_ack_deadline: &mut i64,
    outstanding: &mut BTreeMap<String, i64>,
) -> Result<(), ApiError> {
    if !initial && !request.subscription.is_empty() && request.subscription != subscription {
        return Err(ApiError::invalid_argument(
            "subscription must not change on a streaming pull stream",
        ));
    }
    if !initial && request.max_outstanding_messages != 0 {
        return Err(ApiError::invalid_argument(
            "maxOutstandingMessages can only be set on the initial request",
        ));
    }
    if !initial && request.max_outstanding_bytes != 0 {
        return Err(ApiError::invalid_argument(
            "maxOutstandingBytes can only be set on the initial request",
        ));
    }
    if request.stream_ack_deadline_seconds != 0 {
        *stream_ack_deadline =
            server.grpc_stream_ack_deadline(request.stream_ack_deadline_seconds)?;
    }
    if request.modify_deadline_ack_ids.len() != request.modify_deadline_seconds.len() {
        return Err(ApiError::invalid_argument(
            "modifyDeadlineAckIds and modifyDeadlineSeconds must have the same length",
        ));
    }
    if !request.ack_ids.is_empty() {
        server.grpc_stream_update_ack_deadline(subscription, &request.ack_ids, 0, true)?;
        for ack_id in &request.ack_ids {
            outstanding.remove(ack_id);
        }
    }
    for (i, ack_id) in request.modify_deadline_ack_ids.iter().enumerate() {
        let deadline = request.modify_deadline_seconds[i];
        if deadline < 0 {
            return Err(ApiError::invalid_argument(
                "modifyDeadlineSeconds must be non-negative",
            ));
        }
        if deadline as i64 > server.max_ack_deadline_value() {
            return Err(ApiError::invalid_argument(
                "modifyDeadlineSeconds exceeds maxAckDeadlineSeconds",
            ));
        }
        server.grpc_stream_update_ack_deadline(
            subscription,
            std::slice::from_ref(ack_id),
            deadline as i64,
            false,
        )?;
        if deadline == 0 {
            outstanding.remove(ack_id);
        }
    }
    Ok(())
}

/// `streamingPullHasCapacity`.
fn streaming_has_capacity(
    outstanding: &BTreeMap<String, i64>,
    max_outstanding_messages: i64,
    max_outstanding_bytes: i64,
) -> bool {
    if max_outstanding_messages > 0 && outstanding.len() as i64 >= max_outstanding_messages {
        return false;
    }
    if max_outstanding_bytes > 0
        && streaming_outstanding_bytes(outstanding) >= max_outstanding_bytes
    {
        return false;
    }
    true
}

/// `streamingPullRemainingCapacity`.
fn streaming_remaining_capacity(
    outstanding: &BTreeMap<String, i64>,
    max_outstanding_messages: i64,
) -> i64 {
    if max_outstanding_messages <= 0 {
        return 1;
    }
    let remaining = max_outstanding_messages - outstanding.len() as i64;
    if remaining < 1 {
        return 0;
    }
    remaining
}

/// `streamingOutstandingBytes`.
fn streaming_outstanding_bytes(outstanding: &BTreeMap<String, i64>) -> i64 {
    outstanding.values().sum()
}

/// `streamingReceivedMessageSize` — flow-control byte size of a received message
/// (decoded data + ack id + ordering key + attribute key/value lengths).
fn streaming_received_size(ack_id: &str, message: &crate::model::PubsubMessage) -> i64 {
    let data_len = crate::validation::decode_base64_bytes(&message.data)
        .map(|b| b.len())
        .unwrap_or(0);
    let mut size = data_len + ack_id.len() + message.ordering_key.len();
    for (k, v) in &message.attributes {
        size += k.len() + v.len();
    }
    size as i64
}

/// Base64 StdEncoding of raw bytes (matches legacy `base64.StdEncoding.EncodeToString`).
fn base64_std(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}
