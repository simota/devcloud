//! JSON-protocol (AWS JSON 1.0) HTTP dispatch for SQS, mirroring the JSON paths
//! of `routes.go` + the per-operation handlers.
//!
//! `dispatch_json(target, body)` is the directly-testable core: it takes an
//! `X-Amz-Target` operation name + a JSON request body and returns the HTTP
//! status, the `__type` error code (if any), and the JSON response value —
//! exactly what `writeJSON` / `writeJSONError` would emit. The Query/XML
//! protocol and the socket server land in a later part.

use serde_json::{json, Map, Value};

use crate::errors::error_code;
use crate::messages::{
    ChangeMessageVisibilityBatchEntry, DeleteMessageBatchEntry, ReceiveMessageRequest,
    ReceivedMessage, SendMessageBatchEntry, SendMessageRequest,
};
use crate::{MessageAttributeValue, Server};

const TARGET_PREFIX: &str = "AmazonSQS.";

/// A rendered JSON outcome: HTTP status, optional `__type` error code, body.
pub struct JsonOutcome {
    pub status: u16,
    pub error_type: Option<String>,
    pub body: Value,
}

impl JsonOutcome {
    fn ok(body: Value) -> Self {
        JsonOutcome {
            status: 200,
            error_type: None,
            body,
        }
    }
    fn empty() -> Self {
        JsonOutcome::ok(json!({}))
    }

    /// Builds a `{"__type": code, "message": msg}` error at `status`. Public so
    /// the socket layer can render gate errors (method/protocol) the same way.
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        JsonOutcome {
            status,
            error_type: Some(code.to_string()),
            body: json!({ "__type": code, "message": message }),
        }
    }
}

/// Builds a `{"__type": code, "message": msg}` error at `status`.
fn json_error(status: u16, code: &str, message: &str) -> JsonOutcome {
    JsonOutcome::error(status, code, message)
}

/// Maps a logic error to its AWS code (via the responses.go substring rules) at
/// HTTP 400, matching the per-operation `writeProtocolError(errorCode(err), …)`.
fn mapped_error(err: &str) -> JsonOutcome {
    json_error(400, &error_code(err), err)
}

impl Server {
    /// Dispatch a JSON-protocol request. `target` is the full `X-Amz-Target`
    /// header value (`AmazonSQS.<Op>`); `body` is the JSON request payload.
    pub fn dispatch_json(&mut self, target: &str, body: &[u8]) -> JsonOutcome {
        let operation = match target.strip_prefix(TARGET_PREFIX) {
            Some(op) => op,
            None => return json_error(400, "InvalidAction", "missing X-Amz-Target"),
        };
        let req: Value = if body.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(_) => return json_error(400, "InvalidParameterValue", "invalid json request"),
            }
        };

        match operation {
            "ListQueues" => self.json_list_queues(&req),
            "CreateQueue" => self.json_create_queue(&req),
            "GetQueueUrl" => self.json_get_queue_url(&req),
            "GetQueueAttributes" => self.json_get_queue_attributes(&req),
            "SetQueueAttributes" => self.json_set_queue_attributes(&req),
            "DeleteQueue" => self.json_delete_queue(&req),
            "PurgeQueue" => self.json_purge_queue(&req),
            "TagQueue" => self.json_tag_queue(&req),
            "UntagQueue" => self.json_untag_queue(&req),
            "ListQueueTags" => self.json_list_queue_tags(&req),
            "ListDeadLetterSourceQueues" => self.json_list_dlq_sources(&req),
            "StartMessageMoveTask" => self.json_start_move_task(&req),
            "ListMessageMoveTasks" => self.json_list_move_tasks(&req),
            "CancelMessageMoveTask" => self.json_cancel_move_task(&req),
            "AddPermission" => self.json_add_permission(&req),
            "RemovePermission" => self.json_remove_permission(&req),
            "SendMessage" => self.json_send_message(&req),
            "SendMessageBatch" => self.json_send_message_batch(&req),
            "ReceiveMessage" => self.json_receive_message(&req),
            "DeleteMessage" => self.json_delete_message(&req),
            "DeleteMessageBatch" => self.json_delete_message_batch(&req),
            "ChangeMessageVisibility" => self.json_change_visibility(&req),
            "ChangeMessageVisibilityBatch" => self.json_change_visibility_batch(&req),
            _ => json_error(400, "InvalidAction", "operation is not implemented"),
        }
    }

    // --- queue operations ---

    fn json_list_queues(&self, req: &Value) -> JsonOutcome {
        let prefix = str_field(req, "QueueNamePrefix");
        JsonOutcome::ok(json!({ "QueueUrls": self.list_queue_urls(&prefix) }))
    }

    fn json_create_queue(&mut self, req: &Value) -> JsonOutcome {
        let name = str_field(req, "QueueName");
        let attrs = string_map(req.get("Attributes"));
        // Go accepts both `Tags` and lowercase `tags`.
        let tags = if req.get("Tags").is_some() {
            string_map(req.get("Tags"))
        } else {
            string_map(req.get("tags"))
        };
        match self.create_queue(&name, &attrs, &tags) {
            Ok(q) => JsonOutcome::ok(json!({ "QueueUrl": q.url })),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_get_queue_url(&self, req: &Value) -> JsonOutcome {
        let name = str_field(req, "QueueName");
        match self.queue_by_name(&name) {
            Some(q) => JsonOutcome::ok(json!({ "QueueUrl": q.url })),
            None => json_error(400, "QueueDoesNotExist", "queue does not exist"),
        }
    }

    fn json_get_queue_attributes(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let names = string_array(req.get("AttributeNames"));
        match self.get_queue_attributes(&url, &names) {
            Ok(attrs) => JsonOutcome::ok(json!({ "Attributes": map_to_value(&attrs) })),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_set_queue_attributes(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let attrs = string_map(req.get("Attributes"));
        match self.update_queue_attributes(&url, &attrs) {
            Ok(_) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_delete_queue(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        if self.delete_queue(&url) {
            JsonOutcome::empty()
        } else {
            json_error(400, "QueueDoesNotExist", "queue does not exist")
        }
    }

    fn json_purge_queue(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        match self.purge_queue(&url) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => json_error(400, "QueueDoesNotExist", &e),
        }
    }

    fn json_tag_queue(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let tags = string_map(req.get("Tags"));
        match self.tag_queue(&url, &tags) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_untag_queue(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let keys = string_array(req.get("TagKeys"));
        match self.untag_queue(&url, &keys) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_list_queue_tags(&self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        match self.list_queue_tags(&url) {
            Ok(tags) => JsonOutcome::ok(json!({ "Tags": map_to_value(&tags) })),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_list_dlq_sources(&self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        match self.list_dead_letter_source_queue_urls(&url) {
            Ok(urls) => JsonOutcome::ok(json!({ "QueueUrls": urls })),
            Err(e) => mapped_error(&e),
        }
    }

    // --- move tasks ---

    fn json_start_move_task(&mut self, req: &Value) -> JsonOutcome {
        let source = str_field(req, "SourceArn");
        let dest = str_field(req, "DestinationArn");
        match self.start_message_move_task(&source, &dest) {
            Ok(task) => JsonOutcome::ok(json!({ "TaskHandle": task.task_handle })),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_list_move_tasks(&self, req: &Value) -> JsonOutcome {
        let source = str_field(req, "SourceArn");
        let max = int_field(req, "MaxResults").unwrap_or(0);
        match self.list_message_move_tasks(&source, max) {
            Ok(tasks) => {
                let results: Vec<Value> = tasks
                    .iter()
                    .map(|t| {
                        let mut m = Map::new();
                        m.insert("TaskHandle".into(), json!(t.task_handle));
                        m.insert("Status".into(), json!(t.status));
                        m.insert("SourceArn".into(), json!(t.source_arn));
                        if !t.destination_arn.is_empty() {
                            m.insert("DestinationArn".into(), json!(t.destination_arn));
                        }
                        m.insert(
                            "ApproximateNumberOfMessagesMoved".into(),
                            json!(t.approximate_number_of_messages_moved),
                        );
                        Value::Object(m)
                    })
                    .collect();
                JsonOutcome::ok(json!({ "Results": results }))
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn json_cancel_move_task(&mut self, req: &Value) -> JsonOutcome {
        let handle = str_field(req, "TaskHandle");
        match self.cancel_message_move_task(&handle) {
            Ok(moved) => JsonOutcome::ok(json!({ "ApproximateNumberOfMessagesMoved": moved })),
            Err(e) => mapped_error(&e),
        }
    }

    // --- permissions ---

    fn json_add_permission(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let label = str_field(req, "Label");
        let accounts = string_array(req.get("AWSAccountIds"));
        let actions = string_array(req.get("Actions"));
        match self.add_permission(&url, &label, &accounts, &actions) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_remove_permission(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let label = str_field(req, "Label");
        match self.remove_permission(&url, &label) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    // --- messages ---

    fn json_send_message(&mut self, req: &Value) -> JsonOutcome {
        let input = parse_send_message(req, &str_field(req, "QueueUrl"));
        match self.send_message(&input) {
            Ok(m) => {
                let mut out = Map::new();
                out.insert("MessageId".into(), json!(m.id));
                out.insert("MD5OfMessageBody".into(), json!(m.body_md5));
                if !m.sequence_number.is_empty() {
                    out.insert("SequenceNumber".into(), json!(m.sequence_number));
                }
                let attr_md5 = crate::md5_of_message_attributes(&m.attributes);
                if !attr_md5.is_empty() {
                    out.insert("MD5OfMessageAttributes".into(), json!(attr_md5));
                }
                let sys_md5 = crate::md5_of_message_attributes(&m.system_attributes);
                if !sys_md5.is_empty() {
                    out.insert("MD5OfMessageSystemAttributes".into(), json!(sys_md5));
                }
                JsonOutcome::ok(Value::Object(out))
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn json_send_message_batch(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let entries: Vec<SendMessageBatchEntry> = array(req.get("Entries"))
            .iter()
            .map(|e| {
                let mut s = parse_send_message(e, &url);
                SendMessageBatchEntry {
                    id: str_field(e, "Id"),
                    message_body: std::mem::take(&mut s.message_body),
                    delay_seconds: s.delay_seconds,
                    message_attributes: std::mem::take(&mut s.message_attributes),
                    message_system_attributes: std::mem::take(&mut s.message_system_attributes),
                    message_group_id: std::mem::take(&mut s.message_group_id),
                    message_deduplication_id: std::mem::take(&mut s.message_deduplication_id),
                }
            })
            .collect();
        match self.send_message_batch(&url, &entries) {
            Ok(result) => {
                let successful: Vec<Value> = result
                    .successful
                    .iter()
                    .map(|e| {
                        let mut m = Map::new();
                        m.insert("Id".into(), json!(e.id));
                        m.insert("MessageId".into(), json!(e.message_id));
                        m.insert("MD5OfMessageBody".into(), json!(e.md5_of_message_body));
                        if !e.md5_of_message_attributes.is_empty() {
                            m.insert(
                                "MD5OfMessageAttributes".into(),
                                json!(e.md5_of_message_attributes),
                            );
                        }
                        if !e.md5_of_message_system_attributes.is_empty() {
                            m.insert(
                                "MD5OfMessageSystemAttributes".into(),
                                json!(e.md5_of_message_system_attributes),
                            );
                        }
                        if !e.sequence_number.is_empty() {
                            m.insert("SequenceNumber".into(), json!(e.sequence_number));
                        }
                        Value::Object(m)
                    })
                    .collect();
                JsonOutcome::ok(json!({
                    "Successful": successful,
                    "Failed": failed_to_json(&result.failed),
                }))
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn json_receive_message(&mut self, req: &Value) -> JsonOutcome {
        let input = ReceiveMessageRequest {
            queue_url: str_field(req, "QueueUrl"),
            max_number_of_messages: int_field(req, "MaxNumberOfMessages"),
            visibility_timeout: int_field(req, "VisibilityTimeout"),
            wait_time_seconds: int_field(req, "WaitTimeSeconds"),
            attribute_names: string_array(req.get("AttributeNames")),
            message_attribute_names: string_array(req.get("MessageAttributeNames")),
            message_system_attribute_names: string_array(req.get("MessageSystemAttributeNames")),
        };
        match self.receive_messages(&input) {
            Ok(messages) => {
                let arr: Vec<Value> = messages.iter().map(received_message_to_json).collect();
                JsonOutcome::ok(json!({ "Messages": arr }))
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn json_delete_message(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let handle = str_field(req, "ReceiptHandle");
        match self.delete_message(&url, &handle) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_delete_message_batch(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let entries: Vec<DeleteMessageBatchEntry> = array(req.get("Entries"))
            .iter()
            .map(|e| DeleteMessageBatchEntry {
                id: str_field(e, "Id"),
                receipt_handle: str_field(e, "ReceiptHandle"),
            })
            .collect();
        match self.delete_message_batch(&url, &entries) {
            Ok(result) => JsonOutcome::ok(json!({
                "Successful": id_entries_to_json(&result.successful),
                "Failed": failed_to_json(&result.failed),
            })),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_change_visibility(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let handle = str_field(req, "ReceiptHandle");
        let vis = int_field(req, "VisibilityTimeout").unwrap_or(0);
        match self.change_message_visibility(&url, &handle, vis) {
            Ok(()) => JsonOutcome::empty(),
            Err(e) => mapped_error(&e),
        }
    }

    fn json_change_visibility_batch(&mut self, req: &Value) -> JsonOutcome {
        let url = str_field(req, "QueueUrl");
        let entries: Vec<ChangeMessageVisibilityBatchEntry> = array(req.get("Entries"))
            .iter()
            .map(|e| ChangeMessageVisibilityBatchEntry {
                id: str_field(e, "Id"),
                receipt_handle: str_field(e, "ReceiptHandle"),
                visibility_timeout: int_field(e, "VisibilityTimeout").unwrap_or(0),
            })
            .collect();
        match self.change_message_visibility_batch(&url, &entries) {
            Ok(result) => JsonOutcome::ok(json!({
                "Successful": id_entries_to_json(&result.successful),
                "Failed": failed_to_json(&result.failed),
            })),
            Err(e) => mapped_error(&e),
        }
    }
}

// --- JSON parsing helpers (Value → typed) ---

fn str_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Reads an optional integer (mirrors Go's `*int` request fields). Absent →
/// None; present numeric → Some.
fn int_field(v: &Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

fn array(v: Option<&Value>) -> Vec<&Value> {
    match v.and_then(|x| x.as_array()) {
        Some(a) => a.iter().collect(),
        None => Vec::new(),
    }
}

fn string_array(v: Option<&Value>) -> Vec<String> {
    match v.and_then(|x| x.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|x| x.as_str())
            .map(|s| s.to_string())
            .collect(),
        None => Vec::new(),
    }
}

fn string_map(v: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(Value::Object(m)) = v {
        for (k, val) in m {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn message_attributes(
    v: Option<&Value>,
) -> std::collections::BTreeMap<String, MessageAttributeValue> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(Value::Object(m)) = v {
        for (k, val) in m {
            out.insert(
                k.clone(),
                MessageAttributeValue {
                    data_type: str_field(val, "DataType"),
                    string_value: str_field(val, "StringValue"),
                    binary_value: str_field(val, "BinaryValue"),
                    string_list_values: string_array(val.get("StringListValues")),
                    binary_list_values: string_array(val.get("BinaryListValues")),
                },
            );
        }
    }
    out
}

fn parse_send_message(v: &Value, queue_url: &str) -> SendMessageRequest {
    SendMessageRequest {
        queue_url: queue_url.to_string(),
        message_body: str_field(v, "MessageBody"),
        delay_seconds: int_field(v, "DelaySeconds"),
        message_attributes: message_attributes(v.get("MessageAttributes")),
        message_system_attributes: message_attributes(v.get("MessageSystemAttributes")),
        message_group_id: str_field(v, "MessageGroupId"),
        message_deduplication_id: str_field(v, "MessageDeduplicationId"),
    }
}

// --- JSON encoding helpers (typed → Value, matching Go shapes) ---

fn map_to_value(m: &std::collections::BTreeMap<String, String>) -> Value {
    let mut obj = Map::new();
    for (k, v) in m {
        obj.insert(k.clone(), json!(v));
    }
    Value::Object(obj)
}

fn message_attributes_to_value(
    m: &std::collections::BTreeMap<String, MessageAttributeValue>,
) -> Value {
    let mut obj = Map::new();
    for (k, v) in m {
        let mut a = Map::new();
        a.insert("DataType".into(), json!(v.data_type));
        if !v.string_value.is_empty() {
            a.insert("StringValue".into(), json!(v.string_value));
        }
        if !v.binary_value.is_empty() {
            a.insert("BinaryValue".into(), json!(v.binary_value));
        }
        if !v.string_list_values.is_empty() {
            a.insert("StringListValues".into(), json!(v.string_list_values));
        }
        if !v.binary_list_values.is_empty() {
            a.insert("BinaryListValues".into(), json!(v.binary_list_values));
        }
        obj.insert(k.clone(), Value::Object(a));
    }
    Value::Object(obj)
}

/// Mirrors the JSON shape of Go `receivedMessage` (omitempty fields dropped).
fn received_message_to_json(m: &ReceivedMessage) -> Value {
    let mut out = Map::new();
    out.insert("MessageId".into(), json!(m.message_id));
    out.insert("ReceiptHandle".into(), json!(m.receipt_handle));
    out.insert("MD5OfMessageBody".into(), json!(m.md5_of_message_body));
    if !m.md5_of_message_attributes.is_empty() {
        out.insert(
            "MD5OfMessageAttributes".into(),
            json!(m.md5_of_message_attributes),
        );
    }
    if !m.md5_of_message_system_attributes.is_empty() {
        out.insert(
            "MD5OfMessageSystemAttributes".into(),
            json!(m.md5_of_message_system_attributes),
        );
    }
    out.insert("Body".into(), json!(m.body));
    if !m.attributes.is_empty() {
        out.insert("Attributes".into(), map_to_value(&m.attributes));
    }
    if !m.message_attributes.is_empty() {
        out.insert(
            "MessageAttributes".into(),
            message_attributes_to_value(&m.message_attributes),
        );
    }
    Value::Object(out)
}

fn id_entries_to_json(entries: &[crate::messages::IdResultEntry]) -> Value {
    Value::Array(entries.iter().map(|e| json!({ "Id": e.id })).collect())
}

fn failed_to_json(entries: &[crate::messages::BatchResultErrorEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|e| {
                json!({
                    "Id": e.id,
                    "SenderFault": e.sender_fault,
                    "Code": e.code,
                    "Message": e.message,
                })
            })
            .collect(),
    )
}
