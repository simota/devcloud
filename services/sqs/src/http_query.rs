//! Query-protocol (legacy form-encoded → XML) HTTP dispatch for SQS, mirroring
//! the Query paths of `routes.rs` + the per-operation handlers + `responses.rs`.
//!
//! `dispatch_query(method, path, raw_query, form_body)` is the directly-testable
//! core: it takes the HTTP method, the request path, the URL query string, and
//! (for POST) the `application/x-www-form-urlencoded` body, and returns the HTTP
//! status + the rendered XML — exactly what `writeQueryXML` / `writeQueryError`
//! would emit. It calls the same business methods on `Server` that the JSON path
//! uses; only input parsing (flat form keys) and output rendering (XML) differ.

use std::collections::BTreeMap;

use crate::errors::error_code;
use crate::messages::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchEntry, DeleteMessageBatchEntry,
    ReceiveMessageRequest, ReceivedMessage, SendMessageBatchEntry, SendMessageRequest,
};
use crate::model::MoveTaskState;
use crate::{MessageAttributeValue, Server};

const XMLNS: &str = "http://queue.amazonaws.com/doc/2012-11-05/";
const XML_HEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const REQUEST_ID: &str = "devcloud-sqs";
pub const SQS_API_VERSION: &str = "2012-11-05";

/// A rendered Query outcome: HTTP status + the XML body (header + document, no
/// trailing newline, matching legacy `writeQueryXML`).
pub struct QueryOutcome {
    pub status: u16,
    pub body: String,
}

impl QueryOutcome {
    fn xml(status: u16, body: String) -> Self {
        QueryOutcome { status, body }
    }

    /// Builds the `<ErrorResponse>` envelope, matching legacy `writeQueryError`.
    /// `Type` is always `Sender`, `RequestId` is the fixed devcloud id.
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        let mut x = Xml::new();
        x.open("ErrorResponse");
        x.open("Error");
        x.leaf("Type", "Sender");
        x.leaf("Code", code);
        x.leaf("Message", message);
        x.close("Error");
        x.leaf("RequestId", REQUEST_ID);
        x.close("ErrorResponse");
        QueryOutcome::xml(status, x.finish())
    }
}

impl Server {
    /// Dispatch a Query-protocol request. Mirrors the gating + dispatch flow of
    /// `routes.rs` `handle` + `detectOperation` for the non-JSON path:
    /// path validation, `Version` validation, action dispatch.
    ///
    /// - `method`: "GET" or "POST".
    /// - `path`: request URL path (used to infer the queue URL like legacy does).
    /// - `raw_query`: the URL query string (without the leading `?`).
    /// - `form_body`: the POST body for `application/x-www-form-urlencoded`
    ///   requests (empty for GET).
    pub fn dispatch_query(
        &mut self,
        method: &str,
        path: &str,
        raw_query: &str,
        form_body: &str,
    ) -> QueryOutcome {
        let path = if path.is_empty() { "/" } else { path };
        // Path gate, mirroring routes.rs: a non-root path must carry an account
        // segment so `queueNameFromURL` resolves a name.
        if !is_root_path(path) && queue_name_from_path(path).is_empty() {
            return QueryOutcome::error(404, "InvalidAddress", "SQS endpoint path is invalid");
        }

        // legacy ParseForm merges URL query and (for form bodies) the POST body.
        let mut form = FormValues::parse(raw_query);
        if method == "POST" {
            form.extend(FormValues::parse(form_body));
        }

        // Version validation, mirroring validateQueryAPIVersion. GET reads the
        // query; POST reads the merged form. Both resolve via the merged map.
        if let Err((code, message)) = validate_query_api_version(form.get("Version")) {
            return QueryOutcome::error(400, code, &message);
        }

        let operation = form.get("Action");
        let ctx = QueryCtx { path, form: &form };
        match operation {
            "ListQueues" => self.query_list_queues(&ctx),
            "CreateQueue" => self.query_create_queue(&ctx),
            "GetQueueUrl" => self.query_get_queue_url(&ctx),
            "GetQueueAttributes" => self.query_get_queue_attributes(&ctx),
            "SetQueueAttributes" => self.query_set_queue_attributes(&ctx),
            "DeleteQueue" => self.query_delete_queue(&ctx),
            "PurgeQueue" => self.query_purge_queue(&ctx),
            "TagQueue" => self.query_tag_queue(&ctx),
            "UntagQueue" => self.query_untag_queue(&ctx),
            "ListQueueTags" => self.query_list_queue_tags(&ctx),
            "ListDeadLetterSourceQueues" => self.query_list_dlq_sources(&ctx),
            "StartMessageMoveTask" => self.query_start_move_task(&ctx),
            "ListMessageMoveTasks" => self.query_list_move_tasks(&ctx),
            "CancelMessageMoveTask" => self.query_cancel_move_task(&ctx),
            "AddPermission" => self.query_add_permission(&ctx),
            "RemovePermission" => self.query_remove_permission(&ctx),
            "SendMessage" => self.query_send_message(&ctx),
            "SendMessageBatch" => self.query_send_message_batch(&ctx),
            "ReceiveMessage" => self.query_receive_message(&ctx),
            "DeleteMessage" => self.query_delete_message(&ctx),
            "DeleteMessageBatch" => self.query_delete_message_batch(&ctx),
            "ChangeMessageVisibility" => self.query_change_visibility(&ctx),
            "ChangeMessageVisibilityBatch" => self.query_change_visibility_batch(&ctx),
            _ => QueryOutcome::error(400, "InvalidAction", "operation is not implemented"),
        }
    }

    // --- queue operations ---

    fn query_list_queues(&self, ctx: &QueryCtx) -> QueryOutcome {
        let prefix = ctx.form.get("QueueNamePrefix");
        let urls = self.list_queue_urls(prefix);
        let mut x = response_open("ListQueuesResponse");
        x.open("ListQueuesResult");
        for url in &urls {
            x.leaf("QueueUrl", url);
        }
        x.close("ListQueuesResult");
        response_close(&mut x, "ListQueuesResponse");
        QueryOutcome::xml(200, x.finish())
    }

    fn query_create_queue(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let name = ctx.form.get("QueueName").to_string();
        let attrs = ctx.form.attributes();
        let tags = ctx.form.tags();
        match self.create_queue(&name, &attrs, &tags) {
            Ok(q) => single_result(
                "CreateQueueResponse",
                "CreateQueueResult",
                "QueueUrl",
                &q.url,
            ),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_get_queue_url(&self, ctx: &QueryCtx) -> QueryOutcome {
        let name = ctx.form.get("QueueName");
        match self.queue_by_name(name) {
            Some(q) => single_result(
                "GetQueueUrlResponse",
                "GetQueueUrlResult",
                "QueueUrl",
                &q.url,
            ),
            None => QueryOutcome::error(400, "QueueDoesNotExist", "queue does not exist"),
        }
    }

    fn query_get_queue_attributes(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let names = ctx.form.list_values("AttributeName");
        match self.get_queue_attributes(&url, &names) {
            Ok(attrs) => {
                let mut x = response_open("GetQueueAttributesResponse");
                x.open("GetQueueAttributesResult");
                write_attributes(&mut x, &attrs);
                x.close("GetQueueAttributesResult");
                response_close(&mut x, "GetQueueAttributesResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_set_queue_attributes(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let attrs = ctx.form.attributes();
        match self.update_queue_attributes(&url, &attrs) {
            Ok(_) => empty_success("SetQueueAttributes"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_delete_queue(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        if self.delete_queue(&url) {
            empty_success("DeleteQueue")
        } else {
            QueryOutcome::error(400, "QueueDoesNotExist", "queue does not exist")
        }
    }

    fn query_purge_queue(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        match self.purge_queue(&url) {
            Ok(()) => empty_success("PurgeQueue"),
            Err(e) => QueryOutcome::error(400, "QueueDoesNotExist", &e),
        }
    }

    fn query_tag_queue(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let tags = ctx.form.tags();
        match self.tag_queue(&url, &tags) {
            Ok(()) => empty_success("TagQueue"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_untag_queue(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let keys = ctx.form.list_values("TagKey");
        match self.untag_queue(&url, &keys) {
            Ok(()) => empty_success("UntagQueue"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_list_queue_tags(&self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        match self.list_queue_tags(&url) {
            Ok(tags) => {
                let mut x = response_open("ListQueueTagsResponse");
                x.open("ListQueueTagsResult");
                for (k, v) in &tags {
                    x.open("Tag");
                    x.leaf("Key", k);
                    x.leaf("Value", v);
                    x.close("Tag");
                }
                x.close("ListQueueTagsResult");
                response_close(&mut x, "ListQueueTagsResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_list_dlq_sources(&self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        match self.list_dead_letter_source_queue_urls(&url) {
            Ok(urls) => {
                let mut x = response_open("ListDeadLetterSourceQueuesResponse");
                x.open("ListDeadLetterSourceQueuesResult");
                for u in &urls {
                    x.leaf("QueueUrl", u);
                }
                x.close("ListDeadLetterSourceQueuesResult");
                response_close(&mut x, "ListDeadLetterSourceQueuesResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    // --- move tasks ---

    fn query_start_move_task(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let source = ctx.form.get("SourceArn");
        let dest = ctx.form.get("DestinationArn");
        match self.start_message_move_task(source, dest) {
            Ok(task) => single_result(
                "StartMessageMoveTaskResponse",
                "StartMessageMoveTaskResult",
                "TaskHandle",
                &task.task_handle,
            ),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_list_move_tasks(&self, ctx: &QueryCtx) -> QueryOutcome {
        let source = ctx.form.get("SourceArn");
        let max = ctx.form.int("MaxResults", 0);
        match self.list_message_move_tasks(source, max) {
            Ok(tasks) => {
                let mut x = response_open("ListMessageMoveTasksResponse");
                x.open("ListMessageMoveTasksResult");
                for task in &tasks {
                    write_move_task(&mut x, task);
                }
                x.close("ListMessageMoveTasksResult");
                response_close(&mut x, "ListMessageMoveTasksResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_cancel_move_task(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let handle = ctx.form.get("TaskHandle");
        match self.cancel_message_move_task(handle) {
            Ok(moved) => single_result(
                "CancelMessageMoveTaskResponse",
                "CancelMessageMoveTaskResult",
                "ApproximateNumberOfMessagesMoved",
                &moved.to_string(),
            ),
            Err(e) => mapped_error(&e),
        }
    }

    // --- permissions ---

    fn query_add_permission(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let label = ctx.form.get("Label");
        let accounts = ctx.form.list_values("AWSAccountId");
        let actions = ctx.form.list_values("ActionName");
        match self.add_permission(&url, label, &accounts, &actions) {
            Ok(()) => empty_success("AddPermission"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_remove_permission(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let label = ctx.form.get("Label");
        match self.remove_permission(&url, label) {
            Ok(()) => empty_success("RemovePermission"),
            Err(e) => mapped_error(&e),
        }
    }

    // --- messages ---

    fn query_send_message(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let input = SendMessageRequest {
            queue_url: ctx.queue_url(),
            message_body: ctx.form.get("MessageBody").to_string(),
            delay_seconds: ctx.form.optional_int("DelaySeconds"),
            message_attributes: ctx.form.message_attributes("MessageAttribute"),
            message_system_attributes: ctx.form.message_attributes("MessageSystemAttribute"),
            message_group_id: ctx.form.get("MessageGroupId").to_string(),
            message_deduplication_id: ctx.form.get("MessageDeduplicationId").to_string(),
        };
        match self.send_message(&input) {
            Ok(m) => {
                let mut x = response_open("SendMessageResponse");
                x.open("SendMessageResult");
                x.leaf("MessageId", &m.id);
                x.leaf("MD5OfMessageBody", &m.body_md5);
                let attr_md5 = crate::md5_of_message_attributes(&m.attributes);
                if !attr_md5.is_empty() {
                    x.leaf("MD5OfMessageAttributes", &attr_md5);
                }
                let sys_md5 = crate::md5_of_message_attributes(&m.system_attributes);
                if !sys_md5.is_empty() {
                    x.leaf("MD5OfMessageSystemAttributes", &sys_md5);
                }
                if !m.sequence_number.is_empty() {
                    x.leaf("SequenceNumber", &m.sequence_number);
                }
                x.close("SendMessageResult");
                response_close(&mut x, "SendMessageResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_send_message_batch(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let mut entries: Vec<SendMessageBatchEntry> = Vec::new();
        let mut i = 1;
        loop {
            let prefix = format!("SendMessageBatchRequestEntry.{i}.");
            let id = ctx.form.get(&format!("{prefix}Id"));
            if id.is_empty() {
                break;
            }
            entries.push(SendMessageBatchEntry {
                id: id.to_string(),
                message_body: ctx.form.get(&format!("{prefix}MessageBody")).to_string(),
                delay_seconds: ctx.form.optional_int(&format!("{prefix}DelaySeconds")),
                message_attributes: ctx
                    .form
                    .message_attributes(&format!("{prefix}MessageAttribute")),
                message_system_attributes: ctx
                    .form
                    .message_attributes(&format!("{prefix}MessageSystemAttribute")),
                message_group_id: ctx.form.get(&format!("{prefix}MessageGroupId")).to_string(),
                message_deduplication_id: ctx
                    .form
                    .get(&format!("{prefix}MessageDeduplicationId"))
                    .to_string(),
            });
            i += 1;
        }
        match self.send_message_batch(&url, &entries) {
            Ok(result) => {
                let mut x = response_open("SendMessageBatchResponse");
                x.open("SendMessageBatchResult");
                for e in &result.successful {
                    x.open("SendMessageBatchResultEntry");
                    x.leaf("Id", &e.id);
                    x.leaf("MessageId", &e.message_id);
                    x.leaf("MD5OfMessageBody", &e.md5_of_message_body);
                    if !e.md5_of_message_attributes.is_empty() {
                        x.leaf("MD5OfMessageAttributes", &e.md5_of_message_attributes);
                    }
                    if !e.md5_of_message_system_attributes.is_empty() {
                        x.leaf(
                            "MD5OfMessageSystemAttributes",
                            &e.md5_of_message_system_attributes,
                        );
                    }
                    if !e.sequence_number.is_empty() {
                        x.leaf("SequenceNumber", &e.sequence_number);
                    }
                    x.close("SendMessageBatchResultEntry");
                }
                write_batch_errors(&mut x, &result.failed);
                x.close("SendMessageBatchResult");
                response_close(&mut x, "SendMessageBatchResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_receive_message(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let input = ReceiveMessageRequest {
            queue_url: ctx.queue_url(),
            max_number_of_messages: ctx.form.optional_int("MaxNumberOfMessages"),
            visibility_timeout: ctx.form.optional_int("VisibilityTimeout"),
            wait_time_seconds: ctx.form.optional_int("WaitTimeSeconds"),
            attribute_names: ctx.form.list_values("AttributeName"),
            message_attribute_names: ctx.form.list_values("MessageAttributeName"),
            message_system_attribute_names: ctx.form.list_values("MessageSystemAttributeName"),
        };
        match self.receive_messages(&input) {
            Ok(messages) => {
                let mut x = response_open("ReceiveMessageResponse");
                x.open("ReceiveMessageResult");
                for m in &messages {
                    write_received_message(&mut x, m);
                }
                x.close("ReceiveMessageResult");
                response_close(&mut x, "ReceiveMessageResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_delete_message(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let handle = ctx.form.get("ReceiptHandle");
        match self.delete_message(&url, handle) {
            Ok(()) => empty_success("DeleteMessage"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_delete_message_batch(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let mut entries: Vec<DeleteMessageBatchEntry> = Vec::new();
        let mut i = 1;
        loop {
            let prefix = format!("DeleteMessageBatchRequestEntry.{i}.");
            let id = ctx.form.get(&format!("{prefix}Id"));
            if id.is_empty() {
                break;
            }
            entries.push(DeleteMessageBatchEntry {
                id: id.to_string(),
                receipt_handle: ctx.form.get(&format!("{prefix}ReceiptHandle")).to_string(),
            });
            i += 1;
        }
        match self.delete_message_batch(&url, &entries) {
            Ok(result) => {
                let mut x = response_open("DeleteMessageBatchResponse");
                x.open("DeleteMessageBatchResult");
                for e in &result.successful {
                    x.open("DeleteMessageBatchResultEntry");
                    x.leaf("Id", &e.id);
                    x.close("DeleteMessageBatchResultEntry");
                }
                write_batch_errors(&mut x, &result.failed);
                x.close("DeleteMessageBatchResult");
                response_close(&mut x, "DeleteMessageBatchResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }

    fn query_change_visibility(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let handle = ctx.form.get("ReceiptHandle");
        let vis = ctx.form.int("VisibilityTimeout", 0);
        match self.change_message_visibility(&url, handle, vis) {
            Ok(()) => empty_success("ChangeMessageVisibility"),
            Err(e) => mapped_error(&e),
        }
    }

    fn query_change_visibility_batch(&mut self, ctx: &QueryCtx) -> QueryOutcome {
        let url = ctx.queue_url();
        let mut entries: Vec<ChangeMessageVisibilityBatchEntry> = Vec::new();
        let mut i = 1;
        loop {
            let prefix = format!("ChangeMessageVisibilityBatchRequestEntry.{i}.");
            let id = ctx.form.get(&format!("{prefix}Id"));
            if id.is_empty() {
                break;
            }
            entries.push(ChangeMessageVisibilityBatchEntry {
                id: id.to_string(),
                receipt_handle: ctx.form.get(&format!("{prefix}ReceiptHandle")).to_string(),
                visibility_timeout: ctx.form.int(&format!("{prefix}VisibilityTimeout"), 0),
            });
            i += 1;
        }
        match self.change_message_visibility_batch(&url, &entries) {
            Ok(result) => {
                let mut x = response_open("ChangeMessageVisibilityBatchResponse");
                x.open("ChangeMessageVisibilityBatchResult");
                for e in &result.successful {
                    x.open("ChangeMessageVisibilityBatchResultEntry");
                    x.leaf("Id", &e.id);
                    x.close("ChangeMessageVisibilityBatchResultEntry");
                }
                write_batch_errors(&mut x, &result.failed);
                x.close("ChangeMessageVisibilityBatchResult");
                response_close(&mut x, "ChangeMessageVisibilityBatchResponse");
                QueryOutcome::xml(200, x.finish())
            }
            Err(e) => mapped_error(&e),
        }
    }
}

/// Maps a logic error to its AWS code (via the responses.rs substring rules) at
/// HTTP 400, rendered as an XML error envelope — mirrors the per-operation
/// `writeProtocolError(errorCode(err), …)` on the Query path.
fn mapped_error(err: &str) -> QueryOutcome {
    QueryOutcome::error(400, &error_code(err), err)
}

/// Carries the parsed form + request path through the per-operation handlers,
/// mirroring how legacy threads `*http.Request` + `protocol` into each handler.
struct QueryCtx<'a> {
    path: &'a str,
    form: &'a FormValues,
}

impl QueryCtx<'_> {
    /// Mirrors `requestQueueURL`: prefer an explicit `QueueUrl` form field,
    /// otherwise infer from the request path (requires an account segment).
    fn queue_url(&self) -> String {
        let explicit = self.form.get("QueueUrl");
        if !explicit.is_empty() {
            return explicit.to_string();
        }
        if is_root_path(self.path) || queue_name_from_path(self.path).is_empty() {
            return String::new();
        }
        self.path.to_string()
    }
}

// --- XML rendering helpers (matching legacy encoding/xml byte output) ---

/// Hand-rolled XML document writer matching legacy `encoding/xml`'s element output:
/// no whitespace between tags, `<X></X>` for empty elements, and the same
/// character escaping (`<`,`>`,`&`,`"`,`'`,`\t`,`\n`,`\r`).
struct Xml {
    buf: String,
}

impl Xml {
    fn new() -> Self {
        let mut buf = String::new();
        buf.push_str(XML_HEADER);
        Xml { buf }
    }

    fn open(&mut self, name: &str) {
        self.buf.push('<');
        self.buf.push_str(name);
        self.buf.push('>');
    }

    /// Opens the document root with the SQS `xmlns` attribute.
    fn open_root(&mut self, name: &str) {
        self.buf.push('<');
        self.buf.push_str(name);
        self.buf.push_str(" xmlns=\"");
        self.buf.push_str(XMLNS);
        self.buf.push_str("\">");
    }

    fn close(&mut self, name: &str) {
        self.buf.push_str("</");
        self.buf.push_str(name);
        self.buf.push('>');
    }

    /// Writes `<name>escaped-value</name>`.
    fn leaf(&mut self, name: &str, value: &str) {
        self.open(name);
        escape_xml_into(&mut self.buf, value);
        self.close(name);
    }

    fn finish(self) -> String {
        self.buf
    }
}

/// Escapes text content exactly as legacy `encoding/xml.EscapeText` does for the
/// characters it special-cases (the rest pass through as UTF-8).
fn escape_xml_into(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(ch),
        }
    }
}

/// Opens an XML document at the given response root (header + `<Root xmlns=…>`).
fn response_open(root: &str) -> Xml {
    let mut x = Xml::new();
    x.open_root(root);
    x
}

/// Appends `<ResponseMetadata><RequestId>…</RequestId></ResponseMetadata>` and
/// closes the response root, mirroring the trailing `responseMetadataXML`.
fn response_close(x: &mut Xml, root: &str) {
    x.open("ResponseMetadata");
    x.leaf("RequestId", REQUEST_ID);
    x.close("ResponseMetadata");
    x.close(root);
}

/// Renders a response carrying a single scalar inside its `*Result` wrapper
/// (CreateQueue, GetQueueUrl, StartMessageMoveTask, CancelMessageMoveTask).
fn single_result(root: &str, result: &str, field: &str, value: &str) -> QueryOutcome {
    let mut x = response_open(root);
    x.open(result);
    x.leaf(field, value);
    x.close(result);
    response_close(&mut x, root);
    QueryOutcome::xml(200, x.finish())
}

/// Renders an empty success body (`<OpResponse><ResponseMetadata>…`), mirroring
/// `writeEmptySuccess` for the Query protocol.
fn empty_success(operation: &str) -> QueryOutcome {
    let root = format!("{operation}Response");
    let mut x = response_open(&root);
    response_close(&mut x, &root);
    QueryOutcome::xml(200, x.finish())
}

/// Writes `<Attribute><Name/><Value/></Attribute>` per entry (sorted by name,
/// which `BTreeMap` iteration already provides — matching `attributeXMLList`).
fn write_attributes(x: &mut Xml, attrs: &BTreeMap<String, String>) {
    for (name, value) in attrs {
        x.open("Attribute");
        x.leaf("Name", name);
        x.leaf("Value", value);
        x.close("Attribute");
    }
}

fn write_batch_errors(x: &mut Xml, failed: &[BatchResultErrorEntry]) {
    for e in failed {
        x.open("BatchResultErrorEntry");
        x.leaf("Id", &e.id);
        x.leaf("SenderFault", if e.sender_fault { "true" } else { "false" });
        x.leaf("Code", &e.code);
        x.leaf("Message", &e.message);
        x.close("BatchResultErrorEntry");
    }
}

fn write_move_task(x: &mut Xml, task: &MoveTaskState) {
    x.open("Result");
    x.leaf("TaskHandle", &task.task_handle);
    x.leaf("Status", &task.status);
    x.leaf("SourceArn", &task.source_arn);
    if !task.destination_arn.is_empty() {
        x.leaf("DestinationArn", &task.destination_arn);
    }
    x.leaf(
        "ApproximateNumberOfMessagesMoved",
        &task.approximate_number_of_messages_moved.to_string(),
    );
    x.close("Result");
}

/// Renders a `<Message>` element, mirroring `receivedMessageXML` field order +
/// omitempty rules.
fn write_received_message(x: &mut Xml, m: &ReceivedMessage) {
    x.open("Message");
    x.leaf("MessageId", &m.message_id);
    x.leaf("ReceiptHandle", &m.receipt_handle);
    x.leaf("MD5OfMessageBody", &m.md5_of_message_body);
    if !m.md5_of_message_attributes.is_empty() {
        x.leaf("MD5OfMessageAttributes", &m.md5_of_message_attributes);
    }
    if !m.md5_of_message_system_attributes.is_empty() {
        x.leaf(
            "MD5OfMessageSystemAttributes",
            &m.md5_of_message_system_attributes,
        );
    }
    x.leaf("Body", &m.body);
    for (name, value) in &m.attributes {
        x.open("Attribute");
        x.leaf("Name", name);
        x.leaf("Value", value);
        x.close("Attribute");
    }
    for (name, value) in &m.message_attributes {
        x.open("MessageAttribute");
        x.leaf("Name", name);
        write_message_attribute_value(x, value);
        x.close("MessageAttribute");
    }
    x.close("Message");
}

/// Renders the nested `<Value>` of a message attribute, mirroring
/// `messageAttributeValueXML` (DataType always; String/Binary omitempty).
fn write_message_attribute_value(x: &mut Xml, value: &MessageAttributeValue) {
    x.open("Value");
    x.leaf("DataType", &value.data_type);
    if !value.string_value.is_empty() {
        x.leaf("StringValue", &value.string_value);
    }
    if !value.binary_value.is_empty() {
        x.leaf("BinaryValue", &value.binary_value);
    }
    x.close("Value");
}

// --- gating helpers (mirror routes.rs) ---

fn is_root_path(path: &str) -> bool {
    path.is_empty() || path == "/"
}

/// Mirrors `queueNameFromURL` applied to a bare request path (no scheme): the
/// last path segment when there are at least two segments.
fn queue_name_from_path(path: &str) -> String {
    crate::server::queue_name_from_url(path)
}

/// Mirrors `validateQueryAPIVersion`: returns the error `(code, message)` for a
/// missing or unsupported `Version`.
fn validate_query_api_version(version: &str) -> Result<(), (&'static str, String)> {
    if version.is_empty() {
        return Err((
            "MissingParameter",
            "Version is required for SQS Query protocol".to_string(),
        ));
    }
    if version != SQS_API_VERSION {
        return Err((
            "InvalidParameterValue",
            format!("Version must be {SQS_API_VERSION}"),
        ));
    }
    Ok(())
}

// --- flat form parsing (mirror request_parsing.rs) ---

/// A parsed form, mirroring legacy `url.Values` (multimap with first-value
/// semantics for `.Get`). Keys preserve insertion of all values for the
/// list-collection cases (`queryListValues`).
struct FormValues {
    map: BTreeMap<String, Vec<String>>,
}

impl FormValues {
    /// Parses an `application/x-www-form-urlencoded` string (`a=1&b=2`), URL-
    /// decoding keys and values (`+` → space, `%XX` → byte).
    fn parse(raw: &str) -> Self {
        let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for pair in raw.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = match pair.split_once('=') {
                Some((k, v)) => (form_decode(k), form_decode(v)),
                None => (form_decode(pair), String::new()),
            };
            map.entry(k).or_default().push(v);
        }
        FormValues { map }
    }

    /// Merges another form's values (legacy ParseForm appends body values to the
    /// query values under the same key).
    fn extend(&mut self, other: FormValues) {
        for (k, mut values) in other.map {
            self.map.entry(k).or_default().append(&mut values);
        }
    }

    /// First value for `key`, or `""` (mirrors `url.Values.Get`).
    fn get(&self, key: &str) -> &str {
        self.map
            .get(key)
            .and_then(|v| v.first())
            .map(String::as_str)
            .unwrap_or("")
    }

    fn int(&self, key: &str, fallback: i64) -> i64 {
        let raw = self.get(key);
        if raw.is_empty() {
            return fallback;
        }
        raw.parse().unwrap_or(fallback)
    }

    /// Optional integer (mirrors `optionalFormInt`): absent/empty → None,
    /// unparseable → Some(0) (legacy `formInt` fallback after the presence check).
    fn optional_int(&self, key: &str) -> Option<i64> {
        if self.get(key).is_empty() {
            None
        } else {
            Some(self.int(key, 0))
        }
    }

    /// Mirrors `queryAttributes`: `Attribute.<Name>` flat keys (no further dot)
    /// plus indexed `Attribute.<i>.Name`/`.Value` pairs.
    fn attributes(&self) -> BTreeMap<String, String> {
        let mut attrs = BTreeMap::new();
        for (key, values) in &self.map {
            if let Some(name) = key.strip_prefix("Attribute.") {
                if !name.contains('.') && !values.is_empty() {
                    attrs.insert(name.to_string(), values[0].clone());
                }
            }
        }
        let mut i = 1;
        loop {
            let name = self.get(&format!("Attribute.{i}.Name"));
            if name.is_empty() {
                break;
            }
            let value = self.get(&format!("Attribute.{i}.Value")).to_string();
            attrs.insert(name.to_string(), value);
            i += 1;
        }
        attrs
    }

    /// Mirrors `queryTags`: indexed `Tag.<i>.Key`/`.Value` pairs.
    fn tags(&self) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();
        let mut i = 1;
        loop {
            let key = self.get(&format!("Tag.{i}.Key"));
            if key.is_empty() {
                break;
            }
            let value = self.get(&format!("Tag.{i}.Value")).to_string();
            tags.insert(key.to_string(), value);
            i += 1;
        }
        tags
    }

    /// Mirrors `queryListValues`: bare repeated `prefix`, then indexed
    /// `prefix.<i>`, then `prefix.member.<i>` — empty values are skipped.
    fn list_values(&self, prefix: &str) -> Vec<String> {
        let mut result = Vec::new();
        if let Some(values) = self.map.get(prefix) {
            for value in values {
                if !value.is_empty() {
                    result.push(value.clone());
                }
            }
        }
        let mut i = 1;
        loop {
            let value = self.get(&format!("{prefix}.{i}"));
            if value.is_empty() {
                break;
            }
            result.push(value.to_string());
            i += 1;
        }
        let mut i = 1;
        loop {
            let value = self.get(&format!("{prefix}.member.{i}"));
            if value.is_empty() {
                break;
            }
            result.push(value.to_string());
            i += 1;
        }
        result
    }

    /// Mirrors `queryMessageAttributesWithPrefix`: indexed
    /// `prefix.<i>.Name` + `.Value.DataType`/`.StringValue`/`.BinaryValue`.
    fn message_attributes(&self, prefix: &str) -> BTreeMap<String, MessageAttributeValue> {
        let mut attrs = BTreeMap::new();
        let mut i = 1;
        loop {
            let p = format!("{prefix}.{i}.");
            let name = self.get(&format!("{p}Name"));
            if name.is_empty() {
                break;
            }
            attrs.insert(
                name.to_string(),
                MessageAttributeValue {
                    data_type: self.get(&format!("{p}Value.DataType")).to_string(),
                    string_value: self.get(&format!("{p}Value.StringValue")).to_string(),
                    binary_value: self.get(&format!("{p}Value.BinaryValue")).to_string(),
                    string_list_values: Vec::new(),
                    binary_list_values: Vec::new(),
                },
            );
            i += 1;
        }
        attrs
    }
}

/// URL-decodes a form component: `+` → space, `%XX` → byte, invalid `%`
/// escapes pass through literally (lenient, matching no observable legacy test that
/// exercises malformed escapes).
fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1]);
                let lo = hex_val(bytes[i + 2]);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
