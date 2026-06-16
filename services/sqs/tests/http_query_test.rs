//! Parity tests for the Query-protocol (form-encoded → XML) HTTP dispatch.
//! Drives the `dispatch_query` core and asserts XML shapes captured from the legacy
//! server (`writeQueryXML` / `writeQueryError`). Each test mirrors a legacy
//! `Query*` test in `internal/services/sqs/{server,queue,message}_test.rs`, plus
//! byte-exact golden assertions captured from the legacy reference.

use devcloud_sqs::http_query::QueryOutcome;
use devcloud_sqs::{Config, Server};

fn cfg() -> Config {
    Config {
        addr: "127.0.0.1:9324".to_string(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        queue_url_host: "127.0.0.1:9324".to_string(),
        ..Default::default()
    }
}

const URL: &str = "http://127.0.0.1:9324/000000000000/gold";

/// POST form-encoded request through the Query dispatch (path "/").
fn post(s: &mut Server, body: &str) -> QueryOutcome {
    s.dispatch_query("POST", "/", "", body)
}

/// POST form-encoded request at a specific path (queue-URL inference).
fn post_path(s: &mut Server, path: &str, body: &str) -> QueryOutcome {
    s.dispatch_query("POST", path, "", body)
}

/// URL-query-escape `:` and `/` the way the legacy tests' `urlQueryEscape` does.
fn esc(value: &str) -> String {
    value.replace(':', "%3A").replace('/', "%2F")
}

fn create_queue(s: &mut Server, name: &str) -> String {
    let out = post(
        s,
        &format!("Action=CreateQueue&Version=2012-11-05&QueueName={name}"),
    );
    assert_eq!(out.status, 200, "create {name}: {}", out.body);
    extract(&out.body, "<QueueUrl>", "</QueueUrl>")
}

fn extract(body: &str, open: &str, close: &str) -> String {
    let start = body.find(open).expect("open tag") + open.len();
    let end = body[start..].find(close).expect("close tag") + start;
    body[start..end].to_string()
}

// --- TestQueryListQueuesReturnsXMLResponse + golden ---

#[test]
fn query_list_queues_returns_xml() {
    let mut s = Server::new(cfg());
    let out = post(&mut s, "Action=ListQueues&Version=2012-11-05");
    assert_eq!(out.status, 200);
    assert_eq!(
        out.body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<ListQueuesResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\
<ListQueuesResult></ListQueuesResult>\
<ResponseMetadata><RequestId>devcloud-sqs</RequestId></ResponseMetadata>\
</ListQueuesResponse>"
    );
}

// --- TestQueryCreateQueueReturnsXMLResponse + golden ---

#[test]
fn query_create_queue_returns_xml() {
    let mut s = Server::new(cfg());
    let out = post(
        &mut s,
        "Action=CreateQueue&Version=2012-11-05&QueueName=gold",
    );
    assert_eq!(out.status, 200);
    assert_eq!(
        out.body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<CreateQueueResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\
<CreateQueueResult><QueueUrl>http://127.0.0.1:9324/000000000000/gold</QueueUrl></CreateQueueResult>\
<ResponseMetadata><RequestId>devcloud-sqs</RequestId></ResponseMetadata>\
</CreateQueueResponse>"
    );
    assert!(out.body.contains("<CreateQueueResponse"));
    assert!(out
        .body
        .contains("<QueueUrl>http://127.0.0.1:9324/000000000000/gold</QueueUrl>"));
}

// --- TestUnsupportedQueryActionReturnsXMLError + golden ---

#[test]
fn unsupported_query_action_returns_xml_error() {
    let mut s = Server::new(cfg());
    let out = post(&mut s, "Action=UnknownOperation&Version=2012-11-05");
    assert_eq!(out.status, 400);
    assert!(out.body.contains("<Code>InvalidAction</Code>"));
    assert_eq!(
        out.body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<ErrorResponse><Error><Type>Sender</Type><Code>InvalidAction</Code>\
<Message>operation is not implemented</Message></Error>\
<RequestId>devcloud-sqs</RequestId></ErrorResponse>"
    );
}

// --- TestQueryProtocolRequiresSupportedVersion ---

#[test]
fn query_protocol_requires_supported_version() {
    let mut s = Server::new(cfg());

    // missing Version → MissingParameter
    let out = post(&mut s, "Action=ListQueues");
    assert_eq!(out.status, 400);
    assert!(out.body.contains("<Code>MissingParameter</Code>"));
    assert!(out
        .body
        .contains("<Message>Version is required for SQS Query protocol</Message>"));

    // unsupported Version → InvalidParameterValue
    let out = post(&mut s, "Action=ListQueues&Version=2011-10-01");
    assert_eq!(out.status, 400);
    assert!(out.body.contains("<Code>InvalidParameterValue</Code>"));
    assert!(out
        .body
        .contains("<Message>Version must be 2012-11-05</Message>"));
}

// --- TestQueueURLRequiresAccountPathSegment (Query half) ---

#[test]
fn query_invalid_address_for_pathless_account_segment() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "path-shape");
    let out = post_path(
        &mut s,
        "/path-shape",
        "Action=GetQueueAttributes&Version=2012-11-05&AttributeName.1=All",
    );
    assert_eq!(out.status, 404);
    assert!(out.body.contains("<Code>InvalidAddress</Code>"));
    assert!(out
        .body
        .contains("<Message>SQS endpoint path is invalid</Message>"));
}

// --- TestQueryOperationsInferQueueURLFromRequestPath ---

#[test]
fn query_operations_infer_queue_url_from_request_path() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "path-demo");

    let send = post_path(
        &mut s,
        "/000000000000/path-demo",
        "Action=SendMessage&Version=2012-11-05&MessageBody=from-path",
    );
    assert_eq!(send.status, 200, "{}", send.body);
    assert!(send.body.contains("<SendMessageResponse"));

    let receive = post_path(
        &mut s,
        "/000000000000/path-demo",
        "Action=ReceiveMessage&Version=2012-11-05&MaxNumberOfMessages=1",
    );
    assert_eq!(receive.status, 200, "{}", receive.body);
    assert!(receive.body.contains("<Body>from-path</Body>"));

    let attrs = post_path(
        &mut s,
        "/000000000000/path-demo",
        "Action=GetQueueAttributes&Version=2012-11-05&AttributeName.1=All",
    );
    assert_eq!(attrs.status, 200, "{}", attrs.body);
    assert!(attrs.body.contains("<Name>QueueArn</Name>"));
}

// --- TestQueryGetQueueAttributesAcceptsUnindexedAttributeName ---

#[test]
fn query_get_queue_attributes_accepts_unindexed_attribute_name() {
    let mut s = Server::new(cfg());
    // CreateQueue (Query) with VisibilityTimeout via indexed Attribute keys.
    let out = post(
        &mut s,
        "Action=CreateQueue&Version=2012-11-05&QueueName=query-attribute-name\
&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=7",
    );
    assert_eq!(out.status, 200, "{}", out.body);
    let url = extract(&out.body, "<QueueUrl>", "</QueueUrl>");

    let attrs = post(
        &mut s,
        &format!(
            "Action=GetQueueAttributes&Version=2012-11-05&QueueUrl={}&AttributeName=All",
            esc(&url)
        ),
    );
    assert_eq!(attrs.status, 200, "{}", attrs.body);
    assert!(attrs.body.contains("<Name>VisibilityTimeout</Name>"));
    assert!(attrs.body.contains("<Value>7</Value>"));
}

// --- TestQuerySendMessageRejectsInvalidMessageAttributeName ---

#[test]
fn query_send_message_rejects_invalid_message_attribute_name() {
    let mut s = Server::new(cfg());
    let url = create_queue(&mut s, "query-bad-message-attr-names");
    let out = post(
        &mut s,
        &format!(
            "Action=SendMessage&Version=2012-11-05&QueueUrl={}&MessageBody=query-attrs\
&MessageAttribute.1.Name=AWS.Trace&MessageAttribute.1.Value.DataType=String\
&MessageAttribute.1.Value.StringValue=x",
            esc(&url)
        ),
    );
    assert_eq!(out.status, 400, "{}", out.body);
    assert!(out.body.contains("<Code>InvalidAttributeName</Code>"));
}

// --- TestQueryMessageSystemAttributesReturnXMLMD5 ---

#[test]
fn query_message_system_attributes_return_xml_md5() {
    let mut s = Server::new(cfg());
    let url = create_queue(&mut s, "system-query");
    let trace = "Root=1-abcdef12-345678912345678912345678";

    let send = post(
        &mut s,
        &format!(
            "Action=SendMessage&Version=2012-11-05&QueueUrl={}&MessageBody=query-system\
&MessageSystemAttribute.1.Name=AWSTraceHeader&MessageSystemAttribute.1.Value.DataType=String\
&MessageSystemAttribute.1.Value.StringValue={}",
            esc(&url),
            esc(trace)
        ),
    );
    assert_eq!(send.status, 200, "{}", send.body);
    assert!(send.body.contains("<MD5OfMessageSystemAttributes>"));

    let receive = post(
        &mut s,
        &format!(
            "Action=ReceiveMessage&Version=2012-11-05&QueueUrl={}\
&MessageSystemAttributeName.1=AWSTraceHeader",
            esc(&url)
        ),
    );
    assert_eq!(receive.status, 200, "{}", receive.body);
    assert!(receive.body.contains("<Name>AWSTraceHeader</Name>"));
    assert!(receive.body.contains(trace));
    assert!(receive.body.contains("<MD5OfMessageSystemAttributes>"));
}

// --- TestQueryReceiveMessageReturnsNestedMessageAttributeXML ---

#[test]
fn query_receive_message_returns_nested_message_attribute_xml() {
    let mut s = Server::new(cfg());
    let url = create_queue(&mut s, "query-message-attributes");

    let send = post(
        &mut s,
        &format!(
            "Action=SendMessage&Version=2012-11-05&QueueUrl={}&MessageBody=query-attrs\
&MessageAttribute.1.Name=kind&MessageAttribute.1.Value.DataType=String\
&MessageAttribute.1.Value.StringValue=loop",
            esc(&url)
        ),
    );
    assert_eq!(send.status, 200, "{}", send.body);

    let receive = post(
        &mut s,
        &format!(
            "Action=ReceiveMessage&Version=2012-11-05&QueueUrl={}&MessageAttributeName=All",
            esc(&url)
        ),
    );
    assert_eq!(receive.status, 200, "{}", receive.body);
    for want in [
        "<MessageAttribute>",
        "<Name>kind</Name>",
        "<Value><DataType>String</DataType><StringValue>loop</StringValue></Value>",
        "<MD5OfMessageAttributes>",
    ] {
        assert!(
            receive.body.contains(want),
            "missing {want}: {}",
            receive.body
        );
    }
}

// --- TestQueryReceiveMessageAcceptsReceiveRequestAttemptID ---
// (legacy asserts the parsed request field; the Rust receive request has no such
//  field — legacy threads ReceiveRequestAttemptId only as a no-op passthrough, so
//  the observable Query behavior is simply that the request succeeds.)

#[test]
fn query_receive_message_accepts_receive_request_attempt_id() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "attempt");
    let out = post(
        &mut s,
        "Action=ReceiveMessage&Version=2012-11-05\
&QueueUrl=http%3A%2F%2F127.0.0.1%3A9324%2F000000000000%2Fattempt&ReceiveRequestAttemptId=retry-1",
    );
    assert_eq!(out.status, 200, "{}", out.body);
    assert!(out.body.contains("<ReceiveMessageResponse"));
}

// --- GET method parity (detectOperation GET branch) ---

#[test]
fn get_method_query_dispatch() {
    let mut s = Server::new(cfg());
    let out = s.dispatch_query("GET", "/", "Action=ListQueues&Version=2012-11-05", "");
    assert_eq!(out.status, 200);
    assert!(out.body.contains("<ListQueuesResponse"));
}

// --- empty-success envelope (DeleteQueue) byte-exact + missing-queue error ---

#[test]
fn query_delete_queue_empty_success_and_missing_error() {
    let mut s = Server::new(cfg());
    let url = create_queue(&mut s, "gold");
    assert_eq!(url, URL);
    let del = post(
        &mut s,
        &format!(
            "Action=DeleteQueue&Version=2012-11-05&QueueUrl={}",
            esc(&url)
        ),
    );
    assert_eq!(del.status, 200);
    assert_eq!(
        del.body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<DeleteQueueResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\
<ResponseMetadata><RequestId>devcloud-sqs</RequestId></ResponseMetadata>\
</DeleteQueueResponse>"
    );

    // Deleting again → QueueDoesNotExist error envelope.
    let missing = post(
        &mut s,
        &format!(
            "Action=DeleteQueue&Version=2012-11-05&QueueUrl={}",
            esc(&url)
        ),
    );
    assert_eq!(missing.status, 400);
    assert_eq!(
        missing.body,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<ErrorResponse><Error><Type>Sender</Type><Code>QueueDoesNotExist</Code>\
<Message>queue does not exist</Message></Error>\
<RequestId>devcloud-sqs</RequestId></ErrorResponse>"
    );
}

// --- batch send (Query) reachability + XML shape ---

#[test]
fn query_send_message_batch_xml() {
    let mut s = Server::new(cfg());
    let url = create_queue(&mut s, "batch-q");
    let out = post(
        &mut s,
        &format!(
            "Action=SendMessageBatch&Version=2012-11-05&QueueUrl={}\
&SendMessageBatchRequestEntry.1.Id=a&SendMessageBatchRequestEntry.1.MessageBody=one\
&SendMessageBatchRequestEntry.2.Id=b&SendMessageBatchRequestEntry.2.MessageBody=two",
            esc(&url)
        ),
    );
    assert_eq!(out.status, 200, "{}", out.body);
    assert!(out.body.contains("<SendMessageBatchResultEntry><Id>a</Id>"));
    assert!(out.body.contains("<Id>b</Id>"));
    assert!(out.body.contains("<MD5OfMessageBody>"));
}
