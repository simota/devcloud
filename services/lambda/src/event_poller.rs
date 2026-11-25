//! Background polling for event source mappings.
//!
//! Enabled mappings whose `EventSourceArn` is an SQS queue or a DynamoDB
//! stream are polled from the local SQS / DynamoDB services (over loopback,
//! with their AWS JSON protocol) and invoke their function synchronously with
//! a Lambda-shaped batch, like the Lambda pollers do:
//!
//! - **SQS**: up to `BatchSize` messages are received (10 per call). Records
//!   that `FilterCriteria` rejects are deleted without invoking. A successful
//!   invocation deletes the batch, except the `batchItemFailures` returned
//!   when `FunctionResponseTypes` has `ReportBatchItemFailures`; a failed one
//!   deletes nothing, so the messages reappear after the queue's visibility
//!   timeout (and its redrive policy applies).
//! - **DynamoDB Streams**: each shard is read from `StartingPosition`
//!   (`LATEST` from the first poll, otherwise `TRIM_HORIZON`) in batches of
//!   up to `BatchSize`. A failed batch is retried on the next poll until
//!   `MaximumRetryAttempts` (unset or -1: forever) is exceeded, then skipped;
//!   `batchItemFailures` resume from the first failed sequence number.
//!   Shard positions are kept in memory, so a restart reads from
//!   `StartingPosition` again.
//!
//! Kinesis, Kafka/MSK, and other sources have no local service and are not
//! polled. Payloads and credentials are never logged.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::aws_client::{call, AwsEndpoint};
use crate::event_filter;
use crate::event_source::EventSourceMappingRecord;
use crate::server::{Reply, Server};

/// Default time between polling rounds.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// SQS returns at most 10 messages per `ReceiveMessage`.
const SQS_RECEIVE_MAX: usize = 10;

/// Where event source mappings are polled from.
#[derive(Clone, Debug, Default)]
pub struct EventSources {
    /// The local SQS service; `None` leaves SQS mappings unpolled.
    pub sqs: Option<AwsEndpoint>,
    /// The local DynamoDB service (its Streams API); `None` leaves stream
    /// mappings unpolled.
    pub dynamodb: Option<AwsEndpoint>,
    /// Time between polling rounds; `None` means [`DEFAULT_POLL_INTERVAL`].
    pub poll_interval: Option<Duration>,
}

impl EventSources {
    fn any(&self) -> bool {
        self.sqs.is_some() || self.dynamodb.is_some()
    }
}

/// Per-shard read position of a stream mapping.
#[derive(Clone, Debug)]
struct StreamCursor {
    iterator: String,
    /// Failed attempts of the batch at `iterator`.
    attempts: i64,
}

/// In-memory polling state.
#[derive(Default)]
pub(crate) struct PollState {
    /// Serializes polling rounds (background loop and explicit calls).
    round: tokio::sync::Mutex<()>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// `<uuid>/<shard id>` → position.
    cursors: Mutex<HashMap<String, StreamCursor>>,
    /// `<uuid>` → `LastProcessingResult`.
    results: Mutex<HashMap<String, String>>,
}

enum Source<'a> {
    Sqs {
        region: &'a str,
        account: &'a str,
        queue: &'a str,
    },
    DynamoDbStream,
    Unsupported,
}

fn classify(arn: &str) -> Source<'_> {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" {
        return Source::Unsupported;
    }
    match parts[2] {
        "sqs" => Source::Sqs {
            region: parts[3],
            account: parts[4],
            queue: parts[5],
        },
        "dynamodb" if parts[5].starts_with("table/") && parts[5].contains("/stream/") => {
            Source::DynamoDbStream
        }
        _ => Source::Unsupported,
    }
}

/// Whether the invoke reply is a successful invocation (no function error).
fn invoke_succeeded(reply: &Reply) -> bool {
    reply.status == 200
        && !reply
            .headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Function-Error"))
}

/// `batchItemFailures[].itemIdentifier` of a successful response; `None`
/// when the response is malformed (Lambda then treats the whole batch as
/// failed).
fn batch_item_failures(body: &[u8], known: &HashSet<String>) -> Option<HashSet<String>> {
    if body.is_empty() {
        return Some(HashSet::new());
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    let Some(list) = v.get("batchItemFailures") else {
        return Some(HashSet::new());
    };
    if list.is_null() {
        return Some(HashSet::new());
    }
    let mut out = HashSet::new();
    for item in list.as_array()? {
        let id = item.get("itemIdentifier")?.as_str()?;
        if !known.contains(id) {
            return None;
        }
        out.insert(id.to_string());
    }
    Some(out)
}

/// The Lambda SQS event record for one `ReceiveMessage` message.
fn sqs_record(msg: &Value, arn: &str, region: &str) -> Value {
    let mut attributes = msg.get("Attributes").cloned().unwrap_or(json!({}));
    if !attributes.is_object() {
        attributes = json!({});
    }
    let mut message_attributes = serde_json::Map::new();
    if let Some(attrs) = msg.get("MessageAttributes").and_then(Value::as_object) {
        for (name, a) in attrs {
            let mut out = serde_json::Map::new();
            if let Some(s) = a.get("StringValue") {
                out.insert("stringValue".into(), s.clone());
            }
            if let Some(b) = a.get("BinaryValue") {
                out.insert("binaryValue".into(), b.clone());
            }
            out.insert(
                "stringListValues".into(),
                a.get("StringListValues").cloned().unwrap_or(json!([])),
            );
            out.insert(
                "binaryListValues".into(),
                a.get("BinaryListValues").cloned().unwrap_or(json!([])),
            );
            out.insert(
                "dataType".into(),
                a.get("DataType").cloned().unwrap_or(json!("String")),
            );
            message_attributes.insert(name.clone(), Value::Object(out));
        }
    }
    let mut rec = json!({
        "messageId": msg.get("MessageId").cloned().unwrap_or(Value::Null),
        "receiptHandle": msg.get("ReceiptHandle").cloned().unwrap_or(Value::Null),
        "body": msg.get("Body").cloned().unwrap_or(json!("")),
        "attributes": attributes,
        "messageAttributes": message_attributes,
        "md5OfBody": msg.get("MD5OfMessageBody").cloned().unwrap_or(Value::Null),
        "eventSource": "aws:sqs",
        "eventSourceARN": arn,
        "awsRegion": region,
    });
    if let Some(md5) = msg.get("MD5OfMessageAttributes") {
        rec["md5OfMessageAttributes"] = md5.clone();
    }
    rec
}

impl Server {
    /// Starts the background polling loop (once; no-op without a configured
    /// SQS or DynamoDB endpoint). [`Server::shutdown`] stops it.
    pub fn start_event_source_polling(self: &Arc<Self>) {
        if !self.config.event_sources.any() {
            return;
        }
        let mut task = self.poll.task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let interval = self
            .config
            .event_sources
            .poll_interval
            .unwrap_or(DEFAULT_POLL_INTERVAL);
        let server = Arc::downgrade(self);
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(server) = server.upgrade() else {
                    return;
                };
                server.poll_event_sources_once().await;
            }
        }));
    }

    /// Stops the polling loop; an invocation it was running is cancelled
    /// (its environment is stopped).
    pub(crate) async fn stop_event_source_polling(&self) {
        let task = self.poll.task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    /// Runs one polling round over every enabled mapping and returns the
    /// number of function invocations it made. The background loop calls
    /// this every poll interval; tests call it directly.
    pub async fn poll_event_sources_once(self: &Arc<Self>) -> usize {
        let _round = self.poll.round.lock().await;
        let mappings: Vec<EventSourceMappingRecord> = {
            let st = self.state.lock().unwrap();
            st.event_source_mappings.values().cloned().collect()
        };
        // Forget positions and results of deleted mappings.
        {
            let live: HashSet<&str> = mappings.iter().map(|m| m.uuid.as_str()).collect();
            self.poll
                .cursors
                .lock()
                .unwrap()
                .retain(|k, _| k.split('/').next().is_some_and(|u| live.contains(u)));
            self.poll
                .results
                .lock()
                .unwrap()
                .retain(|k, _| live.contains(k.as_str()));
        }
        let mut invocations = 0;
        for m in mappings.iter().filter(|m| m.enabled) {
            let outcome = match classify(&m.event_source_arn) {
                Source::Sqs {
                    region,
                    account,
                    queue,
                } => match self.config.event_sources.sqs.clone() {
                    Some(ep) => Some(self.poll_sqs(&ep, m, region, account, queue).await),
                    None => None,
                },
                Source::DynamoDbStream => match self.config.event_sources.dynamodb.clone() {
                    Some(ep) => Some(self.poll_stream(&ep, m).await),
                    None => None,
                },
                Source::Unsupported => None,
            };
            let Some(outcome) = outcome else {
                continue;
            };
            let result = match outcome {
                Ok((0, _)) => None,
                Ok((n, result)) => {
                    invocations += n;
                    Some(result)
                }
                Err(e) => Some(format!("PROBLEM: {e}")),
            };
            if let Some(result) = result {
                self.poll
                    .results
                    .lock()
                    .unwrap()
                    .insert(m.uuid.clone(), result);
            }
        }
        invocations
    }

    /// The mapping's `LastProcessingResult`, once it has been polled.
    pub(crate) fn last_processing_result(&self, uuid: &str) -> Option<String> {
        self.poll.results.lock().unwrap().get(uuid).cloned()
    }

    async fn invoke_batch(
        self: &Arc<Self>,
        m: &EventSourceMappingRecord,
        records: &[Value],
    ) -> Reply {
        let payload = serde_json::to_vec(&json!({ "Records": records })).unwrap_or_default();
        self.invoke(
            &m.function_arn,
            &BTreeMap::new(),
            "RequestResponse",
            "",
            &payload,
        )
        .await
    }

    fn reports_batch_item_failures(m: &EventSourceMappingRecord) -> bool {
        m.function_response_types
            .iter()
            .any(|t| t == "ReportBatchItemFailures")
    }

    /// One SQS batch: `(invocations, LastProcessingResult)`.
    async fn poll_sqs(
        self: &Arc<Self>,
        ep: &AwsEndpoint,
        m: &EventSourceMappingRecord,
        region: &str,
        account: &str,
        queue: &str,
    ) -> Result<(usize, String), String> {
        let url = call(
            ep,
            "sqs",
            "AmazonSQS.GetQueueUrl",
            &json!({ "QueueName": queue, "QueueOwnerAWSAccountId": account }),
        )
        .await?;
        let queue_url = url
            .get("QueueUrl")
            .and_then(Value::as_str)
            .ok_or("GetQueueUrl returned no QueueUrl")?
            .to_string();
        let batch_size = m.batch_size.max(1) as usize;
        let mut messages: Vec<Value> = Vec::new();
        while messages.len() < batch_size {
            let want = (batch_size - messages.len()).min(SQS_RECEIVE_MAX);
            let got = call(
                ep,
                "sqs",
                "AmazonSQS.ReceiveMessage",
                &json!({
                    "QueueUrl": queue_url,
                    "MaxNumberOfMessages": want,
                    "WaitTimeSeconds": 0,
                    "AttributeNames": ["All"],
                    "MessageSystemAttributeNames": ["All"],
                    "MessageAttributeNames": ["All"],
                }),
            )
            .await?;
            let batch = got
                .get("Messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if batch.is_empty() {
                break;
            }
            let short = batch.len() < want;
            messages.extend(batch);
            if short {
                break;
            }
        }
        if messages.is_empty() {
            return Ok((0, String::new()));
        }
        let arn = &m.event_source_arn;
        let patterns = event_filter::patterns(m.filter_criteria.as_ref());
        let (keep, dropped): (Vec<Value>, Vec<Value>) = messages
            .iter()
            .map(|msg| sqs_record(msg, arn, region))
            .partition(|r| event_filter::passes(&patterns, &event_filter::sqs_match_view(r)));
        // Filtered-out messages are consumed, as on AWS.
        self.delete_sqs(ep, &queue_url, &dropped).await?;
        if keep.is_empty() {
            return Ok((0, String::new()));
        }
        let reply = self.invoke_batch(m, &keep).await;
        if !invoke_succeeded(&reply) {
            return Ok((1, "PROBLEM: Function call failed".to_string()));
        }
        let failed = if Self::reports_batch_item_failures(m) {
            let ids: HashSet<String> = keep
                .iter()
                .filter_map(|r| r["messageId"].as_str().map(str::to_string))
                .collect();
            match batch_item_failures(&reply.body, &ids) {
                Some(f) => f,
                None => return Ok((1, "PROBLEM: Function call failed".to_string())),
            }
        } else {
            HashSet::new()
        };
        let done: Vec<Value> = keep
            .into_iter()
            .filter(|r| {
                !r["messageId"]
                    .as_str()
                    .is_some_and(|id| failed.contains(id))
            })
            .collect();
        self.delete_sqs(ep, &queue_url, &done).await?;
        Ok((1, "OK".to_string()))
    }

    async fn delete_sqs(
        &self,
        ep: &AwsEndpoint,
        queue_url: &str,
        records: &[Value],
    ) -> Result<(), String> {
        for chunk in records.chunks(SQS_RECEIVE_MAX) {
            let entries: Vec<Value> = chunk
                .iter()
                .enumerate()
                .map(|(i, r)| json!({ "Id": i.to_string(), "ReceiptHandle": r["receiptHandle"] }))
                .collect();
            call(
                ep,
                "sqs",
                "AmazonSQS.DeleteMessageBatch",
                &json!({ "QueueUrl": queue_url, "Entries": entries }),
            )
            .await?;
        }
        Ok(())
    }

    /// One batch per shard of a DynamoDB stream: `(invocations,
    /// LastProcessingResult)`.
    async fn poll_stream(
        self: &Arc<Self>,
        ep: &AwsEndpoint,
        m: &EventSourceMappingRecord,
    ) -> Result<(usize, String), String> {
        const TARGET: &str = "DynamoDB_20120810";
        let arn = &m.event_source_arn;
        let described = call(
            ep,
            "dynamodb",
            &format!("{TARGET}.DescribeStream"),
            &json!({ "StreamArn": arn }),
        )
        .await?;
        let shards: Vec<String> = described["StreamDescription"]["Shards"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s["ShardId"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let patterns = event_filter::patterns(m.filter_criteria.as_ref());
        let mut invocations = 0;
        let mut result = String::from("OK");
        for shard in shards {
            let cursor_key = format!("{}/{shard}", m.uuid);
            let cursor = self.poll.cursors.lock().unwrap().get(&cursor_key).cloned();
            let cursor = match cursor {
                Some(c) => c,
                None => {
                    let kind = match m.starting_position.as_deref() {
                        Some("LATEST") => "LATEST",
                        _ => "TRIM_HORIZON",
                    };
                    let it = call(
                        ep,
                        "dynamodb",
                        &format!("{TARGET}.GetShardIterator"),
                        &json!({ "StreamArn": arn, "ShardId": shard, "ShardIteratorType": kind }),
                    )
                    .await?;
                    StreamCursor {
                        iterator: it["ShardIterator"]
                            .as_str()
                            .ok_or("GetShardIterator returned no ShardIterator")?
                            .to_string(),
                        attempts: 0,
                    }
                }
            };
            let set = |c: StreamCursor| {
                self.poll
                    .cursors
                    .lock()
                    .unwrap()
                    .insert(cursor_key.clone(), c);
            };
            let got = call(
                ep,
                "dynamodb",
                &format!("{TARGET}.GetRecords"),
                &json!({ "ShardIterator": cursor.iterator, "Limit": m.batch_size.clamp(1, 1000) }),
            )
            .await?;
            let next = StreamCursor {
                iterator: got["NextShardIterator"]
                    .as_str()
                    .unwrap_or(&cursor.iterator)
                    .to_string(),
                attempts: 0,
            };
            let records: Vec<Value> = got["Records"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|mut r| {
                    r["eventSourceARN"] = json!(arn);
                    r
                })
                .filter(|r| event_filter::passes(&patterns, r))
                .collect();
            if records.is_empty() {
                set(next);
                continue;
            }
            invocations += 1;
            let reply = self.invoke_batch(m, &records).await;
            let failed_seq = if !invoke_succeeded(&reply) {
                Err(())
            } else if Self::reports_batch_item_failures(m) {
                let seqs: HashSet<String> = records
                    .iter()
                    .filter_map(|r| r["dynamodb"]["SequenceNumber"].as_str().map(str::to_string))
                    .collect();
                match batch_item_failures(&reply.body, &seqs) {
                    // Resume from the first failed record of the batch.
                    Some(f) => Ok(records
                        .iter()
                        .filter_map(|r| r["dynamodb"]["SequenceNumber"].as_str())
                        .find(|s| f.contains(*s))
                        .map(str::to_string)),
                    None => Err(()),
                }
            } else {
                Ok(None)
            };
            match failed_seq {
                Ok(None) => set(next),
                outcome => {
                    result = "PROBLEM: Function call failed".to_string();
                    let attempts = cursor.attempts + 1;
                    let exhausted = m
                        .maximum_retry_attempts
                        .is_some_and(|max| max >= 0 && attempts > max);
                    if exhausted {
                        set(next);
                        continue;
                    }
                    let retry_from = match outcome {
                        Ok(Some(seq)) => {
                            let it = call(
                                ep,
                                "dynamodb",
                                &format!("{TARGET}.GetShardIterator"),
                                &json!({
                                    "StreamArn": arn,
                                    "ShardId": shard,
                                    "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                                    "SequenceNumber": seq,
                                }),
                            )
                            .await?;
                            it["ShardIterator"]
                                .as_str()
                                .unwrap_or(&cursor.iterator)
                                .to_string()
                        }
                        _ => cursor.iterator.clone(),
                    };
                    set(StreamCursor {
                        iterator: retry_from,
                        attempts,
                    });
                }
            }
        }
        Ok((invocations, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_event_source_arns() {
        assert!(matches!(
            classify("arn:aws:sqs:us-east-1:000000000000:orders"),
            Source::Sqs {
                queue: "orders",
                account: "000000000000",
                ..
            }
        ));
        assert!(matches!(
            classify(
                "arn:aws:dynamodb:us-east-1:000000000000:table/T/stream/2024-01-01T00:00:00.000"
            ),
            Source::DynamoDbStream
        ));
        assert!(matches!(
            classify("arn:aws:dynamodb:us-east-1:000000000000:table/T"),
            Source::Unsupported
        ));
        assert!(matches!(
            classify("arn:aws:kinesis:us-east-1:000000000000:stream/s"),
            Source::Unsupported
        ));
        assert!(matches!(classify("not-an-arn"), Source::Unsupported));
    }

    #[test]
    fn parses_batch_item_failures() {
        let known: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        assert_eq!(batch_item_failures(b"", &known), Some(HashSet::new()));
        assert_eq!(batch_item_failures(b"null", &known), Some(HashSet::new()));
        assert_eq!(
            batch_item_failures(br#"{"batchItemFailures":[{"itemIdentifier":"b"}]}"#, &known),
            Some(["b".to_string()].into_iter().collect())
        );
        // Unknown or malformed identifiers fail the whole batch.
        assert_eq!(
            batch_item_failures(br#"{"batchItemFailures":[{"itemIdentifier":"z"}]}"#, &known),
            None
        );
        assert_eq!(
            batch_item_failures(br#"{"batchItemFailures":[{}]}"#, &known),
            None
        );
    }

    #[test]
    fn builds_lambda_sqs_records() {
        let msg = json!({
            "MessageId": "m1",
            "ReceiptHandle": "rh",
            "MD5OfMessageBody": "md5",
            "Body": "hi",
            "Attributes": { "ApproximateReceiveCount": "1" },
            "MessageAttributes": { "k": { "DataType": "String", "StringValue": "v" } },
        });
        let rec = sqs_record(&msg, "arn:aws:sqs:us-east-1:000000000000:q", "us-east-1");
        assert_eq!(rec["messageId"], "m1");
        assert_eq!(rec["body"], "hi");
        assert_eq!(rec["eventSource"], "aws:sqs");
        assert_eq!(rec["attributes"]["ApproximateReceiveCount"], "1");
        assert_eq!(rec["messageAttributes"]["k"]["stringValue"], "v");
        assert_eq!(rec["messageAttributes"]["k"]["dataType"], "String");
        assert_eq!(rec["messageAttributes"]["k"]["stringListValues"], json!([]));
    }
}
