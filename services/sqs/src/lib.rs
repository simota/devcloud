//! Rust reimplementation of the devcloud SQS service (strangler-fig increment
//! #3 of the legacy-to-Rust transmute).
//!
//! Lands incrementally: hashing + validation (part 1), model + persistence
//! (part 2), queue lifecycle/attributes/tags/policy (part 3), and the message
//! lifecycle — send/receive/delete/visibility + batches + FIFO dedup + DLQ
//! redrive + retention cleanup (part 4). The dual-protocol HTTP layer, SigV4,
//! and the daemon seam follow.

pub mod errors;
pub mod hashing;
pub mod http;
pub mod http_json;
pub mod http_query;
pub mod introspect;
pub mod messages;
pub mod model;
pub mod move_tasks;
pub mod persistence;
pub mod policy;
pub mod server;
pub mod sigv4;
pub mod time_fmt;
pub mod validation;

pub use hashing::{md5_hex, md5_of_message_attributes};
pub use messages::{
    ChangeMessageVisibilityBatchEntry, DeleteMessageBatchEntry, ReceiveMessageRequest,
    ReceivedMessage, SendMessageBatchEntry, SendMessageRequest,
};
pub use model::{
    DeduplicationState, MessageAttributeValue, MessageState, MoveTaskState, ZERO_TIME,
};
pub use move_tasks::MessageMoveTaskResult;
pub use persistence::{PersistedQueue, PersistedState};
pub use policy::{
    normalized_permission_actions, parse_redrive_allow_policy, parse_redrive_policy, QueuePolicy,
};
pub use server::{queue_name_from_url, Config, QueueState, Server};
pub use validation::{
    valid_batch_entry_id, valid_message_body, valid_queue_name, validate_message_attribute_name,
    validate_message_attribute_value, validate_message_system_attribute,
};
