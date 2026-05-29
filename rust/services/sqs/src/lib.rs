//! Rust reimplementation of the devcloud SQS service (strangler-fig increment
//! #3 of the Go→Rust transmute).
//!
//! This increment lands the **pure-logic foundation** — the AWS MD5
//! attribute-digest hashing and the message/attribute/queue validators — both
//! verified by golden-oracle parity tests against the Go implementation. The
//! dual-protocol (JSON + Query/XML) HTTP layer, the 24 operation handlers, the
//! visibility/delay/retention scheduler, and the daemon seam follow in
//! subsequent increments.

pub mod hashing;
pub mod model;
pub mod persistence;
pub mod validation;

pub use hashing::{md5_hex, md5_of_message_attributes};
pub use model::{
    DeduplicationState, MessageAttributeValue, MessageState, MoveTaskState, ZERO_TIME,
};
pub use persistence::{PersistedQueue, PersistedState};
pub use validation::{
    valid_batch_entry_id, valid_message_body, valid_queue_name, validate_message_attribute_name,
    validate_message_attribute_value, validate_message_system_attribute,
};
