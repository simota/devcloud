//! Byte-exact round-trip parity for the SQS `state.json` schema.
//!
//! `state_oracle.json` is the literal output of the Go `Server.persistLocked`
//! for a queue with one message (all message timestamp/zero fields exercised).
//! Deserializing it through the Rust types and re-serializing must reproduce the
//! Go bytes exactly — proving field names, ordering, omitempty, and the
//! zero-time representation all match.

use devcloud_sqs::PersistedState;

const ORACLE: &str = include_str!("state_oracle.json");

#[test]
fn state_json_round_trips_byte_for_byte() {
    let parsed = PersistedState::from_json(ORACLE.as_bytes()).expect("parse oracle");
    let reserialized = parsed.to_json_bytes();
    assert_eq!(
        String::from_utf8(reserialized).unwrap(),
        ORACLE,
        "re-serialized state.json must match the Go bytes exactly"
    );
}

#[test]
fn parsed_oracle_has_expected_shape() {
    let parsed = PersistedState::from_json(ORACLE.as_bytes()).expect("parse oracle");
    assert!(parsed.move_tasks.is_empty(), "moveTasks omitted in oracle");
    let q = parsed.queues.get("Orders").expect("Orders queue present");
    assert_eq!(q.name, "Orders");
    assert_eq!(q.sequence, 1);
    assert_eq!(q.messages.len(), 1);
    let m = &q.messages[0];
    assert_eq!(m.id, "m1");
    assert_eq!(m.body, "hello");
    assert_eq!(m.body_md5, "5d41402abc4b2a76b9719d911017c592");
    // Unset message timestamps round-trip as the Go zero time.
    assert_eq!(m.invisible_until, "0001-01-01T00:00:00Z");
    assert_eq!(m.first_receive_at, "0001-01-01T00:00:00Z");
    assert!(!m.deleted);
}

#[test]
fn empty_state_serializes_without_movetasks() {
    let state = PersistedState::default();
    // Empty queues map still emits `"queues":{}`; moveTasks is omitted.
    assert_eq!(
        String::from_utf8(state.to_json_bytes()).unwrap(),
        "{\"queues\":{}}\n"
    );
}
