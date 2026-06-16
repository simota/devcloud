//! Parity tests for the SQS validators, mirroring the cases the legacy service
//! relies on (exact error wording is load-bearing — responses.rs maps message
//! substrings to AWS error codes).

use devcloud_sqs::{
    valid_batch_entry_id, valid_message_body, valid_queue_name, validate_message_attribute_name,
    validate_message_attribute_value, validate_message_system_attribute, MessageAttributeValue,
};

fn attr(data_type: &str, string_value: &str, binary_value: &str) -> MessageAttributeValue {
    MessageAttributeValue {
        data_type: data_type.to_string(),
        string_value: string_value.to_string(),
        binary_value: binary_value.to_string(),
        ..Default::default()
    }
}

#[test]
fn attribute_name_rules() {
    assert!(validate_message_attribute_name("ValidName_1-2.3").is_ok());
    assert_eq!(
        validate_message_attribute_name("").unwrap_err(),
        "invalid attribute name: message attribute name is required"
    );
    assert!(validate_message_attribute_name(&"x".repeat(257))
        .unwrap_err()
        .contains("no longer than 256 characters"));
    assert!(validate_message_attribute_name("AWS.Reserved")
        .unwrap_err()
        .contains("must not start with AWS. or Amazon."));
    assert!(validate_message_attribute_name("amazon.foo")
        .unwrap_err()
        .contains("must not start with AWS. or Amazon."));
    assert!(validate_message_attribute_name(".lead")
        .unwrap_err()
        .contains("period"));
    assert!(validate_message_attribute_name("a..b")
        .unwrap_err()
        .contains("period"));
    assert!(validate_message_attribute_name("bad space")
        .unwrap_err()
        .contains("unsupported characters"));
}

#[test]
fn attribute_value_rules() {
    assert!(validate_message_attribute_value("k", &attr("String", "v", "")).is_ok());
    assert!(validate_message_attribute_value("k", &attr("Number", "3.14", "")).is_ok());
    assert!(
        validate_message_attribute_value("k", &attr("Number", "abc", ""))
            .unwrap_err()
            .contains("Number attributes must be numeric")
    );
    assert!(validate_message_attribute_value("k", &attr("Binary", "", "aGk=")).is_ok());
    assert!(
        validate_message_attribute_value("k", &attr("Binary", "", ""))
            .unwrap_err()
            .contains("BinaryValue is required")
    );
    assert!(
        validate_message_attribute_value("k", &attr("Binary", "", "not base64!!"))
            .unwrap_err()
            .contains("BinaryValue must be base64")
    );
    assert!(validate_message_attribute_value("k", &attr("", "v", ""))
        .unwrap_err()
        .contains("DataType is required"));
    assert!(
        validate_message_attribute_value("k", &attr("String.list", "v", ""))
            .unwrap_err()
            .contains("list DataType is not supported")
    );
    assert!(
        validate_message_attribute_value("k", &attr("Weird", "v", ""))
            .unwrap_err()
            .contains("unsupported DataType")
    );
}

#[test]
fn system_attribute_rules() {
    assert!(
        validate_message_system_attribute("AWSTraceHeader", &attr("String", "Root=1-x", ""))
            .is_ok()
    );
    assert!(
        validate_message_system_attribute("Other", &attr("String", "v", ""))
            .unwrap_err()
            .contains("unsupported message system attribute Other")
    );
}

#[test]
fn message_body_rules() {
    assert!(valid_message_body("normal text"));
    assert!(valid_message_body("tab\tnewline\r\nok"));
    assert!(valid_message_body("emoji 🎉 ok"));
    assert!(!valid_message_body("bad \u{FFFD} replacement"));
    assert!(!valid_message_body("\u{0000}null"));
    assert!(!valid_message_body("\u{0001}control"));
}

#[test]
fn batch_entry_id_rules() {
    assert!(valid_batch_entry_id("id-1_2"));
    assert!(valid_batch_entry_id(&"a".repeat(80)));
    assert!(!valid_batch_entry_id(""));
    assert!(!valid_batch_entry_id(&"a".repeat(81)));
    assert!(!valid_batch_entry_id("has space"));
    assert!(!valid_batch_entry_id("dot.notallowed"));
}

#[test]
fn queue_name_rules() {
    assert!(valid_queue_name("MyQueue"));
    assert!(valid_queue_name("My-Queue_1"));
    assert!(valid_queue_name("Orders.fifo"));
    assert!(valid_queue_name(&"q".repeat(80)));
    assert!(!valid_queue_name(""));
    assert!(!valid_queue_name(&"q".repeat(81)));
    assert!(!valid_queue_name("bad name"));
    assert!(!valid_queue_name("dotted.name")); // only `.fifo` suffix allowed
}
