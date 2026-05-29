//! Golden-oracle parity tests for the SQS MD5 hashing, captured from the Go
//! `md5Hex` / `md5OfMessageAttributes` for identical inputs.

use std::collections::BTreeMap;

use devcloud_sqs::{md5_hex, md5_of_message_attributes, MessageAttributeValue};

fn attr(data_type: &str, string_value: &str, binary_value: &str) -> MessageAttributeValue {
    MessageAttributeValue {
        data_type: data_type.to_string(),
        string_value: string_value.to_string(),
        binary_value: binary_value.to_string(),
        ..Default::default()
    }
}

// All expected digests below are captured from the Go md5Hex /
// md5OfMessageAttributes for identical inputs (golden oracle).
#[test]
fn md5_hex_matches_go() {
    assert_eq!(md5_hex("hello"), "5d41402abc4b2a76b9719d911017c592");
    assert_eq!(md5_hex(""), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(md5_hex("Hello, SQS!"), "982e00ffc10ba49378a8653ca8fecf47");
}

#[test]
fn md5_of_message_attributes_single_string_matches_go() {
    let mut attrs = BTreeMap::new();
    attrs.insert("City".to_string(), attr("String", "Seattle", ""));
    assert_eq!(
        md5_of_message_attributes(&attrs),
        "580b2661ad597285cc5c5592cb7824ed"
    );
}

#[test]
fn md5_of_message_attributes_multi_sorted_matches_go() {
    // Number + Binary + String, keys deliberately out of order; the digest must
    // be over the sorted set (Bin, Count, Name).
    let mut attrs = BTreeMap::new();
    attrs.insert("Count".to_string(), attr("Number", "42", ""));
    attrs.insert("Bin".to_string(), attr("Binary", "", "aGVsbG8=")); // base64 "hello"
    attrs.insert("Name".to_string(), attr("String", "Bob", ""));
    assert_eq!(
        md5_of_message_attributes(&attrs),
        "0dd425e182cfceba85952d4c2100285a"
    );
}

#[test]
fn md5_of_message_attributes_empty_is_empty_string() {
    let attrs: BTreeMap<String, MessageAttributeValue> = BTreeMap::new();
    assert_eq!(md5_of_message_attributes(&attrs), "");
}
