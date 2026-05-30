//! Parity tests for tags / resource policy and the `dispatch` HTTP routing
//! layer, against golden oracles captured from the Go service.

use devcloud_dynamodb::model::{AttributeDefinition, KeySchemaElement, Tag};
use devcloud_dynamodb::requests::{
    CreateTableRequest, ListTagsOfResourceRequest, PutResourcePolicyRequest, ResourceArnRequest,
    TagResourceRequest, UntagResourceRequest,
};
use devcloud_dynamodb::server::{Config, Server};

const ARN: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/T";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_099_289);
    s.create_table(&CreateTableRequest {
        table_name: "T".to_string(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: "S".to_string(),
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: "HASH".to_string(),
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create");
    s
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

fn tag(k: &str, v: &str) -> Tag {
    Tag {
        key: k.to_string(),
        value: v.to_string(),
    }
}

#[test]
fn tags_lifecycle_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    matches(
        &s.tag_resource(&TagResourceRequest {
            resource_arn: ARN.to_string(),
            tags: vec![tag("env", "prod"), tag("team", "core")],
        })
        .expect("tag"),
        include_bytes!("fixtures/tag.json"),
        "tag",
    );
    matches(
        &s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: ARN.to_string(),
            ..Default::default()
        })
        .expect("list"),
        include_bytes!("fixtures/tags_list.json"),
        "tags_list",
    );
    matches(
        &s.untag_resource(&UntagResourceRequest {
            resource_arn: ARN.to_string(),
            tag_keys: vec!["team".to_string()],
        })
        .expect("untag"),
        include_bytes!("fixtures/untag.json"),
        "untag",
    );
    matches(
        &s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: ARN.to_string(),
            ..Default::default()
        })
        .expect("list2"),
        include_bytes!("fixtures/tags_list2.json"),
        "tags_list2",
    );
}

#[test]
fn resource_policy_lifecycle_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    matches(
        &s.put_resource_policy(&PutResourcePolicyRequest {
            resource_arn: ARN.to_string(),
            policy: "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string(),
        })
        .expect("put"),
        include_bytes!("fixtures/putpol.json"),
        "putpol",
    );
    matches(
        &s.get_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect("get"),
        include_bytes!("fixtures/getpol.json"),
        "getpol",
    );
    matches(
        &s.delete_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect("del"),
        include_bytes!("fixtures/delpol.json"),
        "delpol",
    );
    assert_eq!(
        s.get_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect_err("gone")
        .name,
        "PolicyNotFoundException"
    );
}

#[test]
fn full_lifecycle_state_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.tag_resource(&TagResourceRequest {
        resource_arn: ARN.to_string(),
        tags: vec![tag("env", "prod"), tag("team", "core")],
    })
    .expect("tag");
    s.untag_resource(&UntagResourceRequest {
        resource_arn: ARN.to_string(),
        tag_keys: vec!["team".to_string()],
    })
    .expect("untag");
    s.put_resource_policy(&PutResourcePolicyRequest {
        resource_arn: ARN.to_string(),
        policy: "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string(),
    })
    .expect("put");
    s.delete_resource_policy(&ResourceArnRequest {
        resource_arn: ARN.to_string(),
    })
    .expect("del");
    let on_disk = std::fs::read(dir.join("state.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/tagspol_state.json"),
        "tagspol_state",
    );
}

#[test]
fn dispatch_routes_operations() {
    let dir = tempdir();
    let mut s = server(&dir);
    // PutItem via dispatch.
    let put = s.dispatch(
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"T","Item":{"pk":{"S":"x"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(put.status, 200);
    assert_eq!(String::from_utf8_lossy(&put.body), "{}\n");

    // GetItem via dispatch returns the item.
    let get = s.dispatch(
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"T","Key":{"pk":{"S":"x"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(get.status, 200);
    assert_eq!(
        String::from_utf8_lossy(&get.body),
        "{\"Item\":{\"pk\":{\"S\":\"x\"}}}\n"
    );

    // Unknown operation.
    let unknown = s.dispatch(
        "DynamoDB_20120810.Nope",
        b"{}",
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(unknown.status, 400);
    assert_eq!(
        unknown.error_type.as_deref(),
        Some("UnknownOperationException")
    );

    // Bad target prefix.
    let bad = s.dispatch("Bogus.Op", b"{}", "relaxed", None, 1_780_000_000);
    assert_eq!(bad.status, 400);
    assert_eq!(bad.error_type.as_deref(), Some("UnknownOperationException"));

    // Malformed JSON.
    let malformed = s.dispatch(
        "DynamoDB_20120810.PutItem",
        b"{not json",
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(malformed.status, 400);
    assert_eq!(
        malformed.error_type.as_deref(),
        Some("SerializationException")
    );
}

#[test]
fn dispatch_expires_ttl_before_serving() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.dispatch(
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"T","TimeToLiveSpecification":{"AttributeName":"exp","Enabled":true}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    s.dispatch(
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"T","Item":{"pk":{"S":"old"},"exp":{"N":"1"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    // A later dispatch with a now past the expiry removes the item first.
    let scan = s.dispatch(
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"T"}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(
        String::from_utf8_lossy(&scan.body),
        "{\"Count\":0,\"Items\":[],\"ScannedCount\":0}\n"
    );
}

#[test]
fn list_tags_of_unknown_resource_errors() {
    let dir = tempdir();
    let s = server(&dir);
    assert_eq!(
        s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/Nope".to_string(),
            ..Default::default()
        })
        .expect_err("nope")
        .name,
        "ResourceNotFoundException"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-ddb-dispatch-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
