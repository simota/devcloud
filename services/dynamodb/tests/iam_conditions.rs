//! Tests for IAM resource-policy and condition enforcement on DynamoDB
//! operations (`dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`,
//! `dynamodb:ReturnValues`, `dynamodb:IndexName`, `dynamodb:EnclosingOperation`,
//! `aws:ResourceTag/*`, `aws:RequestTag/*`, `aws:TagKeys`, `aws:RequestedRegion`,
//! `aws:username`, policy variables, set quantifiers, `IfExists`, `Null`, and
//! explicit `Deny` precedence).

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement, Tag};
use devcloud_dynamodb::requests::{
    BatchGetItemRequest, BatchGetTableRequest, CreateTableRequest, DeleteItemRequest,
    ExecuteStatementRequest, GetItemRequest, GlobalSecondaryIndexRequest, IndexProjectionRequest,
    PutItemRequest, PutResourcePolicyRequest, QueryRequest, ResourceArnRequest, ScanRequest,
    TagResourceRequest, TransactGet, TransactGetItem, TransactGetItemsRequest, UpdateItemRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use devcloud_dynamodb::sigv4::SignedRequest;
use serde_json::{json, Value};

const ARN: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/Docs";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        access_key_id: "alice".to_string(),
        secret_access_key: "dev".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_300_000);
    s.create_table(&CreateTableRequest {
        table_name: "Docs".to_string(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: "S".to_string(),
            },
            AttributeDefinition {
                attribute_name: "gsiPk".to_string(),
                attribute_type: "S".to_string(),
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: "RANGE".to_string(),
            },
        ],
        global_secondary_indexes: vec![GlobalSecondaryIndexRequest {
            index_name: "byGsi".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsiPk".to_string(),
                key_type: "HASH".to_string(),
            }],
            projection: IndexProjectionRequest {
                projection_type: "ALL".to_string(),
                non_key_attributes: vec![],
            },
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create table");

    for (pk, sk, gsi, title, secret) in [
        ("alice", "1", "g1", "Alice Doc", "top-secret-a"),
        ("bob", "1", "g2", "Bob Doc", "top-secret-b"),
    ] {
        let mut item = Item::new();
        item.insert("pk".to_string(), json!({"S": pk}));
        item.insert("sk".to_string(), json!({"S": sk}));
        item.insert("gsiPk".to_string(), json!({"S": gsi}));
        item.insert("title".to_string(), json!({"S": title}));
        item.insert("secret".to_string(), json!({"S": secret}));
        s.put_item(&PutItemRequest {
            table_name: "Docs".to_string(),
            item,
            ..Default::default()
        })
        .expect("seed item");
    }
    s
}

fn key(pk: &str, sk: &str) -> Item {
    let mut k = Item::new();
    k.insert("pk".to_string(), json!({"S": pk}));
    k.insert("sk".to_string(), json!({"S": sk}));
    k
}

fn put_policy(s: &mut Server, policy: Value) {
    s.put_resource_policy(&PutResourcePolicyRequest {
        resource_arn: ARN.to_string(),
        policy: policy.to_string(),
    })
    .expect("put policy");
}

#[test]
fn leading_keys_condition_enforced_with_policy_variable_and_denies_scan() {
    let dir = tempdir();
    let mut s = server(&dir);

    put_policy(
        &mut s,
        json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:BatchGetItem",
                    "dynamodb:PartiQLSelect"
                ],
                "Resource": ARN,
                "Condition": {
                    "ForAllValues:StringEquals": {
                        "dynamodb:LeadingKeys": ["${aws:username}"]
                    }
                }
            }]
        }),
    );

    // Default caller is "alice": own key is allowed.
    let ok_get = s
        .get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            ..Default::default()
        })
        .expect("alice own item");
    let v: Value = serde_json::from_slice(&ok_get).unwrap();
    assert_eq!(v["Item"]["title"]["S"], "Alice Doc");

    // Accessing "bob" as "alice" is denied.
    let err = s
        .get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("bob", "1"),
            ..Default::default()
        })
        .expect_err("bob denied");
    assert_eq!(err.name, "AccessDeniedException");

    // Query with pk = alice allowed, pk = bob denied.
    let mut vals = BTreeMap::new();
    vals.insert(":p".to_string(), json!({"S": "alice"}));
    assert!(s
        .query(&QueryRequest {
            table_name: "Docs".to_string(),
            key_condition_expression: "pk = :p".to_string(),
            expression_attribute_values: vals,
            ..Default::default()
        })
        .is_ok());

    let mut bad_vals = BTreeMap::new();
    bad_vals.insert(":p".to_string(), json!({"S": "bob"}));
    assert_eq!(
        s.query(&QueryRequest {
            table_name: "Docs".to_string(),
            key_condition_expression: "pk = :p".to_string(),
            expression_attribute_values: bad_vals,
            ..Default::default()
        })
        .expect_err("bob query denied")
        .name,
        "AccessDeniedException"
    );

    // Scan is unbounded on LeadingKeys and therefore denied.
    assert_eq!(
        s.scan(&ScanRequest {
            table_name: "Docs".to_string(),
            ..Default::default()
        })
        .expect_err("scan denied")
        .name,
        "AccessDeniedException"
    );

    // BatchGetItem mixing alice and bob is denied (ForAllValues fails on bob).
    let mut req_items = BTreeMap::new();
    req_items.insert(
        "Docs".to_string(),
        BatchGetTableRequest {
            keys: vec![key("alice", "1"), key("bob", "1")],
            ..Default::default()
        },
    );
    assert_eq!(
        s.batch_get_item(&BatchGetItemRequest {
            request_items: req_items,
            ..Default::default()
        })
        .expect_err("mixed batch denied")
        .name,
        "AccessDeniedException"
    );

    // PartiQL SELECT on alice allowed, on bob denied.
    assert!(s
        .execute_statement(&ExecuteStatementRequest {
            statement: "SELECT pk, title FROM Docs WHERE pk = ?".to_string(),
            parameters: vec![json!({"S": "alice"})],
            ..Default::default()
        })
        .is_ok());
    assert_eq!(
        s.execute_statement(&ExecuteStatementRequest {
            statement: "SELECT pk, title FROM Docs WHERE pk = ?".to_string(),
            parameters: vec![json!({"S": "bob"})],
            ..Default::default()
        })
        .expect_err("partiql bob denied")
        .name,
        "AccessDeniedException"
    );

    // Dispatch extracts principal from SigV4 Authorization header even in relaxed mode.
    let mut headers = BTreeMap::new();
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=bob/20260501/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00"
            .to_string(),
    );
    let body = br#"{"TableName":"Docs","Key":{"pk":{"S":"bob"},"sk":{"S":"1"}}}"#;
    let signed = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:18000",
        headers: &headers,
        body,
    };
    let bob_get = s.dispatch(
        "DynamoDB_20120810.GetItem",
        body,
        "relaxed",
        Some(&signed),
        1_780_300_000,
    );
    assert_eq!(bob_get.status, 200);

    // Deleting the resource policy restores open access.
    s.delete_resource_policy(&ResourceArnRequest {
        resource_arn: ARN.to_string(),
    })
    .expect("delete policy");
    assert!(s
        .scan(&ScanRequest {
            table_name: "Docs".to_string(),
            ..Default::default()
        })
        .is_ok());
}

#[test]
fn attributes_select_return_values_and_enclosing_operation_conditions() {
    let dir = tempdir();
    let mut s = server(&dir);

    // Allow reading only pk, sk, title with SPECIFIC_ATTRIBUTES, and updates without ALL_OLD.
    put_policy(
        &mut s,
        json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["dynamodb:GetItem", "dynamodb:Query"],
                    "Resource": ARN,
                    "Condition": {
                        "ForAllValues:StringEquals": {
                            "dynamodb:Attributes": ["pk", "sk", "title"]
                        },
                        "StringEqualsIfExists": {
                            "dynamodb:Select": "SPECIFIC_ATTRIBUTES"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "dynamodb:UpdateItem",
                    "Resource": ARN,
                    "Condition": {
                        "StringNotEquals": {
                            "dynamodb:ReturnValues": ["ALL_OLD", "UPDATED_OLD"]
                        }
                    }
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "dynamodb:GetItem",
                    "Resource": ARN,
                    "Condition": {
                        "StringEquals": {
                            "dynamodb:EnclosingOperation": "TransactGetItems"
                        }
                    }
                }
            ]
        }),
    );

    // Projected GetItem on allowed attributes succeeds.
    assert!(s
        .get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            projection_expression: "pk, title".to_string(),
            ..Default::default()
        })
        .is_ok());

    // Unprojected GetItem fails (requests all attributes including secret).
    assert_eq!(
        s.get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            ..Default::default()
        })
        .expect_err("unprojected denied")
        .name,
        "AccessDeniedException"
    );

    // Projected GetItem requesting secret fails.
    assert_eq!(
        s.get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            projection_expression: "pk, secret".to_string(),
            ..Default::default()
        })
        .expect_err("secret denied")
        .name,
        "AccessDeniedException"
    );

    // UpdateItem with NONE allowed; with ALL_OLD denied.
    let mut vals = BTreeMap::new();
    vals.insert(":t".to_string(), json!({"S": "Updated"}));
    assert!(s
        .update_item(&UpdateItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            update_expression: "SET title = :t".to_string(),
            expression_attribute_values: vals.clone(),
            return_values: "NONE".to_string(),
            ..Default::default()
        })
        .is_ok());
    assert_eq!(
        s.update_item(&UpdateItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            update_expression: "SET title = :t".to_string(),
            expression_attribute_values: vals,
            return_values: "ALL_OLD".to_string(),
            ..Default::default()
        })
        .expect_err("ALL_OLD denied")
        .name,
        "AccessDeniedException"
    );

    // TransactGetItems explicitly denied by EnclosingOperation Deny even though GetItem is allowed.
    assert_eq!(
        s.transact_get_items(&TransactGetItemsRequest {
            transact_items: vec![TransactGetItem {
                get: Some(TransactGet {
                    table_name: "Docs".to_string(),
                    key: key("alice", "1"),
                    projection_expression: "pk, title".to_string(),
                    ..Default::default()
                }),
            }],
        })
        .expect_err("transact get denied")
        .name,
        "AccessDeniedException"
    );
}

#[test]
fn resource_tag_request_tag_index_and_explicit_deny_conditions() {
    let dir = tempdir();
    let mut s = server(&dir);

    s.tag_resource(&TagResourceRequest {
        resource_arn: ARN.to_string(),
        tags: vec![Tag {
            key: "env".to_string(),
            value: "prod".to_string(),
        }],
    })
    .expect("tag table");

    // Policy with only Deny statements: baseline access remains allowed except matching Deny conditions.
    put_policy(
        &mut s,
        json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "dynamodb:DeleteItem",
                    "Resource": ARN,
                    "Condition": {
                        "StringEquals": {
                            "aws:ResourceTag/env": "prod",
                            "aws:RequestedRegion": "us-east-1"
                        }
                    }
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "dynamodb:Query",
                    "Resource": format!("{ARN}/index/*"),
                    "Condition": {
                        "StringLike": {
                            "dynamodb:IndexName": "by*"
                        }
                    }
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "dynamodb:TagResource",
                    "Resource": ARN,
                    "Condition": {
                        "ForAnyValue:StringEqualsIgnoreCase": {
                            "aws:TagKeys": ["protected"]
                        }
                    }
                }
            ]
        }),
    );

    // DeleteItem denied on prod-tagged table in us-east-1.
    assert_eq!(
        s.delete_item(&DeleteItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            ..Default::default()
        })
        .expect_err("delete denied")
        .name,
        "AccessDeniedException"
    );

    // GetItem still allowed (only Deny statements present, none match GetItem).
    assert!(s
        .get_item(&GetItemRequest {
            table_name: "Docs".to_string(),
            key: key("alice", "1"),
            ..Default::default()
        })
        .is_ok());

    // Query on GSI `byGsi` denied by IndexName wildcard Deny; base table Query allowed.
    let mut gsi_vals = BTreeMap::new();
    gsi_vals.insert(":g".to_string(), json!({"S": "g1"}));
    assert_eq!(
        s.query(&QueryRequest {
            table_name: "Docs".to_string(),
            index_name: "byGsi".to_string(),
            key_condition_expression: "gsiPk = :g".to_string(),
            expression_attribute_values: gsi_vals,
            ..Default::default()
        })
        .expect_err("gsi query denied")
        .name,
        "AccessDeniedException"
    );

    let mut base_vals = BTreeMap::new();
    base_vals.insert(":p".to_string(), json!({"S": "alice"}));
    assert!(s
        .query(&QueryRequest {
            table_name: "Docs".to_string(),
            key_condition_expression: "pk = :p".to_string(),
            expression_attribute_values: base_vals,
            ..Default::default()
        })
        .is_ok());

    // TagResource with key `Protected` denied (case-insensitive ForAnyValue).
    assert_eq!(
        s.tag_resource(&TagResourceRequest {
            resource_arn: ARN.to_string(),
            tags: vec![Tag {
                key: "Protected".to_string(),
                value: "yes".to_string(),
            }],
        })
        .expect_err("protected tag denied")
        .name,
        "AccessDeniedException"
    );
    // Other tag keys allowed.
    assert!(s
        .tag_resource(&TagResourceRequest {
            resource_arn: ARN.to_string(),
            tags: vec![Tag {
                key: "owner".to_string(),
                value: "alice".to_string(),
            }],
        })
        .is_ok());
}

#[test]
fn numeric_null_ifexists_batch_write_transact_write_and_batch_partiql() {
    use devcloud_dynamodb::requests::{
        BatchExecuteStatementRequest, BatchStatementRequest, BatchWriteItemRequest,
        ExecuteTransactionRequest, PutRequest, TransactPut, TransactWriteItem,
        TransactWriteItemsRequest, WriteRequest,
    };

    let dir = tempdir();
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        access_key_id: "alice".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_300_000);
    s.create_table(&CreateTableRequest {
        table_name: "Nums".to_string(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: "N".to_string(),
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: "HASH".to_string(),
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create table");

    let num_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/Nums";
    s.put_resource_policy(&PutResourcePolicyRequest {
        resource_arn: num_arn.to_string(),
        policy: json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:Query",
                    "dynamodb:PartiQLSelect"
                ],
                "Resource": num_arn,
                "Condition": {
                    "ForAllValues:NumericLessThan": {
                        "dynamodb:LeadingKeys": ["100"]
                    },
                    "Null": {
                        "dynamodb:IndexName": "true"
                    },
                    "StringEqualsIfExists": {
                        "dynamodb:EnclosingOperation": ["BatchWriteItem", "TransactWriteItems"]
                    }
                }
            }]
        })
        .to_string(),
    })
    .expect("put policy");

    let item = |id: &str| {
        let mut m = Item::new();
        m.insert("id".to_string(), json!({"N": id}));
        m
    };

    // BatchWriteItem with id < 100 succeeds; id >= 100 fails.
    let mut ok_batch = BTreeMap::new();
    ok_batch.insert(
        "Nums".to_string(),
        vec![WriteRequest {
            put_request: Some(PutRequest { item: item("42") }),
            delete_request: None,
        }],
    );
    assert!(s
        .batch_write_item(&BatchWriteItemRequest {
            request_items: ok_batch,
            ..Default::default()
        })
        .is_ok());

    let mut bad_batch = BTreeMap::new();
    bad_batch.insert(
        "Nums".to_string(),
        vec![WriteRequest {
            put_request: Some(PutRequest { item: item("150") }),
            delete_request: None,
        }],
    );
    assert_eq!(
        s.batch_write_item(&BatchWriteItemRequest {
            request_items: bad_batch,
            ..Default::default()
        })
        .expect_err("id 150 denied")
        .name,
        "AccessDeniedException"
    );

    // TransactWriteItems with id < 100 succeeds; id = 100 fails.
    assert!(s
        .transact_write_items(&TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: Some(TransactPut {
                    table_name: "Nums".to_string(),
                    item: item("99"),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        })
        .is_ok());
    assert_eq!(
        s.transact_write_items(&TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: Some(TransactPut {
                    table_name: "Nums".to_string(),
                    item: item("100"),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        })
        .expect_err("id 100 denied")
        .name,
        "AccessDeniedException"
    );

    // BatchExecuteStatement reports AccessDenied inline for denied key while succeeding for allowed key.
    let bes = s
        .batch_execute_statement(&BatchExecuteStatementRequest {
            statements: vec![
                BatchStatementRequest {
                    statement: "SELECT id FROM Nums WHERE id = ?".to_string(),
                    parameters: vec![json!({"N": "42"})],
                    ..Default::default()
                },
                BatchStatementRequest {
                    statement: "SELECT id FROM Nums WHERE id = ?".to_string(),
                    parameters: vec![json!({"N": "150"})],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .expect("bes");
    let bes_json: Value = serde_json::from_slice(&bes).unwrap();
    assert_eq!(bes_json["Responses"][0]["Item"]["id"]["N"], "42");
    assert_eq!(bes_json["Responses"][1]["Error"]["Code"], "AccessDenied");

    // ExecuteTransaction rejects entire transaction with AccessDeniedException if any statement is denied.
    assert_eq!(
        s.execute_transaction(&ExecuteTransactionRequest {
            transact_statements: vec![BatchStatementRequest {
                statement: "SELECT id FROM Nums WHERE id = ?".to_string(),
                parameters: vec![json!({"N": "200"})],
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect_err("etx denied")
        .name,
        "AccessDeniedException"
    );
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-iam-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
