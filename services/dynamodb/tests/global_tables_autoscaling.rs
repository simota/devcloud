//! Tests for Global Tables (v1 `2017.11.29` & v2 `2019.11.21` replica updates)
//! and DynamoDB Table Replica Auto Scaling metadata operations.

use devcloud_dynamodb::global_tables::{
    AutoScalingPolicyUpdate, AutoScalingSettingsUpdate,
    AutoScalingTargetTrackingScalingPolicyConfigurationUpdate, CreateGlobalTableRequest,
    CreateReplicaAction, CreateReplicationGroupMemberAction, DeleteReplicaAction,
    DeleteReplicationGroupMemberAction, GlobalSecondaryIndexAutoScalingUpdate,
    GlobalTableNameRequest, ListGlobalTablesRequest, ProvisionedThroughputOverride, Replica,
    ReplicaAutoScalingUpdate, ReplicaGlobalSecondaryIndexAutoScalingUpdate,
    ReplicaGlobalSecondaryIndexUpdateInput, ReplicaSettingsUpdate, ReplicaUpdate,
    ReplicationGroupUpdate, UpdateGlobalTableRequest, UpdateGlobalTableSettingsRequest,
    UpdateReplicationGroupMemberAction, UpdateTableReplicaAutoScalingRequest,
};
use devcloud_dynamodb::model::{AttributeDefinition, KeySchemaElement};
use devcloud_dynamodb::requests::{
    CreateTableRequest, GlobalSecondaryIndexRequest, IndexProjectionRequest, UpdateTableRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

fn parse(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("valid JSON")
}

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_100_000);
    s
}

fn create_orders_table(s: &mut Server, name: &str) {
    s.create_table(&CreateTableRequest {
        table_name: name.to_string(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            },
            AttributeDefinition {
                attribute_name: "gsiPk".to_string(),
                attribute_type: "S".to_string(),
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: "HASH".to_string(),
        }],
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
        billing_mode: "PROVISIONED".to_string(),
        ..Default::default()
    })
    .expect("create table");
}

#[test]
fn global_table_v1_lifecycle_settings_and_reload() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_orders_table(&mut s, "Orders");
    create_orders_table(&mut s, "Users");

    // Missing underlying table fails with TableNotFoundException.
    let err = s
        .create_global_table(&CreateGlobalTableRequest {
            global_table_name: "Missing".to_string(),
            replication_group: vec![Replica {
                region_name: "us-east-1".to_string(),
            }],
        })
        .expect_err("missing table");
    assert_eq!(err.name, "TableNotFoundException");

    // Create global table across two regions.
    let created = parse(
        &s.create_global_table(&CreateGlobalTableRequest {
            global_table_name: "Orders".to_string(),
            replication_group: vec![
                Replica {
                    region_name: "us-east-1".to_string(),
                },
                Replica {
                    region_name: "eu-west-1".to_string(),
                },
            ],
        })
        .expect("create global table"),
    );
    let desc = &created["GlobalTableDescription"];
    assert_eq!(desc["GlobalTableName"], "Orders");
    assert_eq!(desc["GlobalTableStatus"], "ACTIVE");
    assert_eq!(
        desc["GlobalTableArn"],
        "arn:aws:dynamodb::000000000000:global-table/Orders"
    );
    assert_eq!(desc["ReplicationGroup"].as_array().unwrap().len(), 2);

    // Duplicate create fails.
    assert_eq!(
        s.create_global_table(&CreateGlobalTableRequest {
            global_table_name: "Orders".to_string(),
            replication_group: vec![Replica {
                region_name: "us-east-1".to_string(),
            }],
        })
        .expect_err("dup")
        .name,
        "GlobalTableAlreadyExistsException"
    );

    // Create second global table in ap-northeast-1 only, test ListGlobalTables filter + pagination.
    s.create_global_table(&CreateGlobalTableRequest {
        global_table_name: "Users".to_string(),
        replication_group: vec![Replica {
            region_name: "ap-northeast-1".to_string(),
        }],
    })
    .expect("create second");

    let filtered = parse(
        &s.list_global_tables(&ListGlobalTablesRequest {
            region_name: "eu-west-1".to_string(),
            ..Default::default()
        })
        .expect("list filtered"),
    );
    assert_eq!(filtered["GlobalTables"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["GlobalTables"][0]["GlobalTableName"], "Orders");

    let page1 = parse(
        &s.list_global_tables(&ListGlobalTablesRequest {
            limit: 1,
            ..Default::default()
        })
        .expect("page1"),
    );
    assert_eq!(page1["GlobalTables"][0]["GlobalTableName"], "Orders");
    assert_eq!(page1["LastEvaluatedGlobalTableName"], "Orders");

    let page2 = parse(
        &s.list_global_tables(&ListGlobalTablesRequest {
            exclusive_start_global_table_name: "Orders".to_string(),
            limit: 1,
            ..Default::default()
        })
        .expect("page2"),
    );
    assert_eq!(page2["GlobalTables"][0]["GlobalTableName"], "Users");
    assert!(page2.get("LastEvaluatedGlobalTableName").is_none());

    // UpdateGlobalTable: add us-west-2, remove eu-west-1.
    let updated = parse(
        &s.update_global_table(&UpdateGlobalTableRequest {
            global_table_name: "Orders".to_string(),
            replica_updates: vec![
                ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: "us-west-2".to_string(),
                    }),
                    delete: None,
                },
                ReplicaUpdate {
                    create: None,
                    delete: Some(DeleteReplicaAction {
                        region_name: "eu-west-1".to_string(),
                    }),
                },
            ],
        })
        .expect("update global table"),
    );
    let regions: Vec<&str> = updated["GlobalTableDescription"]["ReplicationGroup"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["RegionName"].as_str().unwrap())
        .collect();
    assert_eq!(regions, vec!["us-east-1", "us-west-2"]);

    // Duplicate replica add fails.
    assert_eq!(
        s.update_global_table(&UpdateGlobalTableRequest {
            global_table_name: "Orders".to_string(),
            replica_updates: vec![ReplicaUpdate {
                create: Some(CreateReplicaAction {
                    region_name: "us-west-2".to_string(),
                }),
                delete: None,
            }],
        })
        .expect_err("dup replica")
        .name,
        "ReplicaAlreadyExistsException"
    );

    // UpdateGlobalTableSettings + DescribeGlobalTableSettings.
    let settings = parse(
        &s.update_global_table_settings(&UpdateGlobalTableSettingsRequest {
            global_table_name: "Orders".to_string(),
            global_table_billing_mode: "PROVISIONED".to_string(),
            global_table_provisioned_write_capacity_units: Some(25),
            replica_settings_update: vec![ReplicaSettingsUpdate {
                region_name: "us-west-2".to_string(),
                replica_provisioned_read_capacity_units: Some(50),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("update settings"),
    );
    assert_eq!(settings["GlobalTableName"], "Orders");
    assert_eq!(
        settings["ReplicaSettings"][1]["ReplicaProvisionedReadCapacityUnits"],
        50
    );
    assert_eq!(
        settings["ReplicaSettings"][0]["ReplicaProvisionedWriteCapacityUnits"],
        25
    );

    // Survives reload across Server instances.
    let reloaded = server(&dir);
    let described = parse(
        &reloaded
            .describe_global_table(&GlobalTableNameRequest {
                global_table_name: "Orders".to_string(),
            })
            .expect("describe reloaded"),
    );
    assert_eq!(
        described["GlobalTableDescription"]["ReplicationGroup"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let described_settings = parse(
        &reloaded
            .describe_global_table_settings(&GlobalTableNameRequest {
                global_table_name: "Orders".to_string(),
            })
            .expect("describe settings reloaded"),
    );
    assert_eq!(
        described_settings["ReplicaSettings"][1]["ReplicaProvisionedReadCapacityUnits"],
        50
    );
}

#[test]
fn global_table_v2_update_table_replicas_and_reload() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_orders_table(&mut s, "Orders");

    // Add replica via UpdateTable ReplicaUpdates.
    let added = parse(
        &s.update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            replica_updates: vec![ReplicationGroupUpdate {
                create: Some(CreateReplicationGroupMemberAction {
                    region_name: "eu-central-1".to_string(),
                    kms_master_key_id: "alias/ddb-eu".to_string(),
                    provisioned_throughput_override: Some(ProvisionedThroughputOverride {
                        read_capacity_units: Some(40),
                    }),
                    global_secondary_indexes: vec![ReplicaGlobalSecondaryIndexUpdateInput {
                        index_name: "byGsi".to_string(),
                        provisioned_throughput_override: Some(ProvisionedThroughputOverride {
                            read_capacity_units: Some(20),
                        }),
                    }],
                    table_class_override: "STANDARD_INFREQUENT_ACCESS".to_string(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("create replica"),
    );
    let td = &added["TableDescription"];
    assert_eq!(td["GlobalTableVersion"], "2019.11.21");
    assert_eq!(td["Replicas"][0]["RegionName"], "eu-central-1");
    assert_eq!(td["Replicas"][0]["ReplicaStatus"], "ACTIVE");
    assert_eq!(td["Replicas"][0]["KMSMasterKeyId"], "alias/ddb-eu");
    assert_eq!(
        td["Replicas"][0]["ProvisionedThroughputOverride"]["ReadCapacityUnits"],
        40
    );
    assert_eq!(
        td["Replicas"][0]["ReplicaTableClassSummary"]["TableClass"],
        "STANDARD_INFREQUENT_ACCESS"
    );

    // Update replica override.
    let updated = parse(
        &s.update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            replica_updates: vec![ReplicationGroupUpdate {
                update: Some(UpdateReplicationGroupMemberAction {
                    region_name: "eu-central-1".to_string(),
                    provisioned_throughput_override: Some(ProvisionedThroughputOverride {
                        read_capacity_units: Some(80),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("update replica"),
    );
    assert_eq!(
        updated["TableDescription"]["Replicas"][0]["ProvisionedThroughputOverride"]
            ["ReadCapacityUnits"],
        80
    );

    // Reload preserves v2 replicas.
    let s2 = server(&dir);
    let described = parse(&s2.describe_table("Orders").expect("describe reloaded"));
    assert_eq!(described["Table"]["GlobalTableVersion"], "2019.11.21");
    assert_eq!(
        described["Table"]["Replicas"][0]["RegionName"],
        "eu-central-1"
    );

    // Delete replica.
    let deleted = parse(
        &s.update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            replica_updates: vec![ReplicationGroupUpdate {
                delete: Some(DeleteReplicationGroupMemberAction {
                    region_name: "eu-central-1".to_string(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("delete replica"),
    );
    assert!(deleted["TableDescription"].get("Replicas").is_none());

    // Deleting missing replica returns ReplicaNotFoundException.
    let err = s
        .update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            replica_updates: vec![ReplicationGroupUpdate {
                delete: Some(DeleteReplicationGroupMemberAction {
                    region_name: "eu-central-1".to_string(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect_err("missing replica");
    assert_eq!(err.name, "ReplicaNotFoundException");
}

#[test]
fn table_replica_autoscaling_lifecycle_and_validation() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_orders_table(&mut s, "Orders");

    // Add a second region replica so we can test write-sync + per-replica read settings.
    s.update_table(&UpdateTableRequest {
        table_name: "Orders".to_string(),
        replica_updates: vec![ReplicationGroupUpdate {
            create: Some(CreateReplicationGroupMemberAction {
                region_name: "us-west-2".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    })
    .expect("add replica");

    // Initial DescribeTableReplicaAutoScaling shows both regions disabled by default.
    let initial = parse(
        &s.describe_table_replica_auto_scaling("Orders")
            .expect("describe autoscaling"),
    );
    let replicas = initial["TableAutoScalingDescription"]["Replicas"]
        .as_array()
        .unwrap();
    assert_eq!(replicas.len(), 2);
    assert_eq!(replicas[0]["RegionName"], "us-east-1");
    assert_eq!(replicas[1]["RegionName"], "us-west-2");
    assert_eq!(
        replicas[0]["ReplicaProvisionedWriteCapacityAutoScalingSettings"]["AutoScalingDisabled"],
        true
    );

    // Update write autoscaling (syncs across replicas), GSI write, and us-west-2 read + GSI read.
    let updated = parse(
        &s.update_table_replica_auto_scaling(&UpdateTableReplicaAutoScalingRequest {
            table_name: "Orders".to_string(),
            provisioned_write_capacity_auto_scaling_update: Some(AutoScalingSettingsUpdate {
                auto_scaling_disabled: Some(false),
                auto_scaling_role_arn: Some(
                    "arn:aws:iam::000000000000:role/ddb-autoscale".to_string(),
                ),
                minimum_units: Some(5),
                maximum_units: Some(100),
                scaling_policy_update: Some(AutoScalingPolicyUpdate {
                    policy_name: "WritePolicy".to_string(),
                    target_tracking_scaling_policy_configuration: Some(
                        AutoScalingTargetTrackingScalingPolicyConfigurationUpdate {
                            disable_scale_in: Some(false),
                            scale_in_cooldown: Some(60),
                            scale_out_cooldown: Some(30),
                            target_value: 70.0,
                        },
                    ),
                }),
            }),
            global_secondary_index_updates: vec![GlobalSecondaryIndexAutoScalingUpdate {
                index_name: "byGsi".to_string(),
                provisioned_write_capacity_auto_scaling_update: Some(AutoScalingSettingsUpdate {
                    auto_scaling_disabled: Some(false),
                    minimum_units: Some(2),
                    maximum_units: Some(20),
                    ..Default::default()
                }),
            }],
            replica_updates: vec![ReplicaAutoScalingUpdate {
                region_name: "us-west-2".to_string(),
                replica_provisioned_read_capacity_auto_scaling_update: Some(
                    AutoScalingSettingsUpdate {
                        auto_scaling_disabled: Some(false),
                        minimum_units: Some(10),
                        maximum_units: Some(200),
                        scaling_policy_update: Some(AutoScalingPolicyUpdate {
                            policy_name: "WestReadPolicy".to_string(),
                            target_tracking_scaling_policy_configuration: Some(
                                AutoScalingTargetTrackingScalingPolicyConfigurationUpdate {
                                    target_value: 65.0,
                                    ..Default::default()
                                },
                            ),
                        }),
                        ..Default::default()
                    },
                ),
                replica_global_secondary_index_updates: vec![
                    ReplicaGlobalSecondaryIndexAutoScalingUpdate {
                        index_name: "byGsi".to_string(),
                        provisioned_read_capacity_auto_scaling_update: Some(
                            AutoScalingSettingsUpdate {
                                auto_scaling_disabled: Some(false),
                                minimum_units: Some(3),
                                maximum_units: Some(30),
                                ..Default::default()
                            },
                        ),
                    },
                ],
            }],
        })
        .expect("update autoscaling"),
    );

    let r0 = &updated["TableAutoScalingDescription"]["Replicas"][0];
    let r1 = &updated["TableAutoScalingDescription"]["Replicas"][1];
    // Write autoscaling synced to both regions.
    assert_eq!(
        r0["ReplicaProvisionedWriteCapacityAutoScalingSettings"]["MinimumUnits"],
        5
    );
    assert_eq!(
        r1["ReplicaProvisionedWriteCapacityAutoScalingSettings"]["MaximumUnits"],
        100
    );
    assert_eq!(
        r1["ReplicaProvisionedWriteCapacityAutoScalingSettings"]["ScalingPolicies"][0]
            ["PolicyName"],
        "WritePolicy"
    );
    // Read autoscaling set only on us-west-2.
    assert_eq!(
        r0["ReplicaProvisionedReadCapacityAutoScalingSettings"]["AutoScalingDisabled"],
        true
    );
    assert_eq!(
        r1["ReplicaProvisionedReadCapacityAutoScalingSettings"]["MinimumUnits"],
        10
    );
    assert_eq!(
        r1["GlobalSecondaryIndexes"][0]["ProvisionedReadCapacityAutoScalingSettings"]
            ["MaximumUnits"],
        30
    );
    assert_eq!(
        r0["GlobalSecondaryIndexes"][0]["ProvisionedWriteCapacityAutoScalingSettings"]
            ["MaximumUnits"],
        20
    );

    // Validation errors.
    let bad_bounds = s
        .update_table_replica_auto_scaling(&UpdateTableReplicaAutoScalingRequest {
            table_name: "Orders".to_string(),
            provisioned_write_capacity_auto_scaling_update: Some(AutoScalingSettingsUpdate {
                minimum_units: Some(50),
                maximum_units: Some(10),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect_err("min > max");
    assert_eq!(bad_bounds.name, "ValidationException");

    let bad_target = s
        .update_table_replica_auto_scaling(&UpdateTableReplicaAutoScalingRequest {
            table_name: "Orders".to_string(),
            provisioned_write_capacity_auto_scaling_update: Some(AutoScalingSettingsUpdate {
                scaling_policy_update: Some(AutoScalingPolicyUpdate {
                    policy_name: "Bad".to_string(),
                    target_tracking_scaling_policy_configuration: Some(
                        AutoScalingTargetTrackingScalingPolicyConfigurationUpdate {
                            target_value: 150.0,
                            ..Default::default()
                        },
                    ),
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect_err("target > 100");
    assert_eq!(bad_target.name, "ValidationException");

    let bad_region = s
        .update_table_replica_auto_scaling(&UpdateTableReplicaAutoScalingRequest {
            table_name: "Orders".to_string(),
            replica_updates: vec![ReplicaAutoScalingUpdate {
                region_name: "ap-south-1".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect_err("unknown region");
    assert_eq!(bad_region.name, "ReplicaNotFoundException");

    // Dispatch over JSON wire & reload persistence.
    let mut s2 = server(&dir);
    let wire = s2.dispatch(
        "DynamoDB_20120810.DescribeTableReplicaAutoScaling",
        br#"{"TableName":"Orders"}"#,
        "relaxed",
        None,
        1_780_100_000,
    );
    assert_eq!(wire.status, 200);
    let body = parse(&wire.body);
    assert_eq!(
        body["TableAutoScalingDescription"]["Replicas"][1]
            ["ReplicaProvisionedReadCapacityAutoScalingSettings"]["ScalingPolicies"][0]
            ["TargetTrackingScalingPolicyConfiguration"]["TargetValue"],
        json!(65.0)
    );
}

#[test]
fn global_tables_and_autoscaling_dispatch_wire_routes() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_orders_table(&mut s, "Orders");

    let create = s.dispatch(
        "DynamoDB_20120810.CreateGlobalTable",
        br#"{"GlobalTableName":"Orders","ReplicationGroup":[{"RegionName":"us-east-1"},{"RegionName":"eu-west-1"}]}"#,
        "relaxed",
        None,
        1_780_100_000,
    );
    assert_eq!(create.status, 200);

    let list = s.dispatch(
        "DynamoDB_20120810.ListGlobalTables",
        br#"{"RegionName":"eu-west-1"}"#,
        "relaxed",
        None,
        1_780_100_000,
    );
    assert_eq!(list.status, 200);
    assert_eq!(
        parse(&list.body)["GlobalTables"][0]["GlobalTableName"],
        "Orders"
    );

    let upd_as = s.dispatch(
        "DynamoDB_20120810.UpdateTableReplicaAutoScaling",
        br#"{"TableName":"Orders","ProvisionedWriteCapacityAutoScalingUpdate":{"AutoScalingDisabled":false,"MinimumUnits":2,"MaximumUnits":20}}"#,
        "relaxed",
        None,
        1_780_100_000,
    );
    assert_eq!(upd_as.status, 200);
    assert_eq!(
        parse(&upd_as.body)["TableAutoScalingDescription"]["Replicas"][0]
            ["ReplicaProvisionedWriteCapacityAutoScalingSettings"]["MaximumUnits"],
        20
    );
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-gtas-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
