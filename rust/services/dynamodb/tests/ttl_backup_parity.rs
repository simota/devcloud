//! Differential-parity tests for TTL, continuous backups, and backups
//! (DescribeTimeToLive / UpdateTimeToLive / Describe+UpdateContinuousBackups /
//! CreateBackup / DescribeBackup / ListBackups / DeleteBackup /
//! RestoreTableFromBackup) against golden oracles captured from the Go service.

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{
    BackupArnRequest, CreateBackupRequest, CreateTableRequest, ListBackupsRequest,
    PointInTimeRecoverySpecification, PutItemRequest, RestoreTableFromBackupRequest, ScanRequest,
    TimeToLiveSpecification, UpdateContinuousBackupsRequest, UpdateTimeToLiveRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

const ORACLE_NOW: i64 = 1_780_098_846;
const ARN2: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/T/backup/1780098846-snap2";

fn item(pairs: &[(&str, Value)]) -> Item {
    let mut m = Item::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(ORACLE_NOW);
    s
}

fn create_t(s: &mut Server) {
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
}

fn put(s: &mut Server, it: Item) {
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: it,
        ..Default::default()
    })
    .expect("put");
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

/// Reproduces the full Go oracle scenario (TTL + continuous backups + backups +
/// restore + delete) and checks each response plus the final state.json.
#[test]
fn ttl_backups_scenario_matches_go_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    put(
        &mut s,
        item(&[("pk", json!({"S": "a"})), ("v", json!({"N": "1"}))]),
    );
    put(
        &mut s,
        item(&[("pk", json!({"S": "b"})), ("v", json!({"N": "2"}))]),
    );

    // TTL.
    matches(
        &s.describe_time_to_live("T").expect("ttl0"),
        include_bytes!("fixtures/ttl_desc0.json"),
        "ttl_desc0",
    );
    matches(
        &s.update_time_to_live(&UpdateTimeToLiveRequest {
            table_name: "T".to_string(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "exp".to_string(),
                enabled: true,
            },
        })
        .expect("ttl upd"),
        include_bytes!("fixtures/ttl_upd.json"),
        "ttl_upd",
    );
    matches(
        &s.describe_time_to_live("T").expect("ttl1"),
        include_bytes!("fixtures/ttl_desc1.json"),
        "ttl_desc1",
    );

    // Continuous backups.
    matches(
        &s.describe_continuous_backups("T").expect("cb0"),
        include_bytes!("fixtures/cb_desc0.json"),
        "cb_desc0",
    );
    matches(
        &s.update_continuous_backups(&UpdateContinuousBackupsRequest {
            table_name: "T".to_string(),
            point_in_time_recovery_specification: PointInTimeRecoverySpecification {
                point_in_time_recovery_enabled: true,
            },
        })
        .expect("cb upd"),
        include_bytes!("fixtures/cb_upd.json"),
        "cb_upd",
    );
    matches(
        &s.describe_continuous_backups("T").expect("cb1"),
        include_bytes!("fixtures/cb_desc1.json"),
        "cb_desc1",
    );

    // CreateBackup snap1 then snap2.
    matches(
        &s.create_backup(&CreateBackupRequest {
            table_name: "T".to_string(),
            backup_name: "snap1".to_string(),
        })
        .expect("bk1"),
        include_bytes!("fixtures/bk_create.json"),
        "bk_create",
    );
    s.create_backup(&CreateBackupRequest {
        table_name: "T".to_string(),
        backup_name: "snap2".to_string(),
    })
    .expect("bk2");

    matches(
        &s.describe_backup(&BackupArnRequest {
            backup_arn: ARN2.to_string(),
        })
        .expect("desc"),
        include_bytes!("fixtures/bk_desc.json"),
        "bk_desc",
    );
    matches(
        &s.list_backups(&ListBackupsRequest {
            table_name: "T".to_string(),
            ..Default::default()
        })
        .expect("list"),
        include_bytes!("fixtures/bk_list.json"),
        "bk_list",
    );
    matches(
        &s.restore_table_from_backup(&RestoreTableFromBackupRequest {
            backup_arn: ARN2.to_string(),
            target_table_name: "T2".to_string(),
        })
        .expect("restore"),
        include_bytes!("fixtures/bk_restore.json"),
        "bk_restore",
    );
    matches(
        &s.delete_backup(&BackupArnRequest {
            backup_arn: ARN2.to_string(),
        })
        .expect("delete"),
        include_bytes!("fixtures/bk_delete.json"),
        "bk_delete",
    );

    let on_disk = std::fs::read(dir.join("state.json")).expect("read state");
    matches(
        &on_disk,
        include_bytes!("fixtures/ttlbk_state.json"),
        "ttlbk_state",
    );
}

#[test]
fn ttl_expiry_removes_old_items() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    s.update_time_to_live(&UpdateTimeToLiveRequest {
        table_name: "T".to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "exp".to_string(),
            enabled: true,
        },
    })
    .expect("ttl");
    put(
        &mut s,
        item(&[("pk", json!({"S": "old"})), ("exp", json!({"N": "1"}))]),
    );
    put(
        &mut s,
        item(&[
            ("pk", json!({"S": "new"})),
            ("exp", json!({"N": "99999999999"})),
        ]),
    );
    // The HTTP layer runs expiry before each op; here we run it explicitly.
    s.expire_ttl_items(ORACLE_NOW).expect("expire");
    let body = s
        .scan(&ScanRequest {
            table_name: "T".to_string(),
            ..Default::default()
        })
        .expect("scan");
    matches(
        &body,
        include_bytes!("fixtures/ttl_expired_scan.json"),
        "ttl_expired_scan",
    );
}

#[test]
fn backup_errors() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    assert_eq!(
        s.create_backup(&CreateBackupRequest {
            table_name: "T".to_string(),
            backup_name: String::new(),
        })
        .expect_err("noname")
        .message,
        "backup name is required"
    );
    assert_eq!(
        s.describe_backup(&BackupArnRequest {
            backup_arn: "missing".to_string(),
        })
        .expect_err("nobackup")
        .name,
        "BackupNotFoundException"
    );
    assert_eq!(
        s.restore_table_from_backup(&RestoreTableFromBackupRequest {
            backup_arn: "missing".to_string(),
            target_table_name: "X".to_string(),
        })
        .expect_err("norestore")
        .name,
        "BackupNotFoundException"
    );
}

#[test]
fn restore_into_existing_table_is_in_use() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    s.create_backup(&CreateBackupRequest {
        table_name: "T".to_string(),
        backup_name: "snap".to_string(),
    })
    .expect("backup");
    let arn = format!("arn:aws:dynamodb:us-east-1:000000000000:table/T/backup/{ORACLE_NOW}-snap");
    let err = s
        .restore_table_from_backup(&RestoreTableFromBackupRequest {
            backup_arn: arn,
            target_table_name: "T".to_string(),
        })
        .expect_err("exists");
    assert_eq!(err.name, "ResourceInUseException");
}

#[test]
fn update_ttl_requires_attribute_when_enabled() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    let err = s
        .update_time_to_live(&UpdateTimeToLiveRequest {
            table_name: "T".to_string(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: String::new(),
                enabled: true,
            },
        })
        .expect_err("no attr");
    assert_eq!(
        err.message,
        "ttl attribute name is required when ttl is enabled"
    );
}

#[test]
fn backups_survive_reload() {
    let dir = tempdir();
    {
        let mut s = server(&dir);
        create_t(&mut s);
        s.create_backup(&CreateBackupRequest {
            table_name: "T".to_string(),
            backup_name: "snap".to_string(),
        })
        .expect("backup");
    }
    let s2 = server(&dir);
    let body = s2
        .list_backups(&ListBackupsRequest::default())
        .expect("list");
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["BackupSummaries"].as_array().unwrap().len(), 1);
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-ttlbk-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
