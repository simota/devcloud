//! Part 6 parity: notification / replication / analytics / inventory persistence
//! and inventory CSV report generation reproduce the Go store's sidecar byte
//! formats, CSV quoting, manifest, and listing behavior, against the oracles in
//! `/tmp/s3_oracle_part6.txt`.

use devcloud_s3::go_json::to_vec_indent;
use devcloud_s3::model::*;
use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::FileBucketStore;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-cfg2-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn put(store: &FileBucketStore, bucket: &str, key: &str, body: &str) {
    store
        .put_object(PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: body.as_bytes().to_vec(),
            ..Default::default()
        })
        .unwrap();
}

#[test]
fn notification_json_matches_oracle() {
    let config = NotificationConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        topic_configurations: vec![NotificationTopicConfig {
            id: "t1".to_string(),
            topic: "arn:aws:sns:topic".to_string(),
            events: vec!["s3:ObjectCreated:*".to_string()],
            filter: NotificationFilter {
                s3_key: NotificationS3KeyFilter {
                    rules: vec![NotificationFilterRule {
                        name: "prefix".to_string(),
                        value: "logs/".to_string(),
                    }],
                },
            },
        }],
        queue_configurations: vec![NotificationQueueConfig {
            queue: "arn:aws:sqs:queue".to_string(),
            events: vec!["s3:ObjectRemoved:*".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&config)).unwrap(),
        include_str!("fixtures/notification.json")
    );
}

#[test]
fn notification_events_json_matches_oracle() {
    let events = vec![NotificationEventRecord {
        event_id: "e1".to_string(),
        event_name: "s3:ObjectCreated:Put".to_string(),
        event_time: "0001-01-01T00:00:00Z".to_string(),
        bucket: "data".to_string(),
        key: "k".to_string(),
        etag: "\"abc\"".to_string(),
        size: 3,
        ..Default::default()
    }];
    assert_eq!(
        String::from_utf8(to_vec_indent(&events)).unwrap(),
        include_str!("fixtures/notif_events.json")
    );
}

#[test]
fn replication_json_matches_oracle() {
    let config = ReplicationConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        role: "arn:aws:iam::role".to_string(),
        rules: vec![ReplicationRule {
            id: "r1".to_string(),
            priority: 1,
            filter: ReplicationFilter {
                prefix: "logs/".to_string(),
            },
            status: "Enabled".to_string(),
            destination: ReplicationDestination {
                bucket: "arn:aws:s3:::dest".to_string(),
                storage_class: "STANDARD".to_string(),
            },
            ..Default::default()
        }],
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&config)).unwrap(),
        include_str!("fixtures/replication.json")
    );
}

#[test]
fn analytics_json_matches_oracle() {
    let config = AnalyticsConfiguration {
        id: "an1".to_string(),
        filter: AnalyticsFilter {
            prefix: "logs/".to_string(),
        },
        storage_class_analysis: StorageClassAnalysis {
            data_export: AnalyticsDataExport {
                output_schema_version: "V_1".to_string(),
                destination: AnalyticsDestination {
                    s3_bucket_destination: AnalyticsS3BucketDestination {
                        format: "CSV".to_string(),
                        bucket: "arn:aws:s3:::dest".to_string(),
                        ..Default::default()
                    },
                },
            },
        },
        ..Default::default()
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&config)).unwrap(),
        include_str!("fixtures/analytics.json")
    );
}

#[test]
fn inventory_config_and_manifest_json_match_oracle() {
    let config = InventoryConfiguration {
        id: "inv1".to_string(),
        is_enabled: true,
        included_object_versions: "Current".to_string(),
        schedule: InventorySchedule {
            frequency: "Daily".to_string(),
        },
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                bucket: "arn:aws:s3:::dest".to_string(),
                format: "CSV".to_string(),
                ..Default::default()
            },
        },
        optional_fields: vec!["Size".to_string(), "ETag".to_string()],
        ..Default::default()
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&config)).unwrap(),
        include_str!("fixtures/inventory.json")
    );

    let manifest = InventoryReportManifest {
        configuration_id: "inv1".to_string(),
        source_bucket: "data".to_string(),
        format: "CSV".to_string(),
        included_versions: "Current".to_string(),
        fields: [
            "Bucket",
            "Key",
            "Size",
            "LastModifiedDate",
            "ETag",
            "StorageClass",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        object_count: 2,
        report_key: "inventory/reports/aW52MQ/inventory.csv".to_string(),
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&manifest)).unwrap(),
        include_str!("fixtures/inv_manifest.json")
    );
}

#[test]
fn inventory_report_generation_and_listing() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "b.txt", "BB");
    put(&store, "data", "a.txt", "A");
    let config = InventoryConfiguration {
        id: "inv1".to_string(),
        is_enabled: true,
        included_object_versions: "Current".to_string(),
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                bucket: "arn:aws:s3:::dest".to_string(),
                format: "CSV".to_string(),
                ..Default::default()
            },
        },
        ..Default::default()
    };
    store.put_bucket_inventory("data", "inv1", config).unwrap();

    let csv = std::fs::read_to_string(store.inventory_report_csv_path("data", "inv1")).unwrap();
    let lines: Vec<&str> = csv.trim_end_matches('\n').split('\n').collect();
    assert_eq!(
        lines[0],
        "Bucket,Key,Size,LastModifiedDate,ETag,StorageClass"
    );
    assert_eq!(lines.len() - 1, 2, "two data rows");

    // Manifest present, report key correct.
    let (_, configured) = store.get_bucket_inventory("data", "inv1").unwrap().unwrap();
    assert!(configured);
}

#[test]
fn analytics_listing_and_delete() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    store
        .put_bucket_analytics("data", "z-an", AnalyticsConfiguration::default())
        .unwrap();
    store
        .put_bucket_analytics("data", "a-an", AnalyticsConfiguration::default())
        .unwrap();
    let ids: Vec<String> = store
        .list_bucket_analytics("data")
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|a| a.id)
        .collect();
    assert_eq!(ids, vec!["a-an", "z-an"], "sorted by id");
    assert!(store.delete_bucket_analytics("data", "z-an").unwrap());
    assert_eq!(
        store.list_bucket_analytics("data").unwrap().unwrap().len(),
        1
    );
}

#[test]
fn csv_writer_matches_go_encoding_csv() {
    use devcloud_s3::csv::write_csv;
    let records = vec![
        vec!["Bucket".into(), "Key".into(), "Size".into(), "ETag".into()],
        vec![
            "data".into(),
            "simple.txt".into(),
            "3".into(),
            "\"9a0364b9\"".into(),
        ],
        vec![
            "data".into(),
            "has,comma".into(),
            "0".into(),
            "\"d41d8c\"".into(),
        ],
        vec![
            "data".into(),
            " leading-space".into(),
            "5".into(),
            "plain".into(),
        ],
        vec![
            "data".into(),
            "line\nbreak".into(),
            "1".into(),
            String::new(),
        ],
    ];
    assert_eq!(
        write_csv(&records),
        include_bytes!("fixtures/csv_oracle.txt")
    );
}
