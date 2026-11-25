//! GCS bucket retention policies (including lockRetentionPolicy), object holds,
//! per-object retention locks, and bucket lifecycle rules (Delete + SetStorageClass).

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_s3::store::FileBucketStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    server: Server,
}

impl Fixture {
    fn new(now: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-ret-lc-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let mut store = FileBucketStore::new(root.join("buckets"));
        store.set_fixed_now(now);
        let server = Server::new(
            Config {
                project: "devcloud".to_string(),
                location: "US".to_string(),
                ..Default::default()
            },
            store,
        );
        Self { root, server }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn call(server: &mut Server, method: &str, target: &str, body: &str) -> Response {
    let mut req = Request::new(method, target, body.as_bytes().to_vec());
    if !body.is_empty() {
        req.headers
            .insert("content-type".to_string(), "application/json".to_string());
    }
    route(server, &req)
}

fn json(r: &Response) -> serde_json::Value {
    serde_json::from_slice(&r.body).unwrap()
}

#[test]
fn bucket_retention_policy_lock_and_expiry_guards() {
    let mut f = Fixture::new("2026-01-01T00:00:00Z");

    let created = call(
        &mut f.server,
        "POST",
        "/storage/v1/b",
        r#"{"name":"ret-bucket","retentionPolicy":{"retentionPeriod":"3600"}}"#,
    );
    assert_eq!(created.status, 200);
    let b = json(&created);
    assert_eq!(b["retentionPolicy"]["retentionPeriod"], "3600");
    assert_eq!(
        b["retentionPolicy"]["effectiveTime"],
        "2026-01-01T00:00:00Z"
    );
    assert_eq!(b["metageneration"], "1");

    // Upload object; it gets retentionExpirationTime = created + 3600s.
    let up = call(
        &mut f.server,
        "POST",
        "/upload/storage/v1/b/ret-bucket/o?uploadType=media&name=audit.log",
        "immutable",
    );
    assert_eq!(up.status, 200);
    assert_eq!(json(&up)["retentionExpirationTime"], "2026-01-01T01:00:00Z");

    // Delete and overwrite are blocked before expiry (both JSON and XML).
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/ret-bucket/o/audit.log",
            ""
        )
        .status,
        403
    );
    assert_eq!(
        call(&mut f.server, "DELETE", "/ret-bucket/audit.log", "").status,
        403
    );
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/upload/storage/v1/b/ret-bucket/o?uploadType=media&name=audit.log",
            "tampered",
        )
        .status,
        403
    );

    // Lock retention policy requires matching ifMetagenerationMatch.
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b/ret-bucket/lockRetentionPolicy",
            ""
        )
        .status,
        400
    );
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b/ret-bucket/lockRetentionPolicy?ifMetagenerationMatch=99",
            "",
        )
        .status,
        412
    );
    let locked = call(
        &mut f.server,
        "POST",
        "/storage/v1/b/ret-bucket/lockRetentionPolicy?ifMetagenerationMatch=1",
        "",
    );
    assert_eq!(locked.status, 200);
    assert_eq!(json(&locked)["retentionPolicy"]["isLocked"], true);
    assert_eq!(json(&locked)["metageneration"], "2");

    // Locked retention policy cannot be removed or reduced, only extended.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/ret-bucket",
            r#"{"retentionPolicy":null}"#,
        )
        .status,
        403
    );
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/ret-bucket",
            r#"{"retentionPolicy":{"retentionPeriod":"1800"}}"#,
        )
        .status,
        403
    );
    let extended = call(
        &mut f.server,
        "PATCH",
        "/storage/v1/b/ret-bucket",
        r#"{"retentionPolicy":{"retentionPeriod":"7200"}}"#,
    );
    assert_eq!(extended.status, 200);
    assert_eq!(
        json(&extended)["retentionPolicy"]["retentionPeriod"],
        "7200"
    );
    assert_eq!(json(&extended)["retentionPolicy"]["isLocked"], true);

    // Still locked at +3601s because bucket policy was extended to 7200s.
    f.server.set_fixed_now("2026-01-01T01:00:01Z");
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/ret-bucket/o/audit.log",
            ""
        )
        .status,
        403
    );

    // Advance past 7200s -> delete succeeds and empty bucket with GCS sidecars can be deleted!
    f.server.set_fixed_now("2026-01-01T02:00:01Z");
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/ret-bucket/o/audit.log",
            ""
        )
        .status,
        204
    );
    assert_eq!(
        call(&mut f.server, "DELETE", "/storage/v1/b/ret-bucket", "").status,
        204
    );
}

#[test]
fn object_holds_and_per_object_retention_lock() {
    let mut f = Fixture::new("2026-01-01T00:00:00Z");

    let created = call(
        &mut f.server,
        "POST",
        "/storage/v1/b?enableObjectRetention=true",
        r#"{"name":"hold-bucket","defaultEventBasedHold":true,"retentionPolicy":{"retentionPeriod":"600"}}"#,
    );
    assert_eq!(created.status, 200);
    assert_eq!(json(&created)["defaultEventBasedHold"], true);
    assert_eq!(json(&created)["objectRetention"]["mode"], "Enabled");

    // New object inherits eventBasedHold=true.
    let up = call(
        &mut f.server,
        "POST",
        "/upload/storage/v1/b/hold-bucket/o?uploadType=media&name=doc.txt",
        "evidence",
    );
    assert_eq!(up.status, 200);
    assert_eq!(json(&up)["eventBasedHold"], true);

    // Even after 1 hour, eventBasedHold still blocks delete.
    f.server.set_fixed_now("2026-01-01T01:00:00Z");
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            ""
        )
        .status,
        403
    );

    // Releasing eventBasedHold recalculates retentionExpirationTime = now + 600s!
    let released = call(
        &mut f.server,
        "PATCH",
        "/storage/v1/b/hold-bucket/o/doc.txt",
        r#"{"eventBasedHold":false}"#,
    );
    assert_eq!(released.status, 200);
    assert_eq!(
        json(&released)["retentionExpirationTime"],
        "2026-01-01T01:10:00Z"
    );

    // Still blocked at 01:05, unlocked at 01:11.
    f.server.set_fixed_now("2026-01-01T01:05:00Z");
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            ""
        )
        .status,
        403
    );
    f.server.set_fixed_now("2026-01-01T01:11:00Z");

    // Set temporaryHold -> blocks delete again until cleared.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            r#"{"temporaryHold":true}"#,
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            ""
        )
        .status,
        403
    );
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            r#"{"temporaryHold":false}"#,
        )
        .status,
        200
    );

    // Set per-object Unlocked retention.
    let set_ret = call(
        &mut f.server,
        "PATCH",
        "/storage/v1/b/hold-bucket/o/doc.txt",
        r#"{"retention":{"mode":"Unlocked","retainUntilTime":"2026-01-02T00:00:00Z"}}"#,
    );
    assert_eq!(set_ret.status, 200);
    assert_eq!(json(&set_ret)["retention"]["mode"], "Unlocked");

    // Cannot delete or clear Unlocked retention without overrideUnlockedRetention=true.
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            ""
        )
        .status,
        403
    );
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            r#"{"retention":null}"#,
        )
        .status,
        403
    );

    // Clearing with overrideUnlockedRetention=true succeeds.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt?overrideUnlockedRetention=true",
            r#"{"retention":null}"#,
        )
        .status,
        200
    );

    // Upgrade to Locked retention -> cannot downgrade or clear even with overrideUnlockedRetention=true.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            r#"{"retention":{"mode":"Locked","retainUntilTime":"2026-01-03T00:00:00Z"}}"#,
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/hold-bucket/o/doc.txt?overrideUnlockedRetention=true",
            r#"{"retention":{"mode":"Unlocked","retainUntilTime":"2026-01-03T00:00:00Z"}}"#,
        )
        .status,
        403
    );

    // Advance past retainUntilTime -> delete succeeds.
    f.server.set_fixed_now("2026-01-03T00:00:01Z");
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/hold-bucket/o/doc.txt",
            ""
        )
        .status,
        204
    );
}

#[test]
fn lifecycle_rules_delete_and_set_storage_class_json_and_xml() {
    let mut f = Fixture::new("2026-01-01T00:00:00Z");

    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            r#"{"name":"lc-bucket"}"#
        )
        .status,
        200
    );
    for key in [
        "tmp/old.log",
        "tmp/held.log",
        "archive/data.csv",
        "keep/live.txt",
    ] {
        assert_eq!(
            call(
                &mut f.server,
                "POST",
                &format!("/upload/storage/v1/b/lc-bucket/o?uploadType=media&name={key}"),
                "payload",
            )
            .status,
            200
        );
    }
    // Place temporary hold on tmp/held.log so lifecycle Delete must skip it.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/lc-bucket/o/tmp%2Fheld.log",
            r#"{"temporaryHold":true}"#,
        )
        .status,
        200
    );

    // Configure lifecycle rules via JSON PATCH.
    let patch = call(
        &mut f.server,
        "PATCH",
        "/storage/v1/b/lc-bucket",
        r#"{
            "lifecycle": {
                "rule": [
                    {
                        "action": {"type": "Delete"},
                        "condition": {"age": 7, "matchesPrefix": ["tmp/"], "matchesSuffix": [".log"]}
                    },
                    {
                        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
                        "condition": {"age": 7, "matchesPrefix": ["archive/"], "matchesStorageClass": ["STANDARD"]}
                    }
                ]
            }
        }"#,
    );
    assert_eq!(patch.status, 200);

    // XML ?lifecycle reflects the same rules.
    let xml_lc = call(&mut f.server, "GET", "/lc-bucket?lifecycle", "");
    assert_eq!(xml_lc.status, 200);
    let xml_str = String::from_utf8_lossy(&xml_lc.body);
    assert!(xml_str.contains("<Delete></Delete>"));
    assert!(xml_str.contains("storageClass=\"COLDLINE\""));

    // At day 6: nothing expires or transitions yet.
    f.server.set_fixed_now("2026-01-07T00:00:00Z");
    // Wait: Jan 1 + 6 days = Jan 7! Let's check Jan 6 (day 5) first.
    f.server.set_fixed_now("2026-01-06T00:00:00Z");
    let before = call(&mut f.server, "GET", "/storage/v1/b/lc-bucket/o", "");
    assert_eq!(before.status, 200);
    assert_eq!(json(&before)["items"].as_array().unwrap().len(), 4);

    // At day 8 (Jan 9): tmp/old.log is deleted, tmp/held.log is preserved (held),
    // archive/data.csv transitions to COLDLINE, keep/live.txt remains STANDARD.
    f.server.set_fixed_now("2026-01-09T00:00:00Z");
    let after = call(&mut f.server, "GET", "/storage/v1/b/lc-bucket/o", "");
    assert_eq!(after.status, 200);
    let items = json(&after)["items"].as_array().unwrap().clone();
    let names: Vec<&str> = items.iter().map(|o| o["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec!["archive/data.csv", "keep/live.txt", "tmp/held.log"]
    );
    assert_eq!(items[0]["storageClass"], "COLDLINE");
    assert_eq!(items[1]["storageClass"], "STANDARD");

    // XML PUT ?lifecycle with CreatedBefore deletes keep/live.txt.
    let put_xml = call(
        &mut f.server,
        "PUT",
        "/lc-bucket?lifecycle",
        r#"<LifecycleConfiguration><Rule><Action><Delete/></Action><Condition><CreatedBefore>2026-01-02</CreatedBefore><MatchesPrefix><Prefix>keep/</Prefix></MatchesPrefix></Condition></Rule></LifecycleConfiguration>"#,
    );
    assert_eq!(put_xml.status, 200);

    let missing_keep = call(&mut f.server, "GET", "/lc-bucket/keep/live.txt", "");
    assert_eq!(missing_keep.status, 404);
}
