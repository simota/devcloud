//! Conditional GCS mutations against the object state shared with S3.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_s3::model::Object;
use devcloud_s3::objops::{PutObjectInput, UpdateObjectMetadataInput};
use devcloud_s3::store::FileBucketStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    gcs: Server,
    s3: FileBucketStore,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-conditional-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let mut gcs_store = FileBucketStore::new(root.join("buckets"));
        gcs_store.set_fixed_now("2026-10-12T02:00:00Z");
        let mut gcs = Server::new(
            Config {
                upload_session_path: root.join("sessions").to_string_lossy().into_owned(),
                ..Default::default()
            },
            gcs_store,
        );
        let created = perform(
            &mut gcs,
            "POST",
            "/storage/v1/b?project=devcloud",
            br#"{"name":"demo-bucket"}"#,
        );
        assert_eq!(created.status, 200);
        let mut s3 = FileBucketStore::new(root.join("buckets"));
        s3.set_fixed_now("2026-10-12T02:00:01Z");
        Self { root, gcs, s3 }
    }

    fn s3_put(&self, key: &str, body: &[u8]) -> Object {
        self.s3
            .put_object(PutObjectInput {
                bucket: "demo-bucket".to_string(),
                key: key.to_string(),
                body: body.to_vec(),
                content_type: "application/s3".to_string(),
                metadata: BTreeMap::from([("writer".to_string(), "s3".to_string())]),
                ..Default::default()
            })
            .unwrap()
    }

    fn object(&self, key: &str) -> (Object, Vec<u8>) {
        self.s3.get_object("demo-bucket", key).unwrap().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn perform(server: &mut Server, method: &str, target: &str, body: &[u8]) -> Response {
    let mut request = Request::new(method, target, body.to_vec());
    request
        .headers
        .insert("content-type".to_string(), "application/json".to_string());
    route(server, &request)
}

fn json(response: &Response) -> serde_json::Value {
    serde_json::from_slice(&response.body).unwrap()
}

#[test]
fn upload_copy_rewrite_and_compose_preserve_an_existing_s3_object() {
    let mut fixture = Fixture::new();
    fixture.s3_put("source", b"source data");

    for operation in ["media", "multipart", "copyTo", "rewriteTo", "compose"] {
        let key = format!("destination-{operation}");
        let expected = fixture.s3_put(&key, b"s3 winner");
        let (target, body) = match operation {
            "media" => (
                format!("/upload/storage/v1/b/demo-bucket/o?uploadType=media&name={key}&ifGenerationMatch=0"),
                b"source data".to_vec(),
            ),
            "multipart" => (
                format!("/upload/storage/v1/b/demo-bucket/o?uploadType=multipart&name={key}&ifGenerationMatch=0"),
                concat!(
                    "--boundary\r\nContent-Type: application/json\r\n\r\n{}\r\n",
                    "--boundary\r\nContent-Type: text/plain\r\n\r\nsource data\r\n",
                    "--boundary--\r\n",
                ).as_bytes().to_vec(),
            ),
            "compose" => (
                format!("/storage/v1/b/demo-bucket/o/{key}/compose?ifGenerationMatch=0"),
                br#"{"sourceObjects":[{"name":"source"}]}"#.to_vec(),
            ),
            _ => (
                format!("/storage/v1/b/demo-bucket/o/source/{operation}/b/demo-bucket/o/{key}?ifGenerationMatch=0"),
                b"{}".to_vec(),
            ),
        };
        let mut request = Request::new("POST", &target, body);
        request.headers.insert(
            "content-type".to_string(),
            if operation == "multipart" {
                "multipart/related; boundary=boundary"
            } else {
                "application/json"
            }
            .to_string(),
        );
        let response = route(&mut fixture.gcs, &request);
        assert_eq!(response.status, 412, "{operation}: {:?}", response);
        assert_eq!(
            fixture.object(&key),
            (expected, b"s3 winner".to_vec()),
            "{operation}"
        );

        // A matching condition must still permit the same operation.
        let current = perform(
            &mut fixture.gcs,
            "GET",
            &format!("/storage/v1/b/demo-bucket/o/{key}"),
            b"",
        );
        request.query.insert(
            "ifGenerationMatch".to_string(),
            json(&current)["generation"].as_str().unwrap().to_string(),
        );
        let allowed = route(&mut fixture.gcs, &request);
        assert_eq!(allowed.status, 200, "{operation}: {:?}", allowed);
        assert_eq!(fixture.object(&key).1, b"source data", "{operation}");
    }
}

#[test]
fn metadata_and_delete_conditions_preserve_a_newer_s3_generation() {
    let mut fixture = Fixture::new();
    let original = perform(
        &mut fixture.gcs,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=target",
        b"original",
    );
    assert_eq!(original.status, 200);
    let old_generation = json(&original)["generation"].as_str().unwrap().to_string();
    fixture.s3_put("target", b"s3 winner");
    let expected = fixture
        .s3
        .update_object_metadata(UpdateObjectMetadataInput {
            bucket: "demo-bucket".to_string(),
            key: "target".to_string(),
            content_type: "application/s3-new".to_string(),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
    assert_eq!(expected.metageneration, 2);
    let current = perform(
        &mut fixture.gcs,
        "GET",
        "/storage/v1/b/demo-bucket/o/target",
        b"",
    );
    let new_generation = json(&current)["generation"].as_str().unwrap().to_string();
    assert_ne!(old_generation, new_generation);

    for method in ["PATCH", "DELETE"] {
        for (query, status) in [
            (format!("generation={old_generation}"), 404),
            (format!("ifGenerationMatch={old_generation}"), 412),
            ("ifMetagenerationMatch=1".to_string(), 412),
            (
                format!("generation={new_generation}&ifMetagenerationMatch=1"),
                412,
            ),
            ("generation=bad".to_string(), 400),
            (
                format!("generation={new_generation}&ifMetagenerationMatch=bad"),
                400,
            ),
        ] {
            let response = perform(
                &mut fixture.gcs,
                method,
                &format!("/storage/v1/b/demo-bucket/o/target?{query}"),
                br#"{"contentType":"application/stale","metadata":{"writer":"stale"}}"#,
            );
            assert_eq!(response.status, status, "{method} {query}: {:?}", response);
            assert_eq!(
                fixture.object("target"),
                (expected.clone(), b"s3 winner".to_vec())
            );
        }
    }

    let patched = perform(
        &mut fixture.gcs,
        "PATCH",
        &format!("/storage/v1/b/demo-bucket/o/target?generation={new_generation}&ifMetagenerationMatch=2"),
        br#"{"contentType":"application/allowed"}"#,
    );
    assert_eq!(patched.status, 200);
    assert_eq!(json(&patched)["generation"], new_generation);
    assert_eq!(json(&patched)["metageneration"], "3");
    assert_eq!(fixture.object("target").1, b"s3 winner");
    let deleted = perform(
        &mut fixture.gcs,
        "DELETE",
        &format!("/storage/v1/b/demo-bucket/o/target?generation={new_generation}&ifMetagenerationMatch=3"),
        b"",
    );
    assert_eq!(deleted.status, 204);
    assert!(fixture
        .s3
        .get_object("demo-bucket", "target")
        .unwrap()
        .is_none());
}

#[test]
fn rejected_resumable_commit_preserves_the_checkpoint_and_can_be_retried() {
    let mut fixture = Fixture::new();
    let started = perform(
        &mut fixture.gcs,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=target&ifGenerationMatch=0",
        b"{}",
    );
    assert_eq!(started.status, 200);
    let target = started.headers["Location"].clone();
    let id = started.headers["X-GUploader-UploadID"].clone();
    let mut chunk = Request::new("PUT", &target, b"first".to_vec());
    chunk
        .headers
        .insert("content-range".to_string(), "bytes 0-4/10".to_string());
    assert_eq!(route(&mut fixture.gcs, &chunk).status, 308);

    let expected = fixture.s3_put("target", b"s3 winner");
    chunk.body = b"last!".to_vec();
    chunk
        .headers
        .insert("content-range".to_string(), "bytes 5-9/10".to_string());
    let rejected = route(&mut fixture.gcs, &chunk);
    assert_eq!(rejected.status, 412, "{rejected:?}");
    assert_eq!(fixture.object("target"), (expected, b"s3 winner".to_vec()));
    let session_dir = fixture.root.join("sessions").join(id);
    assert_eq!(fs::read(session_dir.join("body.part")).unwrap(), b"first");
    let checkpoint: serde_json::Value =
        serde_json::from_slice(&fs::read(session_dir.join("session.json")).unwrap()).unwrap();
    assert_eq!(checkpoint["ReceivedBytes"], 5);
    let mut progress = Request::new("PUT", &target, Vec::new());
    progress
        .headers
        .insert("content-range".to_string(), "bytes */10".to_string());
    let progress = route(&mut fixture.gcs, &progress);
    assert_eq!(progress.status, 308);
    assert_eq!(progress.headers["Range"], "bytes=0-4");

    fixture.s3.delete_object("demo-bucket", "target").unwrap();
    let committed = route(&mut fixture.gcs, &chunk);
    assert_eq!(committed.status, 200, "{committed:?}");
    assert_eq!(fixture.object("target").1, b"firstlast!");
    assert!(!session_dir.exists());
    let replay = route(&mut fixture.gcs, &chunk);
    assert_eq!(replay.status, 200);
    assert_eq!(replay.body, committed.body);
}

#[test]
fn self_copy_rewrite_and_compose_apply_source_and_destination_conditions() {
    let mut fixture = Fixture::new();
    for operation in ["copyTo", "rewriteTo", "compose"] {
        let key = format!("self-{operation}");
        fixture.s3_put(&key, b"self data");
        let current = perform(
            &mut fixture.gcs,
            "GET",
            &format!("/storage/v1/b/demo-bucket/o/{key}"),
            b"",
        );
        let generation = json(&current)["generation"].as_str().unwrap().to_string();
        let (target, body) = if operation == "compose" {
            (
                format!("/storage/v1/b/demo-bucket/o/{key}/compose?ifGenerationMatch={generation}"),
                format!(
                    r#"{{"sourceObjects":[{{"name":"{key}","generation":"{generation}","objectPreconditions":{{"ifGenerationMatch":"{generation}"}}}}]}}"#
                ),
            )
        } else {
            (
                format!("/storage/v1/b/demo-bucket/o/{key}/{operation}/b/demo-bucket/o/{key}?ifGenerationMatch={generation}&ifSourceGenerationMatch={generation}&ifSourceMetagenerationMatch=1"),
                "{}".to_string(),
            )
        };
        let response = perform(&mut fixture.gcs, "POST", &target, body.as_bytes());
        assert_eq!(response.status, 200, "{operation}: {response:?}");
        assert_eq!(fixture.object(&key).1, b"self data");

        // The prior source generation is now stale even if the destination
        // condition matches the replacement's current generation.
        let replaced = fixture.object(&key);
        let current = perform(
            &mut fixture.gcs,
            "GET",
            &format!("/storage/v1/b/demo-bucket/o/{key}"),
            b"",
        );
        let new_generation = json(&current)["generation"].as_str().unwrap().to_string();
        assert_ne!(generation, new_generation);
        let mut stale = Request::new("POST", &target, body.into_bytes());
        stale
            .query
            .insert("ifGenerationMatch".to_string(), new_generation);
        let rejected = route(&mut fixture.gcs, &stale);
        assert_eq!(rejected.status, 412, "{operation}: {rejected:?}");
        assert_eq!(fixture.object(&key), replaced);
    }
}
