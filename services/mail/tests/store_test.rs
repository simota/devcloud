//! Parity port of `internal/storage/mailstore/store_test.rs`, plus byte-exact
//! golden-oracle assertions captured from the legacy `FileStore` so the on-disk
//! format stays interoperable with the legacy dashboard during the strangler-fig
//! window.

use std::sync::Arc;

use devcloud_mail::{BlobStore, FileBlobStore, FileStore, ListMessagesInput, Message, Store};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("devcloud-mail-test-{tag}-{nanos}-{n}"));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn new_store(tag: &str) -> (FileStore, std::path::PathBuf) {
    let blob_dir = temp_dir(&format!("{tag}-blob"));
    let msg_dir = temp_dir(&format!("{tag}-msg"));
    let blobs: Arc<dyn BlobStore> = Arc::new(FileBlobStore::new(&blob_dir));
    (FileStore::new(&msg_dir, blobs), blob_dir)
}

fn fixed_msg(id: &str, to: &str, subject: &str, received: &str) -> Message {
    Message {
        id: id.to_string(),
        from: "a@example.com".to_string(),
        to: vec![to.to_string()],
        subject: subject.to_string(),
        received_at: Some(received.to_string()),
        ..Default::default()
    }
}

#[test]
fn file_store_append_list_get_raw_and_delete() {
    let (store, _) = new_store("alg");
    let msg = fixed_msg("msg_test", "b@example.com", "Hello", "2026-04-30T10:00:00Z");

    store
        .append(msg, b"Subject: Hello\r\n\r\nBody")
        .expect("append");

    let list = store.list(ListMessagesInput::default()).expect("list");
    assert_eq!(list.messages.len(), 1);
    assert_eq!(list.messages[0].id, "msg_test");

    let raw = store.get_raw("msg_test").expect("get_raw");
    assert_eq!(raw.as_deref(), Some(&b"Subject: Hello\r\n\r\nBody"[..]));

    store.delete("msg_test").expect("delete");
    assert!(store.get("msg_test").expect("get").is_none());
}

#[test]
fn file_store_list_returns_empty_slice_when_log_missing() {
    let (store, _) = new_store("empty");
    let list = store.list(ListMessagesInput::default()).expect("list");
    assert_eq!(list.messages.len(), 0);

    // legacy TestFileStoreListReturnsEmptySliceWhenLogMissing requires the result
    // to serialize `messages` as `[]`, never `null`.
    let encoded = serde_json::to_string(&list).unwrap();
    assert!(
        encoded.contains(r#""messages":[]"#),
        "encoded list = {encoded}, want messages serialized as []"
    );
}

#[test]
fn file_store_delete_all_preserves_tombstones() {
    let (store, _) = new_store("delall");
    for (id, to, subj, ts) in [
        ("msg_one", "one@example.com", "One", "2026-04-30T10:00:00Z"),
        ("msg_two", "two@example.com", "Two", "2026-04-30T10:01:00Z"),
    ] {
        store
            .append(
                fixed_msg(id, to, subj, ts),
                format!("Subject: {subj}\r\n\r\nBody").as_bytes(),
            )
            .expect("append");
    }

    store.delete("msg_one").expect("delete");
    store.delete_all().expect("delete_all");

    let list = store.list(ListMessagesInput::default()).expect("list");
    assert_eq!(list.messages.len(), 0);

    // The log must retain two tombstoned records (no physical deletion).
    let data = std::fs::read_to_string(store.messages_path()).expect("read log");
    let lines: Vec<&str> = data.trim().split('\n').collect();
    assert_eq!(lines.len(), 2, "log line count; data = {data:?}");
    for line in lines {
        let m: Message = serde_json::from_str(line).expect("decode line");
        assert!(m.deleted_at.is_some(), "{} not tombstoned", m.id);
    }
}

#[test]
fn file_store_concurrent_append_and_list_does_not_read_partial_metadata() {
    let (store, _) = new_store("conc");
    let store = Arc::new(store);

    let mut handles = Vec::new();
    for i in 0..40 {
        let s_w = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let ts = format!("2026-04-30T10:{:02}:00Z", i);
            let msg = fixed_msg(
                &format!("msg_{:02}", i),
                "b@example.com",
                &format!("Hello {:02}", i),
                &ts,
            );
            s_w.append(
                msg,
                format!("Subject: Hello {:02}\r\n\r\nBody", i).as_bytes(),
            )
            .expect("append");
        }));
        let s_r = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            s_r.list(ListMessagesInput::default()).expect("list");
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }

    let list = store.list(ListMessagesInput::default()).expect("list");
    assert_eq!(list.messages.len(), 40);
}

// --- Byte-exact golden oracle (captured from the legacy FileStore) ---

#[test]
fn file_store_jsonl_matches_legacy_byte_for_byte() {
    let (store, blob_dir) = new_store("oracle");
    let msg = fixed_msg("msg_test", "b@example.com", "Hello", "2026-04-30T10:00:00Z");
    store
        .append(msg, b"Subject: Hello\r\n\r\nBody")
        .expect("append");

    // legacy golden line (note trailing newline appended by the encoder).
    let want_line = concat!(
        r#"{"id":"msg_test","from":"a@example.com","to":["b@example.com"],"#,
        r#""subject":"Hello","#,
        r#""raw":"987dd1b2a81f2dfc561f46a19e1c2cb87951e618b0b8bb24728b45dffa71a085","#,
        r#""receivedAt":"2026-04-30T10:00:00Z"}"#,
        "\n"
    );
    let got = std::fs::read_to_string(store.messages_path()).expect("read log");
    assert_eq!(
        got, want_line,
        "JSONL must match legacy encoding/json byte-for-byte"
    );

    // Blob is sharded by the legacy content-addressed scheme: <a>/<b>/<id>.blob.
    let want_blob = blob_dir
        .join("98")
        .join("7d")
        .join("987dd1b2a81f2dfc561f46a19e1c2cb87951e618b0b8bb24728b45dffa71a085.blob");
    assert!(
        want_blob.exists(),
        "blob not at legacy shard path: {want_blob:?}"
    );
    assert_eq!(
        std::fs::read(&want_blob).unwrap(),
        b"Subject: Hello\r\n\r\nBody"
    );
}
