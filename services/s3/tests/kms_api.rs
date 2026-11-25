//! Tests for the local KMS JSON API (`X-Amz-Target: TrentService.*` at `POST /`).

use devcloud_s3::http::{route, route_with_auth, AuthConfig, Request, Response};
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::time_fmt::rfc3339_seconds_from_unix;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const OWNER_KEY: &str = "kms-owner";
const OWNER_SECRET: &str = "kms-owner-secret";
const ALICE_KEY: &str = "AKIAALICEKMS";
const ALICE_SECRET: &str = "alice-kms-secret";
const BOB_KEY: &str = "AKIABOBKMS";
const BOB_SECRET: &str = "bob-kms-secret";

fn store() -> FileBucketStore {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-s3-kms-api-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    FileBucketStore::new(dir)
}

fn strict() -> AuthConfig {
    AuthConfig {
        auth_mode: "strict".to_string(),
        access_key_id: OWNER_KEY.to_string(),
        secret_access_key: OWNER_SECRET.to_string(),
        region: "us-east-1".to_string(),
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// SigV4-signs `method target` for `service` in `us-east-1`, signing every header.
fn sign(
    service: &str,
    key: &str,
    secret: &str,
    method: &str,
    target: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Request {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let date = rfc3339_seconds_from_unix(now).replace(['-', ':'], "");
    let date_stamp = &date[..8];
    let payload_hash = hex::encode(Sha256::digest(body));
    let mut req = Request::new(method, target, body.to_vec());
    req.headers
        .insert("host".to_string(), "s3.local:9000".to_string());
    req.headers.insert("x-amz-date".to_string(), date.clone());
    req.headers
        .insert("x-amz-content-sha256".to_string(), payload_hash.clone());
    for (k, v) in headers {
        req.headers.insert((*k).to_string(), (*v).to_string());
    }
    let signed: String = req
        .headers
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = req
        .headers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let canonical = [
        method,
        target,
        "",
        &canonical_headers,
        &signed,
        &payload_hash,
    ]
    .join("\n");
    let scope = format!("{date_stamp}/us-east-1/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k = hmac(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k = hmac(&k, b"us-east-1");
    let k = hmac(&k, service.as_bytes());
    let k = hmac(&k, b"aws4_request");
    let signature = hex::encode(hmac(&k, to_sign.as_bytes()));
    req.headers.insert(
        "authorization".to_string(),
        format!("AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={signed}, Signature={signature}"),
    );
    req
}

fn kms_request(operation: &str, input: &Value) -> (String, Vec<u8>) {
    (
        format!("TrentService.{operation}"),
        input.to_string().into_bytes(),
    )
}

const JSON: (&str, &str) = ("content-type", "application/x-amz-json-1.1");

fn decode(response: Response) -> (u16, Value) {
    let value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    (response.status, value)
}

/// Calls the KMS API unsigned in relaxed mode.
fn relaxed(store: &FileBucketStore, operation: &str, input: Value) -> (u16, Value) {
    let (target, body) = kms_request(operation, &input);
    let mut req = Request::new("POST", "/", body);
    req.headers.insert("x-amz-target".to_string(), target);
    req.headers.insert(JSON.0.to_string(), JSON.1.to_string());
    decode(route(store, &req))
}

/// Calls the KMS API in strict mode, signed for `kms` as `creds`.
fn signed(
    store: &FileBucketStore,
    creds: (&str, &str),
    operation: &str,
    input: Value,
) -> (u16, Value) {
    let (target, body) = kms_request(operation, &input);
    let req = sign(
        "kms",
        creds.0,
        creds.1,
        "POST",
        "/",
        &body,
        &[JSON, ("x-amz-target", &target)],
    );
    decode(route_with_auth(store, &req, &strict()))
}

fn b64(value: &Value) -> &str {
    value.as_str().unwrap()
}

#[test]
fn keys_encrypt_decrypt_and_bind_the_encryption_context() {
    let store = store();
    let (status, created) = relaxed(&store, "CreateKey", json!({"Description": "app"}));
    assert_eq!(status, 200, "{created}");
    let meta = &created["KeyMetadata"];
    let key_id = meta["KeyId"].as_str().unwrap().to_string();
    let arn = meta["Arn"].as_str().unwrap().to_string();
    assert_eq!(
        arn,
        format!("arn:aws:kms:us-east-1:000000000000:key/{key_id}")
    );
    assert_eq!(meta["KeyState"], "Enabled");
    assert_eq!(meta["Enabled"], true);
    assert_eq!(meta["KeySpec"], "SYMMETRIC_DEFAULT");
    assert!(meta["CreationDate"].as_i64().unwrap() > 1_700_000_000);

    // "hello kms" in base64.
    let context = json!({"tenant": "a", "purpose": "test"});
    let (status, encrypted) = relaxed(
        &store,
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": "aGVsbG8ga21z", "EncryptionContext": context}),
    );
    assert_eq!(status, 200, "{encrypted}");
    assert_eq!(encrypted["KeyId"], arn);
    let blob = b64(&encrypted["CiphertextBlob"]).to_string();
    assert_ne!(blob, "aGVsbG8ga21z");

    // Decrypt finds the key from the blob; the context must match exactly.
    let (status, decrypted) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": blob, "EncryptionContext": {"purpose": "test", "tenant": "a"}}),
    );
    assert_eq!(status, 200, "{decrypted}");
    assert_eq!(decrypted["Plaintext"], "aGVsbG8ga21z");
    assert_eq!(decrypted["KeyId"], arn);
    let (status, wrong) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": blob, "EncryptionContext": {"tenant": "b", "purpose": "test"}}),
    );
    assert_eq!(status, 400);
    assert_eq!(wrong["__type"], "InvalidCiphertextException");
    let (_, missing) = relaxed(&store, "Decrypt", json!({"CiphertextBlob": blob}));
    assert_eq!(missing["__type"], "InvalidCiphertextException");
    let (_, garbage) = relaxed(&store, "Decrypt", json!({"CiphertextBlob": "AAAA"}));
    assert_eq!(garbage["__type"], "InvalidCiphertextException");

    // A second key: aliases, IncorrectKeyException, ReEncrypt.
    let (_, other) = relaxed(&store, "CreateKey", json!({}));
    let other_id = other["KeyMetadata"]["KeyId"].as_str().unwrap().to_string();
    let (status, _) = relaxed(
        &store,
        "CreateAlias",
        json!({"AliasName": "alias/other", "TargetKeyId": other_id}),
    );
    assert_eq!(status, 200);
    let (_, dup) = relaxed(
        &store,
        "CreateAlias",
        json!({"AliasName": "alias/other", "TargetKeyId": key_id}),
    );
    assert_eq!(dup["__type"], "AlreadyExistsException");
    let (_, reserved) = relaxed(
        &store,
        "CreateAlias",
        json!({"AliasName": "alias/aws/mine", "TargetKeyId": key_id}),
    );
    assert_eq!(reserved["__type"], "ValidationException");
    let (_, incorrect) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": blob, "KeyId": "alias/other", "EncryptionContext": context}),
    );
    assert_eq!(incorrect["__type"], "IncorrectKeyException");
    let (status, re) = relaxed(
        &store,
        "ReEncrypt",
        json!({"CiphertextBlob": blob, "SourceEncryptionContext": context, "DestinationKeyId": "alias/other"}),
    );
    assert_eq!(status, 200, "{re}");
    assert_eq!(re["SourceKeyId"], arn);
    let (_, again) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": re["CiphertextBlob"], "KeyId": "alias/other"}),
    );
    assert_eq!(again["Plaintext"], "aGVsbG8ga21z");

    // Data keys: the plaintext decrypts from the returned blob.
    let (status, data_key) = relaxed(
        &store,
        "GenerateDataKey",
        json!({"KeyId": "alias/other", "KeySpec": "AES_256"}),
    );
    assert_eq!(status, 200, "{data_key}");
    let plain = b64(&data_key["Plaintext"]).to_string();
    let (_, opened) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": data_key["CiphertextBlob"]}),
    );
    assert_eq!(opened["Plaintext"], plain);
    assert_eq!(plain.len(), 44);
    let (_, no_plain) = relaxed(
        &store,
        "GenerateDataKeyWithoutPlaintext",
        json!({"KeyId": other_id, "NumberOfBytes": 16}),
    );
    assert!(no_plain.get("Plaintext").is_none() && no_plain["CiphertextBlob"].is_string());
    let (_, both) = relaxed(
        &store,
        "GenerateDataKey",
        json!({"KeyId": other_id, "KeySpec": "AES_256", "NumberOfBytes": 32}),
    );
    assert_eq!(both["__type"], "ValidationException");

    // Disabled and pending-deletion keys refuse cryptography.
    let (status, empty) = relaxed(&store, "DisableKey", json!({"KeyId": key_id}));
    assert_eq!((status, empty), (200, json!({})));
    let (_, disabled) = relaxed(
        &store,
        "Decrypt",
        json!({"CiphertextBlob": blob, "EncryptionContext": context}),
    );
    assert_eq!(disabled["__type"], "DisabledException");
    relaxed(&store, "EnableKey", json!({"KeyId": key_id}));
    let (status, scheduled) = relaxed(
        &store,
        "ScheduleKeyDeletion",
        json!({"KeyId": key_id, "PendingWindowInDays": 7}),
    );
    assert_eq!(status, 200, "{scheduled}");
    assert_eq!(scheduled["KeyState"], "PendingDeletion");
    assert_eq!(scheduled["PendingWindowInDays"], 7);
    let (_, pending) = relaxed(
        &store,
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": "aGVsbG8="}),
    );
    assert_eq!(pending["__type"], "KMSInvalidStateException");
    let (_, short) = relaxed(
        &store,
        "ScheduleKeyDeletion",
        json!({"KeyId": other_id, "PendingWindowInDays": 3}),
    );
    assert_eq!(short["__type"], "ValidationException");
    let (_, cancelled) = relaxed(&store, "CancelKeyDeletion", json!({"KeyId": key_id}));
    assert_eq!(cancelled["KeyId"], arn);
    let (_, described) = relaxed(&store, "DescribeKey", json!({"KeyId": arn}));
    assert_eq!(described["KeyMetadata"]["KeyState"], "Disabled");

    // Listing, alias filtering and deletion; key material never leaves the store.
    let (_, keys) = relaxed(&store, "ListKeys", json!({}));
    assert_eq!(keys["Keys"].as_array().unwrap().len(), 2);
    let (_, aliases) = relaxed(&store, "ListAliases", json!({"KeyId": other_id}));
    assert_eq!(aliases["Aliases"][0]["AliasName"], "alias/other");
    let (status, _) = relaxed(&store, "DeleteAlias", json!({"AliasName": "alias/other"}));
    assert_eq!(status, 200);
    let (_, gone) = relaxed(&store, "DeleteAlias", json!({"AliasName": "alias/other"}));
    assert_eq!(gone["__type"], "NotFoundException");
    let (_, unknown) = relaxed(&store, "DescribeKey", json!({"KeyId": "alias/nope"}));
    assert_eq!(unknown["__type"], "NotFoundException");
    let (_, op) = relaxed(&store, "Sign", json!({}));
    assert_eq!(op["__type"], "UnknownOperationException");
    let (_, asym) = relaxed(&store, "CreateKey", json!({"KeySpec": "RSA_2048"}));
    assert_eq!(asym["__type"], "UnsupportedOperationException");
    let listing = route(&store, &Request::new("GET", "/_kms/keys", Vec::new()));
    let listing = String::from_utf8(listing.body).unwrap();
    assert!(listing.contains(&key_id) && !listing.contains("Material"));
}

/// The owner key, a same-account user allowed only `kms:Encrypt` and
/// `kms:DescribeKey`, and a user in another account allowed `kms:*`.
fn strict_store() -> FileBucketStore {
    let store = store();
    let config = json!({
        "Users": [
            {"UserName": "alice", "AccessKeyId": ALICE_KEY, "SecretAccessKey": ALICE_SECRET,
             "Policies": [{"Statement": [{"Effect": "Allow", "Action": ["kms:Encrypt", "kms:DescribeKey"], "Resource": "*"}]}]},
            {"UserName": "bob", "Account": "111111111111", "AccessKeyId": BOB_KEY, "SecretAccessKey": BOB_SECRET,
             "Policies": [{"Statement": [{"Effect": "Allow", "Action": "kms:*", "Resource": "*"}]}]}
        ]
    })
    .to_string();
    let put = sign(
        "s3",
        OWNER_KEY,
        OWNER_SECRET,
        "PUT",
        "/_iam/config",
        config.as_bytes(),
        &[],
    );
    assert_eq!(route_with_auth(&store, &put, &strict()).status, 200);
    store
}

#[test]
fn strict_mode_authorizes_kms_calls() {
    let store = strict_store();
    let owner = (OWNER_KEY, OWNER_SECRET);
    let (status, created) = signed(&store, owner, "CreateKey", json!({}));
    assert_eq!(status, 200, "{created}");
    let key_id = created["KeyMetadata"]["KeyId"]
        .as_str()
        .unwrap()
        .to_string();

    // Unsigned, or signed for another service, is refused.
    let (target, body) = kms_request("ListKeys", &json!({}));
    let mut unsigned = Request::new("POST", "/", body.clone());
    unsigned
        .headers
        .insert("x-amz-target".to_string(), target.clone());
    let (status, refused) = decode(route_with_auth(&store, &unsigned, &strict()));
    assert_eq!(status, 403);
    assert_eq!(refused["__type"], "MissingAuthenticationTokenException");
    let wrong_service = sign(
        "s3",
        OWNER_KEY,
        OWNER_SECRET,
        "POST",
        "/",
        &body,
        &[JSON, ("x-amz-target", &target)],
    );
    assert_eq!(
        route_with_auth(&store, &wrong_service, &strict()).status,
        403
    );

    // alice: Encrypt allowed by her identity policy, Decrypt not.
    let alice = (ALICE_KEY, ALICE_SECRET);
    let (status, encrypted) = signed(
        &store,
        alice,
        "Encrypt",
        json!({"KeyId": key_id, "Plaintext": "aGk="}),
    );
    assert_eq!(status, 200, "{encrypted}");
    let (status, denied) = signed(
        &store,
        alice,
        "Decrypt",
        json!({"CiphertextBlob": encrypted["CiphertextBlob"]}),
    );
    assert_eq!(status, 400);
    assert_eq!(denied["__type"], "AccessDeniedException");
    let (_, decrypted) = signed(
        &store,
        owner,
        "Decrypt",
        json!({"CiphertextBlob": encrypted["CiphertextBlob"]}),
    );
    assert_eq!(decrypted["Plaintext"], "aGk=");

    // bob: no key policy trusts his account.
    let (_, cross) = signed(
        &store,
        (BOB_KEY, BOB_SECRET),
        "DescribeKey",
        json!({"KeyId": key_id}),
    );
    assert_eq!(cross["__type"], "AccessDeniedException");

    // The AWS managed alias/aws/s3 key (created by an SSE-KMS write) is not
    // usable for cryptography directly.
    let bucket = sign("s3", OWNER_KEY, OWNER_SECRET, "PUT", "/kms", b"", &[]);
    assert_eq!(route_with_auth(&store, &bucket, &strict()).status, 200);
    let object = sign(
        "s3",
        OWNER_KEY,
        OWNER_SECRET,
        "PUT",
        "/kms/a.txt",
        b"v",
        &[("x-amz-server-side-encryption", "aws:kms")],
    );
    assert_eq!(route_with_auth(&store, &object, &strict()).status, 200);
    let (status, managed) = signed(
        &store,
        owner,
        "DescribeKey",
        json!({"KeyId": "alias/aws/s3"}),
    );
    assert_eq!(status, 200, "{managed}");
    assert_eq!(managed["KeyMetadata"]["KeyManager"], "AWS");
    let (_, direct) = signed(
        &store,
        owner,
        "Encrypt",
        json!({"KeyId": "alias/aws/s3", "Plaintext": "aGk="}),
    );
    assert_eq!(direct["__type"], "AccessDeniedException");

    // Keys created through the API serve SSE-KMS writes, and disabling one
    // through the API blocks reads of its objects.
    let (_, _) = signed(
        &store,
        owner,
        "CreateAlias",
        json!({"AliasName": "alias/app", "TargetKeyId": key_id}),
    );
    let object = sign(
        "s3",
        OWNER_KEY,
        OWNER_SECRET,
        "PUT",
        "/kms/b.txt",
        b"v",
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "alias/app"),
        ],
    );
    let put = route_with_auth(&store, &object, &strict());
    assert_eq!(put.status, 200);
    assert!(put
        .headers
        .get("x-amz-server-side-encryption-aws-kms-key-id")
        .unwrap()
        .ends_with(&key_id));
    signed(&store, owner, "DisableKey", json!({"KeyId": key_id}));
    let get = sign("s3", OWNER_KEY, OWNER_SECRET, "GET", "/kms/b.txt", b"", &[]);
    let get = route_with_auth(&store, &get, &strict());
    assert_eq!(get.status, 400);
    assert!(String::from_utf8_lossy(&get.body).contains("KMS.DisabledException"));
}
