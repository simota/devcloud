//! Tests for strict-mode bucket policy and canned ACL enforcement.

use devcloud_s3::http::{route, route_with_auth, AuthConfig, Request};
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::time_fmt::rfc3339_seconds_from_unix;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

const ACCESS_KEY: &str = "policy-key";
const SECRET_KEY: &str = "policy-secret";

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-s3-iam-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn strict_auth() -> AuthConfig {
    AuthConfig {
        auth_mode: "strict".to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
        region: "us-east-1".to_string(),
    }
}

fn sign(method: &str, target: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> Request {
    sign_as(ACCESS_KEY, SECRET_KEY, method, target, body, extra_headers)
}

fn sign_as(
    access_key: &str,
    secret_key: &str,
    method: &str,
    target: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> Request {
    sign_for(
        "s3",
        access_key,
        secret_key,
        method,
        target,
        body,
        extra_headers,
    )
}

/// SigV4-signs for `service` (`s3`, `iam`, `sts`) in `us-east-1`.
fn sign_for(
    service: &str,
    access_key: &str,
    secret_key: &str,
    method: &str,
    target: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
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
    for (k, v) in extra_headers {
        req.headers.insert((*k).to_string(), (*v).to_string());
    }
    let signed_headers: String = req
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
    let canonical_query: String = req
        .query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_request = [
        method,
        target.split('?').next().unwrap(),
        &canonical_query,
        &canonical_headers,
        &signed_headers,
        &payload_hash,
    ]
    .join("\n");
    let scope = format!("{date_stamp}/us-east-1/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let hmac = |k: &[u8], d: &[u8]| -> Vec<u8> {
        let mut m = Hmac::<Sha256>::new_from_slice(k).unwrap();
        m.update(d);
        m.finalize().into_bytes().to_vec()
    };
    let k_date = hmac(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac(&k_date, b"us-east-1");
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
    req.headers.insert(
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
    );
    req
}

#[test]
fn strict_mode_enforces_bucket_policy_allow_and_explicit_deny() {
    let store = FileBucketStore::new(tempdir());
    let auth = strict_auth();

    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/site", b"", &[]), &auth).status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &sign("PUT", "/site/public/index.html", b"hello", &[]),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &sign("PUT", "/site/private/secret.txt", b"secret", &[]),
            &auth
        )
        .status,
        200
    );

    // Without policy or public ACL, anonymous GET is denied.
    let anon_before = route_with_auth(
        &store,
        &Request::new("GET", "/site/public/index.html", Vec::new()),
        &auth,
    );
    assert_eq!(anon_before.status, 403);

    // Bucket policy allowing anonymous reads under public/* and explicitly denying delete for everyone.
    let policy = br#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::site/public/*"
            },
            {
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "s3:DeleteObject",
                "Resource": "arn:aws:s3:::site/private/*"
            }
        ]
    }"#;
    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/site?policy", policy, &[]), &auth).status,
        204
    );

    let anon_public = route_with_auth(
        &store,
        &Request::new("GET", "/site/public/index.html", Vec::new()),
        &auth,
    );
    assert_eq!(anon_public.status, 200);
    assert_eq!(anon_public.body, b"hello");

    let anon_private = route_with_auth(
        &store,
        &Request::new("GET", "/site/private/secret.txt", Vec::new()),
        &auth,
    );
    assert_eq!(anon_private.status, 403);

    // Explicit Deny blocks even the authenticated bucket owner.
    let denied_delete = route_with_auth(
        &store,
        &sign("DELETE", "/site/private/secret.txt", b"", &[]),
        &auth,
    );
    assert_eq!(denied_delete.status, 403);

    // Authenticated owner can still delete objects outside the denied resource pattern.
    assert_eq!(
        route_with_auth(
            &store,
            &sign("DELETE", "/site/public/index.html", b"", &[]),
            &auth
        )
        .status,
        204
    );

    // Relaxed mode continues to bypass policy enforcement for local tooling.
    let relaxed = route(
        &store,
        &Request::new("DELETE", "/site/private/secret.txt", Vec::new()),
    );
    assert_eq!(relaxed.status, 204);
}

#[test]
fn strict_mode_enforces_bucket_policy_conditions_and_not_action() {
    let store = FileBucketStore::new(tempdir());
    let auth = strict_auth();

    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/cond", b"", &[]), &auth).status,
        200
    );

    // Require AES256 encryption on PutObject by denying PutObject without it.
    let policy = br#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::cond/*",
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }
            },
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::cond",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": "pub/*"
                    }
                }
            }
        ]
    }"#;
    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/cond?policy", policy, &[]), &auth).status,
        204
    );

    // Unencrypted put is denied.
    let unencrypted = route_with_auth(&store, &sign("PUT", "/cond/pub/a.txt", b"a", &[]), &auth);
    assert_eq!(unencrypted.status, 403);

    // Encrypted put satisfies the condition and succeeds.
    let encrypted = route_with_auth(
        &store,
        &sign(
            "PUT",
            "/cond/pub/a.txt",
            b"a",
            &[("x-amz-server-side-encryption", "AES256")],
        ),
        &auth,
    );
    assert_eq!(encrypted.status, 200);

    // Anonymous list with matching prefix is allowed; non-matching prefix is denied.
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/cond?prefix=pub/", Vec::new()),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/cond?prefix=priv/", Vec::new()),
            &auth
        )
        .status,
        403
    );
}

#[test]
fn strict_mode_enforces_canned_bucket_and_object_acls() {
    let store = FileBucketStore::new(tempdir());
    let auth = strict_auth();

    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/acl-bucket", b"", &[]), &auth).status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &sign(
                "PUT",
                "/acl-bucket/pub.txt",
                b"public",
                &[("x-amz-acl", "public-read")]
            ),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &sign("PUT", "/acl-bucket/priv.txt", b"private", &[]),
            &auth
        )
        .status,
        200
    );

    // Object ACL public-read allows anonymous read of that object only.
    let pub_get = route_with_auth(
        &store,
        &Request::new("GET", "/acl-bucket/pub.txt", Vec::new()),
        &auth,
    );
    assert_eq!(pub_get.status, 200);
    assert_eq!(pub_get.body, b"public");

    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/acl-bucket/priv.txt", Vec::new()),
            &auth
        )
        .status,
        403
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/acl-bucket", Vec::new()),
            &auth
        )
        .status,
        403
    );

    // Invalid credentials never fall back to anonymous public access.
    let mut bad_cred = Request::new("GET", "/acl-bucket/pub.txt", Vec::new());
    bad_cred
        .headers
        .insert("authorization".to_string(), "AWS wrong:bad".to_string());
    bad_cred.headers.insert(
        "date".to_string(),
        "Wed, 25 Nov 2026 05:00:00 GMT".to_string(),
    );
    assert_eq!(route_with_auth(&store, &bad_cred, &auth).status, 403);

    // Bucket ACL public-read allows anonymous listing and reading all objects, not writing.
    assert_eq!(
        route_with_auth(
            &store,
            &sign(
                "PUT",
                "/acl-bucket?acl",
                b"",
                &[("x-amz-acl", "public-read")]
            ),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/acl-bucket", Vec::new()),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("GET", "/acl-bucket/priv.txt", Vec::new()),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("PUT", "/acl-bucket/new.txt", b"nope".to_vec()),
            &auth
        )
        .status,
        403
    );

    // Bucket ACL public-read-write allows anonymous PUT/DELETE as well.
    assert_eq!(
        route_with_auth(
            &store,
            &sign(
                "PUT",
                "/acl-bucket?acl",
                b"",
                &[("x-amz-acl", "public-read-write")],
            ),
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("PUT", "/acl-bucket/new.txt", b"anon-ok".to_vec()),
            &auth
        )
        .status,
        200
    );

    // Explicit bucket policy Deny overrides public-read-write ACL.
    let deny_anon = br#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::acl-bucket/*"
        }]
    }"#;
    assert_eq!(
        route_with_auth(
            &store,
            &sign("PUT", "/acl-bucket?policy", deny_anon, &[]),
            &auth
        )
        .status,
        204
    );
    assert_eq!(
        route_with_auth(
            &store,
            &Request::new("PUT", "/acl-bucket/blocked.txt", b"blocked".to_vec()),
            &auth
        )
        .status,
        403
    );
}

const ALICE_KEY: &str = "AKIAALICE";
const ALICE_SECRET: &str = "alice-secret";
const BOB_KEY: &str = "AKIABOB";
const BOB_SECRET: &str = "bob-secret";

/// Owner-account user `alice` may read `shared/*`; `bob` in account
/// 111111111111 has broad identity permissions; role `reader` in the owner
/// account trusts account 111111111111.
fn iam_config() -> String {
    serde_json::json!({
        "Users": [
            {
                "UserName": "alice",
                "AccessKeyId": ALICE_KEY,
                "SecretAccessKey": ALICE_SECRET,
                "Policies": [{
                    "Statement": [
                        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::data/shared/*"},
                        {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"},
                        {"Effect": "Deny", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::data/shared/secret*"}
                    ]
                }]
            },
            {
                "UserName": "bob",
                "Account": "111111111111",
                "AccessKeyId": BOB_KEY,
                "SecretAccessKey": BOB_SECRET,
                "Policies": [
                    "{\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:*\",\"sts:AssumeRole\"],\"Resource\":\"*\"}]}"
                ]
            }
        ],
        "Roles": [{
            "RoleName": "reader",
            "AssumeRolePolicyDocument": {
                "Statement": [{"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::111111111111:root"}, "Action": "sts:AssumeRole"}]
            },
            "Policies": [{
                "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::data/*"}]
            }],
            "MaxSessionDuration": 7200
        }]
    })
    .to_string()
}

fn setup_iam_store() -> (FileBucketStore, AuthConfig) {
    let store = FileBucketStore::new(tempdir());
    let auth = strict_auth();
    assert_eq!(
        route_with_auth(&store, &sign("PUT", "/data", b"", &[]), &auth).status,
        200
    );
    for key in ["shared/a.txt", "shared/secret.txt", "private/b.txt"] {
        assert_eq!(
            route_with_auth(
                &store,
                &sign("PUT", &format!("/data/{key}"), b"v", &[]),
                &auth
            )
            .status,
            200
        );
    }
    let config = iam_config();
    let put = route_with_auth(
        &store,
        &sign("PUT", "/_iam/config", config.as_bytes(), &[]),
        &auth,
    );
    assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));
    (store, auth)
}

#[test]
fn iam_users_are_authorized_by_identity_and_bucket_policies() {
    let (store, auth) = setup_iam_store();
    let as_alice = |method: &str, target: &str| {
        route_with_auth(
            &store,
            &sign_as(ALICE_KEY, ALICE_SECRET, method, target, b"", &[]),
            &auth,
        )
        .status
    };
    let as_bob = |method: &str, target: &str, body: &[u8]| {
        route_with_auth(
            &store,
            &sign_as(BOB_KEY, BOB_SECRET, method, target, body, &[]),
            &auth,
        )
        .status
    };

    // Same-account user: identity policy grants, explicit Deny wins, everything else is denied.
    assert_eq!(as_alice("GET", "/data/shared/a.txt"), 200);
    assert_eq!(as_alice("GET", "/data/shared/secret.txt"), 403);
    assert_eq!(as_alice("GET", "/data/private/b.txt"), 403);
    assert_eq!(as_alice("DELETE", "/data/shared/a.txt"), 403);
    assert_eq!(as_alice("GET", "/"), 200);
    // Registry keys cannot use the owner-only control endpoints.
    assert_eq!(as_alice("GET", "/_iam/config"), 403);
    assert_eq!(as_alice("GET", "/_kms/keys"), 403);
    // A wrong secret for a registry key fails signature verification.
    assert_eq!(
        route_with_auth(
            &store,
            &sign_as(ALICE_KEY, "wrong", "GET", "/data/shared/a.txt", b"", &[]),
            &auth
        )
        .status,
        403
    );

    // Cross-account user: its own Allow is not enough without a bucket policy grant.
    assert_eq!(as_bob("GET", "/data/shared/a.txt", b""), 403);
    let policy = serde_json::json!({
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::111111111111:user/bob"}, "Action": "s3:GetObject", "Resource": "arn:aws:s3:::data/*"},
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::000000000000:user/alice"}, "Action": "s3:GetObject", "Resource": "arn:aws:s3:::data/private/*",
             "Condition": {"StringEquals": {"aws:PrincipalAccount": "000000000000"}}}
        ]
    })
    .to_string();
    assert_eq!(
        route_with_auth(
            &store,
            &sign("PUT", "/data?policy", policy.as_bytes(), &[]),
            &auth
        )
        .status,
        204
    );
    assert_eq!(as_bob("GET", "/data/shared/a.txt", b""), 200);
    assert_eq!(as_bob("PUT", "/data/shared/new.txt", b"x"), 403);
    // Same-account: the bucket policy alone grants alice private/*.
    assert_eq!(as_alice("GET", "/data/private/b.txt"), 200);

    // GET /_iam/config never returns secrets.
    let config = route_with_auth(&store, &sign("GET", "/_iam/config", b"", &[]), &auth);
    assert_eq!(config.status, 200);
    let text = String::from_utf8(config.body).unwrap();
    assert!(text.contains("alice") && !text.contains(ALICE_SECRET) && !text.contains(BOB_SECRET));
}

#[test]
fn assume_role_sessions_apply_trust_role_and_session_policies() {
    let (store, auth) = setup_iam_store();
    let assume = |key: &str, secret: &str, body: serde_json::Value| {
        let body = body.to_string();
        route_with_auth(
            &store,
            &sign_as(
                key,
                secret,
                "POST",
                "/_iam/assume-role",
                body.as_bytes(),
                &[],
            ),
            &auth,
        )
    };

    // alice's account is not trusted by the role.
    let denied = assume(
        ALICE_KEY,
        ALICE_SECRET,
        serde_json::json!({"RoleArn": "arn:aws:iam::000000000000:role/reader", "RoleSessionName": "a1"}),
    );
    assert_eq!(denied.status, 403);
    // Over MaxSessionDuration.
    let too_long = assume(
        BOB_KEY,
        BOB_SECRET,
        serde_json::json!({"RoleArn": "arn:aws:iam::000000000000:role/reader", "RoleSessionName": "b1", "DurationSeconds": 7201}),
    );
    assert_eq!(too_long.status, 400);

    let issue = |policy: Option<serde_json::Value>| {
        let mut body = serde_json::json!({
            "RoleArn": "arn:aws:iam::000000000000:role/reader",
            "RoleSessionName": "batch-job",
        });
        if let Some(policy) = policy {
            body["Policy"] = serde_json::Value::String(policy.to_string());
        }
        let response = assume(BOB_KEY, BOB_SECRET, body);
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            value["AssumedRoleUser"]["Arn"],
            "arn:aws:sts::000000000000:assumed-role/reader/batch-job"
        );
        let creds = &value["Credentials"];
        (
            creds["AccessKeyId"].as_str().unwrap().to_string(),
            creds["SecretAccessKey"].as_str().unwrap().to_string(),
            creds["SessionToken"].as_str().unwrap().to_string(),
        )
    };
    let with_token = |creds: &(String, String, String), method: &str, target: &str, token: &str| {
        route_with_auth(
            &store,
            &sign_as(
                &creds.0,
                &creds.1,
                method,
                target,
                b"v",
                &[("x-amz-security-token", token)],
            ),
            &auth,
        )
    };

    // Role session without a session policy: role permissions apply.
    let creds = issue(None);
    assert!(creds.0.starts_with("ASIA"));
    assert_eq!(
        with_token(&creds, "GET", "/data/private/b.txt", &creds.2).status,
        200
    );
    assert_eq!(
        with_token(&creds, "PUT", "/data/private/c.txt", &creds.2).status,
        200
    );
    assert_eq!(
        with_token(&creds, "DELETE", "/data/private/c.txt", &creds.2).status,
        403
    );
    // The session token is required and must match.
    let missing_token = route_with_auth(
        &store,
        &sign_as(&creds.0, &creds.1, "GET", "/data/private/b.txt", b"", &[]),
        &auth,
    );
    assert_eq!(missing_token.status, 400);
    assert!(String::from_utf8_lossy(&missing_token.body).contains("InvalidToken"));

    // A session policy narrows the role's permissions to GetObject on shared/*.
    let narrowed = issue(Some(serde_json::json!({
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::data/shared/*"}]
    })));
    assert_eq!(
        with_token(&narrowed, "GET", "/data/shared/a.txt", &narrowed.2).status,
        200
    );
    assert_eq!(
        with_token(&narrowed, "GET", "/data/private/b.txt", &narrowed.2).status,
        403
    );
    assert_eq!(
        with_token(&narrowed, "PUT", "/data/shared/d.txt", &narrowed.2).status,
        403
    );

    // Expired sessions are refused.
    let sessions_path = store.root().join(".iam-sessions.json");
    let mut sessions: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&sessions_path).unwrap()).unwrap();
    for session in sessions["Sessions"].as_array_mut().unwrap() {
        session["Expiration"] = serde_json::json!(1);
    }
    std::fs::write(&sessions_path, sessions.to_string()).unwrap();
    let expired = with_token(&creds, "GET", "/data/private/b.txt", &creds.2);
    assert_eq!(expired.status, 400);
    assert!(String::from_utf8_lossy(&expired.body).contains("ExpiredToken"));
}

#[test]
fn relaxed_mode_ignores_iam_registry_and_trust_policies() {
    let store = FileBucketStore::new(tempdir());
    assert_eq!(
        route(&store, &Request::new("PUT", "/data", Vec::new())).status,
        200
    );
    let config = iam_config();
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/_iam/config", config.into_bytes())
        )
        .status,
        200
    );
    // Untrusted callers can assume the role in relaxed mode.
    let body = serde_json::json!({"RoleArn": "arn:aws:iam::000000000000:role/reader", "RoleSessionName": "local"})
        .to_string();
    assert_eq!(
        route(
            &store,
            &Request::new("POST", "/_iam/assume-role", body.into_bytes())
        )
        .status,
        200
    );
    // An unsigned write still works and the registry files are not buckets.
    assert_eq!(
        route(&store, &Request::new("PUT", "/data/k", b"v".to_vec())).status,
        200
    );
    let list = route(&store, &Request::new("GET", "/", Vec::new()));
    assert_eq!(list.status, 200);
    assert!(!String::from_utf8_lossy(&list.body).contains(".iam"));
    // Invalid configs are refused.
    let invalid =
        r#"{"Users":[{"UserName":"x","Account":"12","AccessKeyId":"k","SecretAccessKey":"s"}]}"#;
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/_iam/config", invalid.as_bytes().to_vec())
        )
        .status,
        400
    );
}

const CAROL_KEY: &str = "AKIACAROL";
const CAROL_SECRET: &str = "carol-secret";

/// `iam_config()` plus a boundary that only allows reads of `data/shared/*`,
/// attached to owner-account user `carol` (who has `s3:*`), and SCPs on account
/// 111111111111 that allow everything except object writes and `sts:AssumeRole`.
fn guarded_config() -> serde_json::Value {
    let mut config: serde_json::Value = serde_json::from_str(&iam_config()).unwrap();
    config["Users"].as_array_mut().unwrap().push(serde_json::json!({
        "UserName": "carol",
        "AccessKeyId": CAROL_KEY,
        "SecretAccessKey": CAROL_SECRET,
        "Policies": [{"Statement": [{"Effect": "Allow", "Action": ["s3:*", "iam:GetUser"], "Resource": "*"}]}],
        "PermissionsBoundary": "arn:aws:iam::000000000000:policy/shared-read"
    }));
    config["ManagedPolicies"] = serde_json::json!([{
        "PolicyName": "shared-read",
        "PolicyDocument": {"Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject", "iam:GetUser"], "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::data/shared/secret*"}
        ]}
    }]);
    config["ServiceControlPolicies"] = serde_json::json!([
        {"PolicyName": "full-access", "TargetAccounts": ["111111111111"],
         "PolicyDocument": {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}},
        {"PolicyName": "no-writes", "TargetAccounts": ["111111111111"],
         "PolicyDocument": {"Statement": [{"Effect": "Deny", "Action": ["s3:PutObject", "sts:AssumeRole"], "Resource": "*"}]}}
    ]);
    config
}

fn put_config(store: &FileBucketStore, auth: &AuthConfig, config: &serde_json::Value) -> u16 {
    let body = config.to_string();
    route_with_auth(
        store,
        &sign("PUT", "/_iam/config", body.as_bytes(), &[]),
        auth,
    )
    .status
}

fn put_bucket_policy(store: &FileBucketStore, auth: &AuthConfig, policy: serde_json::Value) {
    let body = policy.to_string();
    assert_eq!(
        route_with_auth(
            store,
            &sign("PUT", "/data?policy", body.as_bytes(), &[]),
            auth
        )
        .status,
        204
    );
}

#[test]
fn permissions_boundaries_limit_identity_and_indirect_bucket_grants() {
    let (store, auth) = setup_iam_store();
    assert_eq!(put_config(&store, &auth, &guarded_config()), 200);
    let as_carol = |method: &str, target: &str| {
        route_with_auth(
            &store,
            &sign_as(CAROL_KEY, CAROL_SECRET, method, target, b"v", &[]),
            &auth,
        )
        .status
    };
    // s3:* from the identity policy, cut down to GetObject by the boundary.
    assert_eq!(as_carol("GET", "/data/shared/a.txt"), 200);
    assert_eq!(as_carol("GET", "/data/private/b.txt"), 200);
    assert_eq!(as_carol("PUT", "/data/private/new.txt"), 403);
    assert_eq!(as_carol("DELETE", "/data/shared/a.txt"), 403);
    // An explicit Deny in the boundary wins.
    assert_eq!(as_carol("GET", "/data/shared/secret.txt"), 403);

    // A bucket policy naming the account is limited by the boundary...
    put_bucket_policy(
        &store,
        &auth,
        serde_json::json!({"Statement": [{"Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
            "Action": "s3:PutObject", "Resource": "arn:aws:s3:::data/*"}]}),
    );
    assert_eq!(as_carol("PUT", "/data/private/new.txt"), 403);
    // ...one naming the user ARN itself is not.
    put_bucket_policy(
        &store,
        &auth,
        serde_json::json!({"Statement": [{"Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::000000000000:user/carol"},
            "Action": "s3:PutObject", "Resource": "arn:aws:s3:::data/*"}]}),
    );
    assert_eq!(as_carol("PUT", "/data/private/new.txt"), 200);
    assert_eq!(as_carol("DELETE", "/data/private/new.txt"), 403);

    // Users without a boundary are unaffected.
    assert_eq!(
        route_with_auth(
            &store,
            &sign_as(
                ALICE_KEY,
                ALICE_SECRET,
                "GET",
                "/data/shared/a.txt",
                b"",
                &[]
            ),
            &auth
        )
        .status,
        200
    );

    // Boundaries must name a managed policy in the same account.
    let mut bad = guarded_config();
    bad["Users"][2]["PermissionsBoundary"] =
        serde_json::json!("arn:aws:iam::000000000000:policy/missing");
    assert_eq!(put_config(&store, &auth, &bad), 400);
    let mut cross = guarded_config();
    cross["Users"][1]["PermissionsBoundary"] =
        serde_json::json!("arn:aws:iam::000000000000:policy/shared-read");
    assert_eq!(put_config(&store, &auth, &cross), 400);
}

#[test]
fn service_control_policies_limit_member_account_principals() {
    let (store, auth) = setup_iam_store();
    assert_eq!(put_config(&store, &auth, &guarded_config()), 200);
    put_bucket_policy(
        &store,
        &auth,
        serde_json::json!({"Statement": [{"Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::111111111111:root"},
            "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::data/*"}]}),
    );
    let as_bob = |method: &str, target: &str| {
        route_with_auth(
            &store,
            &sign_as(BOB_KEY, BOB_SECRET, method, target, b"v", &[]),
            &auth,
        )
        .status
    };
    // Both sides allow the read; the SCP denies the write despite both grants.
    assert_eq!(as_bob("GET", "/data/shared/a.txt"), 200);
    assert_eq!(as_bob("PUT", "/data/shared/new.txt"), 403);
    // SCPs apply to sts:AssumeRole of member-account callers.
    let assume = serde_json::json!({"RoleArn": "arn:aws:iam::000000000000:role/reader", "RoleSessionName": "scp"})
        .to_string();
    assert_eq!(
        route_with_auth(
            &store,
            &sign_as(
                BOB_KEY,
                BOB_SECRET,
                "POST",
                "/_iam/assume-role",
                assume.as_bytes(),
                &[]
            ),
            &auth
        )
        .status,
        403
    );

    // Without the allow-all SCP, everything not allowed by an SCP is denied.
    let mut deny_only = guarded_config();
    deny_only["ServiceControlPolicies"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    assert_eq!(put_config(&store, &auth, &deny_only), 200);
    assert_eq!(as_bob("GET", "/data/shared/a.txt"), 403);
    // The owner account is the management account: never restricted, and not a target.
    assert_eq!(
        route_with_auth(
            &store,
            &sign_as(
                ALICE_KEY,
                ALICE_SECRET,
                "GET",
                "/data/shared/a.txt",
                b"",
                &[]
            ),
            &auth
        )
        .status,
        200
    );
    let mut on_owner = guarded_config();
    on_owner["ServiceControlPolicies"][1]["TargetAccounts"] = serde_json::json!(["000000000000"]);
    assert_eq!(put_config(&store, &auth, &on_owner), 400);
}

fn query(
    store: &FileBucketStore,
    auth: &AuthConfig,
    service: &str,
    creds: Option<(&str, &str)>,
    body: &str,
) -> (u16, String) {
    let headers = [(
        "content-type",
        "application/x-www-form-urlencoded; charset=utf-8",
    )];
    let req = match creds {
        Some((key, secret)) => {
            sign_for(service, key, secret, "POST", "/", body.as_bytes(), &headers)
        }
        None => {
            let mut req = Request::new("POST", "/", body.as_bytes().to_vec());
            req.headers
                .insert(headers[0].0.to_string(), headers[0].1.to_string());
            req
        }
    };
    let response = route_with_auth(store, &req, auth);
    (response.status, String::from_utf8(response.body).unwrap())
}

fn xml_value<'a>(xml: &'a str, tag: &str) -> &'a str {
    let start = xml.find(&format!("<{tag}>")).unwrap() + tag.len() + 2;
    let end = start + xml[start..].find(&format!("</{tag}>")).unwrap();
    &xml[start..end]
}

#[test]
fn sts_query_api_reports_callers_and_assumes_roles() {
    let (store, auth) = setup_iam_store();
    let owner = Some((ACCESS_KEY, SECRET_KEY));
    let alice = Some((ALICE_KEY, ALICE_SECRET));
    let bob = Some((BOB_KEY, BOB_SECRET));

    let (status, body) = query(
        &store,
        &auth,
        "sts",
        owner,
        "Action=GetCallerIdentity&Version=2011-06-15",
    );
    assert_eq!(status, 200, "{body}");
    assert!(body.starts_with(
        "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
    ));
    assert_eq!(xml_value(&body, "Arn"), "arn:aws:iam::000000000000:root");
    let (_, body) = query(
        &store,
        &auth,
        "sts",
        alice,
        "Action=GetCallerIdentity&Version=2011-06-15",
    );
    assert_eq!(
        xml_value(&body, "Arn"),
        "arn:aws:iam::000000000000:user/alice"
    );
    assert!(xml_value(&body, "UserId").starts_with("AIDA"));

    // Unsigned, or signed for the wrong service, is refused in strict mode.
    let (status, body) = query(
        &store,
        &auth,
        "sts",
        None,
        "Action=GetCallerIdentity&Version=2011-06-15",
    );
    assert_eq!(status, 403);
    assert!(body.contains("<Code>MissingAuthenticationToken</Code>"));
    let (status, _) = query(
        &store,
        &auth,
        "s3",
        alice,
        "Action=GetCallerIdentity&Version=2011-06-15",
    );
    assert_eq!(status, 403);

    // AssumeRole: the trust policy decides; an untrusted caller cannot tell whether the role exists.
    let assume = "Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Freader&RoleSessionName=job+1&DurationSeconds=900";
    let (status, body) = query(
        &store,
        &auth,
        "sts",
        alice,
        &assume.replace("job+1", "job1"),
    );
    assert_eq!(status, 403);
    assert!(body.contains("<Code>AccessDenied</Code>"));
    let (status, body) = query(&store, &auth, "sts", bob, "Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fmissing&RoleSessionName=x1");
    assert_eq!(
        (status, body.contains("<Code>AccessDenied</Code>")),
        (403, true)
    );
    let (status, body) = query(&store, &auth, "sts", bob, &assume.replace("job+1", "job1"));
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        xml_value(&body, "Arn"),
        "arn:aws:sts::000000000000:assumed-role/reader/job1"
    );
    let session_key = xml_value(&body, "AccessKeyId").to_string();
    let session_secret = xml_value(&body, "SecretAccessKey").to_string();
    let token = xml_value(&body, "SessionToken").to_string();
    assert!(session_key.starts_with("ASIA"));
    // The session works against S3 and reports itself through GetCallerIdentity.
    let get = sign_as(
        &session_key,
        &session_secret,
        "GET",
        "/data/private/b.txt",
        b"",
        &[("x-amz-security-token", &token)],
    );
    assert_eq!(route_with_auth(&store, &get, &auth).status, 200);
    let who = sign_for(
        "sts",
        &session_key,
        &session_secret,
        "POST",
        "/",
        b"Action=GetCallerIdentity&Version=2011-06-15",
        &[
            ("content-type", "application/x-www-form-urlencoded"),
            ("x-amz-security-token", &token),
        ],
    );
    let response = route_with_auth(&store, &who, &auth);
    let body = String::from_utf8(response.body).unwrap();
    assert_eq!(xml_value(&body, "Account"), "000000000000");
    assert!(xml_value(&body, "UserId").ends_with(":job1"));
    // Session keys need their token.
    let (status, body) = query(
        &store,
        &auth,
        "sts",
        Some((&session_key, &session_secret)),
        "Action=GetCallerIdentity&Version=2011-06-15",
    );
    assert_eq!(status, 400);
    assert!(body.contains("InvalidToken"));

    let (status, body) = query(
        &store,
        &auth,
        "sts",
        bob,
        "Action=GetSessionToken&Version=2011-06-15",
    );
    assert_eq!(status, 400);
    assert!(body.contains("<Code>InvalidAction</Code>"));
}

#[test]
fn iam_query_api_reads_the_registry_and_manages_boundaries() {
    let (store, auth) = setup_iam_store();
    assert_eq!(put_config(&store, &auth, &guarded_config()), 200);
    let owner = Some((ACCESS_KEY, SECRET_KEY));
    let iam = |creds, body: &str| {
        query(
            &store,
            &auth,
            "iam",
            creds,
            &format!("{body}&Version=2010-05-08"),
        )
    };

    let (status, body) = iam(owner, "Action=ListUsers");
    assert_eq!(status, 200, "{body}");
    // Only the caller's account; never secrets.
    assert!(
        body.contains("<UserName>alice</UserName>") && body.contains("<UserName>carol</UserName>")
    );
    assert!(!body.contains("bob") && !body.contains(ALICE_SECRET));
    assert!(body.contains("<PermissionsBoundaryArn>arn:aws:iam::000000000000:policy/shared-read</PermissionsBoundaryArn>"));

    let (status, body) = iam(owner, "Action=GetRole&RoleName=reader");
    assert_eq!(status, 200, "{body}");
    assert_eq!(xml_value(&body, "MaxSessionDuration"), "7200");
    assert!(xml_value(&body, "RoleId").starts_with("AROA"));
    // URL-encoded, as IAM returns policy documents.
    let trust = xml_value(&body, "AssumeRolePolicyDocument");
    assert!(
        trust.contains("arn%3Aaws%3Aiam%3A%3A111111111111%3Aroot"),
        "{trust}"
    );
    let (status, _) = iam(owner, "Action=GetRole&RoleName=nope");
    assert_eq!(status, 404);
    let (status, body) = iam(
        owner,
        "Action=GetPolicy&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fshared-read",
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(xml_value(&body, "PermissionsBoundaryUsageCount"), "1");

    // Registry principals need the IAM action, limited by their boundary.
    let carol = Some((CAROL_KEY, CAROL_SECRET));
    let (status, body) = iam(carol, "Action=GetUser");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        xml_value(&body, "Arn"),
        "arn:aws:iam::000000000000:user/carol"
    );
    let (status, _) = iam(carol, "Action=ListRoles");
    assert_eq!(status, 403);
    let (status, _) = iam(Some((ALICE_KEY, ALICE_SECRET)), "Action=GetUser");
    assert_eq!(status, 403);
    // bob's policies grant no IAM actions.
    let (status, body) = iam(Some((BOB_KEY, BOB_SECRET)), "Action=GetUser&UserName=alice");
    assert_eq!(status, 403, "{body}");

    // Removing carol's boundary through the API restores her s3:* grant.
    let as_carol_put = || {
        route_with_auth(
            &store,
            &sign_as(
                CAROL_KEY,
                CAROL_SECRET,
                "PUT",
                "/data/private/c.txt",
                b"v",
                &[],
            ),
            &auth,
        )
        .status
    };
    assert_eq!(as_carol_put(), 403);
    let (status, body) = iam(owner, "Action=DeleteUserPermissionsBoundary&UserName=carol");
    assert_eq!(status, 200, "{body}");
    assert_eq!(as_carol_put(), 200);
    let (status, _) = iam(owner, "Action=PutUserPermissionsBoundary&UserName=carol&PermissionsBoundary=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fmissing");
    assert_eq!(status, 404);
    let (status, _) = iam(owner, "Action=PutRolePermissionsBoundary&RoleName=reader&PermissionsBoundary=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fshared-read");
    assert_eq!(status, 200);
    let config = route_with_auth(&store, &sign("GET", "/_iam/config", b"", &[]), &auth);
    let config: serde_json::Value = serde_json::from_slice(&config.body).unwrap();
    assert_eq!(
        config["Roles"][0]["PermissionsBoundary"],
        "arn:aws:iam::000000000000:policy/shared-read"
    );
    assert!(config["Users"][2].get("PermissionsBoundary").is_none());
}

#[test]
fn relaxed_mode_serves_query_apis_as_the_owner() {
    let store = FileBucketStore::new(tempdir());
    let config = iam_config();
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/_iam/config", config.into_bytes())
        )
        .status,
        200
    );
    let post = |body: &str| {
        let mut req = Request::new("POST", "/", body.as_bytes().to_vec());
        req.headers.insert(
            "content-type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        );
        let response = route(&store, &req);
        (response.status, String::from_utf8(response.body).unwrap())
    };
    let (status, body) = post("Action=GetCallerIdentity&Version=2011-06-15");
    assert_eq!(status, 200);
    assert_eq!(xml_value(&body, "Account"), "000000000000");
    let (status, body) = post("Action=ListUsers&Version=2010-05-08");
    assert_eq!(status, 200);
    assert!(body.contains("<UserName>alice</UserName>"));
    // Untrusted callers can assume the role in relaxed mode, as with /_iam/assume-role.
    let (status, body) = post("Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Freader&RoleSessionName=local");
    assert_eq!(status, 200, "{body}");
    // POST / without a recognised API is still not an S3 operation.
    assert_ne!(post("foo=bar").0, 200);
}
