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
    let scope = format!("{date_stamp}/us-east-1/s3/aws4_request");
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
        format!("AWS4{SECRET_KEY}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac(&k_date, b"us-east-1");
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
    req.headers.insert(
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
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
