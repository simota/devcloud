//! Session policies of `AssumeRole` credentials, evaluated for requests signed
//! with those credentials (`strict` mode only).

use devcloud_sts::http::serve_for_test;
use devcloud_sts::{Config, Server};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

const HOST: &str = "127.0.0.1:18032";
const AMZ_DATE: &str = "20261125T100000Z";
const ROLE: &str = "arn:aws:iam::111122223333:role/base";

fn server(mode: &str) -> Server {
    Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        auth_mode: mode.to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        ..Default::default()
    })
}

fn sha(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

fn hm(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

struct Creds<'a> {
    access_key_id: &'a str,
    secret: &'a str,
    token: Option<&'a str>,
}

const OWNER: Creds = Creds {
    access_key_id: "dev",
    secret: "dev",
    token: None,
};

/// SigV4-signed form `POST /` (body hash signed, no `X-Amz-Content-Sha256`).
fn post(s: &Server, creds: &Creds, body: &str) -> (u16, String) {
    let date_stamp = &AMZ_DATE[..8];
    let scope = format!("{date_stamp}/us-east-1/sts/aws4_request");
    let (signed_headers, mut canonical_headers) = (
        "host;x-amz-date",
        format!("host:{HOST}\nx-amz-date:{AMZ_DATE}\n"),
    );
    let signed_headers = match creds.token {
        Some(token) => {
            canonical_headers.push_str(&format!("x-amz-security-token:{token}\n"));
            format!("{signed_headers};x-amz-security-token")
        }
        None => signed_headers.to_string(),
    };
    let canonical_request = format!(
        "POST\n/\n\n{canonical_headers}\n{signed_headers}\n{}",
        sha(body.as_bytes())
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{AMZ_DATE}\n{scope}\n{}",
        sha(canonical_request.as_bytes())
    );
    let k_date = hm(
        format!("AWS4{}", creds.secret).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hm(&k_date, b"us-east-1");
    let k_service = hm(&k_region, b"sts");
    let signing_key = hm(&k_service, b"aws4_request");
    let signature = hex::encode(hm(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    );
    let mut headers = vec![
        ("Host", HOST),
        (
            "Content-Type",
            "application/x-www-form-urlencoded; charset=utf-8",
        ),
        ("X-Amz-Date", AMZ_DATE),
        ("Authorization", auth.as_str()),
    ];
    if let Some(token) = creds.token {
        headers.push(("X-Amz-Security-Token", token));
    }
    let r = serve_for_test(s, "POST", "/", &headers, body.as_bytes());
    (r.status, r.body)
}

fn tag<'a>(xml: &'a str, name: &str) -> &'a str {
    let open = format!("<{name}>");
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("no <{name}> in {xml}"))
        + open.len();
    &xml[start..start + xml[start..].find(&format!("</{name}>")).unwrap()]
}

fn form_encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Owner-signed `AssumeRole` of `ROLE`; returns (key, secret, token).
fn assume(s: &Server, policy: Option<&str>) -> (String, String, String) {
    let mut body = format!(
        "Action=AssumeRole&RoleArn={}&RoleSessionName=base-session",
        form_encode(ROLE)
    );
    if let Some(policy) = policy {
        body.push_str(&format!("&Policy={}", form_encode(policy)));
    }
    let (status, xml) = post(s, &OWNER, &body);
    assert_eq!(status, 200, "{xml}");
    (
        tag(&xml, "AccessKeyId").to_string(),
        tag(&xml, "SecretAccessKey").to_string(),
        tag(&xml, "SessionToken").to_string(),
    )
}

fn chain_body(role: &str, extra: &str) -> String {
    format!(
        "Action=AssumeRole&RoleArn={}&RoleSessionName=next{extra}",
        form_encode(role)
    )
}

const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[
    {"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::111122223333:role/app-*"},
    {"Effect":"Deny","Action":"sts:*","Resource":"arn:aws:iam::111122223333:role/app-admin"}]}"#;

#[test]
fn session_policy_limits_role_chaining() {
    let s = server("strict");
    let (key, secret, token) = assume(&s, Some(POLICY));
    let creds = Creds {
        access_key_id: &key,
        secret: &secret,
        token: Some(&token),
    };

    let (status, xml) = post(
        &s,
        &creds,
        &chain_body("arn:aws:iam::111122223333:role/app-web", ""),
    );
    assert_eq!(status, 200, "{xml}");
    assert_eq!(
        tag(&xml, "Arn"),
        "arn:aws:sts::111122223333:assumed-role/app-web/next"
    );

    // Explicit Deny beats the Allow.
    let (status, xml) = post(
        &s,
        &creds,
        &chain_body("arn:aws:iam::111122223333:role/app-admin", ""),
    );
    assert_eq!(status, 403, "{xml}");
    assert_eq!(tag(&xml, "Code"), "AccessDenied");
    assert!(tag(&xml, "Message").contains("explicit deny in a session policy"));
    assert!(
        tag(&xml, "Message").contains("arn:aws:sts::111122223333:assumed-role/base/base-session")
    );

    // No statement allows it: implicit deny.
    let (status, xml) = post(
        &s,
        &creds,
        &chain_body("arn:aws:iam::111122223333:role/other", ""),
    );
    assert_eq!(status, 403, "{xml}");
    assert!(tag(&xml, "Message").contains("no session policy allows the sts:AssumeRole action"));

    // GetCallerIdentity needs no permission, even under a session policy.
    let (status, xml) = post(&s, &creds, "Action=GetCallerIdentity");
    assert_eq!(status, 200, "{xml}");
}

#[test]
fn chained_session_gets_its_own_policy() {
    let s = server("strict");
    let (key, secret, token) = assume(&s, None);
    let base = Creds {
        access_key_id: &key,
        secret: &secret,
        token: Some(&token),
    };
    // No session policy: the role is not limited (role policies are not emulated).
    let deny_all = r#"{"Statement":{"Effect":"Deny","Action":"*","Resource":"*"}}"#;
    let (status, xml) = post(
        &s,
        &base,
        &chain_body(
            "arn:aws:iam::111122223333:role/app-web",
            &format!("&Policy={}", form_encode(deny_all)),
        ),
    );
    assert_eq!(status, 200, "{xml}");
    let next = Creds {
        access_key_id: tag(&xml, "AccessKeyId"),
        secret: tag(&xml, "SecretAccessKey"),
        token: Some(tag(&xml, "SessionToken")),
    };
    let (status, xml) = post(&s, &next, &chain_body(ROLE, ""));
    assert_eq!(status, 403, "{xml}");
    assert_eq!(tag(&xml, "Code"), "AccessDenied");
}

#[test]
fn condition_keys_are_evaluated() {
    let s = server("strict");
    let policy = r#"{"Statement":{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"*",
        "Condition":{"StringLike":{"sts:RoleSessionName":"ci-*"},"StringEquals":{"aws:PrincipalArn":"arn:aws:iam::111122223333:role/base"}}}}"#;
    let (key, secret, token) = assume(&s, Some(policy));
    let creds = Creds {
        access_key_id: &key,
        secret: &secret,
        token: Some(&token),
    };
    let role = form_encode("arn:aws:iam::111122223333:role/app-web");
    let (status, xml) = post(
        &s,
        &creds,
        &format!("Action=AssumeRole&RoleArn={role}&RoleSessionName=ci-42"),
    );
    assert_eq!(status, 200, "{xml}");
    let (status, _) = post(
        &s,
        &creds,
        &format!("Action=AssumeRole&RoleArn={role}&RoleSessionName=dev-42"),
    );
    assert_eq!(status, 403);
}

#[test]
fn session_credentials_need_their_token_and_secret() {
    let s = server("strict");
    let (key, secret, token) = assume(&s, None);
    let body = "Action=GetCallerIdentity";

    let missing = Creds {
        access_key_id: &key,
        secret: &secret,
        token: None,
    };
    let (status, xml) = post(&s, &missing, body);
    assert_eq!((status, tag(&xml, "Code")), (403, "InvalidClientTokenId"));

    let wrong = Creds {
        access_key_id: &key,
        secret: &secret,
        token: Some("devcloud-sts-wrong"),
    };
    let (status, xml) = post(&s, &wrong, body);
    assert_eq!((status, tag(&xml, "Code")), (403, "InvalidClientTokenId"));

    let bad_secret = Creds {
        access_key_id: &key,
        secret: "not-the-secret",
        token: Some(&token),
    };
    assert_eq!(post(&s, &bad_secret, body).0, 403);

    // A key this server never issued is still unrecognized.
    let unknown = Creds {
        access_key_id: "ASIAUNKNOWN000000000",
        secret: &secret,
        token: Some(&token),
    };
    let (status, xml) = post(&s, &unknown, body);
    assert_eq!(
        (status, tag(&xml, "Code")),
        (403, "UnrecognizedClientException")
    );
}

#[test]
fn role_chaining_is_limited_to_one_hour() {
    let s = server("strict");
    let (key, secret, token) = assume(&s, None);
    let creds = Creds {
        access_key_id: &key,
        secret: &secret,
        token: Some(&token),
    };
    let (status, xml) = post(&s, &creds, &chain_body(ROLE, "&DurationSeconds=3601"));
    assert_eq!((status, tag(&xml, "Code")), (400, "ValidationError"));
    let (status, xml) = post(&s, &creds, &chain_body(ROLE, "&DurationSeconds=3600"));
    assert_eq!(status, 200, "{xml}");
    // The owner key is not chaining.
    let (status, xml) = post(&s, &OWNER, &chain_body(ROLE, "&DurationSeconds=43200"));
    assert_eq!(status, 200, "{xml}");
}

#[test]
fn non_strict_modes_do_not_evaluate_session_policies() {
    let deny_all = r#"{"Statement":{"Effect":"Deny","Action":"*","Resource":"*"}}"#;
    for mode in ["relaxed", "signed-relaxed"] {
        let s = server(mode);
        let (key, secret, token) = assume(&s, Some(deny_all));
        assert!(s.session(&key).is_none(), "{mode} must stay stateless");
        let creds = Creds {
            access_key_id: &key,
            secret: &secret,
            token: Some(&token),
        };
        let (status, xml) = post(&s, &creds, &chain_body(ROLE, "&DurationSeconds=43200"));
        assert_eq!(status, 200, "{mode}: {xml}");
    }
}
