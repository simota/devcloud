//! STS behavior through the full request pipeline.

use devcloud_sts::http::serve_for_test;
use devcloud_sts::{Config, Server};

const FORM: (&str, &str) = (
    "Content-Type",
    "application/x-www-form-urlencoded; charset=utf-8",
);

fn server() -> Server {
    Server::new(Config {
        account_id: "123456789012".to_string(),
        auth_mode: "relaxed".to_string(),
        ..Default::default()
    })
}

fn post(s: &Server, body: &str) -> (u16, String) {
    let r = serve_for_test(s, "POST", "/", &[FORM], body.as_bytes());
    (r.status, r.body)
}

fn tag<'a>(xml: &'a str, name: &str) -> &'a str {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("no <{name}> in {xml}"))
        + open.len();
    let end = start + xml[start..].find(&close).unwrap();
    &xml[start..end]
}

#[test]
fn assume_role_with_inline_policy_returns_credentials() {
    let s = server();
    // What aws-sdk-go-v2 sends: form-encoded, policy JSON percent-encoded.
    let body = "Action=AssumeRole&Version=2011-06-15\
        &RoleArn=arn%3Aaws%3Aiam%3A%3A111122223333%3Arole%2Fcsa%2Fsite-runner\
        &RoleSessionName=site-a%40runner\
        &DurationSeconds=900\
        &Policy=%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%5D%7D";
    let (status, xml) = post(&s, body);
    assert_eq!(status, 200, "{xml}");
    assert!(
        xml.starts_with("<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">")
    );
    assert!(tag(&xml, "AccessKeyId").starts_with("ASIA"));
    assert_eq!(tag(&xml, "AccessKeyId").len(), 20);
    assert!(tag(&xml, "SecretAccessKey").len() >= 40);
    assert!(!tag(&xml, "SessionToken").is_empty());
    assert_eq!(
        tag(&xml, "Arn"),
        "arn:aws:sts::111122223333:assumed-role/site-runner/site-a@runner"
    );
    assert!(tag(&xml, "AssumedRoleId").ends_with(":site-a@runner"));
    assert!(tag(&xml, "PackedPolicySize").parse::<u32>().unwrap() >= 1);
    let expiration = tag(&xml, "Expiration");
    assert_eq!(expiration.len(), "2026-11-03T10:00:00Z".len());
    assert!(expiration.ends_with('Z'));
}

#[test]
fn each_call_issues_distinct_credentials() {
    let s = server();
    let body = "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1";
    let (_, a) = post(&s, body);
    let (_, b) = post(&s, body);
    assert_ne!(tag(&a, "AccessKeyId"), tag(&b, "AccessKeyId"));
    assert_ne!(tag(&a, "SessionToken"), tag(&b, "SessionToken"));
}

#[test]
fn assume_role_validation_errors() {
    let s = server();
    let cases = [
        ("Action=AssumeRole&RoleSessionName=s1", "ValidationError"),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=x",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=bad%20name",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:user/u&RoleSessionName=s1",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1&DurationSeconds=60",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1&Policy=not-json",
            "MalformedPolicyDocument",
        ),
    ];
    for (body, code) in cases {
        let (status, xml) = post(&s, body);
        assert_eq!(status, 400, "{body}");
        assert_eq!(tag(&xml, "Code"), code, "{body}");
        assert_eq!(tag(&xml, "Type"), "Sender");
    }
}

#[test]
fn get_caller_identity_via_get_and_post() {
    let s = server();
    let (status, xml) = post(&s, "Action=GetCallerIdentity&Version=2011-06-15");
    assert_eq!(status, 200);
    assert_eq!(tag(&xml, "Account"), "123456789012");
    assert_eq!(tag(&xml, "Arn"), "arn:aws:iam::123456789012:root");

    let r = serve_for_test(
        &s,
        "GET",
        "/?Action=GetCallerIdentity&Version=2011-06-15",
        &[],
        b"",
    );
    assert_eq!(r.status, 200);
}

#[test]
fn unknown_and_missing_action() {
    let s = server();
    let (status, xml) = post(&s, "Action=GetSessionToken");
    assert_eq!((status, tag(&xml, "Code")), (400, "InvalidAction"));
    let (status, xml) = post(&s, "Version=2011-06-15");
    assert_eq!((status, tag(&xml, "Code")), (400, "MissingAction"));
}

#[test]
fn strict_mode_requires_signature() {
    let s = Server::new(Config {
        auth_mode: "strict".to_string(),
        ..Default::default()
    });
    let (status, _) = post(&s, "Action=GetCallerIdentity");
    assert_eq!(status, 403);
}
