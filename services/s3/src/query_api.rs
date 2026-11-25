//! AWS IAM and STS Query APIs (and the routing of the KMS JSON API) served at
//! `POST /` on the S3 port, backed by the local identity registry
//! (`identities.rs`).
//!
//! A request is one of these APIs when it is `POST /` and either carries
//! `X-Amz-Target: TrentService.*` (KMS, `kms_api.rs`) or has a form-encoded
//! body with an `Action` from the tables below (or, for an unknown action, the
//! IAM or STS `Version`). S3 has no `POST /` operation, so this never shadows an
//! S3 request. In `strict` mode the request must be SigV4-signed for the API's
//! service (`iam`, `sts`, or `kms`); IAM accepts `us-east-1`, where SDKs sign
//! the global endpoint, as well as the configured region.
//!
//! STS: `GetCallerIdentity`, `AssumeRole`.
//! IAM: `GetUser`, `ListUsers`, `GetRole`, `ListRoles`, `GetPolicy`,
//! `PutUserPermissionsBoundary`, `DeleteUserPermissionsBoundary`,
//! `PutRolePermissionsBoundary`, `DeleteRolePermissionsBoundary`.
//!
//! Authorization in `strict` mode: unsigned requests are refused; the owner key
//! may call everything; a registry principal needs the IAM action allowed by its
//! identity policies within its session policy, boundary, and SCPs, and sees
//! only its own account. `GetCallerIdentity` needs no permission, as in AWS, and
//! `AssumeRole` is decided by the role's trust policy. In `relaxed` mode every
//! request is the owner. Responses never contain user secrets.

use std::collections::BTreeMap;
use std::path::Path;

use crate::http::{percent_decode, AuthConfig, Request, Response};
use crate::iam::Caller;
use crate::identities::{self, AssumeRoleInput, IamConfig, IamRole, IamUser, OWNER_ACCOUNT};
use crate::xml::{encode, Element};

const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const IAM_XMLNS: &str = "https://iam.amazonaws.com/doc/2010-05-08/";
const STS_VERSION: &str = "2011-06-15";
const IAM_VERSION: &str = "2010-05-08";

const STS_ACTIONS: &[&str] = &["GetCallerIdentity", "AssumeRole"];
const IAM_ACTIONS: &[&str] = &[
    "GetUser",
    "ListUsers",
    "GetRole",
    "ListRoles",
    "GetPolicy",
    "PutUserPermissionsBoundary",
    "DeleteUserPermissionsBoundary",
    "PutRolePermissionsBoundary",
    "DeleteRolePermissionsBoundary",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Api {
    Kms,
    Iam,
    Sts,
}

impl Api {
    /// The SigV4 service name requests to this API are signed for.
    pub(crate) fn signing_service(self) -> &'static str {
        match self {
            Api::Kms => "kms",
            Api::Iam => "iam",
            Api::Sts => "sts",
        }
    }
}

/// Which local API `POST /` addresses, if any.
pub(crate) fn classify(req: &Request) -> Option<Api> {
    if req.method != "POST" || req.path != "/" {
        return None;
    }
    if req.header("x-amz-target").starts_with("TrentService.") {
        return Some(Api::Kms);
    }
    if !req
        .header("content-type")
        .to_ascii_lowercase()
        .starts_with("application/x-www-form-urlencoded")
    {
        return None;
    }
    let form = parse_form(&req.body);
    let action = form.get("Action").map(String::as_str).unwrap_or("");
    if STS_ACTIONS.contains(&action) {
        return Some(Api::Sts);
    }
    if IAM_ACTIONS.contains(&action) {
        return Some(Api::Iam);
    }
    match form.get("Version").map(String::as_str) {
        Some(STS_VERSION) if !action.is_empty() => Some(Api::Sts),
        Some(IAM_VERSION) if !action.is_empty() => Some(Api::Iam),
        _ => None,
    }
}

/// Parses an `application/x-www-form-urlencoded` body.
pub(crate) fn parse_form(body: &[u8]) -> BTreeMap<String, String> {
    let text = String::from_utf8_lossy(body);
    let decode = |v: &str| percent_decode(&v.replace('+', " "));
    text.split('&')
        .filter(|p| !p.is_empty())
        .map(|part| {
            let (k, v) = part.split_once('=').unwrap_or((part, ""));
            (decode(k), decode(v))
        })
        .collect()
}

pub(crate) fn handle(
    root: &Path,
    req: &Request,
    auth: &AuthConfig,
    caller: &Caller,
    api: Api,
) -> Response {
    let strict = auth.auth_mode.eq_ignore_ascii_case("strict");
    if api == Api::Kms {
        return crate::kms_api::handle(root, req, auth, caller);
    }
    let xmlns = if api == Api::Sts {
        STS_XMLNS
    } else {
        IAM_XMLNS
    };
    if strict && *caller == Caller::Anonymous {
        return error(
            xmlns,
            403,
            "MissingAuthenticationToken",
            "Request is missing Authentication Token",
        );
    }
    let form = parse_form(&req.body);
    let param = |name: &str| param(&form, name);
    let ctx = Ctx {
        root,
        req,
        caller,
        strict,
        xmlns,
    };
    match param("Action") {
        "GetCallerIdentity" => get_caller_identity(&ctx),
        "AssumeRole" => assume_role(&ctx, auth, &form),
        "GetUser" => get_user(&ctx, param("UserName")),
        "ListUsers" => list_users(&ctx),
        "GetRole" => get_role(&ctx, param("RoleName")),
        "ListRoles" => list_roles(&ctx),
        "GetPolicy" => get_policy(&ctx, param("PolicyArn")),
        "PutUserPermissionsBoundary" => set_boundary(
            &ctx,
            Target::User(param("UserName")),
            Some(param("PermissionsBoundary")),
        ),
        "DeleteUserPermissionsBoundary" => {
            set_boundary(&ctx, Target::User(param("UserName")), None)
        }
        "PutRolePermissionsBoundary" => set_boundary(
            &ctx,
            Target::Role(param("RoleName")),
            Some(param("PermissionsBoundary")),
        ),
        "DeleteRolePermissionsBoundary" => {
            set_boundary(&ctx, Target::Role(param("RoleName")), None)
        }
        other => error(
            xmlns,
            400,
            "InvalidAction",
            &format!("The action {other} is not valid for this web service."),
        ),
    }
}

struct Ctx<'a> {
    root: &'a Path,
    req: &'a Request,
    caller: &'a Caller,
    strict: bool,
    xmlns: &'static str,
}

impl Ctx<'_> {
    /// The account whose IAM entities the caller sees.
    fn account(&self) -> &str {
        match self.caller {
            Caller::Iam(p) => &p.account,
            _ => OWNER_ACCOUNT,
        }
    }

    /// Strict mode: a registry principal needs `action` on `resource_arn`.
    fn allowed(&self, action: &'static str, resource_arn: &str) -> bool {
        match self.caller {
            Caller::Iam(p) if self.strict => {
                crate::iam::identity_allows(p, self.req, action, resource_arn, self.caller)
            }
            _ => true,
        }
    }

    fn denied(&self, action: &str, resource_arn: &str) -> Response {
        let who = match self.caller {
            Caller::Iam(p) => p.arn.clone(),
            _ => String::new(),
        };
        error(
            self.xmlns,
            403,
            "AccessDenied",
            &format!(
                "User: {who} is not authorized to perform: {action} on resource: {resource_arn}"
            ),
        )
    }

    fn respond(&self, action: &str, result: Element) -> Response {
        let body = Element::new(&format!("{action}Response"))
            .attr("xmlns", self.xmlns)
            .child(result)
            .child(Element::new("ResponseMetadata").text_child("RequestId", &request_id()));
        xml_response(200, encode(&body))
    }

    fn no_such_entity(&self, what: &str) -> Response {
        error(
            self.xmlns,
            404,
            "NoSuchEntity",
            &format!("The {what} cannot be found."),
        )
    }
}

fn get_caller_identity(ctx: &Ctx<'_>) -> Response {
    let (arn, user_id, account) = match ctx.caller {
        Caller::Iam(p) => (p.arn.clone(), p.user_id.clone(), p.account.clone()),
        _ => (
            format!("arn:aws:iam::{OWNER_ACCOUNT}:root"),
            OWNER_ACCOUNT.to_string(),
            OWNER_ACCOUNT.to_string(),
        ),
    };
    ctx.respond(
        "GetCallerIdentity",
        Element::new("GetCallerIdentityResult")
            .text_child("Arn", &arn)
            .text_child("UserId", &user_id)
            .text_child("Account", &account),
    )
}

fn param<'a>(form: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    form.get(name).map(String::as_str).unwrap_or("")
}

fn assume_role(ctx: &Ctx<'_>, auth: &AuthConfig, form: &BTreeMap<String, String>) -> Response {
    let param = |name: &str| param(form, name);
    let duration_seconds = match param("DurationSeconds") {
        "" => None,
        value => match value.parse::<i64>() {
            Ok(v) => Some(v),
            Err(_) => return error(ctx.xmlns, 400, "ValidationError", "invalid DurationSeconds"),
        },
    };
    let input = AssumeRoleInput {
        role_arn: param("RoleArn").to_string(),
        role_session_name: param("RoleSessionName").to_string(),
        policy: match param("Policy") {
            "" => None,
            policy => Some(serde_json::Value::String(policy.to_string())),
        },
        duration_seconds,
    };
    match identities::issue_session(ctx.root, ctx.req, auth, ctx.caller, &input) {
        Ok(session) => ctx.respond(
            "AssumeRole",
            Element::new("AssumeRoleResult")
                .child(
                    Element::new("Credentials")
                        .text_child("AccessKeyId", &session.access_key_id)
                        .text_child("SecretAccessKey", &session.secret_access_key)
                        .text_child("SessionToken", &session.session_token)
                        .text_child("Expiration", &session.expiration),
                )
                .child(
                    Element::new("AssumedRoleUser")
                        .text_child("AssumedRoleId", &session.assumed_role_id)
                        .text_child("Arn", &session.assumed_role_arn),
                ),
        ),
        // STS does not reveal whether a role exists.
        Err((404, _, _)) => error(
            ctx.xmlns,
            403,
            "AccessDenied",
            "not authorized to perform sts:AssumeRole",
        ),
        Err((status, code, message)) => error(ctx.xmlns, status, code, message),
    }
}

fn get_user(ctx: &Ctx<'_>, user_name: &str) -> Response {
    let config = identities::load_config(ctx.root);
    let user_name = if user_name.is_empty() {
        match ctx.caller {
            Caller::Iam(p) if p.user_name.is_empty() => {
                return error(
                    ctx.xmlns,
                    400,
                    "ValidationError",
                    "Must specify userName when calling with non-User credentials",
                )
            }
            Caller::Iam(p) => p.user_name.clone(),
            _ => {
                // The owner key is the account root.
                let arn = format!("arn:aws:iam::{OWNER_ACCOUNT}:root");
                if !ctx.allowed("iam:GetUser", &arn) {
                    return ctx.denied("iam:GetUser", &arn);
                }
                return ctx.respond(
                    "GetUser",
                    Element::new("GetUserResult").child(
                        Element::new("User")
                            .text_child("UserId", OWNER_ACCOUNT)
                            .text_child("Arn", &arn)
                            .text_child("CreateDate", "1970-01-01T00:00:00Z"),
                    ),
                );
            }
        }
    } else {
        user_name.to_string()
    };
    let account = ctx.account();
    let arn = format!("arn:aws:iam::{account}:user/{user_name}");
    if !ctx.allowed("iam:GetUser", &arn) {
        return ctx.denied("iam:GetUser", &arn);
    }
    let Some(user) = config
        .users
        .iter()
        .find(|u| u.account == account && u.user_name == user_name)
    else {
        return ctx.no_such_entity(&format!("user with name {user_name}"));
    };
    ctx.respond(
        "GetUser",
        Element::new("GetUserResult").child(user_element("User", user)),
    )
}

fn list_users(ctx: &Ctx<'_>) -> Response {
    let account = ctx.account();
    let resource = format!("arn:aws:iam::{account}:user/*");
    if !ctx.allowed("iam:ListUsers", &resource) {
        return ctx.denied("iam:ListUsers", &resource);
    }
    let config = identities::load_config(ctx.root);
    let mut users = Element::new("Users");
    for user in config.users.iter().filter(|u| u.account == account) {
        users = users.child(user_element("member", user));
    }
    ctx.respond(
        "ListUsers",
        Element::new("ListUsersResult")
            .child(users)
            .text_child("IsTruncated", "false"),
    )
}

fn get_role(ctx: &Ctx<'_>, role_name: &str) -> Response {
    let account = ctx.account();
    let arn = format!("arn:aws:iam::{account}:role/{role_name}");
    if !ctx.allowed("iam:GetRole", &arn) {
        return ctx.denied("iam:GetRole", &arn);
    }
    let config = identities::load_config(ctx.root);
    let Some(role) = config
        .roles
        .iter()
        .find(|r| r.account == account && r.role_name == role_name)
    else {
        return ctx.no_such_entity(&format!("role with name {role_name}"));
    };
    ctx.respond(
        "GetRole",
        Element::new("GetRoleResult").child(role_element("Role", role)),
    )
}

fn list_roles(ctx: &Ctx<'_>) -> Response {
    let account = ctx.account();
    let resource = format!("arn:aws:iam::{account}:role/*");
    if !ctx.allowed("iam:ListRoles", &resource) {
        return ctx.denied("iam:ListRoles", &resource);
    }
    let config = identities::load_config(ctx.root);
    let mut roles = Element::new("Roles");
    for role in config.roles.iter().filter(|r| r.account == account) {
        roles = roles.child(role_element("member", role));
    }
    ctx.respond(
        "ListRoles",
        Element::new("ListRolesResult")
            .child(roles)
            .text_child("IsTruncated", "false"),
    )
}

fn get_policy(ctx: &Ctx<'_>, policy_arn: &str) -> Response {
    if !ctx.allowed("iam:GetPolicy", policy_arn) {
        return ctx.denied("iam:GetPolicy", policy_arn);
    }
    let config = identities::load_config(ctx.root);
    let account = ctx.account();
    let Some(policy) = config
        .managed_policies
        .iter()
        .find(|p| p.account == account && p.arn() == policy_arn)
    else {
        return ctx.no_such_entity(&format!("policy {policy_arn}"));
    };
    let attached = config
        .users
        .iter()
        .filter(|u| u.permissions_boundary.as_deref() == Some(policy_arn))
        .count()
        + config
            .roles
            .iter()
            .filter(|r| r.permissions_boundary.as_deref() == Some(policy_arn))
            .count();
    ctx.respond(
        "GetPolicy",
        Element::new("GetPolicyResult").child(
            Element::new("Policy")
                .text_child("PolicyName", &policy.policy_name)
                .text_child("PolicyId", &unique_policy_id(&policy.arn()))
                .text_child("Arn", &policy.arn())
                .text_child("Path", "/")
                .text_child("DefaultVersionId", "v1")
                .text_child("AttachmentCount", "0")
                .text_child("PermissionsBoundaryUsageCount", &attached.to_string())
                .text_child("IsAttachable", "true"),
        ),
    )
}

enum Target<'a> {
    User(&'a str),
    Role(&'a str),
}

fn set_boundary(ctx: &Ctx<'_>, target: Target<'_>, boundary: Option<&str>) -> Response {
    let account = ctx.account().to_string();
    let (action, arn) = match (&target, boundary.is_some()) {
        (Target::User(name), true) => (
            "iam:PutUserPermissionsBoundary",
            format!("arn:aws:iam::{account}:user/{name}"),
        ),
        (Target::User(name), false) => (
            "iam:DeleteUserPermissionsBoundary",
            format!("arn:aws:iam::{account}:user/{name}"),
        ),
        (Target::Role(name), true) => (
            "iam:PutRolePermissionsBoundary",
            format!("arn:aws:iam::{account}:role/{name}"),
        ),
        (Target::Role(name), false) => (
            "iam:DeleteRolePermissionsBoundary",
            format!("arn:aws:iam::{account}:role/{name}"),
        ),
    };
    if !ctx.allowed(action, &arn) {
        return ctx.denied(action, &arn);
    }
    let mut config: IamConfig = identities::load_config(ctx.root);
    if let Some(policy_arn) = boundary {
        if !config
            .managed_policies
            .iter()
            .any(|p| p.account == account && p.arn() == policy_arn)
        {
            return ctx.no_such_entity(&format!("policy {policy_arn}"));
        }
    }
    let new_value = boundary.map(str::to_string);
    let (slot, label) = match target {
        Target::User(name) => (
            config
                .users
                .iter_mut()
                .find(|u| u.account == account && u.user_name == name)
                .map(|u| &mut u.permissions_boundary),
            format!("user with name {name}"),
        ),
        Target::Role(name) => (
            config
                .roles
                .iter_mut()
                .find(|r| r.account == account && r.role_name == name)
                .map(|r| &mut r.permissions_boundary),
            format!("role with name {name}"),
        ),
    };
    let Some(slot) = slot else {
        return ctx.no_such_entity(&label);
    };
    *slot = new_value;
    if identities::save_config(ctx.root, &config).is_err() {
        return error(
            ctx.xmlns,
            500,
            "ServiceFailure",
            "could not write IAM config",
        );
    }
    let name = action.trim_start_matches("iam:");
    ctx.respond(name, Element::new(&format!("{name}Result")))
}

fn user_element(name: &str, user: &IamUser) -> Element {
    let el = Element::new(name)
        .text_child("Path", "/")
        .text_child("UserName", &user.user_name)
        .text_child("UserId", &user.user_id())
        .text_child("Arn", &user.arn())
        .text_child("CreateDate", &user.create_date);
    with_boundary(el, user.permissions_boundary.as_deref())
}

fn role_element(name: &str, role: &IamRole) -> Element {
    let mut el = Element::new(name)
        .text_child("Path", "/")
        .text_child("RoleName", &role.role_name)
        .text_child("RoleId", &role.role_id())
        .text_child("Arn", &role.arn())
        .text_child("CreateDate", &role.create_date);
    if let Some(doc) = &role.assume_role_policy_document {
        // IAM returns policy documents URL-encoded.
        el = el.text_child(
            "AssumeRolePolicyDocument",
            &crate::percent::aws_percent_encode(&doc.to_string(), ""),
        );
    }
    el = el.text_child("MaxSessionDuration", &role.max_session_duration.to_string());
    with_boundary(el, role.permissions_boundary.as_deref())
}

fn with_boundary(el: Element, boundary: Option<&str>) -> Element {
    match boundary {
        Some(arn) => el.child(
            Element::new("PermissionsBoundary")
                .text_child("PermissionsBoundaryType", "Policy")
                .text_child("PermissionsBoundaryArn", arn),
        ),
        None => el,
    }
}

fn unique_policy_id(arn: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(arn.as_bytes()));
    format!("ANPA{}", digest[..17].to_ascii_uppercase())
}

fn request_id() -> String {
    let h = identities::random_hex("query-request");
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn error(xmlns: &str, status: u16, code: &str, message: &str) -> Response {
    let kind = if status >= 500 { "Receiver" } else { "Sender" };
    let body = Element::new("ErrorResponse")
        .attr("xmlns", xmlns)
        .child(
            Element::new("Error")
                .text_child("Type", kind)
                .text_child("Code", code)
                .text_child("Message", message),
        )
        .text_child("RequestId", &request_id());
    xml_response(status, encode(&body))
}

fn xml_response(status: u16, body: Vec<u8>) -> Response {
    let mut response = Response::with_body(status, body);
    response
        .headers
        .insert("Content-Type".to_string(), "text/xml".to_string());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_bodies_decode_plus_and_percent_escapes() {
        let form = parse_form(b"Action=AssumeRole&RoleSessionName=a+b%2Fc&Empty=");
        assert_eq!(form["Action"], "AssumeRole");
        assert_eq!(form["RoleSessionName"], "a b/c");
        assert_eq!(form["Empty"], "");
    }

    #[test]
    fn classify_needs_post_root_and_a_known_api() {
        let form = |body: &str| {
            let mut req = Request::new("POST", "/", body.as_bytes().to_vec());
            req.headers.insert(
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            );
            req
        };
        assert_eq!(classify(&form("Action=GetCallerIdentity")), Some(Api::Sts));
        assert_eq!(classify(&form("Action=ListUsers")), Some(Api::Iam));
        assert_eq!(
            classify(&form("Action=CreateUser&Version=2010-05-08")),
            Some(Api::Iam)
        );
        assert_eq!(classify(&form("Action=Nope")), None);
        let mut kms = Request::new("POST", "/", b"{}".to_vec());
        kms.headers.insert(
            "x-amz-target".to_string(),
            "TrentService.ListKeys".to_string(),
        );
        assert_eq!(classify(&kms), Some(Api::Kms));
        let mut get = form("Action=GetCallerIdentity");
        get.method = "GET".to_string();
        assert_eq!(classify(&get), None);
        let mut bucket = form("Action=GetCallerIdentity");
        bucket.path = "/bucket".to_string();
        assert_eq!(classify(&bucket), None);
    }
}
