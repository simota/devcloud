//! GCS IAM policy and bucket/object ACL handlers, predefined ACL expansion,
//! and XML ACL helpers.

use devcloud_s3::base64;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::xml::Element;
use serde::{Deserialize, Serialize};

use crate::http::{
    json_error, method_not_allowed, url_decode, url_encode, Request, Response, Server,
};
use crate::state::{
    read_bucket_acl, read_bucket_meta, read_default_object_acl, read_iam_policy, read_object_acl,
    write_bucket_acl, write_default_object_acl, write_iam_policy, write_object_acl,
    AccessControlEntry, GcsIamPolicy, IamBinding,
};

#[derive(Debug, Serialize)]
struct IamPolicyResponse {
    kind: String,
    #[serde(rename = "resourceId")]
    resource_id: String,
    version: i32,
    etag: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    bindings: Vec<IamBinding>,
}

#[derive(Debug, Default, Deserialize)]
struct IamPolicyRequest {
    #[serde(default)]
    version: Option<i32>,
    #[serde(default)]
    etag: String,
    #[serde(default)]
    bindings: Vec<IamBinding>,
}

#[derive(Debug, Serialize)]
struct TestIamPermissionsResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    permissions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BucketAccessControlResource {
    pub kind: String,
    pub id: String,
    #[serde(rename = "selfLink")]
    pub self_link: String,
    pub bucket: String,
    pub entity: String,
    pub role: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "entityId", skip_serializing_if = "String::is_empty")]
    pub entity_id: String,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectAccessControlResource {
    pub kind: String,
    pub id: String,
    #[serde(rename = "selfLink")]
    pub self_link: String,
    pub bucket: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub object: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub generation: String,
    pub entity: String,
    pub role: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "entityId", skip_serializing_if = "String::is_empty")]
    pub entity_id: String,
    pub etag: String,
}

#[derive(Debug, Serialize)]
struct BucketAclListResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<BucketAccessControlResource>,
}

#[derive(Debug, Serialize)]
struct ObjectAclListResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<ObjectAccessControlResource>,
}

#[derive(Debug, Default, Deserialize)]
struct AclMutationRequest {
    #[serde(default)]
    entity: String,
    #[serde(default)]
    role: String,
}

fn require_bucket(store: &FileBucketStore, bucket: &str) -> Result<(), Response> {
    match store.get_bucket(bucket) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(json_error(404, "notFound", "bucket not found")),
        Err(_) => Err(json_error(400, "invalid", "invalid bucket name")),
    }
}

fn check_uniform_access_blocks_acl(store: &FileBucketStore, bucket: &str) -> Result<(), Response> {
    if let Ok(meta) = read_bucket_meta(store, bucket) {
        if meta.uniform_bucket_level_access_enabled() {
            return Err(json_error(
                400,
                "invalid",
                "Cannot get or update legacy ACL when uniform bucket-level access is enabled",
            ));
        }
    }
    Ok(())
}

fn iam_policy_resource(bucket: &str, policy: &GcsIamPolicy) -> IamPolicyResponse {
    IamPolicyResponse {
        kind: "storage#policy".to_string(),
        resource_id: format!("projects/_/buckets/{bucket}"),
        version: policy.version.max(1),
        etag: policy.etag.clone(),
        bindings: policy.bindings.clone(),
    }
}

pub fn handle_bucket_iam(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if let Err(r) = require_bucket(server.store(), bucket) {
        return r;
    }
    match req.method.as_str() {
        "GET" => match read_iam_policy(server.store(), bucket) {
            Ok(policy) => Response::json_ok(&iam_policy_resource(bucket, &policy)),
            Err(_) => json_error(500, "backendError", "internal error"),
        },
        "PUT" => {
            let parsed: IamPolicyRequest = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) => return json_error(400, "invalid", "invalid json request"),
            };
            for binding in &parsed.bindings {
                if binding.role.trim().is_empty() {
                    return json_error(400, "invalid", "binding role is required");
                }
            }
            let mut current = match read_iam_policy(server.store(), bucket) {
                Ok(p) => p,
                Err(_) => return json_error(500, "backendError", "internal error"),
            };
            if !parsed.etag.is_empty() && parsed.etag != current.etag {
                return json_error(412, "conditionNotMet", "policy etag mismatch");
            }
            current.revision = current.revision.saturating_add(1);
            current.version = parsed.version.unwrap_or(current.version).max(1);
            current.etag = base64::std_encode(&current.revision.to_be_bytes());
            current.bindings = parsed.bindings;
            if write_iam_policy(server.store(), bucket, &current).is_err() {
                return json_error(500, "backendError", "internal error");
            }
            Response::json_ok(&iam_policy_resource(bucket, &current))
        }
        _ => method_not_allowed("GET, PUT"),
    }
}

pub fn handle_bucket_test_iam_permissions(
    server: &mut Server,
    req: &Request,
    bucket: &str,
) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    if let Err(r) = require_bucket(server.store(), bucket) {
        return r;
    }
    let mut requested = Vec::new();
    for value in req.query_all("permissions") {
        for part in value.split(',') {
            let trimmed = part.trim();
            if !trimmed.is_empty() && !requested.iter().any(|p| p == trimmed) {
                requested.push(trimmed.to_string());
            }
        }
    }
    let principal = req.header("x-goog-principal").trim();
    let permissions = if principal.is_empty() {
        requested
    } else {
        let policy = match read_iam_policy(server.store(), bucket) {
            Ok(p) => p,
            Err(_) => return json_error(500, "backendError", "internal error"),
        };
        requested
            .into_iter()
            .filter(|perm| policy_grants_permission(&policy, principal, perm))
            .collect()
    };
    Response::json_ok(&TestIamPermissionsResponse {
        kind: "storage#testIamPermissionsResponse".to_string(),
        permissions,
    })
}

fn policy_grants_permission(policy: &GcsIamPolicy, principal: &str, permission: &str) -> bool {
    for binding in &policy.bindings {
        let member_match = binding
            .members
            .iter()
            .any(|m| m == principal || m == "allUsers" || m == "allAuthenticatedUsers");
        if member_match && role_grants_permission(&binding.role, permission) {
            return true;
        }
    }
    false
}

fn role_grants_permission(role: &str, permission: &str) -> bool {
    match role {
        "roles/storage.admin" | "roles/owner" => permission.starts_with("storage."),
        "roles/storage.objectAdmin" => permission.starts_with("storage.objects."),
        "roles/storage.objectViewer" | "roles/storage.legacyObjectReader" => {
            matches!(permission, "storage.objects.get" | "storage.objects.list")
        }
        "roles/storage.objectCreator" => permission == "storage.objects.create",
        "roles/storage.legacyBucketOwner" => matches!(
            permission,
            "storage.buckets.get"
                | "storage.buckets.update"
                | "storage.buckets.delete"
                | "storage.buckets.getIamPolicy"
                | "storage.buckets.setIamPolicy"
                | "storage.objects.get"
                | "storage.objects.list"
                | "storage.objects.create"
                | "storage.objects.delete"
                | "storage.objects.update"
        ),
        "roles/storage.legacyBucketWriter" => matches!(
            permission,
            "storage.buckets.get"
                | "storage.objects.get"
                | "storage.objects.list"
                | "storage.objects.create"
                | "storage.objects.delete"
        ),
        "roles/storage.legacyBucketReader" => {
            matches!(
                permission,
                "storage.buckets.get" | "storage.objects.get" | "storage.objects.list"
            )
        }
        "roles/storage.legacyObjectOwner" => matches!(
            permission,
            "storage.objects.get" | "storage.objects.delete" | "storage.objects.update"
        ),
        _ => false,
    }
}

// --- ACL entry construction & predefined ACL expansion ---------------------

pub fn build_acl_entry(entity: &str, role: &str) -> AccessControlEntry {
    let email = entity
        .strip_prefix("user-")
        .or_else(|| entity.strip_prefix("group-"))
        .filter(|s| s.contains('@'))
        .unwrap_or("")
        .to_string();
    let domain = entity.strip_prefix("domain-").unwrap_or("").to_string();
    AccessControlEntry {
        entity: entity.to_string(),
        role: role.to_string(),
        email,
        domain,
        entity_id: String::new(),
    }
}

pub fn default_bucket_acl(project: &str) -> Vec<AccessControlEntry> {
    let proj = if project.is_empty() {
        "devcloud"
    } else {
        project
    };
    vec![
        build_acl_entry(&format!("project-owners-{proj}"), "OWNER"),
        build_acl_entry(&format!("project-editors-{proj}"), "OWNER"),
        build_acl_entry(&format!("project-viewers-{proj}"), "READER"),
    ]
}

pub fn expand_predefined_acl(
    predefined: &str,
    project: &str,
    for_bucket: bool,
) -> Result<Vec<AccessControlEntry>, String> {
    let proj = if project.is_empty() {
        "devcloud"
    } else {
        project
    };
    let mut entries = match predefined {
        "private" => vec![build_acl_entry(&format!("project-owners-{proj}"), "OWNER")],
        "projectPrivate" => default_bucket_acl(proj),
        "authenticatedRead" => vec![
            build_acl_entry(&format!("project-owners-{proj}"), "OWNER"),
            build_acl_entry("allAuthenticatedUsers", "READER"),
        ],
        "publicRead" => vec![
            build_acl_entry(&format!("project-owners-{proj}"), "OWNER"),
            build_acl_entry("allUsers", "READER"),
        ],
        "publicReadWrite" => {
            if !for_bucket {
                return Err("publicReadWrite is only valid for bucket ACLs".to_string());
            }
            vec![
                build_acl_entry(&format!("project-owners-{proj}"), "OWNER"),
                build_acl_entry("allUsers", "WRITER"),
            ]
        }
        "bucketOwnerRead" => vec![build_acl_entry(&format!("project-owners-{proj}"), "READER")],
        "bucketOwnerFullControl" => {
            vec![build_acl_entry(&format!("project-owners-{proj}"), "OWNER")]
        }
        other => return Err(format!("invalid predefined ACL {other:?}")),
    };
    entries.sort_by(|a, b| a.entity.cmp(&b.entity));
    Ok(entries)
}

fn validate_bucket_role(role: &str) -> bool {
    matches!(role, "OWNER" | "WRITER" | "READER")
}

fn validate_object_role(role: &str) -> bool {
    matches!(role, "OWNER" | "READER")
}

pub fn effective_bucket_acl(
    store: &FileBucketStore,
    bucket: &str,
    project: &str,
) -> Vec<AccessControlEntry> {
    match read_bucket_acl(store, bucket) {
        Ok(v) if !v.is_empty() => v,
        _ => default_bucket_acl(project),
    }
}

pub fn effective_object_acl(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    project: &str,
) -> Vec<AccessControlEntry> {
    if let Ok(Some(v)) = read_object_acl(store, bucket, key) {
        return v;
    }
    if let Ok(v) = read_default_object_acl(store, bucket) {
        if !v.is_empty() {
            return v;
        }
    }
    default_bucket_acl(project)
}

pub fn bucket_acl_resources(
    bucket: &str,
    entries: &[AccessControlEntry],
) -> Vec<BucketAccessControlResource> {
    entries
        .iter()
        .map(|e| bucket_acl_resource(bucket, e))
        .collect()
}

fn bucket_acl_resource(bucket: &str, entry: &AccessControlEntry) -> BucketAccessControlResource {
    BucketAccessControlResource {
        kind: "storage#bucketAccessControl".to_string(),
        id: format!("{bucket}/{}", entry.entity),
        self_link: format!(
            "/storage/v1/b/{}/acl/{}",
            url_encode(bucket),
            url_encode(&entry.entity)
        ),
        bucket: bucket.to_string(),
        entity: entry.entity.clone(),
        role: entry.role.clone(),
        email: entry.email.clone(),
        domain: entry.domain.clone(),
        entity_id: entry.entity_id.clone(),
        etag: "CAE=".to_string(),
    }
}

fn default_object_acl_resource(
    bucket: &str,
    entry: &AccessControlEntry,
) -> ObjectAccessControlResource {
    ObjectAccessControlResource {
        kind: "storage#objectAccessControl".to_string(),
        id: format!("{bucket}/defaultObjectAcl/{}", entry.entity),
        self_link: format!(
            "/storage/v1/b/{}/defaultObjectAcl/{}",
            url_encode(bucket),
            url_encode(&entry.entity)
        ),
        bucket: bucket.to_string(),
        object: String::new(),
        generation: String::new(),
        entity: entry.entity.clone(),
        role: entry.role.clone(),
        email: entry.email.clone(),
        domain: entry.domain.clone(),
        entity_id: entry.entity_id.clone(),
        etag: "CAE=".to_string(),
    }
}

fn object_acl_resource(
    bucket: &str,
    key: &str,
    generation: &str,
    entry: &AccessControlEntry,
) -> ObjectAccessControlResource {
    ObjectAccessControlResource {
        kind: "storage#objectAccessControl".to_string(),
        id: format!("{bucket}/{key}/{generation}/{}", entry.entity),
        self_link: format!(
            "/storage/v1/b/{}/o/{}/acl/{}",
            url_encode(bucket),
            url_encode(key),
            url_encode(&entry.entity)
        ),
        bucket: bucket.to_string(),
        object: key.to_string(),
        generation: generation.to_string(),
        entity: entry.entity.clone(),
        role: entry.role.clone(),
        email: entry.email.clone(),
        domain: entry.domain.clone(),
        entity_id: entry.entity_id.clone(),
        etag: "CAE=".to_string(),
    }
}

// --- Bucket ACL JSON endpoints ---------------------------------------------

pub fn handle_bucket_acl(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    entity_escaped: Option<&str>,
) -> Response {
    if let Err(r) = require_bucket(server.store(), bucket) {
        return r;
    }
    if let Err(r) = check_uniform_access_blocks_acl(server.store(), bucket) {
        return r;
    }
    let project = server.project().to_string();
    match entity_escaped {
        None => match req.method.as_str() {
            "GET" => {
                let entries = effective_bucket_acl(server.store(), bucket, &project);
                Response::json_ok(&BucketAclListResponse {
                    kind: "storage#bucketAccessControls".to_string(),
                    items: bucket_acl_resources(bucket, &entries),
                })
            }
            "POST" => {
                let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                    Ok(v) => v,
                    Err(_) => return json_error(400, "invalid", "invalid json request"),
                };
                if parsed.entity.trim().is_empty() {
                    return json_error(400, "required", "entity is required");
                }
                if !validate_bucket_role(&parsed.role) {
                    return json_error(400, "invalid", "invalid ACL role");
                }
                let mut entries = effective_bucket_acl(server.store(), bucket, &project);
                let new_entry = build_acl_entry(parsed.entity.trim(), &parsed.role);
                upsert_acl_entry(&mut entries, new_entry.clone());
                if write_bucket_acl(server.store(), bucket, &entries).is_err() {
                    return json_error(500, "backendError", "internal error");
                }
                Response::json_ok(&bucket_acl_resource(bucket, &new_entry))
            }
            _ => method_not_allowed("GET, POST"),
        },
        Some(raw) => {
            let entity = url_decode(raw);
            if entity.is_empty() {
                return json_error(400, "invalid", "invalid entity");
            }
            let mut entries = effective_bucket_acl(server.store(), bucket, &project);
            match req.method.as_str() {
                "GET" => match entries.iter().find(|e| e.entity == entity) {
                    Some(e) => Response::json_ok(&bucket_acl_resource(bucket, e)),
                    None => json_error(404, "notFound", "ACL entity not found"),
                },
                "PUT" | "PATCH" => {
                    let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                        Ok(v) => v,
                        Err(_) => return json_error(400, "invalid", "invalid json request"),
                    };
                    if !validate_bucket_role(&parsed.role) {
                        return json_error(400, "invalid", "invalid ACL role");
                    }
                    let updated = build_acl_entry(&entity, &parsed.role);
                    upsert_acl_entry(&mut entries, updated.clone());
                    if write_bucket_acl(server.store(), bucket, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    Response::json_ok(&bucket_acl_resource(bucket, &updated))
                }
                "DELETE" => {
                    let before = entries.len();
                    entries.retain(|e| e.entity != entity);
                    if entries.len() == before {
                        return json_error(404, "notFound", "ACL entity not found");
                    }
                    if write_bucket_acl(server.store(), bucket, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    Response::empty_status(204)
                }
                _ => method_not_allowed("GET, PUT, PATCH, DELETE"),
            }
        }
    }
}

// --- Default Object ACL JSON endpoints -------------------------------------

pub fn handle_default_object_acl(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    entity_escaped: Option<&str>,
) -> Response {
    if let Err(r) = require_bucket(server.store(), bucket) {
        return r;
    }
    if let Err(r) = check_uniform_access_blocks_acl(server.store(), bucket) {
        return r;
    }
    let mut entries = match read_default_object_acl(server.store(), bucket) {
        Ok(v) => v,
        Err(_) => return json_error(500, "backendError", "internal error"),
    };
    match entity_escaped {
        None => match req.method.as_str() {
            "GET" => {
                let items = entries
                    .iter()
                    .map(|e| default_object_acl_resource(bucket, e))
                    .collect();
                Response::json_ok(&ObjectAclListResponse {
                    kind: "storage#objectAccessControls".to_string(),
                    items,
                })
            }
            "POST" => {
                let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                    Ok(v) => v,
                    Err(_) => return json_error(400, "invalid", "invalid json request"),
                };
                if parsed.entity.trim().is_empty() {
                    return json_error(400, "required", "entity is required");
                }
                if !validate_object_role(&parsed.role) {
                    return json_error(400, "invalid", "invalid object ACL role");
                }
                let entry = build_acl_entry(parsed.entity.trim(), &parsed.role);
                upsert_acl_entry(&mut entries, entry.clone());
                if write_default_object_acl(server.store(), bucket, &entries).is_err() {
                    return json_error(500, "backendError", "internal error");
                }
                Response::json_ok(&default_object_acl_resource(bucket, &entry))
            }
            _ => method_not_allowed("GET, POST"),
        },
        Some(raw) => {
            let entity = url_decode(raw);
            if entity.is_empty() {
                return json_error(400, "invalid", "invalid entity");
            }
            match req.method.as_str() {
                "GET" => match entries.iter().find(|e| e.entity == entity) {
                    Some(e) => Response::json_ok(&default_object_acl_resource(bucket, e)),
                    None => json_error(404, "notFound", "ACL entity not found"),
                },
                "PUT" | "PATCH" => {
                    let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                        Ok(v) => v,
                        Err(_) => return json_error(400, "invalid", "invalid json request"),
                    };
                    if !validate_object_role(&parsed.role) {
                        return json_error(400, "invalid", "invalid object ACL role");
                    }
                    let updated = build_acl_entry(&entity, &parsed.role);
                    upsert_acl_entry(&mut entries, updated.clone());
                    if write_default_object_acl(server.store(), bucket, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    Response::json_ok(&default_object_acl_resource(bucket, &updated))
                }
                "DELETE" => {
                    let before = entries.len();
                    entries.retain(|e| e.entity != entity);
                    if entries.len() == before {
                        return json_error(404, "notFound", "ACL entity not found");
                    }
                    if write_default_object_acl(server.store(), bucket, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    Response::empty_status(204)
                }
                _ => method_not_allowed("GET, PUT, PATCH, DELETE"),
            }
        }
    }
}

// --- Object ACL JSON endpoints ---------------------------------------------

pub fn handle_object_acl(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    key: &str,
    entity_escaped: Option<&str>,
) -> Response {
    if let Err(r) = require_bucket(server.store(), bucket) {
        return r;
    }
    if let Err(r) = check_uniform_access_blocks_acl(server.store(), bucket) {
        return r;
    }
    let object = match server.store().get_object_snapshot(bucket, key) {
        Ok(Some((obj, _))) => obj,
        Ok(None) => return json_error(404, "notFound", "object not found"),
        Err(_) => return json_error(404, "notFound", "bucket not found"),
    };
    let generation = crate::http::object_generation_string(&object);
    let project = server.project().to_string();
    let mut entries = effective_object_acl(server.store(), bucket, key, &project);
    match entity_escaped {
        None => match req.method.as_str() {
            "GET" => {
                let items = entries
                    .iter()
                    .map(|e| object_acl_resource(bucket, key, &generation, e))
                    .collect();
                Response::json_ok(&ObjectAclListResponse {
                    kind: "storage#objectAccessControls".to_string(),
                    items,
                })
            }
            "POST" => {
                let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                    Ok(v) => v,
                    Err(_) => return json_error(400, "invalid", "invalid json request"),
                };
                if parsed.entity.trim().is_empty() {
                    return json_error(400, "required", "entity is required");
                }
                if !validate_object_role(&parsed.role) {
                    return json_error(400, "invalid", "invalid object ACL role");
                }
                let entry = build_acl_entry(parsed.entity.trim(), &parsed.role);
                upsert_acl_entry(&mut entries, entry.clone());
                if write_object_acl(server.store(), bucket, key, &entries).is_err() {
                    return json_error(500, "backendError", "internal error");
                }
                crate::notifications::emit_object_notification(
                    server,
                    "OBJECT_METADATA_UPDATE",
                    &object,
                );
                Response::json_ok(&object_acl_resource(bucket, key, &generation, &entry))
            }
            _ => method_not_allowed("GET, POST"),
        },
        Some(raw) => {
            let entity = url_decode(raw);
            if entity.is_empty() {
                return json_error(400, "invalid", "invalid entity");
            }
            match req.method.as_str() {
                "GET" => match entries.iter().find(|e| e.entity == entity) {
                    Some(e) => Response::json_ok(&object_acl_resource(bucket, key, &generation, e)),
                    None => json_error(404, "notFound", "ACL entity not found"),
                },
                "PUT" | "PATCH" => {
                    let parsed: AclMutationRequest = match serde_json::from_slice(&req.body) {
                        Ok(v) => v,
                        Err(_) => return json_error(400, "invalid", "invalid json request"),
                    };
                    if !validate_object_role(&parsed.role) {
                        return json_error(400, "invalid", "invalid object ACL role");
                    }
                    let updated = build_acl_entry(&entity, &parsed.role);
                    upsert_acl_entry(&mut entries, updated.clone());
                    if write_object_acl(server.store(), bucket, key, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    crate::notifications::emit_object_notification(
                        server,
                        "OBJECT_METADATA_UPDATE",
                        &object,
                    );
                    Response::json_ok(&object_acl_resource(bucket, key, &generation, &updated))
                }
                "DELETE" => {
                    let before = entries.len();
                    entries.retain(|e| e.entity != entity);
                    if entries.len() == before {
                        return json_error(404, "notFound", "ACL entity not found");
                    }
                    if write_object_acl(server.store(), bucket, key, &entries).is_err() {
                        return json_error(500, "backendError", "internal error");
                    }
                    crate::notifications::emit_object_notification(
                        server,
                        "OBJECT_METADATA_UPDATE",
                        &object,
                    );
                    Response::empty_status(204)
                }
                _ => method_not_allowed("GET, PUT, PATCH, DELETE"),
            }
        }
    }
}

fn upsert_acl_entry(entries: &mut Vec<AccessControlEntry>, entry: AccessControlEntry) {
    if let Some(existing) = entries.iter_mut().find(|e| e.entity == entry.entity) {
        *existing = entry;
    } else {
        entries.push(entry);
    }
    entries.sort_by(|a, b| a.entity.cmp(&b.entity));
}

// --- XML ACL rendering & parsing -------------------------------------------

pub fn render_acl_xml(entries: &[AccessControlEntry]) -> Vec<u8> {
    let mut entries_el = Element::new("Entries");
    for entry in entries {
        let permission = match entry.role.as_str() {
            "OWNER" => "FULL_CONTROL",
            "WRITER" => "WRITE",
            _ => "READ",
        };
        let scope = if entry.entity == "allUsers" {
            Element::new("Scope").attr("type", "AllUsers")
        } else if entry.entity == "allAuthenticatedUsers" {
            Element::new("Scope").attr("type", "AllAuthenticatedUsers")
        } else if !entry.email.is_empty() {
            let scope_type = if entry.entity.starts_with("group-") {
                "GroupByEmail"
            } else {
                "UserByEmail"
            };
            Element::new("Scope")
                .attr("type", scope_type)
                .text_child("EmailAddress", &entry.email)
        } else if !entry.domain.is_empty() {
            Element::new("Scope")
                .attr("type", "Domain")
                .text_child("Domain", &entry.domain)
        } else {
            Element::new("Scope")
                .attr("type", "UserById")
                .text_child("ID", &entry.entity)
        };
        entries_el = entries_el.child(
            Element::new("Entry")
                .child(scope)
                .text_child("Permission", permission),
        );
    }
    let root = Element::new("AccessControlList")
        .child(Element::new("Owner").text_child("ID", "devcloud"))
        .child(entries_el);
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(&root));
    out
}

pub fn parse_acl_xml(xml: &str, for_bucket: bool) -> Result<Vec<AccessControlEntry>, String> {
    let mut entries = Vec::new();
    let mut cursor = xml;
    while let Some(start) = cursor.find("<Entry") {
        let rest = &cursor[start..];
        let Some(end) = rest.find("</Entry>") else {
            return Err("malformed Entry element in ACL XML".to_string());
        };
        let block = &rest[..end + "</Entry>".len()];
        cursor = &rest[end + "</Entry>".len()..];

        let permission = extract_tag_text(block, "Permission")
            .ok_or_else(|| "missing Permission in ACL Entry".to_string())?;
        let role = match permission.trim() {
            "FULL_CONTROL" | "OWNER" => "OWNER",
            "WRITE" | "WRITER" => {
                if !for_bucket {
                    return Err("WRITE permission is only valid for bucket ACLs".to_string());
                }
                "WRITER"
            }
            "READ" | "READER" => "READER",
            other => return Err(format!("invalid ACL permission {other:?}")),
        };
        let entity = if block.contains("AllUsers") {
            "allUsers".to_string()
        } else if block.contains("AllAuthenticatedUsers") {
            "allAuthenticatedUsers".to_string()
        } else if let Some(email) = extract_tag_text(block, "EmailAddress") {
            if block.contains("GroupByEmail") {
                format!("group-{}", email.trim())
            } else {
                format!("user-{}", email.trim())
            }
        } else if let Some(domain) = extract_tag_text(block, "Domain") {
            format!("domain-{}", domain.trim())
        } else if let Some(id) = extract_tag_text(block, "ID") {
            id.trim().to_string()
        } else {
            return Err("missing Scope target in ACL Entry".to_string());
        };
        upsert_acl_entry(&mut entries, build_acl_entry(&entity, role));
    }
    Ok(entries)
}

pub(crate) fn extract_tag_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open_prefix = format!("<{tag}");
    let start = xml.find(&open_prefix)?;
    let after_open = &xml[start + open_prefix.len()..];
    let gt = after_open.find('>')?;
    let content = &after_open[gt + 1..];
    let close = format!("</{tag}>");
    let end = content.find(&close)?;
    Some(&content[..end])
}
