//! GCS XML API surface (path-style `/{bucket}/{object}` and virtual-host style
//! `{bucket}.localhost` / `{bucket}.storage.googleapis.com`).
//!
//! Covers service bucket listing, bucket CRUD + `?acl`/`?lifecycle`/`?location`/
//! `?storageClass`/`?versioning`, object PUT/Copy/GET/HEAD/DELETE + `?acl`, and
//! multipart uploads (`?uploads` / `?uploadId`, see `xml_multipart`).

use std::collections::BTreeMap;

use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::{ConditionalError, StoreError};
use devcloud_s3::validation::valid_bucket_name;
use devcloud_s3::xml::Element;

use crate::http::{
    apply_response_header_overrides, emit_dashboard_event, object_generation_string,
    object_generation_value, url_decode_path, user_metadata, Request, Response, Server,
};
use crate::iam_acl::{
    effective_bucket_acl, effective_object_acl, expand_predefined_acl, extract_tag_text,
    parse_acl_xml, render_acl_xml,
};
use crate::lifecycle::{
    apply_bucket_lifecycle, effective_storage_class, parse_lifecycle_xml, render_lifecycle_xml,
    validate_lifecycle,
};
use crate::notifications::emit_object_notification;
use crate::retention::{object_mutation_blocked_reason, validate_object_retention_update};
use crate::state::{
    delete_object_sidecars, read_bucket_meta, read_object_meta, write_bucket_acl,
    write_bucket_meta, write_object_acl, write_object_meta, GcsBucketMeta, GcsLifecycle,
    GcsObjectMeta, GcsObjectRetention,
};

pub fn xml_response(status: u16, body: Vec<u8>) -> Response {
    let mut headers = BTreeMap::new();
    headers.insert(
        "Content-Type".to_string(),
        "application/xml; charset=utf-8".to_string(),
    );
    Response {
        status,
        headers,
        body,
    }
}

pub fn xml_error(status: u16, code: &str, message: &str) -> Response {
    let root = Element::new("Error")
        .text_child("Code", code)
        .text_child("Message", message);
    let mut body = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    body.extend_from_slice(&devcloud_s3::xml::encode(&root));
    xml_response(status, body)
}

pub(crate) fn virtual_host_bucket(host: &str) -> Option<String> {
    let host_only = host.split(':').next().unwrap_or("").trim();
    if host_only.is_empty() {
        return None;
    }
    let bucket = host_only
        .strip_suffix(".storage.googleapis.com")
        .or_else(|| host_only.strip_suffix(".localhost"))?;
    if !bucket.is_empty() && !bucket.contains('.') && valid_bucket_name(bucket) {
        Some(bucket.to_string())
    } else {
        None
    }
}

pub fn handle_xml_request(server: &mut Server, req: &Request) -> Response {
    if let Some(vhost_bucket) = virtual_host_bucket(req.header("host")) {
        let trimmed = req.path.trim_start_matches('/');
        if trimmed.is_empty() {
            return handle_xml_bucket(server, req, &vhost_bucket);
        }
        let key = url_decode_path(trimmed);
        return handle_xml_object(server, req, &vhost_bucket, &key);
    }

    let trimmed = req.path.trim_start_matches('/');
    if trimmed.is_empty() {
        return handle_xml_service(server, req);
    }
    match trimmed.split_once('/') {
        None => {
            let bucket = url_decode_path(trimmed);
            handle_xml_bucket(server, req, &bucket)
        }
        Some((raw_bucket, raw_key)) => {
            let bucket = url_decode_path(raw_bucket);
            if raw_key.is_empty() {
                handle_xml_bucket(server, req, &bucket)
            } else {
                let key = url_decode_path(raw_key);
                handle_xml_object(server, req, &bucket, &key)
            }
        }
    }
}

fn handle_xml_service(server: &mut Server, req: &Request) -> Response {
    if req.method != "GET" {
        return xml_error(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        );
    }
    let prefix = req.query_value("prefix");
    let buckets = match server.store().list_buckets() {
        Ok(b) => b,
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let mut buckets_el = Element::new("Buckets");
    for bucket in buckets {
        if !prefix.is_empty() && !bucket.name.starts_with(prefix) {
            continue;
        }
        buckets_el = buckets_el.child(
            Element::new("Bucket")
                .text_child("Name", &bucket.name)
                .text_child("CreationDate", &bucket.created_at),
        );
    }
    let root = Element::new("ListAllMyBucketsResult")
        .attr("xmlns", "http://doc.s3.amazonaws.com/2006-03-01")
        .child(Element::new("Owner").text_child("ID", "devcloud"))
        .child(buckets_el);
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(&root));
    xml_response(200, out)
}

fn handle_xml_bucket(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if !valid_bucket_name(bucket) {
        return xml_error(
            400,
            "InvalidBucketName",
            "The specified bucket is not valid.",
        );
    }
    match req.method.as_str() {
        "PUT" => {
            if req.query.contains_key("acl") {
                return handle_xml_put_bucket_acl(server, req, bucket);
            }
            if req.query.contains_key("lifecycle") {
                return handle_xml_put_bucket_lifecycle(server, req, bucket);
            }
            handle_xml_create_bucket(server, req, bucket)
        }
        "HEAD" => match server.store().get_bucket(bucket) {
            Ok(Some(_)) => {
                let meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
                let mut r = Response::empty_status(200);
                r.headers.insert(
                    "x-goog-metageneration".to_string(),
                    meta.metageneration.max(1).to_string(),
                );
                r
            }
            Ok(None) => xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
            Err(_) => xml_error(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
            ),
        },
        "DELETE" => match server.store().delete_bucket(bucket) {
            Ok(true) => {
                emit_dashboard_event("gcs.bucket.deleted", serde_json::json!({"bucket": bucket}));
                Response::empty_status(204)
            }
            Ok(false) => xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
            Err(StoreError::BucketNotEmpty) => xml_error(
                409,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty.",
            ),
            Err(_) => xml_error(500, "InternalError", "internal error"),
        },
        "GET" => {
            if server.store().get_bucket(bucket).ok().flatten().is_none() {
                return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.");
            }
            if req.query.contains_key("acl") {
                return handle_xml_get_bucket_acl(server, bucket);
            }
            if req.query.contains_key("uploads") {
                return crate::xml_multipart::list_uploads(server, req, bucket);
            }
            if req.query.contains_key("lifecycle") {
                return handle_xml_get_bucket_lifecycle(server, bucket);
            }
            if req.query.contains_key("location") {
                let meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
                let loc = if !meta.location.is_empty() {
                    meta.location
                } else {
                    server.default_location().to_string()
                };
                let root = Element::new("LocationConstraint").text(&loc);
                let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
                out.extend_from_slice(&devcloud_s3::xml::encode(&root));
                return xml_response(200, out);
            }
            if req.query.contains_key("storageClass") {
                let meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
                let sc = if !meta.storage_class.is_empty() {
                    meta.storage_class
                } else {
                    "STANDARD".to_string()
                };
                let root = Element::new("StorageClass").text(&sc);
                let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
                out.extend_from_slice(&devcloud_s3::xml::encode(&root));
                return xml_response(200, out);
            }
            if req.query.contains_key("versioning") {
                let root = Element::new("VersioningConfiguration");
                let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
                out.extend_from_slice(&devcloud_s3::xml::encode(&root));
                return xml_response(200, out);
            }
            handle_xml_list_objects(server, req, bucket)
        }
        _ => xml_error(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
    }
}

fn handle_xml_create_bucket(server: &mut Server, req: &Request, bucket: &str) -> Response {
    let body_str = String::from_utf8_lossy(&req.body);
    let location = extract_tag_text(&body_str, "LocationConstraint")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let storage_class = extract_tag_text(&body_str, "StorageClass")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let predefined_acl = req.header("x-goog-acl").trim();
    let acl_entries = if !predefined_acl.is_empty() {
        match expand_predefined_acl(predefined_acl, server.project(), true) {
            Ok(v) => Some(v),
            Err(e) => return xml_error(400, "InvalidArgument", &e),
        }
    } else {
        None
    };

    match server.store().create_bucket(bucket) {
        Ok((created, true)) => {
            let meta = GcsBucketMeta {
                location,
                storage_class,
                metageneration: 1,
                updated: created.created_at.clone(),
                ..Default::default()
            };
            let _ = write_bucket_meta(server.store(), bucket, &meta);
            if let Some(entries) = acl_entries {
                let _ = write_bucket_acl(server.store(), bucket, &entries);
            }
            emit_dashboard_event("gcs.bucket.created", serde_json::json!({"bucket": bucket}));
            Response::empty_status(200)
        }
        Ok((_, false)) => xml_error(
            409,
            "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded and you already own it.",
        ),
        Err(_) => xml_error(
            400,
            "InvalidBucketName",
            "The specified bucket is not valid.",
        ),
    }
}

fn handle_xml_get_bucket_acl(server: &Server, bucket: &str) -> Response {
    if let Ok(meta) = read_bucket_meta(server.store(), bucket) {
        if meta.uniform_bucket_level_access_enabled() {
            return xml_error(
                400,
                "InvalidArgument",
                "Cannot get legacy ACL when uniform bucket-level access is enabled.",
            );
        }
    }
    let entries = effective_bucket_acl(server.store(), bucket, server.project());
    xml_response(200, render_acl_xml(&entries))
}

fn handle_xml_put_bucket_acl(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if server.store().get_bucket(bucket).ok().flatten().is_none() {
        return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.");
    }
    if let Ok(meta) = read_bucket_meta(server.store(), bucket) {
        if meta.uniform_bucket_level_access_enabled() {
            return xml_error(
                400,
                "InvalidArgument",
                "Cannot update legacy ACL when uniform bucket-level access is enabled.",
            );
        }
    }
    let predefined = req.header("x-goog-acl").trim();
    let entries = if !predefined.is_empty() {
        match expand_predefined_acl(predefined, server.project(), true) {
            Ok(v) => v,
            Err(e) => return xml_error(400, "InvalidArgument", &e),
        }
    } else {
        let xml = String::from_utf8_lossy(&req.body);
        match parse_acl_xml(&xml, true) {
            Ok(v) => v,
            Err(e) => return xml_error(400, "MalformedACLError", &e),
        }
    };
    if write_bucket_acl(server.store(), bucket, &entries).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    Response::empty_status(200)
}

fn handle_xml_get_bucket_lifecycle(server: &Server, bucket: &str) -> Response {
    let meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    let lifecycle = meta.lifecycle.unwrap_or_default();
    xml_response(200, render_lifecycle_xml(&lifecycle))
}

fn handle_xml_put_bucket_lifecycle(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if server.store().get_bucket(bucket).ok().flatten().is_none() {
        return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.");
    }
    let lifecycle = if req.body.is_empty() {
        GcsLifecycle::default()
    } else {
        let xml = String::from_utf8_lossy(&req.body);
        match parse_lifecycle_xml(&xml) {
            Ok(v) => v,
            Err(e) => return xml_error(400, "MalformedXML", &e),
        }
    };
    if validate_lifecycle(&lifecycle).is_err() {
        return xml_error(400, "MalformedXML", "invalid lifecycle configuration");
    }
    let mut meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    meta.lifecycle = if lifecycle.rule.is_empty() {
        None
    } else {
        Some(lifecycle)
    };
    meta.metageneration = meta.metageneration.max(1) + 1;
    meta.updated = server.now();
    if write_bucket_meta(server.store(), bucket, &meta).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    Response::empty_status(200)
}

fn handle_xml_list_objects(server: &mut Server, req: &Request, bucket: &str) -> Response {
    apply_bucket_lifecycle(server, bucket);
    let prefix = req.query_value("prefix");
    let delimiter = req.query_value("delimiter");
    let is_v2 = req.query_value("list-type") == "2";
    let marker = if is_v2 {
        let cont = req.query_value("continuation-token");
        if !cont.is_empty() {
            cont
        } else {
            req.query_value("start-after")
        }
    } else {
        req.query_value("marker")
    };
    let max_keys: usize = if req.query_value("max-keys").is_empty() {
        1000
    } else {
        match req.query_value("max-keys").parse::<usize>() {
            Ok(v) => v,
            Err(_) => return xml_error(400, "InvalidArgument", "invalid max-keys"),
        }
    };

    let objects = match server.store().list_objects(bucket, prefix) {
        Ok(Some(v)) => v,
        Ok(None) => return xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };

    let mut entries: Vec<(String, Option<devcloud_s3::model::Object>)> = Vec::new();
    let mut seen_prefixes: BTreeMap<String, ()> = BTreeMap::new();
    for obj in objects {
        if !delimiter.is_empty() {
            let remainder = obj.key.strip_prefix(prefix).unwrap_or(&obj.key);
            if let Some(idx) = remainder.find(delimiter) {
                let p = format!("{}{}", prefix, &remainder[..idx + delimiter.len()]);
                if !marker.is_empty() && p.as_str() <= marker {
                    continue;
                }
                seen_prefixes.insert(p, ());
                continue;
            }
        }
        if !marker.is_empty() && obj.key.as_str() <= marker {
            continue;
        }
        entries.push((obj.key.clone(), Some(obj)));
    }
    for p in seen_prefixes.into_keys() {
        entries.push((p, None));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let truncated = entries.len() > max_keys;
    let slice = &entries[..entries.len().min(max_keys)];
    let next_token = if truncated {
        slice
            .last()
            .map(|(name, _)| name.clone())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let mut root = Element::new("ListBucketResult")
        .attr("xmlns", "http://doc.s3.amazonaws.com/2006-03-01")
        .text_child("Name", bucket)
        .text_child("Prefix", prefix);
    if is_v2 {
        if !req.query_value("continuation-token").is_empty() {
            root = root.text_child("ContinuationToken", req.query_value("continuation-token"));
        }
        root = root.text_child("KeyCount", &slice.len().to_string());
    } else {
        root = root.text_child("Marker", marker);
    }
    root = root
        .text_child("MaxKeys", &max_keys.to_string())
        .text_child("IsTruncated", if truncated { "true" } else { "false" });
    if !delimiter.is_empty() {
        root = root.text_child("Delimiter", delimiter);
    }
    if truncated {
        if is_v2 {
            root = root.text_child("NextContinuationToken", &next_token);
        } else {
            root = root.text_child("NextMarker", &next_token);
        }
    }
    for (name, maybe_obj) in slice {
        if let Some(obj) = maybe_obj {
            root = root.child(
                Element::new("Contents")
                    .text_child("Key", &obj.key)
                    .text_child("Generation", &object_generation_string(obj))
                    .text_child("MetaGeneration", &obj.metageneration.max(1).to_string())
                    .text_child("LastModified", &obj.last_modified)
                    .text_child("ETag", &obj.etag)
                    .text_child("Size", &obj.size.to_string()),
            );
        } else {
            root = root.child(Element::new("CommonPrefixes").text_child("Prefix", name));
        }
    }
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(&root));
    xml_response(200, out)
}

fn handle_xml_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    if !valid_bucket_name(bucket) {
        return xml_error(
            400,
            "InvalidBucketName",
            "The specified bucket is not valid.",
        );
    }
    if key.is_empty() {
        return xml_error(400, "InvalidArgument", "object key is required");
    }
    if let Some(resp) = crate::xml_multipart::handle_object_multipart(server, req, bucket, key) {
        return resp;
    }
    match req.method.as_str() {
        "GET" | "HEAD" => {
            if req.query.contains_key("acl") && req.method == "GET" {
                return handle_xml_get_object_acl(server, bucket, key);
            }
            handle_xml_get_object(server, req, bucket, key)
        }
        "PUT" => {
            if req.query.contains_key("acl") {
                return handle_xml_put_object_acl(server, req, bucket, key);
            }
            if !req.header("x-goog-copy-source").is_empty() {
                return handle_xml_copy_object(server, req, bucket, key);
            }
            handle_xml_put_object(server, req, bucket, key)
        }
        "DELETE" => handle_xml_delete_object(server, req, bucket, key),
        _ => xml_error(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
    }
}

pub(crate) fn check_xml_preconditions(
    req: &Request,
    found: Option<&devcloud_s3::model::Object>,
) -> Result<(), Response> {
    let gen_actual = found.map(object_generation_value).unwrap_or(0);
    let meta_actual = found.map(|o| o.metageneration.max(1)).unwrap_or(0);
    let gen_hdr = req.header("x-goog-if-generation-match").trim();
    if !gen_hdr.is_empty() {
        let Ok(expected) = gen_hdr.parse::<i64>() else {
            return Err(xml_error(
                400,
                "InvalidArgument",
                "invalid x-goog-if-generation-match",
            ));
        };
        if expected != gen_actual {
            return Err(xml_error(
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold.",
            ));
        }
    }
    let meta_hdr = req.header("x-goog-if-metageneration-match").trim();
    if !meta_hdr.is_empty() {
        let Ok(expected) = meta_hdr.parse::<i64>() else {
            return Err(xml_error(
                400,
                "InvalidArgument",
                "invalid x-goog-if-metageneration-match",
            ));
        };
        if expected != meta_actual {
            return Err(xml_error(
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold.",
            ));
        }
    }
    Ok(())
}

fn handle_xml_get_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    apply_bucket_lifecycle(server, bucket);
    let (object, body) = match server.store().get_object_snapshot(bucket, key) {
        Ok(Some(v)) => v,
        Ok(None) => return xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(StoreError::BucketNotExist) => {
            return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let requested_gen = req.query_value("generation").trim();
    if !requested_gen.is_empty() {
        match requested_gen.parse::<i64>() {
            Ok(g) if g > 0 => {
                if g != object_generation_value(&object) {
                    return xml_error(404, "NoSuchKey", "The specified key does not exist.");
                }
            }
            _ => return xml_error(400, "InvalidArgument", "invalid generation"),
        }
    }
    if let Err(r) = check_xml_preconditions(req, Some(&object)) {
        return r;
    }
    let if_match = req.header("if-match").trim();
    if !if_match.is_empty() && if_match != "*" && if_match != object.etag {
        return xml_error(
            412,
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold.",
        );
    }
    let if_none_match = req.header("if-none-match").trim();
    if !if_none_match.is_empty() && (if_none_match == "*" || if_none_match == object.etag) {
        let mut r = Response::empty_status(304);
        r.headers.insert("ETag".to_string(), object.etag.clone());
        return r;
    }

    let mut resp = crate::http::build_media_response(server.store(), req, &object, &body, true);
    apply_response_header_overrides(req, &mut resp);
    resp
}

fn handle_xml_put_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    if server.store().get_bucket(bucket).ok().flatten().is_none() {
        return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.");
    }
    let bucket_meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    let predefined_acl = req.header("x-goog-acl").trim();
    let acl_entries = if !predefined_acl.is_empty() {
        if bucket_meta.uniform_bucket_level_access_enabled() {
            return xml_error(
                400,
                "InvalidArgument",
                "Cannot specify ACL when uniform bucket-level access is enabled.",
            );
        }
        match expand_predefined_acl(predefined_acl, server.project(), false) {
            Ok(v) => Some(v),
            Err(e) => return xml_error(400, "InvalidArgument", &e),
        }
    } else {
        None
    };

    let ret_mode = req.header("x-goog-object-retention-mode").trim();
    let ret_until = req
        .header("x-goog-object-retention-retain-until-time")
        .trim();
    let requested_retention = if !ret_mode.is_empty() || !ret_until.is_empty() {
        Some(GcsObjectRetention {
            mode: ret_mode.to_string(),
            retain_until_time: ret_until.to_string(),
        })
    } else {
        None
    };
    let validated_retention = if requested_retention.is_some() {
        match validate_object_retention_update(
            &bucket_meta,
            &None,
            requested_retention,
            false,
            &server.now(),
        ) {
            Ok(v) => v,
            Err(r) => {
                return xml_error(
                    r.status,
                    if r.status == 403 {
                        "AccessDenied"
                    } else {
                        "InvalidArgument"
                    },
                    "invalid object retention configuration",
                )
            }
        }
    } else {
        None
    };

    let now = server.now();
    let store = server.store();
    let result = store.put_object_if(
        PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: req.body.clone(),
            content_md5: req.header("content-md5").to_string(),
            content_type: req.header("content-type").to_string(),
            content_encoding: req.header("content-encoding").to_string(),
            cache_control: req.header("cache-control").to_string(),
            content_disposition: req.header("content-disposition").to_string(),
            metadata: user_metadata(&req.headers),
            ..Default::default()
        },
        |current| {
            check_xml_preconditions(req, current)?;
            if let Some(existing) = current {
                if let Some(reason) = object_mutation_blocked_reason(store, bucket, existing, &now)
                {
                    return Err(xml_error(403, "AccessDenied", &reason));
                }
            }
            Ok(())
        },
    );

    let object = match result {
        Ok(obj) => obj,
        Err(ConditionalError::Condition(resp)) => return resp,
        Err(ConditionalError::Store(StoreError::InvalidContentMd5)) => {
            return xml_error(
                400,
                "InvalidDigest",
                "The Content-MD5 you specified is invalid.",
            )
        }
        Err(ConditionalError::Store(StoreError::ContentMd5Mismatch)) => {
            return xml_error(
                400,
                "BadDigest",
                "The Content-MD5 you specified did not match what we received.",
            )
        }
        Err(ConditionalError::Store(StoreError::BucketNotExist)) => {
            return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };

    let obj_meta = GcsObjectMeta {
        storage_class: req
            .header("x-goog-storage-class")
            .trim()
            .to_ascii_uppercase(),
        temporary_hold: false,
        event_based_hold: bucket_meta.default_event_based_hold,
        retention_expiration_time: String::new(),
        retention: validated_retention,
    };
    let _ = write_object_meta(server.store(), bucket, key, &obj_meta);
    if let Some(entries) = acl_entries {
        let _ = write_object_acl(server.store(), bucket, key, &entries);
    }
    emit_dashboard_event(
        "gcs.object.put",
        serde_json::json!({
            "bucket": bucket,
            "key": key,
            "etag": object.etag,
            "contentLength": object.size,
        }),
    );
    emit_object_notification(server, "OBJECT_FINALIZE", &object);

    let mut resp = Response::empty_status(200);
    crate::http::set_object_headers_with_meta(&mut resp, &object, &bucket_meta, &obj_meta, true);
    resp
}

fn handle_xml_copy_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    let raw_source = req
        .header("x-goog-copy-source")
        .trim()
        .trim_start_matches('/');
    let Some((src_bucket_raw, src_key_raw)) = raw_source.split_once('/') else {
        return xml_error(400, "InvalidArgument", "invalid x-goog-copy-source header");
    };
    let src_bucket = url_decode_path(src_bucket_raw);
    let src_key = url_decode_path(src_key_raw);
    let (source, body) = match server.store().get_object_snapshot(&src_bucket, &src_key) {
        Ok(Some(v)) => v,
        Ok(None) => return xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(_) => return xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
    };
    let src_gen_hdr = req.header("x-goog-copy-source-if-generation-match").trim();
    if !src_gen_hdr.is_empty() {
        let Ok(expected) = src_gen_hdr.parse::<i64>() else {
            return xml_error(
                400,
                "InvalidArgument",
                "invalid source generation precondition",
            );
        };
        if expected != object_generation_value(&source) {
            return xml_error(
                412,
                "PreconditionFailed",
                "source generation precondition failed",
            );
        }
    }
    let src_meta_hdr = req
        .header("x-goog-copy-source-if-metageneration-match")
        .trim();
    if !src_meta_hdr.is_empty() {
        let Ok(expected) = src_meta_hdr.parse::<i64>() else {
            return xml_error(
                400,
                "InvalidArgument",
                "invalid source metageneration precondition",
            );
        };
        if expected != source.metageneration.max(1) {
            return xml_error(
                412,
                "PreconditionFailed",
                "source metageneration precondition failed",
            );
        }
    }

    let replace = req
        .header("x-goog-metadata-directive")
        .eq_ignore_ascii_case("REPLACE");
    let metadata = if replace {
        user_metadata(&req.headers)
    } else {
        source.metadata.clone()
    };
    let content_type = if replace && !req.header("content-type").is_empty() {
        req.header("content-type").to_string()
    } else {
        source.content_type.clone()
    };
    let content_encoding = if replace {
        req.header("content-encoding").to_string()
    } else {
        source.content_encoding.clone()
    };
    let cache_control = if replace {
        req.header("cache-control").to_string()
    } else {
        source.cache_control.clone()
    };
    let content_disposition = if replace {
        req.header("content-disposition").to_string()
    } else {
        source.content_disposition.clone()
    };

    let bucket_meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    let now = server.now();
    let store = server.store();
    let object = match store.put_object_if(
        PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body,
            content_type,
            content_encoding,
            cache_control,
            content_disposition,
            metadata,
            ..Default::default()
        },
        |current| {
            check_xml_preconditions(req, current)?;
            if let Some(existing) = current {
                if let Some(reason) = object_mutation_blocked_reason(store, bucket, existing, &now)
                {
                    return Err(xml_error(403, "AccessDenied", &reason));
                }
            }
            Ok(())
        },
    ) {
        Ok(obj) => obj,
        Err(ConditionalError::Condition(r)) => return r,
        Err(ConditionalError::Store(StoreError::BucketNotExist)) => {
            return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let obj_meta = GcsObjectMeta {
        storage_class: effective_storage_class(
            &bucket_meta,
            &read_object_meta(server.store(), &src_bucket, &src_key).unwrap_or_default(),
        ),
        event_based_hold: bucket_meta.default_event_based_hold,
        ..Default::default()
    };
    let _ = write_object_meta(server.store(), bucket, key, &obj_meta);
    emit_object_notification(server, "OBJECT_FINALIZE", &object);

    let root = Element::new("CopyObjectResult")
        .text_child("LastModified", &object.last_modified)
        .text_child("ETag", &object.etag);
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(&root));
    let mut resp = xml_response(200, out);
    crate::http::set_object_headers_with_meta(&mut resp, &object, &bucket_meta, &obj_meta, true);
    resp
}

fn handle_xml_delete_object(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let existing = match server.store().get_object_snapshot(bucket, key) {
        Ok(Some((obj, _))) => obj,
        Ok(None) => return xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(StoreError::BucketNotExist) => {
            return xml_error(404, "NoSuchBucket", "The specified bucket does not exist.")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let requested_gen = req.query_value("generation").trim();
    if !requested_gen.is_empty() {
        match requested_gen.parse::<i64>() {
            Ok(g) if g > 0 => {
                if g != object_generation_value(&existing) {
                    return xml_error(404, "NoSuchKey", "The specified key does not exist.");
                }
            }
            _ => return xml_error(400, "InvalidArgument", "invalid generation"),
        }
    }
    let now = server.now();
    let store = server.store();
    match store.delete_object_if(bucket, key, |current| {
        let Some(obj) = current else {
            return Err(xml_error(
                404,
                "NoSuchKey",
                "The specified key does not exist.",
            ));
        };
        check_xml_preconditions(req, Some(obj))?;
        if let Some(reason) = object_mutation_blocked_reason(store, bucket, obj, &now) {
            return Err(xml_error(403, "AccessDenied", &reason));
        }
        Ok(())
    }) {
        Ok(true) => {
            let _ = delete_object_sidecars(server.store(), bucket, key);
            emit_dashboard_event(
                "gcs.object.deleted",
                serde_json::json!({"bucket": bucket, "key": key}),
            );
            emit_object_notification(server, "OBJECT_DELETE", &existing);
            Response::empty_status(204)
        }
        Ok(false) => xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(ConditionalError::Condition(r)) => r,
        Err(ConditionalError::Store(StoreError::ObjectLocked)) => {
            xml_error(403, "AccessDenied", "object is locked")
        }
        Err(_) => xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
    }
}

fn handle_xml_get_object_acl(server: &Server, bucket: &str, key: &str) -> Response {
    if let Ok(meta) = read_bucket_meta(server.store(), bucket) {
        if meta.uniform_bucket_level_access_enabled() {
            return xml_error(
                400,
                "InvalidArgument",
                "Cannot get legacy ACL when uniform bucket-level access is enabled.",
            );
        }
    }
    match server.store().get_object_snapshot(bucket, key) {
        Ok(Some(_)) => {}
        Ok(None) => return xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(_) => return xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
    }
    let entries = effective_object_acl(server.store(), bucket, key, server.project());
    xml_response(200, render_acl_xml(&entries))
}

fn handle_xml_put_object_acl(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    if let Ok(meta) = read_bucket_meta(server.store(), bucket) {
        if meta.uniform_bucket_level_access_enabled() {
            return xml_error(
                400,
                "InvalidArgument",
                "Cannot update legacy ACL when uniform bucket-level access is enabled.",
            );
        }
    }
    let object = match server.store().get_object_snapshot(bucket, key) {
        Ok(Some((obj, _))) => obj,
        Ok(None) => return xml_error(404, "NoSuchKey", "The specified key does not exist."),
        Err(_) => return xml_error(404, "NoSuchBucket", "The specified bucket does not exist."),
    };
    let predefined = req.header("x-goog-acl").trim();
    let entries = if !predefined.is_empty() {
        match expand_predefined_acl(predefined, server.project(), false) {
            Ok(v) => v,
            Err(e) => return xml_error(400, "InvalidArgument", &e),
        }
    } else {
        let xml = String::from_utf8_lossy(&req.body);
        match parse_acl_xml(&xml, false) {
            Ok(v) => v,
            Err(e) => return xml_error(400, "MalformedACLError", &e),
        }
    };
    if write_object_acl(server.store(), bucket, key, &entries).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    emit_object_notification(server, "OBJECT_METADATA_UPDATE", &object);
    Response::empty_status(200)
}
