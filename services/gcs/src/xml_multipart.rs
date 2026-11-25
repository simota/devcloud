//! GCS XML API multipart uploads (S3-compatible wire format):
//!
//! - `POST /{bucket}/{object}?uploads` — initiate (`InitiateMultipartUploadResult`)
//! - `PUT /{bucket}/{object}?partNumber=N&uploadId=ID` — upload part (`ETag` header)
//! - `GET /{bucket}/{object}?uploadId=ID` — list parts (`ListPartsResult`)
//! - `POST /{bucket}/{object}?uploadId=ID` — complete (`CompleteMultipartUploadResult`)
//! - `DELETE /{bucket}/{object}?uploadId=ID` — abort
//! - `GET /{bucket}?uploads` — list in-progress uploads (`ListMultipartUploadsResult`)
//!
//! Parts are stored by the shared S3 `FileBucketStore` multipart plane. Like
//! Cloud Storage, every part except the last must be at least 5 MiB, part
//! numbers are 1..=10000, and completion requires ascending part numbers whose
//! ETags match the uploaded parts.
//!
//! Initiate accepts `x-goog-acl` (predefined object ACL), `x-goog-storage-class`,
//! and `x-goog-object-retention-mode` / `-retain-until-time`. They are validated
//! up front like a single-request XML `PUT`, kept in a per-upload sidecar, and
//! applied to the object when the upload completes.

use devcloud_s3::model::MultipartPart;
use devcloud_s3::objops::CreateMultipartUploadInput;
use devcloud_s3::store::{ConditionalError, StoreError};
use devcloud_s3::xml::Element;

use crate::http::{emit_dashboard_event, user_metadata, Request, Response, Server};
use crate::iam_acl::{expand_predefined_acl, extract_tag_text};
use crate::notifications::emit_object_notification;
use crate::retention::{object_mutation_blocked_reason, validate_object_retention_update};
use crate::state::{
    delete_multipart_init, read_bucket_meta, read_multipart_init, write_multipart_init,
    write_object_acl, write_object_meta, GcsMultipartInit, GcsObjectMeta, GcsObjectRetention,
};
use crate::xml_api::{check_xml_preconditions, xml_error, xml_response};

pub const MIN_PART_SIZE: i64 = 5 * 1024 * 1024;
const DEFAULT_MAX_PARTS: usize = 1000;
const DEFAULT_MAX_UPLOADS: usize = 1000;

fn xml_ok(root: &Element) -> Response {
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(root));
    xml_response(200, out)
}

fn no_such_bucket() -> Response {
    xml_error(404, "NoSuchBucket", "The specified bucket does not exist.")
}

fn no_such_upload() -> Response {
    xml_error(404, "NoSuchUpload", "The requested upload was not found.")
}

fn store_error(e: &StoreError) -> Response {
    match e {
        StoreError::BucketNotExist => no_such_bucket(),
        StoreError::InvalidUploadId | StoreError::MultipartUploadNotExist => no_such_upload(),
        StoreError::InvalidPartNumber => xml_error(
            400,
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive.",
        ),
        StoreError::InvalidPart(_) => xml_error(
            400,
            "InvalidPart",
            "One or more of the specified parts could not be found.",
        ),
        StoreError::InvalidContentMd5 => xml_error(
            400,
            "InvalidDigest",
            "The Content-MD5 you specified is invalid.",
        ),
        StoreError::ContentMd5Mismatch => xml_error(
            400,
            "BadDigest",
            "The Content-MD5 you specified did not match what we received.",
        ),
        _ => xml_error(500, "InternalError", "internal error"),
    }
}

/// Dispatches object-level multipart requests; `None` when `req` is not one.
pub fn handle_object_multipart(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Option<Response> {
    let has_upload_id = req.query.contains_key("uploadId");
    let resp = match req.method.as_str() {
        "POST" if req.query.contains_key("uploads") => initiate(server, req, bucket, key),
        "POST" if has_upload_id => complete(server, req, bucket, key),
        "PUT" if has_upload_id => upload_part(server, req, bucket, key),
        "GET" if has_upload_id => list_parts(server, req, bucket, key),
        "DELETE" if has_upload_id => abort(server, req, bucket, key),
        "POST" => xml_error(
            400,
            "InvalidArgument",
            "POST on an object requires the uploads or uploadId parameter.",
        ),
        _ => return None,
    };
    Some(resp)
}

/// Validates the object settings carried by initiate headers the same way a
/// single-request XML `PUT` does.
fn parse_initiate_settings(
    server: &Server,
    req: &Request,
    bucket: &str,
) -> Result<GcsMultipartInit, Response> {
    let bucket_meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    let predefined_acl = req.header("x-goog-acl").trim();
    let acl = if predefined_acl.is_empty() {
        None
    } else {
        if bucket_meta.uniform_bucket_level_access_enabled() {
            return Err(xml_error(
                400,
                "InvalidArgument",
                "Cannot specify ACL when uniform bucket-level access is enabled.",
            ));
        }
        Some(
            expand_predefined_acl(predefined_acl, server.project(), false)
                .map_err(|e| xml_error(400, "InvalidArgument", &e))?,
        )
    };
    let ret_mode = req.header("x-goog-object-retention-mode").trim();
    let ret_until = req
        .header("x-goog-object-retention-retain-until-time")
        .trim();
    let retention = if ret_mode.is_empty() && ret_until.is_empty() {
        None
    } else {
        validate_object_retention_update(
            &bucket_meta,
            &None,
            Some(GcsObjectRetention {
                mode: ret_mode.to_string(),
                retain_until_time: ret_until.to_string(),
            }),
            false,
            &server.now(),
        )
        .map_err(|r| {
            xml_error(
                r.status,
                if r.status == 403 {
                    "AccessDenied"
                } else {
                    "InvalidArgument"
                },
                "invalid object retention configuration",
            )
        })?
    };
    Ok(GcsMultipartInit {
        acl,
        storage_class: req
            .header("x-goog-storage-class")
            .trim()
            .to_ascii_uppercase(),
        retention,
    })
}

fn display_storage_class(init: &GcsMultipartInit) -> &str {
    if init.storage_class.is_empty() {
        "STANDARD"
    } else {
        &init.storage_class
    }
}

fn initiate(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    if server.store().get_bucket(bucket).ok().flatten().is_none() {
        return no_such_bucket();
    }
    let settings = match parse_initiate_settings(server, req, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let upload = match server
        .store()
        .create_multipart_upload(CreateMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: req.header("content-type").to_string(),
            content_encoding: req.header("content-encoding").to_string(),
            cache_control: req.header("cache-control").to_string(),
            content_disposition: req.header("content-disposition").to_string(),
            metadata: user_metadata(&req.headers),
            ..Default::default()
        }) {
        Ok(u) => u,
        Err(e) => return store_error(&e),
    };
    if settings != GcsMultipartInit::default()
        && write_multipart_init(server.store(), bucket, &upload.upload_id, &settings).is_err()
    {
        let _ = server
            .store()
            .abort_multipart_upload(bucket, key, &upload.upload_id);
        return xml_error(500, "InternalError", "internal error");
    }
    xml_ok(
        &Element::new("InitiateMultipartUploadResult")
            .attr("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
            .text_child("Bucket", bucket)
            .text_child("Key", key)
            .text_child("UploadId", &upload.upload_id),
    )
}

fn upload_part(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    let Ok(part_number) = req.query_value("partNumber").trim().parse::<i64>() else {
        return store_error(&StoreError::InvalidPartNumber);
    };
    let part = match server.store().upload_part(
        bucket,
        key,
        req.query_value("uploadId"),
        part_number,
        &req.body,
        req.header("content-md5"),
    ) {
        Ok(p) => p,
        Err(e) => return store_error(&e),
    };
    let mut resp = Response::empty_status(200);
    resp.headers.insert("ETag".to_string(), quoted(&part.etag));
    resp
}

fn quoted(etag: &str) -> String {
    format!("\"{}\"", etag.trim_matches('"'))
}

fn parse_page_size(raw: &str, default: usize) -> Result<usize, ()> {
    if raw.trim().is_empty() {
        return Ok(default);
    }
    match raw.trim().parse::<usize>() {
        Ok(n) => Ok(n.min(default)),
        Err(_) => Err(()),
    }
}

fn list_parts(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    let upload_id = req.query_value("uploadId");
    let (upload, parts) = match server.store().list_parts(bucket, key, upload_id) {
        Ok(Some(found)) => found,
        Ok(None) => return no_such_upload(),
        Err(e) => return store_error(&e),
    };
    let Ok(max_parts) = parse_page_size(req.query_value("max-parts"), DEFAULT_MAX_PARTS) else {
        return xml_error(400, "InvalidArgument", "invalid max-parts");
    };
    let marker_raw = req.query_value("part-number-marker").trim();
    let marker: i64 = if marker_raw.is_empty() {
        0
    } else {
        match marker_raw.parse() {
            Ok(v) if v >= 0 => v,
            _ => return xml_error(400, "InvalidArgument", "invalid part-number-marker"),
        }
    };
    let init = read_multipart_init(server.store(), bucket, &upload.upload_id).unwrap_or_default();
    let remaining: Vec<&MultipartPart> = parts.iter().filter(|p| p.part_number > marker).collect();
    let truncated = remaining.len() > max_parts;
    let page = &remaining[..remaining.len().min(max_parts)];
    let next_marker = page.last().map(|p| p.part_number).unwrap_or(marker);
    let mut root = Element::new("ListPartsResult")
        .attr("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
        .text_child("Bucket", bucket)
        .text_child("Key", &upload.key)
        .text_child("UploadId", &upload.upload_id)
        .text_child("StorageClass", display_storage_class(&init))
        .text_child("PartNumberMarker", &marker.to_string())
        .text_child("NextPartNumberMarker", &next_marker.to_string())
        .text_child("MaxParts", &max_parts.to_string())
        .text_child("IsTruncated", if truncated { "true" } else { "false" });
    for part in page {
        root = root.child(
            Element::new("Part")
                .text_child("PartNumber", &part.part_number.to_string())
                .text_child("LastModified", &part.last_modified)
                .text_child("ETag", &quoted(&part.etag))
                .text_child("Size", &part.size.to_string()),
        );
    }
    xml_ok(&root)
}

/// Parses `<CompleteMultipartUpload>` into `(part number, optional ETag)`.
fn parse_complete_body(body: &[u8]) -> Option<Vec<(i64, Option<String>)>> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<CompleteMultipartUpload") {
        return None;
    }
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Part>") {
        let after = &rest[start + "<Part>".len()..];
        let end = after.find("</Part>")?;
        let part_xml = &after[..end];
        let number = extract_tag_text(part_xml, "PartNumber")?
            .trim()
            .parse()
            .ok()?;
        let etag = extract_tag_text(part_xml, "ETag").map(|e| {
            e.trim()
                .replace("&quot;", "\"")
                .replace("&#34;", "\"")
                .trim_matches('"')
                .to_string()
        });
        out.push((number, etag));
        rest = &after[end + "</Part>".len()..];
    }
    Some(out)
}

fn complete(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    let upload_id = req.query_value("uploadId").to_string();
    let Some(requested) = parse_complete_body(&req.body).filter(|p| !p.is_empty()) else {
        return xml_error(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate.",
        );
    };
    let mut previous = 0;
    for (number, _) in &requested {
        if *number <= previous {
            return xml_error(
                400,
                "InvalidPartOrder",
                "The list of parts was not in ascending order.",
            );
        }
        previous = *number;
    }
    let stored = match server.store().list_parts(bucket, key, &upload_id) {
        Ok(Some((_, parts))) => parts,
        Ok(None) => return no_such_upload(),
        Err(e) => return store_error(&e),
    };
    for (index, (number, etag)) in requested.iter().enumerate() {
        let Some(part) = stored.iter().find(|p| p.part_number == *number) else {
            return store_error(&StoreError::InvalidPart(*number));
        };
        if etag
            .as_deref()
            .is_some_and(|e| e != part.etag.trim_matches('"'))
        {
            return store_error(&StoreError::InvalidPart(*number));
        }
        if index + 1 < requested.len() && part.size < MIN_PART_SIZE {
            return xml_error(
                400,
                "EntityTooSmall",
                "Your proposed upload is smaller than the minimum allowed object size.",
            );
        }
    }
    let part_numbers: Vec<i64> = requested.iter().map(|(n, _)| *n).collect();

    let now = server.now();
    let store = server.store();
    let completed =
        store.complete_multipart_upload_if(bucket, key, &upload_id, &part_numbers, |current| {
            check_xml_preconditions(req, current)?;
            if let Some(existing) = current {
                if let Some(reason) = object_mutation_blocked_reason(store, bucket, existing, &now)
                {
                    return Err(xml_error(403, "AccessDenied", &reason));
                }
            }
            Ok(())
        });
    let object = match completed {
        Ok(Some(object)) => object,
        Ok(None) => return no_such_upload(),
        Err(ConditionalError::Condition(resp)) => return resp,
        Err(ConditionalError::Store(e)) => return store_error(&e),
    };

    let init = read_multipart_init(server.store(), bucket, &upload_id).unwrap_or_default();
    let bucket_meta = read_bucket_meta(server.store(), bucket).unwrap_or_default();
    let obj_meta = GcsObjectMeta {
        storage_class: init.storage_class,
        event_based_hold: bucket_meta.default_event_based_hold,
        retention: init.retention,
        ..Default::default()
    };
    let _ = write_object_meta(server.store(), bucket, key, &obj_meta);
    if let Some(entries) = init.acl {
        let _ = write_object_acl(server.store(), bucket, key, &entries);
    }
    let _ = delete_multipart_init(server.store(), bucket, &upload_id);
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

    let mut resp = xml_ok(
        &Element::new("CompleteMultipartUploadResult")
            .attr("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
            .text_child("Location", &format!("/{bucket}/{key}"))
            .text_child("Bucket", bucket)
            .text_child("Key", key)
            .text_child("ETag", &quoted(&object.etag)),
    );
    resp.headers.insert(
        "x-goog-generation".to_string(),
        crate::http::object_generation_string(&object),
    );
    resp
}

fn abort(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    let upload_id = req.query_value("uploadId");
    match server
        .store()
        .abort_multipart_upload(bucket, key, upload_id)
    {
        Ok(true) => {
            let _ = delete_multipart_init(server.store(), bucket, upload_id);
            Response::empty_status(204)
        }
        Ok(false) => no_such_upload(),
        Err(e) => store_error(&e),
    }
}

/// `GET /{bucket}?uploads` with `prefix`, `key-marker`, and `max-uploads`.
pub fn list_uploads(server: &mut Server, req: &Request, bucket: &str) -> Response {
    let uploads = match server.store().list_multipart_uploads(bucket) {
        Ok(Some(u)) => u,
        Ok(None) => return no_such_bucket(),
        Err(e) => return store_error(&e),
    };
    let Ok(max_uploads) = parse_page_size(req.query_value("max-uploads"), DEFAULT_MAX_UPLOADS)
    else {
        return xml_error(400, "InvalidArgument", "invalid max-uploads");
    };
    let prefix = req.query_value("prefix");
    let key_marker = req.query_value("key-marker");
    let upload_id_marker = req.query_value("upload-id-marker");
    let matching: Vec<_> = uploads
        .iter()
        .filter(|u| {
            u.key.starts_with(prefix)
                && (u.key.as_str() > key_marker
                    || (!upload_id_marker.is_empty()
                        && u.key == key_marker
                        && u.upload_id.as_str() > upload_id_marker))
        })
        .collect();
    let truncated = matching.len() > max_uploads;
    let page = &matching[..matching.len().min(max_uploads)];
    let mut root = Element::new("ListMultipartUploadsResult")
        .attr("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")
        .text_child("Bucket", bucket)
        .text_child("KeyMarker", key_marker)
        .text_child("UploadIdMarker", upload_id_marker)
        .text_child("Prefix", prefix)
        .text_child("MaxUploads", &max_uploads.to_string())
        .text_child("IsTruncated", if truncated { "true" } else { "false" });
    if truncated {
        if let Some(last) = page.last() {
            root = root
                .text_child("NextKeyMarker", &last.key)
                .text_child("NextUploadIdMarker", &last.upload_id);
        }
    }
    for upload in page {
        let init =
            read_multipart_init(server.store(), bucket, &upload.upload_id).unwrap_or_default();
        root = root.child(
            Element::new("Upload")
                .text_child("Key", &upload.key)
                .text_child("UploadId", &upload.upload_id)
                .text_child("StorageClass", display_storage_class(&init))
                .text_child("Initiated", &upload.created_at),
        );
    }
    xml_ok(&root)
}
