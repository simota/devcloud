//! S3 XML response builders and listing logic, ported from the legacy `responses.rs`
//! and `listing.rs`. Each builder produces an [`Element`] tree serialized to
//! byte-exact XML via [`crate::xml::encode`]; `omitempty` is applied by
//! conditionally adding child elements.

use crate::model::{
    AnalyticsConfiguration, InventoryConfiguration, LifecycleConfiguration, MultipartPart,
    MultipartUpload, NotificationConfiguration, NotificationFilter, Object, ObjectLegalHold,
    ObjectLockConfiguration, ObjectRetention, ReplicationConfiguration, NULL_VERSION_ID,
};
use crate::percent::aws_percent_encode;
use crate::xml::{encode, Element};
use serde::{Deserialize, Serialize};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

fn bool_str(b: bool) -> String {
    if b {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

/// `<Error>` response body.
pub fn error_xml(code: &str, message: &str) -> Vec<u8> {
    encode(
        &Element::new("Error")
            .text_child("Code", code)
            .text_child("Message", message),
    )
}

/// `<ListAllMyBucketsResult>` for `GET /`.
pub fn list_all_my_buckets(buckets: &[(String, String)]) -> Vec<u8> {
    let mut bucket_list = Element::new("Buckets");
    for (name, creation_date) in buckets {
        bucket_list = bucket_list.child(
            Element::new("Bucket")
                .text_child("Name", name)
                .text_child("CreationDate", creation_date),
        );
    }
    encode(
        &Element::new("ListAllMyBucketsResult")
            .attr("xmlns", XMLNS)
            .child(owner_element())
            .child(bucket_list),
    )
}

fn owner_element() -> Element {
    Element::new("Owner")
        .text_child("ID", "devcloud")
        .text_child("DisplayName", "devcloud")
}

/// `<AccessControlPolicy>` for an ACL GET; `acl` empty defaults to `private`.
pub fn access_control_policy(acl: &str) -> Vec<u8> {
    let acl = if acl.is_empty() { "private" } else { acl };
    let permission = if matches!(
        acl,
        "public-read" | "authenticated-read" | "bucket-owner-read"
    ) {
        "READ"
    } else {
        "FULL_CONTROL"
    };
    let grantee = Element::new("Grantee")
        .attr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        .attr("xsi:type", "CanonicalUser")
        .text_child("ID", "devcloud")
        .text_child("DisplayName", "devcloud");
    let grant = Element::new("Grant")
        .child(grantee)
        .text_child("Permission", permission);
    encode(
        &Element::new("AccessControlPolicy")
            .attr("xmlns", XMLNS)
            .child(owner_element())
            .child(Element::new("AccessControlList").child(grant))
            .text_child("CannedACL", acl),
    )
}

/// `<LocationConstraint>` for `GET /<bucket>?location`.
pub fn location_constraint(value: &str) -> Vec<u8> {
    encode(
        &Element::new("LocationConstraint")
            .attr("xmlns", XMLNS)
            .text(value),
    )
}

/// `<VersioningConfiguration>` for `GET /<bucket>?versioning`.
pub fn versioning_configuration(status: &str) -> Vec<u8> {
    let mut el = Element::new("VersioningConfiguration").attr("xmlns", XMLNS);
    if !status.is_empty() {
        el = el.text_child("Status", status);
    }
    encode(&el)
}

/// `<LifecycleConfiguration>` for `GET /<bucket>?lifecycle`.
pub fn lifecycle_configuration(config: &LifecycleConfiguration) -> Vec<u8> {
    let xmlns = if config.xmlns.is_empty() {
        XMLNS
    } else {
        &config.xmlns
    };
    let mut el = Element::new("LifecycleConfiguration").attr("xmlns", xmlns);
    for rule in &config.rules {
        let mut rule_el = Element::new("Rule");
        if !rule.id.is_empty() {
            rule_el = rule_el.text_child("ID", &rule.id);
        }
        if !rule.prefix.is_empty() {
            rule_el = rule_el.text_child("Prefix", &rule.prefix);
        }
        if !rule.filter.prefix.is_empty() {
            rule_el =
                rule_el.child(Element::new("Filter").text_child("Prefix", &rule.filter.prefix));
        }
        rule_el = rule_el.text_child("Status", &rule.status);
        let mut expiration = Element::new("Expiration");
        if let Some(days) = rule.expiration.days {
            expiration = expiration.text_child("Days", &days.to_string());
        }
        if !rule.expiration.date.is_empty() {
            expiration = expiration.text_child("Date", &rule.expiration.date);
        }
        el = el.child(rule_el.child(expiration));
    }
    encode(&el)
}

/// `<ObjectLockConfiguration>` for `GET /<bucket>?object-lock`.
pub fn object_lock_configuration(config: &ObjectLockConfiguration) -> Vec<u8> {
    let xmlns = if config.xmlns.is_empty() {
        XMLNS
    } else {
        &config.xmlns
    };
    let mut el = Element::new("ObjectLockConfiguration").attr("xmlns", xmlns);
    if !config.object_lock_enabled.is_empty() {
        el = el.text_child("ObjectLockEnabled", &config.object_lock_enabled);
    }
    let retention = &config.rule.default_retention;
    if !retention.mode.is_empty() || retention.days != 0 || retention.years != 0 {
        let mut default_retention = Element::new("DefaultRetention");
        if !retention.mode.is_empty() {
            default_retention = default_retention.text_child("Mode", &retention.mode);
        }
        if retention.days != 0 {
            default_retention = default_retention.text_child("Days", &retention.days.to_string());
        }
        if retention.years != 0 {
            default_retention = default_retention.text_child("Years", &retention.years.to_string());
        }
        el = el.child(Element::new("Rule").child(default_retention));
    }
    encode(&el)
}

/// `<Retention>` for `GET /<bucket>/<key>?retention`.
pub fn object_retention(retention: &ObjectRetention) -> Vec<u8> {
    let mut el = Element::new("Retention");
    if !retention.mode.is_empty() {
        el = el.text_child("Mode", &retention.mode);
    }
    if !retention.retain_until_date.is_empty() {
        el = el.text_child("RetainUntilDate", &retention.retain_until_date);
    }
    encode(&el)
}

/// `<LegalHold>` for `GET /<bucket>/<key>?legal-hold`.
pub fn object_legal_hold(legal_hold: &ObjectLegalHold) -> Vec<u8> {
    let mut el = Element::new("LegalHold");
    if !legal_hold.status.is_empty() {
        el = el.text_child("Status", &legal_hold.status);
    }
    encode(&el)
}

/// `<NotificationConfiguration>` for `GET /<bucket>?notification`.
pub fn notification_configuration(config: &NotificationConfiguration) -> Vec<u8> {
    let xmlns = if config.xmlns.is_empty() {
        XMLNS
    } else {
        &config.xmlns
    };
    let mut el = Element::new("NotificationConfiguration").attr("xmlns", xmlns);
    for topic in &config.topic_configurations {
        let mut child = Element::new("TopicConfiguration");
        if !topic.id.is_empty() {
            child = child.text_child("Id", &topic.id);
        }
        child = child.text_child("Topic", &topic.topic);
        for event in &topic.events {
            child = child.text_child("Event", event);
        }
        child = notification_filter_child(child, &topic.filter);
        el = el.child(child);
    }
    for queue in &config.queue_configurations {
        let mut child = Element::new("QueueConfiguration");
        if !queue.id.is_empty() {
            child = child.text_child("Id", &queue.id);
        }
        child = child.text_child("Queue", &queue.queue);
        for event in &queue.events {
            child = child.text_child("Event", event);
        }
        child = notification_filter_child(child, &queue.filter);
        el = el.child(child);
    }
    for lambda in &config.lambda_function_configurations {
        let mut child = Element::new("CloudFunctionConfiguration");
        if !lambda.id.is_empty() {
            child = child.text_child("Id", &lambda.id);
        }
        child = child.text_child("CloudFunction", &lambda.lambda_function);
        for event in &lambda.events {
            child = child.text_child("Event", event);
        }
        child = notification_filter_child(child, &lambda.filter);
        el = el.child(child);
    }
    if config.event_bridge_configuration.is_some() {
        el = el.child(Element::new("EventBridgeConfiguration"));
    }
    encode(&el)
}

fn notification_filter_child(mut parent: Element, filter: &NotificationFilter) -> Element {
    if filter.s3_key.rules.is_empty() {
        return parent;
    }
    let mut s3_key = Element::new("S3Key");
    for rule in &filter.s3_key.rules {
        s3_key = s3_key.child(
            Element::new("FilterRule")
                .text_child("Name", &rule.name)
                .text_child("Value", &rule.value),
        );
    }
    parent = parent.child(Element::new("Filter").child(s3_key));
    parent
}

/// `<InventoryConfiguration>` for `GET /<bucket>?inventory&id=...`.
pub fn inventory_configuration(config: &InventoryConfiguration) -> Vec<u8> {
    encode(&inventory_configuration_element(config).attr(
        "xmlns",
        if config.xmlns.is_empty() {
            XMLNS
        } else {
            &config.xmlns
        },
    ))
}

/// `<ListInventoryConfigurationsResult>` for `GET /<bucket>?inventory`.
pub fn list_inventory_configurations_result(configs: &[InventoryConfiguration]) -> Vec<u8> {
    let mut el = Element::new("ListInventoryConfigurationsResult")
        .attr("xmlns", XMLNS)
        .text_child("IsTruncated", "false");
    for config in configs {
        el = el.child(inventory_configuration_element(config));
    }
    encode(&el)
}

fn inventory_configuration_element(config: &InventoryConfiguration) -> Element {
    let mut el = Element::new("InventoryConfiguration");
    if !config.id.is_empty() {
        el = el.text_child("Id", &config.id);
    }
    el = el.text_child("IsEnabled", &bool_str(config.is_enabled));

    let dest = &config.destination.s3_bucket_destination;
    let mut s3_dest = Element::new("S3BucketDestination");
    if !dest.account_id.is_empty() {
        s3_dest = s3_dest.text_child("AccountId", &dest.account_id);
    }
    if !dest.bucket.is_empty() {
        s3_dest = s3_dest.text_child("Bucket", &dest.bucket);
    }
    if !dest.format.is_empty() {
        s3_dest = s3_dest.text_child("Format", &dest.format);
    }
    if !dest.prefix.is_empty() {
        s3_dest = s3_dest.text_child("Prefix", &dest.prefix);
    }
    el = el.child(Element::new("Destination").child(s3_dest));

    if !config.schedule.frequency.is_empty() {
        el = el.child(Element::new("Schedule").text_child("Frequency", &config.schedule.frequency));
    }
    if !config.included_object_versions.is_empty() {
        el = el.text_child("IncludedObjectVersions", &config.included_object_versions);
    }
    if !config.optional_fields.is_empty() {
        let mut fields = Element::new("OptionalFields");
        for field in &config.optional_fields {
            fields = fields.text_child("Field", field);
        }
        el = el.child(fields);
    }
    el
}

/// `<AnalyticsConfiguration>` for `GET /<bucket>?analytics&id=...`.
pub fn analytics_configuration(config: &AnalyticsConfiguration) -> Vec<u8> {
    encode(&analytics_configuration_element(config).attr(
        "xmlns",
        if config.xmlns.is_empty() {
            XMLNS
        } else {
            &config.xmlns
        },
    ))
}

/// `<ListAnalyticsConfigurationsResult>` for `GET /<bucket>?analytics`.
pub fn list_analytics_configurations_result(configs: &[AnalyticsConfiguration]) -> Vec<u8> {
    let mut el = Element::new("ListAnalyticsConfigurationsResult")
        .attr("xmlns", XMLNS)
        .text_child("IsTruncated", "false");
    for config in configs {
        el = el.child(analytics_configuration_element(config));
    }
    encode(&el)
}

fn analytics_configuration_element(config: &AnalyticsConfiguration) -> Element {
    let mut el = Element::new("AnalyticsConfiguration");
    if !config.id.is_empty() {
        el = el.text_child("Id", &config.id);
    }
    if !config.filter.prefix.is_empty() {
        el = el.child(Element::new("Filter").text_child("Prefix", &config.filter.prefix));
    }

    let export = &config.storage_class_analysis.data_export;
    let dest = &export.destination.s3_bucket_destination;
    let mut s3_dest = Element::new("S3BucketDestination");
    if !dest.format.is_empty() {
        s3_dest = s3_dest.text_child("Format", &dest.format);
    }
    if !dest.bucket.is_empty() {
        s3_dest = s3_dest.text_child("Bucket", &dest.bucket);
    }
    if !dest.prefix.is_empty() {
        s3_dest = s3_dest.text_child("Prefix", &dest.prefix);
    }
    let mut data_export = Element::new("DataExport");
    if !export.output_schema_version.is_empty() {
        data_export = data_export.text_child("OutputSchemaVersion", &export.output_schema_version);
    }
    data_export = data_export.child(Element::new("Destination").child(s3_dest));
    el.child(Element::new("StorageClassAnalysis").child(data_export))
}

/// `<ReplicationConfiguration>` for `GET /<bucket>?replication`.
pub fn replication_configuration(config: &ReplicationConfiguration) -> Vec<u8> {
    let xmlns = if config.xmlns.is_empty() {
        XMLNS
    } else {
        &config.xmlns
    };
    let mut el = Element::new("ReplicationConfiguration").attr("xmlns", xmlns);
    if !config.role.is_empty() {
        el = el.text_child("Role", &config.role);
    }
    for rule in &config.rules {
        let mut child = Element::new("Rule");
        if !rule.id.is_empty() {
            child = child.text_child("ID", &rule.id);
        }
        if rule.priority != 0 {
            child = child.text_child("Priority", &rule.priority.to_string());
        }
        if !rule.prefix.is_empty() {
            child = child.text_child("Prefix", &rule.prefix);
        }
        if !rule.filter.prefix.is_empty() {
            child = child.child(Element::new("Filter").text_child("Prefix", &rule.filter.prefix));
        }
        child = child.text_child("Status", &rule.status);
        let mut destination =
            Element::new("Destination").text_child("Bucket", &rule.destination.bucket);
        if !rule.destination.storage_class.is_empty() {
            destination = destination.text_child("StorageClass", &rule.destination.storage_class);
        }
        child = child.child(destination);
        if !rule.delete_marker_replication.status.is_empty() {
            child = child.child(
                Element::new("DeleteMarkerReplication")
                    .text_child("Status", &rule.delete_marker_replication.status),
            );
        }
        el = el.child(child);
    }
    encode(&el)
}

/// `<CopyObjectResult>`.
pub fn copy_object_result(last_modified: &str, etag: &str) -> Vec<u8> {
    encode(
        &Element::new("CopyObjectResult")
            .text_child("LastModified", last_modified)
            .text_child("ETag", etag),
    )
}

/// `<InitiateMultipartUploadResult>`.
pub fn initiate_multipart_upload_result(bucket: &str, key: &str, upload_id: &str) -> Vec<u8> {
    encode(
        &Element::new("InitiateMultipartUploadResult")
            .attr("xmlns", XMLNS)
            .text_child("Bucket", bucket)
            .text_child("Key", key)
            .text_child("UploadId", upload_id),
    )
}

/// `<CompleteMultipartUploadResult>`.
pub fn complete_multipart_upload_result(bucket: &str, key: &str, etag: &str) -> Vec<u8> {
    encode(
        &Element::new("CompleteMultipartUploadResult")
            .attr("xmlns", XMLNS)
            .text_child("Location", &format!("/{bucket}/{key}"))
            .text_child("Bucket", bucket)
            .text_child("Key", key)
            .text_child("ETag", etag),
    )
}

/// `<ListMultipartUploadsResult>`.
pub fn list_multipart_uploads_result(bucket: &str, uploads: &[MultipartUpload]) -> Vec<u8> {
    let mut el = Element::new("ListMultipartUploadsResult")
        .attr("xmlns", XMLNS)
        .text_child("Bucket", bucket)
        .text_child("IsTruncated", "false");
    for upload in uploads {
        el = el.child(
            Element::new("Upload")
                .text_child("Key", &upload.key)
                .text_child("UploadId", &upload.upload_id)
                .text_child("Initiated", &seconds(&upload.created_at))
                .text_child("StorageClass", "STANDARD"),
        );
    }
    encode(&el)
}

/// `<ListPartsResult>`.
pub fn list_parts_result(
    upload: &MultipartUpload,
    parts: &[MultipartPart],
    part_number_marker: i64,
    max_parts: i64,
    is_truncated: bool,
    next_part_number_marker: i64,
) -> Vec<u8> {
    let mut el = Element::new("ListPartsResult")
        .attr("xmlns", XMLNS)
        .text_child("Bucket", &upload.bucket)
        .text_child("Key", &upload.key)
        .text_child("UploadId", &upload.upload_id)
        .text_child("PartNumberMarker", &part_number_marker.to_string());
    if next_part_number_marker != 0 {
        el = el.text_child("NextPartNumberMarker", &next_part_number_marker.to_string());
    }
    el = el
        .text_child("MaxParts", &max_parts.to_string())
        .text_child("IsTruncated", &bool_str(is_truncated));
    for part in parts {
        el = el.child(
            Element::new("Part")
                .text_child("PartNumber", &part.part_number.to_string())
                .text_child("LastModified", &seconds(&part.last_modified))
                .text_child("ETag", &part.etag)
                .text_child("Size", &part.size.to_string()),
        );
    }
    encode(&el)
}

/// Parses S3's `max-parts` query value: empty -> 1000, clamped to 1000,
/// negative/malformed is an error. Mirrors the legacy `parseMaxParts`.
pub fn parse_max_parts(value: &str) -> Result<i64, InvalidMaxKeys> {
    parse_max_keys(value)
}

/// Parses S3's `part-number-marker` query value.
pub fn parse_part_number_marker(value: &str) -> Result<i64, InvalidMaxKeys> {
    if value.is_empty() {
        return Ok(0);
    }
    match value.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(n),
        _ => Err(InvalidMaxKeys),
    }
}

/// Paginates uploaded parts. Mirrors the legacy `paginateParts`.
pub fn paginate_parts(
    parts: &[MultipartPart],
    part_number_marker: i64,
    max_parts: i64,
) -> (Vec<MultipartPart>, bool, i64) {
    if max_parts == 0 {
        for part in parts {
            if part.part_number > part_number_marker {
                return (Vec::new(), true, part_number_marker);
            }
        }
        return (Vec::new(), false, 0);
    }
    let mut page = Vec::new();
    let mut next_part_number_marker = 0;
    for part in parts {
        if part.part_number <= part_number_marker {
            continue;
        }
        if page.len() as i64 >= max_parts {
            return (page, true, next_part_number_marker);
        }
        page.push(part.clone());
        next_part_number_marker = part.part_number;
    }
    (page, false, 0)
}

fn seconds(value: &str) -> String {
    crate::time_fmt::parse_rfc3339(value)
        .map(|(secs, _)| crate::time_fmt::rfc3339_seconds_from_unix(secs))
        .unwrap_or_else(|| value.to_string())
}

// --- object listing --------------------------------------------------------

/// The result of paginating a flat object list (the legacy `objectListing`).
#[derive(Debug, Default, Clone)]
pub struct ObjectListing {
    pub contents: Vec<Object>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    pub next_marker: String,
    pub next_continuation_token: String,
}

/// The result of paginating a version list (the legacy `versionListing`).
#[derive(Debug, Default, Clone)]
pub struct VersionListing {
    pub versions: Vec<Object>,
    pub truncated: bool,
    pub next_key_marker: String,
    pub next_version_id_marker: String,
}

/// The effective version ID of an object (empty -> `null`).
pub fn object_version_id(object: &Object) -> String {
    if object.version_id.is_empty() {
        NULL_VERSION_ID.to_string()
    } else {
        object.version_id.clone()
    }
}

/// Maps each key to its first-seen (latest) version ID. Mirrors the legacy
/// `latestObjectVersionIDs`.
pub fn latest_object_version_ids(versions: &[Object]) -> std::collections::HashMap<String, String> {
    let mut latest = std::collections::HashMap::new();
    for object in versions {
        latest
            .entry(object.key.clone())
            .or_insert_with(|| object_version_id(object));
    }
    latest
}

/// Error from [`parse_max_keys`] when the `max-keys` value is malformed.
#[derive(Debug, PartialEq, Eq)]
pub struct InvalidMaxKeys;

/// Parses the `max-keys` query value: empty -> 1000, clamped to 1000, negative
/// is an error. Mirrors the legacy `parseMaxKeys`.
pub fn parse_max_keys(value: &str) -> Result<i64, InvalidMaxKeys> {
    if value.is_empty() {
        return Ok(1000);
    }
    match value.parse::<i64>() {
        Ok(n) if n < 0 => Err(InvalidMaxKeys),
        Ok(n) if n > 1000 => Ok(1000),
        Ok(n) => Ok(n),
        Err(_) => Err(InvalidMaxKeys),
    }
}

#[derive(Serialize, Deserialize)]
struct ContinuationToken {
    #[serde(rename = "lastKey")]
    last_key: String,
}

/// Base64-RawURL of `{"lastKey":...}`. Mirrors the legacy `encodeContinuationToken`.
pub fn encode_continuation_token(last_key: &str) -> String {
    let bytes = crate::wire_json::to_vec_compact(&ContinuationToken {
        last_key: last_key.to_string(),
    });
    crate::base64::raw_url_encode(&bytes)
}

/// Decodes a continuation token to its `lastKey`. Mirrors the legacy
/// `decodeContinuationToken`; `None` on malformed input.
pub fn decode_continuation_token(value: &str) -> Option<String> {
    let data = crate::base64::raw_url_decode(value)?;
    let token: ContinuationToken = serde_json::from_slice(&data).ok()?;
    Some(token.last_key)
}

/// URL-encodes a listing value when `encoding_type == "url"`. Mirrors the legacy
/// `encodeListValue`.
pub fn encode_list_value(value: &str, encoding_type: &str) -> String {
    if encoding_type != "url" || value.is_empty() {
        value.to_string()
    } else {
        aws_percent_encode(value, "~-_.")
    }
}

/// Paginates `objects` into an [`ObjectListing`] honoring prefix/delimiter/
/// marker/max-keys. Mirrors the legacy `buildObjectListing`.
pub fn build_object_listing(
    objects: &[Object],
    prefix: &str,
    delimiter: &str,
    marker: &str,
    max_keys: i64,
) -> ObjectListing {
    let mut listing = ObjectListing::default();
    if max_keys == 0 {
        return listing;
    }
    let mut marker = marker.to_string();
    let mut common_prefixes_seen = std::collections::HashSet::new();
    let mut count: i64 = 0;
    let mut i = 0;
    while i < objects.len() {
        let object = &objects[i];
        if !marker.is_empty() && object.key <= marker {
            i += 1;
            continue;
        }
        let mut item_key = object.key.clone();
        let mut item_is_object = true;
        let mut last_key_for_item = object.key.clone();
        if !delimiter.is_empty() {
            let remainder = object.key.strip_prefix(prefix).unwrap_or(&object.key);
            if let Some(index) = remainder.find(delimiter) {
                item_key = format!("{}{}", prefix, &remainder[..index + delimiter.len()]);
                item_is_object = false;
                while i + 1 < objects.len() && objects[i + 1].key.starts_with(&item_key) {
                    i += 1;
                    last_key_for_item = objects[i].key.clone();
                }
                if common_prefixes_seen.contains(&item_key) {
                    i += 1;
                    continue;
                }
            }
        }
        if count >= max_keys {
            listing.truncated = true;
            listing.next_marker = marker.clone();
            listing.next_continuation_token = encode_continuation_token(&marker);
            if listing.next_marker.is_empty() {
                listing.next_marker = object.key.clone();
                listing.next_continuation_token = encode_continuation_token(&object.key);
            }
            return listing;
        }
        if item_is_object {
            listing.contents.push(object.clone());
        } else {
            common_prefixes_seen.insert(item_key.clone());
            listing.common_prefixes.push(item_key);
        }
        count += 1;
        marker = last_key_for_item;
        i += 1;
    }
    listing
}

/// Paginates `versions` into a [`VersionListing`] honoring the key/version
/// markers and max-keys. Mirrors the legacy `buildVersionListing`.
pub fn build_version_listing(
    versions: &[Object],
    key_marker: &str,
    version_id_marker: &str,
    max_keys: i64,
) -> VersionListing {
    let mut listing = VersionListing::default();
    if max_keys == 0 {
        return listing;
    }
    let mut started = key_marker.is_empty();
    for object in versions {
        let version_id = object_version_id(object);
        if !started {
            if object.key.as_str() < key_marker {
                continue;
            } else if object.key.as_str() > key_marker {
                started = true;
            } else if version_id_marker.is_empty() {
                continue;
            } else if version_id == version_id_marker {
                started = true;
                continue;
            } else {
                continue;
            }
        }
        if listing.versions.len() as i64 >= max_keys {
            listing.truncated = true;
            let last = listing.versions.last().unwrap();
            listing.next_key_marker = last.key.clone();
            listing.next_version_id_marker = object_version_id(last);
            return listing;
        }
        listing.versions.push(object.clone());
    }
    listing
}

// --- ListBucketResult / ListVersionsResult XML -----------------------------

/// Parameters for rendering `<ListBucketResult>`. Field order matches the legacy
/// `listBucketResult` struct.
#[derive(Debug, Default, Clone)]
pub struct ListBucketResult {
    pub name: String,
    pub prefix: String,
    pub delimiter: String,
    pub marker: String,
    pub next_marker: String,
    pub continuation_token: String,
    pub next_continuation_token: String,
    pub start_after: String,
    pub key_count: i64,
    pub max_keys: i64,
    pub is_truncated: bool,
    pub list_type: i64,
    pub contents: Vec<ObjectElement>,
    pub common_prefixes: Vec<String>,
}

/// A `<Contents>` entry.
#[derive(Debug, Default, Clone)]
pub struct ObjectElement {
    pub key: String,
    pub last_modified: String,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
}

impl ListBucketResult {
    pub fn to_xml(&self) -> Vec<u8> {
        let mut el = Element::new("ListBucketResult")
            .attr("xmlns", XMLNS)
            .text_child("Name", &self.name)
            .text_child("Prefix", &self.prefix);
        if !self.delimiter.is_empty() {
            el = el.text_child("Delimiter", &self.delimiter);
        }
        if !self.marker.is_empty() {
            el = el.text_child("Marker", &self.marker);
        }
        if !self.next_marker.is_empty() {
            el = el.text_child("NextMarker", &self.next_marker);
        }
        if !self.continuation_token.is_empty() {
            el = el.text_child("ContinuationToken", &self.continuation_token);
        }
        if !self.next_continuation_token.is_empty() {
            el = el.text_child("NextContinuationToken", &self.next_continuation_token);
        }
        if !self.start_after.is_empty() {
            el = el.text_child("StartAfter", &self.start_after);
        }
        el = el
            .text_child("KeyCount", &self.key_count.to_string())
            .text_child("MaxKeys", &self.max_keys.to_string())
            .text_child("IsTruncated", &bool_str(self.is_truncated));
        if self.list_type != 0 {
            el = el.text_child("ListType", &self.list_type.to_string());
        }
        for c in &self.contents {
            el = el.child(
                Element::new("Contents")
                    .text_child("Key", &c.key)
                    .text_child("LastModified", &c.last_modified)
                    .text_child("ETag", &c.etag)
                    .text_child("Size", &c.size.to_string())
                    .text_child("StorageClass", &c.storage_class),
            );
        }
        for prefix in &self.common_prefixes {
            el = el.child(Element::new("CommonPrefixes").text_child("Prefix", prefix));
        }
        encode(&el)
    }
}

/// Parameters for rendering `<ListVersionsResult>`.
#[derive(Debug, Default, Clone)]
pub struct ListVersionsResult {
    pub name: String,
    pub prefix: String,
    pub key_marker: String,
    pub version_id_marker: String,
    pub next_key_marker: String,
    pub next_version_id_marker: String,
    pub max_keys: i64,
    pub is_truncated: bool,
    pub versions: Vec<VersionElement>,
    pub delete_markers: Vec<DeleteMarkerElement>,
}

/// A `<Version>` entry.
#[derive(Debug, Default, Clone)]
pub struct VersionElement {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
}

/// A `<DeleteMarker>` entry.
#[derive(Debug, Default, Clone)]
pub struct DeleteMarkerElement {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
}

impl ListVersionsResult {
    pub fn to_xml(&self) -> Vec<u8> {
        let mut el = Element::new("ListVersionsResult")
            .attr("xmlns", XMLNS)
            .text_child("Name", &self.name)
            .text_child("Prefix", &self.prefix);
        if !self.key_marker.is_empty() {
            el = el.text_child("KeyMarker", &self.key_marker);
        }
        if !self.version_id_marker.is_empty() {
            el = el.text_child("VersionIdMarker", &self.version_id_marker);
        }
        if !self.next_key_marker.is_empty() {
            el = el.text_child("NextKeyMarker", &self.next_key_marker);
        }
        if !self.next_version_id_marker.is_empty() {
            el = el.text_child("NextVersionIdMarker", &self.next_version_id_marker);
        }
        el = el
            .text_child("MaxKeys", &self.max_keys.to_string())
            .text_child("IsTruncated", &bool_str(self.is_truncated));
        for v in &self.versions {
            el = el.child(
                Element::new("Version")
                    .text_child("Key", &v.key)
                    .text_child("VersionId", &v.version_id)
                    .text_child("IsLatest", &bool_str(v.is_latest))
                    .text_child("LastModified", &v.last_modified)
                    .text_child("ETag", &v.etag)
                    .text_child("Size", &v.size.to_string())
                    .text_child("StorageClass", &v.storage_class),
            );
        }
        for d in &self.delete_markers {
            el = el.child(
                Element::new("DeleteMarker")
                    .text_child("Key", &d.key)
                    .text_child("VersionId", &d.version_id)
                    .text_child("IsLatest", &bool_str(d.is_latest))
                    .text_child("LastModified", &d.last_modified),
            );
        }
        encode(&el)
    }
}
