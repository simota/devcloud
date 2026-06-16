//! URL path parsing + resource-id validation.
//!
//! Mirrors `internal/services/pubsub/{path_parsing,validation}.rs`. Paths are
//! `/v1/projects/<project>/<collection>/<id>` with optional `:action` suffixes
//! and sub-collections. Segments are percent-decoded and trimmed, matching legacy
//! `pathParts`.

/// Splits a path into percent-decoded, trimmed segments, mirroring `pathParts`.
/// A segment that fails to decode becomes `"\0"` (so it never matches a valid
/// id), exactly as legacy does.
pub fn path_parts(path: &str) -> Vec<String> {
    path.trim_matches('/')
        .split('/')
        .map(|part| match percent_decode(part) {
            Some(decoded) => decoded.trim().to_string(),
            None => "\0".to_string(),
        })
        .collect()
}

fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = (bytes[i + 1] as char).to_digit(16)?;
                let lo = (bytes[i + 2] as char).to_digit(16)?;
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            // `+` is NOT space in path decoding (legacy url.PathUnescape).
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// `^[A-Za-z0-9][A-Za-z0-9._~+%-]{0,254}$`, mirroring `resourceIDPattern`. Used
/// for both project ids and resource ids.
pub fn valid_resource_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    if !(first.is_ascii_alphanumeric()) {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'~' | b'+' | b'%' | b'-'))
}

pub fn valid_project_id(id: &str) -> bool {
    valid_resource_id(id)
}

/// `projects/<project>/topics/<id>` validity, mirroring `validFullTopicName`.
pub fn valid_full_topic_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() == 4
        && parts[0] == "projects"
        && parts[2] == "topics"
        && valid_project_id(parts[1])
        && valid_resource_id(parts[3])
}

pub fn valid_full_schema_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() == 4
        && parts[0] == "projects"
        && parts[2] == "schemas"
        && valid_project_id(parts[1])
        && valid_resource_id(parts[3])
}

/// `projects/<project>/topics/<id>`.
pub fn topic_name(project: &str, topic_id: &str) -> String {
    format!("projects/{project}/topics/{topic_id}")
}

/// `projects/<project>/subscriptions/<id>`.
pub fn subscription_name(project: &str, subscription_id: &str) -> String {
    format!("projects/{project}/subscriptions/{subscription_id}")
}

/// `projects/<project>/subscriptions/<id>` validity.
pub fn valid_full_subscription_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() == 4
        && parts[0] == "projects"
        && parts[2] == "subscriptions"
        && valid_project_id(parts[1])
        && valid_resource_id(parts[3])
}

/// `projects/<project>/snapshots/<id>` validity.
pub fn valid_full_snapshot_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    parts.len() == 4
        && parts[0] == "projects"
        && parts[2] == "snapshots"
        && valid_project_id(parts[1])
        && valid_resource_id(parts[3])
}

/// The project component of a resource name (`projects/<p>/...`), mirroring
/// `resourceProject`.
pub fn resource_project(name: &str) -> String {
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() >= 2 && parts[0] == "projects" {
        parts[1].to_string()
    } else {
        String::new()
    }
}

// --- matchers (return parsed components) -----------------------------------

/// `/v1/projects/<p>/topics` collection path.
pub fn topics_collection(path: &str) -> Option<String> {
    let parts = path_parts(path);
    if parts.len() == 4
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == "topics"
        && !parts[2].is_empty()
    {
        Some(parts[2].clone())
    } else {
        None
    }
}

/// `/v1/projects/<p>/topics/<id>` — returns `(project, topic_id)`.
pub fn topic_name_parts(path: &str) -> Option<(String, String)> {
    let parts = path_parts(path);
    if parts.len() == 5
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == "topics"
        && !parts[2].is_empty()
        && !parts[4].is_empty()
        && !parts[4].contains(':')
    {
        Some((parts[2].clone(), parts[4].clone()))
    } else {
        None
    }
}

/// `/v1/projects/<p>/topics/<id>/subscriptions`.
pub fn topic_subscriptions_parts(path: &str) -> Option<(String, String)> {
    sub_collection_parts(path, "topics", "subscriptions")
}

/// `/v1/projects/<p>/topics/<id>/snapshots`.
pub fn topic_snapshots_parts(path: &str) -> Option<(String, String)> {
    sub_collection_parts(path, "topics", "snapshots")
}

/// `/v1/projects/<p>/subscriptions` collection path.
pub fn subscriptions_collection(path: &str) -> Option<String> {
    let parts = path_parts(path);
    if parts.len() == 4
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == "subscriptions"
        && !parts[2].is_empty()
    {
        Some(parts[2].clone())
    } else {
        None
    }
}

/// `/v1/projects/<p>/subscriptions/<id>` — returns `(project, subscription_id)`.
pub fn subscription_name_parts(path: &str) -> Option<(String, String)> {
    let parts = path_parts(path);
    if parts.len() == 5
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == "subscriptions"
        && !parts[2].is_empty()
        && !parts[4].is_empty()
        && !parts[4].contains(':')
    {
        Some((parts[2].clone(), parts[4].clone()))
    } else {
        None
    }
}

/// `/v1/projects/<p>/subscriptions/<id>:<action>` — returns
/// `(project, subscription_id, action)`.
pub fn subscription_action_parts(path: &str) -> Option<(String, String, String)> {
    let parts = path_parts(path);
    if parts.len() != 5
        || parts[0] != "v1"
        || parts[1] != "projects"
        || parts[3] != "subscriptions"
        || parts[2].is_empty()
    {
        return None;
    }
    let (sub_id, action) = parts[4].split_once(':')?;
    if sub_id.is_empty() || action.is_empty() {
        return None;
    }
    Some((parts[2].clone(), sub_id.to_string(), action.to_string()))
}

/// `/v1/projects/<p>/snapshots` collection path.
pub fn snapshots_collection(path: &str) -> Option<String> {
    simple_collection(path, "snapshots")
}

/// `/v1/projects/<p>/snapshots/<id>` — returns `(project, snapshot_id)`.
pub fn snapshot_name_parts(path: &str) -> Option<(String, String)> {
    leaf_name_parts(path, "snapshots")
}

/// `projects/<project>/snapshots/<id>`.
pub fn snapshot_name(project: &str, snapshot_id: &str) -> String {
    format!("projects/{project}/snapshots/{snapshot_id}")
}

/// `/v1/projects/<p>/schemas` collection path.
pub fn schemas_collection(path: &str) -> Option<String> {
    simple_collection(path, "schemas")
}

/// `/v1/projects/<p>/schemas/<id>` — returns `(project, schema_id)`.
pub fn schema_name_parts(path: &str) -> Option<(String, String)> {
    leaf_name_parts(path, "schemas")
}

/// `projects/<project>/schemas/<id>`.
pub fn schema_name(project: &str, schema_id: &str) -> String {
    format!("projects/{project}/schemas/{schema_id}")
}

/// `/v1/projects/<p>/schemas:validateMessage` — returns the project.
pub fn schemas_validate_message(path: &str) -> Option<String> {
    let parts = path_parts(path);
    if parts.len() == 4
        && parts[0] == "v1"
        && parts[1] == "projects"
        && !parts[2].is_empty()
        && parts[3] == "schemas:validateMessage"
    {
        Some(parts[2].clone())
    } else {
        None
    }
}

fn simple_collection(path: &str, collection: &str) -> Option<String> {
    let parts = path_parts(path);
    if parts.len() == 4
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == collection
        && !parts[2].is_empty()
    {
        Some(parts[2].clone())
    } else {
        None
    }
}

fn leaf_name_parts(path: &str, collection: &str) -> Option<(String, String)> {
    let parts = path_parts(path);
    if parts.len() == 5
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == collection
        && !parts[2].is_empty()
        && !parts[4].is_empty()
        && !parts[4].contains(':')
    {
        Some((parts[2].clone(), parts[4].clone()))
    } else {
        None
    }
}

/// `/v1/projects/<p>/topics/<id>:<action>` — returns `(project, topic_id, action)`.
pub fn topic_action_parts(path: &str) -> Option<(String, String, String)> {
    let parts = path_parts(path);
    if parts.len() != 5
        || parts[0] != "v1"
        || parts[1] != "projects"
        || parts[3] != "topics"
        || parts[2].is_empty()
    {
        return None;
    }
    let (topic_id, action) = parts[4].split_once(':')?;
    if topic_id.is_empty() || action.is_empty() {
        return None;
    }
    Some((parts[2].clone(), topic_id.to_string(), action.to_string()))
}

fn sub_collection_parts(path: &str, collection: &str, sub: &str) -> Option<(String, String)> {
    let parts = path_parts(path);
    if parts.len() == 6
        && parts[0] == "v1"
        && parts[1] == "projects"
        && parts[3] == collection
        && parts[5] == sub
        && !parts[2].is_empty()
        && !parts[4].is_empty()
    {
        Some((parts[2].clone(), parts[4].clone()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ids() {
        assert!(valid_resource_id("orders"));
        assert!(valid_resource_id("a.b_c~d+e%f-g"));
        assert!(!valid_resource_id(""));
        assert!(!valid_resource_id(".bad"));
        assert!(!valid_resource_id("has space"));
    }

    #[test]
    fn parses_topic_paths() {
        assert_eq!(
            topic_name_parts("/v1/projects/dev/topics/orders"),
            Some(("dev".to_string(), "orders".to_string()))
        );
        assert!(topic_name_parts("/v1/projects/dev/topics/orders:publish").is_none());
        assert_eq!(
            topics_collection("/v1/projects/dev/topics"),
            Some("dev".to_string())
        );
        assert_eq!(
            topic_subscriptions_parts("/v1/projects/dev/topics/orders/subscriptions"),
            Some(("dev".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn full_names() {
        assert!(valid_full_topic_name("projects/dev/topics/orders"));
        assert!(!valid_full_topic_name("projects/dev/topics"));
        assert_eq!(resource_project("projects/dev/topics/x"), "dev");
    }
}
