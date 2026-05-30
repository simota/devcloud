//! Bucket-name, object-key, and upload-ID validation, ported from the Go S3
//! `validateBucketName` / `validateObjectKey` / `validateUploadID`.

/// Reports whether `name` is a valid S3 bucket name (3–63 chars, lowercase
/// alphanumerics / `-` / `.`, alnum start/end, no `..`/`.-`/`-.`, not IPv4-like).
pub fn valid_bucket_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 3 || bytes.len() > 63 {
        return false;
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return false;
    }
    if !is_bucket_alnum(bytes[0]) || !is_bucket_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    if name.contains(".-") || name.contains("-.") || is_ipv4_like(name) {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

fn is_bucket_alnum(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit()
}

fn is_ipv4_like(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    parts
        .iter()
        .all(|part| !part.is_empty() && part.len() <= 3 && part.chars().all(|c| c.is_ascii_digit()))
}

/// Reports whether `key` is a valid object key (non-empty, no NUL byte).
pub fn valid_object_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('\0')
}

/// Reports whether `upload_id` is a valid multipart upload ID (32 lowercase hex).
pub fn valid_upload_id(upload_id: &str) -> bool {
    upload_id.len() == 32
        && upload_id
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_names() {
        assert!(valid_bucket_name("my-bucket"));
        assert!(valid_bucket_name("a.b.c"));
        assert!(valid_bucket_name("abc"));
        assert!(!valid_bucket_name("ab")); // too short
        assert!(!valid_bucket_name(&"a".repeat(64))); // too long
        assert!(!valid_bucket_name("-bucket")); // bad start
        assert!(!valid_bucket_name("bucket-")); // bad end
        assert!(!valid_bucket_name("My-Bucket")); // uppercase
        assert!(!valid_bucket_name("a..b")); // double dot
        assert!(!valid_bucket_name("a.-b")); // ".-"
        assert!(!valid_bucket_name("a-.b")); // "-."
        assert!(!valid_bucket_name("192.168.1.1")); // IPv4-like
        assert!(!valid_bucket_name("a/b/c")); // slash
        assert!(!valid_bucket_name("a_b_c")); // underscore not allowed
    }

    #[test]
    fn object_keys() {
        assert!(valid_object_key("path/to/object"));
        assert!(!valid_object_key(""));
        assert!(!valid_object_key("has\0null"));
    }

    #[test]
    fn upload_ids() {
        assert!(valid_upload_id("0123456789abcdef0123456789abcdef"));
        assert!(!valid_upload_id("short"));
        assert!(!valid_upload_id("0123456789ABCDEF0123456789abcdef")); // uppercase
        assert!(!valid_upload_id("0123456789abcdef0123456789abcdeg")); // 'g'
    }
}
