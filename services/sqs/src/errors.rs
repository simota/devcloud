//! Mirrors `errorCode` from `internal/services/sqs/responses.rs`: maps an error
//! message (lowercased substring match) to its AWS error code. The substring
//! contract is why the error wording across the crate is preserved verbatim.

/// Maps a legacy-style error message to its AWS error code via substring match,
/// in the same priority order as the legacy `errorCode` switch.
pub fn error_code(err: &str) -> String {
    let m = err.to_lowercase();
    let code = if m.contains("queue name exists") {
        "QueueNameExists"
    } else if m.contains("queue does not exist") {
        "QueueDoesNotExist"
    } else if m.contains("receipt handle is invalid") {
        "ReceiptHandleIsInvalid"
    } else if m.contains("batch entry id must be unique") {
        "BatchEntryIdsNotDistinct"
    } else if m.contains("batch entry id") {
        "InvalidBatchEntryId"
    } else if m.contains("attribute name") {
        "InvalidAttributeName"
    } else if m.contains("attribute value") {
        "InvalidAttributeValue"
    } else if m.contains("invalid characters") {
        "InvalidMessageContents"
    } else {
        "InvalidParameterValue"
    };
    code.to_string()
}
