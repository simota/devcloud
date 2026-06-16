//! state.json persistence.
//!
//! Parity: `internal/services/redshift/storage.rs`. The stored JSON shape
//! (field names, omitempty behavior, RFC3339Nano timestamps, 2-space indent)
//! matches legacy `json.MarshalIndent` output so legacy and Rust can read each
//! other's state files. Clusters and cluster snapshots are control-plane
//! models that arrive in part 4 of increment #10; until then they are carried
//! through load/persist as opaque JSON so they are never dropped.
//! TODO(agent): replace the opaque clusters/snapshots passthrough with real
//! models in part 4 (including `normalizeClusterEndpoints`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::cluster::{ClusterSnapshot, ClusterSnapshotMetadata};
use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::model::{ensure_public_schema, Column, Database, QualifiedName, Schema, Table};
use crate::pg_types::PgField;
use crate::server::{default_str, ServerState, StatementRecord};

const STATE_FILE_NAME: &str = "state.json";

/// Mirrors `storedState`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredState {
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<StoredDatabase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    clusters: Option<BTreeMap<String, ClusterSnapshot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots: Option<BTreeMap<String, ClusterSnapshotMetadata>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statements: Option<BTreeMap<String, StoredStatement>>,
    #[serde(rename = "clientTokenIndex", skip_serializing_if = "Option::is_none")]
    client_token_index: Option<BTreeMap<String, String>>,
    #[serde(rename = "nextStatementId", skip_serializing_if = "i64_is_zero")]
    next_statement_id: i64,
}

/// Mirrors `storedDatabase`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredDatabase {
    schemas: BTreeMap<String, StoredSchema>,
}

/// Mirrors `storedSchema`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredSchema {
    tables: BTreeMap<String, StoredTable>,
}

/// Mirrors `storedTable`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredTable {
    name: StoredQualifiedName,
    columns: Vec<StoredColumn>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rows: Vec<Vec<String>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    kind: String,
    #[serde(rename = "viewSql", skip_serializing_if = "String::is_empty")]
    view_sql: String,
    #[serde(rename = "distStyle", skip_serializing_if = "String::is_empty")]
    dist_style: String,
    #[serde(rename = "distKey", skip_serializing_if = "String::is_empty")]
    dist_key: String,
    #[serde(rename = "sortKeys", skip_serializing_if = "Vec::is_empty")]
    sort_keys: Vec<String>,
}

/// Mirrors `storedQualifiedName`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredQualifiedName {
    schema: String,
    table: String,
}

/// Mirrors `storedColumn`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredColumn {
    name: String,
    #[serde(rename = "dataType")]
    data_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    encoding: String,
    #[serde(rename = "defaultValue", skip_serializing_if = "String::is_empty")]
    default_value: String,
    #[serde(skip_serializing_if = "bool_is_false")]
    identity: bool,
}

/// Mirrors `storedStatement`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredStatement {
    id: String,
    #[serde(rename = "clusterIdentifier")]
    cluster_identifier: String,
    database: String,
    #[serde(rename = "dbUser")]
    db_user: String,
    #[serde(rename = "sessionId", skip_serializing_if = "String::is_empty")]
    session_id: String,
    #[serde(rename = "queryString")]
    query_string: String,
    #[serde(rename = "resultFormat", skip_serializing_if = "String::is_empty")]
    result_format: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(rename = "hasResultSet")]
    has_result_set: bool,
    // legacy tags this omitempty, but a non-pointer struct is never omitted by
    // encoding/json, so it is always serialized here too.
    result: StoredQueryResult,
}

/// Mirrors `storedQueryResult`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredQueryResult {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<StoredPgField>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rows: Vec<Vec<String>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    tag: String,
}

/// Mirrors `storedPGField`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StoredPgField {
    name: String,
    #[serde(rename = "typeOid")]
    type_oid: i32,
    #[serde(rename = "typeSize")]
    type_size: i16,
}

fn i64_is_zero(value: &i64) -> bool {
    *value == 0
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

/// The legacy `loadState` multi-return, bundled.
#[derive(Default)]
pub(crate) struct LoadedState {
    pub database: Option<Database>,
    pub clusters: BTreeMap<String, ClusterSnapshot>,
    pub snapshots: BTreeMap<String, ClusterSnapshotMetadata>,
    pub statements: BTreeMap<String, StatementRecord>,
    pub client_token_index: BTreeMap<String, String>,
    pub next_statement_id: i64,
}

/// Mirrors `loadState` (empty path / missing file → empty state, no error).
pub(crate) fn load_state(storage_path: &str) -> Result<LoadedState, SqlError> {
    let Some(path) = state_file_path(storage_path) else {
        return Ok(LoadedState::default());
    };
    let data = match std::fs::read(&path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LoadedState::default());
        }
        Err(err) => return Err(SqlError::new(format!("load redshift state: {err}"))),
    };
    let state: StoredState = serde_json::from_slice(&data)
        .map_err(|err| SqlError::new(format!("decode redshift state: {err}")))?;
    let statements = statements_from_stored(state.statements.unwrap_or_default());
    let mut next_statement_id = state.next_statement_id;
    if next_statement_id == 0 {
        next_statement_id = max_stored_statement_sequence(&statements);
    }
    Ok(LoadedState {
        database: state.database.map(database_from_stored),
        clusters: state.clusters.unwrap_or_default(),
        snapshots: snapshots_from_stored(state.snapshots.unwrap_or_default()),
        statements,
        client_token_index: state.client_token_index.unwrap_or_default(),
        next_statement_id,
    })
}

/// Mirrors `Server.persistLocked` (the file-writing half; the lock is held by
/// the caller via `&ServerState`).
pub(crate) fn persist_state(storage_path: &str, state: &ServerState) -> Result<(), SqlError> {
    let Some(path) = state_file_path(storage_path) else {
        return Ok(());
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|err| SqlError::new(format!("create redshift state directory: {err}")))?;
    }
    let stored = StoredState {
        database: Some(database_to_stored(&state.db)),
        clusters: if state.clusters.is_empty() {
            None
        } else {
            Some(state.clusters.clone())
        },
        snapshots: if state.snapshots.is_empty() {
            None
        } else {
            Some(state.snapshots.clone())
        },
        statements: statements_to_stored(&state.statements),
        client_token_index: if state.client_token_index.is_empty() {
            None
        } else {
            Some(state.client_token_index.clone())
        },
        next_statement_id: state.next_statement_id,
    };
    let data = serde_json::to_vec_pretty(&stored)
        .map_err(|err| SqlError::new(format!("encode redshift state: {err}")))?;
    let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
    write_private_file(&temp_path, &data)
        .map_err(|err| SqlError::new(format!("write redshift state: {err}")))?;
    std::fs::rename(&temp_path, &path)
        .map_err(|err| SqlError::new(format!("replace redshift state: {err}")))?;
    Ok(())
}

/// Mirrors legacy `os.WriteFile(path, data, 0o600)`.
fn write_private_file(path: &Path, data: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Mirrors `stateFilePath`.
fn state_file_path(storage_path: &str) -> Option<PathBuf> {
    if storage_path.is_empty() {
        return None;
    }
    Some(Path::new(storage_path).join(STATE_FILE_NAME))
}

/// Mirrors `databaseToStored`.
fn database_to_stored(db: &Database) -> StoredDatabase {
    let mut stored = StoredDatabase::default();
    for (schema_name, schema_state) in &db.schemas {
        let mut stored_schema = StoredSchema::default();
        for (table_name, table_state) in &schema_state.tables {
            stored_schema.tables.insert(
                table_name.clone(),
                StoredTable {
                    name: StoredQualifiedName {
                        schema: table_state.name.schema.clone(),
                        table: table_state.name.table.clone(),
                    },
                    columns: columns_to_stored(&table_state.columns),
                    rows: table_state.rows.clone(),
                    kind: table_state.kind.clone(),
                    view_sql: table_state.view_sql.clone(),
                    dist_style: table_state.dist_style.clone(),
                    dist_key: table_state.dist_key.clone(),
                    sort_keys: table_state.sort_keys.clone(),
                },
            );
        }
        stored.schemas.insert(schema_name.clone(), stored_schema);
    }
    stored
}

/// Mirrors `databaseFromStored` (name fallbacks from map keys + public schema).
fn database_from_stored(stored: StoredDatabase) -> Database {
    let mut db = Database::default();
    for (schema_name, stored_schema) in stored.schemas {
        let mut schema_state = Schema::default();
        for (table_name, stored_table) in stored_schema.tables {
            let mut name = QualifiedName {
                schema: stored_table.name.schema,
                table: stored_table.name.table,
            };
            if name.schema.is_empty() {
                name.schema = schema_name.clone();
            }
            if name.table.is_empty() {
                name.table = table_name.clone();
            }
            schema_state.tables.insert(
                table_name,
                Table {
                    name,
                    columns: columns_from_stored(stored_table.columns),
                    rows: stored_table.rows,
                    kind: stored_table.kind,
                    view_sql: stored_table.view_sql,
                    dist_style: stored_table.dist_style,
                    dist_key: stored_table.dist_key,
                    sort_keys: stored_table.sort_keys,
                },
            );
        }
        db.schemas.insert(schema_name, schema_state);
    }
    ensure_public_schema(&mut db);
    db
}

/// Mirrors `columnsToStored`.
fn columns_to_stored(columns: &[Column]) -> Vec<StoredColumn> {
    columns
        .iter()
        .map(|column| StoredColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            encoding: column.encoding.clone(),
            default_value: column.default_value.clone(),
            identity: column.identity,
        })
        .collect()
}

/// Mirrors `columnsFromStored`.
fn columns_from_stored(columns: Vec<StoredColumn>) -> Vec<Column> {
    columns
        .into_iter()
        .map(|stored| Column {
            name: stored.name,
            data_type: stored.data_type,
            encoding: stored.encoding,
            default_value: stored.default_value,
            identity: stored.identity,
            ..Column::default()
        })
        .collect()
}

/// Mirrors `statementsToStored` (nil when empty → omitted from the JSON).
fn statements_to_stored(
    statements: &BTreeMap<String, StatementRecord>,
) -> Option<BTreeMap<String, StoredStatement>> {
    if statements.is_empty() {
        return None;
    }
    let stored = statements
        .iter()
        .map(|(id, stmt)| {
            (
                id.clone(),
                StoredStatement {
                    id: stmt.id.clone(),
                    cluster_identifier: stmt.cluster_identifier.clone(),
                    database: stmt.database.clone(),
                    db_user: stmt.db_user.clone(),
                    session_id: stmt.session_id.clone(),
                    query_string: stmt.query_string.clone(),
                    result_format: default_str(&stmt.result_format, "JSON"),
                    created_at: format_rfc3339_nano(stmt.created_at),
                    updated_at: format_rfc3339_nano(stmt.updated_at),
                    status: stmt.status.clone(),
                    error: stmt.error.clone(),
                    has_result_set: stmt.has_result_set,
                    result: query_result_to_stored(&stmt.result),
                },
            )
        })
        .collect();
    Some(stored)
}

/// Mirrors `snapshotsFromStored` (identifier fallback from the map key).
fn snapshots_from_stored(
    stored: BTreeMap<String, ClusterSnapshotMetadata>,
) -> BTreeMap<String, ClusterSnapshotMetadata> {
    stored
        .into_iter()
        .map(|(id, mut snapshot)| {
            snapshot.snapshot_identifier = default_str(&snapshot.snapshot_identifier, &id);
            (id, snapshot)
        })
        .collect()
}

/// Mirrors `statementsFromStored`.
fn statements_from_stored(
    stored: BTreeMap<String, StoredStatement>,
) -> BTreeMap<String, StatementRecord> {
    stored
        .into_iter()
        .map(|(id, stored_stmt)| {
            let created_at = parse_stored_time(&stored_stmt.created_at);
            let mut updated_at = parse_stored_time(&stored_stmt.updated_at);
            if updated_at == legacy_zero_time() {
                updated_at = created_at;
            }
            let stmt_id = default_str(&stored_stmt.id, &id);
            (
                id,
                StatementRecord {
                    id: stmt_id,
                    cluster_identifier: stored_stmt.cluster_identifier,
                    database: stored_stmt.database,
                    db_user: stored_stmt.db_user,
                    session_id: stored_stmt.session_id,
                    query_string: stored_stmt.query_string,
                    result_format: default_str(&stored_stmt.result_format, "JSON"),
                    created_at,
                    updated_at,
                    status: stored_stmt.status,
                    error: stored_stmt.error,
                    has_result_set: stored_stmt.has_result_set,
                    result: query_result_from_stored(stored_stmt.result),
                },
            )
        })
        .collect()
}

/// Mirrors `queryResultToStored`.
fn query_result_to_stored(result: &QueryResult) -> StoredQueryResult {
    StoredQueryResult {
        fields: result
            .fields
            .iter()
            .map(|field| StoredPgField {
                name: field.name.clone(),
                type_oid: field.type_oid,
                type_size: field.type_size,
            })
            .collect(),
        rows: result.rows.clone(),
        tag: result.tag.clone(),
    }
}

/// Mirrors `queryResultFromStored`.
fn query_result_from_stored(stored: StoredQueryResult) -> QueryResult {
    QueryResult {
        fields: stored
            .fields
            .into_iter()
            .map(|field| PgField {
                name: field.name,
                type_oid: field.type_oid,
                type_size: field.type_size,
            })
            .collect(),
        rows: stored.rows,
        tag: stored.tag,
    }
}

/// Mirrors `maxStoredStatementSequence`.
fn max_stored_statement_sequence(statements: &BTreeMap<String, StatementRecord>) -> i64 {
    let mut max_id = 0;
    for stmt in statements.values() {
        let Some(last) = stmt.id.split('-').next_back() else {
            continue;
        };
        if let Ok(id) = last.parse::<i64>() {
            if id > max_id {
                max_id = id;
            }
        }
    }
    max_id
}

// --- time helpers -----------------------------------------------------------
//
// legacy stores timestamps as RFC3339Nano (`2006-01-02T15:04:05.999999999Z07:00`:
// trailing fraction zeros trimmed, "Z" for UTC) and parses them back with
// `time.Parse`, falling back to the zero time (0001-01-01T00:00:00Z) on error.

/// Seconds from legacy zero time (year 1) to the Unix epoch.
const GO_ZERO_TIME_SECONDS_BEFORE_EPOCH: u64 = 62_135_596_800;

/// Mirrors legacy `time.Time{}` as a `SystemTime`.
pub(crate) fn legacy_zero_time() -> SystemTime {
    UNIX_EPOCH - Duration::from_secs(GO_ZERO_TIME_SECONDS_BEFORE_EPOCH)
}

/// Mirrors `parseStoredTime` (zero time on empty/invalid input).
fn parse_stored_time(value: &str) -> SystemTime {
    parse_rfc3339_nano(value).unwrap_or_else(legacy_zero_time)
}

/// Mirrors `time.Time.Unix()`.
pub(crate) fn unix_seconds(time: SystemTime) -> i64 {
    let (secs, _) = unix_parts(time);
    secs
}

/// (seconds, nanos) since the Unix epoch with floored seconds (legacy semantics).
fn unix_parts(time: SystemTime) -> (i64, u32) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => (after.as_secs() as i64, after.subsec_nanos()),
        Err(err) => {
            let before = err.duration();
            if before.subsec_nanos() == 0 {
                (-(before.as_secs() as i64), 0)
            } else {
                (
                    -(before.as_secs() as i64) - 1,
                    1_000_000_000 - before.subsec_nanos(),
                )
            }
        }
    }
}

fn system_time_from_unix(secs: i64, nanos: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs()) + Duration::new(0, nanos)
    }
}

/// Howard Hinnant's `days_from_civil`.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64; // [0, 399]
    let mp = if month > 2 { month - 3 } else { month + 9 } as i64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + day as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if month <= 2 { y + 1 } else { y }, month, day)
}

/// Formats a `SystemTime` like legacy `t.UTC().Format(time.RFC3339)` (no
/// fractional seconds, `Z` zone). Used for cluster-snapshot timestamps.
pub(crate) fn format_rfc3339(time: SystemTime) -> String {
    let (secs, _) = unix_parts(time);
    let days = secs.div_euclid(86_400);
    let second_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        second_of_day / 3600,
        (second_of_day / 60) % 60,
        second_of_day % 60,
    );
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Formats a `SystemTime` like legacy `t.UTC().Format(time.RFC3339Nano)`.
pub(crate) fn format_rfc3339_nano(time: SystemTime) -> String {
    let (secs, nanos) = unix_parts(time);
    let days = secs.div_euclid(86_400);
    let second_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        second_of_day / 3600,
        (second_of_day / 60) % 60,
        second_of_day % 60,
    );
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos > 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        out.push('.');
        out.push_str(&fraction);
    }
    out.push('Z');
    out
}

/// Parses RFC3339(Nano) (`Z` or `±HH:MM` offsets). Mirrors what
/// `time.Parse(time.RFC3339Nano, ...)` accepts for stored state.
pub(crate) fn parse_rfc3339_nano(value: &str) -> Option<SystemTime> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = value.get(range)?;
        if part.bytes().all(|b| b.is_ascii_digit()) {
            part.parse::<i64>().ok()
        } else {
            None
        }
    };
    if bytes[4] != b'-' || bytes[7] != b'-' || (bytes[10] != b'T' && bytes[10] != b't') {
        return None;
    }
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut pos = 19;
    let mut nanos: u32 = 0;
    if bytes.get(pos) == Some(&b'.') {
        let start = pos + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end == start || end - start > 9 {
            return None;
        }
        let mut fraction = value[start..end].to_string();
        while fraction.len() < 9 {
            fraction.push('0');
        }
        nanos = fraction.parse().ok()?;
        pos = end;
    }
    let offset_seconds: i64 = match bytes.get(pos) {
        Some(b'Z') | Some(b'z') if pos + 1 == bytes.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            if pos + 6 != bytes.len() || bytes[pos + 3] != b':' {
                return None;
            }
            let oh = digits(pos + 1..pos + 3)?;
            let om = digits(pos + 4..pos + 6)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let magnitude = oh * 3600 + om * 60;
            if *sign == b'-' {
                -magnitude
            } else {
                magnitude
            }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month as u32, day as u32);
    let secs = days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds;
    Some(system_time_from_unix(secs, nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_nano_round_trips_legacy_formatting() {
        for (input, normalized) in [
            ("2024-05-06T07:08:09Z", "2024-05-06T07:08:09Z"),
            ("2024-05-06T07:08:09.5Z", "2024-05-06T07:08:09.5Z"),
            (
                "2024-12-31T23:59:59.123456789Z",
                "2024-12-31T23:59:59.123456789Z",
            ),
            ("2024-05-06T07:08:09.120Z", "2024-05-06T07:08:09.12Z"),
            ("2024-05-06T09:08:09+02:00", "2024-05-06T07:08:09Z"),
            ("0001-01-01T00:00:00Z", "0001-01-01T00:00:00Z"),
        ] {
            let parsed = parse_rfc3339_nano(input).unwrap_or_else(|| panic!("parse {input}"));
            assert_eq!(format_rfc3339_nano(parsed), normalized, "input {input}");
        }
        assert_eq!(parse_rfc3339_nano(""), None);
        assert_eq!(parse_rfc3339_nano("not-a-time"), None);
        assert_eq!(parse_rfc3339_nano("2024-05-06 07:08:09Z"), None);
    }

    #[test]
    fn legacy_zero_time_matches_legacy_unix_seconds() {
        // legacy: time.Time{}.Unix() == -62135596800.
        assert_eq!(unix_seconds(legacy_zero_time()), -62_135_596_800);
        assert_eq!(
            format_rfc3339_nano(legacy_zero_time()),
            "0001-01-01T00:00:00Z"
        );
    }

    /// The part-3 slice of legacy `TestStatePersistsCatalogRowsAndClusterMetadata`
    /// / `TestStatePersistsDataAPIStatementHistoryAndResults` (the full tests
    /// need the cluster control plane + Data API and are ported in part 4):
    /// catalog rows and statement history survive a state.json reload.
    #[test]
    fn state_persists_catalog_rows_and_statement_history() {
        use crate::{Config, Server};

        let dir = std::env::temp_dir().join(format!(
            "devcloud-redshift-state-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp storage dir");
        let storage_path = dir.to_string_lossy().to_string();

        let server = Server::new(Config {
            storage_path: storage_path.clone(),
            cluster_identifier: "devcloud".to_string(),
            database: "dev".to_string(),
            user: "dev".to_string(),
            ..Config::default()
        });
        let mut wire = Vec::new();
        server.handle_simple_query(
            &mut wire,
            "create schema if not exists loop;\n\
             create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id);\n\
             insert into loop.events values (1, 'created');",
        );

        let reloaded = Server::new(Config {
            storage_path: storage_path.clone(),
            database: "dev".to_string(),
            user: "dev".to_string(),
            ..Config::default()
        });
        let result = reloaded
            .execute_sql("select id, payload from loop.events where id = 1")
            .expect("select after reload");
        assert_eq!(
            result.rows,
            vec![vec!["1".to_string(), "created".to_string()]],
            "rows after reload"
        );
        let statements = reloaded.statement_snapshots();
        assert_eq!(statements.len(), 3, "statement history after reload");
        assert!(
            statements.iter().all(|stmt| stmt.status == "FINISHED"),
            "statement history after reload = {statements:?}"
        );
        // A new statement continues the persisted id sequence.
        let snapshot =
            reloaded.record_sql_history("select 1", &crate::engine::QueryResult::default(), None);
        assert_eq!(snapshot.id, "devcloud-redshift-4");

        std::fs::remove_dir_all(&dir).expect("remove temp storage dir");
    }
}
