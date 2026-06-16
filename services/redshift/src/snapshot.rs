//! Dashboard catalog + statement snapshot types and safe SQL preview helpers.
//!
//! Parity: the catalog/statement slice of `internal/services/redshift/snapshot.rs`
//! (`CatalogSnapshot`, `StatementSnapshot`, `Server.StatementSnapshots`) plus
//! `safeSQLPreview` / `redshiftQueryID` from `dataapi.rs`. The dashboard SQL
//! runner (`ExecuteDashboardSQL`) arrives with the Data API port (part 4).

use serde::Serialize;

use crate::cluster::{
    cluster_snapshot_from_config, default_cluster_identifier, ClusterEndpoint, ClusterSnapshot,
    ServerlessNamespace, ServerlessWorkgroup,
};
use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::model::table_snapshot_type;
use crate::pg_types::pg_field_type_name;
use crate::server::{default_str, Server, ServerShared, StatementRecord};
use crate::sql_parse::split_sql_statements;
use crate::storage::unix_seconds;

/// Carries the failing statement snapshot alongside the error, mirroring legacy
/// `ExecuteDashboardSQL` returning `(QueryResultSnapshot{Statement: ...}, err)`.
#[derive(Debug)]
pub struct DashboardSqlError {
    pub statement: StatementSnapshot,
    pub error: SqlError,
}

impl std::fmt::Display for DashboardSqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for DashboardSqlError {}

/// Mirrors the service-level `Snapshot` (control-plane status + clusters).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ServiceSnapshot {
    pub status: String,
    pub running: bool,
    pub region: String,
    #[serde(rename = "storagePath", skip_serializing_if = "String::is_empty")]
    pub storage_path: String,
    #[serde(rename = "backendKind")]
    pub backend_kind: String,
    #[serde(rename = "backendMode")]
    pub backend_mode: String,
    pub clusters: Vec<ClusterSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CatalogSnapshot {
    pub database: String,
    pub schemas: Vec<SchemaSnapshot>,
    pub tables: Vec<TableSnapshot>,
    pub columns: Vec<TableColumnSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SchemaSnapshot {
    pub name: String,
    pub owner: String,
    #[serde(rename = "tableCount")]
    pub table_count: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TableSnapshot {
    pub schema: String,
    pub name: String,
    #[serde(rename = "type")]
    pub table_type: String,
    #[serde(rename = "columnCount")]
    pub column_count: usize,
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    #[serde(rename = "distStyle")]
    pub dist_style: String,
    #[serde(rename = "distKey", skip_serializing_if = "String::is_empty")]
    pub dist_key: String,
    #[serde(rename = "sortKeys", skip_serializing_if = "Vec::is_empty")]
    pub sort_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TableColumnSnapshot {
    pub schema: String,
    pub table: String,
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: String,
    pub ordinal: usize,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub encoding: String,
    #[serde(rename = "defaultValue", skip_serializing_if = "String::is_empty")]
    pub default_value: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub identity: bool,
}

/// Mirrors `TableDetailSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TableDetailSnapshot {
    pub table: TableSnapshot,
    pub columns: Vec<TableColumnSnapshot>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Vec<String>>,
}

/// Mirrors `QueryFieldSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryFieldSnapshot {
    pub name: String,
    #[serde(rename = "typeName")]
    pub type_name: String,
}

/// Mirrors `QueryResultSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryResultSnapshot {
    pub statement: StatementSnapshot,
    pub columns: Vec<QueryFieldSnapshot>,
    pub rows: Vec<Vec<String>>,
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    #[serde(rename = "commandTag")]
    pub command_tag: String,
}

/// Mirrors `StatementSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StatementSnapshot {
    pub id: String,
    #[serde(rename = "clusterIdentifier")]
    pub cluster_identifier: String,
    pub database: String,
    #[serde(rename = "dbUser")]
    pub db_user: String,
    #[serde(rename = "sessionId", skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    pub status: String,
    #[serde(rename = "queryPreview")]
    pub query_preview: String,
    #[serde(rename = "queryRedacted")]
    pub query_redacted: bool,
    #[serde(rename = "queryTruncated")]
    pub query_truncated: bool,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "updatedAt")]
    pub updated_at: i64,
    #[serde(rename = "hasResultSet")]
    pub has_result_set: bool,
    #[serde(rename = "resultRows")]
    pub result_rows: usize,
    #[serde(rename = "redshiftQueryId")]
    pub redshift_query_id: i64,
}

impl Server {
    /// Mirrors `Server.StatementSnapshots` (ids sorted byte-wise, which is the
    /// BTreeMap iteration order).
    pub fn statement_snapshots(&self) -> Vec<StatementSnapshot> {
        let state = self.shared.lock_state();
        state
            .statements
            .values()
            .map(statement_snapshot_from_record)
            .collect()
    }

    /// Mirrors `Server.TableDetailSnapshot`: table metadata, columns, and up to
    /// `limit` rows (default 100). Returns `None` when the table is missing.
    pub fn table_detail_snapshot(
        &self,
        schema_name: &str,
        table_name: &str,
        limit: usize,
    ) -> Option<TableDetailSnapshot> {
        let limit = if limit == 0 { 100 } else { limit };
        let state = self.shared.lock_state();
        let schema = default_str(schema_name, "public");
        let table_state = state.db.schemas.get(&schema)?.tables.get(table_name)?;
        let mut detail = TableDetailSnapshot {
            table: TableSnapshot {
                schema: table_state.name.schema.clone(),
                name: table_state.name.table.clone(),
                table_type: table_snapshot_type(table_state).to_string(),
                column_count: table_state.columns.len(),
                row_count: table_state.rows.len(),
                dist_style: default_str(&table_state.dist_style, "even"),
                dist_key: table_state.dist_key.clone(),
                sort_keys: table_state.sort_keys.clone(),
            },
            columns: Vec::new(),
            rows: Vec::new(),
        };
        for (i, column) in table_state.columns.iter().enumerate() {
            detail.columns.push(TableColumnSnapshot {
                schema: table_state.name.schema.clone(),
                table: table_state.name.table.clone(),
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                ordinal: i + 1,
                encoding: column.encoding.clone(),
                default_value: column.default_value.clone(),
                identity: column.identity,
            });
        }
        let row_limit = limit.min(table_state.rows.len());
        detail.rows = table_state.rows[..row_limit].to_vec();
        Some(detail)
    }

    /// Mirrors `Server.ExecuteDashboardSQL`: runs each `;`-split statement
    /// through the translator+backend path, records redacted SQL history, and
    /// returns the last statement's columns/rows (capped at `max_rows`).
    pub fn execute_dashboard_sql(
        &self,
        sql: &str,
        max_rows: usize,
    ) -> Result<QueryResultSnapshot, DashboardSqlError> {
        let max_rows = if max_rows == 0 { 100 } else { max_rows };
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Err(DashboardSqlError {
                statement: StatementSnapshot::default(),
                error: SqlError::new("SQL is required"),
            });
        }
        let mut last_result = QueryResult::default();
        let mut last_snapshot = StatementSnapshot::default();
        for statement_text in &statements {
            if let Err(err) = self.shared.validate_statement_size(statement_text) {
                last_snapshot = self.record_sql_history(
                    "[statement exceeds maxStatementBytes]",
                    &QueryResult::default(),
                    Some(&err),
                );
                return Err(DashboardSqlError {
                    statement: last_snapshot,
                    error: err,
                });
            }
            let result = self.execute_sql(statement_text);
            match result {
                Ok(result) => {
                    last_snapshot = self.record_sql_history(statement_text, &result, None);
                    last_result = result;
                }
                Err(err) => {
                    last_snapshot = self.record_sql_history(
                        statement_text,
                        &QueryResult::default(),
                        Some(&err),
                    );
                    return Err(DashboardSqlError {
                        statement: last_snapshot,
                        error: err,
                    });
                }
            }
        }
        let columns = last_result
            .fields
            .iter()
            .map(|field| QueryFieldSnapshot {
                name: field.name.clone(),
                type_name: pg_field_type_name(field).to_string(),
            })
            .collect();
        let row_limit = max_rows.min(last_result.rows.len());
        Ok(QueryResultSnapshot {
            statement: last_snapshot,
            columns,
            rows: last_result.rows[..row_limit].to_vec(),
            row_count: last_result.rows.len(),
            command_tag: last_result.tag.clone(),
        })
    }

    /// Mirrors `Server.Snapshot` (service status snapshot).
    pub fn service_snapshot(&self) -> ServiceSnapshot {
        let config = &self.shared.config;
        ServiceSnapshot {
            status: "running".to_string(),
            running: true,
            region: default_str(&config.region, "us-east-1"),
            storage_path: config.storage_path.clone(),
            backend_kind: default_str(&config.backend_kind, "memory"),
            backend_mode: default_str(&config.backend_mode, "embedded"),
            clusters: self.shared.cluster_snapshots_locked(),
        }
    }
}

impl ServerShared {
    /// Mirrors `clusterSnapshotsLocked` (sorted by identifier = BTreeMap order).
    pub(crate) fn cluster_snapshots_locked(&self) -> Vec<ClusterSnapshot> {
        self.lock_state().clusters.values().cloned().collect()
    }

    /// Mirrors `Server.clusterSnapshot` (the configured/default cluster).
    pub(crate) fn cluster_snapshot(&self) -> ClusterSnapshot {
        let id = default_cluster_identifier(&self.config.cluster_identifier);
        if let Some(cluster) = self.lock_state().clusters.get(&id) {
            return cluster.clone();
        }
        cluster_snapshot_from_config(&self.config)
    }

    /// Mirrors `Server.serverlessNamespace`.
    pub(crate) fn serverless_namespace(&self) -> ServerlessNamespace {
        let cluster = self.cluster_snapshot();
        let database = default_str(
            &cluster.database_name,
            &default_str(&self.config.database, "dev"),
        );
        ServerlessNamespace {
            namespace_name: database.clone(),
            db_name: database,
            status: "AVAILABLE".to_string(),
        }
    }

    /// Mirrors `Server.serverlessWorkgroup`.
    pub(crate) fn serverless_workgroup(&self) -> ServerlessWorkgroup {
        let cluster = self.cluster_snapshot();
        ServerlessWorkgroup {
            workgroup_name: cluster.cluster_identifier.clone(),
            namespace_name: default_str(
                &cluster.database_name,
                &default_str(&self.config.database, "dev"),
            ),
            status: "AVAILABLE".to_string(),
            endpoint: ClusterEndpoint {
                address: cluster.endpoint.address,
                port: cluster.endpoint.port,
            },
        }
    }
}

/// Mirrors `statementSnapshotFromStatement`.
pub(crate) fn statement_snapshot_from_record(stmt: &StatementRecord) -> StatementSnapshot {
    let (preview, redacted, truncated) = safe_sql_preview(&stmt.query_string, 200);
    StatementSnapshot {
        id: stmt.id.clone(),
        cluster_identifier: stmt.cluster_identifier.clone(),
        database: stmt.database.clone(),
        db_user: stmt.db_user.clone(),
        session_id: stmt.session_id.clone(),
        status: stmt.status.clone(),
        query_preview: preview,
        query_redacted: redacted,
        query_truncated: truncated,
        created_at: unix_seconds(stmt.created_at),
        updated_at: unix_seconds(stmt.updated_at),
        has_result_set: stmt.has_result_set,
        result_rows: stmt.result.rows.len(),
        redshift_query_id: redshift_query_id(&stmt.id),
    }
}

impl ServerShared {
    /// Mirrors `Server.CatalogSnapshot`.
    pub(crate) fn catalog_snapshot(&self) -> CatalogSnapshot {
        let state = self.lock_state();
        let mut snapshot = CatalogSnapshot {
            database: default_str(&self.config.database, "dev"),
            ..CatalogSnapshot::default()
        };
        for (schema_name, schema_state) in &state.db.schemas {
            snapshot.schemas.push(SchemaSnapshot {
                name: schema_name.clone(),
                owner: default_str(&self.config.user, "dev"),
                table_count: schema_state.tables.len(),
            });
            for (table_name, table_state) in &schema_state.tables {
                snapshot.tables.push(TableSnapshot {
                    schema: schema_name.clone(),
                    name: table_name.clone(),
                    table_type: table_snapshot_type(table_state).to_string(),
                    column_count: table_state.columns.len(),
                    row_count: table_state.rows.len(),
                    dist_style: default_str(&table_state.dist_style, "even"),
                    dist_key: table_state.dist_key.clone(),
                    sort_keys: table_state.sort_keys.clone(),
                });
                for (i, column) in table_state.columns.iter().enumerate() {
                    snapshot.columns.push(TableColumnSnapshot {
                        schema: schema_name.clone(),
                        table: table_name.clone(),
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        ordinal: i + 1,
                        encoding: column.encoding.clone(),
                        default_value: column.default_value.clone(),
                        identity: column.identity,
                    });
                }
            }
        }
        snapshot
    }
}

/// Mirrors `safeSQLPreview`: whitespace-collapsed preview, fully redacted when
/// any credential-ish token appears, truncated to `max_bytes` otherwise.
/// Returns (preview, redacted, truncated).
pub(crate) fn safe_sql_preview(sql: &str, max_bytes: usize) -> (String, bool, bool) {
    let preview = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if preview.is_empty() || max_bytes == 0 {
        return (String::new(), false, false);
    }
    let lower = preview.to_lowercase();
    for token in [
        "authorization",
        "credentials",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "iam_role",
        "password",
    ] {
        if lower.contains(token) {
            return ("[redacted]".to_string(), true, false);
        }
    }
    if preview.len() <= max_bytes {
        return (preview, false, false);
    }
    let mut end = max_bytes;
    while !preview.is_char_boundary(end) {
        end -= 1;
    }
    (preview[..end].to_string(), false, true)
}

/// Mirrors `redshiftQueryID`: deterministic numeric id derived from the
/// statement id with legacy exact wrapping/negation arithmetic (never 0).
pub(crate) fn redshift_query_id(id: &str) -> i64 {
    let mut result: i64 = 0;
    for ch in id.chars() {
        result = result.wrapping_mul(31).wrapping_add(ch as i64);
        if result < 0 {
            result = result.wrapping_neg();
        }
    }
    if result == 0 {
        return 1;
    }
    result
}
