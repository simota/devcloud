//! Server wiring: config, shared state, and the SQL execution entry point.
//!
//! Parity: the SQL-foundation slice of `internal/services/redshift/server.rs`
//! and `sql_execute.rs`, plus part 3's state.json restore in `NewServer` and
//! `executeSQLBatch`. The HTTP control plane and Data API land in later parts
//! of increment #10; the pgwire listener lives in `pgwire.rs`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use devcloud_s3::store::FileBucketStore;

use crate::backend::{self, SqlBackend};
use crate::backend_memory::MemoryBackend;
use crate::cluster::{
    cluster_snapshot_from_config, default_cluster_identifier, normalize_cluster_endpoints,
    ClusterSnapshot, ClusterSnapshotMetadata,
};
use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::model::{ensure_public_schema, Column, Database, QualifiedName, Table};
use crate::pg_types::PgField;
use crate::storage;
use crate::translator::{
    MetadataEffect, MetadataEffectKind, Passthrough, RedshiftTranslator, Session,
};

/// Mirrors legacy `Config`.
#[derive(Default)]
pub struct Config {
    pub sql_addr: String,
    pub api_addr: String,
    pub region: String,
    pub cluster_identifier: String,
    pub database: String,
    pub node_type: String,
    pub number_of_nodes: i64,
    pub storage_path: String,
    pub max_statement_bytes: i64,
    pub max_copy_input_bytes: i64,
    pub auth_mode: String,
    pub user: String,
    pub password: String,
    pub account_id: String,
    pub backend_kind: String,
    pub backend_mode: String,
    /// Mirrors `Config.ObjectStore`: shared S3 `FileBucketStore` used by
    /// COPY/UNLOAD over `s3://` URIs. `None` mirrors legacy nil ObjectStore.
    pub object_store: Option<Arc<FileBucketStore>>,
    pub sql_backend: Option<Arc<dyn SqlBackend>>,
    pub translator: Option<Arc<dyn RedshiftTranslator>>,
    /// Daemon seam only: when true, Data API mutations print `DEVCLOUD_EVENT`
    /// lines on stdout. legacy tests run with a nil publisher (off).
    pub events_enabled: bool,
}

pub struct Server {
    pub(crate) shared: Arc<ServerShared>,
    backend: Arc<dyn SqlBackend>,
    translator: Arc<dyn RedshiftTranslator>,
}

pub struct ServerShared {
    pub(crate) config: SharedConfig,
    pub(crate) state: Mutex<ServerState>,
}

pub(crate) struct SharedConfig {
    pub sql_addr: String,
    pub region: String,
    pub cluster_identifier: String,
    pub database: String,
    pub node_type: String,
    pub number_of_nodes: i64,
    pub user: String,
    pub password: String,
    pub auth_mode: String,
    pub storage_path: String,
    pub max_statement_bytes: i64,
    pub max_copy_input_bytes: i64,
    pub account_id: String,
    pub backend_kind: String,
    pub backend_mode: String,
    pub object_store: Option<Arc<FileBucketStore>>,
    pub events_enabled: bool,
}

pub(crate) struct ServerState {
    pub db: Database,
    /// Provisioned-cluster control plane. legacy uses `map[string]ClusterSnapshot`
    /// and sorts ids on output (`sort.Strings`); a BTreeMap gives the same
    /// byte-wise ordering directly.
    pub clusters: BTreeMap<String, ClusterSnapshot>,
    /// Cluster-snapshot control plane (`map[string]ClusterSnapshotMetadata`).
    pub snapshots: BTreeMap<String, ClusterSnapshotMetadata>,
    /// Statement history (Data API / pgwire). legacy uses a `map[string]*statement`.
    pub statements: BTreeMap<String, StatementRecord>,
    /// Data API session metadata (`map[string]*session`).
    pub sessions: BTreeMap<String, SessionRecord>,
    /// Mirrors `Server.nextStatementID`.
    pub next_statement_id: i64,
    /// Mirrors `Server.nextSessionID`.
    pub next_session_id: i64,
    /// Mirrors `Server.clientTokenIndex` (Data API idempotency).
    pub client_token_index: BTreeMap<String, String>,
}

/// Mirrors legacy `statement` struct (dataapi.rs).
#[derive(Debug, Clone)]
pub struct StatementRecord {
    pub id: String,
    pub cluster_identifier: String,
    pub database: String,
    pub db_user: String,
    pub session_id: String,
    pub query_string: String,
    pub result_format: String,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub status: String,
    pub error: String,
    pub has_result_set: bool,
    pub result: QueryResult,
}

/// Mirrors legacy `session` struct (dataapi.rs).
#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub id: String,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub expires_at: SystemTime,
    pub session_keep_alive_seconds: i64,
}

impl Server {
    /// Mirrors `NewServer`: restores state.json (errors ignored exactly like
    /// legacy), seeds the `public` schema and the default cluster, normalizes
    /// restored cluster endpoints, and wires the memory backend unless injected.
    pub fn new(cfg: Config) -> Server {
        let config = SharedConfig {
            sql_addr: cfg.sql_addr,
            region: cfg.region,
            cluster_identifier: cfg.cluster_identifier,
            database: cfg.database,
            node_type: cfg.node_type,
            number_of_nodes: cfg.number_of_nodes,
            user: cfg.user,
            password: cfg.password,
            auth_mode: cfg.auth_mode,
            storage_path: cfg.storage_path,
            max_statement_bytes: cfg.max_statement_bytes,
            max_copy_input_bytes: cfg.max_copy_input_bytes,
            account_id: cfg.account_id,
            backend_kind: cfg.backend_kind,
            backend_mode: cfg.backend_mode,
            object_store: cfg.object_store,
            events_enabled: cfg.events_enabled,
        };

        let mut db = Database::default();
        let mut clusters: BTreeMap<String, ClusterSnapshot> = BTreeMap::new();
        clusters.insert(
            default_cluster_identifier(&config.cluster_identifier),
            cluster_snapshot_from_config(&config),
        );
        let mut snapshots: BTreeMap<String, ClusterSnapshotMetadata> = BTreeMap::new();
        let mut statements = BTreeMap::new();
        let mut client_token_index = BTreeMap::new();
        let mut next_statement_id = 0;
        if let Ok(loaded) = storage::load_state(&config.storage_path) {
            if let Some(stored_db) = loaded.database {
                db = stored_db;
            }
            if !loaded.clusters.is_empty() {
                clusters = loaded.clusters;
                normalize_cluster_endpoints(&mut clusters, &config);
            }
            if !loaded.snapshots.is_empty() {
                snapshots = loaded.snapshots;
            }
            if !loaded.statements.is_empty() {
                statements = loaded.statements;
            }
            if !loaded.client_token_index.is_empty() {
                client_token_index = loaded.client_token_index;
            }
            next_statement_id = loaded.next_statement_id;
        }
        ensure_public_schema(&mut db);
        let shared = Arc::new(ServerShared {
            config,
            state: Mutex::new(ServerState {
                db,
                clusters,
                snapshots,
                statements,
                sessions: BTreeMap::new(),
                next_statement_id,
                next_session_id: 0,
                client_token_index,
            }),
        });
        let backend: Arc<dyn SqlBackend> = match cfg.sql_backend {
            Some(injected) => injected,
            None => {
                let exec_shared = Arc::clone(&shared);
                let catalog_shared = Arc::clone(&shared);
                Arc::new(MemoryBackend::new(
                    Some(Box::new(move |statement| {
                        exec_shared.execute_sql_memory_backend(statement)
                    })),
                    Some(Box::new(move || catalog_shared.memory_catalog_snapshot())),
                ))
            }
        };
        let translator: Arc<dyn RedshiftTranslator> = match cfg.translator {
            Some(injected) => injected,
            None => Arc::new(Passthrough),
        };
        Server {
            shared,
            backend,
            translator,
        }
    }

    /// Mirrors `Server.executeSQL`: runs the statement through the configured
    /// `RedshiftTranslator` (default: passthrough), executes the backend SQL,
    /// then applies any translator metadata effects.
    pub fn execute_sql(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let session = Session {
            database: default_str(&self.shared.config.database, "dev"),
            user: default_str(&self.shared.config.user, "dev"),
            schema: "public".to_string(),
        };
        let translated = self.translator.translate(&session, statement)?;
        if translated.handled_by_devcloud {
            return Err(SqlError::new(
                "devcloud-handled Redshift translation results are not wired yet",
            ));
        }
        if !translated.parameters.is_empty() {
            return Err(SqlError::new(
                "Redshift SQL translation parameters are not supported yet",
            ));
        }
        if !translated.side_effects.is_empty() {
            return Err(SqlError::new(
                "Redshift SQL translation side effects are not supported yet",
            ));
        }
        let backend_sql = if translated.backend_sql.trim().is_empty() {
            statement
        } else {
            translated.backend_sql.as_str()
        };
        let result = self.backend.exec(backend_sql)?;
        self.shared
            .apply_translation_metadata_effects(&translated.metadata_effects)?;
        Ok(query_result_from_backend(result))
    }

    /// Mirrors `Server.executeSQLBatch`: runs the statements in order and
    /// rolls the in-memory catalog back to its prior state when any fails.
    pub fn execute_sql_batch(&self, statements: &[String]) -> Result<QueryResult, SqlError> {
        // legacy deep-copies via databaseFromStored(databaseToStored(s.db));
        // `Database` owns all of its data, so a clone is the same copy.
        let previous = self.shared.lock_state().db.clone();

        let mut result = QueryResult::default();
        for statement in statements {
            match self.execute_sql(statement) {
                Ok(statement_result) => result = statement_result,
                Err(err) => {
                    let mut state = self.shared.lock_state();
                    state.db = previous;
                    ensure_public_schema(&mut state.db);
                    self.shared.persist_locked(&state)?;
                    return Err(err);
                }
            }
        }
        Ok(result)
    }

    pub fn backend(&self) -> &Arc<dyn SqlBackend> {
        &self.backend
    }

    /// Mirrors `Server.CatalogSnapshot` (dashboard catalog snapshot).
    pub fn catalog_snapshot(&self) -> crate::snapshot::CatalogSnapshot {
        self.shared.catalog_snapshot()
    }
}

impl ServerShared {
    pub(crate) fn lock_state(&self) -> MutexGuard<'_, ServerState> {
        self.state.lock().unwrap()
    }

    /// Mirrors `executeSQLMemoryBackend` (the exec function handed to the
    /// memory backend).
    fn execute_sql_memory_backend(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<backend::ExecResult, SqlError> {
        let result = self.execute_sql_memory(statement)?;
        Ok(query_result_to_backend(result))
    }

    /// Mirrors `memoryCatalogSnapshot`.
    fn memory_catalog_snapshot(&self) -> Result<backend::CatalogSnapshot, SqlError> {
        let state = self.lock_state();
        let mut schemas = Vec::with_capacity(state.db.schemas.len());
        for (schema_name, schema_state) in &state.db.schemas {
            let mut tables = Vec::with_capacity(schema_state.tables.len());
            for (table_name, table_state) in &schema_state.tables {
                let columns = table_state
                    .columns
                    .iter()
                    .map(|column| backend::Column {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                    })
                    .collect();
                let mut table = backend::Table {
                    schema: table_state.name.schema.clone(),
                    name: table_state.name.table.clone(),
                    kind: table_state.kind.clone(),
                    columns,
                };
                if table.schema.is_empty() {
                    table.schema = schema_name.clone();
                }
                if table.name.is_empty() {
                    table.name = table_name.clone();
                }
                tables.push(table);
            }
            schemas.push(backend::Schema {
                name: schema_name.clone(),
                tables,
            });
        }
        Ok(backend::CatalogSnapshot { schemas })
    }

    /// Mirrors `Server.applyTranslationMetadataEffects`.
    fn apply_translation_metadata_effects(
        &self,
        effects: &[MetadataEffect],
    ) -> Result<(), SqlError> {
        if effects.is_empty() {
            return Ok(());
        }
        let mut state = self.lock_state();
        for effect in effects {
            match effect.kind {
                MetadataEffectKind::CreateTable => {
                    let schema_name = default_str(&effect.schema, "public");
                    if effect.table.is_empty() {
                        return Err(SqlError::new(
                            "CREATE TABLE metadata effect requires a table name",
                        ));
                    }
                    let columns = effect
                        .columns
                        .iter()
                        .map(|metadata| Column {
                            name: metadata.name.clone(),
                            data_type: metadata.data_type.clone(),
                            encoding: metadata.encoding.clone(),
                            default_value: metadata.default_value.clone(),
                            identity: metadata.identity,
                            ..Column::default()
                        })
                        .collect();
                    let schema_state = state.db.schemas.entry(schema_name.clone()).or_default();
                    schema_state.tables.insert(
                        effect.table.clone(),
                        Table {
                            name: QualifiedName {
                                schema: schema_name,
                                table: effect.table.clone(),
                            },
                            columns,
                            dist_style: effect.value.clone(),
                            dist_key: effect.name.clone(),
                            sort_keys: effect.sort_keys.clone(),
                            ..Table::default()
                        },
                    );
                }
                other => {
                    return Err(SqlError::new(format!(
                        "unsupported Redshift SQL metadata effect: {other}"
                    )));
                }
            }
        }
        self.persist_locked(&state)
    }

    /// Mirrors `persistLocked`. Storage is keyed off StoragePath exactly like
    /// legacy (empty path → no-op, which is what most tests use).
    pub(crate) fn persist_locked(&self, state: &ServerState) -> Result<(), SqlError> {
        storage::persist_state(&self.config.storage_path, state)
    }

    /// Mirrors `nextStatementIDValue`. Like legacy, this takes the lock on its own
    /// (callers re-lock afterwards to insert the record).
    pub(crate) fn next_statement_id_value(&self) -> String {
        let mut state = self.lock_state();
        state.next_statement_id += 1;
        format!("devcloud-redshift-{}", state.next_statement_id)
    }

    /// Mirrors `nextSessionIDValue`.
    pub(crate) fn next_session_id_value(&self) -> String {
        let mut state = self.lock_state();
        state.next_session_id += 1;
        format!("devcloud-redshift-session-{}", state.next_session_id)
    }

    /// Mirrors `sessionIDForRequest`: validates keep-alive, allocates a session
    /// id when needed, and upserts the session record. Returns the session id
    /// (empty string when no session is implied).
    pub(crate) fn session_id_for_request(
        &self,
        session_id: &str,
        keep_alive_seconds: i64,
        now: SystemTime,
    ) -> Result<String, SqlError> {
        if keep_alive_seconds < 0 {
            return Err(SqlError::new(
                "SessionKeepAliveSeconds must be non-negative",
            ));
        }
        let mut session_id = session_id.trim().to_string();
        if session_id.is_empty() && keep_alive_seconds <= 0 {
            return Ok(String::new());
        }
        if session_id.is_empty() {
            session_id = self.next_session_id_value();
        }
        let expires_at = if keep_alive_seconds > 0 {
            now + std::time::Duration::from_secs(keep_alive_seconds as u64)
        } else {
            now
        };
        let mut state = self.lock_state();
        if let Some(existing) = state.sessions.get_mut(&session_id) {
            existing.updated_at = now;
            existing.session_keep_alive_seconds = keep_alive_seconds;
            existing.expires_at = expires_at;
        } else {
            state.sessions.insert(
                session_id.clone(),
                SessionRecord {
                    id: session_id.clone(),
                    created_at: now,
                    updated_at: now,
                    expires_at,
                    session_keep_alive_seconds: keep_alive_seconds,
                },
            );
        }
        Ok(session_id)
    }

    /// Mirrors `validateStatementSize`.
    pub(crate) fn validate_statement_size(&self, statement: &str) -> Result<(), SqlError> {
        let max_bytes = self.config.max_statement_bytes;
        if max_bytes <= 0 {
            return Ok(());
        }
        if statement.len() as i64 > max_bytes {
            return Err(SqlError::new(format!(
                "SQL statement exceeds maxStatementBytes ({max_bytes} bytes)"
            )));
        }
        Ok(())
    }
}

/// Mirrors `compactSQLStatements`.
pub fn compact_sql_statements(statements: &[String]) -> Vec<String> {
    statements
        .iter()
        .map(|statement| statement.trim())
        .filter(|trimmed| !trimmed.is_empty())
        .map(str::to_string)
        .collect()
}

/// Mirrors `defaultString`.
pub(crate) fn default_str(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

/// Mirrors `queryResultToBackend`.
pub(crate) fn query_result_to_backend(result: QueryResult) -> backend::ExecResult {
    backend::ExecResult {
        fields: result
            .fields
            .into_iter()
            .map(|field| backend::Field {
                name: field.name,
                type_oid: field.type_oid,
                type_size: field.type_size,
            })
            .collect(),
        rows: result.rows,
        tag: result.tag,
    }
}

/// Mirrors `queryResultFromBackend`.
pub(crate) fn query_result_from_backend(result: backend::ExecResult) -> QueryResult {
    QueryResult {
        fields: result
            .fields
            .into_iter()
            .map(|field| PgField {
                name: field.name,
                type_oid: field.type_oid,
                type_size: field.type_size,
            })
            .collect(),
        rows: result.rows,
        tag: result.tag,
    }
}
