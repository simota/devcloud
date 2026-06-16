//! On-disk persistence — port of `internal/services/bigquery/storage.rs`.
//!
//! Layout under the storage root (default `.devcloud/data/bigquery`), byte
//! compatible with the legacy service so state survives switching engines:
//!
//! ```text
//! projects/<project>/datasets/<dataset>/dataset.json          (indent 2 + \n)
//! projects/<project>/datasets/<dataset>/iam-policy.json       (indent 2 + \n)
//! projects/<project>/datasets/<dataset>/tables/<t>/table.json (indent 2 + \n)
//! projects/<project>/datasets/<dataset>/tables/<t>/iam-policy.json
//! projects/<project>/datasets/<dataset>/tables/<t>/rows/streaming-buffer.jsonl
//! projects/<project>/datasets/<dataset>/routines/<r>/routine.json
//! projects/<project>/jobs/<job>.json                          (indent 2 + \n)
//! ```
//!
//! Resource files are written atomically (`<path>.tmp` + rename), exactly like
//! the legacy implementation; the streaming buffer is append-only JSONL with one
//! compact `json.Encoder`-style row per line.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::model::{
    DatasetResource, IamPolicy, QueryJobRecord, RawJson, RoutineResource, StoredRow, TableResource,
};
use crate::responses::{
    dataset_etag, default_iam_policy, normalize_iam_policy, unix_millis_string,
};
use crate::server::{now_unix_nanos, Server};
use crate::wire_json;

/// Persistence failure: any I/O or JSON error maps to the legacy handlers' 500
/// `backendError`.
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        StorageError::Json(err)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(err) => write!(f, "{err}"),
            StorageError::Json(err) => write!(f, "{err}"),
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

impl Server {
    // --- paths (legacy storage.rs path helpers) --------------------------------

    pub fn storage_root(&self) -> PathBuf {
        if self.config.storage_path.trim().is_empty() {
            Path::new(".devcloud").join("data").join("bigquery")
        } else {
            PathBuf::from(&self.config.storage_path)
        }
    }

    pub fn dataset_dir(&self, project_id: &str, dataset_id: &str) -> PathBuf {
        self.storage_root()
            .join("projects")
            .join(project_id)
            .join("datasets")
            .join(dataset_id)
    }

    pub fn dataset_path(&self, project_id: &str, dataset_id: &str) -> PathBuf {
        self.dataset_dir(project_id, dataset_id)
            .join("dataset.json")
    }

    pub fn dataset_iam_policy_path(&self, project_id: &str, dataset_id: &str) -> PathBuf {
        self.dataset_dir(project_id, dataset_id)
            .join("iam-policy.json")
    }

    pub fn table_dir(&self, project_id: &str, dataset_id: &str, table_id: &str) -> PathBuf {
        self.dataset_dir(project_id, dataset_id)
            .join("tables")
            .join(table_id)
    }

    pub fn table_path(&self, project_id: &str, dataset_id: &str, table_id: &str) -> PathBuf {
        self.table_dir(project_id, dataset_id, table_id)
            .join("table.json")
    }

    pub fn table_iam_policy_path(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> PathBuf {
        self.table_dir(project_id, dataset_id, table_id)
            .join("iam-policy.json")
    }

    pub fn routine_dir(&self, project_id: &str, dataset_id: &str, routine_id: &str) -> PathBuf {
        self.dataset_dir(project_id, dataset_id)
            .join("routines")
            .join(routine_id)
    }

    pub fn routine_path(&self, project_id: &str, dataset_id: &str, routine_id: &str) -> PathBuf {
        self.routine_dir(project_id, dataset_id, routine_id)
            .join("routine.json")
    }

    pub fn rows_path(&self, project_id: &str, dataset_id: &str, table_id: &str) -> PathBuf {
        self.table_dir(project_id, dataset_id, table_id)
            .join("rows")
            .join("streaming-buffer.jsonl")
    }

    pub fn query_job_path(&self, project_id: &str, job_id: &str) -> PathBuf {
        self.storage_root()
            .join("projects")
            .join(project_id)
            .join("jobs")
            .join(format!("{job_id}.json"))
    }

    // --- datasets -----------------------------------------------------------

    pub fn read_dataset(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> StorageResult<Option<DatasetResource>> {
        read_json_resource(&self.dataset_path(project_id, dataset_id))
    }

    pub fn read_datasets(&self, project_id: &str) -> StorageResult<Vec<DatasetResource>> {
        let root = self
            .storage_root()
            .join("projects")
            .join(project_id)
            .join("datasets");
        let mut datasets: Vec<DatasetResource> = Vec::new();
        for name in read_subdir_names(&root)? {
            if let Some(dataset) = self.read_dataset(project_id, &name)? {
                datasets.push(dataset);
            }
        }
        datasets.sort_by(|a, b| {
            a.dataset_reference
                .dataset_id
                .cmp(&b.dataset_reference.dataset_id)
        });
        Ok(datasets)
    }

    pub fn write_dataset(&self, dataset: &DatasetResource) -> StorageResult<()> {
        let path = self.dataset_path(
            &dataset.dataset_reference.project_id,
            &dataset.dataset_reference.dataset_id,
        );
        write_atomic(&path, &wire_json::to_vec_indent(dataset))
    }

    // --- IAM policies -------------------------------------------------------

    pub fn read_iam_policy(&self, path: &Path) -> StorageResult<IamPolicy> {
        let data = match fs::read(path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(default_iam_policy())
            }
            Err(err) => return Err(err.into()),
        };
        let policy: IamPolicy = serde_json::from_slice(&data)?;
        Ok(normalize_iam_policy(policy, now_unix_nanos()))
    }

    pub fn write_iam_policy(&self, path: &Path, policy: &IamPolicy) -> StorageResult<()> {
        write_atomic(path, &wire_json::to_vec_indent(policy))
    }

    // --- tables -------------------------------------------------------------

    pub fn read_table(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> StorageResult<Option<TableResource>> {
        read_json_resource(&self.table_path(project_id, dataset_id, table_id))
    }

    pub fn read_tables(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> StorageResult<Vec<TableResource>> {
        let root = self.dataset_dir(project_id, dataset_id).join("tables");
        let mut tables: Vec<TableResource> = Vec::new();
        for name in read_subdir_names(&root)? {
            if let Some(table) = self.read_table(project_id, dataset_id, &name)? {
                tables.push(table);
            }
        }
        tables.sort_by(|a, b| a.table_reference.table_id.cmp(&b.table_reference.table_id));
        Ok(tables)
    }

    pub fn write_table(&self, table: &TableResource) -> StorageResult<()> {
        let path = self.table_path(
            &table.table_reference.project_id,
            &table.table_reference.dataset_id,
            &table.table_reference.table_id,
        );
        write_atomic(&path, &wire_json::to_vec_indent(table))
    }

    // --- routines -----------------------------------------------------------

    pub fn read_routine(
        &self,
        project_id: &str,
        dataset_id: &str,
        routine_id: &str,
    ) -> StorageResult<Option<RoutineResource>> {
        read_json_resource(&self.routine_path(project_id, dataset_id, routine_id))
    }

    pub fn read_routines(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> StorageResult<Vec<RoutineResource>> {
        let root = self.dataset_dir(project_id, dataset_id).join("routines");
        let mut routines: Vec<RoutineResource> = Vec::new();
        for name in read_subdir_names(&root)? {
            if let Some(routine) = self.read_routine(project_id, dataset_id, &name)? {
                routines.push(routine);
            }
        }
        routines.sort_by(|a, b| {
            a.routine_reference
                .routine_id
                .cmp(&b.routine_reference.routine_id)
        });
        Ok(routines)
    }

    pub fn write_routine(&self, routine: &RoutineResource) -> StorageResult<()> {
        let path = self.routine_path(
            &routine.routine_reference.project_id,
            &routine.routine_reference.dataset_id,
            &routine.routine_reference.routine_id,
        );
        write_atomic(&path, &wire_json::to_vec_indent(routine))
    }

    // --- streaming buffer ---------------------------------------------------

    /// Appends rows to the streaming buffer, one compact `json.Encoder`-style
    /// line per row. Raw values are compacted on write, exactly as legacy
    /// encoder does for `json.RawMessage`.
    pub fn append_rows(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        rows: &[StoredRow],
    ) -> StorageResult<()> {
        let path = self.rows_path(project_id, dataset_id, table_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        for row in rows {
            file.write_all(&wire_json::to_vec(&compact_row(row)?))?;
        }
        Ok(())
    }

    pub fn read_rows(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> StorageResult<Vec<StoredRow>> {
        let data = match fs::read(self.rows_path(project_id, dataset_id, table_id)) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut rows = Vec::new();
        for row in serde_json::Deserializer::from_slice(&data).into_iter::<StoredRow>() {
            rows.push(row?);
        }
        Ok(rows)
    }

    // --- query jobs ---------------------------------------------------------

    pub fn write_query_job(
        &self,
        project_id: &str,
        job_id: &str,
        job: &QueryJobRecord,
    ) -> StorageResult<()> {
        write_atomic(
            &self.query_job_path(project_id, job_id),
            &wire_json::to_vec_indent(job),
        )
    }

    pub fn read_query_job(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> StorageResult<Option<QueryJobRecord>> {
        read_json_resource(&self.query_job_path(project_id, job_id))
    }

    /// legacy `readQueryJobRecords`: every `<job>.json` under the project's jobs
    /// dir, sorted by job id.
    pub fn read_query_job_records(&self, project_id: &str) -> StorageResult<Vec<QueryJobRecord>> {
        let root = self
            .storage_root()
            .join("projects")
            .join(project_id)
            .join("jobs");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut jobs: Vec<QueryJobRecord> = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_dir() || !name.ends_with(".json") {
                continue;
            }
            let job_id = name.trim_end_matches(".json");
            if let Some(job) = self.read_query_job(project_id, job_id)? {
                jobs.push(job);
            }
        }
        jobs.sort_by(|a, b| a.job.job_reference.job_id.cmp(&b.job.job_reference.job_id));
        Ok(jobs)
    }

    // --- row stats ----------------------------------------------------------

    /// legacy `refreshTableRowStats`: recomputes `numRows`/`numBytes` (bytes =
    /// `len(json.Marshal(row.JSON))` summed) and bumps etag/lastModifiedTime.
    pub fn refresh_table_row_stats(&self, table: &TableResource) -> StorageResult<()> {
        let rows = self.read_rows(
            &table.table_reference.project_id,
            &table.table_reference.dataset_id,
            &table.table_reference.table_id,
        )?;
        let mut bytes = 0usize;
        for row in &rows {
            bytes += wire_json::marshal(&row.json).len();
        }
        let now = now_unix_nanos();
        let mut table = table.clone();
        table.num_rows = rows.len().to_string();
        table.num_bytes = bytes.to_string();
        table.etag = dataset_etag(now);
        table.last_modified_time = unix_millis_string(now);
        self.write_table(&table)
    }
}

/// Reads one JSON resource file; missing file → `None` (legacy
/// `(resource, false, nil)`).
fn read_json_resource<T: serde::de::DeserializeOwned>(path: &Path) -> StorageResult<Option<T>> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(Some(serde_json::from_slice(&data)?))
}

/// Lists immediate subdirectory names. The legacy code iterates `os.ReadDir`
/// output (sorted by filename) and skips non-directories; the final ordering
/// comes from the explicit resource-id sort in each caller.
fn read_subdir_names(root: &Path) -> StorageResult<Vec<String>> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

/// legacy tmp-file + rename atomic write (`MkdirAll` + `O_CREATE|O_TRUNC` +
/// `Rename`).
fn write_atomic(path: &Path, data: &[u8]) -> StorageResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(match path.extension() {
        Some(ext) => format!("{}.tmp", ext.to_string_lossy()),
        None => "tmp".to_string(),
    });
    if let Err(err) = fs::write(&tmp, data) {
        let _ = fs::remove_file(&tmp);
        return Err(err.into());
    }
    if let Err(err) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(err.into());
    }
    Ok(())
}

/// Re-creates a row with every raw JSON value compacted, matching what legacy
/// `json.Encoder` does to `json.RawMessage` on write.
fn compact_row(row: &StoredRow) -> StorageResult<StoredRow> {
    let mut json: BTreeMap<String, RawJson> = BTreeMap::new();
    for (key, value) in &row.json {
        json.insert(
            key.clone(),
            serde_json::value::RawValue::from_string(wire_json::compact(value.get()))?,
        );
    }
    Ok(StoredRow {
        insert_id: row.insert_id.clone(),
        json,
        inserted_at: row.inserted_at.clone(),
    })
}
