//! Dashboard snapshots — port of `internal/services/bigquery/dashboard.rs`
//! (the read-only views the legacy dashboard renders) plus `readQueryJobs` /
//! `jobSnapshotFromRecord` / `rawMapForSnapshot` from `storage.rs`.
//!
//! The legacy daemon's dashboard talks to the in-process legacy server even when the
//! Rust engine owns the protocol port (both read the same on-disk state), so
//! these exist for behavior parity and the ported dashboard tests.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::model::{
    Clustering, JobResource, QueryJobRecord, RangePartitioning, RawJson, RoutineResource,
    TableResource, TableSchema, TimePartitioning, ViewDefinition,
};
use crate::server::Server;
use crate::storage::StorageResult;

/// legacy `Snapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    pub status: String,
    pub running: bool,
    pub project: String,
    pub location: String,
    #[serde(rename = "storagePath")]
    pub storage_path: String,
    pub datasets: Vec<DatasetSnapshot>,
    pub jobs: Vec<JobSnapshot>,
}

/// legacy `DatasetSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DatasetSnapshot {
    pub id: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    #[serde(rename = "datasetId")]
    pub dataset_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub location: String,
    #[serde(rename = "friendlyName", skip_serializing_if = "String::is_empty")]
    pub friendly_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub tables: Vec<TableSnapshot>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routines: Vec<RoutineResource>,
}

/// legacy `TableSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TableSnapshot {
    pub id: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    #[serde(rename = "datasetId")]
    pub dataset_id: String,
    #[serde(rename = "tableId")]
    pub table_id: String,
    #[serde(rename = "type")]
    pub table_type: String,
    #[serde(rename = "friendlyName", skip_serializing_if = "String::is_empty")]
    pub friendly_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(rename = "numRows")]
    pub num_rows: String,
    #[serde(rename = "numBytes")]
    pub num_bytes: String,
    pub schema: TableSchema,
    #[serde(rename = "timePartitioning", skip_serializing_if = "Option::is_none")]
    pub time_partitioning: Option<TimePartitioning>,
    #[serde(rename = "rangePartitioning", skip_serializing_if = "Option::is_none")]
    pub range_partitioning: Option<RangePartitioning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clustering: Option<Clustering>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<ViewDefinition>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<RowSnapshot>,
}

/// legacy `RowSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RowSnapshot {
    #[serde(rename = "insertId", skip_serializing_if = "String::is_empty")]
    pub insert_id: String,
    #[serde(rename = "insertedAt", skip_serializing_if = "String::is_empty")]
    pub inserted_at: String,
    pub json: BTreeMap<String, Value>,
}

/// legacy `JobSnapshot`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct JobSnapshot {
    #[serde(rename = "projectId")]
    pub project_id: String,
    #[serde(rename = "jobId")]
    pub job_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub location: String,
    pub state: String,
    pub job: JobResource,
}

impl Server {
    /// legacy `Snapshot`.
    pub fn snapshot(&self) -> Snapshot {
        let project_id = self.project_id().to_string();
        let mut snapshot = Snapshot {
            status: "running".to_string(),
            running: true,
            project: project_id.clone(),
            location: self.default_location().to_string(),
            storage_path: self.storage_root().to_string_lossy().into_owned(),
            datasets: Vec::new(),
            jobs: Vec::new(),
        };
        let Ok(datasets) = self.read_datasets(&project_id) else {
            return snapshot;
        };
        for dataset in datasets {
            let mut dataset_snapshot = DatasetSnapshot {
                id: dataset.id,
                project_id: dataset.dataset_reference.project_id,
                dataset_id: dataset.dataset_reference.dataset_id.clone(),
                location: dataset.location,
                friendly_name: dataset.friendly_name,
                description: dataset.description,
                tables: Vec::new(),
                routines: Vec::new(),
            };
            let dataset_id = dataset.dataset_reference.dataset_id;
            let Ok(tables) = self.read_tables(&project_id, &dataset_id) else {
                snapshot.datasets.push(dataset_snapshot);
                continue;
            };
            for table in tables {
                dataset_snapshot.tables.push(self.table_snapshot(table, 0));
            }
            if let Ok(routines) = self.read_routines(&project_id, &dataset_id) {
                dataset_snapshot.routines = routines;
            }
            snapshot.datasets.push(dataset_snapshot);
        }
        if let Ok(jobs) = self.read_query_jobs(&project_id) {
            snapshot.jobs = jobs;
        }
        snapshot
    }

    /// legacy `DatasetSnapshot`.
    pub fn dataset_snapshot(&self, project_id: &str, dataset_id: &str) -> Option<DatasetSnapshot> {
        let dataset = self.read_dataset(project_id, dataset_id).ok()??;
        let mut result = DatasetSnapshot {
            id: dataset.id,
            project_id: dataset.dataset_reference.project_id,
            dataset_id: dataset.dataset_reference.dataset_id,
            location: dataset.location,
            friendly_name: dataset.friendly_name,
            description: dataset.description,
            tables: Vec::new(),
            routines: Vec::new(),
        };
        let Ok(tables) = self.read_tables(project_id, dataset_id) else {
            return Some(result);
        };
        for table in tables {
            result.tables.push(self.table_snapshot(table, 0));
        }
        if let Ok(routines) = self.read_routines(project_id, dataset_id) {
            result.routines = routines;
        }
        Some(result)
    }

    /// legacy `TableSnapshot`.
    pub fn table_snapshot_for(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        row_limit: usize,
    ) -> Option<TableSnapshot> {
        let table = self.read_table(project_id, dataset_id, table_id).ok()??;
        Some(self.table_snapshot(table, row_limit))
    }

    /// legacy `JobSnapshot`.
    pub fn job_snapshot(&self, project_id: &str, job_id: &str) -> Option<JobSnapshot> {
        let job = self.read_query_job(project_id, job_id).ok()??;
        Some(job_snapshot_from_record(job))
    }

    /// legacy `tableSnapshot`.
    fn table_snapshot(&self, table: TableResource, row_limit: usize) -> TableSnapshot {
        let mut result = TableSnapshot {
            id: table.id,
            project_id: table.table_reference.project_id.clone(),
            dataset_id: table.table_reference.dataset_id.clone(),
            table_id: table.table_reference.table_id.clone(),
            table_type: table.table_type,
            friendly_name: table.friendly_name,
            description: table.description,
            num_rows: table.num_rows,
            num_bytes: table.num_bytes,
            schema: table.schema,
            time_partitioning: table.time_partitioning,
            range_partitioning: table.range_partitioning,
            clustering: table.clustering,
            view: table.view,
            rows: Vec::new(),
        };
        if row_limit == 0 {
            return result;
        }
        let Ok(mut rows) = self.read_rows(
            &table.table_reference.project_id,
            &table.table_reference.dataset_id,
            &table.table_reference.table_id,
        ) else {
            return result;
        };
        rows.truncate(row_limit);
        result.rows = rows
            .into_iter()
            .map(|row| RowSnapshot {
                insert_id: row.insert_id,
                inserted_at: row.inserted_at,
                json: raw_map_for_snapshot(row.json),
            })
            .collect();
        result
    }

    /// legacy `readQueryJobs` (storage.rs).
    pub(crate) fn read_query_jobs(&self, project_id: &str) -> StorageResult<Vec<JobSnapshot>> {
        let records = self.read_query_job_records(project_id)?;
        Ok(records.into_iter().map(job_snapshot_from_record).collect())
    }
}

/// legacy `jobSnapshotFromRecord` (storage.rs).
fn job_snapshot_from_record(job: QueryJobRecord) -> JobSnapshot {
    JobSnapshot {
        project_id: job.job.job_reference.project_id.clone(),
        job_id: job.job.job_reference.job_id.clone(),
        location: job.job.job_reference.location.clone(),
        state: job.job.status.state.clone(),
        job: job.job,
    }
}

/// legacy `rawMapForSnapshot` (storage.rs): decode each raw value (undecodable
/// bytes fall back to their literal text).
fn raw_map_for_snapshot(values: BTreeMap<String, RawJson>) -> BTreeMap<String, Value> {
    values
        .into_iter()
        .map(|(key, raw)| {
            let value = crate::sql_eval::raw_value_for_response(raw.get());
            (key, value)
        })
        .collect()
}
