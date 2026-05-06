package bigquery

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *Server) datasetDir(projectID string, datasetID string) string {
	return filepath.Join(s.storageRoot(), "projects", projectID, "datasets", datasetID)
}

func (s *Server) datasetPath(projectID string, datasetID string) string {
	return filepath.Join(s.datasetDir(projectID, datasetID), "dataset.json")
}

func (s *Server) datasetIAMPolicyPath(projectID string, datasetID string) string {
	return filepath.Join(s.datasetDir(projectID, datasetID), "iam-policy.json")
}

func (s *Server) tableDir(projectID string, datasetID string, tableID string) string {
	return filepath.Join(s.datasetDir(projectID, datasetID), "tables", tableID)
}

func (s *Server) tablePath(projectID string, datasetID string, tableID string) string {
	return filepath.Join(s.tableDir(projectID, datasetID, tableID), "table.json")
}

func (s *Server) tableIAMPolicyPath(projectID string, datasetID string, tableID string) string {
	return filepath.Join(s.tableDir(projectID, datasetID, tableID), "iam-policy.json")
}

func (s *Server) routineDir(projectID string, datasetID string, routineID string) string {
	return filepath.Join(s.datasetDir(projectID, datasetID), "routines", routineID)
}

func (s *Server) routinePath(projectID string, datasetID string, routineID string) string {
	return filepath.Join(s.routineDir(projectID, datasetID, routineID), "routine.json")
}

func (s *Server) rowsPath(projectID string, datasetID string, tableID string) string {
	return filepath.Join(s.tableDir(projectID, datasetID, tableID), "rows", "streaming-buffer.jsonl")
}

func (s *Server) queryJobPath(projectID string, jobID string) string {
	return filepath.Join(s.storageRoot(), "projects", projectID, "jobs", jobID+".json")
}

func (s *Server) storageRoot() string {
	if strings.TrimSpace(s.config.StoragePath) == "" {
		return filepath.Join(".devcloud", "data", "bigquery")
	}
	return s.config.StoragePath
}

func (s *Server) defaultLocation() string {
	if strings.TrimSpace(s.config.Location) == "" {
		return "US"
	}
	return s.config.Location
}

func (s *Server) maxRequestBytes() int64 {
	if s.config.MaxRequestBytes <= 0 {
		return 10 * 1024 * 1024
	}
	return s.config.MaxRequestBytes
}

func (s *Server) maxRowsPerTable() int64 {
	if s.config.MaxRowsPerTable <= 0 {
		return 1000000
	}
	return s.config.MaxRowsPerTable
}

func (s *Server) maxResultRows() int {
	if s.config.MaxResultRows <= 0 {
		return 10000
	}
	return s.config.MaxResultRows
}

func (s *Server) effectiveUseLegacySQL(value *bool) bool {
	if value == nil {
		return s.config.DefaultLegacySQL
	}
	return *value
}

func (s *Server) readDataset(projectID string, datasetID string) (datasetResource, bool, error) {
	f, err := os.Open(s.datasetPath(projectID, datasetID))
	if errors.Is(err, os.ErrNotExist) {
		return datasetResource{}, false, nil
	}
	if err != nil {
		return datasetResource{}, false, err
	}
	defer f.Close()

	var dataset datasetResource
	if err := json.NewDecoder(f).Decode(&dataset); err != nil {
		return datasetResource{}, false, err
	}
	return dataset, true, nil
}

func (s *Server) readDatasets(projectID string) ([]datasetResource, error) {
	root := filepath.Join(s.storageRoot(), "projects", projectID, "datasets")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	datasets := make([]datasetResource, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dataset, found, err := s.readDataset(projectID, entry.Name())
		if err != nil {
			return nil, err
		}
		if found {
			datasets = append(datasets, dataset)
		}
	}
	sort.Slice(datasets, func(i, j int) bool {
		return datasets[i].DatasetReference.DatasetID < datasets[j].DatasetReference.DatasetID
	})
	return datasets, nil
}

func (s *Server) writeDataset(dataset datasetResource) error {
	path := s.datasetPath(dataset.DatasetReference.ProjectID, dataset.DatasetReference.DatasetID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(dataset); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) readIAMPolicy(path string) (iamPolicy, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return defaultIAMPolicy(), nil
	}
	if err != nil {
		return iamPolicy{}, err
	}
	defer f.Close()

	var policy iamPolicy
	if err := json.NewDecoder(f).Decode(&policy); err != nil {
		return iamPolicy{}, err
	}
	return normalizeIAMPolicy(policy), nil
}

func (s *Server) writeIAMPolicy(path string, policy iamPolicy) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(policy); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) readTable(projectID string, datasetID string, tableID string) (tableResource, bool, error) {
	f, err := os.Open(s.tablePath(projectID, datasetID, tableID))
	if errors.Is(err, os.ErrNotExist) {
		return tableResource{}, false, nil
	}
	if err != nil {
		return tableResource{}, false, err
	}
	defer f.Close()

	var table tableResource
	if err := json.NewDecoder(f).Decode(&table); err != nil {
		return tableResource{}, false, err
	}
	return table, true, nil
}

func (s *Server) readTables(projectID string, datasetID string) ([]tableResource, error) {
	root := filepath.Join(s.datasetDir(projectID, datasetID), "tables")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tables := make([]tableResource, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		table, found, err := s.readTable(projectID, datasetID, entry.Name())
		if err != nil {
			return nil, err
		}
		if found {
			tables = append(tables, table)
		}
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableReference.TableID < tables[j].TableReference.TableID
	})
	return tables, nil
}

func (s *Server) writeTable(table tableResource) error {
	path := s.tablePath(table.TableReference.ProjectID, table.TableReference.DatasetID, table.TableReference.TableID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(table); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) readRoutine(projectID string, datasetID string, routineID string) (routineResource, bool, error) {
	f, err := os.Open(s.routinePath(projectID, datasetID, routineID))
	if errors.Is(err, os.ErrNotExist) {
		return routineResource{}, false, nil
	}
	if err != nil {
		return routineResource{}, false, err
	}
	defer f.Close()

	var routine routineResource
	if err := json.NewDecoder(f).Decode(&routine); err != nil {
		return routineResource{}, false, err
	}
	return routine, true, nil
}

func (s *Server) readRoutines(projectID string, datasetID string) ([]routineResource, error) {
	root := filepath.Join(s.datasetDir(projectID, datasetID), "routines")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	routines := make([]routineResource, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		routine, found, err := s.readRoutine(projectID, datasetID, entry.Name())
		if err != nil {
			return nil, err
		}
		if found {
			routines = append(routines, routine)
		}
	}
	sort.Slice(routines, func(i, j int) bool {
		return routines[i].RoutineReference.RoutineID < routines[j].RoutineReference.RoutineID
	})
	return routines, nil
}

func (s *Server) writeRoutine(routine routineResource) error {
	path := s.routinePath(routine.RoutineReference.ProjectID, routine.RoutineReference.DatasetID, routine.RoutineReference.RoutineID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(routine); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) appendRows(projectID string, datasetID string, tableID string, rows []storedRow) error {
	path := s.rowsPath(projectID, datasetID, tableID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

func (s *Server) readRows(projectID string, datasetID string, tableID string) ([]storedRow, error) {
	f, err := os.Open(s.rowsPath(projectID, datasetID, tableID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	var rows []storedRow
	for {
		var row storedRow
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *Server) writeQueryJob(projectID string, jobID string, job queryJobRecord) error {
	path := s.queryJobPath(projectID, jobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(job); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) readQueryJob(projectID string, jobID string) (queryJobRecord, bool, error) {
	f, err := os.Open(s.queryJobPath(projectID, jobID))
	if errors.Is(err, os.ErrNotExist) {
		return queryJobRecord{}, false, nil
	}
	if err != nil {
		return queryJobRecord{}, false, err
	}
	defer f.Close()

	var job queryJobRecord
	if err := json.NewDecoder(f).Decode(&job); err != nil {
		return queryJobRecord{}, false, err
	}
	return job, true, nil
}

func (s *Server) readQueryJobs(projectID string) ([]JobSnapshot, error) {
	records, err := s.readQueryJobRecords(projectID)
	if err != nil {
		return nil, err
	}
	jobs := make([]JobSnapshot, 0, len(records))
	for _, job := range records {
		jobs = append(jobs, jobSnapshotFromRecord(job))
	}
	return jobs, nil
}

func (s *Server) readQueryJobRecords(projectID string) ([]queryJobRecord, error) {
	root := filepath.Join(s.storageRoot(), "projects", projectID, "jobs")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	jobs := make([]queryJobRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		jobID := strings.TrimSuffix(entry.Name(), ".json")
		job, found, err := s.readQueryJob(projectID, jobID)
		if err != nil {
			return nil, err
		}
		if found {
			jobs = append(jobs, job)
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Job.JobReference.JobID < jobs[j].Job.JobReference.JobID
	})
	return jobs, nil
}

func jobSnapshotFromRecord(job queryJobRecord) JobSnapshot {
	return JobSnapshot{
		ProjectID: job.Job.JobReference.ProjectID,
		JobID:     job.Job.JobReference.JobID,
		Location:  job.Job.JobReference.Location,
		State:     job.Job.Status.State,
		Job:       job.Job,
	}
}

func rawMapForSnapshot(values map[string]json.RawMessage) map[string]any {
	result := make(map[string]any, len(values))
	for key, raw := range values {
		var value any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			result[key] = string(raw)
			continue
		}
		result[key] = value
	}
	return result
}

func (s *Server) refreshTableRowStats(table tableResource) error {
	rows, err := s.readRows(table.TableReference.ProjectID, table.TableReference.DatasetID, table.TableReference.TableID)
	if err != nil {
		return err
	}
	var bytes int
	for _, row := range rows {
		data, err := json.Marshal(row.JSON)
		if err != nil {
			return err
		}
		bytes += len(data)
	}
	now := time.Now().UTC()
	table.NumRows = strconv.Itoa(len(rows))
	table.NumBytes = strconv.Itoa(bytes)
	table.ETag = datasetETag(now)
	table.LastModifiedTime = unixMillisString(now)
	return s.writeTable(table)
}
