package bigquery

import "net/url"

type Snapshot struct {
	Status      string            `json:"status"`
	Running     bool              `json:"running"`
	Project     string            `json:"project"`
	Location    string            `json:"location"`
	StoragePath string            `json:"storagePath"`
	Datasets    []DatasetSnapshot `json:"datasets"`
	Jobs        []JobSnapshot     `json:"jobs"`
}

type DatasetSnapshot struct {
	ID           string            `json:"id"`
	ProjectID    string            `json:"projectId"`
	DatasetID    string            `json:"datasetId"`
	Location     string            `json:"location,omitempty"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Description  string            `json:"description,omitempty"`
	Tables       []TableSnapshot   `json:"tables"`
	Routines     []routineResource `json:"routines,omitempty"`
}

type TableSnapshot struct {
	ID                string             `json:"id"`
	ProjectID         string             `json:"projectId"`
	DatasetID         string             `json:"datasetId"`
	TableID           string             `json:"tableId"`
	Type              string             `json:"type"`
	FriendlyName      string             `json:"friendlyName,omitempty"`
	Description       string             `json:"description,omitempty"`
	NumRows           string             `json:"numRows"`
	NumBytes          string             `json:"numBytes"`
	Schema            tableSchema        `json:"schema"`
	TimePartitioning  *timePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *rangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *clustering        `json:"clustering,omitempty"`
	View              *viewDefinition    `json:"view,omitempty"`
	Rows              []RowSnapshot      `json:"rows,omitempty"`
}

type RowSnapshot struct {
	InsertID   string         `json:"insertId,omitempty"`
	InsertedAt string         `json:"insertedAt,omitempty"`
	JSON       map[string]any `json:"json"`
}

type JobSnapshot struct {
	ProjectID string      `json:"projectId"`
	JobID     string      `json:"jobId"`
	Location  string      `json:"location,omitempty"`
	State     string      `json:"state"`
	Job       jobResource `json:"job"`
}

func (s *Server) Snapshot() Snapshot {
	projectID := s.projectID()
	snapshot := Snapshot{
		Status:      "running",
		Running:     true,
		Project:     projectID,
		Location:    s.defaultLocation(),
		StoragePath: s.storageRoot(),
		Datasets:    []DatasetSnapshot{},
		Jobs:        []JobSnapshot{},
	}
	datasets, err := s.readDatasets(projectID)
	if err != nil {
		return snapshot
	}
	for _, dataset := range datasets {
		datasetSnapshot := DatasetSnapshot{
			ID:           dataset.ID,
			ProjectID:    dataset.DatasetReference.ProjectID,
			DatasetID:    dataset.DatasetReference.DatasetID,
			Location:     dataset.Location,
			FriendlyName: dataset.FriendlyName,
			Description:  dataset.Description,
			Tables:       []TableSnapshot{},
		}
		tables, err := s.readTables(projectID, dataset.DatasetReference.DatasetID)
		if err != nil {
			snapshot.Datasets = append(snapshot.Datasets, datasetSnapshot)
			continue
		}
		for _, table := range tables {
			datasetSnapshot.Tables = append(datasetSnapshot.Tables, s.tableSnapshot(table, 0))
		}
		if routines, err := s.readRoutines(projectID, dataset.DatasetReference.DatasetID); err == nil {
			datasetSnapshot.Routines = routines
		}
		snapshot.Datasets = append(snapshot.Datasets, datasetSnapshot)
	}
	jobs, err := s.readQueryJobs(projectID)
	if err == nil {
		snapshot.Jobs = jobs
	}
	return snapshot
}

func (s *Server) DatasetSnapshot(projectID string, datasetID string) (DatasetSnapshot, bool) {
	dataset, found, err := s.readDataset(projectID, datasetID)
	if err != nil || !found {
		return DatasetSnapshot{}, false
	}
	result := DatasetSnapshot{
		ID:           dataset.ID,
		ProjectID:    dataset.DatasetReference.ProjectID,
		DatasetID:    dataset.DatasetReference.DatasetID,
		Location:     dataset.Location,
		FriendlyName: dataset.FriendlyName,
		Description:  dataset.Description,
		Tables:       []TableSnapshot{},
	}
	tables, err := s.readTables(projectID, datasetID)
	if err != nil {
		return result, true
	}
	for _, table := range tables {
		result.Tables = append(result.Tables, s.tableSnapshot(table, 0))
	}
	if routines, err := s.readRoutines(projectID, datasetID); err == nil {
		result.Routines = routines
	}
	return result, true
}

func (s *Server) TableSnapshot(projectID string, datasetID string, tableID string, rowLimit int) (TableSnapshot, bool) {
	table, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil || !found {
		return TableSnapshot{}, false
	}
	return s.tableSnapshot(table, rowLimit), true
}

func (s *Server) JobSnapshot(projectID string, jobID string) (JobSnapshot, bool) {
	job, found, err := s.readQueryJob(projectID, jobID)
	if err != nil || !found {
		return JobSnapshot{}, false
	}
	return jobSnapshotFromRecord(job), true
}

func (s *Server) tableSnapshot(table tableResource, rowLimit int) TableSnapshot {
	result := TableSnapshot{
		ID:                table.ID,
		ProjectID:         table.TableReference.ProjectID,
		DatasetID:         table.TableReference.DatasetID,
		TableID:           table.TableReference.TableID,
		Type:              table.Type,
		FriendlyName:      table.FriendlyName,
		Description:       table.Description,
		NumRows:           table.NumRows,
		NumBytes:          table.NumBytes,
		Schema:            table.Schema,
		TimePartitioning:  table.TimePartitioning,
		RangePartitioning: table.RangePartitioning,
		Clustering:        table.Clustering,
		View:              table.View,
	}
	if rowLimit <= 0 {
		return result
	}
	rows, err := s.readRows(table.TableReference.ProjectID, table.TableReference.DatasetID, table.TableReference.TableID)
	if err != nil {
		return result
	}
	if rowLimit < len(rows) {
		rows = rows[:rowLimit]
	}
	result.Rows = make([]RowSnapshot, 0, len(rows))
	for _, row := range rows {
		result.Rows = append(result.Rows, RowSnapshot{
			InsertID:   row.InsertID,
			InsertedAt: row.InsertedAt,
			JSON:       rawMapForSnapshot(row.JSON),
		})
	}
	return result
}

func (s *Server) datasetSelfLink(projectID string, datasetID string) string {
	return "/bigquery/v2/projects/" + url.PathEscape(projectID) + "/datasets/" + url.PathEscape(datasetID)
}

func (s *Server) tableSelfLink(projectID string, datasetID string, tableID string) string {
	return s.datasetSelfLink(projectID, datasetID) + "/tables/" + url.PathEscape(tableID)
}

func (s *Server) routineSelfLink(projectID string, datasetID string, routineID string) string {
	return s.datasetSelfLink(projectID, datasetID) + "/routines/" + url.PathEscape(routineID)
}
