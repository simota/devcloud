package dashboard

import (
	"net/http"
	"net/url"
)

func (s *Server) handleBigQueryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	project := defaultString(s.config.BigQueryProject, "devcloud")
	location := defaultString(s.config.BigQueryLocation, "US")
	datasetCount := 0
	jobCount := 0
	if s.bq != nil {
		snapshot := s.bq.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		project = snapshot.Project
		location = snapshot.Location
		datasetCount = len(snapshot.Datasets)
		jobCount = len(snapshot.Jobs)
	}
	writeJSON(w, map[string]any{
		"service":      "bigquery",
		"status":       status,
		"running":      running,
		"endpoint":     defaultString(s.config.BigQueryEndpoint, "http://127.0.0.1:9050"),
		"project":      project,
		"location":     location,
		"authMode":     defaultString(s.config.BigQueryAuthMode, "relaxed"),
		"storagePath":  defaultString(s.config.BigQueryStoragePath, ".devcloud/data/bigquery"),
		"datasetCount": datasetCount,
		"jobCount":     jobCount,
	})
}

func (s *Server) handleBigQueryProjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.bq == nil {
		http.Error(w, "bigquery service is disabled", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.bq.Snapshot()
	writeJSON(w, map[string]any{
		"projects": []map[string]any{{
			"projectId":    snapshot.Project,
			"location":     snapshot.Location,
			"datasetCount": len(snapshot.Datasets),
			"jobCount":     len(snapshot.Jobs),
			"datasets":     snapshot.Datasets,
			"jobs":         snapshot.Jobs,
		}},
	})
}

func (s *Server) handleBigQueryProjectResource(w http.ResponseWriter, r *http.Request) {
	if s.bq == nil {
		http.Error(w, "bigquery service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/bigquery/projects/")
	if err != nil || len(parts) == 0 {
		http.Error(w, "invalid bigquery path", http.StatusBadRequest)
		return
	}
	projectID := parts[0]
	if len(parts) == 2 && parts[1] == "queries" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		s.forwardBigQueryRequest(w, r, "/bigquery/v2/projects/"+url.PathEscape(projectID)+"/queries")
		return
	}
	if len(parts) == 2 && parts[1] == "jobs" && r.Method == http.MethodPost {
		s.forwardBigQueryRequest(w, r, "/bigquery/v2/projects/"+url.PathEscape(projectID)+"/jobs")
		return
	}
	if len(parts) == 2 && parts[1] == "datasets" && r.Method == http.MethodPost {
		s.forwardBigQueryRequest(w, r, "/bigquery/v2/projects/"+url.PathEscape(projectID)+"/datasets")
		return
	}
	if len(parts) == 4 && parts[1] == "datasets" && parts[3] == "tables" && r.Method == http.MethodPost {
		s.forwardBigQueryRequest(w, r, "/bigquery/v2/projects/"+url.PathEscape(projectID)+"/datasets/"+url.PathEscape(parts[2])+"/tables")
		return
	}
	if len(parts) == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "insertAll" && r.Method == http.MethodPost {
		s.forwardBigQueryRequest(w, r, "/bigquery/v2/projects/"+url.PathEscape(projectID)+"/datasets/"+url.PathEscape(parts[2])+"/tables/"+url.PathEscape(parts[4])+"/insertAll")
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	switch {
	case len(parts) == 2 && parts[1] == "datasets":
		snapshot := s.bq.Snapshot()
		if snapshot.Project != projectID {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasets": snapshot.Datasets})
	case len(parts) == 4 && parts[1] == "datasets" && parts[3] == "tables":
		dataset, found := s.bq.DatasetSnapshot(projectID, parts[2])
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tables": dataset.Tables})
	case len(parts) == 5 && parts[1] == "datasets" && parts[3] == "tables":
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], 0)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "table": table})
	case len(parts) == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "schema":
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], 0)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "schema": table.Schema})
	case len(parts) == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "rows":
		limit, ok := positiveLimitFromRequest(w, r, 100)
		if !ok {
			return
		}
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], limit)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "rows": table.Rows})
	case len(parts) == 2 && parts[1] == "jobs":
		snapshot := s.bq.Snapshot()
		if snapshot.Project != projectID {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "jobs": snapshot.Jobs})
	case len(parts) == 3 && parts[1] == "jobs":
		job, found := s.bq.JobSnapshot(projectID, parts[2])
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "jobId": parts[2], "job": job})
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) forwardBigQueryRequest(w http.ResponseWriter, r *http.Request, path string) {
	forwardURL := &url.URL{
		Path:     path,
		RawQuery: r.URL.RawQuery,
	}
	req := r.Clone(r.Context())
	req.URL = forwardURL
	req.RequestURI = ""
	req.Body = r.Body
	req.Header = r.Header.Clone()
	s.bq.ServeHTTP(w, req)
}
