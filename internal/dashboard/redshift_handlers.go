package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (s *Server) handleRedshiftStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	region := defaultString(s.config.RedshiftRegion, "us-east-1")
	clusterCount := 0
	backendKind := "postgres"
	backendMode := "managed"
	if s.redshift != nil {
		snapshot := s.redshift.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		region = snapshot.Region
		clusterCount = len(snapshot.Clusters)
		backendKind = defaultString(snapshot.BackendKind, backendKind)
		backendMode = defaultString(snapshot.BackendMode, backendMode)
	}
	writeJSON(w, map[string]any{
		"service":      "redshift",
		"status":       status,
		"running":      running,
		"sqlEndpoint":  defaultString(s.config.RedshiftSQLEndpoint, "127.0.0.1:5439"),
		"apiEndpoint":  defaultString(s.config.RedshiftAPIEndpoint, "http://127.0.0.1:9099"),
		"region":       region,
		"clusterCount": clusterCount,
		"storagePath":  defaultString(s.config.RedshiftStoragePath, ".devcloud/data/redshift"),
		"backendKind":  backendKind,
		"backendMode":  backendMode,
	})
}

func (s *Server) handleRedshiftClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.redshift.Snapshot()
	writeJSON(w, map[string]any{
		"clusters": snapshot.Clusters,
	})
}

func (s *Server) handleRedshiftCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{
		"catalog": s.redshift.CatalogSnapshot(),
	})
}

func (s *Server) handleRedshiftStatements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{
		"statements": s.redshift.StatementSnapshots(),
	})
}

func (s *Server) handleRedshiftTable(w http.ResponseWriter, r *http.Request) {
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/redshift/tables/")
	if err != nil || len(parts) != 2 {
		http.Error(w, "invalid redshift table path", http.StatusBadRequest)
		return
	}
	limit, ok := positiveLimitFromRequest(w, r, 100)
	if !ok {
		return
	}
	detail, found := s.redshift.TableDetailSnapshot(parts[0], parts[1], limit)
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"schema":  parts[0],
		"table":   parts[1],
		"detail":  detail,
		"columns": detail.Columns,
		"rows":    detail.Rows,
	})
}

type redshiftDashboardQueryRequest struct {
	SQL     string `json:"sql"`
	MaxRows int    `json:"maxRows"`
}

func (s *Server) handleRedshiftQuery(w http.ResponseWriter, r *http.Request) {
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request redshiftDashboardQueryRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid redshift query request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.SQL) == "" {
		http.Error(w, "sql is required", http.StatusBadRequest)
		return
	}
	result, err := s.redshift.ExecuteDashboardSQL(request.SQL, request.MaxRows)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]any{
			"error": "redshift query failed",
		})
		return
	}
	writeJSON(w, map[string]any{
		"result": result,
	})
}
