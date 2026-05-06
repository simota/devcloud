package bigquery

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) routes() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-bigquery")
	if !s.authorize(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="devcloud-bigquery"`)
		writeError(w, http.StatusUnauthorized, "authError", "invalid authentication credentials")
		return
	}

	switch {
	case strings.HasPrefix(r.URL.EscapedPath(), "/upload/bigquery/v2/projects/"):
		s.handleUploadProjectResource(w, r)
	case r.URL.Path == "/bigquery/v2/projects":
		s.handleProjects(w, r)
	case strings.HasPrefix(r.URL.EscapedPath(), "/bigquery/v2/projects/"):
		s.handleProjectResource(w, r)
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) handleUploadProjectResource(w http.ResponseWriter, r *http.Request) {
	parts, err := uploadProjectResourceParts(r.URL.EscapedPath())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid resource path")
		return
	}
	if len(parts) == 2 && parts[1] == "jobs" && parts[0] != "" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		if err := validateResourceID(parts[0], "project"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		s.insertUploadJob(w, r, parts[0])
		return
	}
	writeError(w, http.StatusNotFound, "notFound", "not found")
}

func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	projectID := s.projectID()
	writeJSON(w, http.StatusOK, projectsListResponse{
		Kind: "bigquery#projectList",
		Projects: []projectListItem{{
			Kind:         "bigquery#project",
			ID:           projectID,
			NumericID:    "0",
			ProjectRef:   projectReference{ProjectID: projectID},
			FriendlyName: projectID,
		}},
		TotalItems: 1,
	})
}

func (s *Server) handleProjectResource(w http.ResponseWriter, r *http.Request) {
	parts, err := projectResourceParts(r.URL.EscapedPath())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid resource path")
		return
	}
	if len(parts) == 2 && parts[1] == "serviceAccount" && parts[0] != "" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		if err := validateResourceID(parts[0], "project"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, serviceAccountResponse{
			Kind:  "bigquery#getServiceAccountResponse",
			Email: "devcloud-bigquery@" + parts[0] + ".iam.gserviceaccount.com",
		})
		return
	}
	if len(parts) >= 2 && parts[1] == "queries" && parts[0] != "" {
		s.handleQueries(w, r, parts)
		return
	}
	if len(parts) >= 2 && parts[1] == "jobs" && parts[0] != "" {
		s.handleJobs(w, r, parts)
		return
	}
	if len(parts) >= 2 && parts[1] == "datasets" && parts[0] != "" {
		s.handleDatasets(w, r, parts)
		return
	}
	writeError(w, http.StatusNotFound, "notFound", "not found")
}

func (s *Server) handleQueries(w http.ResponseWriter, r *http.Request, parts []string) {
	projectID := parts[0]
	if err := validateResourceID(projectID, "project"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	switch len(parts) {
	case 2:
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		s.queryRows(w, r, projectID)
	case 3:
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		s.getQueryResults(w, r, projectID, parts[2])
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request, parts []string) {
	projectID := parts[0]
	if err := validateResourceID(projectID, "project"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	switch len(parts) {
	case 2:
		switch r.Method {
		case http.MethodGet:
			s.listJobs(w, r, projectID)
		case http.MethodPost:
			s.insertJob(w, r, projectID)
		default:
			methodNotAllowed(w, "GET, POST")
		}
	case 3:
		switch r.Method {
		case http.MethodGet:
			job, found, err := s.readQueryJob(projectID, parts[2])
			if err != nil {
				writeError(w, http.StatusInternalServerError, "backendError", "internal error")
				return
			}
			if !found {
				writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Job %s:%s", projectID, parts[2]))
				return
			}
			writeJSON(w, http.StatusOK, job.Job)
		case http.MethodDelete:
			s.deleteJobMetadata(w, r, projectID, parts[2])
		default:
			methodNotAllowed(w, "GET, DELETE")
			return
		}
	case 4:
		switch parts[3] {
		case "cancel":
			if r.Method != http.MethodPost {
				methodNotAllowed(w, "POST")
				return
			}
			s.cancelJob(w, r, projectID, parts[2])
		case "getQueryResults":
			if r.Method != http.MethodGet {
				methodNotAllowed(w, "GET")
				return
			}
			s.getQueryResults(w, r, projectID, parts[2])
		case "delete":
			if r.Method != http.MethodDelete {
				methodNotAllowed(w, "DELETE")
				return
			}
			s.deleteJobMetadata(w, r, projectID, parts[2])
		default:
			writeError(w, http.StatusNotFound, "notFound", "not found")
		}
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) handleDatasets(w http.ResponseWriter, r *http.Request, parts []string) {
	projectID := parts[0]
	if err := validateResourceID(projectID, "project"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	switch len(parts) {
	case 2:
		switch r.Method {
		case http.MethodGet:
			s.listDatasets(w, r, projectID)
		case http.MethodPost:
			s.createDataset(w, r, projectID)
		default:
			methodNotAllowed(w, "GET, POST")
		}
	case 3:
		datasetID, action := splitResourceAction(parts[2])
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if action != "" {
			s.handleDatasetIAMPolicy(w, r, projectID, datasetID, action)
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.getDataset(w, r, projectID, datasetID)
		case http.MethodPatch:
			s.patchDataset(w, r, projectID, datasetID, false)
		case http.MethodPut:
			s.patchDataset(w, r, projectID, datasetID, true)
		case http.MethodDelete:
			s.deleteDataset(w, r, projectID, datasetID)
		default:
			methodNotAllowed(w, "GET, PATCH, PUT, DELETE")
		}
	case 4:
		datasetID := parts[2]
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		switch parts[3] {
		case "tables":
			switch r.Method {
			case http.MethodGet:
				s.listTables(w, r, projectID, datasetID)
			case http.MethodPost:
				s.createTable(w, r, projectID, datasetID)
			default:
				methodNotAllowed(w, "GET, POST")
			}
		case "routines":
			switch r.Method {
			case http.MethodGet:
				s.listRoutines(w, r, projectID, datasetID)
			case http.MethodPost:
				s.createRoutine(w, r, projectID, datasetID)
			default:
				methodNotAllowed(w, "GET, POST")
			}
		default:
			writeError(w, http.StatusNotFound, "notFound", "not found")
		}
	case 5:
		datasetID := parts[2]
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		switch parts[3] {
		case "tables":
			tableID, action := splitResourceAction(parts[4])
			if err := validateResourceID(tableID, "table"); err != nil {
				writeError(w, http.StatusBadRequest, "invalid", err.Error())
				return
			}
			if action != "" {
				s.handleTableIAMPolicy(w, r, projectID, datasetID, tableID, action)
				return
			}
			switch r.Method {
			case http.MethodGet:
				s.getTable(w, r, projectID, datasetID, tableID)
			case http.MethodPatch:
				s.patchTable(w, r, projectID, datasetID, tableID, false)
			case http.MethodPut:
				s.patchTable(w, r, projectID, datasetID, tableID, true)
			case http.MethodDelete:
				s.deleteTable(w, r, projectID, datasetID, tableID)
			default:
				methodNotAllowed(w, "GET, PATCH, PUT, DELETE")
			}
		case "routines":
			routineID := parts[4]
			if err := validateResourceID(routineID, "routine"); err != nil {
				writeError(w, http.StatusBadRequest, "invalid", err.Error())
				return
			}
			switch r.Method {
			case http.MethodGet:
				s.getRoutine(w, r, projectID, datasetID, routineID)
			case http.MethodPatch:
				s.patchRoutine(w, r, projectID, datasetID, routineID, false)
			case http.MethodPut:
				s.patchRoutine(w, r, projectID, datasetID, routineID, true)
			case http.MethodDelete:
				s.deleteRoutine(w, r, projectID, datasetID, routineID)
			default:
				methodNotAllowed(w, "GET, PATCH, PUT, DELETE")
			}
		default:
			writeError(w, http.StatusNotFound, "notFound", "not found")
		}
	case 6:
		if parts[3] != "tables" {
			writeError(w, http.StatusNotFound, "notFound", "not found")
			return
		}
		datasetID := parts[2]
		tableID := parts[4]
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if err := validateResourceID(tableID, "table"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		switch parts[5] {
		case "insertAll":
			if r.Method != http.MethodPost {
				methodNotAllowed(w, "POST")
				return
			}
			s.insertRows(w, r, projectID, datasetID, tableID)
		case "data":
			if r.Method != http.MethodGet {
				methodNotAllowed(w, "GET")
				return
			}
			s.listRows(w, r, projectID, datasetID, tableID)
		default:
			writeError(w, http.StatusNotFound, "notFound", "not found")
		}
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) authorize(r *http.Request) bool {
	mode := strings.ToLower(strings.TrimSpace(s.config.AuthMode))
	switch mode {
	case "", "off", "relaxed":
		return true
	case "oauth-relaxed":
		return bearerTokenFromRequest(r) != ""
	case "bearer-dev", "strict":
		token := bearerTokenFromRequest(r)
		expected := strings.TrimSpace(s.config.BearerToken)
		if token == "" || expected == "" {
			return false
		}
		return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
	default:
		return false
	}
}

func (s *Server) projectID() string {
	projectID := strings.TrimSpace(s.config.Project)
	if projectID == "" {
		return "devcloud"
	}
	return projectID
}

func bearerTokenFromRequest(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	scheme, token, ok := strings.Cut(auth, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func projectResourceParts(escapedPath string) ([]string, error) {
	suffix := strings.TrimPrefix(escapedPath, "/bigquery/v2/projects/")
	rawParts := strings.Split(strings.Trim(suffix, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		part, err := url.PathUnescape(raw)
		if err != nil {
			return nil, err
		}
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts, nil
}

func uploadProjectResourceParts(escapedPath string) ([]string, error) {
	suffix := strings.TrimPrefix(escapedPath, "/upload/bigquery/v2/projects/")
	rawParts := strings.Split(strings.Trim(suffix, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		part, err := url.PathUnescape(raw)
		if err != nil {
			return nil, err
		}
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts, nil
}

func splitResourceAction(part string) (string, string) {
	resourceID, action, ok := strings.Cut(part, ":")
	if !ok {
		return part, ""
	}
	return resourceID, action
}

func validateResourceID(id string, kind string) error {
	if id == "" {
		return fmt.Errorf("%s id is required", kind)
	}
	for _, r := range id {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("%s id contains unsupported character", kind)
	}
	return nil
}

func validateRoutineResource(routine routineResource) error {
	if err := validateResourceID(routine.RoutineReference.RoutineID, "routine"); err != nil {
		return err
	}
	if strings.TrimSpace(routine.RoutineType) == "" {
		return fmt.Errorf("routineType is required")
	}
	switch strings.ToUpper(routine.RoutineType) {
	case "SCALAR_FUNCTION", "PROCEDURE", "TABLE_VALUED_FUNCTION":
	default:
		return fmt.Errorf("unsupported routineType %q", routine.RoutineType)
	}
	return nil
}

func (s *Server) decodeDatasetRequest(body io.Reader) (datasetResource, error) {
	var request datasetResource
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return datasetResource{}, err
	}
	return request, nil
}

func (s *Server) decodeTableRequest(body io.Reader) (tableResource, error) {
	var request tableResource
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return tableResource{}, err
	}
	return request, nil
}

func (s *Server) decodeRoutineRequest(body io.Reader) (routineResource, error) {
	var request routineResource
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return routineResource{}, err
	}
	return request, nil
}

func (s *Server) decodeInsertAllRequest(body io.Reader) (insertAllRequest, error) {
	var request insertAllRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return insertAllRequest{}, err
	}
	return request, nil
}

func (s *Server) decodeQueryRequest(body io.Reader) (queryRequest, error) {
	var request queryRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return queryRequest{}, err
	}
	return request, nil
}

func (s *Server) decodeJobInsertRequest(body io.Reader) (jobInsertRequest, error) {
	var request jobInsertRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return jobInsertRequest{}, err
	}
	return request, nil
}

func (s *Server) decodeSetIAMPolicyRequest(body io.Reader) (setIAMPolicyRequest, error) {
	var request setIAMPolicyRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return setIAMPolicyRequest{}, err
	}
	return request, nil
}

func (s *Server) decodeTestIAMPermissionsRequest(body io.Reader) (testIAMPermissionsRequest, error) {
	var request testIAMPermissionsRequest
	decoder := json.NewDecoder(http.MaxBytesReader(nil, io.NopCloser(body), s.maxRequestBytes()))
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return testIAMPermissionsRequest{}, err
	}
	return request, nil
}

func (s *Server) decodeMultipartJobInsertRequest(w http.ResponseWriter, r *http.Request) (jobInsertRequest, []byte, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") || params["boundary"] == "" {
		return jobInsertRequest{}, nil, fmt.Errorf("multipart content type is required")
	}
	reader := multipart.NewReader(http.MaxBytesReader(w, r.Body, s.maxRequestBytes()), params["boundary"])
	metadataPart, err := reader.NextPart()
	if err != nil {
		return jobInsertRequest{}, nil, err
	}
	defer metadataPart.Close()
	var request jobInsertRequest
	if err := json.NewDecoder(metadataPart).Decode(&request); err != nil {
		return jobInsertRequest{}, nil, err
	}

	mediaPart, err := reader.NextPart()
	if err != nil {
		return jobInsertRequest{}, nil, err
	}
	defer mediaPart.Close()
	media, err := io.ReadAll(mediaPart)
	if err != nil {
		return jobInsertRequest{}, nil, err
	}
	return request, media, nil
}
