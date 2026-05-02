package bigquery

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	s3svc "devcloud/internal/services/s3"
)

type Config struct {
	Addr             string
	Project          string
	Location         string
	AuthMode         string
	BearerToken      string
	StoragePath      string
	MaxRowsPerTable  int64
	MaxRequestBytes  int64
	MaxResultRows    int
	DefaultLegacySQL bool
	ObjectStore      s3svc.BucketStore
}

type Server struct {
	config Config
}

func NewServer(cfg Config) *Server {
	return &Server{config: cfg}
}

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
		if parts[3] != "tables" {
			writeError(w, http.StatusNotFound, "notFound", "not found")
			return
		}
		datasetID := parts[2]
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.listTables(w, r, projectID, datasetID)
		case http.MethodPost:
			s.createTable(w, r, projectID, datasetID)
		default:
			methodNotAllowed(w, "GET, POST")
		}
	case 5:
		if parts[3] != "tables" {
			writeError(w, http.StatusNotFound, "notFound", "not found")
			return
		}
		datasetID := parts[2]
		tableID, action := splitResourceAction(parts[4])
		if err := validateResourceID(datasetID, "dataset"); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
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

func (s *Server) createDataset(w http.ResponseWriter, r *http.Request, projectID string) {
	request, err := s.decodeDatasetRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.DatasetReference.ProjectID == "" {
		request.DatasetReference.ProjectID = projectID
	}
	if request.DatasetReference.ProjectID != projectID {
		writeError(w, http.StatusBadRequest, "invalid", "datasetReference.projectId must match request project")
		return
	}
	if err := validateResourceID(request.DatasetReference.DatasetID, "dataset"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	path := s.datasetPath(projectID, request.DatasetReference.DatasetID)
	if _, err := os.Stat(path); err == nil {
		writeError(w, http.StatusConflict, "duplicate", fmt.Sprintf("Already Exists: Dataset %s:%s", projectID, request.DatasetReference.DatasetID))
		return
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}

	now := time.Now().UTC()
	resource := datasetResource{
		Kind:             "bigquery#dataset",
		ID:               projectID + ":" + request.DatasetReference.DatasetID,
		SelfLink:         s.datasetSelfLink(projectID, request.DatasetReference.DatasetID),
		DatasetReference: request.DatasetReference,
		Location:         defaultString(request.Location, s.defaultLocation()),
		FriendlyName:     request.FriendlyName,
		Description:      request.Description,
		Labels:           request.Labels,
		ETag:             datasetETag(now),
		CreationTime:     unixMillisString(now),
		LastModifiedTime: unixMillisString(now),
	}
	if err := s.writeDataset(resource); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	w.Header().Set("Location", resource.SelfLink)
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) getDataset(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	resource, found, err := s.readDataset(projectID, datasetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request, projectID string) {
	datasets, err := s.readDatasets(projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if offset > len(datasets) {
		offset = len(datasets)
	}
	end := offset + maxResults
	if end > len(datasets) {
		end = len(datasets)
	}
	items := make([]datasetListItem, 0, end-offset)
	for _, dataset := range datasets[offset:end] {
		items = append(items, datasetListItem{
			Kind:             "bigquery#dataset",
			ID:               dataset.ID,
			DatasetReference: dataset.DatasetReference,
			Location:         dataset.Location,
			FriendlyName:     dataset.FriendlyName,
		})
	}
	response := datasetsListResponse{
		Kind:       "bigquery#datasetList",
		Datasets:   items,
		TotalItems: len(datasets),
	}
	if end < len(datasets) {
		response.NextPageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) patchDataset(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, replace bool) {
	existing, found, err := s.readDataset(projectID, datasetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found && !replace {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}

	request, err := s.decodeDatasetRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.DatasetReference.ProjectID != "" && request.DatasetReference.ProjectID != projectID {
		writeError(w, http.StatusBadRequest, "invalid", "datasetReference.projectId must match request project")
		return
	}
	if request.DatasetReference.DatasetID != "" && request.DatasetReference.DatasetID != datasetID {
		writeError(w, http.StatusBadRequest, "invalid", "datasetReference.datasetId must match request dataset")
		return
	}

	now := time.Now().UTC()
	if !found {
		existing = datasetResource{
			Kind:             "bigquery#dataset",
			ID:               projectID + ":" + datasetID,
			SelfLink:         s.datasetSelfLink(projectID, datasetID),
			DatasetReference: datasetReference{ProjectID: projectID, DatasetID: datasetID},
			Location:         s.defaultLocation(),
			CreationTime:     unixMillisString(now),
		}
	}
	if replace {
		existing.FriendlyName = request.FriendlyName
		existing.Description = request.Description
		existing.Labels = request.Labels
		existing.Location = defaultString(request.Location, s.defaultLocation())
	} else {
		if request.FriendlyName != "" {
			existing.FriendlyName = request.FriendlyName
		}
		if request.Description != "" {
			existing.Description = request.Description
		}
		if request.Labels != nil {
			existing.Labels = request.Labels
		}
		if request.Location != "" {
			existing.Location = request.Location
		}
	}
	existing.ETag = datasetETag(now)
	existing.LastModifiedTime = unixMillisString(now)
	if err := s.writeDataset(existing); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) deleteDataset(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	dir := s.datasetDir(projectID, datasetID)
	if _, err := os.Stat(filepath.Join(dir, "dataset.json")); errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if r.URL.Query().Get("deleteContents") != "true" {
		if hasChildren(filepath.Join(dir, "tables")) {
			writeError(w, http.StatusConflict, "duplicate", "dataset is not empty")
			return
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createTable(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	if _, found, err := s.readDataset(projectID, datasetID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}

	request, err := s.decodeTableRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.TableReference.ProjectID == "" {
		request.TableReference.ProjectID = projectID
	}
	if request.TableReference.DatasetID == "" {
		request.TableReference.DatasetID = datasetID
	}
	if request.TableReference.ProjectID != projectID || request.TableReference.DatasetID != datasetID {
		writeError(w, http.StatusBadRequest, "invalid", "tableReference must match request project and dataset")
		return
	}
	if err := validateResourceID(request.TableReference.TableID, "table"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if err := validateTableSchema(request.Schema); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	path := s.tablePath(projectID, datasetID, request.TableReference.TableID)
	if _, err := os.Stat(path); err == nil {
		writeError(w, http.StatusConflict, "duplicate", fmt.Sprintf("Already Exists: Table %s:%s.%s", projectID, datasetID, request.TableReference.TableID))
		return
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}

	now := time.Now().UTC()
	resource := tableResource{
		Kind:              "bigquery#table",
		ID:                projectID + ":" + datasetID + "." + request.TableReference.TableID,
		SelfLink:          s.tableSelfLink(projectID, datasetID, request.TableReference.TableID),
		TableReference:    request.TableReference,
		Type:              defaultString(request.Type, "TABLE"),
		Schema:            request.Schema,
		FriendlyName:      request.FriendlyName,
		Description:       request.Description,
		Labels:            request.Labels,
		TimePartitioning:  request.TimePartitioning,
		RangePartitioning: request.RangePartitioning,
		Clustering:        request.Clustering,
		ETag:              datasetETag(now),
		CreationTime:      unixMillisString(now),
		LastModifiedTime:  unixMillisString(now),
		NumRows:           "0",
		NumBytes:          "0",
		Location:          s.defaultLocation(),
	}
	if err := s.writeTable(resource); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	w.Header().Set("Location", resource.SelfLink)
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) getTable(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	resource, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	if _, found, err := s.readDataset(projectID, datasetID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}
	tables, err := s.readTables(projectID, datasetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if offset > len(tables) {
		offset = len(tables)
	}
	end := offset + maxResults
	if end > len(tables) {
		end = len(tables)
	}
	items := make([]tableListItem, 0, end-offset)
	for _, table := range tables[offset:end] {
		items = append(items, tableListItem{
			Kind:              "bigquery#table",
			ID:                table.ID,
			TableReference:    table.TableReference,
			Type:              table.Type,
			FriendlyName:      table.FriendlyName,
			TimePartitioning:  table.TimePartitioning,
			RangePartitioning: table.RangePartitioning,
			Clustering:        table.Clustering,
		})
	}
	response := tablesListResponse{
		Kind:       "bigquery#tableList",
		Tables:     items,
		TotalItems: len(tables),
	}
	if end < len(tables) {
		response.NextPageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) patchTable(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string, replace bool) {
	existing, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	request, err := s.decodeTableRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.TableReference.ProjectID != "" && request.TableReference.ProjectID != projectID ||
		request.TableReference.DatasetID != "" && request.TableReference.DatasetID != datasetID ||
		request.TableReference.TableID != "" && request.TableReference.TableID != tableID {
		writeError(w, http.StatusBadRequest, "invalid", "tableReference must match request table")
		return
	}
	if request.Schema.Fields != nil {
		if err := validateTableSchema(request.Schema); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
	}

	now := time.Now().UTC()
	if replace {
		existing.FriendlyName = request.FriendlyName
		existing.Description = request.Description
		existing.Labels = request.Labels
		existing.Schema = request.Schema
		existing.Type = defaultString(request.Type, "TABLE")
		existing.TimePartitioning = request.TimePartitioning
		existing.RangePartitioning = request.RangePartitioning
		existing.Clustering = request.Clustering
	} else {
		if request.FriendlyName != "" {
			existing.FriendlyName = request.FriendlyName
		}
		if request.Description != "" {
			existing.Description = request.Description
		}
		if request.Labels != nil {
			existing.Labels = request.Labels
		}
		if request.Schema.Fields != nil {
			existing.Schema = request.Schema
		}
		if request.Type != "" {
			existing.Type = request.Type
		}
		if request.TimePartitioning != nil {
			existing.TimePartitioning = request.TimePartitioning
		}
		if request.RangePartitioning != nil {
			existing.RangePartitioning = request.RangePartitioning
		}
		if request.Clustering != nil {
			existing.Clustering = request.Clustering
		}
	}
	existing.ETag = datasetETag(now)
	existing.LastModifiedTime = unixMillisString(now)
	if err := s.writeTable(existing); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) deleteTable(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	dir := s.tableDir(projectID, datasetID, tableID)
	if _, err := os.Stat(filepath.Join(dir, "table.json")); errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDatasetIAMPolicy(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, action string) {
	if _, found, err := s.readDataset(projectID, datasetID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}
	s.handleIAMPolicy(w, r, action, s.datasetIAMPolicyPath(projectID, datasetID))
}

func (s *Server) handleTableIAMPolicy(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string, action string) {
	if _, found, err := s.readTable(projectID, datasetID, tableID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	s.handleIAMPolicy(w, r, action, s.tableIAMPolicyPath(projectID, datasetID, tableID))
}

func (s *Server) handleIAMPolicy(w http.ResponseWriter, r *http.Request, action string, path string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	switch action {
	case "getIamPolicy":
		policy, err := s.readIAMPolicy(path)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
		writeJSON(w, http.StatusOK, policy)
	case "setIamPolicy":
		request, err := s.decodeSetIAMPolicyRequest(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
			return
		}
		policy := normalizeIAMPolicy(request.Policy)
		if err := s.writeIAMPolicy(path, policy); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
		writeJSON(w, http.StatusOK, policy)
	case "testIamPermissions":
		request, err := s.decodeTestIAMPermissionsRequest(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
			return
		}
		writeJSON(w, http.StatusOK, testIAMPermissionsResponse{Permissions: request.Permissions})
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) insertRows(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	table, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	request, err := s.decodeInsertAllRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	existingRows, err := s.readRows(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	seenInsertIDs := make(map[string]struct{}, len(existingRows)+len(request.Rows))
	for _, row := range existingRows {
		if row.InsertID != "" {
			seenInsertIDs[row.InsertID] = struct{}{}
		}
	}

	now := time.Now().UTC()
	accepted := make([]storedRow, 0, len(request.Rows))
	insertErrors := make([]insertError, 0)
	for i, row := range request.Rows {
		values, rowErrors := validateRowJSON(row.JSON, table.Schema, request.IgnoreUnknownValues)
		if len(rowErrors) > 0 {
			insertErrors = append(insertErrors, insertError{
				Index:  i,
				Errors: rowErrors,
			})
			continue
		}
		if row.InsertID != "" {
			if _, duplicate := seenInsertIDs[row.InsertID]; duplicate {
				continue
			}
			seenInsertIDs[row.InsertID] = struct{}{}
		}
		accepted = append(accepted, storedRow{
			InsertID:   row.InsertID,
			JSON:       values,
			InsertedAt: now.Format(time.RFC3339Nano),
		})
	}
	if len(insertErrors) > 0 && !request.SkipInvalidRows {
		accepted = nil
	}
	if len(accepted) > 0 {
		if int64(len(existingRows)+len(accepted)) > s.maxRowsPerTable() {
			writeError(w, http.StatusBadRequest, "quotaExceeded", "table row limit exceeded")
			return
		}
		if err := s.appendRows(projectID, datasetID, tableID, accepted); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
		if err := s.refreshTableRowStats(table); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
	}

	writeJSON(w, http.StatusOK, insertAllResponse{
		Kind:         "bigquery#tableDataInsertAllResponse",
		InsertErrors: insertErrors,
	})
}

func (s *Server) listRows(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, tableID string) {
	table, found, err := s.readTable(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Table %s:%s.%s", projectID, datasetID, tableID))
		return
	}
	rows, err := s.readRows(projectID, datasetID, tableID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if offset > len(rows) {
		offset = len(rows)
	}
	end := offset + maxResults
	if end > len(rows) {
		end = len(rows)
	}
	fields := selectedRowFields(table.Schema, r.URL.Query().Get("selectedFields"))
	responseRows := make([]tableDataRow, 0, end-offset)
	for _, row := range rows[offset:end] {
		responseRows = append(responseRows, tableDataRow{F: formatRowValues(row.JSON, fields)})
	}
	response := tableDataListResponse{
		Kind:      "bigquery#tableDataList",
		ETag:      datasetETag(time.Now().UTC()),
		TotalRows: strconv.Itoa(len(rows)),
		Rows:      responseRows,
	}
	if end < len(rows) {
		response.PageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) queryRows(w http.ResponseWriter, r *http.Request, projectID string) {
	request, err := s.decodeQueryRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	job, err := s.createQueryJob(projectID, jobReference{Location: request.Location}, request.Query, request.QueryParameters, request.MaxResults, false, request.DryRun, s.effectiveUseLegacySQL(request.UseLegacySQL))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalidQuery", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job.Response)
}

func (s *Server) insertJob(w http.ResponseWriter, r *http.Request, projectID string) {
	request, err := s.decodeJobInsertRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.JobReference.ProjectID != "" && request.JobReference.ProjectID != projectID {
		writeError(w, http.StatusBadRequest, "invalid", "jobReference.projectId must match request project")
		return
	}
	switch {
	case strings.TrimSpace(request.Configuration.Query.Query) != "":
		job, err := s.createQueryJob(projectID, request.JobReference, request.Configuration.Query.Query, request.Configuration.Query.QueryParameters, 0, true, request.Configuration.DryRun, s.effectiveUseLegacySQL(request.Configuration.Query.UseLegacySQL))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalidQuery", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Copy.DestinationTable.TableID != "":
		job, err := s.createCopyJob(projectID, request.JobReference, request.Configuration.Copy)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Load.DestinationTable.TableID != "":
		job, err := s.createLoadJob(r.Context(), projectID, request.JobReference, request.Configuration.Load)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Extract.SourceTable.TableID != "":
		job, err := s.createExtractJob(r.Context(), projectID, request.JobReference, request.Configuration.Extract)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, job.Job)
	default:
		writeError(w, http.StatusBadRequest, "invalid", "configuration.query.query, configuration.copy, configuration.load, or configuration.extract is required")
	}
}

func (s *Server) insertUploadJob(w http.ResponseWriter, r *http.Request, projectID string) {
	if r.URL.Query().Get("uploadType") != "multipart" {
		writeError(w, http.StatusBadRequest, "invalid", "uploadType=multipart is required")
		return
	}
	request, media, err := s.decodeMultipartJobInsertRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid multipart upload request")
		return
	}
	if request.JobReference.ProjectID != "" && request.JobReference.ProjectID != projectID {
		writeError(w, http.StatusBadRequest, "invalid", "jobReference.projectId must match request project")
		return
	}
	if request.Configuration.Load.DestinationTable.TableID == "" {
		writeError(w, http.StatusBadRequest, "invalid", "configuration.load.destinationTable is required")
		return
	}
	job, err := s.createUploadLoadJob(projectID, request.JobReference, request.Configuration.Load, bytes.NewReader(media))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job.Job)
}

func (s *Server) createCopyJob(requestProjectID string, requestedRef jobReference, config copyJobConfiguration) (queryJobRecord, error) {
	sourceTableRefs, err := copySourceTables(config)
	if err != nil {
		return queryJobRecord{}, err
	}
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	if _, found, err := s.readDataset(destinationRef.ProjectID, destinationRef.DatasetID); err != nil {
		return queryJobRecord{}, err
	} else if !found {
		return queryJobRecord{}, fmt.Errorf("not found: dataset %s:%s", destinationRef.ProjectID, destinationRef.DatasetID)
	}

	var source tableResource
	sourceRows := make([]storedRow, 0)
	for index, sourceTableRef := range sourceTableRefs {
		sourceTableRef = normalizeTableReference(sourceTableRef, requestProjectID)
		if err := validateTableReference(sourceTableRef); err != nil {
			return queryJobRecord{}, err
		}
		sourceTable, found, err := s.readTable(sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if !found {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		}
		if index == 0 {
			source = sourceTable
		} else if !reflect.DeepEqual(source.Schema, sourceTable.Schema) {
			return queryJobRecord{}, fmt.Errorf("source tables must have matching schemas")
		}
		rows, err := s.readRows(sourceTableRef.ProjectID, sourceTableRef.DatasetID, sourceTableRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		sourceRows = append(sourceRows, rows...)
	}

	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_EMPTY")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	if writeDisposition != "WRITE_EMPTY" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_APPEND" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}

	destination, destinationFound, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !destinationFound && createDisposition == "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	}
	if destinationFound && writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	now := time.Now().UTC()
	if !destinationFound || writeDisposition == "WRITE_TRUNCATE" {
		destination = source
		destination.ID = destinationRef.ProjectID + ":" + destinationRef.DatasetID + "." + destinationRef.TableID
		destination.SelfLink = s.tableSelfLink(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		destination.TableReference = destinationRef
		destination.CreationTime = unixMillisString(now)
	}
	destination.ETag = datasetETag(now)
	destination.LastModifiedTime = unixMillisString(now)
	if err := s.writeTable(destination); err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(sourceRows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, sourceRows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(destination); err != nil {
		return queryJobRecord{}, err
	}

	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Copy: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createLoadJob(ctx context.Context, requestProjectID string, requestedRef jobReference, config loadJobConfiguration) (queryJobRecord, error) {
	if s.config.ObjectStore == nil {
		return queryJobRecord{}, fmt.Errorf("local GCS object store is not configured")
	}
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	sourceFormat := normalizeDataFormat(config.SourceFormat, "NEWLINE_DELIMITED_JSON")
	if sourceFormat != "NEWLINE_DELIMITED_JSON" && sourceFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported load sourceFormat %q", config.SourceFormat)
	}
	if len(config.SourceURIs) == 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.sourceUris is required")
	}
	table, found, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		if createDisposition == "CREATE_NEVER" {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		}
		var err error
		table, err = s.createLoadDestinationTable(destinationRef, config.Schema)
		if err != nil {
			return queryJobRecord{}, err
		}
	}
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_APPEND")
	if writeDisposition != "WRITE_APPEND" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_EMPTY" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}
	if writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	if config.SkipLeadingRows < 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.skipLeadingRows must be non-negative")
	}
	rows, err := s.loadRowsFromGCSObjects(ctx, config.SourceURIs, sourceFormat, table.Schema, config.SkipLeadingRows)
	if err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(rows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, rows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(table); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Load: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createUploadLoadJob(requestProjectID string, requestedRef jobReference, config loadJobConfiguration, media io.Reader) (queryJobRecord, error) {
	destinationRef := normalizeTableReference(config.DestinationTable, requestProjectID)
	if err := validateTableReference(destinationRef); err != nil {
		return queryJobRecord{}, err
	}
	createDisposition := defaultString(config.CreateDisposition, "CREATE_IF_NEEDED")
	if createDisposition != "CREATE_IF_NEEDED" && createDisposition != "CREATE_NEVER" {
		return queryJobRecord{}, fmt.Errorf("unsupported createDisposition %q", config.CreateDisposition)
	}
	sourceFormat := normalizeDataFormat(config.SourceFormat, "NEWLINE_DELIMITED_JSON")
	if sourceFormat != "NEWLINE_DELIMITED_JSON" && sourceFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported load sourceFormat %q", config.SourceFormat)
	}
	table, found, err := s.readTable(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		if createDisposition == "CREATE_NEVER" {
			return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		}
		var err error
		table, err = s.createLoadDestinationTable(destinationRef, config.Schema)
		if err != nil {
			return queryJobRecord{}, err
		}
	}
	writeDisposition := defaultString(config.WriteDisposition, "WRITE_APPEND")
	if writeDisposition != "WRITE_APPEND" && writeDisposition != "WRITE_TRUNCATE" && writeDisposition != "WRITE_EMPTY" {
		return queryJobRecord{}, fmt.Errorf("unsupported writeDisposition %q", config.WriteDisposition)
	}
	if writeDisposition == "WRITE_EMPTY" {
		existingRows, err := s.readRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)
		if err != nil {
			return queryJobRecord{}, err
		}
		if len(existingRows) > 0 {
			return queryJobRecord{}, fmt.Errorf("destination table is not empty")
		}
	}

	if config.SkipLeadingRows < 0 {
		return queryJobRecord{}, fmt.Errorf("configuration.load.skipLeadingRows must be non-negative")
	}
	rows, err := loadRows(media, sourceFormat, table.Schema, config.SkipLeadingRows)
	if err != nil {
		return queryJobRecord{}, err
	}
	if writeDisposition == "WRITE_TRUNCATE" {
		if err := os.Remove(s.rowsPath(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return queryJobRecord{}, err
		}
	}
	if len(rows) > 0 {
		if err := s.appendRows(destinationRef.ProjectID, destinationRef.DatasetID, destinationRef.TableID, rows); err != nil {
			return queryJobRecord{}, err
		}
	}
	if err := s.refreshTableRowStats(table); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Load: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) createLoadDestinationTable(ref tableReference, schema tableSchema) (tableResource, error) {
	if len(schema.Fields) == 0 {
		return tableResource{}, fmt.Errorf("configuration.load.schema is required when creating a destination table")
	}
	if err := validateTableSchema(schema); err != nil {
		return tableResource{}, err
	}
	if _, found, err := s.readDataset(ref.ProjectID, ref.DatasetID); err != nil {
		return tableResource{}, err
	} else if !found {
		return tableResource{}, fmt.Errorf("not found: dataset %s:%s", ref.ProjectID, ref.DatasetID)
	}

	now := time.Now().UTC()
	table := tableResource{
		Kind:             "bigquery#table",
		ID:               ref.ProjectID + ":" + ref.DatasetID + "." + ref.TableID,
		SelfLink:         s.tableSelfLink(ref.ProjectID, ref.DatasetID, ref.TableID),
		TableReference:   ref,
		Type:             "TABLE",
		Schema:           schema,
		ETag:             datasetETag(now),
		CreationTime:     unixMillisString(now),
		LastModifiedTime: unixMillisString(now),
		NumRows:          "0",
		NumBytes:         "0",
		Location:         s.defaultLocation(),
	}
	if err := s.writeTable(table); err != nil {
		return tableResource{}, err
	}
	return table, nil
}

func (s *Server) createExtractJob(ctx context.Context, requestProjectID string, requestedRef jobReference, config extractJobConfiguration) (queryJobRecord, error) {
	if s.config.ObjectStore == nil {
		return queryJobRecord{}, fmt.Errorf("local GCS object store is not configured")
	}
	sourceRef := normalizeTableReference(config.SourceTable, requestProjectID)
	if err := validateTableReference(sourceRef); err != nil {
		return queryJobRecord{}, err
	}
	destinationFormat := normalizeDataFormat(config.DestinationFormat, "NEWLINE_DELIMITED_JSON")
	if destinationFormat != "NEWLINE_DELIMITED_JSON" && destinationFormat != "JSON" && destinationFormat != "CSV" {
		return queryJobRecord{}, fmt.Errorf("unsupported extract destinationFormat %q", config.DestinationFormat)
	}
	if len(config.DestinationURIs) != 1 {
		return queryJobRecord{}, fmt.Errorf("configuration.extract.destinationUris must contain exactly one URI")
	}
	destinationBucket, destinationKey, err := parseGCSURI(config.DestinationURIs[0])
	if err != nil {
		return queryJobRecord{}, err
	}
	table, found, err := s.readTable(sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	if !found {
		return queryJobRecord{}, fmt.Errorf("not found: table %s:%s.%s", sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	}
	rows, err := s.readRows(sourceRef.ProjectID, sourceRef.DatasetID, sourceRef.TableID)
	if err != nil {
		return queryJobRecord{}, err
	}
	body, contentType, err := extractedRows(rows, destinationFormat, table.Schema)
	if err != nil {
		return queryJobRecord{}, err
	}
	if _, err := s.config.ObjectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      destinationBucket,
		Key:         destinationKey,
		Body:        bytes.NewReader(body),
		ContentType: contentType,
	}); err != nil {
		return queryJobRecord{}, err
	}

	now := time.Now().UTC()
	return s.createCompletedJob(requestProjectID, requestedRef, jobConfiguration{Extract: config}, jobStatistics{
		CreationTime: unixMillisString(now),
		StartTime:    unixMillisString(now),
		EndTime:      unixMillisString(now),
	})
}

func (s *Server) loadRowsFromGCSObjects(ctx context.Context, sourceURIs []string, sourceFormat string, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	rows := make([]storedRow, 0)
	for _, uri := range sourceURIs {
		bucket, key, err := parseGCSURI(uri)
		if err != nil {
			return nil, err
		}
		_, body, found, err := s.config.ObjectStore.GetObject(ctx, bucket, key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("source object not found")
		}
		loadedRows, err := loadRows(bytes.NewReader(body), sourceFormat, schema, skipLeadingRows)
		if err != nil {
			return nil, err
		}
		rows = append(rows, loadedRows...)
	}
	return rows, nil
}

func loadRows(body io.Reader, sourceFormat string, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	switch normalizeDataFormat(sourceFormat, "NEWLINE_DELIMITED_JSON") {
	case "NEWLINE_DELIMITED_JSON":
		return loadRowsFromNDJSON(body, schema)
	case "CSV":
		return loadRowsFromCSV(body, schema, skipLeadingRows)
	default:
		return nil, fmt.Errorf("unsupported load sourceFormat %q", sourceFormat)
	}
}

func loadRowsFromNDJSON(body io.Reader, schema tableSchema) ([]storedRow, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	decoder := json.NewDecoder(body)
	rows := make([]storedRow, 0)
	for {
		var row map[string]json.RawMessage
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("invalid newline-delimited JSON object")
		}
		values, rowErrors := validateRowJSON(row, schema, false)
		if len(rowErrors) > 0 {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		rows = append(rows, storedRow{JSON: values, InsertedAt: now})
	}
	return rows, nil
}

func loadRowsFromCSV(body io.Reader, schema tableSchema, skipLeadingRows int) ([]storedRow, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	reader := csv.NewReader(body)
	reader.FieldsPerRecord = len(schema.Fields)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("invalid CSV row")
	}
	if skipLeadingRows > len(records) {
		records = nil
	} else if skipLeadingRows > 0 {
		records = records[skipLeadingRows:]
	}
	rows := make([]storedRow, 0, len(records))
	for _, record := range records {
		row := make(map[string]json.RawMessage, len(schema.Fields))
		for i, field := range schema.Fields {
			raw, err := csvCellRawMessage(record[i], field)
			if err != nil {
				return nil, err
			}
			row[field.Name] = raw
		}
		values, rowErrors := validateRowJSON(row, schema, false)
		if len(rowErrors) > 0 {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		rows = append(rows, storedRow{JSON: values, InsertedAt: now})
	}
	return rows, nil
}

func csvCellRawMessage(value string, field tableFieldSchema) (json.RawMessage, error) {
	if value == "" {
		return json.RawMessage("null"), nil
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON":
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return encoded, nil
	case "INTEGER", "INT64":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		return json.RawMessage(value), nil
	case "FLOAT", "FLOAT64":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		return json.RawMessage(value), nil
	case "BOOLEAN", "BOOL":
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("source row does not match destination schema")
		}
		if parsed {
			return json.RawMessage("true"), nil
		}
		return json.RawMessage("false"), nil
	case "RECORD", "STRUCT":
		return nil, fmt.Errorf("CSV load does not support RECORD fields")
	default:
		return nil, fmt.Errorf("unsupported field type %q", field.Type)
	}
}

func extractedRows(rows []storedRow, format string, schema tableSchema) ([]byte, string, error) {
	switch normalizeDataFormat(format, "NEWLINE_DELIMITED_JSON") {
	case "NEWLINE_DELIMITED_JSON", "JSON":
		body, err := extractedNDJSON(rows, schema)
		return body, "application/x-ndjson", err
	case "CSV":
		body, err := extractedCSV(rows, schema)
		return body, "text/csv", err
	default:
		return nil, "", fmt.Errorf("unsupported extract destinationFormat %q", format)
	}
}

func extractedNDJSON(rows []storedRow, schema tableSchema) ([]byte, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, row := range rows {
		value := make(map[string]any, len(schema.Fields))
		for _, field := range schema.Fields {
			raw, ok := row.JSON[field.Name]
			if !ok || isJSONNull(raw) {
				value[field.Name] = nil
				continue
			}
			value[field.Name] = rawValueForResponse(raw)
		}
		if err := encoder.Encode(value); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}

func extractedCSV(rows []storedRow, schema tableSchema) ([]byte, error) {
	var body bytes.Buffer
	writer := csv.NewWriter(&body)
	for _, row := range rows {
		record := make([]string, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			raw, ok := row.JSON[field.Name]
			if !ok || isJSONNull(raw) {
				record = append(record, "")
				continue
			}
			record = append(record, csvValueString(rawValueForResponse(raw)))
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func csvValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

func normalizeDataFormat(value string, fallback string) string {
	return strings.ToUpper(strings.TrimSpace(defaultString(value, fallback)))
}

func parseGCSURI(uri string) (string, string, error) {
	if strings.ContainsAny(uri, "*?[") {
		return "", "", fmt.Errorf("wildcard GCS URIs are not supported")
	}
	trimmed := strings.TrimSpace(uri)
	if !strings.HasPrefix(trimmed, "gs://") {
		return "", "", fmt.Errorf("only gs:// URIs are supported")
	}
	withoutScheme := strings.TrimPrefix(trimmed, "gs://")
	bucket, key, ok := strings.Cut(withoutScheme, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fmt.Errorf("gs:// URI must include bucket and object")
	}
	return bucket, key, nil
}

func (s *Server) createCompletedJob(requestProjectID string, requestedRef jobReference, config jobConfiguration, statistics jobStatistics) (queryJobRecord, error) {
	now := time.Now().UTC()
	jobID := strings.TrimSpace(requestedRef.JobID)
	if jobID == "" {
		jobID = "devcloud_job_" + strconv.FormatInt(now.UnixNano(), 10)
	} else if err := validateResourceID(jobID, "job"); err != nil {
		return queryJobRecord{}, err
	} else if _, found, err := s.readQueryJob(requestProjectID, jobID); err != nil {
		return queryJobRecord{}, err
	} else if found {
		return queryJobRecord{}, fmt.Errorf("already exists: job %s:%s", requestProjectID, jobID)
	}
	jobRef := jobReference{
		ProjectID: requestProjectID,
		JobID:     jobID,
		Location:  defaultString(requestedRef.Location, s.defaultLocation()),
	}
	resource := jobResource{
		Kind:          "bigquery#job",
		ID:            requestProjectID + ":" + jobID,
		SelfLink:      "/bigquery/v2/projects/" + url.PathEscape(requestProjectID) + "/jobs/" + url.PathEscape(jobID),
		JobReference:  jobRef,
		Configuration: config,
		Status:        jobStatus{State: "DONE"},
		Statistics:    statistics,
	}
	job := queryJobRecord{
		Job: resource,
		Response: queryResponse{
			Kind:         "bigquery#queryResponse",
			JobReference: jobRef,
			TotalRows:    "0",
			JobComplete:  true,
			CacheHit:     false,
		},
	}
	if err := s.writeQueryJob(requestProjectID, jobID, job); err != nil {
		return queryJobRecord{}, err
	}
	return job, nil
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, projectID string) {
	jobs, err := s.readQueryJobRecords(projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if offset > len(jobs) {
		offset = len(jobs)
	}
	end := offset + maxResults
	if end > len(jobs) {
		end = len(jobs)
	}
	items := make([]jobResource, 0, end-offset)
	for _, job := range jobs[offset:end] {
		items = append(items, job.Job)
	}
	response := jobsListResponse{
		Kind: "bigquery#jobList",
		Jobs: items,
	}
	if end < len(jobs) {
		response.NextPageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, projectID string, jobID string) {
	if err := validateResourceID(jobID, "job"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	job, found, err := s.readQueryJob(projectID, jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Job %s:%s", projectID, jobID))
		return
	}
	writeJSON(w, http.StatusOK, jobCancelResponse{
		Kind: "bigquery#jobCancelResponse",
		Job:  job.Job,
	})
}

func (s *Server) deleteJobMetadata(w http.ResponseWriter, r *http.Request, projectID string, jobID string) {
	if err := validateResourceID(jobID, "job"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	path := s.queryJobPath(projectID, jobID)
	if err := os.Remove(path); errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Job %s:%s", projectID, jobID))
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getQueryResults(w http.ResponseWriter, r *http.Request, projectID string, jobID string) {
	if err := validateResourceID(jobID, "job"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	job, found, err := s.readQueryJob(projectID, jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Job %s:%s", projectID, jobID))
		return
	}
	offset, err := rowOffsetFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	maxResults, err := maxResultsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.pageQueryResponse(job.Response, offset, maxResults))
}

func (s *Server) pageQueryResponse(response queryResponse, offset int, maxResults int) queryResponse {
	if offset < 0 {
		offset = 0
	}
	if maxResults <= 0 || maxResults > s.maxResultRows() {
		maxResults = s.maxResultRows()
	}
	if offset > len(response.Rows) {
		offset = len(response.Rows)
	}
	end := offset + maxResults
	if end > len(response.Rows) {
		end = len(response.Rows)
	}
	paged := response
	paged.Rows = response.Rows[offset:end]
	if end < len(response.Rows) {
		paged.PageToken = strconv.Itoa(end)
	} else {
		paged.PageToken = ""
	}
	return paged
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
	ID           string          `json:"id"`
	ProjectID    string          `json:"projectId"`
	DatasetID    string          `json:"datasetId"`
	Location     string          `json:"location,omitempty"`
	FriendlyName string          `json:"friendlyName,omitempty"`
	Description  string          `json:"description,omitempty"`
	Tables       []TableSnapshot `json:"tables"`
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

func (s *Server) createQueryJob(requestProjectID string, requestedRef jobReference, rawQuery string, parameters []queryParameter, maxResults int, includeConfiguration bool, dryRun bool, useLegacySQL bool) (queryJobRecord, error) {
	if useLegacySQL {
		return queryJobRecord{}, fmt.Errorf("legacy SQL is not supported; set useLegacySql to false")
	}
	effectiveQuery, err := bindQueryParameters(rawQuery, parameters)
	if err != nil {
		return queryJobRecord{}, err
	}
	result, err := s.executeQueryForJob(requestProjectID, effectiveQuery, dryRun)
	if err != nil {
		return queryJobRecord{}, err
	}
	now := time.Now().UTC()
	jobID := strings.TrimSpace(requestedRef.JobID)
	if jobID == "" {
		jobID = "devcloud_query_" + strconv.FormatInt(now.UnixNano(), 10)
	} else if err := validateResourceID(jobID, "job"); err != nil {
		return queryJobRecord{}, err
	} else if _, found, err := s.readQueryJob(requestProjectID, jobID); err != nil {
		return queryJobRecord{}, err
	} else if found {
		return queryJobRecord{}, fmt.Errorf("already exists: job %s:%s", requestProjectID, jobID)
	}
	jobRef := jobReference{
		ProjectID: requestProjectID,
		JobID:     jobID,
		Location:  defaultString(requestedRef.Location, s.defaultLocation()),
	}
	response := queryResponse{
		Kind:         "bigquery#queryResponse",
		Schema:       tableSchema{Fields: result.Fields},
		JobReference: jobRef,
		TotalRows:    strconv.Itoa(len(result.Rows)),
		Rows:         result.Rows,
		JobComplete:  true,
		CacheHit:     false,
	}
	resource := jobResource{
		Kind:         "bigquery#job",
		ID:           requestProjectID + ":" + jobID,
		SelfLink:     "/bigquery/v2/projects/" + url.PathEscape(requestProjectID) + "/jobs/" + url.PathEscape(jobID),
		JobReference: jobRef,
		Status: jobStatus{
			State: "DONE",
		},
		Statistics: jobStatistics{
			CreationTime: unixMillisString(now),
			StartTime:    unixMillisString(now),
			EndTime:      unixMillisString(now),
			Query: queryStatistics{
				TotalRows: strconv.Itoa(len(result.Rows)),
				CacheHit:  false,
				DryRun:    dryRun,
			},
		},
	}
	if includeConfiguration {
		resource.Configuration = jobConfiguration{
			DryRun: dryRun,
			Query: queryJobConfiguration{
				Query:           rawQuery,
				UseLegacySQL:    boolPtr(false),
				QueryParameters: parameters,
			},
		}
	}
	job := queryJobRecord{
		Job:      resource,
		Response: response,
	}
	if err := s.writeQueryJob(requestProjectID, jobID, job); err != nil {
		return queryJobRecord{}, err
	}
	job.Response = s.pageQueryResponse(response, 0, maxResults)
	return job, nil
}

func (s *Server) executeQueryForJob(requestProjectID string, rawQuery string, dryRun bool) (queryExecutionResult, error) {
	if dryRun {
		return s.dryRunQuery(requestProjectID, rawQuery)
	}
	return s.executeQuery(requestProjectID, rawQuery)
}

func (s *Server) dryRunQuery(requestProjectID string, rawQuery string) (queryExecutionResult, error) {
	query, err := parseSimpleSelect(rawQuery, requestProjectID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	table, found, err := s.readTable(query.ProjectID, query.DatasetID, query.TableID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	if !found {
		return queryExecutionResult{}, fmt.Errorf("not found: table %s:%s.%s", query.ProjectID, query.DatasetID, query.TableID)
	}
	if query.Aggregate.Function != "" {
		if query.GroupBy != "" {
			fields, err := groupedAggregateDryRunFields(table.Schema, query)
			if err != nil {
				return queryExecutionResult{}, err
			}
			return queryExecutionResult{Fields: fields}, nil
		}
		fields, err := aggregateDryRunFields(table.Schema, query.Aggregate)
		if err != nil {
			return queryExecutionResult{}, err
		}
		return queryExecutionResult{Fields: fields}, nil
	}
	fields, err := fieldsForQuery(table.Schema, query.SelectedFields)
	if err != nil {
		return queryExecutionResult{}, err
	}
	return queryExecutionResult{Fields: fields}, nil
}

func (s *Server) executeQuery(requestProjectID string, rawQuery string) (queryExecutionResult, error) {
	query, err := parseSimpleSelect(rawQuery, requestProjectID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	table, found, err := s.readTable(query.ProjectID, query.DatasetID, query.TableID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	if !found {
		return queryExecutionResult{}, fmt.Errorf("not found: table %s:%s.%s", query.ProjectID, query.DatasetID, query.TableID)
	}
	rows, err := s.readRows(query.ProjectID, query.DatasetID, query.TableID)
	if err != nil {
		return queryExecutionResult{}, err
	}
	selectedFields, err := fieldsForQuery(table.Schema, query.SelectedFields)
	if err != nil {
		return queryExecutionResult{}, err
	}
	filtered := make([]storedRow, 0, len(rows))
	for _, row := range rows {
		matches, err := rowMatchesQuery(row, query)
		if err != nil {
			return queryExecutionResult{}, err
		}
		if matches {
			filtered = append(filtered, row)
		}
	}
	if query.Aggregate.Function != "" {
		if query.GroupBy != "" {
			return executeGroupedAggregateQuery(filtered, table.Schema, query)
		}
		return executeAggregateQuery(filtered, table.Schema, query)
	}
	if query.OrderBy != "" {
		sort.SliceStable(filtered, func(i, j int) bool {
			left := filtered[i].JSON[query.OrderBy]
			right := filtered[j].JSON[query.OrderBy]
			cmp := compareRawValues(left, right)
			if query.OrderDesc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	if query.Offset > 0 {
		if query.Offset >= len(filtered) {
			filtered = nil
		} else {
			filtered = filtered[query.Offset:]
		}
	}
	if query.Limit >= 0 && query.Limit < len(filtered) {
		filtered = filtered[:query.Limit]
	}
	responseRows := make([]tableDataRow, 0, len(filtered))
	for _, row := range filtered {
		responseRows = append(responseRows, tableDataRow{F: formatRowValues(row.JSON, selectedFields)})
	}
	return queryExecutionResult{Fields: selectedFields, Rows: responseRows}, nil
}

func executeAggregateQuery(rows []storedRow, schema tableSchema, query simpleSelectQuery) (queryExecutionResult, error) {
	fieldName := query.Aggregate.Alias
	if fieldName == "" {
		fieldName = "f0_"
	}

	field, hasField := aggregateField(schema, query.Aggregate)

	switch query.Aggregate.Field {
	case "*":
		if query.Aggregate.Function != "COUNT" {
			return queryExecutionResult{}, fmt.Errorf("%s requires a field", query.Aggregate.Function)
		}
		data, _ := json.Marshal(strconv.Itoa(len(rows)))
		return queryExecutionResult{
			Fields: []tableFieldSchema{{
				Name: fieldName,
				Type: "INTEGER",
				Mode: "NULLABLE",
			}},
			Rows: []tableDataRow{{
				F: []tableCell{{V: rawValueForResponse(data)}},
			}},
		}, nil
	default:
		if !hasField {
			return queryExecutionResult{}, fmt.Errorf("aggregate field %q does not exist", query.Aggregate.Field)
		}
	}

	switch query.Aggregate.Function {
	case "COUNT":
		count := 0
		for _, row := range rows {
			raw, ok := row.JSON[query.Aggregate.Field]
			if ok && !isJSONNull(raw) {
				count++
			}
		}
		return singleAggregateResult(fieldName, "INTEGER", strconv.Itoa(count)), nil
	case "SUM":
		if !isNumericField(field) {
			return queryExecutionResult{}, fmt.Errorf("SUM requires a numeric field")
		}
		sum, count, err := sumAggregate(rows, query.Aggregate.Field, isIntegerField(field))
		if err != nil {
			return queryExecutionResult{}, err
		}
		if count == 0 {
			return singleAggregateNullResult(fieldName, aggregateNumericType(field)), nil
		}
		return singleAggregateResult(fieldName, aggregateNumericType(field), sum), nil
	case "AVG":
		if !isNumericField(field) {
			return queryExecutionResult{}, fmt.Errorf("AVG requires a numeric field")
		}
		sum, count, err := floatAggregate(rows, query.Aggregate.Field)
		if err != nil {
			return queryExecutionResult{}, err
		}
		if count == 0 {
			return singleAggregateNullResult(fieldName, "FLOAT"), nil
		}
		return singleAggregateResult(fieldName, "FLOAT", strconv.FormatFloat(sum/float64(count), 'f', -1, 64)), nil
	case "MIN", "MAX":
		raw, ok := minMaxAggregate(rows, query.Aggregate.Field, query.Aggregate.Function)
		if !ok {
			return singleAggregateNullResult(fieldName, defaultString(field.Type, "STRING")), nil
		}
		return queryExecutionResult{
			Fields: []tableFieldSchema{{
				Name: fieldName,
				Type: defaultString(field.Type, "STRING"),
				Mode: "NULLABLE",
			}},
			Rows: []tableDataRow{{
				F: []tableCell{{V: rawValueForResponse(raw)}},
			}},
		}, nil
	default:
		return queryExecutionResult{}, fmt.Errorf("unsupported aggregate function")
	}
}

func executeGroupedAggregateQuery(rows []storedRow, schema tableSchema, query simpleSelectQuery) (queryExecutionResult, error) {
	fields, err := groupedAggregateDryRunFields(schema, query)
	if err != nil {
		return queryExecutionResult{}, err
	}
	groups := make(map[string][]storedRow)
	groupValues := make(map[string]json.RawMessage)
	for _, row := range rows {
		raw, ok := row.JSON[query.GroupBy]
		if !ok {
			raw = json.RawMessage("null")
		}
		key := string(raw)
		groups[key] = append(groups[key], row)
		groupValues[key] = raw
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		cmp := compareRawValues(groupValues[keys[i]], groupValues[keys[j]])
		if query.OrderDesc {
			return cmp > 0
		}
		return cmp < 0
	})
	if query.OrderBy != "" && query.OrderBy != query.GroupBy {
		return queryExecutionResult{}, fmt.Errorf("ORDER BY supports grouped field only for GROUP BY queries")
	}
	if query.Offset > 0 {
		if query.Offset >= len(keys) {
			keys = nil
		} else {
			keys = keys[query.Offset:]
		}
	}
	if query.Limit >= 0 && query.Limit < len(keys) {
		keys = keys[:query.Limit]
	}

	responseRows := make([]tableDataRow, 0, len(keys))
	for _, key := range keys {
		aggregate, err := executeAggregateQuery(groups[key], schema, simpleSelectQuery{Aggregate: query.Aggregate})
		if err != nil {
			return queryExecutionResult{}, err
		}
		cell := tableCell{V: nil}
		if raw := groupValues[key]; !isJSONNull(raw) {
			cell.V = rawValueForResponse(raw)
		}
		if len(aggregate.Rows) == 0 || len(aggregate.Rows[0].F) == 0 {
			return queryExecutionResult{}, fmt.Errorf("aggregate result is empty")
		}
		responseRows = append(responseRows, tableDataRow{
			F: []tableCell{cell, aggregate.Rows[0].F[0]},
		})
	}
	return queryExecutionResult{Fields: fields, Rows: responseRows}, nil
}

func parseSimpleSelect(rawQuery string, defaultProjectID string) (simpleSelectQuery, error) {
	query := strings.TrimSpace(strings.TrimSuffix(rawQuery, ";"))
	if query == "" {
		return simpleSelectQuery{}, fmt.Errorf("query is required")
	}
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "SELECT ") {
		return simpleSelectQuery{}, fmt.Errorf("only SELECT queries are supported")
	}
	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex < 0 {
		return simpleSelectQuery{}, fmt.Errorf("SELECT query requires FROM")
	}
	selected := strings.TrimSpace(query[len("SELECT "):fromIndex])
	if selected == "" {
		return simpleSelectQuery{}, fmt.Errorf("SELECT list is required")
	}
	rest := strings.TrimSpace(query[fromIndex+len(" FROM "):])
	tableExpr, rest := nextQueryToken(rest)
	projectID, datasetID, tableID, err := parseTableIdentifier(tableExpr, defaultProjectID)
	if err != nil {
		return simpleSelectQuery{}, err
	}
	parsed := simpleSelectQuery{
		ProjectID:      projectID,
		DatasetID:      datasetID,
		TableID:        tableID,
		SelectedFields: parseSelectedFields(selected),
		Limit:          -1,
		Offset:         0,
		WhereOperator:  "",
		WhereField:     "",
		WhereValueRaw:  nil,
		OrderBy:        "",
	}
	if len(parsed.SelectedFields) == 0 {
		return simpleSelectQuery{}, fmt.Errorf("SELECT list is required")
	}
	if aggregate, ok, err := parseAggregateSelection(selected); err != nil {
		return simpleSelectQuery{}, err
	} else if ok {
		parsed.Aggregate = aggregate
		parsed.SelectedFields = nil
	} else if groupField, aggregate, ok, err := parseGroupedAggregateSelection(selected); err != nil {
		return simpleSelectQuery{}, err
	} else if ok {
		parsed.Aggregate = aggregate
		parsed.SelectedFields = []string{groupField}
	}
	rest = strings.TrimSpace(rest)
	for rest != "" {
		upperRest := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(upperRest, "WHERE "):
			conditionEnd := len(rest)
			for _, marker := range []string{" GROUP BY ", " ORDER BY ", " LIMIT ", " OFFSET "} {
				if idx := strings.Index(strings.ToUpper(rest), marker); idx >= 0 && idx < conditionEnd {
					conditionEnd = idx
				}
			}
			condition := strings.TrimSpace(rest[len("WHERE "):conditionEnd])
			conditionGroups, err := parseSimpleConditionGroups(condition)
			if err != nil {
				return simpleSelectQuery{}, err
			}
			conditions := flattenWhereConditionGroups(conditionGroups)
			parsed.WhereConditions = conditions
			parsed.WhereConditionGroups = conditionGroups
			if len(conditions) > 0 {
				parsed.WhereField = conditions[0].Field
				parsed.WhereOperator = conditions[0].Operator
				parsed.WhereValueRaw = conditions[0].ValueRaw
			}
			rest = strings.TrimSpace(rest[conditionEnd:])
		case strings.HasPrefix(upperRest, "GROUP BY "):
			value := strings.TrimSpace(rest[len("GROUP BY "):])
			field, suffix := nextQueryToken(value)
			if field == "" {
				return simpleSelectQuery{}, fmt.Errorf("GROUP BY field is required")
			}
			parsed.GroupBy = strings.Trim(field, "`")
			rest = strings.TrimSpace(suffix)
		case strings.HasPrefix(upperRest, "ORDER BY "):
			if parsed.Aggregate.Function != "" && parsed.GroupBy == "" {
				return simpleSelectQuery{}, fmt.Errorf("ORDER BY is not supported for aggregate queries")
			}
			value := strings.TrimSpace(rest[len("ORDER BY "):])
			field, suffix := nextQueryToken(value)
			if field == "" {
				return simpleSelectQuery{}, fmt.Errorf("ORDER BY field is required")
			}
			parsed.OrderBy = strings.Trim(field, "`")
			rest = strings.TrimSpace(suffix)
			direction, directionSuffix := nextQueryToken(rest)
			switch strings.ToUpper(direction) {
			case "ASC":
				rest = strings.TrimSpace(directionSuffix)
			case "DESC":
				parsed.OrderDesc = true
				rest = strings.TrimSpace(directionSuffix)
			}
		case strings.HasPrefix(upperRest, "LIMIT "):
			value, suffix := nextQueryToken(strings.TrimSpace(rest[len("LIMIT "):]))
			limit, err := strconv.Atoi(value)
			if err != nil || limit < 0 {
				return simpleSelectQuery{}, fmt.Errorf("LIMIT must be a non-negative integer")
			}
			parsed.Limit = limit
			rest = strings.TrimSpace(suffix)
		case strings.HasPrefix(upperRest, "OFFSET "):
			value, suffix := nextQueryToken(strings.TrimSpace(rest[len("OFFSET "):]))
			offset, err := strconv.Atoi(value)
			if err != nil || offset < 0 {
				return simpleSelectQuery{}, fmt.Errorf("OFFSET must be a non-negative integer")
			}
			parsed.Offset = offset
			rest = strings.TrimSpace(suffix)
		default:
			return simpleSelectQuery{}, fmt.Errorf("unsupported query clause")
		}
	}
	if parsed.GroupBy != "" {
		if parsed.Aggregate.Function == "" {
			return simpleSelectQuery{}, fmt.Errorf("GROUP BY requires an aggregate selection")
		}
		if len(parsed.SelectedFields) != 1 || parsed.SelectedFields[0] != parsed.GroupBy {
			return simpleSelectQuery{}, fmt.Errorf("GROUP BY field must be selected")
		}
	}
	return parsed, nil
}

func bindQueryParameters(rawQuery string, parameters []queryParameter) (string, error) {
	if len(parameters) == 0 {
		return rawQuery, nil
	}
	replacements := make(map[string]string, len(parameters))
	for _, parameter := range parameters {
		name := strings.TrimSpace(parameter.Name)
		if name == "" {
			return "", fmt.Errorf("named query parameter name is required")
		}
		if err := validateResourceID(name, "query parameter"); err != nil {
			return "", err
		}
		value, err := parameterSQLLiteral(parameter)
		if err != nil {
			return "", err
		}
		replacements[name] = value
	}
	bound, used, err := replaceNamedParameters(rawQuery, replacements)
	if err != nil {
		return "", err
	}
	for name := range replacements {
		if !used[name] {
			return "", fmt.Errorf("query parameter %q was not used", name)
		}
	}
	return bound, nil
}

func replaceNamedParameters(query string, replacements map[string]string) (string, map[string]bool, error) {
	var out strings.Builder
	used := make(map[string]bool, len(replacements))
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(query); {
		ch := query[i]
		switch ch {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			out.WriteByte(ch)
			i++
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			out.WriteByte(ch)
			i++
		case '@':
			if inSingleQuote || inDoubleQuote {
				out.WriteByte(ch)
				i++
				continue
			}
			end := i + 1
			for end < len(query) && isParameterNameByte(query[end]) {
				end++
			}
			if end == i+1 {
				out.WriteByte(ch)
				i++
				continue
			}
			name := query[i+1 : end]
			value, ok := replacements[name]
			if !ok {
				return "", nil, fmt.Errorf("query parameter %q was not provided", name)
			}
			used[name] = true
			out.WriteString(value)
			i = end
		default:
			out.WriteByte(ch)
			i++
		}
	}
	return out.String(), used, nil
}

func isParameterNameByte(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_'
}

func parameterSQLLiteral(parameter queryParameter) (string, error) {
	value := parameter.ParameterValue.Value
	fieldType := strings.ToUpper(defaultString(parameter.ParameterType.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON":
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	case "INTEGER", "INT64":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("query parameter %q must be an integer", parameter.Name)
		}
		return value, nil
	case "FLOAT", "FLOAT64":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("query parameter %q must be a number", parameter.Name)
		}
		return value, nil
	case "BOOLEAN", "BOOL":
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return "", fmt.Errorf("query parameter %q must be a boolean", parameter.Name)
		}
		if parsed {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("unsupported query parameter type %q", parameter.ParameterType.Type)
	}
}

func parseAggregateSelection(selected string) (aggregateSelection, bool, error) {
	expr := strings.TrimSpace(selected)
	if strings.Contains(expr, ",") {
		return aggregateSelection{}, false, nil
	}
	alias := ""
	for _, marker := range []string{" AS ", " as "} {
		if left, right, ok := strings.Cut(expr, marker); ok {
			expr = strings.TrimSpace(left)
			alias = strings.Trim(strings.TrimSpace(right), "`")
			if alias == "" {
				return aggregateSelection{}, true, fmt.Errorf("aggregate alias is empty")
			}
			break
		}
	}
	upper := strings.ToUpper(expr)
	function := ""
	for _, candidate := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX"} {
		if strings.HasPrefix(upper, candidate+"(") {
			function = candidate
			break
		}
	}
	if function == "" || !strings.HasSuffix(expr, ")") {
		if strings.Contains(upper, "(") || strings.Contains(upper, ")") {
			return aggregateSelection{}, true, fmt.Errorf("unsupported aggregate expression")
		}
		return aggregateSelection{}, false, nil
	}
	field := strings.TrimSpace(expr[len(function)+1 : len(expr)-1])
	field = strings.Trim(field, "`")
	if field == "" {
		return aggregateSelection{}, true, fmt.Errorf("%s requires a field or *", function)
	}
	if field == "*" && function != "COUNT" {
		return aggregateSelection{}, true, fmt.Errorf("%s requires a field", function)
	}
	return aggregateSelection{
		Function: function,
		Field:    field,
		Alias:    alias,
	}, true, nil
}

func parseGroupedAggregateSelection(selected string) (string, aggregateSelection, bool, error) {
	parts := strings.Split(selected, ",")
	if len(parts) != 2 {
		return "", aggregateSelection{}, false, nil
	}
	var groupField string
	var aggregate aggregateSelection
	aggregateSeen := false
	groupFieldCount := 0
	for _, part := range parts {
		expr := strings.TrimSpace(part)
		parsedAggregate, ok, err := parseAggregateSelection(expr)
		if err != nil {
			return "", aggregateSelection{}, true, err
		}
		if ok {
			if aggregate.Function != "" {
				return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one aggregate expression")
			}
			aggregate = parsedAggregate
			aggregateSeen = true
			continue
		}
		if strings.ContainsAny(expr, "()") {
			return "", aggregateSelection{}, true, fmt.Errorf("unsupported grouped SELECT expression")
		}
		if groupFieldCount > 0 && aggregateSeen {
			return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one selected field")
		}
		groupField = strings.Trim(expr, "`")
		groupFieldCount++
	}
	if !aggregateSeen {
		return "", aggregateSelection{}, false, nil
	}
	if groupFieldCount != 1 {
		return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one selected field")
	}
	if groupField == "" || aggregate.Function == "" {
		return "", aggregateSelection{}, false, nil
	}
	return groupField, aggregate, true, nil
}

func aggregateField(schema tableSchema, aggregate aggregateSelection) (tableFieldSchema, bool) {
	if aggregate.Field == "*" {
		return tableFieldSchema{}, false
	}
	for _, field := range schema.Fields {
		if field.Name == aggregate.Field {
			return field, true
		}
	}
	return tableFieldSchema{}, false
}

func aggregateDryRunFields(schema tableSchema, aggregate aggregateSelection) ([]tableFieldSchema, error) {
	fieldName := aggregate.Alias
	if fieldName == "" {
		fieldName = "f0_"
	}
	if aggregate.Field == "*" {
		if aggregate.Function != "COUNT" {
			return nil, fmt.Errorf("%s requires a field", aggregate.Function)
		}
		return []tableFieldSchema{{Name: fieldName, Type: "INTEGER", Mode: "NULLABLE"}}, nil
	}
	field, ok := aggregateField(schema, aggregate)
	if !ok {
		return nil, fmt.Errorf("aggregate field %q does not exist", aggregate.Field)
	}
	switch aggregate.Function {
	case "COUNT":
		return []tableFieldSchema{{Name: fieldName, Type: "INTEGER", Mode: "NULLABLE"}}, nil
	case "SUM":
		if !isNumericField(field) {
			return nil, fmt.Errorf("SUM requires a numeric field")
		}
		return []tableFieldSchema{{Name: fieldName, Type: aggregateNumericType(field), Mode: "NULLABLE"}}, nil
	case "AVG":
		if !isNumericField(field) {
			return nil, fmt.Errorf("AVG requires a numeric field")
		}
		return []tableFieldSchema{{Name: fieldName, Type: "FLOAT", Mode: "NULLABLE"}}, nil
	case "MIN", "MAX":
		return []tableFieldSchema{{Name: fieldName, Type: defaultString(field.Type, "STRING"), Mode: "NULLABLE"}}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function")
	}
}

func groupedAggregateDryRunFields(schema tableSchema, query simpleSelectQuery) ([]tableFieldSchema, error) {
	groupFields, err := fieldsForQuery(schema, []string{query.GroupBy})
	if err != nil {
		return nil, err
	}
	aggregateFields, err := aggregateDryRunFields(schema, query.Aggregate)
	if err != nil {
		return nil, err
	}
	return append(groupFields, aggregateFields...), nil
}

func singleAggregateResult(name string, fieldType string, value string) queryExecutionResult {
	data, _ := json.Marshal(value)
	return queryExecutionResult{
		Fields: []tableFieldSchema{{
			Name: name,
			Type: fieldType,
			Mode: "NULLABLE",
		}},
		Rows: []tableDataRow{{
			F: []tableCell{{V: rawValueForResponse(data)}},
		}},
	}
}

func singleAggregateNullResult(name string, fieldType string) queryExecutionResult {
	return queryExecutionResult{
		Fields: []tableFieldSchema{{
			Name: name,
			Type: fieldType,
			Mode: "NULLABLE",
		}},
		Rows: []tableDataRow{{
			F: []tableCell{{V: nil}},
		}},
	}
}

func sumAggregate(rows []storedRow, fieldName string, integer bool) (string, int, error) {
	if integer {
		var sum int64
		count := 0
		for _, row := range rows {
			raw, ok := row.JSON[fieldName]
			if !ok || isJSONNull(raw) {
				continue
			}
			value, ok := rawInt(raw)
			if !ok {
				return "", 0, fmt.Errorf("SUM field contains a non-integer value")
			}
			sum += value
			count++
		}
		return strconv.FormatInt(sum, 10), count, nil
	}
	sum, count, err := floatAggregate(rows, fieldName)
	if err != nil {
		return "", 0, err
	}
	return strconv.FormatFloat(sum, 'f', -1, 64), count, nil
}

func floatAggregate(rows []storedRow, fieldName string) (float64, int, error) {
	var sum float64
	count := 0
	for _, row := range rows {
		raw, ok := row.JSON[fieldName]
		if !ok || isJSONNull(raw) {
			continue
		}
		value, ok := rawFloat(raw)
		if !ok {
			return 0, 0, fmt.Errorf("aggregate field contains a non-numeric value")
		}
		sum += value
		count++
	}
	return sum, count, nil
}

func minMaxAggregate(rows []storedRow, fieldName string, function string) (json.RawMessage, bool) {
	var selected json.RawMessage
	found := false
	for _, row := range rows {
		raw, ok := row.JSON[fieldName]
		if !ok || isJSONNull(raw) {
			continue
		}
		if !found {
			selected = raw
			found = true
			continue
		}
		cmp := compareRawValues(raw, selected)
		if function == "MIN" && cmp < 0 || function == "MAX" && cmp > 0 {
			selected = raw
		}
	}
	return selected, found
}

func isNumericField(field tableFieldSchema) bool {
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
		return true
	default:
		return false
	}
}

func isIntegerField(field tableFieldSchema) bool {
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	return fieldType == "INTEGER" || fieldType == "INT64"
}

func aggregateNumericType(field tableFieldSchema) string {
	if isIntegerField(field) {
		return "INTEGER"
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "FLOAT"))
	if fieldType == "FLOAT64" {
		return "FLOAT"
	}
	return fieldType
}

func nextQueryToken(value string) (string, string) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "`") {
		end := strings.Index(value[1:], "`")
		if end >= 0 {
			tokenEnd := end + 2
			return value[:tokenEnd], strings.TrimSpace(value[tokenEnd:])
		}
	}
	index := strings.IndexFunc(value, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
	if index < 0 {
		return value, ""
	}
	return value[:index], strings.TrimSpace(value[index:])
}

func parseTableIdentifier(identifier string, defaultProjectID string) (string, string, string, error) {
	trimmed := strings.Trim(strings.TrimSpace(identifier), "`")
	parts := strings.Split(trimmed, ".")
	if len(parts) == 2 {
		return defaultString(defaultProjectID, "devcloud"), parts[0], parts[1], nil
	}
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2], nil
	}
	return "", "", "", fmt.Errorf("FROM table must be dataset.table or project.dataset.table")
}

func copySourceTables(config copyJobConfiguration) ([]tableReference, error) {
	if len(config.SourceTables) > 0 {
		if config.SourceTable.TableID != "" {
			return nil, fmt.Errorf("copy job supports sourceTable or sourceTables, not both")
		}
		return config.SourceTables, nil
	}
	if config.SourceTable.TableID == "" {
		return nil, fmt.Errorf("configuration.copy.sourceTable is required")
	}
	return []tableReference{config.SourceTable}, nil
}

func normalizeTableReference(ref tableReference, defaultProjectID string) tableReference {
	if ref.ProjectID == "" {
		ref.ProjectID = defaultString(defaultProjectID, "devcloud")
	}
	return ref
}

func validateTableReference(ref tableReference) error {
	if err := validateResourceID(ref.ProjectID, "project"); err != nil {
		return err
	}
	if err := validateResourceID(ref.DatasetID, "dataset"); err != nil {
		return err
	}
	return validateResourceID(ref.TableID, "table")
}

func parseSelectedFields(selected string) []string {
	if strings.TrimSpace(selected) == "*" {
		return []string{"*"}
	}
	parts := strings.Split(selected, ",")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		field := strings.Trim(strings.TrimSpace(part), "`")
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func parseSimpleConditions(condition string) ([]whereCondition, error) {
	parts := splitANDConditions(condition)
	if len(parts) == 0 {
		return nil, fmt.Errorf("WHERE condition is required")
	}
	conditions := make([]whereCondition, 0, len(parts))
	for _, part := range parts {
		field, op, value, err := parseSimpleCondition(part)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, whereCondition{
			Field:    field,
			Operator: op,
			ValueRaw: value,
		})
	}
	return conditions, nil
}

func parseSimpleConditionGroups(condition string) ([][]whereCondition, error) {
	groups := splitORConditionGroups(condition)
	if len(groups) == 0 {
		return nil, fmt.Errorf("WHERE condition is required")
	}
	conditionGroups := make([][]whereCondition, 0, len(groups))
	for _, group := range groups {
		conditions, err := parseSimpleConditions(group)
		if err != nil {
			return nil, err
		}
		conditionGroups = append(conditionGroups, conditions)
	}
	return conditionGroups, nil
}

func flattenWhereConditionGroups(groups [][]whereCondition) []whereCondition {
	size := 0
	for _, group := range groups {
		size += len(group)
	}
	conditions := make([]whereCondition, 0, size)
	for _, group := range groups {
		conditions = append(conditions, group...)
	}
	return conditions
}

func splitORConditionGroups(condition string) []string {
	parts := strings.Fields(condition)
	if len(parts) == 0 {
		return nil
	}
	groups := make([]string, 0, 1)
	var current []string
	for _, part := range parts {
		if strings.EqualFold(part, "OR") {
			if len(current) == 0 {
				return nil
			}
			groups = append(groups, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, part)
	}
	if len(current) == 0 {
		return nil
	}
	groups = append(groups, strings.Join(current, " "))
	return groups
}

func splitANDConditions(condition string) []string {
	parts := strings.Fields(condition)
	if len(parts) == 0 {
		return nil
	}
	conditions := make([]string, 0, 1)
	var current []string
	for _, part := range parts {
		if strings.EqualFold(part, "AND") {
			if len(current) == 0 {
				return nil
			}
			conditions = append(conditions, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, part)
	}
	if len(current) == 0 {
		return nil
	}
	conditions = append(conditions, strings.Join(current, " "))
	return conditions
}

func parseSimpleCondition(condition string) (string, string, json.RawMessage, error) {
	negated := false
	trimmedCondition := strings.TrimSpace(condition)
	if strings.HasPrefix(strings.ToUpper(trimmedCondition), "NOT ") {
		negated = true
		trimmedCondition = strings.TrimSpace(trimmedCondition[len("NOT "):])
	}
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(trimmedCondition, op); idx >= 0 {
			field := strings.Trim(strings.TrimSpace(trimmedCondition[:idx]), "`")
			value := strings.TrimSpace(trimmedCondition[idx+len(op):])
			if field == "" || value == "" {
				return "", "", nil, fmt.Errorf("WHERE condition must compare a field to a literal")
			}
			raw, err := rawJSONLiteral(value)
			if err != nil {
				return "", "", nil, err
			}
			if negated {
				op = "NOT " + op
			}
			return field, op, raw, nil
		}
	}
	return "", "", nil, fmt.Errorf("WHERE supports simple comparisons only")
}

func rawJSONLiteral(value string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") && len(trimmed) >= 2 {
		data, _ := json.Marshal(strings.Trim(trimmed, "'"))
		return data, nil
	}
	if strings.HasPrefix(trimmed, `"`) && strings.HasSuffix(trimmed, `"`) {
		var s string
		if err := json.Unmarshal([]byte(trimmed), &s); err != nil {
			return nil, fmt.Errorf("invalid string literal")
		}
		return []byte(trimmed), nil
	}
	var valueAny any
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&valueAny); err != nil {
		return nil, fmt.Errorf("WHERE literal must be a number, boolean, null, or quoted string")
	}
	return []byte(trimmed), nil
}

func fieldsForQuery(schema tableSchema, selected []string) ([]tableFieldSchema, error) {
	if len(selected) == 1 && selected[0] == "*" {
		return schema.Fields, nil
	}
	byName := make(map[string]tableFieldSchema, len(schema.Fields))
	for _, field := range schema.Fields {
		byName[field.Name] = field
	}
	fields := make([]tableFieldSchema, 0, len(selected))
	for _, name := range selected {
		field, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("selected field %q does not exist", name)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func rowMatchesQuery(row storedRow, query simpleSelectQuery) (bool, error) {
	if len(query.WhereConditionGroups) == 0 {
		if len(query.WhereConditions) > 0 {
			query.WhereConditionGroups = [][]whereCondition{query.WhereConditions}
		} else if query.WhereOperator != "" {
			query.WhereConditionGroups = [][]whereCondition{{
				{
					Field:    query.WhereField,
					Operator: query.WhereOperator,
					ValueRaw: query.WhereValueRaw,
				},
			}}
		}
	}
	if len(query.WhereConditionGroups) == 0 {
		return true, nil
	}
	for _, group := range query.WhereConditionGroups {
		matches, err := rowMatchesAllConditions(row, group)
		if err != nil {
			return false, err
		}
		if matches {
			return true, nil
		}
	}
	return false, nil
}

func rowMatchesAllConditions(row storedRow, conditions []whereCondition) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}
	for _, condition := range conditions {
		raw, ok := row.JSON[condition.Field]
		if !ok || isJSONNull(raw) {
			return false, nil
		}
		cmp := compareRawValues(raw, condition.ValueRaw)
		var matches bool
		switch condition.Operator {
		case "=":
			matches = cmp == 0
		case "NOT =":
			matches = cmp != 0
		case "!=":
			matches = cmp != 0
		case "NOT !=":
			matches = cmp == 0
		case ">":
			matches = cmp > 0
		case "NOT >":
			matches = cmp <= 0
		case ">=":
			matches = cmp >= 0
		case "NOT >=":
			matches = cmp < 0
		case "<":
			matches = cmp < 0
		case "NOT <":
			matches = cmp >= 0
		case "<=":
			matches = cmp <= 0
		case "NOT <=":
			matches = cmp > 0
		default:
			return false, fmt.Errorf("unsupported WHERE operator")
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

func compareRawValues(left json.RawMessage, right json.RawMessage) int {
	leftNumber, leftNumberOK := rawFloat(left)
	rightNumber, rightNumberOK := rawFloat(right)
	if leftNumberOK && rightNumberOK {
		switch {
		case leftNumber < rightNumber:
			return -1
		case leftNumber > rightNumber:
			return 1
		default:
			return 0
		}
	}
	leftString := fmt.Sprint(rawValueForResponse(left))
	rightString := fmt.Sprint(rawValueForResponse(right))
	return strings.Compare(leftString, rightString)
}

func rawFloat(raw json.RawMessage) (float64, bool) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		value, err := strconv.ParseFloat(asString, 64)
		return value, err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return 0, false
	}
	value, err := strconv.ParseFloat(number.String(), 64)
	return value, err == nil
}

func rawInt(raw json.RawMessage) (int64, bool) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		value, err := strconv.ParseInt(asString, 10, 64)
		return value, err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return 0, false
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	return value, err == nil
}

func validateTableSchema(schema tableSchema) error {
	for _, field := range schema.Fields {
		if err := validateTableField(field); err != nil {
			return err
		}
	}
	return nil
}

func validateTableField(field tableFieldSchema) error {
	if err := validateResourceID(field.Name, "field"); err != nil {
		return err
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON", "RECORD", "STRUCT":
	default:
		return fmt.Errorf("unsupported field type %q", field.Type)
	}
	mode := strings.ToUpper(defaultString(field.Mode, "NULLABLE"))
	switch mode {
	case "NULLABLE", "REQUIRED", "REPEATED":
	default:
		return fmt.Errorf("unsupported field mode %q", field.Mode)
	}
	if fieldType == "RECORD" || fieldType == "STRUCT" {
		if len(field.Fields) == 0 {
			return fmt.Errorf("record field %q requires nested fields", field.Name)
		}
		for _, nested := range field.Fields {
			if err := validateTableField(nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRowJSON(row map[string]json.RawMessage, schema tableSchema, ignoreUnknownValues bool) (map[string]json.RawMessage, []insertErrorItem) {
	errors := make([]insertErrorItem, 0)
	values := make(map[string]json.RawMessage)
	fieldsByName := make(map[string]tableFieldSchema, len(schema.Fields))
	for _, field := range schema.Fields {
		fieldsByName[field.Name] = field
	}
	for key, raw := range row {
		field, ok := fieldsByName[key]
		if !ok {
			if ignoreUnknownValues {
				continue
			}
			errors = append(errors, insertErrorItem{
				Reason:   "invalid",
				Location: key,
				Message:  fmt.Sprintf("no such field: %s", key),
			})
			continue
		}
		if err := validateFieldValue(raw, field); err != nil {
			errors = append(errors, insertErrorItem{
				Reason:   "invalid",
				Location: key,
				Message:  err.Error(),
			})
			continue
		}
		values[key] = raw
	}
	for _, field := range schema.Fields {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			raw, ok := values[field.Name]
			if !ok || isJSONNull(raw) {
				errors = append(errors, insertErrorItem{
					Reason:   "invalid",
					Location: field.Name,
					Message:  fmt.Sprintf("required field %q is missing", field.Name),
				})
			}
		}
	}
	return values, errors
}

func validateFieldValue(raw json.RawMessage, field tableFieldSchema) error {
	if isJSONNull(raw) {
		if strings.EqualFold(field.Mode, "REQUIRED") {
			return fmt.Errorf("required field %q cannot be null", field.Name)
		}
		return nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return fmt.Errorf("field %q must be an array", field.Name)
		}
		itemField := field
		itemField.Mode = "NULLABLE"
		for _, value := range values {
			if err := validateFieldValue(value, itemField); err != nil {
				return err
			}
		}
		return nil
	}

	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("field %q must be a string", field.Name)
		}
	case "INTEGER", "INT64":
		if !isIntegerJSON(raw) {
			return fmt.Errorf("field %q must be an integer", field.Name)
		}
	case "FLOAT", "FLOAT64":
		if !isFloatJSON(raw) {
			return fmt.Errorf("field %q must be a number", field.Name)
		}
	case "BOOLEAN", "BOOL":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("field %q must be a boolean", field.Name)
		}
	case "JSON":
		return nil
	case "RECORD", "STRUCT":
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return fmt.Errorf("field %q must be an object", field.Name)
		}
		_, errors := validateRowJSON(object, tableSchema{Fields: field.Fields}, false)
		if len(errors) > 0 {
			return fmt.Errorf("field %q has invalid nested values", field.Name)
		}
	default:
		return fmt.Errorf("unsupported field type %q", field.Type)
	}
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.EqualFold(strings.TrimSpace(string(raw)), "null")
}

func isIntegerJSON(raw json.RawMessage) bool {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		_, err := strconv.ParseInt(asString, 10, 64)
		return err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return false
	}
	_, err := strconv.ParseInt(number.String(), 10, 64)
	return err == nil
}

func isFloatJSON(raw json.RawMessage) bool {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		_, err := strconv.ParseFloat(asString, 64)
		return err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return false
	}
	_, err := strconv.ParseFloat(number.String(), 64)
	return err == nil
}

func rowOffsetFromRequest(r *http.Request) (int, error) {
	token := strings.TrimSpace(r.URL.Query().Get("pageToken"))
	if token == "" {
		token = strings.TrimSpace(r.URL.Query().Get("startIndex"))
	}
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid page token")
	}
	return offset, nil
}

func maxResultsFromRequest(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("maxResults"))
	if raw == "" {
		return 100, nil
	}
	maxResults, err := strconv.Atoi(raw)
	if err != nil || maxResults <= 0 {
		return 0, fmt.Errorf("maxResults must be positive")
	}
	if maxResults > 10000 {
		return 10000, nil
	}
	return maxResults, nil
}

func selectedRowFields(schema tableSchema, selectedFields string) []tableFieldSchema {
	if strings.TrimSpace(selectedFields) == "" {
		return schema.Fields
	}
	allowed := make(map[string]bool)
	for _, field := range strings.Split(selectedFields, ",") {
		name := strings.TrimSpace(field)
		if name != "" {
			allowed[name] = true
		}
	}
	fields := make([]tableFieldSchema, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if allowed[field.Name] {
			fields = append(fields, field)
		}
	}
	return fields
}

func formatRowValues(row map[string]json.RawMessage, fields []tableFieldSchema) []tableCell {
	values := make([]tableCell, 0, len(fields))
	for _, field := range fields {
		raw, ok := row[field.Name]
		if !ok || isJSONNull(raw) {
			values = append(values, tableCell{V: nil})
			continue
		}
		values = append(values, tableCell{V: rawValueForResponse(raw)})
	}
	return values
}

func rawValueForResponse(raw json.RawMessage) any {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return string(raw)
	}
	return value
}

func hasChildren(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) > 0
}

func datasetETag(t time.Time) string {
	return fmt.Sprintf("\"%d\"", t.UnixNano())
}

func unixMillisString(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixMilli())
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func boolPtr(value bool) *bool {
	return &value
}

func defaultIAMPolicy() iamPolicy {
	return iamPolicy{
		Version:  1,
		ETag:     datasetETag(time.Unix(0, 0).UTC()),
		Bindings: []iamBinding{},
	}
}

func normalizeIAMPolicy(policy iamPolicy) iamPolicy {
	if policy.Version == 0 {
		policy.Version = 1
	}
	if policy.ETag == "" {
		policy.ETag = datasetETag(time.Now().UTC())
	}
	if policy.Bindings == nil {
		policy.Bindings = []iamBinding{}
	}
	return policy
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, reason string, message string) {
	writeJSON(w, status, errorResponse{
		Error: errorBody{
			Code:    status,
			Message: message,
			Errors: []errorItem{{
				Domain:  "global",
				Reason:  reason,
				Message: message,
			}},
			Status: statusText(status),
		},
	})
}

func statusText(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ALREADY_EXISTS"
	case http.StatusMethodNotAllowed:
		return "METHOD_NOT_ALLOWED"
	default:
		return strings.ToUpper(strings.ReplaceAll(http.StatusText(status), " ", "_"))
	}
}

type projectsListResponse struct {
	Kind       string            `json:"kind"`
	Projects   []projectListItem `json:"projects"`
	TotalItems int               `json:"totalItems"`
}

type projectListItem struct {
	Kind         string           `json:"kind"`
	ID           string           `json:"id"`
	NumericID    string           `json:"numericId"`
	ProjectRef   projectReference `json:"projectReference"`
	FriendlyName string           `json:"friendlyName"`
}

type projectReference struct {
	ProjectID string `json:"projectId"`
}

type datasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

type tableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

type datasetResource struct {
	Kind             string            `json:"kind,omitempty"`
	ID               string            `json:"id,omitempty"`
	SelfLink         string            `json:"selfLink,omitempty"`
	DatasetReference datasetReference  `json:"datasetReference"`
	Location         string            `json:"location,omitempty"`
	FriendlyName     string            `json:"friendlyName,omitempty"`
	Description      string            `json:"description,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	ETag             string            `json:"etag,omitempty"`
	CreationTime     string            `json:"creationTime,omitempty"`
	LastModifiedTime string            `json:"lastModifiedTime,omitempty"`
}

type datasetsListResponse struct {
	Kind          string            `json:"kind"`
	Datasets      []datasetListItem `json:"datasets,omitempty"`
	TotalItems    int               `json:"totalItems"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

type datasetListItem struct {
	Kind             string           `json:"kind"`
	ID               string           `json:"id"`
	DatasetReference datasetReference `json:"datasetReference"`
	Location         string           `json:"location,omitempty"`
	FriendlyName     string           `json:"friendlyName,omitempty"`
}

type tableResource struct {
	Kind              string             `json:"kind,omitempty"`
	ID                string             `json:"id,omitempty"`
	SelfLink          string             `json:"selfLink,omitempty"`
	TableReference    tableReference     `json:"tableReference"`
	Type              string             `json:"type,omitempty"`
	Schema            tableSchema        `json:"schema,omitempty"`
	FriendlyName      string             `json:"friendlyName,omitempty"`
	Description       string             `json:"description,omitempty"`
	Labels            map[string]string  `json:"labels,omitempty"`
	TimePartitioning  *timePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *rangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *clustering        `json:"clustering,omitempty"`
	ETag              string             `json:"etag,omitempty"`
	CreationTime      string             `json:"creationTime,omitempty"`
	LastModifiedTime  string             `json:"lastModifiedTime,omitempty"`
	NumRows           string             `json:"numRows,omitempty"`
	NumBytes          string             `json:"numBytes,omitempty"`
	Location          string             `json:"location,omitempty"`
}

type timePartitioning struct {
	Type          string `json:"type,omitempty"`
	Field         string `json:"field,omitempty"`
	ExpirationMS  string `json:"expirationMs,omitempty"`
	RequireFilter bool   `json:"requirePartitionFilter,omitempty"`
}

type rangePartitioning struct {
	Field string         `json:"field,omitempty"`
	Range partitionRange `json:"range,omitempty"`
}

type partitionRange struct {
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
	Interval string `json:"interval,omitempty"`
}

type clustering struct {
	Fields []string `json:"fields,omitempty"`
}

type tableSchema struct {
	Fields []tableFieldSchema `json:"fields,omitempty"`
}

type tableFieldSchema struct {
	Name        string             `json:"name"`
	Type        string             `json:"type,omitempty"`
	Mode        string             `json:"mode,omitempty"`
	Description string             `json:"description,omitempty"`
	Fields      []tableFieldSchema `json:"fields,omitempty"`
}

type tablesListResponse struct {
	Kind          string          `json:"kind"`
	Tables        []tableListItem `json:"tables,omitempty"`
	TotalItems    int             `json:"totalItems"`
	NextPageToken string          `json:"nextPageToken,omitempty"`
}

type tableListItem struct {
	Kind              string             `json:"kind"`
	ID                string             `json:"id"`
	TableReference    tableReference     `json:"tableReference"`
	Type              string             `json:"type,omitempty"`
	FriendlyName      string             `json:"friendlyName,omitempty"`
	TimePartitioning  *timePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *rangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *clustering        `json:"clustering,omitempty"`
}

type insertAllRequest struct {
	SkipInvalidRows     bool           `json:"skipInvalidRows"`
	IgnoreUnknownValues bool           `json:"ignoreUnknownValues"`
	Rows                []insertAllRow `json:"rows"`
}

type insertAllRow struct {
	InsertID string                     `json:"insertId,omitempty"`
	JSON     map[string]json.RawMessage `json:"json"`
}

type insertAllResponse struct {
	Kind         string        `json:"kind"`
	InsertErrors []insertError `json:"insertErrors,omitempty"`
}

type insertError struct {
	Index  int               `json:"index"`
	Errors []insertErrorItem `json:"errors"`
}

type insertErrorItem struct {
	Reason   string `json:"reason"`
	Location string `json:"location,omitempty"`
	Message  string `json:"message"`
}

type storedRow struct {
	InsertID   string                     `json:"insertId,omitempty"`
	JSON       map[string]json.RawMessage `json:"json"`
	InsertedAt string                     `json:"insertedAt"`
}

type tableDataListResponse struct {
	Kind      string         `json:"kind"`
	ETag      string         `json:"etag,omitempty"`
	TotalRows string         `json:"totalRows"`
	PageToken string         `json:"pageToken,omitempty"`
	Rows      []tableDataRow `json:"rows,omitempty"`
}

type tableDataRow struct {
	F []tableCell `json:"f"`
}

type tableCell struct {
	V any `json:"v"`
}

type serviceAccountResponse struct {
	Kind  string `json:"kind"`
	Email string `json:"email"`
}

type queryRequest struct {
	Query           string           `json:"query"`
	UseLegacySQL    *bool            `json:"useLegacySql,omitempty"`
	Location        string           `json:"location,omitempty"`
	MaxResults      int              `json:"maxResults,omitempty"`
	DryRun          bool             `json:"dryRun,omitempty"`
	QueryParameters []queryParameter `json:"queryParameters,omitempty"`
}

type jobInsertRequest struct {
	JobReference  jobReference     `json:"jobReference,omitempty"`
	Configuration jobConfiguration `json:"configuration"`
}

type setIAMPolicyRequest struct {
	Policy iamPolicy `json:"policy"`
}

type testIAMPermissionsRequest struct {
	Permissions []string `json:"permissions,omitempty"`
}

type testIAMPermissionsResponse struct {
	Permissions []string `json:"permissions,omitempty"`
}

type iamPolicy struct {
	Version  int          `json:"version,omitempty"`
	ETag     string       `json:"etag,omitempty"`
	Bindings []iamBinding `json:"bindings,omitempty"`
}

type iamBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members,omitempty"`
}

type jobConfiguration struct {
	DryRun  bool                    `json:"dryRun,omitempty"`
	Query   queryJobConfiguration   `json:"query,omitempty"`
	Copy    copyJobConfiguration    `json:"copy,omitempty"`
	Load    loadJobConfiguration    `json:"load,omitempty"`
	Extract extractJobConfiguration `json:"extract,omitempty"`
}

type queryJobConfiguration struct {
	Query           string           `json:"query,omitempty"`
	UseLegacySQL    *bool            `json:"useLegacySql,omitempty"`
	QueryParameters []queryParameter `json:"queryParameters,omitempty"`
}

type queryParameter struct {
	Name           string              `json:"name,omitempty"`
	ParameterType  queryParameterType  `json:"parameterType"`
	ParameterValue queryParameterValue `json:"parameterValue"`
}

type queryParameterType struct {
	Type string `json:"type"`
}

type queryParameterValue struct {
	Value string `json:"value,omitempty"`
}

type copyJobConfiguration struct {
	SourceTable       tableReference   `json:"sourceTable,omitempty"`
	SourceTables      []tableReference `json:"sourceTables,omitempty"`
	DestinationTable  tableReference   `json:"destinationTable,omitempty"`
	CreateDisposition string           `json:"createDisposition,omitempty"`
	WriteDisposition  string           `json:"writeDisposition,omitempty"`
}

type loadJobConfiguration struct {
	SourceURIs        []string       `json:"sourceUris,omitempty"`
	DestinationTable  tableReference `json:"destinationTable,omitempty"`
	Schema            tableSchema    `json:"schema,omitempty"`
	SourceFormat      string         `json:"sourceFormat,omitempty"`
	SkipLeadingRows   int            `json:"skipLeadingRows,omitempty"`
	CreateDisposition string         `json:"createDisposition,omitempty"`
	WriteDisposition  string         `json:"writeDisposition,omitempty"`
}

type extractJobConfiguration struct {
	SourceTable       tableReference `json:"sourceTable,omitempty"`
	DestinationURIs   []string       `json:"destinationUris,omitempty"`
	DestinationFormat string         `json:"destinationFormat,omitempty"`
}

type queryResponse struct {
	Kind         string         `json:"kind"`
	Schema       tableSchema    `json:"schema,omitempty"`
	JobReference jobReference   `json:"jobReference"`
	TotalRows    string         `json:"totalRows"`
	PageToken    string         `json:"pageToken,omitempty"`
	Rows         []tableDataRow `json:"rows,omitempty"`
	JobComplete  bool           `json:"jobComplete"`
	CacheHit     bool           `json:"cacheHit"`
}

type jobsListResponse struct {
	Kind          string        `json:"kind"`
	Jobs          []jobResource `json:"jobs,omitempty"`
	NextPageToken string        `json:"nextPageToken,omitempty"`
}

type jobCancelResponse struct {
	Kind string      `json:"kind"`
	Job  jobResource `json:"job"`
}

type jobReference struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

type jobResource struct {
	Kind          string           `json:"kind"`
	ID            string           `json:"id"`
	SelfLink      string           `json:"selfLink"`
	JobReference  jobReference     `json:"jobReference"`
	Configuration jobConfiguration `json:"configuration,omitempty"`
	Status        jobStatus        `json:"status"`
	Statistics    jobStatistics    `json:"statistics,omitempty"`
}

type jobStatus struct {
	State string `json:"state"`
}

type jobStatistics struct {
	CreationTime string          `json:"creationTime,omitempty"`
	StartTime    string          `json:"startTime,omitempty"`
	EndTime      string          `json:"endTime,omitempty"`
	Query        queryStatistics `json:"query,omitempty"`
}

type queryStatistics struct {
	TotalRows string `json:"totalRows,omitempty"`
	CacheHit  bool   `json:"cacheHit"`
	DryRun    bool   `json:"dryRun,omitempty"`
}

type queryJobRecord struct {
	Job      jobResource   `json:"job"`
	Response queryResponse `json:"response"`
}

type queryExecutionResult struct {
	Fields []tableFieldSchema
	Rows   []tableDataRow
}

type simpleSelectQuery struct {
	ProjectID            string
	DatasetID            string
	TableID              string
	SelectedFields       []string
	Aggregate            aggregateSelection
	WhereConditions      []whereCondition
	WhereConditionGroups [][]whereCondition
	WhereField           string
	WhereOperator        string
	WhereValueRaw        json.RawMessage
	GroupBy              string
	OrderBy              string
	OrderDesc            bool
	Limit                int
	Offset               int
}

type aggregateSelection struct {
	Function string
	Field    string
	Alias    string
}

type whereCondition struct {
	Field    string
	Operator string
	ValueRaw json.RawMessage
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Errors  []errorItem `json:"errors"`
	Status  string      `json:"status"`
}

type errorItem struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}
