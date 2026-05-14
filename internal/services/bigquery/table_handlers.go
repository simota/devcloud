package bigquery

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"devcloud/internal/events"
)

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
		View:              request.View,
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
	events.Emit(s.eventPublisher, events.Event{
		Type:    "bigquery.table.created",
		Service: "bigquery",
		Payload: map[string]any{"project": projectID, "dataset": datasetID, "table": request.TableReference.TableID},
	})
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
			View:              table.View,
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
		existing.View = request.View
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
		if request.View != nil {
			existing.View = request.View
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
	events.Emit(s.eventPublisher, events.Event{
		Type:    "bigquery.table.deleted",
		Service: "bigquery",
		Payload: map[string]any{"project": projectID, "dataset": datasetID, "table": tableID},
	})
	w.WriteHeader(http.StatusNoContent)
}
