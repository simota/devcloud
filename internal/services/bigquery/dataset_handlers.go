package bigquery

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

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
		if hasChildren(filepath.Join(dir, "tables")) || hasChildren(filepath.Join(dir, "routines")) {
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
