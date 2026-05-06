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

func (s *Server) createRoutine(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	if _, found, err := s.readDataset(projectID, datasetID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}

	request, err := s.decodeRoutineRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.RoutineReference.ProjectID == "" {
		request.RoutineReference.ProjectID = projectID
	}
	if request.RoutineReference.DatasetID == "" {
		request.RoutineReference.DatasetID = datasetID
	}
	if request.RoutineReference.ProjectID != projectID || request.RoutineReference.DatasetID != datasetID {
		writeError(w, http.StatusBadRequest, "invalid", "routineReference must match request project and dataset")
		return
	}
	if err := validateRoutineResource(request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	path := s.routinePath(projectID, datasetID, request.RoutineReference.RoutineID)
	if _, err := os.Stat(path); err == nil {
		writeError(w, http.StatusConflict, "duplicate", fmt.Sprintf("Already Exists: Routine %s:%s.%s", projectID, datasetID, request.RoutineReference.RoutineID))
		return
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}

	now := time.Now().UTC()
	resource := request
	resource.Kind = "bigquery#routine"
	resource.ID = projectID + ":" + datasetID + "." + request.RoutineReference.RoutineID
	resource.SelfLink = s.routineSelfLink(projectID, datasetID, request.RoutineReference.RoutineID)
	resource.ETag = datasetETag(now)
	resource.CreationTime = unixMillisString(now)
	resource.LastModifiedTime = unixMillisString(now)
	if err := s.writeRoutine(resource); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) getRoutine(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, routineID string) {
	resource, found, err := s.readRoutine(projectID, datasetID, routineID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID))
		return
	}
	writeJSON(w, http.StatusOK, resource)
}

func (s *Server) listRoutines(w http.ResponseWriter, r *http.Request, projectID string, datasetID string) {
	if _, found, err := s.readDataset(projectID, datasetID); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
		return
	}
	routines, err := s.readRoutines(projectID, datasetID)
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
	if offset > len(routines) {
		offset = len(routines)
	}
	end := offset + maxResults
	if end > len(routines) {
		end = len(routines)
	}
	response := routinesListResponse{
		Kind:       "bigquery#routineList",
		Routines:   routines[offset:end],
		TotalItems: len(routines),
	}
	if end < len(routines) {
		response.NextPageToken = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) patchRoutine(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, routineID string, replace bool) {
	existing, found, err := s.readRoutine(projectID, datasetID, routineID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID))
		return
	}
	request, err := s.decodeRoutineRequest(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if request.RoutineReference.ProjectID != "" && request.RoutineReference.ProjectID != projectID ||
		request.RoutineReference.DatasetID != "" && request.RoutineReference.DatasetID != datasetID ||
		request.RoutineReference.RoutineID != "" && request.RoutineReference.RoutineID != routineID {
		writeError(w, http.StatusBadRequest, "invalid", "routineReference must match request routine")
		return
	}

	now := time.Now().UTC()
	if replace {
		creationTime := existing.CreationTime
		existing = request
		existing.Kind = "bigquery#routine"
		existing.ID = projectID + ":" + datasetID + "." + routineID
		existing.SelfLink = s.routineSelfLink(projectID, datasetID, routineID)
		existing.RoutineReference = routineReference{ProjectID: projectID, DatasetID: datasetID, RoutineID: routineID}
		existing.CreationTime = creationTime
	} else {
		if request.RoutineType != "" {
			existing.RoutineType = request.RoutineType
		}
		if request.Language != "" {
			existing.Language = request.Language
		}
		if request.Arguments != nil {
			existing.Arguments = request.Arguments
		}
		if request.ReturnType != nil {
			existing.ReturnType = request.ReturnType
		}
		if request.DefinitionBody != "" {
			existing.DefinitionBody = request.DefinitionBody
		}
		if request.Description != "" {
			existing.Description = request.Description
		}
		if request.DeterminismLevel != "" {
			existing.DeterminismLevel = request.DeterminismLevel
		}
		if request.ImportedLibraries != nil {
			existing.ImportedLibraries = request.ImportedLibraries
		}
	}
	if err := validateRoutineResource(existing); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	existing.ETag = datasetETag(now)
	existing.LastModifiedTime = unixMillisString(now)
	if err := s.writeRoutine(existing); err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) deleteRoutine(w http.ResponseWriter, r *http.Request, projectID string, datasetID string, routineID string) {
	dir := s.routineDir(projectID, datasetID, routineID)
	if _, err := os.Stat(filepath.Join(dir, "routine.json")); errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, "notFound", fmt.Sprintf("Not found: Routine %s:%s.%s", projectID, datasetID, routineID))
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
