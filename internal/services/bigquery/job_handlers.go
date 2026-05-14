package bigquery

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"devcloud/internal/events"
)

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
		job, err := s.createQueryJob(projectID, request.JobReference, request.Configuration.Query, 0, true, request.Configuration.DryRun, s.effectiveUseLegacySQL(request.Configuration.Query.UseLegacySQL))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalidQuery", err.Error())
			return
		}
		events.Emit(s.eventPublisher, events.Event{
			Type:    "bigquery.job.inserted",
			Service: "bigquery",
			Payload: map[string]any{"project": projectID, "jobType": "query"},
		})
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Copy.DestinationTable.TableID != "":
		job, err := s.createCopyJob(projectID, request.JobReference, request.Configuration.Copy)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		events.Emit(s.eventPublisher, events.Event{
			Type:    "bigquery.job.inserted",
			Service: "bigquery",
			Payload: map[string]any{"project": projectID, "jobType": "copy"},
		})
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Load.DestinationTable.TableID != "":
		job, err := s.createLoadJob(r.Context(), projectID, request.JobReference, request.Configuration.Load)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		events.Emit(s.eventPublisher, events.Event{
			Type:    "bigquery.job.inserted",
			Service: "bigquery",
			Payload: map[string]any{"project": projectID, "jobType": "load"},
		})
		writeJSON(w, http.StatusOK, job.Job)
	case request.Configuration.Extract.SourceTable.TableID != "":
		job, err := s.createExtractJob(r.Context(), projectID, request.JobReference, request.Configuration.Extract)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		events.Emit(s.eventPublisher, events.Event{
			Type:    "bigquery.job.inserted",
			Service: "bigquery",
			Payload: map[string]any{"project": projectID, "jobType": "extract"},
		})
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
