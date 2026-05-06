package bigquery

import (
	"fmt"
	"net/http"
)

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
