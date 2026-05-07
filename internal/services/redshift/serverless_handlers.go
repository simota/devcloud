package redshift

import "net/http"

func (s *Server) handleListServerlessNamespaces(w http.ResponseWriter, r *http.Request) {
	var request serverlessListRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"namespaces": []serverlessNamespace{s.serverlessNamespace()},
	})
}

func (s *Server) handleGetServerlessNamespace(w http.ResponseWriter, r *http.Request) {
	var request serverlessNamespaceRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	namespace := s.serverlessNamespace()
	if request.NamespaceName != "" && request.NamespaceName != namespace.NamespaceName {
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "namespace does not exist")
		return
	}
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"namespace": namespace})
}

func (s *Server) handleListServerlessWorkgroups(w http.ResponseWriter, r *http.Request) {
	var request serverlessListRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"workgroups": []serverlessWorkgroup{s.serverlessWorkgroup()},
	})
}

func (s *Server) handleGetServerlessWorkgroup(w http.ResponseWriter, r *http.Request) {
	var request serverlessWorkgroupRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	workgroup := s.serverlessWorkgroup()
	if request.WorkgroupName != "" && request.WorkgroupName != workgroup.WorkgroupName {
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "workgroup does not exist")
		return
	}
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"workgroup": workgroup})
}
