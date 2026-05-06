package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	if r.Method == http.MethodPost {
		s.handleSchemaCreate(w, r, project)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET, POST")
		return
	}
	schemaView, ok := parseSchemaView(w, r)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	schemas := make([]schemaResource, 0, len(s.schemas))
	for _, schema := range s.schemas {
		if resourceProject(schema.Name) == project {
			schemas = append(schemas, schema.public(schemaView))
		}
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Name < schemas[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(schemas), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"schemas": schemas[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSchemaCreate(w http.ResponseWriter, r *http.Request, project string) {
	schemaID := strings.TrimSpace(r.URL.Query().Get("schemaId"))
	if schemaID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schemaId is required")
		return
	}
	if !validResourceID(schemaID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
		return
	}
	name := schemaName(project, schemaID)
	var request schemaResource
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.Name != "" && request.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name does not match request path")
		return
	}
	if request.Type != "" && !validSchemaType(request.Type) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
		return
	}
	if err := validateSchemaDefinition(request.Type, request.Definition); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	request.Name = name
	if request.RevisionID == "" {
		request.RevisionID = "1"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.schemas[name]; exists {
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "schema already exists")
		return
	}
	s.schemas[name] = request
	if err := s.saveResourcesLocked(); err != nil {
		delete(s.schemas, name)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, request)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	project, schemaID, ok := schemaNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := schemaName(project, schemaID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(schemaID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
		return
	}
	switch r.Method {
	case http.MethodPut:
		var request schemaResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Name != "" && request.Name != name {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name does not match request path")
			return
		}
		if request.Type != "" && !validSchemaType(request.Type) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
			return
		}
		if err := validateSchemaDefinition(request.Type, request.Definition); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		request.Name = name
		if request.RevisionID == "" {
			request.RevisionID = "1"
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.schemas[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "schema already exists")
			return
		}
		s.schemas[name] = request
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.schemas, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, request)
	case http.MethodGet:
		schemaView, ok := parseSchemaView(w, r)
		if !ok {
			return
		}
		s.mu.Lock()
		schema, found := s.schemas[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		writeJSON(w, http.StatusOK, schema.public(schemaView))
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.schemas[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		delete(s.schemas, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (s *Server) handleSchemaValidateMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, ok := schemasValidateMessageParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		Name     string         `json:"name"`
		Schema   schemaResource `json:"schema"`
		Message  string         `json:"message"`
		Encoding string         `json:"encoding"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	hasInlineSchema := !emptySchemaResource(request.Schema)
	if request.Name == "" && !hasInlineSchema {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name or inline schema is required")
		return
	}
	if request.Name != "" && hasInlineSchema {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "only one of schema name or inline schema may be set")
		return
	}
	if request.Encoding != "" && !validSchemaEncoding(request.Encoding) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema encoding")
		return
	}
	if request.Message != "" {
		message, err := decodeBase64Bytes(request.Message)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message must be base64 encoded")
			return
		}
		if !validSchemaMessageData(message, request.Encoding) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message is invalid for schema encoding")
			return
		}
	}
	if request.Name != "" {
		if !validFullSchemaName(request.Name) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
			return
		}
		if resourceProject(request.Name) != project {
			writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "schema belongs to a different project")
			return
		}
		s.mu.Lock()
		_, found := s.schemas[request.Name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	if request.Schema.Name != "" {
		if !validFullSchemaName(request.Schema.Name) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
			return
		}
		if resourceProject(request.Schema.Name) != project {
			writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "schema belongs to a different project")
			return
		}
	}
	if !validSchemaType(request.Schema.Type) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
		return
	}
	if err := validateSchemaDefinition(request.Schema.Type, request.Schema.Definition); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func parseSchemaView(w http.ResponseWriter, r *http.Request) (string, bool) {
	view := strings.TrimSpace(r.URL.Query().Get("view"))
	switch view {
	case "", "FULL", "BASIC":
		return view, true
	default:
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema view")
		return "", false
	}
}

func schemaName(project string, schemaID string) string {
	return fmt.Sprintf("projects/%s/schemas/%s", project, schemaID)
}
