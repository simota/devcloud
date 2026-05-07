package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (s *Server) handleDynamoDBStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	tableCount := 0
	if s.dynamo != nil {
		snapshot := s.dynamo.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		tableCount = len(snapshot.Tables)
	}
	writeJSON(w, map[string]any{
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.DynamoDBEndpoint, "http://127.0.0.1:8000"),
		"region":      defaultString(s.config.DynamoDBRegion, "us-east-1"),
		"storagePath": defaultString(s.config.DynamoDBStoragePath, ".devcloud/data/dynamodb"),
		"tableCount":  tableCount,
	})
}

func (s *Server) handleDynamoDBTables(w http.ResponseWriter, r *http.Request) {
	if s.dynamo == nil {
		http.Error(w, "dynamodb service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.dynamo.Snapshot()
		writeJSON(w, map[string]any{
			"tables": snapshot.Tables,
		})
	case http.MethodPost:
		s.forwardDynamoDBDashboardOperation(w, r, "CreateTable", "")
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleDynamoDBTable(w http.ResponseWriter, r *http.Request) {
	if s.dynamo == nil {
		http.Error(w, "dynamodb service is disabled", http.StatusServiceUnavailable)
		return
	}
	tablePath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/dynamodb/tables/")
	escapedTable, suffix, hasSuffix := strings.Cut(tablePath, "/")
	tableName, err := url.PathUnescape(escapedTable)
	if err != nil {
		http.Error(w, "invalid table path", http.StatusBadRequest)
		return
	}
	if tableName == "" {
		http.NotFound(w, r)
		return
	}
	if hasSuffix {
		switch suffix {
		case "items":
			if r.Method == http.MethodPost {
				s.forwardDynamoDBDashboardOperation(w, r, "PutItem", tableName)
				return
			}
		case "items/update":
			s.forwardDynamoDBDashboardOperation(w, r, "UpdateItem", tableName)
			return
		case "items/delete":
			s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, "DeleteItem", tableName, tableName)
			return
		case "ttl":
			if r.Method == http.MethodPost {
				s.forwardDynamoDBDashboardOperation(w, r, "UpdateTimeToLive", tableName)
				return
			}
		case "query":
			s.forwardDynamoDBDashboardOperation(w, r, "Query", tableName)
			return
		case "scan":
			s.forwardDynamoDBDashboardOperation(w, r, "Scan", tableName)
			return
		case "delete":
			s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, "DeleteTable", tableName, tableName)
			return
		}
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	table, found := s.dynamo.TableSnapshot(tableName)
	if !found {
		http.NotFound(w, r)
		return
	}
	if !hasSuffix {
		writeJSON(w, map[string]any{
			"table": table,
		})
		return
	}
	switch suffix {
	case "indexes":
		writeJSON(w, map[string]any{
			"tableName":              tableName,
			"globalSecondaryIndexes": table.GlobalSecondaryIndexes,
			"localSecondaryIndexes":  table.LocalSecondaryIndexes,
		})
		return
	case "ttl":
		writeJSON(w, map[string]any{
			"tableName":             tableName,
			"timeToLiveDescription": table.TimeToLiveDescription,
		})
		return
	case "streams":
		streamEnabled := table.StreamSpecification != nil && table.StreamSpecification.StreamEnabled
		writeJSON(w, map[string]any{
			"tableName":           tableName,
			"streamEnabled":       streamEnabled,
			"latestStreamArn":     table.LatestStreamArn,
			"latestStreamLabel":   table.LatestStreamLabel,
			"streamSpecification": table.StreamSpecification,
		})
		return
	case "items":
	default:
		http.NotFound(w, r)
		return
	}
	limit := 100
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	items, found := s.dynamo.TableItems(tableName, limit)
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"tableName": tableName,
		"items":     items,
	})
}

type dashboardDynamoDBOperationRequest struct {
	Input        json.RawMessage `json:"input"`
	Confirmation string          `json:"confirmation"`
}

func (s *Server) forwardDynamoDBDashboardOperation(w http.ResponseWriter, r *http.Request, operation string, tableName string) {
	s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, operation, tableName, "")
}

func (s *Server) forwardDynamoDBDashboardOperationWithConfirmation(w http.ResponseWriter, r *http.Request, operation string, tableName string, requiredConfirmation string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request dashboardDynamoDBOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid json request", http.StatusBadRequest)
		return
	}
	if requiredConfirmation != "" && request.Confirmation != requiredConfirmation {
		http.Error(w, "confirmation must match table name", http.StatusBadRequest)
		return
	}
	input, err := normalizeDynamoDBDashboardInput(request.Input, tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPost
	req.URL = &url.URL{Path: "/"}
	req.RequestURI = ""
	req.Body = io.NopCloser(bytes.NewReader(input))
	req.ContentLength = int64(len(input))
	req.Header = make(http.Header)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+operation)
	s.dynamo.ServeHTTP(w, req)
}

func normalizeDynamoDBDashboardInput(raw json.RawMessage, tableName string) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("input is required")
	}
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, errors.New("input must be valid JSON")
	}
	if input == nil {
		return nil, errors.New("input must be a JSON object")
	}
	if tableName != "" {
		if existing, ok := input["TableName"]; ok {
			if existingName, ok := existing.(string); !ok || existingName != tableName {
				return nil, errors.New("input TableName must match the selected table")
			}
		} else {
			input["TableName"] = tableName
		}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, errors.New("input could not be encoded")
	}
	return encoded, nil
}
