package redshift

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type statement struct {
	ID                string
	ClusterIdentifier string
	Database          string
	DbUser            string
	SessionID         string
	QueryString       string
	ResultFormat      string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Status            string
	Error             string
	HasResultSet      bool
	Result            queryResult
}

type session struct {
	ID                      string
	CreatedAt               time.Time
	UpdatedAt               time.Time
	ExpiresAt               time.Time
	SessionKeepAliveSeconds int
}

type executeStatementRequest struct {
	ClusterIdentifier       string
	Database                string
	DbUser                  string
	SQL                     string `json:"Sql"`
	ClientToken             string
	SessionID               string `json:"SessionId"`
	SessionKeepAliveSeconds int
	ResultFormat            string
}

type batchExecuteStatementRequest struct {
	ClusterIdentifier       string
	Database                string
	DbUser                  string
	SQLs                    []string `json:"Sqls"`
	ClientToken             string
	SessionID               string `json:"SessionId"`
	SessionKeepAliveSeconds int
	ResultFormat            string
}

type statementIDRequest struct {
	ID string `json:"Id"`
}

type getStatementResultRequest struct {
	ID         string `json:"Id"`
	MaxResults int
	NextToken  string
}

type listStatementsRequest struct {
	Status string
}

type listMetadataRequest struct {
	ClusterIdentifier string
	ConnectedDatabase string
	Database          string
	DbUser            string
	Schema            string
	SchemaPattern     string
	TablePattern      string
	MaxResults        int
	NextToken         string
}

type describeTableRequest struct {
	ClusterIdentifier string
	ConnectedDatabase string
	Database          string
	DbUser            string
	Schema            string
	Table             string
	MaxResults        int
	NextToken         string
}

type executeStatementResponse struct {
	ID                string `json:"Id"`
	ClusterIdentifier string
	Database          string
	DbUser            string
	SessionID         string `json:"SessionId,omitempty"`
	CreatedAt         int64
}

type describeStatementResponse struct {
	ID                string `json:"Id"`
	ClusterIdentifier string
	Database          string
	DbUser            string
	SessionID         string `json:"SessionId,omitempty"`
	QueryString       string
	Status            string
	Error             string `json:",omitempty"`
	CreatedAt         int64
	UpdatedAt         int64
	Duration          int64
	HasResultSet      bool
	ResultRows        int64
	ResultSize        int64
	RedshiftQueryID   int64 `json:"RedshiftQueryId"`
}

type statementListItem struct {
	ID           string `json:"Id"`
	QueryString  string
	Status       string
	CreatedAt    int64
	UpdatedAt    int64
	HasResultSet bool
}

type dataAPIResultField struct {
	IsNull       bool     `json:"isNull,omitempty"`
	LongValue    *int64   `json:"longValue,omitempty"`
	DoubleValue  *float64 `json:"doubleValue,omitempty"`
	BooleanValue *bool    `json:"booleanValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
}

type columnMetadata struct {
	Name            string `json:"name"`
	Label           string `json:"label"`
	SchemaName      string `json:"schemaName,omitempty"`
	TableName       string `json:"tableName,omitempty"`
	TypeName        string `json:"typeName"`
	ColumnDefault   string `json:"columnDefault,omitempty"`
	IsCaseSensitive bool   `json:"isCaseSensitive"`
	IsSigned        bool   `json:"isSigned"`
	Nullable        int    `json:"nullable"`
	Precision       int    `json:"precision"`
	Scale           int    `json:"scale"`
}

type tableMember struct {
	Name   string
	Schema string
	Type   string
}

func decodeDataAPIRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(target); err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "invalid JSON request")
		return false
	}
	return true
}

func (s *Server) nextStatementIDValue() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextStatementID++
	return fmt.Sprintf("devcloud-redshift-%d", s.nextStatementID)
}

func (s *Server) statementByID(w http.ResponseWriter, id string) *statement {
	if id == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Id is required")
		return nil
	}
	s.mu.Lock()
	stmt := s.statements[id]
	s.mu.Unlock()
	if stmt == nil {
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "statement does not exist")
		return nil
	}
	return stmt
}

func executeStatementResponseFromStatement(stmt *statement) executeStatementResponse {
	return executeStatementResponse{
		ID:                stmt.ID,
		ClusterIdentifier: stmt.ClusterIdentifier,
		Database:          stmt.Database,
		DbUser:            stmt.DbUser,
		SessionID:         stmt.SessionID,
		CreatedAt:         stmt.CreatedAt.Unix(),
	}
}

func describeStatementResponseFromStatement(stmt *statement) describeStatementResponse {
	return describeStatementResponse{
		ID:                stmt.ID,
		ClusterIdentifier: stmt.ClusterIdentifier,
		Database:          stmt.Database,
		DbUser:            stmt.DbUser,
		SessionID:         stmt.SessionID,
		QueryString:       safeStatementQueryString(stmt.QueryString),
		Status:            stmt.Status,
		Error:             stmt.Error,
		CreatedAt:         stmt.CreatedAt.Unix(),
		UpdatedAt:         stmt.UpdatedAt.Unix(),
		HasResultSet:      stmt.HasResultSet,
		ResultRows:        int64(len(stmt.Result.rows)),
		ResultSize:        approximateResultSize(stmt.Result),
		RedshiftQueryID:   redshiftQueryID(stmt.ID),
	}
}

func getStatementResultResponse(result queryResult, rows [][]string) map[string]any {
	records := make([][]dataAPIResultField, 0, len(rows))
	for _, row := range rows {
		fields := make([]dataAPIResultField, 0, len(row))
		for i, value := range row {
			typeOID := pgTypeVarcharOID
			if i < len(result.fields) {
				typeOID = result.fields[i].TypeOID
			}
			fields = append(fields, dataAPIField(value, typeOID))
		}
		records = append(records, fields)
	}
	metadata := make([]columnMetadata, 0, len(result.fields))
	for i, field := range result.fields {
		metadata = append(metadata, columnMetadataFromPGField(field, i))
	}
	return map[string]any{
		"ColumnMetadata": metadata,
		"Records":        records,
		"TotalNumRows":   len(result.rows),
	}
}

func getStatementResultV2Response(result queryResult, rows [][]string) (map[string]any, error) {
	records := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		record, err := csvRecord(row)
		if err != nil {
			return nil, err
		}
		records = append(records, map[string]string{"CSVRecords": record})
	}
	metadata := make([]columnMetadata, 0, len(result.fields))
	for i, field := range result.fields {
		metadata = append(metadata, columnMetadataFromPGField(field, i))
	}
	return map[string]any{
		"ColumnMetadata": metadata,
		"Records":        records,
		"ResultFormat":   "CSV",
		"TotalNumRows":   len(result.rows),
	}, nil
}

func csvRecord(row []string) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(row); err != nil {
		return "", err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

func dataAPIField(value string, typeOID int32) dataAPIResultField {
	switch typeOID {
	case pgTypeInt4OID:
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return dataAPIResultField{LongValue: &parsed}
		}
	case pgTypeFloat8OID:
		if parsed, err := strconv.ParseFloat(value, 64); err == nil {
			return dataAPIResultField{DoubleValue: &parsed}
		}
	case pgTypeBoolOID:
		if parsed, err := strconv.ParseBool(strings.ToLower(value)); err == nil {
			return dataAPIResultField{BooleanValue: &parsed}
		}
	}
	return dataAPIResultField{StringValue: &value}
}

func columnMetadataFromPGField(field pgField, ordinal int) columnMetadata {
	typeName := "varchar"
	precision := 256
	signed := false
	switch field.TypeOID {
	case pgTypeInt4OID:
		typeName = "int4"
		precision = 10
		signed = true
	case pgTypeFloat8OID:
		typeName = "float8"
		precision = 17
		signed = true
	case pgTypeBoolOID:
		typeName = "bool"
		precision = 1
	}
	return columnMetadata{
		Name:            field.Name,
		Label:           field.Name,
		TypeName:        typeName,
		IsCaseSensitive: false,
		IsSigned:        signed,
		Nullable:        1,
		Precision:       precision,
		Scale:           0,
	}
}

func columnMetadataFromColumn(column column, ordinal int) columnMetadata {
	return columnMetadataFromPGField(pgField{
		Name:     column.name,
		TypeOID:  pgTypeOID(column.dataType),
		TypeSize: pgTypeSize(column.dataType),
	}, ordinal)
}

func approximateResultSize(result queryResult) int64 {
	var size int64
	for _, row := range result.rows {
		for _, value := range row {
			size += int64(len(value))
		}
	}
	return size
}

func redshiftQueryID(id string) int64 {
	var result int64
	for _, ch := range id {
		result = result*31 + int64(ch)
		if result < 0 {
			result = -result
		}
	}
	if result == 0 {
		return 1
	}
	return result
}

func safeStatementQueryString(sql string) string {
	preview, redacted, _ := safeSQLPreview(sql, len(sql))
	if redacted {
		return preview
	}
	return sql
}

func (s *Server) nextSessionIDValue() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSessionID++
	return fmt.Sprintf("devcloud-redshift-session-%d", s.nextSessionID)
}

func (s *Server) sessionIDForRequest(sessionID string, keepAliveSeconds int, now time.Time) (string, error) {
	if keepAliveSeconds < 0 {
		return "", fmt.Errorf("SessionKeepAliveSeconds must be non-negative")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" && keepAliveSeconds <= 0 {
		return "", nil
	}
	if sessionID == "" {
		sessionID = s.nextSessionIDValue()
	}
	expiresAt := now
	if keepAliveSeconds > 0 {
		expiresAt = now.Add(time.Duration(keepAliveSeconds) * time.Second)
	}
	s.mu.Lock()
	if existing := s.sessions[sessionID]; existing != nil {
		existing.UpdatedAt = now
		existing.SessionKeepAliveSeconds = keepAliveSeconds
		existing.ExpiresAt = expiresAt
	} else {
		s.sessions[sessionID] = &session{
			ID:                      sessionID,
			CreatedAt:               now,
			UpdatedAt:               now,
			ExpiresAt:               expiresAt,
			SessionKeepAliveSeconds: keepAliveSeconds,
		}
	}
	s.mu.Unlock()
	return sessionID, nil
}

func safeSQLPreview(sql string, maxBytes int) (string, bool, bool) {
	preview := strings.Join(strings.Fields(sql), " ")
	if preview == "" || maxBytes <= 0 {
		return "", false, false
	}
	lower := strings.ToLower(preview)
	for _, token := range []string{
		"authorization",
		"credentials",
		"access_key_id",
		"secret_access_key",
		"session_token",
		"iam_role",
		"password",
	} {
		if strings.Contains(lower, token) {
			return "[redacted]", true, false
		}
	}
	if len(preview) <= maxBytes {
		return preview, false, false
	}
	return preview[:maxBytes], false, true
}
